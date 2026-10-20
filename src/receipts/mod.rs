//! Per-replica Wicket receipt chain.
//!
//! The gateway calls the vendored `infinity-kernel` in this process. Receipt
//! bytes are whatever that kernel issues: no new field and no new entry type.
//! Each process has its own Ed25519 key, log, and anchor. The head lives in
//! memory after one startup verification. Postgres is not involved.

use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex,
    },
    time::{SystemTime, UNIX_EPOCH},
};

use infinity_kernel::{
    evaluate, issue_outcome, issue_receipt, verify_log, verify_signatures, Actor, ApprovalSet,
    LogEntry, Policy, Proposal, Signer, TrustedKeys, POLICY_VERSION, PROPOSAL_VERSION,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

/// Kernel policy id hashed into every gateway receipt. Stable so a reader can
/// see that every entry was judged under the same rules.
const POLICY_ID: &str = "llm-gateway.receipts.v1";
const ANCHOR_VERSION: &str = "infinity.anchor.v1";

/// `sha256:<64 lowercase hex>`, the form the kernel accepts on an outcome.
pub fn sha256_prefixed(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

/// What the gateway knows about a request, and nothing it must not keep.
/// `action` is a short code (`reserve`, `hold`, or the 4xx code). Message
/// text never goes in here.
#[derive(Debug, Clone)]
pub struct Subject {
    pub request_id: String,
    pub tenant_id: String,
    pub key_id: String,
    pub model: String,
    pub action: String,
    pub request_sha256: Option<String>,
}

/// An allow receipt the stream must close with an outcome.
#[derive(Clone)]
pub struct OpenReceipt {
    pub chain: std::sync::Arc<ReceiptChain>,
    pub decision_receipt_id: String,
    pub request_sha256: String,
}

#[derive(Debug, thiserror::Error)]
pub enum ReceiptError {
    #[error("receipt log failed verification: {0}")]
    Unverified(String),
    #[error("prev_hash does not match the chain head")]
    PrevHashMismatch,
    #[error("the receipt log is unavailable")]
    Unavailable,
    #[error("receipt log io: {0}")]
    Io(String),
}

fn io_err(err: impl std::fmt::Display) -> ReceiptError {
    ReceiptError::Io(err.to_string())
}

struct Inner {
    signer: Signer,
    log: File,
    anchor: File,
    head: Option<LogEntry>,
    count: u64,
    failed: bool,
}

/// One replica's chain. Cheap to clone the `Arc`; the log stays here.
pub struct ReceiptChain {
    key_id: String,
    log_path: PathBuf,
    anchor_path: PathBuf,
    public_key_path: PathBuf,
    rotate_every: u64,
    verifies: AtomicU64,
    fail_appends: AtomicU64,
    inner: Mutex<Inner>,
}

impl ReceiptChain {
    /// Create or reopen this replica's chain. Verifies the whole log once.
    /// A log that does not verify refuses to open.
    pub fn open(directory: &Path, rotate_every: u64) -> Result<Self, ReceiptError> {
        if rotate_every == 0 {
            return Err(ReceiptError::Io("rotate_every must be at least 1".into()));
        }
        fs::create_dir_all(directory).map_err(io_err)?;
        let log_path = directory.join("receipts.jsonl");
        let anchor_path = directory.join("anchor.jsonl");
        let key_path = directory.join("signing.priv");
        let public_key_path = directory.join("signing.pub");

        let signer = load_or_create_signer(&key_path)?;
        let public_key = signer.public_key_text();
        fs::write(&public_key_path, format!("{public_key}\n")).map_err(io_err)?;

        let mut log = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&log_path)
            .map_err(io_err)?;
        lock_exclusive(&log)?;
        let text = read_handle(&mut log)?;
        let anchor_text = fs::read_to_string(&anchor_path).unwrap_or_default();
        let entries = verify_snapshot(&text, &anchor_text, &signer)?;

        let anchor = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&anchor_path)
            .map_err(io_err)?;

        let count = entries.len() as u64;
        let head = entries.last().cloned();
        Ok(Self {
            key_id: signer.key_id().to_string(),
            log_path,
            anchor_path,
            public_key_path,
            rotate_every,
            verifies: AtomicU64::new(1),
            fail_appends: AtomicU64::new(0),
            inner: Mutex::new(Inner {
                signer,
                log,
                anchor,
                head,
                count,
                failed: false,
            }),
        })
    }

    pub fn key_id(&self) -> &str {
        &self.key_id
    }

    pub fn log_path(&self) -> &Path {
        &self.log_path
    }

    pub fn anchor_path(&self) -> &Path {
        &self.anchor_path
    }

    pub fn public_key_path(&self) -> &Path {
        &self.public_key_path
    }

    /// How many times the whole chain was read and verified. Startup is one.
    /// A request append does not increment this. Rotation does.
    pub fn verifies(&self) -> u64 {
        self.verifies.load(Ordering::SeqCst)
    }

    pub fn len(&self) -> u64 {
        self.inner.lock().unwrap_or_else(|p| p.into_inner()).count
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn head_id(&self) -> Option<String> {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .head
            .as_ref()
            .map(|entry| entry.receipt_id().to_string())
    }

    /// Test hook: the next `n` appends fail before they touch the log.
    pub fn fail_next_appends(&self, n: u64) {
        self.fail_appends.store(n, Ordering::SeqCst);
    }

    /// Reservation → kernel `allow`.
    pub fn allow(&self, subject: &Subject) -> Result<String, ReceiptError> {
        self.judge(subject, "read", false, "allow")
    }

    /// 4xx → kernel `deny`.
    pub fn deny(&self, subject: &Subject) -> Result<String, ReceiptError> {
        self.judge(subject, "write", false, "deny")
    }

    /// Approval hold → kernel `await_human_approval`.
    pub fn await_approval(&self, subject: &Subject) -> Result<String, ReceiptError> {
        self.judge(subject, "read", true, "await_human_approval")
    }

    /// Stream end → kernel outcome. Hashes only; the kernel never sees text.
    pub fn outcome(
        &self,
        decision_receipt_id: &str,
        status: &str,
        request_sha256: &str,
        response_sha256: &str,
    ) -> Result<String, ReceiptError> {
        let decision_receipt_id = decision_receipt_id.to_string();
        let status = status.to_string();
        let request_sha256 = request_sha256.to_string();
        let response_sha256 = response_sha256.to_string();
        let issued_at = issued_at_now()?;
        self.with_head(move |previous| {
            issue_outcome(
                &decision_receipt_id,
                &status,
                Some(request_sha256),
                Some(response_sha256),
                previous,
                issued_at,
            )
            .map(LogEntry::from)
            .map_err(|err| ReceiptError::Io(err.to_string()))
        })
    }

    /// Append an already-built entry. The only check against the log is that
    /// `prev_hash` equals the head kept in memory. Tests use this to offer a
    /// receipt whose previous hash is not the head.
    #[cfg(test)]
    pub(crate) fn append_prepared(&self, entry: LogEntry) -> Result<String, ReceiptError> {
        self.with_head(move |_| Ok(entry))
    }

    fn judge(
        &self,
        subject: &Subject,
        effect: &str,
        requires_approval: bool,
        expect: &str,
    ) -> Result<String, ReceiptError> {
        let subject = subject.clone();
        let effect = effect.to_string();
        let expect = expect.to_string();
        let issued_at = issued_at_now()?;
        self.with_head(move |previous| {
            let proposal = proposal_for(&subject, &effect, requires_approval);
            let call_digest = subject.request_sha256.clone();
            let caller_id = Some(subject.key_id.clone());
            let decision = evaluate(proposal, gateway_policy(), ApprovalSet::default())
                .map_err(|err| ReceiptError::Io(err.to_string()))?;
            if decision.verdict != expect {
                return Err(ReceiptError::Io(format!(
                    "kernel verdict was {}, expected {expect}",
                    decision.verdict
                )));
            }
            issue_receipt(&decision, previous, issued_at, call_digest, caller_id)
                .map(LogEntry::from)
                .map_err(|err| ReceiptError::Io(err.to_string()))
        })
    }

    fn with_head(
        &self,
        make: impl FnOnce(Option<&LogEntry>) -> Result<LogEntry, ReceiptError>,
    ) -> Result<String, ReceiptError> {
        if self.take_failure() {
            return Err(ReceiptError::Unavailable);
        }
        let mut inner = self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if inner.failed {
            return Err(ReceiptError::Unavailable);
        }
        let entry = make(inner.head.as_ref())?;
        self.persist(&mut inner, entry)
    }

    fn persist(&self, inner: &mut Inner, mut entry: LogEntry) -> Result<String, ReceiptError> {
        let expected = inner.head.as_ref().map(|head| head.receipt_id());
        if entry.previous_receipt_hash() != expected {
            return Err(ReceiptError::PrevHashMismatch);
        }
        entry.sign(&inner.signer);
        let line = serde_json::to_string(&entry).map_err(io_err)?;
        writeln!(inner.log, "{line}").map_err(|err| {
            inner.failed = true;
            io_err(err)
        })?;
        if let Err(err) = inner.log.sync_all() {
            inner.failed = true;
            return Err(io_err(err));
        }
        let id = entry.receipt_id().to_string();
        inner.head = Some(entry);
        inner.count += 1;
        if inner.count % self.rotate_every == 0 {
            self.publish_anchor(inner)?;
        }
        Ok(id)
    }

    /// Re-read and verify the chain, then append an anchor for the head.
    /// This is the only path that reads the log after startup.
    fn publish_anchor(&self, inner: &mut Inner) -> Result<(), ReceiptError> {
        let text = read_handle(&mut inner.log)?;
        let anchor_text = fs::read_to_string(&self.anchor_path).unwrap_or_default();
        let entries = match verify_snapshot(&text, &anchor_text, &inner.signer) {
            Ok(entries) => entries,
            Err(err) => {
                inner.failed = true;
                return Err(err);
            }
        };
        self.verifies.fetch_add(1, Ordering::SeqCst);
        let disk_head = entries.last().map(|entry| entry.receipt_id().to_string());
        let memory_head = inner.head.as_ref().map(|entry| entry.receipt_id().to_string());
        if disk_head != memory_head || entries.len() as u64 != inner.count {
            inner.failed = true;
            return Err(ReceiptError::Unverified(
                "rotation verify disagreed with the in-memory head".into(),
            ));
        }
        let head = memory_head.ok_or_else(|| {
            inner.failed = true;
            ReceiptError::Unverified("rotation of an empty log".into())
        })?;
        let count = inner.count;
        let mut record = json!({
            "version": ANCHOR_VERSION,
            "count": count,
            "head_receipt_id": head,
        });
        record["key_id"] = json!(inner.signer.key_id());
        record["signature"] = json!(inner.signer.sign_anchor(count, &head));
        let line = serde_json::to_string(&record).map_err(io_err)?;
        if let Err(err) = writeln!(inner.anchor, "{line}").and_then(|()| inner.anchor.sync_all()) {
            inner.failed = true;
            return Err(io_err(err));
        }
        Ok(())
    }

    fn take_failure(&self) -> bool {
        let mut current = self.fail_appends.load(Ordering::SeqCst);
        loop {
            if current == 0 {
                return false;
            }
            match self.fail_appends.compare_exchange(
                current,
                current - 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
    }
}

fn gateway_policy() -> Policy {
    Policy {
        version: POLICY_VERSION.into(),
        policy_id: POLICY_ID.into(),
        denied_effects: vec!["write".into()],
        effects_requiring_approval: vec![],
        risks_requiring_approval: vec![],
        callers: None,
    }
}

fn proposal_for(subject: &Subject, effect: &str, requires_approval: bool) -> Proposal {
    Proposal {
        version: PROPOSAL_VERSION.into(),
        proposal_id: subject.request_id.clone(),
        actor: Actor {
            kind: "tenant".into(),
            id: subject.tenant_id.clone(),
        },
        action: subject.action.clone(),
        target: if subject.model.is_empty() {
            "gateway".into()
        } else {
            subject.model.clone()
        },
        effect: effect.into(),
        risk: "low".into(),
        requires_human_approval: requires_approval,
        policy_version: POLICY_ID.into(),
        payload: json!({ "code": subject.action }),
        evidence_refs: vec![],
        approval_id: None,
        call_digest: None,
        caller_id: Some(subject.key_id.clone()),
    }
}

fn verify_snapshot(log_text: &str, anchor_text: &str, signer: &Signer) -> Result<Vec<LogEntry>, ReceiptError> {
    let entries = read_entries(log_text)?;
    if !verify_log(&entries).map_err(|err| ReceiptError::Unverified(err.to_string()))? {
        return Err(ReceiptError::Unverified(
            "a receipt hash, link, or outcome does not verify".into(),
        ));
    }
    let keys = TrustedKeys::from_signer(signer);
    verify_signatures(&entries, &keys, true).map_err(|err| ReceiptError::Unverified(err.to_string()))?;
    check_anchors(&entries, anchor_text, &keys)?;
    Ok(entries)
}

fn read_entries(text: &str) -> Result<Vec<LogEntry>, ReceiptError> {
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str::<LogEntry>(line).map_err(|err| ReceiptError::Unverified(err.to_string())))
        .collect()
}

fn check_anchors(entries: &[LogEntry], text: &str, keys: &TrustedKeys) -> Result<(), ReceiptError> {
    for (index, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let number = index + 1;
        let record: Value = serde_json::from_str(line)
            .map_err(|err| ReceiptError::Unverified(format!("anchor line {number}: {err}")))?;
        let version = record.get("version").and_then(Value::as_str);
        let count = record.get("count").and_then(Value::as_u64);
        let head = record.get("head_receipt_id").and_then(Value::as_str);
        if version != Some(ANCHOR_VERSION) || count.unwrap_or(0) == 0 || head.unwrap_or("").is_empty() {
            return Err(ReceiptError::Unverified(format!("anchor line {number} is malformed")));
        }
        let count = count.unwrap_or(0);
        let head = head.unwrap_or("");
        let key_id = record.get("key_id").and_then(Value::as_str);
        let signature = record.get("signature").and_then(Value::as_str);
        let (Some(key_id), Some(signature)) = (key_id, signature) else {
            return Err(ReceiptError::Unverified(format!("anchor line {number} is unsigned")));
        };
        if !keys.verify_anchor(key_id, count, head, signature) {
            return Err(ReceiptError::Unverified(format!(
                "anchor line {number} signature does not verify"
            )));
        }
        let Some(entry) = count.checked_sub(1).and_then(|i| entries.get(i as usize)) else {
            return Err(ReceiptError::Unverified(format!(
                "anchor line {number} expects {count} entries but the log is shorter"
            )));
        };
        if entry.receipt_id() != head {
            return Err(ReceiptError::Unverified(format!(
                "anchor line {number} does not match the log"
            )));
        }
    }
    Ok(())
}

fn read_handle(file: &mut File) -> Result<String, ReceiptError> {
    file.seek(SeekFrom::Start(0)).map_err(io_err)?;
    let mut text = String::new();
    file.read_to_string(&mut text).map_err(io_err)?;
    Ok(text)
}

fn load_or_create_signer(path: &Path) -> Result<Signer, ReceiptError> {
    if path.exists() {
        ensure_private(path)?;
        let text = fs::read_to_string(path).map_err(io_err)?;
        return Signer::from_text(&text).map_err(|err| ReceiptError::Io(err.to_string()));
    }
    let mut seed = [0u8; 32];
    getrandom::getrandom(&mut seed).map_err(|_| ReceiptError::Io("could not generate a signing key".into()))?;
    let signer = Signer::from_seed(seed);
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path).map_err(io_err)?;
    writeln!(file, "{}", signer.private_key_text()).map_err(io_err)?;
    file.sync_all().map_err(io_err)?;
    Ok(signer)
}

/// Refuse a signing key that group or other can read. Windows has no mode
/// bits; the file stays in the replica's directory, which is that replica's
/// to protect.
#[cfg(unix)]
fn ensure_private(path: &Path) -> Result<(), ReceiptError> {
    use std::os::unix::fs::PermissionsExt;
    let mode = fs::metadata(path).map_err(io_err)?.permissions().mode();
    if mode & 0o077 != 0 {
        return Err(ReceiptError::Io(
            "signing key file is readable by group or others".into(),
        ));
    }
    Ok(())
}

#[cfg(not(unix))]
fn ensure_private(_path: &Path) -> Result<(), ReceiptError> {
    Ok(())
}

#[cfg(unix)]
fn lock_exclusive(file: &File) -> Result<(), ReceiptError> {
    use std::os::unix::io::AsRawFd;
    // SAFETY: `file` is an open descriptor we own, and `flock` only uses that fd.
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if rc == 0 {
        Ok(())
    } else {
        Err(ReceiptError::Io(
            "receipt log is locked by another process; each replica needs its own directory".into(),
        ))
    }
}

#[cfg(windows)]
fn lock_exclusive(file: &File) -> Result<(), ReceiptError> {
    use std::os::windows::io::AsRawHandle;
    #[repr(C)]
    struct Overlapped {
        internal: usize,
        internal_high: usize,
        offset: u32,
        offset_high: u32,
        event: *mut core::ffi::c_void,
    }
    extern "system" {
        fn LockFileEx(
            file: *mut core::ffi::c_void,
            flags: u32,
            reserved: u32,
            bytes_low: u32,
            bytes_high: u32,
            overlapped: *mut Overlapped,
        ) -> i32;
    }
    const LOCKFILE_FAIL_IMMEDIATELY: u32 = 0x0000_0001;
    const LOCKFILE_EXCLUSIVE_LOCK: u32 = 0x0000_0002;
    let mut overlapped = Overlapped {
        internal: 0,
        internal_high: 0,
        offset: 0,
        offset_high: 0,
        event: core::ptr::null_mut(),
    };
    // SAFETY: the handle belongs to `file`, and `overlapped` outlives this call.
    // FAIL_IMMEDIATELY returns before the function does, so the stack record
    // does not have to outlive the lock. The lock lasts until the handle closes.
    let ok = unsafe {
        LockFileEx(
            file.as_raw_handle() as *mut core::ffi::c_void,
            LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY,
            0,
            u32::MAX,
            u32::MAX,
            &mut overlapped,
        )
    };
    if ok != 0 {
        Ok(())
    } else {
        Err(ReceiptError::Io(
            "receipt log is locked by another process; each replica needs its own directory".into(),
        ))
    }
}

fn issued_at_now() -> Result<String, ReceiptError> {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ReceiptError::Io("the system clock is before 1970".into()))?
        .as_secs();
    Ok(rfc3339_from_unix(secs as i64))
}

/// Seconds since 1970 as `YYYY-MM-DDTHH:MM:SSZ`. Same civil conversion the
/// kernel's command-line tool uses.
fn rfc3339_from_unix(secs: i64) -> String {
    let (days, rem) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    let (hour, minute, second) = (rem / 3600, rem % 3600 / 60, rem % 60);
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let day_of_era = z.rem_euclid(146_097);
    let year_of_era = (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let mp = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
}

#[cfg(test)]
mod tests {
    use super::*;
    use infinity_kernel::{issue_receipt, ApprovalSet, LogEntry};
    use std::sync::atomic::AtomicU64;

    fn scratch(name: &str) -> PathBuf {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "llm-gw-receipts-{name}-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn subject(action: &str) -> Subject {
        Subject {
            request_id: format!("req-{action}"),
            tenant_id: "acme".into(),
            key_id: "ak_test".into(),
            model: "mock/chat".into(),
            action: action.into(),
            request_sha256: Some(sha256_prefixed(b"request-bytes")),
        }
    }

    fn lines(path: &Path) -> Vec<String> {
        fs::read_to_string(path)
            .unwrap()
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(str::to_string)
            .collect()
    }

    fn python() -> &'static str {
        if std::process::Command::new("python3")
            .arg("--version")
            .output()
            .is_ok()
        {
            "python3"
        } else {
            "python"
        }
    }

    fn ickverify_ok(chain: &ReceiptChain, anchor: bool) -> bool {
        let script = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("vendor/wicket/verifier/ickverify.py");
        let mut cmd = std::process::Command::new(python());
        cmd.arg(&script)
            .arg(chain.log_path())
            .arg("--trusted-keys")
            .arg(chain.public_key_path())
            .arg("--require-signatures");
        if anchor {
            cmd.arg("--anchor").arg(chain.anchor_path());
        }
        cmd.status().expect("python3 ickverify.py").success()
    }

    #[test]
    fn four_mappings_verify_with_unchanged_ickverify() {
        let dir = scratch("map");
        let chain = ReceiptChain::open(&dir, 100).unwrap();
        let allow_id = chain.allow(&subject("reserve")).unwrap();
        let response = sha256_prefixed(b"Hello, world");
        chain
            .outcome(&allow_id, "completed", subject("reserve").request_sha256.as_deref().unwrap(), &response)
            .unwrap();
        chain.deny(&subject("no_messages")).unwrap();
        chain.await_approval(&subject("hold")).unwrap();

        let text = fs::read_to_string(chain.log_path()).unwrap();
        assert!(!text.contains("TOP-SECRET-PROMPT"), "{text}");
        assert!(!text.contains("Hello, world"), "{text}");
        let rows: Vec<Value> = text.lines().filter(|l| !l.is_empty()).map(|l| serde_json::from_str(l).unwrap()).collect();
        assert_eq!(rows[0]["verdict"], "allow");
        assert_eq!(rows[0]["version"], "infinity.receipt.v2");
        assert_eq!(rows[1]["version"], "infinity.outcome.v1");
        assert_eq!(rows[1]["decision_receipt_id"], rows[0]["receipt_id"]);
        assert_eq!(rows[1]["request_sha256"], subject("reserve").request_sha256.unwrap());
        assert_eq!(rows[1]["response_sha256"], response);
        assert!(rows[1].get("request").is_none());
        assert_eq!(rows[2]["verdict"], "deny");
        assert_eq!(rows[3]["verdict"], "await_human_approval");
        assert!(ickverify_ok(&chain, false), "ickverify rejected the chain");
    }

    #[test]
    fn startup_verifies_once_and_appends_against_the_memory_head() {
        let dir = scratch("head");
        let chain = ReceiptChain::open(&dir, 100).unwrap();
        assert_eq!(chain.verifies(), 1);
        chain.allow(&subject("reserve")).unwrap();
        chain.deny(&subject("no_messages")).unwrap();
        assert_eq!(chain.verifies(), 1, "an append must not rescan the log");

        let mut text = fs::read_to_string(chain.log_path()).unwrap();
        let newline = text.find('\n').unwrap();
        text.replace_range(newline - 1..newline, "0");
        fs::write(chain.log_path(), &text).unwrap();

        chain.await_approval(&subject("hold")).unwrap();
        assert_eq!(chain.verifies(), 1, "the damaged prefix was not read");
        assert_eq!(chain.len(), 3);

        drop(chain);
        match ReceiptChain::open(&dir, 100) {
            Err(ReceiptError::Unverified(_)) => {}
            Err(err) => panic!("expected a verification failure, got {err}"),
            Ok(_) => panic!("a damaged log must not open"),
        }
    }

    #[test]
    fn a_prev_hash_that_is_not_the_head_is_refused() {
        let dir = scratch("prev");
        let chain = ReceiptChain::open(&dir, 100).unwrap();
        chain.allow(&subject("reserve")).unwrap();
        let before = lines(chain.log_path());

        let proposal = proposal_for(&subject("forged"), "read", false);
        let decision = evaluate(proposal, gateway_policy(), ApprovalSet::default()).unwrap();
        let forged = issue_receipt(&decision, None, "2020-01-01T00:00:00Z".into(), None, None).unwrap();
        let err = chain.append_prepared(LogEntry::from(forged)).unwrap_err();
        assert!(matches!(err, ReceiptError::PrevHashMismatch), "{err}");
        assert_eq!(lines(chain.log_path()), before);
        assert_eq!(chain.len(), 1);
    }

    #[test]
    fn rotation_verifies_again_and_anchors_the_new_head() {
        let dir = scratch("rotate");
        let chain = ReceiptChain::open(&dir, 2).unwrap();
        assert_eq!(chain.verifies(), 1);
        chain.allow(&subject("reserve")).unwrap();
        assert!(lines(chain.anchor_path()).is_empty());
        chain.deny(&subject("no_messages")).unwrap();
        assert_eq!(chain.verifies(), 2, "rotation must verify the chain again");

        let anchors = lines(chain.anchor_path());
        assert_eq!(anchors.len(), 1);
        let anchor: Value = serde_json::from_str(&anchors[0]).unwrap();
        assert_eq!(anchor["version"], ANCHOR_VERSION);
        assert_eq!(anchor["count"], 2);
        assert_eq!(anchor["head_receipt_id"], chain.head_id().unwrap());
        assert!(ickverify_ok(&chain, true), "ickverify rejected the anchored log");

        let original = fs::read_to_string(chain.log_path()).unwrap();
        let mut damaged = original.clone();
        let byte = damaged.as_bytes()[10];
        damaged.replace_range(10..11, if byte == b'a' { "b" } else { "a" });
        let script = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("vendor/wicket/verifier/ickverify.py");
        let damaged_path = dir.join("damaged.jsonl");
        fs::write(&damaged_path, damaged).unwrap();
        let status = std::process::Command::new(python())
            .arg(&script)
            .arg(&damaged_path)
            .arg("--anchor")
            .arg(chain.anchor_path())
            .arg("--trusted-keys")
            .arg(chain.public_key_path())
            .arg("--require-signatures")
            .status()
            .unwrap();
        assert!(!status.success(), "a tampered log must fail ickverify");
        assert!(ickverify_ok(&chain, true));
        let _ = original;
    }
}
