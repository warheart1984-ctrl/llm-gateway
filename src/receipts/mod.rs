//! Per-replica Wicket receipt chain.
//!
//! The gateway calls the vendored `infinity-kernel` in this process. Receipt
//! bytes are whatever that kernel issues: no new field and no new entry type.
//! Each process has its own Ed25519 key, log, anchor and lock file.
//!
//! One writer thread owns the log. Requests hand it work and wait for the
//! answer without blocking a runtime thread. The writer takes every job that
//! is waiting, writes them as one batch and flushes the disk once, then
//! answers each job: nobody hears "written" before the flush.
//!
//! The whole chain is verified once at startup. After that the writer appends
//! against the head it keeps in memory, and re-reads only the entries written
//! since the last check when it anchors: every `anchor_every` entries, every
//! `anchor_interval`, and at shutdown.
//!
//! Hashes in receipts are salted per request. The salt goes back to the
//! client in `x-receipt-salt`, so whoever holds the request and the reply can
//! recompute both digests, and someone who can only read the log cannot
//! test a guessed prompt against them.

use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc::{self, RecvTimeoutError},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use infinity_kernel::{
    Actor, ApprovalSet, LogEntry, POLICY_VERSION, PROPOSAL_VERSION, Policy, Proposal, Signer,
    TrustedKeys, evaluate, issue_outcome, issue_receipt, record_external_denial, verify_log,
    verify_outcome, verify_receipt, verify_signatures,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::oneshot;

/// Kernel policy id hashed into every gateway receipt. Stable so a reader can
/// see that every entry was judged under the same rules.
const POLICY_ID: &str = "llm-gateway.receipts.v1";
/// Names the gateway as the authority behind a refusal it decided itself.
const AUTHORITY: &str = "llm-gateway";
/// Used when a refusal code is not in the kernel's code alphabet.
const FALLBACK_REASON: &str = "GATEWAY_REFUSED";
const ANCHOR_VERSION: &str = "infinity.anchor.v1";
/// Most jobs written under one disk flush.
const MAX_BATCH: usize = 512;

const REQUEST_DOMAIN: &[u8] = b"llm-gateway/request/v1\n";
const REPLY_DOMAIN: &[u8] = b"llm-gateway/reply/v1\n";
const RAW_REPLY_DOMAIN: &[u8] = b"llm-gateway/raw-reply/v1\n";

// ---- salted digests ------------------------------------------------------------------------

/// 32 random bytes per request. Never written to the log.
#[derive(Clone, PartialEq, Eq)]
pub struct Salt([u8; 32]);

impl Salt {
    pub fn generate() -> Result<Self, ReceiptError> {
        let mut bytes = [0u8; 32];
        getrandom::getrandom(&mut bytes)
            .map_err(|_| ReceiptError::Io("could not generate a receipt salt".into()))?;
        Ok(Self(bytes))
    }

    pub fn to_hex(&self) -> String {
        self.0.iter().map(|b| format!("{b:02x}")).collect()
    }

    pub fn from_hex(text: &str) -> Option<Self> {
        if text.len() != 64 {
            return None;
        }
        let mut bytes = [0u8; 32];
        for (i, byte) in bytes.iter_mut().enumerate() {
            *byte = u8::from_str_radix(text.get(2 * i..2 * i + 2)?, 16).ok()?;
        }
        Some(Self(bytes))
    }
}

impl std::fmt::Debug for Salt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Salt(..)")
    }
}

fn prefixed(hasher: Sha256) -> String {
    format!("sha256:{:x}", hasher.finalize())
}

/// `sha256(salt || "llm-gateway/request/v1\n" || body)`, over the request
/// body exactly as the client sent it.
pub fn request_digest(salt: &Salt, body: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(salt.0);
    hasher.update(REQUEST_DOMAIN);
    hasher.update(body);
    prefixed(hasher)
}

#[derive(Default)]
struct ToolCall {
    id: String,
    name: String,
    arguments: String,
}

/// Digest of a normalized reply, built from the same pieces the client
/// receives, so the client can rebuild it from what it holds:
///
/// ```text
/// sha256( salt || "llm-gateway/reply/v1\n"
///         || sha256(content) || sha256(reasoning) || sha256(tools) )
/// ```
///
/// `content` is every `token` concatenated, `reasoning` every `reasoning`
/// concatenated. `tools` is, for each tool index in ascending order,
/// `index 0x1F id 0x1F name 0x1F arguments 0x1E`, where `name` and
/// `arguments` are that index's deltas concatenated and `id` is the first
/// non-empty id sent for it.
pub struct ReplyDigest {
    salt: Salt,
    content: Sha256,
    reasoning: Sha256,
    tools: BTreeMap<u32, ToolCall>,
}

impl ReplyDigest {
    pub fn new(salt: Salt) -> Self {
        Self {
            salt,
            content: Sha256::new(),
            reasoning: Sha256::new(),
            tools: BTreeMap::new(),
        }
    }

    pub fn content(&mut self, text: &str) {
        self.content.update(text.as_bytes());
    }

    pub fn reasoning(&mut self, text: &str) {
        self.reasoning.update(text.as_bytes());
    }

    pub fn tool(&mut self, index: u32, id: Option<&str>, name: Option<&str>, arguments: Option<&str>) {
        let call = self.tools.entry(index).or_default();
        if call.id.is_empty()
            && let Some(id) = id
        {
            call.id.push_str(id);
        }
        if let Some(name) = name {
            call.name.push_str(name);
        }
        if let Some(arguments) = arguments {
            call.arguments.push_str(arguments);
        }
    }

    pub fn finish(&self) -> String {
        let mut tools = Sha256::new();
        for (index, call) in &self.tools {
            tools.update(index.to_string().as_bytes());
            for part in [&call.id, &call.name, &call.arguments] {
                tools.update([0x1f]);
                tools.update(part.as_bytes());
            }
            tools.update([0x1e]);
        }
        let mut hasher = Sha256::new();
        hasher.update(self.salt.0);
        hasher.update(REPLY_DOMAIN);
        hasher.update(self.content.clone().finalize());
        hasher.update(self.reasoning.clone().finalize());
        hasher.update(tools.finalize());
        prefixed(hasher)
    }
}

/// Digest of a passthrough reply: `sha256(salt || "llm-gateway/raw-reply/v1\n"
/// || every byte relayed)`.
pub struct RawReplyDigest(Sha256);

impl RawReplyDigest {
    pub fn new(salt: &Salt) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(salt.0);
        hasher.update(RAW_REPLY_DOMAIN);
        Self(hasher)
    }

    pub fn update(&mut self, bytes: &[u8]) {
        self.0.update(bytes);
    }

    pub fn finish(&self) -> String {
        prefixed(self.0.clone())
    }
}

// ---- chain --------------------------------------------------------------------------------

/// What the gateway knows about a request, and nothing it must not keep.
/// `action` is a short code (`reserve`, `hold`, or the refusal code). Message
/// text never goes in here. `call_digest` is a salted request digest.
#[derive(Debug, Clone)]
pub struct Subject {
    pub request_id: String,
    pub tenant_id: String,
    pub key_id: String,
    pub model: String,
    pub action: String,
    pub call_digest: Option<String>,
}

/// An allow receipt the stream must close with an outcome.
#[derive(Clone)]
pub struct OpenReceipt {
    pub chain: Arc<ReceiptChain>,
    pub decision_receipt_id: String,
    pub request_digest: String,
    pub salt: Salt,
}

#[derive(Debug, thiserror::Error)]
pub enum ReceiptError {
    #[error("receipt log failed verification: {0}")]
    Unverified(String),
    #[error("the receipt log is unavailable")]
    Unavailable,
    #[error("receipt log io: {0}")]
    Io(String),
}

fn io_err(err: impl std::fmt::Display) -> ReceiptError {
    ReceiptError::Io(err.to_string())
}

/// How often the writer re-checks what it wrote and anchors the head.
#[derive(Debug, Clone, Copy)]
pub struct ChainOptions {
    /// Anchor after this many new entries.
    pub anchor_every: u64,
    /// Anchor when this much time has passed and there is anything new.
    pub anchor_interval: Duration,
}

impl Default for ChainOptions {
    fn default() -> Self {
        Self {
            anchor_every: 1_000,
            anchor_interval: Duration::from_secs(60),
        }
    }
}

type Answer = Result<String, ReceiptError>;
type Reply = oneshot::Sender<Answer>;

enum Work {
    /// A refusal the gateway decided: recorded as an external denial with
    /// the gateway's own reason.
    Deny {
        subject: Subject,
    },
    Judge {
        subject: Subject,
        effect: &'static str,
        requires_approval: bool,
        expect: &'static str,
    },
    Outcome {
        decision_receipt_id: String,
        status: &'static str,
        request: String,
        response: String,
    },
}

struct Job {
    work: Work,
    reply: Option<Reply>,
}

#[derive(Default)]
struct Shared {
    count: AtomicU64,
    head: Mutex<Option<String>>,
    verifies: AtomicU64,
    anchors: AtomicU64,
    failed: AtomicBool,
    fail_appends: AtomicU64,
}

impl Shared {
    fn take_failure(&self) -> bool {
        let mut current = self.fail_appends.load(Ordering::SeqCst);
        loop {
            if current == 0 {
                return false;
            }
            match self.fail_appends.compare_exchange(current, current - 1, Ordering::SeqCst, Ordering::SeqCst) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
    }
}

/// One replica's chain. Share it behind an `Arc`; the writer thread owns the
/// files and stops when the chain is dropped.
pub struct ReceiptChain {
    key_id: String,
    log_path: PathBuf,
    anchor_path: PathBuf,
    public_key_path: PathBuf,
    shared: Arc<Shared>,
    jobs: Option<mpsc::Sender<Job>>,
    writer: Option<thread::JoinHandle<()>>,
}

impl ReceiptChain {
    /// Create or reopen this replica's chain. Verifies the whole log once.
    /// A log that does not verify, or a directory another process holds,
    /// refuses to open.
    pub fn open(directory: &Path, options: ChainOptions) -> Result<Self, ReceiptError> {
        if options.anchor_every == 0 {
            return Err(ReceiptError::Io("anchor_every must be at least 1".into()));
        }
        if options.anchor_interval.is_zero() {
            return Err(ReceiptError::Io("anchor_interval must be above zero".into()));
        }
        fs::create_dir_all(directory).map_err(io_err)?;
        let log_path = directory.join("receipts.jsonl");
        let anchor_path = directory.join("anchor.jsonl");
        let key_path = directory.join("signing.priv");
        let public_key_path = directory.join("signing.pub");

        // The lock is on its own file so the log stays readable: on Windows a
        // lock on the log itself would stop ickverify and backups reading it.
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(directory.join("receipts.lock"))
            .map_err(io_err)?;
        lock_exclusive(&lock)?;

        let signer = load_or_create_signer(&key_path)?;
        fs::write(&public_key_path, format!("{}\n", signer.public_key_text())).map_err(io_err)?;

        let mut log = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&log_path)
            .map_err(io_err)?;
        let text = read_from(&mut log, 0)?;
        let anchor_text = fs::read_to_string(&anchor_path).unwrap_or_default();
        let entries = verify_snapshot(&text, &anchor_text, &signer)?;
        let anchored_count = anchor_text
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .filter_map(|record| record.get("count").and_then(Value::as_u64))
            .max()
            .unwrap_or(0);

        let anchor = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&anchor_path)
            .map_err(io_err)?;

        let count = entries.len() as u64;
        let head = entries.last().cloned();
        let head_id = head.as_ref().map(|entry| entry.receipt_id().to_string());
        let shared = Arc::new(Shared::default());
        shared.count.store(count, Ordering::SeqCst);
        shared.verifies.store(1, Ordering::SeqCst);
        *shared.head.lock().unwrap_or_else(|p| p.into_inner()) = head_id.clone();

        let key_id = signer.key_id().to_string();
        let writer = Writer {
            keys: TrustedKeys::from_signer(&signer),
            signer,
            log,
            anchor,
            _lock: lock,
            log_path: log_path.clone(),
            head,
            count,
            verified_offset: text.len() as u64,
            verified_count: count,
            verified_head: head_id,
            anchored_count,
            last_anchor: Instant::now(),
            options,
            shared: Arc::clone(&shared),
        };
        let (jobs, inbox) = mpsc::channel();
        let handle = thread::Builder::new()
            .name("receipt-writer".into())
            .spawn(move || writer.run(inbox))
            .map_err(io_err)?;

        Ok(Self {
            key_id,
            log_path,
            anchor_path,
            public_key_path,
            shared,
            jobs: Some(jobs),
            writer: Some(handle),
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

    /// How many times the log was read back and checked. Startup is one, each
    /// anchor adds one. A request append adds nothing.
    pub fn verifies(&self) -> u64 {
        self.shared.verifies.load(Ordering::SeqCst)
    }

    /// Anchor records written by this process.
    pub fn anchors(&self) -> u64 {
        self.shared.anchors.load(Ordering::SeqCst)
    }

    /// Entries durably in the log.
    pub fn len(&self) -> u64 {
        self.shared.count.load(Ordering::SeqCst)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn head_id(&self) -> Option<String> {
        self.shared.head.lock().unwrap_or_else(|p| p.into_inner()).clone()
    }

    /// True once a write or a check failed. Every later append is refused
    /// until the process restarts and the log verifies again.
    pub fn is_failed(&self) -> bool {
        self.shared.failed.load(Ordering::SeqCst)
    }

    /// Test hook: the next `n` appends fail before they touch the log.
    pub fn fail_next_appends(&self, n: u64) {
        self.shared.fail_appends.store(n, Ordering::SeqCst);
    }

    /// Reservation → kernel `allow`.
    pub async fn allow(&self, subject: &Subject) -> Answer {
        self.judge(subject, "read", false, "allow").await
    }

    /// Governance refusal → `deny` with reason codes
    /// `["EXTERNAL_DENY", <the refusal code in upper case>]`.
    pub async fn deny(&self, subject: &Subject) -> Answer {
        let (reply, answer) = oneshot::channel();
        self.send(Work::Deny { subject: subject.clone() }, Some(reply));
        wait(answer).await
    }

    /// Approval hold → kernel `await_human_approval`.
    pub async fn await_approval(&self, subject: &Subject) -> Answer {
        self.judge(subject, "read", true, "await_human_approval").await
    }

    /// Stream end → kernel outcome, waiting until it is durable.
    pub async fn outcome(&self, open: &OpenReceipt, status: &'static str, response: String) -> Answer {
        wait(self.outcome_pending(open, status, response)).await
    }

    /// Queue an outcome and return the receiver, for code that has to poll
    /// it rather than `.await` it.
    pub fn outcome_pending(&self, open: &OpenReceipt, status: &'static str, response: String) -> oneshot::Receiver<Answer> {
        let (reply, answer) = oneshot::channel();
        self.send(Work::Outcome {
            decision_receipt_id: open.decision_receipt_id.clone(),
            status,
            request: open.request_digest.clone(),
            response,
        }, Some(reply));
        answer
    }

    /// Queue an outcome nobody waits for: a client that hung up, or a stream
    /// that already failed. It is still written in order.
    pub fn outcome_detached(&self, open: &OpenReceipt, status: &'static str, response: String) {
        self.send(Work::Outcome {
            decision_receipt_id: open.decision_receipt_id.clone(),
            status,
            request: open.request_digest.clone(),
            response,
        }, None);
    }

    async fn judge(&self, subject: &Subject, effect: &'static str, requires_approval: bool, expect: &'static str) -> Answer {
        let (reply, answer) = oneshot::channel();
        self.send(Work::Judge {
            subject: subject.clone(),
            effect,
            requires_approval,
            expect,
        }, Some(reply));
        wait(answer).await
    }

    /// A job the writer never receives drops its reply, which reads as
    /// `Unavailable` to whoever waits on it.
    fn send(&self, work: Work, reply: Option<Reply>) {
        if let Some(jobs) = &self.jobs {
            let _ = jobs.send(Job { work, reply });
        }
    }
}

async fn wait(answer: oneshot::Receiver<Answer>) -> Answer {
    answer.await.unwrap_or(Err(ReceiptError::Unavailable))
}

impl Drop for ReceiptChain {
    /// Stop the writer after it drains what is queued and anchors the head,
    /// then release the lock.
    fn drop(&mut self) {
        self.jobs.take();
        if let Some(writer) = self.writer.take() {
            let _ = writer.join();
        }
    }
}

// ---- writer -------------------------------------------------------------------------------

struct Writer {
    signer: Signer,
    keys: TrustedKeys,
    log: File,
    anchor: File,
    _lock: File,
    log_path: PathBuf,
    head: Option<LogEntry>,
    count: u64,
    /// Bytes of the log already read back and checked.
    verified_offset: u64,
    verified_count: u64,
    verified_head: Option<String>,
    anchored_count: u64,
    last_anchor: Instant,
    options: ChainOptions,
    shared: Arc<Shared>,
}

fn answer(reply: Option<Reply>, result: Answer) {
    if let Some(reply) = reply {
        let _ = reply.send(result);
    }
}

impl Writer {
    fn run(mut self, inbox: mpsc::Receiver<Job>) {
        loop {
            let wait = if self.count == self.anchored_count {
                self.options.anchor_interval
            } else {
                self.options
                    .anchor_interval
                    .saturating_sub(self.last_anchor.elapsed())
                    .max(Duration::from_millis(1))
            };
            match inbox.recv_timeout(wait) {
                Ok(first) => {
                    let mut batch = vec![first];
                    while batch.len() < MAX_BATCH {
                        match inbox.try_recv() {
                            Ok(job) => batch.push(job),
                            Err(_) => break,
                        }
                    }
                    self.commit(batch);
                }
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => {
                    self.anchor_if(true);
                    return;
                }
            }
            self.anchor_if(false);
        }
    }

    fn fail(&self, why: &ReceiptError) {
        if !self.shared.failed.swap(true, Ordering::SeqCst) {
            tracing::error!(error = %why, log = %self.log_path.display(), "receipt chain stopped; requests that need receipts get 503 until restart");
        }
    }

    fn build(&self, work: Work, issued_at: String) -> Result<LogEntry, ReceiptError> {
        let previous = self.head.as_ref();
        match work {
            Work::Deny { subject } => {
                let proposal = proposal_for(&subject, "read", false);
                let decision =
                    record_external_denial(&proposal, &gateway_policy(), AUTHORITY, &reason_code(&subject.action))
                        .map_err(io_err)?;
                issue_receipt(&decision, previous, issued_at, subject.call_digest, Some(subject.key_id))
                    .map(LogEntry::from)
                    .map_err(io_err)
            }
            Work::Judge { subject, effect, requires_approval, expect } => {
                let proposal = proposal_for(&subject, effect, requires_approval);
                let decision = evaluate(proposal, gateway_policy(), ApprovalSet::default()).map_err(io_err)?;
                if decision.verdict != expect {
                    return Err(ReceiptError::Io(format!(
                        "kernel verdict was {}, expected {expect}",
                        decision.verdict
                    )));
                }
                issue_receipt(&decision, previous, issued_at, subject.call_digest, Some(subject.key_id))
                    .map(LogEntry::from)
                    .map_err(io_err)
            }
            Work::Outcome { decision_receipt_id, status, request, response } => issue_outcome(
                &decision_receipt_id,
                status,
                Some(request),
                Some(response),
                previous,
                issued_at,
            )
            .map(LogEntry::from)
            .map_err(io_err),
        }
    }

    /// Build every job against the moving head, write them in one go, flush
    /// once, then answer. On a failed write nobody is told "written".
    fn commit(&mut self, batch: Vec<Job>) {
        let start_head = self.head.clone();
        let start_count = self.count;
        let mut lines = String::new();
        let mut written: Vec<(Option<Reply>, String)> = Vec::with_capacity(batch.len());
        for job in batch {
            if self.shared.failed.load(Ordering::SeqCst) || self.shared.take_failure() {
                answer(job.reply, Err(ReceiptError::Unavailable));
                continue;
            }
            let issued_at = match issued_at_now() {
                Ok(at) => at,
                Err(err) => {
                    answer(job.reply, Err(err));
                    continue;
                }
            };
            let mut entry = match self.build(job.work, issued_at) {
                Ok(entry) => entry,
                Err(err) => {
                    answer(job.reply, Err(err));
                    continue;
                }
            };
            entry.sign(&self.signer);
            match serde_json::to_string(&entry) {
                Ok(line) => {
                    lines.push_str(&line);
                    lines.push('\n');
                    written.push((job.reply, entry.receipt_id().to_string()));
                    self.head = Some(entry);
                    self.count += 1;
                }
                Err(err) => answer(job.reply, Err(io_err(err))),
            }
        }
        if written.is_empty() {
            return;
        }
        let flushed = self
            .log
            .write_all(lines.as_bytes())
            .and_then(|()| self.log.sync_data());
        match flushed {
            Ok(()) => {
                self.shared.count.store(self.count, Ordering::SeqCst);
                *self.shared.head.lock().unwrap_or_else(|p| p.into_inner()) =
                    self.head.as_ref().map(|entry| entry.receipt_id().to_string());
                for (reply, id) in written {
                    answer(reply, Ok(id));
                }
            }
            Err(err) => {
                let err = io_err(err);
                self.fail(&err);
                self.head = start_head;
                self.count = start_count;
                for (reply, _) in written {
                    answer(reply, Err(ReceiptError::Unavailable));
                }
            }
        }
    }

    fn anchor_if(&mut self, closing: bool) {
        if self.shared.failed.load(Ordering::SeqCst) || self.count == self.anchored_count {
            return;
        }
        let due = closing
            || self.count - self.anchored_count >= self.options.anchor_every
            || self.last_anchor.elapsed() >= self.options.anchor_interval;
        if due && let Err(err) = self.anchor() {
            self.fail(&err);
        }
    }

    /// Read back only what was written since the last check, confirm each
    /// entry's hash, link and signature, confirm the disk ends at the head in
    /// memory, then write a signed anchor for that head.
    fn anchor(&mut self) -> Result<(), ReceiptError> {
        let mut file = File::open(&self.log_path).map_err(io_err)?;
        let text = read_from(&mut file, self.verified_offset)?;
        let mut previous = self.verified_head.clone();
        let mut checked = self.verified_count;
        let mut consumed = 0u64;
        for line in text.split_inclusive('\n') {
            if !line.ends_with('\n') {
                return Err(ReceiptError::Unverified("the log ends in a partial line".into()));
            }
            consumed += line.len() as u64;
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let entry: LogEntry =
                serde_json::from_str(line).map_err(|err| ReceiptError::Unverified(err.to_string()))?;
            let hash_ok = match &entry {
                LogEntry::Decision(receipt) => verify_receipt(receipt),
                LogEntry::Outcome(outcome) => verify_outcome(outcome),
            }
            .map_err(|err| ReceiptError::Unverified(err.to_string()))?;
            if !hash_ok {
                return Err(ReceiptError::Unverified(format!("entry {} has the wrong hash", checked + 1)));
            }
            if entry.previous_receipt_hash() != previous.as_deref() {
                return Err(ReceiptError::Unverified(format!("entry {} does not link to the one before", checked + 1)));
            }
            let signed = match (entry.key_id(), entry.signature()) {
                (Some(key_id), Some(signature)) => self.keys.verify_entry(key_id, entry.receipt_id(), signature),
                _ => false,
            };
            if !signed {
                return Err(ReceiptError::Unverified(format!("entry {} is not signed by this replica", checked + 1)));
            }
            previous = Some(entry.receipt_id().to_string());
            checked += 1;
        }
        let memory_head = self.head.as_ref().map(|entry| entry.receipt_id().to_string());
        if checked != self.count || previous != memory_head {
            return Err(ReceiptError::Unverified("the log on disk does not end at the head in memory".into()));
        }
        self.verified_offset += consumed;
        self.verified_count = checked;
        self.verified_head = previous;
        self.shared.verifies.fetch_add(1, Ordering::SeqCst);

        let head = memory_head.ok_or_else(|| ReceiptError::Unverified("nothing to anchor".into()))?;
        let record = json!({
            "version": ANCHOR_VERSION,
            "count": self.count,
            "head_receipt_id": head,
            "key_id": self.signer.key_id(),
            "signature": self.signer.sign_anchor(self.count, &head),
        });
        let line = serde_json::to_string(&record).map_err(io_err)?;
        writeln!(self.anchor, "{line}")
            .and_then(|()| self.anchor.sync_data())
            .map_err(io_err)?;
        self.anchored_count = self.count;
        self.last_anchor = Instant::now();
        self.shared.anchors.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

// ---- kernel inputs ------------------------------------------------------------------------

/// `budget_exhausted` → `BUDGET_EXHAUSTED`. Gateway codes are lower-case
/// ASCII with underscores; anything else becomes `GATEWAY_REFUSED`.
fn reason_code(action: &str) -> String {
    let code = action.to_ascii_uppercase();
    let valid = code.len() <= 64
        && code.bytes().next().is_some_and(|b| b.is_ascii_uppercase())
        && code.bytes().all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_');
    if valid { code } else { FALLBACK_REASON.into() }
}

/// The kernel judges the gateway's `allow` and `await` under this policy.
/// Refusals are the gateway's own and go through `record_external_denial`.
fn gateway_policy() -> Policy {
    Policy {
        version: POLICY_VERSION.into(),
        policy_id: POLICY_ID.into(),
        denied_effects: vec![],
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

// ---- startup verification -----------------------------------------------------------------

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
    if !text.is_empty() && !text.ends_with('\n') {
        return Err(ReceiptError::Unverified("the log ends in a partial line".into()));
    }
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
        let count = record.get("count").and_then(Value::as_u64).unwrap_or(0);
        let head = record.get("head_receipt_id").and_then(Value::as_str).unwrap_or("");
        if version != Some(ANCHOR_VERSION) || count == 0 || head.is_empty() {
            return Err(ReceiptError::Unverified(format!("anchor line {number} is malformed")));
        }
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

fn read_from(file: &mut File, offset: u64) -> Result<String, ReceiptError> {
    file.seek(SeekFrom::Start(offset)).map_err(io_err)?;
    let mut text = String::new();
    file.read_to_string(&mut text).map_err(io_err)?;
    Ok(text)
}

// ---- keys and locks -----------------------------------------------------------------------

fn load_or_create_signer(path: &Path) -> Result<Signer, ReceiptError> {
    if path.exists() {
        ensure_private(path)?;
        let text = fs::read_to_string(path).map_err(io_err)?;
        return Signer::from_text(&text).map_err(io_err);
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

const LOCKED: &str = "receipt directory is locked by another process; each replica needs its own directory";

#[cfg(unix)]
fn lock_exclusive(file: &File) -> Result<(), ReceiptError> {
    use std::os::unix::io::AsRawFd;
    // SAFETY: `file` is an open descriptor we own, and `flock` only uses that fd.
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if rc == 0 { Ok(()) } else { Err(ReceiptError::Io(LOCKED.into())) }
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
    unsafe extern "system" {
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
            file.as_raw_handle(),
            LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY,
            0,
            u32::MAX,
            u32::MAX,
            &mut overlapped,
        )
    };
    if ok != 0 { Ok(()) } else { Err(ReceiptError::Io(LOCKED.into())) }
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

/// The interpreter that runs `ickverify.py`, for tests. `None` means skip,
/// except under CI, where a missing interpreter fails the test instead of
/// quietly skipping the verifier.
#[doc(hidden)]
pub fn test_python() -> Option<&'static str> {
    for candidate in ["python3", "python"] {
        let works = std::process::Command::new(candidate)
            .arg("--version")
            .output()
            .is_ok_and(|out| out.status.success());
        if works {
            return Some(candidate);
        }
    }
    assert!(std::env::var_os("CI").is_none(), "CI must have python to run ickverify.py");
    eprintln!("no python on PATH: skipping the ickverify.py check");
    None
}

#[cfg(test)]
mod tests {
    use super::*;

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

    fn options(anchor_every: u64) -> ChainOptions {
        ChainOptions {
            anchor_every,
            anchor_interval: Duration::from_secs(3600),
        }
    }

    fn subject(action: &str, salt: &Salt) -> Subject {
        Subject {
            request_id: format!("req-{action}"),
            tenant_id: "acme".into(),
            key_id: "ak_test".into(),
            model: "mock/chat".into(),
            action: action.into(),
            call_digest: Some(request_digest(salt, b"request-bytes")),
        }
    }

    fn open_receipt(chain: &Arc<ReceiptChain>, id: String, salt: &Salt) -> OpenReceipt {
        OpenReceipt {
            chain: Arc::clone(chain),
            decision_receipt_id: id,
            request_digest: request_digest(salt, b"request-bytes"),
            salt: salt.clone(),
        }
    }

    fn lines(path: &Path) -> Vec<String> {
        fs::read_to_string(path)
            .unwrap_or_default()
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(str::to_string)
            .collect()
    }

    /// `ickverify.py`'s exit code: 0 verified, 1 not verified, 2 unreadable.
    fn ickverify(log: &Path, chain: &ReceiptChain, anchor: bool) -> Option<i32> {
        let python = test_python()?;
        let script = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("vendor/wicket/verifier/ickverify.py");
        let mut cmd = std::process::Command::new(python);
        cmd.arg(&script)
            .arg(log)
            .arg("--trusted-keys")
            .arg(chain.public_key_path())
            .arg("--require-signatures");
        if anchor {
            cmd.arg("--anchor").arg(chain.anchor_path());
        }
        Some(cmd.output().expect("run ickverify.py").status.code().unwrap_or(-1))
    }

    async fn settle_anchor(chain: &ReceiptChain, anchors: u64) {
        for _ in 0..200 {
            if chain.anchors() >= anchors || chain.is_failed() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn four_mappings_verify_with_unchanged_ickverify() {
        let dir = scratch("map");
        let chain = Arc::new(ReceiptChain::open(&dir, options(100)).unwrap());
        let salt = Salt::generate().unwrap();
        let allow_id = chain.allow(&subject("reserve", &salt)).await.unwrap();
        let mut reply = ReplyDigest::new(salt.clone());
        reply.content("Hello, world");
        let open = open_receipt(&chain, allow_id, &salt);
        chain.outcome(&open, "completed", reply.finish()).await.unwrap();
        chain.deny(&subject("budget_exhausted", &salt)).await.unwrap();
        chain.await_approval(&subject("hold", &salt)).await.unwrap();

        // Readable while the chain is open: the lock is on its own file.
        let text = fs::read_to_string(chain.log_path()).unwrap();
        assert!(!text.contains("Hello, world"), "{text}");
        let rows: Vec<Value> = text.lines().map(|l| serde_json::from_str(l).unwrap()).collect();
        assert_eq!(rows[0]["verdict"], "allow");
        assert_eq!(rows[0]["version"], "infinity.receipt.v2");
        assert_eq!(rows[0]["call_digest"], request_digest(&salt, b"request-bytes"));
        assert_eq!(rows[1]["version"], "infinity.outcome.v1");
        assert_eq!(rows[1]["decision_receipt_id"], rows[0]["receipt_id"]);
        assert_eq!(rows[1]["response_sha256"], reply.finish());
        assert_eq!(rows[2]["verdict"], "deny");
        assert_eq!(rows[2]["reason_codes"], json!(["EXTERNAL_DENY", "BUDGET_EXHAUSTED"]));
        assert_eq!(rows[3]["verdict"], "await_human_approval");
        if let Some(code) = ickverify(chain.log_path(), &chain, false) {
            assert_eq!(code, 0, "ickverify rejected the chain");
        }
    }

    #[tokio::test]
    async fn startup_verifies_once_and_appends_against_the_memory_head() {
        let dir = scratch("head");
        let chain = ReceiptChain::open(&dir, options(100)).unwrap();
        let salt = Salt::generate().unwrap();
        assert_eq!(chain.verifies(), 1);
        chain.allow(&subject("reserve", &salt)).await.unwrap();
        chain.deny(&subject("rate_limited", &salt)).await.unwrap();
        assert_eq!(chain.verifies(), 1, "an append must not read the log back");

        let mut text = fs::read_to_string(chain.log_path()).unwrap();
        let newline = text.find('\n').unwrap();
        text.replace_range(newline - 1..newline, "0");
        fs::write(chain.log_path(), &text).unwrap();

        chain.await_approval(&subject("hold", &salt)).await.unwrap();
        assert_eq!(chain.verifies(), 1, "the damaged prefix was not read");
        assert_eq!(chain.len(), 3);

        drop(chain);
        match ReceiptChain::open(&dir, options(100)) {
            Err(ReceiptError::Unverified(_)) => {}
            Err(err) => panic!("expected a verification failure, got {err}"),
            Ok(_) => panic!("a damaged log must not open"),
        }
    }

    #[tokio::test]
    async fn anchoring_reads_only_new_entries_and_anchors_the_head() {
        let dir = scratch("anchor");
        let chain = Arc::new(ReceiptChain::open(&dir, options(2)).unwrap());
        let salt = Salt::generate().unwrap();
        chain.allow(&subject("reserve", &salt)).await.unwrap();
        assert!(lines(chain.anchor_path()).is_empty());
        chain.deny(&subject("forbidden", &salt)).await.unwrap();
        settle_anchor(&chain, 1).await;
        assert_eq!(chain.anchors(), 1);
        assert_eq!(chain.verifies(), 2, "an anchor reads back what was written");

        let anchors = lines(chain.anchor_path());
        let anchor: Value = serde_json::from_str(&anchors[0]).unwrap();
        assert_eq!(anchor["version"], ANCHOR_VERSION);
        assert_eq!(anchor["count"], 2);
        assert_eq!(anchor["head_receipt_id"], chain.head_id().unwrap());
        if let Some(code) = ickverify(chain.log_path(), &chain, true) {
            assert_eq!(code, 0, "ickverify rejected the anchored log");
        }

        // Backdating one entry keeps the JSON valid but breaks its hash: the
        // unchanged verifier must say "not verified" (1), not "unreadable" (2).
        let original = fs::read_to_string(chain.log_path()).unwrap();
        let at = original.find("\"issued_at\":\"").unwrap() + "\"issued_at\":\"".len();
        let mut damaged = original.clone();
        damaged.replace_range(at..at + 4, "1999");
        assert_ne!(damaged, original);
        let damaged_path = dir.join("damaged.jsonl");
        fs::write(&damaged_path, damaged).unwrap();
        if let Some(code) = ickverify(&damaged_path, &chain, true) {
            assert_eq!(code, 1, "a backdated entry must fail ickverify");
        }
    }

    #[tokio::test]
    async fn tampering_after_startup_is_caught_at_the_next_anchor() {
        let dir = scratch("runtime-tamper");
        let chain = ReceiptChain::open(&dir, options(3)).unwrap();
        let salt = Salt::generate().unwrap();
        chain.allow(&subject("reserve", &salt)).await.unwrap();
        let mut text = fs::read_to_string(chain.log_path()).unwrap();
        text = text.replacen("\"allow\"", "\"deny\" ", 1);
        fs::write(chain.log_path(), &text).unwrap();
        chain.allow(&subject("reserve", &salt)).await.unwrap();
        chain.allow(&subject("reserve", &salt)).await.unwrap();
        settle_anchor(&chain, 1).await;
        assert!(chain.is_failed(), "the anchor check must stop the chain");
        assert_eq!(chain.anchors(), 0, "no anchor for a log that does not verify");
        assert!(matches!(
            chain.allow(&subject("reserve", &salt)).await,
            Err(ReceiptError::Unavailable)
        ));
    }

    #[tokio::test]
    async fn a_quiet_chain_still_anchors_on_the_timer() {
        let dir = scratch("timer");
        let chain = ReceiptChain::open(
            &dir,
            ChainOptions {
                anchor_every: 1_000_000,
                anchor_interval: Duration::from_millis(50),
            },
        )
        .unwrap();
        let salt = Salt::generate().unwrap();
        chain.allow(&subject("reserve", &salt)).await.unwrap();
        settle_anchor(&chain, 1).await;
        assert_eq!(chain.anchors(), 1);
    }

    #[tokio::test]
    async fn shutdown_anchors_what_is_left() {
        let dir = scratch("shutdown");
        let chain = ReceiptChain::open(&dir, options(1_000_000)).unwrap();
        let salt = Salt::generate().unwrap();
        chain.allow(&subject("reserve", &salt)).await.unwrap();
        let head = chain.head_id().unwrap();
        let anchor_path = chain.anchor_path().to_path_buf();
        drop(chain);
        let anchors = lines(&anchor_path);
        let anchor: Value = serde_json::from_str(anchors.last().unwrap()).unwrap();
        assert_eq!(anchor["head_receipt_id"], head);
        let reopened = ReceiptChain::open(&dir, options(1_000_000)).unwrap();
        assert_eq!(reopened.len(), 1);
    }

    #[tokio::test]
    async fn a_second_process_on_the_same_directory_is_refused() {
        let dir = scratch("lock");
        let chain = ReceiptChain::open(&dir, options(100)).unwrap();
        match ReceiptChain::open(&dir, options(100)) {
            Err(ReceiptError::Io(message)) => assert!(message.contains("locked"), "{message}"),
            Err(err) => panic!("expected the lock to refuse, got {err}"),
            Ok(_) => panic!("two writers on one directory"),
        }
        drop(chain);
        ReceiptChain::open(&dir, options(100)).expect("the lock is released on drop");
    }

    #[tokio::test]
    async fn a_failed_write_answers_unavailable_and_writes_nothing() {
        let dir = scratch("fail");
        let chain = ReceiptChain::open(&dir, options(100)).unwrap();
        let salt = Salt::generate().unwrap();
        chain.fail_next_appends(1);
        assert!(matches!(
            chain.allow(&subject("reserve", &salt)).await,
            Err(ReceiptError::Unavailable)
        ));
        assert_eq!(chain.len(), 0);
        assert!(lines(chain.log_path()).is_empty());
        chain.allow(&subject("reserve", &salt)).await.unwrap();
        assert_eq!(chain.len(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_appends_share_flushes_and_stay_one_chain() {
        let dir = scratch("concurrent");
        let chain = Arc::new(ReceiptChain::open(&dir, options(1_000_000)).unwrap());
        let salt = Salt::generate().unwrap();
        let tasks: Vec<_> = (0..64)
            .map(|_| {
                let chain = Arc::clone(&chain);
                let salt = salt.clone();
                tokio::spawn(async move {
                    for _ in 0..10 {
                        chain.allow(&subject("reserve", &salt)).await.unwrap();
                    }
                })
            })
            .collect();
        for task in tasks {
            task.await.unwrap();
        }
        assert_eq!(chain.len(), 640);
        let text = fs::read_to_string(chain.log_path()).unwrap();
        let entries = read_entries(&text).unwrap();
        assert_eq!(entries.len(), 640);
        assert!(verify_log(&entries).unwrap(), "concurrent appends must still form one chain");
    }

    #[test]
    fn refusal_codes_become_kernel_codes() {
        assert_eq!(reason_code("budget_exhausted"), "BUDGET_EXHAUSTED");
        assert_eq!(reason_code("rate_limited"), "RATE_LIMITED");
        for odd in ["", "9lives", "has space", "dash-ed", &"x".repeat(65)] {
            assert_eq!(reason_code(odd), FALLBACK_REASON, "{odd:?}");
        }
    }

    #[test]
    fn digests_are_salted_and_independent_of_chunking() {
        let a = Salt::generate().unwrap();
        let b = Salt::generate().unwrap();
        assert_ne!(request_digest(&a, b"same prompt"), request_digest(&b, b"same prompt"));
        assert_eq!(request_digest(&a, b"same prompt"), request_digest(&a, b"same prompt"));
        assert_eq!(Salt::from_hex(&a.to_hex()), Some(a.clone()));

        let mut chunked = ReplyDigest::new(a.clone());
        for piece in ["Hel", "lo", ", world"] {
            chunked.content(piece);
        }
        chunked.tool(0, Some("call_1"), Some("lookup"), Some("{\"q\":"));
        chunked.tool(0, None, None, Some("1}"));
        let mut whole = ReplyDigest::new(a.clone());
        whole.content("Hello, world");
        whole.tool(0, Some("call_1"), Some("lookup"), Some("{\"q\":1}"));
        assert_eq!(chunked.finish(), whole.finish());

        let mut moved = ReplyDigest::new(a);
        moved.reasoning("Hello, world");
        moved.tool(0, Some("call_1"), Some("lookup"), Some("{\"q\":1}"));
        assert_ne!(moved.finish(), whole.finish(), "content and reasoning are hashed apart");
    }

    /// Run with `cargo test --release -- --ignored --nocapture receipt_throughput`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[ignore = "measurement, not a check"]
    async fn receipt_throughput() {
        for writers in [1usize, 16, 128] {
            let dir = scratch("throughput");
            let chain = Arc::new(ReceiptChain::open(&dir, ChainOptions::default()).unwrap());
            let salt = Salt::generate().unwrap();
            let per = 4000 / writers;
            let started = Instant::now();
            let tasks: Vec<_> = (0..writers)
                .map(|_| {
                    let chain = Arc::clone(&chain);
                    let salt = salt.clone();
                    tokio::spawn(async move {
                        for _ in 0..per {
                            chain.allow(&subject("reserve", &salt)).await.unwrap();
                        }
                    })
                })
                .collect();
            for task in tasks {
                task.await.unwrap();
            }
            let elapsed = started.elapsed();
            let total = (per * writers) as f64;
            println!(
                "{writers:>4} concurrent writers: {total} receipts in {elapsed:?} = {:.0}/s",
                total / elapsed.as_secs_f64()
            );
            drop(chain);
            let started = Instant::now();
            let reopened = ReceiptChain::open(&dir, ChainOptions::default()).unwrap();
            println!("     reopen and verify {} entries: {:?}", reopened.len(), started.elapsed());
        }
    }
}
