//! In-memory store for rolling summaries.
//!
//! The store is bounded by both entry count and total characters, behind a
//! lock that is never held across an `.await`. Keys include the tenant id
//! and a per-process secret for isolation.

use std::{
    sync::Arc,
    time::Instant,
};

use dashmap::{DashMap, DashSet};
use hmac::{Hmac, Mac};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use tokio::sync::watch;

use crate::providers::ChatMessage;

type HmacSha256 = Hmac<Sha256>;
pub type EntryRef = Arc<Mutex<SummaryEntry>>;

/// A stored summary entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SummaryEntry {
    /// Number of turns this summary covers (from the start of the conversation).
    pub turns_covered: usize,
    /// HMAC-SHA256 of the exact turn prefix this summary covers.
    pub prefix_hash: [u8; 32],
    /// The summary text.
    pub summary: String,
    /// How many times this conversation has been folded.
    pub fold_count: u32,
    /// Tokens spent on summarizer calls for this conversation.
    pub tokens_spent: u64,
    /// Tokens saved on main calls by using this summary.
    pub tokens_saved: u64,
    /// The summarizer model this summary was made with.
    pub summarizer_model: String,
    /// The facts the summary's "Current facts" block is written from.
    #[serde(default)]
    pub ledger: crate::folding::ledger::Ledger,
    /// When this entry was last accessed (for LRU).
    #[serde(skip, default = "instant_now")]
    pub last_accessed: Instant,
}

impl SummaryEntry {
    /// What the entry holds, for the store's `max_total_chars`.
    pub fn chars(&self) -> usize {
        self.summary.len() + self.ledger.chars()
    }
}

fn instant_now() -> Instant {
    Instant::now()
}

/// The conversation folding store.
///
/// Keys are HMAC-SHA256 over (tenant_id, protected_messages, first_turn).
/// The store is an LRU bounded by both `max_entries` and `max_total_chars`.
pub struct FoldingStore {
    /// Per-process secret for HMAC key derivation.
    secret: [u8; 32],
    /// Store entries, keyed by conversation key.
    entries: DashMap<[u8; 32], EntryRef>,
    /// Maximum number of entries.
    max_entries: usize,
    /// Maximum total characters across all summaries.
    max_total_chars: usize,
    /// Current total characters.
    total_chars: std::sync::atomic::AtomicUsize,
    /// Prefix mismatches that caused an entry to be dropped.
    prefix_mismatches: std::sync::atomic::AtomicU64,
    /// Conversation keys with a fold running. Kept beside the entries, never
    /// inside one, so a running fold cannot change the committed summary.
    in_flight: DashSet<[u8; 32]>,
    /// `true` once shutdown has asked every fold task to stop.
    shutdown: watch::Sender<bool>,
    /// Conversations whose last fold was rejected, with the head length that
    /// fold covered. Bounded by `max_entries`, like the summaries.
    backoffs: DashMap<[u8; 32], usize>,
}

impl FoldingStore {
    /// Create a new folding store with a random secret.
    pub fn new(max_entries: usize, max_total_chars: usize) -> Self {
        let mut secret = [0u8; 32];
        getrandom::getrandom(&mut secret).expect("getrandom failed");
        Self {
            secret,
            entries: DashMap::new(),
            max_entries: max_entries.max(1),
            max_total_chars: max_total_chars.max(1),
            total_chars: std::sync::atomic::AtomicUsize::new(0),
            prefix_mismatches: std::sync::atomic::AtomicU64::new(0),
            in_flight: DashSet::new(),
            shutdown: watch::channel(false).0,
            backoffs: DashMap::new(),
        }
    }

    /// Whether shutdown has asked folds to stop.
    pub fn shutting_down(&self) -> bool {
        *self.shutdown.borrow()
    }

    /// A fold covering `head_turns` turns was rejected: hold off this
    /// conversation. When the map is full an arbitrary mark is dropped, which
    /// only costs that conversation one more paid attempt.
    pub fn back_off(&self, key: [u8; 32], head_turns: usize) {
        if self.backoffs.len() >= self.max_entries && !self.backoffs.contains_key(&key) {
            let victim = self.backoffs.iter().next().map(|item| *item.key());
            if let Some(victim) = victim {
                self.backoffs.remove(&victim);
            }
        }
        self.backoffs.insert(key, head_turns);
    }

    /// The head length the last rejected fold of this conversation covered.
    pub fn backed_off_at(&self, key: &[u8; 32]) -> Option<usize> {
        self.backoffs.get(key).map(|item| *item.value())
    }

    /// A fold of this conversation committed: no more backing off.
    pub fn clear_backoff(&self, key: &[u8; 32]) {
        self.backoffs.remove(key);
    }

    /// Reserve the right to fold this conversation. False when a fold is
    /// already running or the process is shutting down.
    pub fn try_begin(&self, key: [u8; 32]) -> bool {
        if *self.shutdown.borrow() {
            return false;
        }
        self.in_flight.insert(key)
    }

    /// The fold for this key has finished, one way or another.
    pub fn finish(&self, key: &[u8; 32]) {
        self.in_flight.remove(key);
    }

    /// Ask every in-flight fold to stop. Each task releases its reservation
    /// itself; dropping it would abandon the prompt half.
    pub fn cancel(&self) {
        // `send` refuses to store the value when nobody is listening yet.
        // Shutdown still has to stick, so a fold that starts afterwards sees it.
        self.shutdown.send_modify(|flag| *flag = true);
    }

    pub fn subscribe_shutdown(&self) -> watch::Receiver<bool> {
        self.shutdown.subscribe()
    }

    /// Store `entry` only when the committed prefix is still the one this
    /// fold started from. `None` means the fold started with no summary.
    pub fn commit_if_current(
        &self,
        key: [u8; 32],
        started_prefix: Option<[u8; 32]>,
        entry: SummaryEntry,
    ) -> bool {
        let current = self.entries.get(&key).map(|existing| existing.lock().prefix_hash);
        if current != started_prefix {
            return false;
        }
        self.insert(key, entry);
        true
    }

    /// Compute the conversation key: HMAC-SHA256 over the tenant, the API key
    /// that sent the request, the protected messages and the first turn.
    /// Two sessions in one tenant that share a system prompt and opening turn
    /// but use different keys get different entries, instead of evicting
    /// each other's summary on every request.
    pub fn conversation_key(
        &self,
        tenant_id: &str,
        key_id: &str,
        protected: &[ChatMessage],
        first_turn: Option<&ChatMessage>,
    ) -> [u8; 32] {
        let mut mac = HmacSha256::new_from_slice(&self.secret).expect("HMAC key is valid");
        field(&mut mac, tenant_id.as_bytes());
        field(&mut mac, key_id.as_bytes());
        count(&mut mac, protected.len());
        for msg in protected {
            Self::message_identity(msg, &mut mac);
        }
        count(&mut mac, usize::from(first_turn.is_some()));
        if let Some(turn) = first_turn {
            Self::message_identity(turn, &mut mac);
        }
        mac.finalize().into_bytes().as_slice().try_into().unwrap()
    }

    /// Compute the prefix hash: HMAC-SHA256 over the exact turn list.
    pub fn prefix_hash(&self, tenant_id: &str, turns: &[ChatMessage]) -> [u8; 32] {
        let mut mac = HmacSha256::new_from_slice(&self.secret).expect("HMAC key is valid");
        field(&mut mac, tenant_id.as_bytes());
        count(&mut mac, turns.len());
        for turn in turns {
            Self::message_identity(turn, &mut mac);
        }
        mac.finalize().into_bytes().as_slice().try_into().unwrap()
    }

    /// Hash a message's identity (role, text, tool fields) into the MAC. Every
    /// field is length-prefixed and every list counted, so no text can imitate
    /// a field boundary: one message reading "a\0user\0b" and two messages
    /// "a", "b" hash differently.
    fn message_identity(msg: &ChatMessage, mac: &mut HmacSha256) {
        field(mac, msg.role().as_bytes());
        match msg.content() {
            Some(content) => {
                count(mac, 1);
                field(mac, content.text().as_bytes());
            }
            None => count(mac, 0),
        }
        match msg {
            ChatMessage::Tool { tool_call_id, name, .. } => {
                field(mac, tool_call_id.as_bytes());
                match name {
                    Some(name) => {
                        count(mac, 1);
                        field(mac, name.as_bytes());
                    }
                    None => count(mac, 0),
                }
            }
            ChatMessage::Assistant { tool_calls, .. } => {
                count(mac, tool_calls.len());
                for tc in tool_calls {
                    field(mac, tc.function.name.as_bytes());
                    field(mac, tc.function.arguments.as_bytes());
                }
            }
            _ => {}
        }
    }

    /// Look up an entry by conversation key, verifying the prefix hash matches.
    /// Returns the entry if the prefix matches, otherwise removes the entry and returns None.
    pub fn get(&self, key: &[u8; 32], prefix_hash: &[u8; 32]) -> Option<EntryRef> {
        let entry_ref = self.entries.get(key)?.clone();
        let matches = {
            let mut entry = entry_ref.lock();
            if entry.prefix_hash == *prefix_hash {
                entry.last_accessed = Instant::now();
                true
            } else {
                false
            }
        };
        if matches {
            Some(entry_ref)
        } else {
            // Prefix mismatch: drop the entry and let the caller record it.
            self.prefix_mismatches
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.remove_entry(key);
            None
        }
    }

    /// Look up an entry against the exact prefix it claims to cover. A
    /// mismatch removes the entry and increments `prefix_mismatches`.
    pub fn lookup(
        &self,
        key: &[u8; 32],
        tenant_id: &str,
        turns: &[ChatMessage],
    ) -> Option<EntryRef> {
        let turns_covered = self.entries.get(key)?.lock().turns_covered;
        let covered_end = turns_covered.min(turns.len());
        let prefix_hash = self.prefix_hash(tenant_id, &turns[..covered_end]);
        self.get(key, &prefix_hash)
    }

    pub fn remove(&self, key: &[u8; 32]) {
        self.remove_entry(key);
    }

    /// Remove an entry by key and update total chars.
    fn remove_entry(&self, key: &[u8; 32]) {
        if let Some((_, entry)) = self.entries.remove(key) {
            self.total_chars
                .fetch_sub(entry.lock().chars(), std::sync::atomic::Ordering::AcqRel);
        }
    }

    /// Insert or update an entry, evicting LRU entries if necessary.
    pub fn insert(&self, key: [u8; 32], entry: SummaryEntry) {
        let chars = entry.chars();
        let entry = Arc::new(Mutex::new(entry));

        // Replacing an existing key must not double-count its characters.
        self.remove_entry(&key);

        // Evict by entry count. Stop if nothing can be evicted: equal
        // timestamps used to make `evict_lru` return false and spin here.
        while self.entries.len() >= self.max_entries {
            if !self.evict_lru() {
                break;
            }
        }

        // Evict by total characters.
        while self.total_chars.load(std::sync::atomic::Ordering::Acquire) + chars > self.max_total_chars {
            if !self.evict_lru() {
                break; // Nothing left to evict
            }
        }

        if chars <= self.max_total_chars {
            self.total_chars.fetch_add(chars, std::sync::atomic::Ordering::AcqRel);
            self.entries.insert(key, entry);
        }
    }

    /// Evict the least recently used entry. Returns true if an entry was evicted.
    /// A tie still evicts one entry, so a full store cannot stall an insert.
    fn evict_lru(&self) -> bool {
        let mut oldest_key: Option<[u8; 32]> = None;
        let mut oldest_time = Instant::now();

        for entry_ref in self.entries.iter() {
            let accessed = entry_ref.lock().last_accessed;
            let older = match oldest_key {
                None => true,
                Some(_) => accessed < oldest_time,
            };
            if older {
                oldest_time = accessed;
                oldest_key = Some(*entry_ref.key());
            }
        }

        if let Some(key) = oldest_key {
            self.remove_entry(&key);
            true
        } else {
            false
        }
    }

    /// Get current store stats.
    pub fn stats(&self) -> (usize, usize) {
        (
            self.entries.len(),
            self.total_chars.load(std::sync::atomic::Ordering::Acquire),
        )
    }

    pub fn prefix_mismatches(&self) -> u64 {
        self.prefix_mismatches
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// Errors from store operations.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("store capacity exceeded")]
    CapacityExceeded,
}

/// One length-prefixed field: its byte length, then its bytes.
fn field(mac: &mut HmacSha256, bytes: &[u8]) {
    mac.update(&(bytes.len() as u64).to_le_bytes());
    mac.update(bytes);
}

/// A count (list length, or 0/1 for an absent/present option).
fn count(mac: &mut HmacSha256, n: usize) {
    mac.update(&(n as u64).to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::{ChatMessage, Content};

    fn msg(role: &str, text: &str) -> ChatMessage {
        match role {
            "system" => ChatMessage::System { content: Content::Text(text.into()) },
            "user" => ChatMessage::User { content: Content::Text(text.into()) },
            "assistant" => ChatMessage::Assistant { content: Some(Content::Text(text.into())), tool_calls: vec![] },
            _ => panic!("unknown role"),
        }
    }

    #[test]
    fn same_conversation_gets_same_key() {
        let store = FoldingStore::new(100, 10000);
        let protected = vec![msg("system", "sys")];
        let first_turn = msg("user", "hello");

        let key1 = store.conversation_key("tenant1", "ak", &protected, Some(&first_turn));
        let key2 = store.conversation_key("tenant1", "ak", &protected, Some(&first_turn));
        let key3 = store.conversation_key("tenant1", "ak",
            &protected,
            Some(&first_turn),
        );
        assert_eq!(key1, key2);
        assert_eq!(key1, key3);
    }

    #[test]
    fn different_first_turn_gets_different_key() {
        let store = FoldingStore::new(100, 10000);
        let protected = vec![msg("system", "sys")];

        let key1 = store.conversation_key("tenant1", "ak", &protected, Some(&msg("user", "hello")));
        let key2 = store.conversation_key("tenant1", "ak", &protected, Some(&msg("user", "goodbye")));
        assert_ne!(key1, key2);
    }

    #[test]
    fn different_tenant_gets_different_key() {
        let store = FoldingStore::new(100, 10000);
        let protected = vec![msg("system", "sys")];
        let first_turn = msg("user", "hello");

        let key1 = store.conversation_key("tenant1", "ak", &protected, Some(&first_turn));
        let key2 = store.conversation_key("tenant2", "ak", &protected, Some(&first_turn));
        assert_ne!(key1, key2);
    }

    #[test]
    fn different_api_keys_in_one_tenant_get_different_keys() {
        // Two sessions sharing a system prompt and opening turn used to share
        // one entry and evict each other's summary on every request.
        let store = FoldingStore::new(100, 10000);
        let protected = vec![msg("system", "sys")];
        let first_turn = msg("user", "hello");
        assert_ne!(
            store.conversation_key("tenant1", "ak_one", &protected, Some(&first_turn)),
            store.conversation_key("tenant1", "ak_two", &protected, Some(&first_turn)),
        );
    }

    #[test]
    fn text_cannot_imitate_a_message_boundary() {
        // With NUL-joined fields, one message "a\0user\0b" hashed like the two
        // messages "a", "b". Length prefixes make them differ.
        let store = FoldingStore::new(100, 10000);
        let one = [msg("user", "a\0user\0b")];
        let two = [msg("user", "a"), msg("user", "b")];
        assert_ne!(store.prefix_hash("tenant1", &one), store.prefix_hash("tenant1", &two));
        let protected_one = [msg("system", "s\0user\0x")];
        let protected_two = [msg("system", "s")];
        assert_ne!(
            store.conversation_key("tenant1", "ak", &protected_one, None),
            store.conversation_key("tenant1", "ak", &protected_two, Some(&msg("user", "x"))),
        );
        // Same message and list counts; only the field lengths tell these
        // apart.
        let call = |name: &str, arguments: &str| ChatMessage::Assistant {
            content: None,
            tool_calls: vec![crate::providers::ToolCall {
                id: "call_1".into(),
                kind: "function".into(),
                function: crate::providers::FunctionCall { name: name.into(), arguments: arguments.into() },
            }],
        };
        assert_ne!(
            store.prefix_hash("tenant1", &[call("f\0x", "y")]),
            store.prefix_hash("tenant1", &[call("f", "x\0y")]),
        );
        assert_ne!(
            store.conversation_key("t\0a", "b", &[], None),
            store.conversation_key("t", "a\0b", &[], None),
        );
    }

    #[test]
    fn prefix_hash_matches_on_same_turns() {
        let store = FoldingStore::new(100, 10000);
        let turns = vec![msg("user", "hello"), msg("assistant", "hi")];

        let hash1 = store.prefix_hash("tenant1", &turns);
        let hash2 = store.prefix_hash("tenant1", &turns);
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn prefix_hash_differs_on_different_turns() {
        let store = FoldingStore::new(100, 10000);
        let turns1 = vec![msg("user", "hello"), msg("assistant", "hi")];
        let turns2 = vec![msg("user", "hello"), msg("assistant", "hey")];

        let hash1 = store.prefix_hash("tenant1", &turns1);
        let hash2 = store.prefix_hash("tenant1", &turns2);
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn lookup_with_mismatched_prefix_removes_entry() {
        let store = FoldingStore::new(100, 10000);
        let protected = vec![msg("system", "sys")];
        let first_turn = msg("user", "hello");
        let key = store.conversation_key("tenant1", "ak", &protected, Some(&first_turn));

        let turns = vec![msg("user", "hello"), msg("assistant", "hi")];
        let prefix_hash = store.prefix_hash("tenant1", &turns);

        let entry = SummaryEntry {
            turns_covered: 2,
            prefix_hash,
            summary: "test summary".into(),
            fold_count: 1,
            tokens_spent: 100,
            tokens_saved: 500,
            ledger: Default::default(),
            summarizer_model: "model".into(),
            last_accessed: Instant::now(),
        };
        store.insert(key, entry);

        // Modify the first turn - prefix hash should not match
        let wrong_turns = vec![msg("user", "different"), msg("assistant", "hi")];
        let wrong_prefix = store.prefix_hash("tenant1", &wrong_turns);

        // Lookup with wrong prefix should remove entry
        let result = store.get(&key, &wrong_prefix);
        assert!(result.is_none());
        assert_eq!(store.prefix_mismatches(), 1);

        // Entry should be gone even with correct prefix
        let result = store.get(&key, &prefix_hash);
        assert!(result.is_none());
    }

    #[test]
    fn lookup_drops_an_edited_early_turn_and_counts_the_mismatch() {
        let store = FoldingStore::new(100, 10000);
        let protected = vec![msg("system", "sys")];
        let first = msg("user", "hello");
        let key = store.conversation_key("tenant1", "ak", &protected, Some(&first));
        let turns = vec![first, msg("assistant", "hi"), msg("user", "later")];
        let prefix_hash = store.prefix_hash("tenant1", &turns[..2]);
        store.insert(
            key,
            SummaryEntry {
                turns_covered: 2,
                prefix_hash,
                summary: "kept".into(),
                fold_count: 1,
                tokens_spent: 0,
                tokens_saved: 0,
                ledger: Default::default(),
                summarizer_model: "model".into(),
                last_accessed: Instant::now(),
            },
        );

        let mut edited = turns.clone();
        edited[0] = msg("user", "hello edited");
        assert!(store.lookup(&key, "tenant1", &edited).is_none());
        assert_eq!(store.prefix_mismatches(), 1);
        assert!(store.lookup(&key, "tenant1", &turns).is_none());
    }

    #[test]
    fn the_same_conversation_keeps_its_key_when_later_turns_are_added() {
        let store = FoldingStore::new(100, 10000);
        let protected = vec![msg("system", "sys")];
        let first = msg("user", "hello");
        let short = store.conversation_key("tenant1", "ak", &protected, Some(&first));
        let long = store.conversation_key("tenant1", "ak", &protected, Some(&first));
        assert_eq!(short, long);
        let longer_prefix = store.prefix_hash(
            "tenant1",
            &[first.clone(), msg("assistant", "one"), msg("user", "two")],
        );
        let shorter_prefix = store.prefix_hash("tenant1", &[first, msg("assistant", "one")]);
        assert_ne!(longer_prefix, shorter_prefix);
        assert_eq!(
            store.conversation_key("tenant1", "ak", &protected, Some(&msg("user", "hello"))),
            short
        );
    }

    #[test]
    fn lru_evicts_by_entry_count() {
        let store = FoldingStore::new(2, 10000);
        let protected = vec![msg("system", "sys")];
        let mut keys = Vec::new();

        for i in 0..3 {
            let first_turn = msg("user", &format!("turn{i}"));
            let key = store.conversation_key("tenant1", "ak", &protected, Some(&first_turn));
            let turns = vec![first_turn.clone(), msg("assistant", "resp")];
            let prefix_hash = store.prefix_hash("tenant1", &turns);

            let entry = SummaryEntry {
                turns_covered: 2,
                prefix_hash,
                summary: "x".repeat(100),
                fold_count: 1,
                tokens_spent: 100,
                tokens_saved: 500,
                ledger: Default::default(),
                summarizer_model: "model".into(),
                last_accessed: Instant::now() - std::time::Duration::from_secs(30 - i as u64),
            };
            store.insert(key, entry);
            keys.push(key);
        }

        assert_eq!(store.entries.len(), 2);
        assert!(store.entries.get(&keys[0]).is_none(), "the least recently used entry is the one evicted");
        assert!(store.entries.get(&keys[2]).is_some(), "the newest entry stays");
    }

    #[test]
    fn lru_evicts_by_total_chars() {
        let store = FoldingStore::new(100, 500);
        let protected = vec![msg("system", "sys")];

        for i in 0..3 {
            let first_turn = msg("user", &format!("turn{i}"));
            let key = store.conversation_key("tenant1", "ak", &protected, Some(&first_turn));
            let turns = vec![first_turn.clone(), msg("assistant", "resp")];
            let prefix_hash = store.prefix_hash("tenant1", &turns);

            let entry = SummaryEntry {
                turns_covered: 2,
                prefix_hash,
                summary: "x".repeat(200),
                fold_count: 1,
                tokens_spent: 100,
                tokens_saved: 500,
                ledger: Default::default(),
                summarizer_model: "model".into(),
                last_accessed: Instant::now(),
            };
            store.insert(key, entry);
        }

        // Should only fit 2 entries of 200 chars each (500 total)
        let total = store.total_chars.load(std::sync::atomic::Ordering::Acquire);
        assert!(total <= 500);
    }

    fn stored(prefix: [u8; 32], summary: &str) -> SummaryEntry {
        SummaryEntry {
            turns_covered: 1,
            prefix_hash: prefix,
            summary: summary.into(),
            fold_count: 1,
            tokens_spent: 0,
            tokens_saved: 0,
            ledger: Default::default(),
            summarizer_model: "model".into(),
            last_accessed: Instant::now(),
        }
    }

    #[test]
    fn the_ledger_counts_towards_the_stores_size() {
        let mut entry = stored([1u8; 32], "short");
        entry.ledger.context = "x".repeat(200);
        assert_eq!(entry.chars(), 205);
        // Too big for the store with its ledger, though not by its summary.
        let store = FoldingStore::new(10, 150);
        store.insert([5u8; 32], entry);
        assert!(store.entries.get(&[5u8; 32]).is_none());
        assert_eq!(store.total_chars.load(std::sync::atomic::Ordering::Acquire), 0);
    }

    #[test]
    fn one_fold_runs_per_conversation_and_shutdown_blocks_another() {
        let store = FoldingStore::new(10, 1000);
        let key = [7u8; 32];
        assert!(store.try_begin(key));
        assert!(!store.try_begin(key), "a second fold must not start");
        store.finish(&key);
        assert!(store.try_begin(key));
        store.cancel();
        assert!(!store.try_begin([8u8; 32]), "shutdown starts no new fold");
    }

    #[test]
    fn commit_keeps_the_summary_the_fold_started_from() {
        let store = FoldingStore::new(10, 1000);
        let key = [4u8; 32];
        let first = [1u8; 32];
        let second = [2u8; 32];
        assert!(store.commit_if_current(key, None, stored(first, "one")));
        assert!(!store.commit_if_current(key, None, stored(second, "displaced")));
        assert_eq!(store.entries.get(&key).unwrap().lock().prefix_hash, first);
        assert!(store.commit_if_current(key, Some(first), stored(second, "next")));
        assert_eq!(store.entries.get(&key).unwrap().lock().summary, "next");
        assert!(!store.commit_if_current(key, Some(first), stored([3u8; 32], "stale")));
        assert_eq!(store.entries.get(&key).unwrap().lock().summary, "next");
    }
}
