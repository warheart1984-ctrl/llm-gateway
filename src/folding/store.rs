//! In-memory store for rolling summaries.
//!
//! The store is bounded by both entry count and total characters, behind a
//! lock that is never held across an `.await`. Keys include the tenant id
//! and a per-process secret for isolation.

use std::{
    sync::Arc,
    time::Instant,
};

use dashmap::DashMap;
use hmac::{Hmac, Mac};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use sha2::Sha256;

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
    /// When this entry was last accessed (for LRU).
    #[serde(skip, default = "instant_now")]
    pub last_accessed: Instant,
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
        }
    }

    /// Compute the conversation key: HMAC-SHA256 over (tenant_id, protected_messages, first_turn).
    pub fn conversation_key(
        &self,
        tenant_id: &str,
        protected: &[ChatMessage],
        first_turn: Option<&ChatMessage>,
    ) -> [u8; 32] {
        let mut mac = HmacSha256::new_from_slice(&self.secret).expect("HMAC key is valid");
        mac.update(tenant_id.as_bytes());
        mac.update(b"\0");
        for msg in protected {
            Self::message_identity(msg, &mut mac);
        }
        mac.update(b"\0");
        if let Some(turn) = first_turn {
            Self::message_identity(turn, &mut mac);
        }
        mac.finalize().into_bytes().as_slice().try_into().unwrap()
    }

    /// Compute the prefix hash: HMAC-SHA256 over the exact turn list.
    pub fn prefix_hash(&self, tenant_id: &str, turns: &[ChatMessage]) -> [u8; 32] {
        let mut mac = HmacSha256::new_from_slice(&self.secret).expect("HMAC key is valid");
        mac.update(tenant_id.as_bytes());
        mac.update(b"\0");
        for turn in turns {
            Self::message_identity(turn, &mut mac);
        }
        mac.finalize().into_bytes().as_slice().try_into().unwrap()
    }

    /// Hash a message's identity (role, name, tool fields, text) into the MAC.
    fn message_identity(msg: &ChatMessage, mac: &mut HmacSha256) {
        mac.update(msg.role().as_bytes());
        mac.update(b"\0");
        if let Some(content) = msg.content() {
            mac.update(content.text().as_bytes());
        }
        mac.update(b"\0");
        match msg {
            ChatMessage::Tool { tool_call_id, name, .. } => {
                mac.update(tool_call_id.as_bytes());
                mac.update(b"\0");
                if let Some(n) = name {
                    mac.update(n.as_bytes());
                }
                mac.update(b"\0");
            }
            ChatMessage::Assistant { tool_calls, .. } => {
                for tc in tool_calls {
                    mac.update(tc.function.name.as_bytes());
                    mac.update(b"\0");
                    mac.update(tc.function.arguments.as_bytes());
                    mac.update(b"\0");
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

    /// Remove an entry by key and update total chars.
    fn remove_entry(&self, key: &[u8; 32]) {
        if let Some((_, entry)) = self.entries.remove(key) {
            self.total_chars
                .fetch_sub(entry.lock().summary.len(), std::sync::atomic::Ordering::AcqRel);
        }
    }

    /// Insert or update an entry, evicting LRU entries if necessary.
    pub fn insert(&self, key: [u8; 32], entry: SummaryEntry) {
        let chars = entry.summary.len();
        let entry = Arc::new(Mutex::new(entry));

        // Replacing an existing key must not double-count its characters.
        self.remove_entry(&key);

        // Evict by entry count.
        while self.entries.len() >= self.max_entries {
            self.evict_lru();
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
    fn evict_lru(&self) -> bool {
        let mut oldest_key: Option<[u8; 32]> = None;
        let mut oldest_time = Instant::now();

        for entry_ref in self.entries.iter() {
            let entry = entry_ref.lock();
            if entry.last_accessed < oldest_time {
                oldest_time = entry.last_accessed;
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

        let key1 = store.conversation_key("tenant1", &protected, Some(&first_turn));
        let key2 = store.conversation_key("tenant1", &protected, Some(&first_turn));
        let key3 = store.conversation_key(
            "tenant1",
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

        let key1 = store.conversation_key("tenant1", &protected, Some(&msg("user", "hello")));
        let key2 = store.conversation_key("tenant1", &protected, Some(&msg("user", "goodbye")));
        assert_ne!(key1, key2);
    }

    #[test]
    fn different_tenant_gets_different_key() {
        let store = FoldingStore::new(100, 10000);
        let protected = vec![msg("system", "sys")];
        let first_turn = msg("user", "hello");

        let key1 = store.conversation_key("tenant1", &protected, Some(&first_turn));
        let key2 = store.conversation_key("tenant2", &protected, Some(&first_turn));
        assert_ne!(key1, key2);
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
        let key = store.conversation_key("tenant1", &protected, Some(&first_turn));

        let turns = vec![msg("user", "hello"), msg("assistant", "hi")];
        let prefix_hash = store.prefix_hash("tenant1", &turns);

        let entry = SummaryEntry {
            turns_covered: 2,
            prefix_hash,
            summary: "test summary".into(),
            fold_count: 1,
            tokens_spent: 100,
            tokens_saved: 500,
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
    fn lru_evicts_by_entry_count() {
        let store = FoldingStore::new(2, 10000);
        let protected = vec![msg("system", "sys")];

        for i in 0..3 {
            let first_turn = msg("user", &format!("turn{i}"));
            let key = store.conversation_key("tenant1", &protected, Some(&first_turn));
            let turns = vec![first_turn.clone(), msg("assistant", "resp")];
            let prefix_hash = store.prefix_hash("tenant1", &turns);

            let entry = SummaryEntry {
                turns_covered: 2,
                prefix_hash,
                summary: "x".repeat(100),
                fold_count: 1,
                tokens_spent: 100,
                tokens_saved: 500,
                summarizer_model: "model".into(),
                last_accessed: Instant::now(),
            };
            store.insert(key, entry);
        }

        // Should only have 2 entries (max_entries)
        assert_eq!(store.entries.len(), 2);
    }

    #[test]
    fn lru_evicts_by_total_chars() {
        let store = FoldingStore::new(100, 500);
        let protected = vec![msg("system", "sys")];

        for i in 0..3 {
            let first_turn = msg("user", &format!("turn{i}"));
            let key = store.conversation_key("tenant1", &protected, Some(&first_turn));
            let turns = vec![first_turn.clone(), msg("assistant", "resp")];
            let prefix_hash = store.prefix_hash("tenant1", &turns);

            let entry = SummaryEntry {
                turns_covered: 2,
                prefix_hash,
                summary: "x".repeat(200),
                fold_count: 1,
                tokens_spent: 100,
                tokens_saved: 500,
                summarizer_model: "model".into(),
                last_accessed: Instant::now(),
            };
            store.insert(key, entry);
        }

        // Should only fit 2 entries of 200 chars each (500 total)
        let total = store.total_chars.load(std::sync::atomic::Ordering::Acquire);
        assert!(total <= 500);
    }
}
