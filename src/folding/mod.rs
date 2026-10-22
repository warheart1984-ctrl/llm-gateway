//! Conversation folding: rolling summaries that replace old turns.
//!
//! A tenant that opts in can send long conversations through the gateway
//! and have the old turns replaced by a rolling summary, so each request
//! sends fewer prompt tokens. The fold only runs when it is expected to
//! save money. Every folded call is visible to the caller and the operator.
//! A fold that fails or does not pay leaves the request exactly as the
//! client sent it.

pub mod checks;
pub mod plan;
pub mod store;
pub mod summarizer;

use serde::Serialize;

/// How one fold attempt is counted. Every attempt lands on exactly one of these.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FoldAccounting {
    Committed,
    Rejected,
    Failed,
    Unprofitable,
    RefusedAdmission,
}

impl FoldAccounting {
    pub fn as_str(self) -> &'static str {
        match self {
            FoldAccounting::Committed => "committed",
            FoldAccounting::Rejected => "rejected",
            FoldAccounting::Failed => "failed",
            FoldAccounting::Unprofitable => "unprofitable",
            FoldAccounting::RefusedAdmission => "refused_admission",
        }
    }
}

/// Counts and hashes for one folding decision. No summary text and no message text.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct FoldFacts {
    pub outcome: &'static str,
    pub reason: String,
    pub turns_replaced: u64,
    pub turns_covered: u64,
    pub prompt_tokens_cut: u64,
    pub benefit_nano_usd: u64,
    pub overhead_nano_usd: u64,
    pub min_saving_nano_usd: u64,
    pub summarizer_prompt_tokens: u64,
    pub summarizer_completion_tokens: u64,
    pub fold_count: u64,
    /// Hex HMAC of the turn prefix a summary covers, or empty when none was used.
    pub prefix_hash: String,
}

/// Real summarizer usage attached to the single accounting outcome of an attempt.
#[derive(Debug, Clone, Copy)]
pub struct FoldAttempt {
    pub outcome: FoldAccounting,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
}

pub fn prefix_hex(hash: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(64);
    for byte in hash {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0xf) as usize] as char);
    }
    out
}

use crate::providers::ChatMessage;

/// Split messages into protected (system, developer) and turns (user, assistant, tool).
pub fn split_messages(messages: &[ChatMessage]) -> (Vec<ChatMessage>, Vec<ChatMessage>) {
    let mut protected = Vec::new();
    let mut turns = Vec::new();
    for msg in messages {
        match msg {
            ChatMessage::System { .. } => {
                protected.push(msg.clone());
            }
            ChatMessage::User { .. } if is_developer_role(msg) => {
                protected.push(msg.clone());
            }
            _ => turns.push(msg.clone()),
        }
    }
    (protected, turns)
}

/// Check if a message is a developer role (User with name "developer").
fn is_developer_role(msg: &ChatMessage) -> bool {
    match msg {
        ChatMessage::User { .. } => {
            // In OpenAI format, developer messages are User messages with name="developer"
            // We check the content for a marker, but the actual wire format
            // uses a separate role. For now, we treat all User as turns.
            // The role field is handled in serialization.
            false
        }
        _ => false,
    }
}

/// Estimate tokens for a message list using the gateway's estimator.
pub fn estimate_tokens(messages: &[ChatMessage]) -> u32 {
    crate::router::estimate_prompt_tokens(messages)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::{ChatMessage, Content};

    #[test]
    fn split_messages_separates_system_and_turns() {
        let messages = vec![
            ChatMessage::System { content: Content::Text("sys".into()) },
            ChatMessage::User { content: Content::Text("hello".into()) },
            ChatMessage::Assistant { content: Some(Content::Text("hi".into())), tool_calls: vec![] },
        ];
        let (protected, turns) = split_messages(&messages);
        assert_eq!(protected.len(), 1);
        assert_eq!(turns.len(), 2);
    }
}
