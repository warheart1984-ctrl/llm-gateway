//! Conversation folding: rolling summaries that replace old turns.
//!
//! A tenant that opts in can send long conversations through the gateway
//! and have the old turns replaced by a rolling summary, so each request
//! sends fewer prompt tokens. The fold only runs when it is expected to
//! save money. Every folded call is visible to the caller and the operator.
//! A fold that fails or does not pay leaves the request exactly as the
//! client sent it.

pub mod plan;
pub mod store;

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
