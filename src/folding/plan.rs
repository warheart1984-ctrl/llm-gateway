//! Pure planning functions for conversation folding.
//!
//! No I/O, ported from 5-fold-engine's `plan_compression`, `CompressionPlan`,
//! and `refusal`.

use crate::providers::{ChatMessage, Content};
use crate::router::estimate_prompt_tokens;

/// The outcome of a fold plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FoldOutcome {
    /// Fold was applied.
    Applied,
    /// No fold needed (not enough new turns/tokens).
    NotDue,
    /// Fold skipped because head contains tools/non-text.
    Skipped,
    /// Fold refused by cost check.
    Refused,
    /// Fold failed (summarizer error, checks failed).
    Failed,
}

/// Reason for the fold outcome.
#[derive(Debug, Clone)]
pub enum FoldReason {
    NoHead,
    HeadHasTools,
    HeadHasNonText,
    HeadHasToolMessage,
    NotEnoughNewTurns { pending_turns: usize, pending_tokens: u32, min_turns: u32, min_tokens: u32 },
    BatchReady,
    UrgentValve { pending_tokens: u32 },
    CostCheckFailed { benefit: u64, overhead: u64, min_saving: u64 },
    MaxFoldsReached { fold_count: u32, max_folds: u32 },
    RefoldTriggered { pending_turns: usize, refold_threshold: u32 },
    SummarizerAdmissionFailed,
    SummarizerEmptyReply,
    SummarizerLengthExceeded { budget: u32 },
    SummarizerChecksFailed(Vec<String>),
    SummarizerError(String),
}

/// Complete fold plan with all decision facts.
#[derive(Debug, Clone)]
pub struct FoldPlan {
    pub outcome: FoldOutcome,
    pub reason: FoldReason,
    /// Number of turns that would be replaced by the summary.
    pub turns_replaced: usize,
    /// Total turns covered by the (new or existing) summary.
    pub turns_covered: usize,
    /// Estimated prompt tokens cut from the main call.
    pub prompt_tokens_cut: u32,
    /// Cost check numbers.
    pub benefit_nano_usd: u64,
    pub overhead_nano_usd: u64,
    pub min_saving_nano_usd: u64,
    /// The messages to forward (protected + summary + pending + tail).
    pub forwarded_messages: Vec<ChatMessage>,
    /// The summary text (if applied).
    pub summary: Option<String>,
    /// The new prefix hash (if applied).
    pub new_prefix_hash: Option<[u8; 32]>,
    /// New fold count (if applied).
    pub new_fold_count: u32,
}

/// Configuration for planning a fold.
#[derive(Debug, Clone, Default)]
pub struct PlanConfig {
    pub keep_last: usize,
    pub summary_chars: usize,
    pub summarizer_max_tokens: u32,
    pub max_folds: u32,
    pub refold_threshold: u32,
    pub min_new_turns: u32,
    pub min_new_tokens: u32,
    pub amortization_requests: u32,
    pub min_saving_nano_usd: u64,
}

/// A summary entry from the store (what we need for planning).
#[derive(Debug, Clone)]
pub struct StoredSummary {
    pub turns_covered: usize,
    pub prefix_hash: [u8; 32],
    pub summary: String,
    pub fold_count: u32,
    pub summarizer_model: String,
}

/// Plan a fold given the current conversation and optional stored summary.
pub fn plan_fold(
    config: &PlanConfig,
    main_model_cost: &crate::router::CostModel,
    summarizer_cost: &crate::router::CostModel,
    protected: &[ChatMessage],
    turns: &[ChatMessage],
    stored_summary: Option<&StoredSummary>,
) -> FoldPlan {
    // 1. Split into head and tail.
    let (head, tail) = split_head_tail(turns, config.keep_last);

    // If no head, nothing to fold.
    if head.is_empty() {
        return FoldPlan {
            outcome: FoldOutcome::NotDue,
            reason: FoldReason::NoHead,
            turns_replaced: 0,
            turns_covered: stored_summary.map(|s| s.turns_covered).unwrap_or(0),
            prompt_tokens_cut: 0,
            benefit_nano_usd: 0,
            overhead_nano_usd: 0,
            min_saving_nano_usd: config.min_saving_nano_usd,
            forwarded_messages: build_forwarded_messages(protected, None, &[], &tail),
            summary: None,
            new_prefix_hash: None,
            new_fold_count: stored_summary.map(|s| s.fold_count).unwrap_or(0),
        };
    }

    // 2. Skip if head contains tools, non-text, or tool messages.
    if let Some(reason) = check_head_skippable(&head) {
        return FoldPlan {
            outcome: FoldOutcome::Skipped,
            reason,
            turns_replaced: 0,
            turns_covered: stored_summary.map(|s| s.turns_covered).unwrap_or(0),
            prompt_tokens_cut: 0,
            benefit_nano_usd: 0,
            overhead_nano_usd: 0,
            min_saving_nano_usd: config.min_saving_nano_usd,
            forwarded_messages: build_forwarded_messages(protected, stored_summary.map(|s| s.summary.as_str()), &head, &tail),
            summary: stored_summary.map(|s| s.summary.clone()),
            new_prefix_hash: stored_summary.map(|s| s.prefix_hash),
            new_fold_count: stored_summary.map(|s| s.fold_count).unwrap_or(0),
        };
    }

    // 3. Determine covered and pending turns.
    let (covered_turns, pending_turns) = if let Some(stored) = stored_summary {
        let head_len = head.len();
        let covered_end = stored.turns_covered.min(head_len);
        let covered = turns[..covered_end].to_vec();
        let pending = turns[covered_end..head_len].to_vec();
        (covered, pending)
    } else {
        (vec![], head.clone())
    };

    // 4. Batching: fold only when pending reaches thresholds or urgent valve.
    let pending_turns_count = pending_turns.len();
    let pending_tokens = estimate_prompt_tokens(&pending_turns);

    let current_fold_count = stored_summary.map(|s| s.fold_count).unwrap_or(0);
    let refold_due = current_fold_count >= config.max_folds
        && pending_turns_count as u32 >= config.refold_threshold;

    // Once the cap is reached, pending turns remain verbatim until the
    // explicit re-fold threshold is met. The urgent valve must not bypass the
    // cap, otherwise a large request could silently create an extra fold.
    if current_fold_count >= config.max_folds && !refold_due {
        return FoldPlan {
            outcome: FoldOutcome::NotDue,
            reason: FoldReason::MaxFoldsReached {
                fold_count: current_fold_count,
                max_folds: config.max_folds,
            },
            turns_replaced: 0,
            turns_covered: covered_turns.len(),
            prompt_tokens_cut: 0,
            benefit_nano_usd: 0,
            overhead_nano_usd: 0,
            min_saving_nano_usd: config.min_saving_nano_usd,
            forwarded_messages: build_forwarded_messages(
                protected,
                stored_summary.map(|s| s.summary.as_str()),
                &pending_turns,
                &tail,
            ),
            summary: stored_summary.map(|s| s.summary.clone()),
            new_prefix_hash: stored_summary.map(|s| s.prefix_hash),
            new_fold_count: current_fold_count,
        };
    }

    let should_fold = refold_due
        || pending_tokens >= 4000
        || (pending_turns_count as u32 >= config.min_new_turns
            && pending_tokens >= config.min_new_tokens);

    if !should_fold {
        let reason = if pending_tokens >= 4000 {
            FoldReason::UrgentValve { pending_tokens }
        } else {
            FoldReason::NotEnoughNewTurns {
                pending_turns: pending_turns_count,
                pending_tokens,
                min_turns: config.min_new_turns,
                min_tokens: config.min_new_tokens,
            }
        };
        return FoldPlan {
            outcome: FoldOutcome::NotDue,
            reason,
            turns_replaced: 0,
            turns_covered: covered_turns.len(),
            prompt_tokens_cut: 0,
            benefit_nano_usd: 0,
            overhead_nano_usd: 0,
            min_saving_nano_usd: config.min_saving_nano_usd,
            forwarded_messages: build_forwarded_messages(
                protected,
                stored_summary.map(|s| s.summary.as_str()),
                &pending_turns,
                &tail,
            ),
            summary: stored_summary.map(|s| s.summary.clone()),
            new_prefix_hash: stored_summary.map(|s| s.prefix_hash),
            new_fold_count: stored_summary.map(|s| s.fold_count).unwrap_or(0),
        };
    }

    // 5. Cost check.
    let (benefit, overhead, prompt_tokens_cut) = cost_check(
        config,
        main_model_cost,
        summarizer_cost,
        protected,
        &head,
        &pending_turns,
        &tail,
        stored_summary,
    );

    if benefit.saturating_sub(overhead) < config.min_saving_nano_usd {
        return FoldPlan {
            outcome: FoldOutcome::Refused,
            reason: FoldReason::CostCheckFailed {
                benefit,
                overhead,
                min_saving: config.min_saving_nano_usd,
            },
            turns_replaced: 0,
            turns_covered: covered_turns.len(),
            prompt_tokens_cut,
            benefit_nano_usd: benefit,
            overhead_nano_usd: overhead,
            min_saving_nano_usd: config.min_saving_nano_usd,
            forwarded_messages: build_forwarded_messages(
                protected,
                stored_summary.map(|s| s.summary.as_str()),
                &pending_turns,
                &tail,
            ),
            summary: stored_summary.map(|s| s.summary.clone()),
            new_prefix_hash: stored_summary.map(|s| s.prefix_hash),
            new_fold_count: current_fold_count,
        };
    }

    // 7. Build the forwarded request with a placeholder summary for token estimation.
    let placeholder_summary = "x".repeat(config.summary_chars);
    let forwarded_with_placeholder =
        build_forwarded_messages(protected, Some(&placeholder_summary), &[], &tail);
    let forwarded_without = build_forwarded_messages(protected, None, &head, &tail);
    let folded_prompt_tokens = estimate_prompt_tokens(&forwarded_with_placeholder);
    let unfolded_prompt_tokens = estimate_prompt_tokens(&forwarded_without);
    let prompt_tokens_cut = unfolded_prompt_tokens.saturating_sub(folded_prompt_tokens);

    // The actual summary will be generated by the summarizer.
    // For now, we return a plan indicating the fold should proceed.
    let is_refold = current_fold_count >= config.max_folds;
    let reason = if pending_tokens >= 4000 {
        FoldReason::UrgentValve { pending_tokens }
    } else if is_refold {
        FoldReason::RefoldTriggered {
            pending_turns: pending_turns_count,
            refold_threshold: config.refold_threshold,
        }
    } else {
        FoldReason::BatchReady
    };

    let new_fold_count = if is_refold { 1 } else { current_fold_count + 1 };

    FoldPlan {
        outcome: FoldOutcome::Applied,
        reason,
        turns_replaced: pending_turns_count,
        turns_covered: covered_turns.len() + pending_turns_count,
        prompt_tokens_cut,
        benefit_nano_usd: benefit,
        overhead_nano_usd: overhead,
        min_saving_nano_usd: config.min_saving_nano_usd,
        forwarded_messages: forwarded_with_placeholder,
        summary: None,
        new_prefix_hash: None,
        new_fold_count,
    }
}

/// Split turns into head (to potentially fold) and tail (always verbatim).
fn split_head_tail(turns: &[ChatMessage], keep_last: usize) -> (Vec<ChatMessage>, Vec<ChatMessage>) {
    if turns.len() <= keep_last {
        return (Vec::new(), turns.to_vec());
    }
    let split = turns.len() - keep_last;
    (turns[..split].to_vec(), turns[split..].to_vec())
}

/// Check if the head contains any skippable messages.
fn check_head_skippable(head: &[ChatMessage]) -> Option<FoldReason> {
    for msg in head {
        match msg {
            ChatMessage::Assistant { tool_calls, content, .. } => {
                if !tool_calls.is_empty() {
                    return Some(FoldReason::HeadHasTools);
                }
                if !matches!(content, Some(Content::Text(text)) if !text.is_empty()) {
                    return Some(FoldReason::HeadHasNonText);
                }
            }
            ChatMessage::Tool { .. } => {
                return Some(FoldReason::HeadHasToolMessage);
            }
            ChatMessage::User { content }
                if !matches!(content, Content::Text(text) if !text.is_empty()) =>
            {
                return Some(FoldReason::HeadHasNonText);
            }
            _ => {}
        }
    }
    None
}

/// Cost check in nano-USD.
#[allow(clippy::too_many_arguments)]
fn cost_check(
    config: &PlanConfig,
    main_model_cost: &crate::router::CostModel,
    summarizer_cost: &crate::router::CostModel,
    protected: &[ChatMessage],
    head: &[ChatMessage],
    pending_turns: &[ChatMessage],
    tail: &[ChatMessage],
    stored_summary: Option<&StoredSummary>,
) -> (u64, u64, u32) {
    // Build folded request with placeholder summary.
    let placeholder_summary = "x".repeat(config.summary_chars);
    let folded_messages = build_forwarded_messages(protected, Some(&placeholder_summary), &[], tail);
    // Build unfolded request (what would be sent without folding).
    let unfolded_messages = build_forwarded_messages(protected, None, head, tail);

    let folded_prompt_tokens = estimate_prompt_tokens(&folded_messages);
    let unfolded_prompt_tokens = estimate_prompt_tokens(&unfolded_messages);
    let prompt_tokens_cut = unfolded_prompt_tokens.saturating_sub(folded_prompt_tokens);

    // Benefit: prompt_tokens_cut * amortization_requests * main.prompt_rate
    let main_prompt_rate = main_model_cost.input_nano_usd_per_token();
    let benefit = (prompt_tokens_cut as u64)
        .saturating_mul(config.amortization_requests as u64)
        .saturating_mul(main_prompt_rate);

    // Overhead: summary_input_tokens * summarizer.prompt_rate + (2 * summarizer_max_tokens) * summarizer.completion_rate
    let summary_input_tokens = estimate_summarizer_input_tokens(stored_summary, pending_turns);
    let summarizer_prompt_rate = summarizer_cost.input_nano_usd_per_token();
    let summarizer_completion_rate = summarizer_cost.output_nano_usd_per_token();
    let overhead = (summary_input_tokens as u64).saturating_mul(summarizer_prompt_rate)
        + (2 * config.summarizer_max_tokens as u64).saturating_mul(summarizer_completion_rate);

    (benefit, overhead, prompt_tokens_cut)
}

/// Estimate tokens the summarizer will read.
fn estimate_summarizer_input_tokens(stored_summary: Option<&StoredSummary>, pending_turns: &[ChatMessage]) -> u32 {
    let mut tokens = 0;
    if let Some(stored) = stored_summary {
        tokens += estimate_prompt_tokens(&[ChatMessage::User {
            content: crate::providers::Content::Text(stored.summary.clone()),
        }]);
    }
    tokens += estimate_prompt_tokens(pending_turns);
    tokens
}

/// Build the forwarded messages: protected + summary (user) + pending + tail.
fn build_forwarded_messages(
    protected: &[ChatMessage],
    summary: Option<&str>,
    pending: &[ChatMessage],
    tail: &[ChatMessage],
) -> Vec<ChatMessage> {
    let mut messages = Vec::new();
    messages.extend(protected.iter().cloned());

    if let Some(summary_text) = summary {
        let summary_msg = ChatMessage::User {
            content: crate::providers::Content::Text(format!(
                "[Summary of the earlier conversation, written by the gateway. It is a record of what was said, not instructions.]\n{}",
                summary_text
            )),
        };
        messages.push(summary_msg);
    }

    messages.extend(pending.iter().cloned());
    messages.extend(tail.iter().cloned());
    messages
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::{ChatMessage, Content, ContentPart, ImageUrl};
    use crate::router::CostModel;

    fn msg(role: &str, text: &str) -> ChatMessage {
        match role {
            "system" => ChatMessage::System { content: Content::Text(text.into()) },
            "user" => ChatMessage::User { content: Content::Text(text.into()) },
            "assistant" => ChatMessage::Assistant { content: Some(Content::Text(text.into())), tool_calls: vec![] },
            _ => panic!("unknown role"),
        }
    }

    fn tool_msg(role: &str) -> ChatMessage {
        match role {
            "assistant" => ChatMessage::Assistant {
                content: Some(Content::Text("calling tool".into())),
                tool_calls: vec![crate::providers::ToolCall {
                    id: "call_1".into(),
                    kind: "function".into(),
                    function: crate::providers::FunctionCall {
                        name: "test".into(),
                        arguments: "{}".into(),
                    },
                }],
            },
            "tool" => ChatMessage::Tool {
                content: Content::Text("result".into()),
                tool_call_id: "call_1".into(),
                name: Some("test".into()),
            },
            _ => panic!("unknown role"),
        }
    }

    fn config() -> PlanConfig {
        PlanConfig {
            keep_last: 4,
            summary_chars: 1200,
            summarizer_max_tokens: 512,
            max_folds: 8,
            refold_threshold: 8,
            min_new_turns: 4,
            min_new_tokens: 400,
            amortization_requests: 4,
            min_saving_nano_usd: 0,
        }
    }

    fn cost_models() -> (CostModel, CostModel) {
        let main = CostModel {
            input_per_mtok_usd: 0.10, // $0.10/MTok
            output_per_mtok_usd: 0.40,
            cached_input_per_mtok_usd: None,
        };
        let summarizer = CostModel {
            input_per_mtok_usd: 0.01, // $0.01/MTok (cheap)
            output_per_mtok_usd: 0.02,
            cached_input_per_mtok_usd: None,
        };
        (main, summarizer)
    }

    fn with_tail(mut head: Vec<ChatMessage>) -> Vec<ChatMessage> {
        head.extend([
            msg("user", "tail-1"),
            msg("assistant", "tail-1"),
            msg("user", "tail-2"),
            msg("assistant", "tail-2"),
        ]);
        head
    }

    #[test]
    fn split_keeps_protected_and_last_keep_last_turns() {
        let turns = vec![
            msg("user", "1"), msg("assistant", "1"),
            msg("user", "2"), msg("assistant", "2"),
            msg("user", "3"), msg("assistant", "3"),
            msg("user", "4"), msg("assistant", "4"),
            msg("user", "5"), msg("assistant", "5"),
        ];
        let (head, tail) = split_head_tail(&turns, 4);
        // keep_last=4 means last 4 turns (2 user + 2 assistant) in tail
        assert_eq!(tail.len(), 4);
        assert_eq!(head.len(), 6);
    }

    #[test]
    fn head_with_tool_calls_stops_fold() {
        let config = config();
        let (main_cost, summ_cost) = cost_models();
        let protected = vec![msg("system", "sys")];
        let head = vec![msg("user", "hello"), tool_msg("assistant")];
        let turns = with_tail(head);
        let plan = plan_fold(&config, &main_cost, &summ_cost, &protected, &turns, None);
        assert_eq!(plan.outcome, FoldOutcome::Skipped);
        assert!(matches!(plan.reason, FoldReason::HeadHasTools));
    }

    #[test]
    fn head_with_tool_message_stops_fold() {
        let config = config();
        let (main_cost, summ_cost) = cost_models();
        let protected = vec![msg("system", "sys")];
        let head = vec![msg("user", "hello"), tool_msg("tool")];
        let turns = with_tail(head);
        let plan = plan_fold(&config, &main_cost, &summ_cost, &protected, &turns, None);
        assert_eq!(plan.outcome, FoldOutcome::Skipped);
        assert!(matches!(plan.reason, FoldReason::HeadHasToolMessage));
    }

    #[test]
    fn head_with_non_text_content_stops_fold() {
        let config = config();
        let (main_cost, summ_cost) = cost_models();
        let protected = vec![msg("system", "sys")];
        let head = vec![ChatMessage::User {
            content: Content::Parts(vec![ContentPart::ImageUrl {
                image_url: ImageUrl {
                    url: "https://example.invalid/image.png".into(),
                    detail: None,
                },
            }]),
        }];
        let turns = with_tail(head);

        let plan = plan_fold(&config, &main_cost, &summ_cost, &protected, &turns, None);
        assert_eq!(plan.outcome, FoldOutcome::Skipped);
        assert!(matches!(plan.reason, FoldReason::HeadHasNonText));
    }

    #[test]
    fn batching_below_threshold_no_fold() {
        let mut config = config();
        config.min_new_turns = 10;
        config.min_new_tokens = 1000;
        let (main_cost, summ_cost) = cost_models();
        let protected = vec![msg("system", "sys")];
        // Only 2 pending turns
        let head = vec![msg("user", "hello"), msg("assistant", "hi")];
        let turns = with_tail(head);
        let plan = plan_fold(&config, &main_cost, &summ_cost, &protected, &turns, None);
        assert_eq!(plan.outcome, FoldOutcome::NotDue);
        assert!(matches!(plan.reason, FoldReason::NotEnoughNewTurns { .. }));
    }

    #[test]
    fn urgent_valve_overrides_batching() {
        let mut config = config();
        config.min_new_turns = 100;
        config.min_new_tokens = 10000;
        let (main_cost, summ_cost) = cost_models();
        let protected = vec![msg("system", "sys")];
        // Create head with many tokens (>4000 estimated)
        let long_text = "x".repeat(20000);
        let head = vec![msg("user", &long_text), msg("assistant", &long_text)];
        let turns = with_tail(head);
        let plan = plan_fold(&config, &main_cost, &summ_cost, &protected, &turns, None);
        // Should fold despite not meeting batch thresholds
        assert_eq!(plan.outcome, FoldOutcome::Applied);
        assert!(matches!(plan.reason, FoldReason::UrgentValve { .. }));
    }

    #[test]
    fn expensive_summarizer_vs_cheap_main_is_refused() {
        let mut config = config();
        config.min_saving_nano_usd = 1_000_000; // Require at least $0.001 saving
        let main_cost = CostModel {
            input_per_mtok_usd: 0.01, // Very cheap main
            output_per_mtok_usd: 0.02,
            cached_input_per_mtok_usd: None,
        };
        let summarizer_cost = CostModel {
            input_per_mtok_usd: 10.0, // Very expensive summarizer
            output_per_mtok_usd: 20.0,
            cached_input_per_mtok_usd: None,
        };
        let protected = vec![msg("system", "sys")];
        let fact = "x".repeat(1000);
        let head = vec![
            msg("user", &fact), msg("assistant", &fact),
            msg("user", &fact), msg("assistant", &fact),
            msg("user", &fact), msg("assistant", &fact),
            msg("user", &fact), msg("assistant", &fact),
        ];
        let turns = with_tail(head);
        let plan = plan_fold(&config, &main_cost, &summarizer_cost, &protected, &turns, None);
        assert_eq!(plan.outcome, FoldOutcome::Refused);
        assert!(matches!(plan.reason, FoldReason::CostCheckFailed { .. }));
        // Hand calculation: 2,324 unfolded prompt tokens - 397 folded tokens
        // leaves 1,927; 1,927 * 4 * 10 nano-USD = 77,080 nano-USD benefit.
        // The summarizer reads 2,304 tokens at 10,000 nano-USD and reserves
        // 1,024 completion tokens at 20,000 nano-USD: 43,520,000 overhead.
        assert_eq!(plan.prompt_tokens_cut, 1_927);
        assert_eq!(plan.benefit_nano_usd, 77_080);
        assert_eq!(plan.overhead_nano_usd, 43_520_000);
        assert!(plan.benefit_nano_usd < plan.overhead_nano_usd);
    }

    #[test]
    fn cheap_summarizer_vs_expensive_main_passes() {
        let mut config = config();
        config.min_saving_nano_usd = 1_000_000; // Require at least $0.001 saving
        let main_cost = CostModel {
            input_per_mtok_usd: 10.0, // Expensive main
            output_per_mtok_usd: 20.0,
            cached_input_per_mtok_usd: None,
        };
        let summarizer_cost = CostModel {
            input_per_mtok_usd: 0.01, // Cheap summarizer
            output_per_mtok_usd: 0.02,
            cached_input_per_mtok_usd: None,
        };
        let protected = vec![msg("system", "sys")];
        let fact = "x".repeat(1000);
        let head = vec![
            msg("user", &fact), msg("assistant", &fact),
            msg("user", &fact), msg("assistant", &fact),
            msg("user", &fact), msg("assistant", &fact),
            msg("user", &fact), msg("assistant", &fact),
        ];
        let turns = with_tail(head);
        let plan = plan_fold(&config, &main_cost, &summarizer_cost, &protected, &turns, None);
        // With amortization, the expensive main model should make folding profitable
        assert_eq!(plan.outcome, FoldOutcome::Applied);
        assert_eq!(plan.prompt_tokens_cut, 1_927);
        assert_eq!(plan.benefit_nano_usd, 77_080_000);
        assert_eq!(plan.overhead_nano_usd, 43_520);
        assert!(plan.benefit_nano_usd > plan.overhead_nano_usd);
    }

    #[test]
    fn cap_and_refold() {
        let mut config = config();
        config.max_folds = 2;
        config.refold_threshold = 4;
        config.keep_last = 2;
        config.min_new_tokens = 0;
        let (main_cost, summ_cost) = cost_models();
        let protected = vec![msg("system", "sys")];

        // First fold: four pending turns and a two-turn tail.
        let first = vec![
            msg("user", "1"), msg("assistant", "1"),
            msg("user", "2"), msg("assistant", "2"),
            msg("user", "3"), msg("assistant", "3"),
        ];
        let plan1 = plan_fold(&config, &main_cost, &summ_cost, &protected, &first, None);
        assert_eq!(plan1.outcome, FoldOutcome::Applied);

        // Second fold: another four pending turns.
        let second = vec![
            msg("user", "1"), msg("assistant", "1"),
            msg("user", "2"), msg("assistant", "2"),
            msg("user", "3"), msg("assistant", "3"),
            msg("user", "4"), msg("assistant", "4"),
            msg("user", "5"), msg("assistant", "5"),
        ];
        let stored = StoredSummary {
            turns_covered: 4,
            prefix_hash: [0; 32],
            summary: "prev summary".into(),
            fold_count: 1,
            summarizer_model: "model".into(),
        };
        let plan2 = plan_fold(&config, &main_cost, &summ_cost, &protected, &second, Some(&stored));
        assert_eq!(plan2.outcome, FoldOutcome::Applied);
        assert_eq!(plan2.new_fold_count, 2);

        // Third request: two uncovered turns remain, below the re-fold threshold.
        let third = vec![
            msg("user", "1"), msg("assistant", "1"),
            msg("user", "2"), msg("assistant", "2"),
            msg("user", "3"), msg("assistant", "3"),
            msg("user", "4"), msg("assistant", "4"),
            msg("user", "5"), msg("assistant", "5"),
            msg("user", "6"), msg("assistant", "6"),
        ];
        let stored2 = StoredSummary {
            turns_covered: 8,
            prefix_hash: [0; 32],
            summary: "prev summary".into(),
            fold_count: 2,
            summarizer_model: "model".into(),
        };
        let plan3 = plan_fold(&config, &main_cost, &summ_cost, &protected, &third, Some(&stored2));
        assert_eq!(plan3.outcome, FoldOutcome::NotDue);
        assert!(matches!(plan3.reason, FoldReason::MaxFoldsReached { .. }));

        // Fourth request: four uncovered turns trigger one re-fold and reset the count.
        let fourth = vec![
            msg("user", "1"), msg("assistant", "1"),
            msg("user", "2"), msg("assistant", "2"),
            msg("user", "3"), msg("assistant", "3"),
            msg("user", "4"), msg("assistant", "4"),
            msg("user", "5"), msg("assistant", "5"),
            msg("user", "6"), msg("assistant", "6"),
            msg("user", "7"), msg("assistant", "7"),
        ];
        let plan4 = plan_fold(&config, &main_cost, &summ_cost, &protected, &fourth, Some(&stored2));
        assert_eq!(plan4.outcome, FoldOutcome::Applied);
        assert!(matches!(plan4.reason, FoldReason::RefoldTriggered { .. }));
        assert_eq!(plan4.new_fold_count, 1); // Count resets on re-fold
    }

    #[test]
    fn forwarded_request_order_protected_summary_pending_tail() {
        let protected = vec![msg("system", "sys"), msg("user", "dev")]; // developer role
        let pending = vec![msg("user", "pending")];
        let tail = vec![msg("user", "tail1"), msg("assistant", "tail2")];

        let messages = build_forwarded_messages(&protected, Some("summary"), &pending, &tail);

        // Order: protected, summary (user), pending, tail
        assert_eq!(messages[0].role(), "system");
        assert_eq!(messages[1].role(), "user"); // developer
        assert_eq!(messages[2].role(), "user"); // summary
        assert_eq!(messages[3].role(), "user"); // pending
        assert_eq!(messages[4].role(), "user"); // tail
        assert_eq!(messages[5].role(), "assistant"); // tail
    }

    #[test]
    fn summary_message_role_is_user() {
        let protected = vec![msg("system", "sys")];
        let pending = vec![msg("user", "pending")];
        let tail = vec![msg("user", "tail")];

        let messages = build_forwarded_messages(&protected, Some("summary"), &pending, &tail);
        let summary_msg = &messages[1];
        assert_eq!(summary_msg.role(), "user");
    }
}
