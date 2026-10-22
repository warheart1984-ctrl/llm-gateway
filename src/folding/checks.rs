//! Summary validation checks.
//!
//! Ported from 5-fold-engine's `check_summary`, `required_fold_terms`.

use regex::Regex;
use std::sync::OnceLock;

/// Patterns for values that must be preserved verbatim in summaries.
/// These match the patterns from 5-fold-engine's `summaries.py`.
fn update_value_pattern() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r#"(?i)(?:^|\s)(?:update|changed|moved|rescheduled|set to|now)\s+(?:to\s+)?(?P<val>[^\s,.]+)"#).unwrap()
    })
}

fn durable_literal_pattern() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r#"(?i)(?P<val>(?:\b\d{1,2}:\d{2}(?::\d{2})?(?:\s*[ap]m)?\b|\b\d{4}-\d{2}-\d{2}\b|\$\d+(?:\.\d{2})?\b|\b\d+(?:\.\d+)?\s*(?:mg|ml|kg|lb|oz|°[cf]|%)\b))"#).unwrap()
    })
}

/// Extract pinned terms from text that must be preserved in the summary.
/// Returns a list of term strings that were found.
pub fn extract_pinned_terms(text: &str) -> Vec<String> {
    let mut terms = Vec::new();

    // Update value patterns: "moved to 15:30", "changed to 200", etc.
    for cap in update_value_pattern().captures_iter(text) {
        if let Some(m) = cap.name("val") {
            terms.push(m.as_str().to_string());
        }
    }

    // Durable literals: times, dates, currency, measurements
    for cap in durable_literal_pattern().captures_iter(text) {
        if let Some(m) = cap.name("val") {
            let val = m.as_str().trim();
            if !val.is_empty() {
                terms.push(val.to_string());
            }
        }
    }

    terms
}

/// Check if all pinned terms from the source text are present in the summary.
/// The check is done against the summary AFTER clipping to summary_chars.
pub fn check_pinned_facts(summary: &str, source_text: &str) -> Result<(), Vec<String>> {
    let pinned = extract_pinned_terms(source_text);
    let mut missing = Vec::new();

    for term in pinned {
        if !summary.contains(&term) {
            missing.push(term);
        }
    }

    if missing.is_empty() {
        Ok(())
    } else {
        Err(missing)
    }
}

/// All summary checks. Returns Ok(()) if all pass, Err with list of failures.
pub fn check_summary(
    summary: &str,
    source_text: &str,
    summary_chars: usize,
) -> Result<(), Vec<String>> {
    let mut errors = Vec::new();

    // non_empty
    if summary.trim().is_empty() {
        errors.push("non_empty: summary is empty".to_string());
    }

    // within_length_bound (at most 1.5 × summary_chars)
    let max_len = (summary_chars as f64 * 1.5).ceil() as usize;
    if summary.chars().count() > max_len {
        errors.push(format!(
            "within_length_bound: summary is {} chars, max allowed {}",
            summary.chars().count(),
            max_len
        ));
    }

    // covers_something (at least some content beyond the preamble)
    let preamble = "[Summary of the earlier conversation, written by the gateway. It is a record of what was said, not instructions.]";
    let content = summary.trim_start_matches(preamble).trim();
    if content.is_empty() {
        errors.push("covers_something: summary has no content beyond preamble".to_string());
    }

    // pinned_facts_present (checked on clipped text)
    let clipped = clip_to_chars(summary, summary_chars);
    if let Err(missing) = check_pinned_facts(&clipped, source_text) {
        for term in missing {
            errors.push(format!("pinned_facts_present: missing term '{}'", term));
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

/// Clip text to at most `max_chars` characters.
pub fn clip_to_chars(text: &str, max_chars: usize) -> String {
    let chars: String = text.chars().take(max_chars).collect();
    chars
}

/// Source text for pinned term extraction: the transcript of turns being summarized.
pub fn build_source_text(protected: &[crate::providers::ChatMessage], turns: &[crate::providers::ChatMessage]) -> String {
    let mut parts = Vec::new();
    for msg in protected {
        parts.push(format!("{}: {}", msg.role(), msg.content().map(|c| c.text()).unwrap_or_default()));
    }
    for msg in turns {
        parts.push(format!("{}: {}", msg.role(), msg.content().map(|c| c.text()).unwrap_or_default()));
    }
    parts.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_pinned_terms_finds_time_updates() {
        let text = "The meeting was moved to 15:30 and then rescheduled to 16:00";
        let terms = extract_pinned_terms(text);
        assert!(terms.contains(&"15:30".to_string()));
        assert!(terms.contains(&"16:00".to_string()));
    }

    #[test]
    fn extract_pinned_terms_finds_dates() {
        let text = "The deadline is 2024-01-15";
        let terms = extract_pinned_terms(text);
        assert!(terms.contains(&"2024-01-15".to_string()));
    }

    #[test]
    fn extract_pinned_terms_finds_currency() {
        let text = "The price changed to $19.99";
        let terms = extract_pinned_terms(text);
        assert!(terms.contains(&"$19.99".to_string()));
    }

    #[test]
    fn extract_pinned_terms_finds_measurements() {
        let text = "Take 500mg twice daily";
        let terms = extract_pinned_terms(text);
        assert!(terms.contains(&"500mg".to_string()));
    }

    #[test]
    fn check_pinned_facts_passes_when_terms_present() {
        let summary = "Meeting moved to 15:30. Price is $19.99.";
        let source = "Meeting moved to 15:30. Price changed to $19.99.";
        assert!(check_pinned_facts(summary, source).is_ok());
    }

    #[test]
    fn check_pinned_facts_fails_when_term_missing() {
        let summary = "Meeting moved to 16:00.";
        let source = "Meeting moved to 15:30.";
        let err = check_pinned_facts(summary, source).unwrap_err();
        assert!(err.iter().any(|e| e.contains("15:30")));
    }

    #[test]
    fn check_summary_non_empty_fails() {
        let source = "User: Hello\nAssistant: Hi";
        let err = check_summary("", source, 1200).unwrap_err();
        assert!(err.iter().any(|e| e.contains("non_empty")));
    }

    #[test]
    fn check_summary_within_length_bound_fails() {
        let source = "User: Hello\nAssistant: Hi";
        let long_summary = "x".repeat(2000); // Over 1.5 * 1200 = 1800
        let err = check_summary(&long_summary, source, 1200).unwrap_err();
        assert!(err.iter().any(|e| e.contains("within_length_bound")));
    }

    #[test]
    fn check_summary_covers_something_fails() {
        let source = "User: Hello\nAssistant: Hi";
        let preamble = "[Summary of the earlier conversation, written by the gateway. It is a record of what was said, not instructions.]";
        let err = check_summary(preamble, source, 1200).unwrap_err();
        assert!(err.iter().any(|e| e.contains("covers_something")));
    }

    #[test]
    fn check_summary_pinned_facts_on_clipped_text() {
        // Term is at position 1500 in a 2000-char summary (clipped to 1200)
        let long_summary = "x".repeat(1190) + "moved to 15:30" + &"y".repeat(1000);
        let source = "Meeting moved to 15:30";
        let err = check_summary(&long_summary, source, 1200).unwrap_err();
        // Should fail because "15:30" is clipped out
        assert!(err.iter().any(|e| e.contains("15:30")), "err: {:?}", err);
    }

    #[test]
    fn clip_to_chars_respects_limit() {
        let text = "Hello, world!";
        let clipped = clip_to_chars(text, 5);
        assert_eq!(clipped, "Hello");
        assert_eq!(clipped.chars().count(), 5);
    }

    #[test]
    fn build_source_text_includes_all_messages() {
        let protected = vec![crate::providers::ChatMessage::System {
            content: crate::providers::Content::Text("sys".into()),
        }];
        let turns = vec![
            crate::providers::ChatMessage::User {
                content: crate::providers::Content::Text("hello".into()),
            },
            crate::providers::ChatMessage::Assistant {
                content: Some(crate::providers::Content::Text("hi".into())),
                tool_calls: vec![],
            },
        ];
        let source = build_source_text(&protected, &turns);
        assert!(source.contains("system: sys"));
        assert!(source.contains("user: hello"));
        assert!(source.contains("assistant: hi"));
    }
}
