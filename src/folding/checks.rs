//! Summary validation checks.
//!
//! Ported from 5-fold-engine's `check_summary`, `required_fold_terms`.

use regex::Regex;
use std::sync::OnceLock;

/// Explicit replacement values ("moved to 15:30", "is now 8443", "actually
/// use Tromso instead of Oslo"). Ported from 5-fold-engine's
/// `_UPDATE_VALUE_PATTERNS`; its lookaheads become a consumed delimiter, which
/// changes nothing because only group 1 is kept.
fn update_value_patterns() -> &'static [Regex] {
    static RE: OnceLock<Vec<Regex>> = OnceLock::new();
    RE.get_or_init(|| {
        const VALUE: &str = r"(\d{1,2}:\d{2}|[A-Za-z0-9][\w-]*)";
        [
            format!(
                r"(?i)\b(?:moves?\s+to|moved\s+to|slipped\s+to|changed\s+to|set\s+to|switch(?:es|ed)?\s+to|change\s+(?:it\s+)?to|make\s+it)\s+{VALUE}"
            ),
            format!(r"(?i)\b(?:is|are)\s+now\s+{VALUE}"),
            format!(r"(?i)\bnow\s+listens\s+on\s+{VALUE}"),
            format!(
                r"(?i)\b(?:correction|corrected|updated|actually)[:,]?\s+(?:use|using|choose|choosing|keep|keeping)\s+{VALUE}\s+(?:instead\s+of|rather\s+than)\b"
            ),
            format!(
                r"(?i)\b(?:correction|corrected|updated|actually)[:,]?\s+{VALUE}\s+(?:instead\s+of|rather\s+than)\s+[A-Za-z0-9][\w-]*"
            ),
            r"\b(?:the\s+)?(?:correct|right|new|updated)\s+(?:value|name|date|time|amount|number|setting|address|city|port)\s+(?:is|should\s+be|will\s+be)\s+(\d{4}-\d{2}-\d{2}|\d{1,2}:\d{2}|\d+(?:[.,]\d+)?(?:\s?[A-Z]{2,4})?|[A-Z][A-Za-z0-9_-]*)(?:[\s,.;!?]|$)".to_string(),
            r"(?i)\b(?:correction|corrected|updated)[:,]?\s+(?:the\s+)?(?:date|time|value|amount|number)\s+(?:is|should\s+be|will\s+be)\s+(\d{4}-\d{2}-\d{2}|\d{1,2}:\d{2}|\d+(?:[.,]\d+)?(?:\s?[A-Z]{2,4})?)(?:[\s,.;!?]|$)".to_string(),
            format!(r"(?i)\b(?:actually|correction|corrected|updated)[:,]?\s+(?:it'?s|its|that'?s|the\s+answer\s+is)\s+{VALUE}"),
            r"\b([A-Z][A-Za-z0-9_-]*)\s+replaces\b".to_string(),
        ]
        .iter()
        .map(|pattern| Regex::new(pattern).expect("update pattern compiles"))
        .collect()
    })
}

/// Stable values a summary must keep even when not phrased as an update.
/// 5-fold-engine's `_DURABLE_PATTERNS` (dates, times, hyphenated or
/// underscored identifiers, ticket ids, short quoted strings), plus currency
/// amounts and measurements, which this gateway pins as well.
///
/// One deliberate difference: a quote only opens after a non-word character,
/// so the apostrophes in "don't ... it's" do not pin the text between them.
fn durable_patterns() -> &'static [Regex] {
    static RE: OnceLock<Vec<Regex>> = OnceLock::new();
    RE.get_or_init(|| {
        [
            r"\b\d{4}-\d{2}-\d{2}\b",
            r"\b\d{1,2}:\d{2}\b",
            r"\b[A-Za-z][A-Za-z0-9]*[-_][A-Za-z0-9][\w-]*\b",
            r"\b[A-Z]{2,}-\d+\b",
            r#"(?:^|[^\w])["']([^"']{1,64})["']"#,
            r"\$\d+(?:\.\d{2})?\b",
            r"(?i)\b\d+(?:\.\d+)?\s*(?:mg|ml|kg|lb|oz|%)",
        ]
        .iter()
        .map(|pattern| Regex::new(pattern).expect("durable pattern compiles"))
        .collect()
    })
}

/// Values that must survive in the clipped summary or the fold is refused,
/// in the order found, without case-insensitive duplicates. Port of
/// 5-fold-engine's `required_fold_terms`.
pub fn extract_pinned_terms(text: &str) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    let mut terms = Vec::new();
    let mut keep = |value: &str| {
        let value = value.trim();
        if !value.is_empty() && seen.insert(value.to_lowercase()) {
            terms.push(value.to_string());
        }
    };
    for pattern in update_value_patterns() {
        for cap in pattern.captures_iter(text) {
            if let Some(value) = cap.get(1) {
                keep(value.as_str());
            }
        }
    }
    for pattern in durable_patterns() {
        for cap in pattern.captures_iter(text) {
            if let Some(value) = cap.get(1).or_else(|| cap.get(0)) {
                keep(value.as_str());
            }
        }
    }
    terms
}

/// Check if all pinned terms from the source text are present in the summary,
/// ignoring case as 5-fold-engine does. The check is done against the summary
/// AFTER clipping to summary_chars.
pub fn check_pinned_facts(summary: &str, source_text: &str) -> Result<(), Vec<String>> {
    let pinned = extract_pinned_terms(source_text);
    let mut missing = Vec::new();
    let folded = summary.to_lowercase();

    for term in pinned {
        if !folded.contains(&term.to_lowercase()) {
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
/// Only the turns being folded: protected messages are sent verbatim on every
/// request and never reach the summarizer, so pinning their values would
/// demand terms the summarizer never saw and refuse every fold.
pub fn build_source_text(turns: &[crate::providers::ChatMessage]) -> String {
    let mut parts = Vec::new();
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
    fn build_source_text_is_the_folded_turns_only() {
        let turns = vec![
            crate::providers::ChatMessage::User {
                content: crate::providers::Content::Text("hello".into()),
            },
            crate::providers::ChatMessage::Assistant {
                content: Some(crate::providers::Content::Text("hi".into())),
                tool_calls: vec![],
            },
        ];
        let source = build_source_text(&turns);
        assert_eq!(source, "user: hello\nassistant: hi");
    }

    /// The same inputs through 5-fold-engine's `required_fold_terms` give
    /// exactly these lists (checked against `gateway/summaries.py`).
    #[test]
    fn pinned_terms_match_five_fold_engine() {
        let cases: [(&str, &[&str]); 7] = [
            ("Deploy to the staging-db host, ticket ACME-42.", &["staging-db", "ACME-42"]),
            ("The password hint is \"blue horse\".", &["blue horse"]),
            ("The meeting moved to 15:30 on 2026-11-02.", &["15:30", "2026-11-02"]),
            ("Actually use Tromso instead of Oslo.", &["Tromso"]),
            ("I am now going to explain the plan.", &[]),
            ("Please update the doc tomorrow.", &[]),
            ("The port is now 8443.", &["8443"]),
        ];
        for (text, expected) in cases {
            assert_eq!(extract_pinned_terms(text), expected, "{text}");
        }
    }

    #[test]
    fn apostrophes_in_ordinary_words_pin_nothing() {
        assert!(extract_pinned_terms("I don't know, it's late.").is_empty());
        assert_eq!(extract_pinned_terms("call it 'Project Blue' now"), ["Project Blue"]);
    }

    #[test]
    fn pinned_terms_are_compared_without_case() {
        assert!(check_pinned_facts("deploy to STAGING-DB", "Deploy to the staging-db host").is_ok());
    }
}
