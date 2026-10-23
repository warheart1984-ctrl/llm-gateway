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

/// A current value and the field it belongs to: "owner is now Ines" pins
/// `owner: Ines`. A pinned value alone can survive in a summary that no longer
/// says what it is: in the folding benchmark a summary that kept "Ines", but
/// still told the story as "Mira owns the project ... later Ines", passed the
/// value check and lost the fact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldPin {
    pub field: String,
    pub value: String,
}

impl std::fmt::Display for FieldPin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.field, self.value)
    }
}

/// Updates that name their field. Each has `field` and `value` groups; the
/// field is one word.
fn field_value_patterns() -> &'static [Regex] {
    static RE: OnceLock<Vec<Regex>> = OnceLock::new();
    RE.get_or_init(|| {
        const FIELD: &str = r"(?P<field>[A-Za-z][A-Za-z0-9_-]*)";
        const VALUE: &str = r"(?P<value>\d{1,2}:\d{2}|[A-Za-z0-9][\w-]*)";
        [
            // "the owner is now Ines", "external uploads are now permitted"
            format!(r"\b{FIELD}\s+(?:is|are)\s+now\s+{VALUE}"),
            // "Noor replaces Mira as owner"
            r"\b(?P<value>[A-Z][\w-]*)\s+replaces\s+[A-Z][\w-]*\s+as\s+(?:the\s+)?(?P<field>[a-z][a-z0-9_-]*)".to_string(),
            // "delivery moves to Tromso", "release slipped to 2027-03-04"
            format!(
                r"\b{FIELD}\s+(?:has\s+)?(?:moves?|moved|slipped|changed|switched|was\s+set|is\s+set)\s+to\s+{VALUE}"
            ),
            // "the new city is Bergen"
            format!(r"(?i:\b(?:the\s+)?(?:new|correct|right|updated)\s+){FIELD}\s+(?:is|should\s+be|will\s+be)\s+{VALUE}"),
            // "staging now listens on 8444"
            format!(r"\b{FIELD}\s+now\s+listens\s+on\s+(?:port\s+)?{VALUE}"),
        ]
        .iter()
        .map(|pattern| Regex::new(pattern).expect("field update pattern compiles"))
        .collect()
    })
}

/// Words that point at something else rather than name a field, in any case
/// ("It is now 15:30").
const NOT_FIELDS: &[&str] = &[
    "it", "this", "that", "these", "those", "which", "what", "who", "there", "here", "he", "she", "they",
    "we", "i", "you", "and", "or", "but", "now", "so", "then",
];

/// The field/value pairs `text` sets, the latest value for each field, in the
/// order each field first appears. Fields are lowercased.
pub fn extract_field_pins(text: &str) -> Vec<FieldPin> {
    let mut found: Vec<(usize, String, String)> = Vec::new();
    for pattern in field_value_patterns() {
        for cap in pattern.captures_iter(text) {
            let (Some(field), Some(value)) = (cap.name("field"), cap.name("value")) else { continue };
            let field = field.as_str().to_lowercase();
            if NOT_FIELDS.contains(&field.as_str()) {
                continue;
            }
            found.push((value.start(), field, value.as_str().to_string()));
        }
    }
    found.sort_by_key(|(at, _, _)| *at);
    let mut pins: Vec<FieldPin> = Vec::new();
    for (_, field, value) in found {
        match pins.iter_mut().find(|pin| pin.field == field) {
            Some(pin) => pin.value = value,
            None => pins.push(FieldPin { field, value }),
        }
    }
    pins
}

/// Whether one line of a summary states `pin` as current: it names the field,
/// and the value follows it before any "(was ...)" note of an older value.
fn line_states(line: &str, pin: &FieldPin) -> bool {
    let field = pin.field.to_lowercase();
    let value = pin.value.to_lowercase();
    line.match_indices(&field).any(|(at, _)| {
        let rest = &line[at + field.len()..];
        rest.split("(was").next().unwrap_or(rest).contains(&value)
    })
}

/// Check that every pin is stated as current on some line of the summary,
/// ignoring case. Like the value check, done on the clipped summary.
pub fn check_field_pins(summary: &str, pins: &[FieldPin]) -> Result<(), Vec<String>> {
    let lines: Vec<String> = summary.lines().map(str::to_lowercase).collect();
    let missing: Vec<String> = pins
        .iter()
        .filter(|pin| !lines.iter().any(|line| line_states(line, pin)))
        .map(ToString::to_string)
        .collect();
    if missing.is_empty() { Ok(()) } else { Err(missing) }
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
            // A quoted string is kept as written; a bare match is dropped
            // when it is only an ordinary hyphenated word.
            if let Some(value) = cap.get(1) {
                keep(value.as_str());
            } else if let Some(value) = cap.get(0)
                && !is_plain_hyphenated_word(value.as_str())
            {
                keep(value.as_str());
            }
        }
    }
    terms
}

/// First parts that make a lowercase hyphenated word ordinary English:
/// prefixes ("re-run", "e-mail", "self-check") and the usual compound heads
/// ("well-known", "follow-up", "built-in", "read-only", "end-to-end").
const ENGLISH_HEADS: &[&str] = &[
    "all", "anti", "back", "built", "check", "co", "counter", "cross", "day", "de", "down", "e", "end",
    "ex", "first", "follow", "front", "full", "half", "high", "ill", "in", "inter", "last", "log",
    "long", "low", "mid", "multi", "non", "off", "on", "one", "opt", "out", "over", "post", "pre",
    "read", "real", "re", "self", "semi", "set", "short", "sign", "so", "sub", "two", "un", "under",
    "up", "well", "write",
];

/// Deliberate divergence from 5-fold-engine, whose identifier pattern pins
/// every hyphenated word. On ordinary prose that made the pinned list run past
/// `summary_chars`, so every fold was paid for and refused. A match is an
/// ordinary word, and not pinned, only when it is all lowercase letters and
/// hyphens and its first part is a common English prefix or compound head.
/// Anything with a digit, an underscore or a capital ("build-42",
/// "api_key", "ACME-42") is still pinned, and so is a lowercase name like
/// "staging-db" whose first part is not on the list.
fn is_plain_hyphenated_word(value: &str) -> bool {
    let mut parts = value.split('-');
    let first = parts.next().unwrap_or_default();
    value.contains('-')
        && value.chars().all(|c| c.is_ascii_lowercase() || c == '-')
        && ENGLISH_HEADS.contains(&first)
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
    let preamble = crate::folding::plan::SUMMARY_PREAMBLE;
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
        let preamble = crate::folding::plan::SUMMARY_PREAMBLE;
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
    fn ordinary_hyphenated_words_are_not_values() {
        assert!(
            extract_pinned_terms("A well-known follow-up: e-mail the team and re-run the self-check.").is_empty(),
            "{:?}",
            extract_pinned_terms("A well-known follow-up: e-mail the team and re-run the self-check.")
        );
        assert_eq!(
            extract_pinned_terms("Use staging-db, build-42, BLUE_TEAM and api_key_v2 by 2026-11-02 for $19.99."),
            ["2026-11-02", "staging-db", "build-42", "BLUE_TEAM", "api_key_v2", "$19.99"]
        );
    }

    #[test]
    fn apostrophes_in_ordinary_words_pin_nothing() {
        assert!(extract_pinned_terms("I don't know, it's late.").is_empty());
        assert_eq!(extract_pinned_terms("call it 'Project Blue' now"), ["Project Blue"]);
    }

    fn pin(field: &str, value: &str) -> FieldPin {
        FieldPin { field: field.into(), value: value.into() }
    }

    #[test]
    fn field_pins_name_what_each_update_sets() {
        let text = "Correction: owner is now Ines, superseding the earlier value.\n\
                    Ownership correction: Noor replaces Mira as owner.\n\
                    Budget correction: the ceiling is now 1800 USD.\n\
                    Correction: external uploads are now permitted.\n\
                    Correction: delivery moves to Bergen. Release slipped to 2027-03-04.\n\
                    The new city is Tromso. Correction: staging now listens on 8444.";
        assert_eq!(
            extract_field_pins(text),
            [
                pin("owner", "Noor"),
                pin("ceiling", "1800"),
                pin("uploads", "permitted"),
                pin("delivery", "Bergen"),
                pin("release", "2027-03-04"),
                pin("city", "Tromso"),
                pin("staging", "8444"),
            ]
        );
    }

    #[test]
    fn the_latest_update_of_a_field_wins() {
        let text = "Delivery moves to Bergen. Later: delivery moves to Tromso. The OWNER is now Noor, owner is now Ines.";
        assert_eq!(extract_field_pins(text), [pin("delivery", "Tromso"), pin("owner", "Ines")]);
    }

    #[test]
    fn pointing_words_and_sentence_starts_are_not_fields() {
        assert!(extract_field_pins("It is now 15:30. Then it is now late, and that is now done.").is_empty());
        assert!(extract_field_pins("Noor replaces Mira, and the team moves on.").is_empty());
    }

    #[test]
    fn a_field_pin_needs_its_value_on_the_fields_line() {
        let pins = [pin("owner", "Ines")];
        assert!(check_field_pins("Current facts:\n- Owner: Ines (was Noor)\n- City: Oslo", &pins).is_ok());
        // The value is kept, but not as the owner: what the benchmark saw.
        let err = check_field_pins("Current facts:\n- Owner: Mira\n- Note: Ines joined", &pins).unwrap_err();
        assert_eq!(err, ["owner: Ines"]);
        // An older value only.
        assert!(check_field_pins("- owner: Noor (was Ines)", &pins).is_err());
        // Prose that says it on one line still counts.
        assert!(check_field_pins("The owner is now Ines.", &pins).is_ok());
    }

    #[test]
    fn pinned_terms_are_compared_without_case() {
        assert!(check_pinned_facts("deploy to STAGING-DB", "Deploy to the staging-db host").is_ok());
    }
}
