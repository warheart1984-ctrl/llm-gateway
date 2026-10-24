//! The fact ledger: a conversation's facts as data the gateway owns.
//!
//! In the folding benchmark a summarizer asked to keep facts in prose (and
//! later in a "Current facts" block it wrote itself) kept stating replaced
//! values, reading corrections backwards, and writing two lines for one fact
//! with different values; text checks caught some of it and refused useful
//! folds over the rest. So the summarizer no longer writes the facts. It
//! returns fact updates from the new turns, each with the sentence it came
//! from. The gateway checks every update against the conversation, applies
//! them in the order the conversation made them, keeps each value's source and
//! what it replaced, and writes the "Current facts" block itself. A fact a
//! later fold does not mention is carried forward unchanged: the summarizer
//! cannot drop or flip it.

use serde::{Deserialize, Serialize};

use crate::{folding::checks, providers::ChatMessage};

/// A conversation's facts, its standing instructions, and its context note.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ledger {
    pub facts: Vec<Fact>,
    /// What the user told the model to do from now on ("answer in at most
    /// three bullets"), word for word as the conversation has it. A summary
    /// that kept such a rule only as a sentence of its context note lost it:
    /// in the task-success benchmark the folded model then broke it.
    #[serde(default)]
    pub instructions: Vec<Instruction>,
    /// Instructions a later one replaced, the most recent last.
    #[serde(default)]
    pub retired: Vec<Instruction>,
    /// The summarizer's note of what else matters, rewritten each fold.
    pub context: String,
}

/// One fact's current value, where it came from, and what it replaced.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Fact {
    /// A snake_case name.
    pub field: String,
    pub value: String,
    /// The sentence that set the value, as the conversation has it.
    pub evidence: String,
    /// The turn that sentence is in, counted from the conversation's start.
    pub turn: usize,
    /// The values this one replaced, oldest first.
    pub history: Vec<Replaced>,
}

/// A standing instruction, as the conversation worded it, and its turn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Instruction {
    pub text: String,
    pub turn: usize,
}

/// A value a fact used to have, and the turn that set it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Replaced {
    pub value: String,
    pub turn: usize,
}

/// One update as the summarizer returns it. A value may come as a JSON
/// number or boolean; it is compared as text.
#[derive(Debug, Clone, Deserialize)]
pub struct Update {
    pub field: String,
    pub value: serde_json::Value,
    pub evidence: String,
}

/// One standing instruction as the summarizer returns it: the sentence, and
/// the earlier instruction it changes, if any.
#[derive(Debug, Clone, Deserialize)]
pub struct InstructionUpdate {
    pub text: String,
    #[serde(default)]
    pub replaces: Option<String>,
}

/// The summarizer's reply.
#[derive(Debug, Clone, Deserialize)]
pub struct Reply {
    #[serde(default)]
    pub instructions: Vec<InstructionUpdate>,
    #[serde(default)]
    pub updates: Vec<Update>,
    #[serde(default)]
    pub context: String,
}

const MAX_FIELD_CHARS: usize = 48;
const MAX_VALUE_CHARS: usize = 200;
/// Replaced values kept per fact. Only the latest is shown; the rest are
/// provenance, and the oldest go first.
const MAX_HISTORY: usize = 8;
/// Replaced instructions kept, the oldest going first.
const MAX_RETIRED: usize = 8;

impl Ledger {
    /// Characters the ledger holds, for the store's bound.
    pub fn chars(&self) -> usize {
        self.context.len()
            + self.instructions.iter().chain(&self.retired).map(|instruction| instruction.text.len()).sum::<usize>()
            + self
                .facts
                .iter()
                .map(|fact| {
                    fact.field.len()
                        + fact.value.len()
                        + fact.evidence.len()
                        + fact.history.iter().map(|old| old.value.len()).sum::<usize>()
                })
                .sum::<usize>()
    }

    /// The "Current facts:" block: one line per field, the latest value, and
    /// the value it replaced as "(was ...)". Empty when there are no facts.
    pub fn facts_block(&self) -> String {
        if self.facts.is_empty() {
            return String::new();
        }
        let mut block = String::from("Current facts:");
        for fact in &self.facts {
            block.push_str("\n- ");
            block.push_str(&fact.field.replace('_', " "));
            block.push_str(": ");
            block.push_str(&fact.value);
            if let Some(old) = fact.history.last() {
                block.push_str(" (was ");
                block.push_str(&old.value);
                block.push(')');
            }
        }
        block
    }

    /// The standing instructions block, each word for word. Empty when there
    /// are none.
    pub fn instructions_block(&self) -> String {
        if self.instructions.is_empty() {
            return String::new();
        }
        let mut block = String::from("Standing instructions from the user, still in force:");
        for instruction in &self.instructions {
            block.push_str("\n- ");
            block.push_str(&instruction.text);
        }
        block
    }

    /// The summary the model is sent: the standing instructions, the facts
    /// block, then the context note cut to what `summary_chars` leaves.
    /// Instructions and facts are never cut: a ledger whose instructions and
    /// facts alone run past `summary_chars` cannot be folded.
    pub fn render(&self, summary_chars: usize) -> Result<String, String> {
        let facts = [self.instructions_block(), self.facts_block()]
            .into_iter()
            .filter(|block| !block.is_empty())
            .collect::<Vec<_>>()
            .join("\n\n");
        let facts_chars = facts.chars().count();
        if facts_chars > summary_chars {
            return Err(format!(
                "within_length_bound: the instructions and facts alone are {facts_chars} chars, over the {summary_chars} allowed"
            ));
        }
        let context = self.context.trim();
        if context.is_empty() {
            return Ok(facts);
        }
        let heading = if facts.is_empty() { "Context:\n" } else { "\n\nContext:\n" };
        let room = summary_chars.saturating_sub(facts_chars + heading.chars().count());
        if room == 0 {
            return Ok(facts);
        }
        let context: String = context.chars().take(room).collect();
        Ok(format!("{facts}{heading}{context}"))
    }
}

/// Parse the summarizer's reply: one JSON object, optionally in a code fence.
pub fn parse_reply(text: &str) -> Result<Reply, String> {
    let trimmed = text.trim();
    let body = trimmed
        .strip_prefix("```json")
        .or_else(|| trimmed.strip_prefix("```"))
        .and_then(|rest| rest.trim_end().strip_suffix("```"))
        .unwrap_or(trimmed)
        .trim();
    serde_json::from_str(body).map_err(|_| "format: the reply is not the JSON object asked for".to_string())
}

/// Text compared without case, runs of whitespace or curly quotes.
fn normalized(text: &str) -> String {
    text.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
        .replace(['\u{2018}', '\u{2019}'], "'")
        .replace(['\u{201c}', '\u{201d}'], "\"")
}

fn value_text(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(text) => text.trim().to_string(),
        other => other.to_string(),
    }
}

/// A field name as stored: lowercase snake_case.
fn field_name(field: &str) -> Option<String> {
    let name: String = field.trim().to_lowercase().chars().map(|c| if c == ' ' || c == '-' { '_' } else { c }).collect();
    let valid = !name.is_empty()
        && name.len() <= MAX_FIELD_CHARS
        && name.starts_with(|c: char| c.is_ascii_lowercase())
        && name.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    valid.then_some(name)
}

/// Words of a field name that say what it is about, not generic ones.
fn field_words(field: &str) -> Vec<&str> {
    const GENERIC: &[&str] = &["the", "a", "of", "and", "project", "current", "new", "value"];
    field.split('_').filter(|word| word.len() > 2 && !GENERIC.contains(word)).collect()
}

/// The words of a field name its evidence must name at least one of: those
/// that say which fact it is ("staging" in staging_port), not what kind of
/// value it holds (port, date, budget, usd), which a fact may be named by
/// whatever word one sentence used ("budget_usd" for "the ceiling is ...").
fn distinctive_words(field: &str) -> Vec<&str> {
    field_words(field).into_iter().filter(|word| !KINDS.contains(word)).collect()
}

/// Words for the kind of value a fact holds rather than which fact it is.
const KINDS: &[&str] = &[
    "port", "date", "time", "usd", "eur", "gbp", "city", "name", "number", "amount", "budget", "address", "count",
    "total", "limit", "id",
];

/// Whether two names are one fact: one's words all belong to the other's,
/// and what the longer adds is only the kind of value ("staging" and
/// "staging_port", "release" and "release_date"). A name that adds which
/// thing it is about ("birch_204_owner" beside "owner") is another fact.
fn same_fact(a: &str, b: &str) -> bool {
    let (a, b) = (field_words(a), field_words(b));
    let covers = |small: &[&str], large: &[&str]| {
        !small.is_empty()
            && small.iter().all(|word| large.contains(word))
            && large.iter().filter(|word| !small.contains(word)).all(|word| KINDS.contains(word))
    };
    covers(&a, &b) || covers(&b, &a)
}

/// Whether `evidence` names `word`: some word of it starts with the field
/// word's first three letters (five letters or fewer: "owner", "owns") or
/// first four ("staging", "stage").
fn names_word(evidence: &str, word: &str) -> bool {
    let stem: String = word.chars().take(if word.chars().count() <= 5 { 3 } else { 4 }).collect();
    evidence.split(|c: char| !c.is_alphanumeric()).any(|said| said.starts_with(&stem))
}

/// Whether `evidence` runs past one sentence: a full stop, question or
/// exclamation mark followed by more words.
fn more_than_one_sentence(evidence: &str) -> bool {
    let chars: Vec<char> = evidence.chars().collect();
    chars.windows(3).any(|w| matches!(w[0], '.' | '!' | '?') && w[1] == ' ' && w[2].is_alphanumeric())
}

/// A checked update, placed in the conversation.
struct Placed {
    turn: usize,
    offset: usize,
    field: String,
    value: String,
    evidence: String,
}

/// Check each update against the turns being folded and apply them to
/// `prior` in the order the conversation made them. `first_turn` is the
/// number of turns before `pending`. Any update that fails is reported, and
/// nothing is applied: an update that cannot be verified never becomes
/// current.
pub fn apply(
    prior: &Ledger,
    reply: &Reply,
    pending: &[ChatMessage],
    first_turn: usize,
) -> Result<Ledger, Vec<String>> {
    let turns: Vec<String> =
        pending.iter().map(|msg| normalized(&msg.content().map(|c| c.text()).unwrap_or_default())).collect();
    let mut errors = Vec::new();
    let mut placed = Vec::new();
    for update in &reply.updates {
        let value = value_text(&update.value);
        let Some(field) = field_name(&update.field) else {
            errors.push(format!("ledger_field: '{}' is not a field name (snake_case, at most {MAX_FIELD_CHARS} chars)", update.field));
            continue;
        };
        if value.is_empty() || value.chars().count() > MAX_VALUE_CHARS || value.contains('\n') {
            errors.push(format!("ledger_field: {field} has no usable value"));
            continue;
        }
        let evidence = normalized(&update.evidence);
        let evidence = evidence.trim_end_matches(['.', '!', '?', ';', ',', ' ']);
        let found = (evidence.chars().count() >= 3)
            .then(|| turns.iter().enumerate().find_map(|(index, turn)| turn.find(evidence).map(|at| (index, at))))
            .flatten();
        let Some((index, offset)) = found else {
            errors.push(format!("ledger_evidence: the evidence for {field} is not a sentence from the new turns"));
            continue;
        };
        // One sentence: a turn that states two facts is not evidence for either.
        if more_than_one_sentence(evidence) {
            errors.push(format!("ledger_evidence: the evidence for {field} is more than one sentence; quote only the one that states it"));
            continue;
        }
        if !evidence.contains(&normalized(&value)) {
            errors.push(format!("ledger_evidence: the evidence for {field} does not contain '{value}'"));
            continue;
        }
        // "Noor replaces Mira" can make Noor current, never Mira.
        let replaced_here = checks::apply_supersessions(vec![], update.evidence.trim());
        if checks::is_replaced(&value, &replaced_here) {
            errors.push(format!("ledger_backwards: '{value}' for {field} is the value its own evidence replaces"));
            continue;
        }
        // "staging now listens on 8444" sets the staging fact: it cannot be
        // filed under another fact the ledger keeps, nor start a new one
        // ("ceiling: 1800" beside a kept "spending_ceiling_usd").
        let is_new = !prior.facts.iter().any(|fact| fact.field == field);
        let named = checks::field_updates(update.evidence.trim())
            .into_iter()
            .filter(|(_, named, said)| {
                let said_value = value.eq_ignore_ascii_case(said)
                    || value.split_whitespace().next().is_some_and(|first| first.eq_ignore_ascii_case(said));
                said_value && (is_new || !field.contains(named.as_str()))
            })
            .find_map(|(_, named, _)| {
                prior.facts.iter().find(|fact| fact.field != field && fact.field.contains(named.as_str())).map(|fact| fact.field.clone())
            });
        if let Some(other) = named {
            errors.push(format!("ledger_misfiled: the evidence for {field} sets {other}; use that field name"));
            continue;
        }
        // "Production listens on port 9443" is not evidence for staging_port.
        let words = distinctive_words(&field);
        if !words.is_empty() && !words.iter().any(|word| names_word(evidence, word)) {
            errors.push(format!("ledger_misfiled: the evidence for {field} names none of {}", words.join(", ")));
            continue;
        }
        placed.push(Placed { turn: first_turn + index, offset, field, value, evidence: update.evidence.trim().to_string() });
    }
    // The conversation's order, not the reply's, decides which value is latest.
    placed.sort_by_key(|update| (update.turn, update.offset));

    let mut ledger = Ledger { facts: prior.facts.clone(), context: reply.context.trim().to_string(), ..Default::default() };
    for update in placed {
        if let Some(fact) = ledger.facts.iter_mut().find(|fact| fact.field == update.field) {
            if update.turn < fact.turn || fact.value.eq_ignore_ascii_case(&update.value) {
                continue;
            }
            fact.history.push(Replaced { value: std::mem::take(&mut fact.value), turn: fact.turn });
            if fact.history.len() > MAX_HISTORY {
                fact.history.remove(0);
            }
            fact.value = update.value;
            fact.evidence = update.evidence;
            fact.turn = update.turn;
            continue;
        }
        // A second name for a fact already kept would leave one of them stale
        // after the next correction ("owner: Ines" beside "project owner:
        // Noor", "release: 2027-03-04" beside "release date: 2027-02-18"): a
        // name whose words all belong to the other's is the same fact.
        let words = field_words(&update.field);
        if let Some(other) = ledger.facts.iter().find(|fact| same_fact(&fact.field, &update.field)) {
            errors.push(format!(
                "ledger_duplicate: {} is the same fact as {}; use the field name {}",
                update.field, other.field, other.field
            ));
            continue;
        }
        if let Some(other) = ledger.facts.iter().find(|fact| {
            fact.value.eq_ignore_ascii_case(&update.value) && field_words(&fact.field).iter().any(|word| words.contains(word))
        }) {
            errors.push(format!(
                "ledger_duplicate: {} repeats {} = '{}'; use the field name {}",
                update.field, other.field, update.value, other.field
            ));
            continue;
        }
        // A value another fact replaced is not that fact's current value under a new name.
        if let Some(other) = ledger.facts.iter().find(|fact| {
            !fact.value.eq_ignore_ascii_case(&update.value)
                && fact.history.iter().any(|old| old.value.eq_ignore_ascii_case(&update.value))
                && field_words(&fact.field).iter().any(|word| words.contains(word))
        }) {
            errors.push(format!(
                "ledger_duplicate: '{}' for {} was replaced in {} by '{}'",
                update.value, update.field, other.field, other.value
            ));
            continue;
        }
        ledger.facts.push(Fact {
            field: update.field,
            value: update.value,
            evidence: update.evidence,
            turn: update.turn,
            history: Vec::new(),
        });
    }
    apply_instructions(&mut ledger, prior, reply, pending, first_turn, &mut errors);
    if errors.is_empty() { Ok(ledger) } else { Err(errors) }
}

/// An instruction's sentence as the conversation has it: found in a pending
/// turn ignoring case and runs of whitespace, and copied from the turn, so
/// the gateway keeps the user's words, not the summarizer's copy of them.
fn verbatim(pending: &[ChatMessage], text: &str) -> Option<(usize, usize, String)> {
    let wanted = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let wanted = wanted.trim_end_matches(['.', '!', '?', ';', ',', ' ']);
    if wanted.chars().count() < 8 {
        return None;
    }
    pending.iter().enumerate().find_map(|(index, msg)| {
        let turn = msg.content().map(|c| c.text()).unwrap_or_default().split_whitespace().collect::<Vec<_>>().join(" ");
        if !turn.is_ascii() || !wanted.is_ascii() {
            return None;
        }
        let at = turn.to_ascii_lowercase().find(&wanted.to_ascii_lowercase())?;
        let mut end = at + wanted.len();
        // Keep the sentence's own closing mark.
        if turn[end..].starts_with(['.', '!', '?']) {
            end += 1;
        }
        Some((index, at, turn[at..end].to_string()))
    })
}

/// Words that say a sentence changes an earlier rule.
fn says_it_changes(text: &str) -> bool {
    let text = text.to_lowercase();
    ["rule change", "instead", "no longer", "replaces the"].iter().any(|word| text.contains(word))
}

fn same_text(a: &str, b: &str) -> bool {
    normalized(a).trim_end_matches(['.', '!', '?', ' ']) == normalized(b).trim_end_matches(['.', '!', '?', ' '])
}

/// Check each instruction against the turns being folded and apply them in
/// the conversation's order: a new one is kept word for word, one that
/// replaces another retires it, and one that says it changes a rule must
/// name the rule it changes.
fn apply_instructions(
    ledger: &mut Ledger,
    prior: &Ledger,
    reply: &Reply,
    pending: &[ChatMessage],
    first_turn: usize,
    errors: &mut Vec<String>,
) {
    ledger.instructions = prior.instructions.clone();
    ledger.retired = prior.retired.clone();
    let mut placed = Vec::new();
    for update in &reply.instructions {
        let short: String = update.text.chars().take(60).collect();
        let Some((index, offset, text)) = verbatim(pending, &update.text) else {
            errors.push(format!("instruction_evidence: '{short}' is not a sentence from the new turns"));
            continue;
        };
        if more_than_one_sentence(&normalized(&text)) {
            errors.push(format!("instruction_evidence: '{short}' is more than one sentence; quote only the instruction"));
            continue;
        }
        placed.push((first_turn + index, offset, text, update.replaces.clone()));
    }
    placed.sort_by_key(|(turn, offset, _, _)| (*turn, *offset));
    for (turn, _, text, replaces) in placed {
        if ledger.instructions.iter().any(|kept| same_text(&kept.text, &text)) {
            continue;
        }
        match replaces.filter(|old| !old.trim().is_empty()) {
            Some(old) => {
                let Some(at) = ledger.instructions.iter().position(|kept| same_text(&kept.text, &old)) else {
                    let short: String = old.chars().take(60).collect();
                    errors.push(format!("instruction_replaces: '{short}' is not a standing instruction"));
                    continue;
                };
                let gone = ledger.instructions.remove(at);
                ledger.retired.push(gone);
                if ledger.retired.len() > MAX_RETIRED {
                    ledger.retired.remove(0);
                }
            }
            None if says_it_changes(&text) && !ledger.instructions.is_empty() => {
                let short: String = text.chars().take(60).collect();
                errors.push(format!("instruction_replaces: '{short}' changes a rule; name the instruction it replaces"));
                continue;
            }
            None => {}
        }
        ledger.instructions.push(Instruction { text, turn });
    }
}

/// Sentences of `text` that read as a standing instruction: "standing rule",
/// "from now on", "every answer", "always", "never" and the like. Each one in
/// the turns being folded must end up kept or replaced.
pub fn instruction_sentences(text: &str) -> Vec<String> {
    static RE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let re = RE.get_or_init(|| {
        regex::Regex::new(
            r"(?i)\b(standing rule|rule change|from now on|every answer|all (?:your )?answers|any answer|always|never)\b",
        )
        .expect("instruction pattern compiles")
    });
    let flat = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut sentences = Vec::new();
    let mut start = 0;
    let chars: Vec<(usize, char)> = flat.char_indices().collect();
    for (k, (at, c)) in chars.iter().enumerate() {
        let ends = matches!(c, '.' | '!' | '?') && chars.get(k + 1).is_none_or(|(_, next)| *next == ' ');
        if ends || k + 1 == chars.len() {
            let end = at + c.len_utf8();
            let sentence = flat[start..end].trim().to_string();
            if re.is_match(&sentence) {
                sentences.push(sentence);
            }
            start = end;
        }
    }
    sentences
}

/// Whether a sentence the turns gave as an instruction is kept or was replaced.
pub fn keeps_instruction(ledger: &Ledger, sentence: &str) -> bool {
    let wanted = normalized(sentence);
    let wanted = wanted.trim_end_matches(['.', '!', '?', ' ']);
    ledger.instructions.iter().chain(&ledger.retired).any(|kept| {
        let kept = normalized(&kept.text);
        let kept = kept.trim_end_matches(['.', '!', '?', ' ']);
        kept.contains(wanted) || wanted.contains(kept)
    })
}

/// Whether some current fact states `pin`: its value contains the pinned
/// value, and its name or its evidence names the pinned field. The evidence
/// counts because the summarizer may name a fact better than the
/// conversation's word ("budget_usd" for "the ceiling is now 1250 USD").
pub fn states_pin(ledger: &Ledger, pin: &checks::FieldPin) -> bool {
    let value = pin.value.to_lowercase();
    let field = pin.field.to_lowercase();
    ledger.facts.iter().any(|fact| {
        fact.value.to_lowercase().contains(&value)
            && (fact.field.contains(&field) || normalized(&fact.evidence).contains(&field))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::Content;

    fn user(text: &str) -> ChatMessage {
        ChatMessage::User { content: Content::Text(text.into()) }
    }

    fn reply(json: &str) -> Reply {
        parse_reply(json).unwrap()
    }

    fn values(ledger: &Ledger) -> Vec<(String, String)> {
        ledger.facts.iter().map(|fact| (fact.field.clone(), fact.value.clone())).collect()
    }

    fn pair(field: &str, value: &str) -> (String, String) {
        (field.to_string(), value.to_string())
    }

    #[test]
    fn an_instruction_is_kept_in_the_conversations_own_words() {
        let turns = [user("Hi.  Standing rule for every answer from now on: at most THREE bullets. Thanks.")];
        let r = reply(r#"{"instructions":[{"text":"standing rule for every answer from now on: at most three bullets"}]}"#);
        let ledger = apply(&Ledger::default(), &r, &turns, 4).unwrap();
        assert_eq!(
            ledger.instructions,
            [Instruction { text: "Standing rule for every answer from now on: at most THREE bullets.".into(), turn: 4 }]
        );
        // The same instruction again changes nothing.
        let again = apply(&ledger, &r, &turns, 9).unwrap();
        assert_eq!(again.instructions.len(), 1);
        // One not in the turns, or two sentences at once, is refused.
        let made_up = reply(r#"{"instructions":[{"text":"Always answer in French, please."}]}"#);
        assert_eq!(
            apply(&Ledger::default(), &made_up, &turns, 4).unwrap_err(),
            ["instruction_evidence: 'Always answer in French, please.' is not a sentence from the new turns"]
        );
        let two = reply(r#"{"instructions":[{"text":"Hi. Standing rule for every answer from now on: at most THREE bullets."}]}"#);
        assert!(apply(&Ledger::default(), &two, &turns, 4).unwrap_err()[0].contains("more than one sentence"));
    }

    #[test]
    fn a_rule_change_retires_the_rule_it_names() {
        let first = [user("Standing rule: end every answer with this exact line: Ref: A.")];
        let kept = apply(&Ledger::default(), &reply(r#"{"instructions":[{"text":"Standing rule: end every answer with this exact line: Ref: A."}]}"#), &first, 0).unwrap();
        let change = [user("Rule change: from now on end every answer with this exact line instead: Ref: B.")];
        let named = reply(r#"{"instructions":[{"text":"Rule change: from now on end every answer with this exact line instead: Ref: B.","replaces":"Standing rule: end every answer with this exact line: Ref: A."}]}"#);
        let changed = apply(&kept, &named, &change, 10).unwrap();
        assert_eq!(changed.instructions.len(), 1);
        assert!(changed.instructions[0].text.ends_with("Ref: B."));
        assert_eq!(changed.retired, kept.instructions);
        // A change that names no rule, or a rule not kept, is refused.
        let unnamed = reply(r#"{"instructions":[{"text":"Rule change: from now on end every answer with this exact line instead: Ref: B."}]}"#);
        assert!(apply(&kept, &unnamed, &change, 10).unwrap_err()[0].starts_with("instruction_replaces: "), "changes a rule");
        let unknown = reply(r#"{"instructions":[{"text":"Rule change: from now on end every answer with this exact line instead: Ref: B.","replaces":"Never use emoji."}]}"#);
        assert_eq!(
            apply(&kept, &unknown, &change, 10).unwrap_err(),
            ["instruction_replaces: 'Never use emoji.' is not a standing instruction"]
        );
    }

    #[test]
    fn instructions_count_towards_the_ledgers_size() {
        let ledger = Ledger {
            instructions: vec![Instruction { text: "x".repeat(30), turn: 1 }],
            retired: vec![Instruction { text: "y".repeat(20), turn: 0 }],
            context: "z".repeat(5),
            ..Default::default()
        };
        assert_eq!(ledger.chars(), 55);
    }

    #[test]
    fn instructions_render_first_and_are_never_cut() {
        let ledger = Ledger {
            facts: vec![Fact { field: "owner".into(), value: "Ines".into(), evidence: String::new(), turn: 3, history: vec![] }],
            instructions: vec![Instruction { text: "Never mention BIRCH-204.".into(), turn: 1 }],
            context: "Note. ".repeat(40),
            ..Default::default()
        };
        let rendered = ledger.render(160).unwrap();
        assert!(
            rendered.starts_with("Standing instructions from the user, still in force:\n- Never mention BIRCH-204.\n\nCurrent facts:\n- owner: Ines\n\nContext:\nNote."),
            "{rendered}"
        );
        assert_eq!(rendered.chars().count(), 160);
        assert!(ledger.render(60).unwrap_err().starts_with("within_length_bound: the instructions and facts alone are"));
    }

    #[test]
    fn instruction_sentences_are_found_and_others_are_not() {
        let text = "Workshop note 03: the team filed notes. Standing rule: never mention BIRCH-204 or Ana in any answer; \
                    that project is confidential. BIRCH-204 note: do not merge its budget. From now on, dates as DD.MM.YYYY, \
                    for example 18.02.2027.";
        assert_eq!(
            instruction_sentences(text),
            [
                "Standing rule: never mention BIRCH-204 or Ana in any answer; that project is confidential.",
                "From now on, dates as DD.MM.YYYY, for example 18.02.2027.",
            ]
        );
        let ledger = Ledger {
            instructions: vec![Instruction { text: "From now on, dates as DD.MM.YYYY, for example 18.02.2027.".into(), turn: 1 }],
            retired: vec![Instruction { text: "Standing rule: never mention BIRCH-204 or Ana in any answer; that project is confidential.".into(), turn: 0 }],
            ..Default::default()
        };
        assert!(instruction_sentences(text).iter().all(|sentence| keeps_instruction(&ledger, sentence)));
        assert!(!keeps_instruction(&Ledger::default(), "Always answer in French."));
        // Each instruction word on its own.
        assert_eq!(instruction_sentences("Never use emoji."), ["Never use emoji."]);
        assert_eq!(instruction_sentences("Always cite the ticket."), ["Always cite the ticket."]);
        assert_eq!(instruction_sentences("Keep all your answers short."), ["Keep all your answers short."]);
    }

    /// Every kind of turn in the folding benchmark's conversations: only the
    /// five rules read as instructions.
    #[test]
    fn only_the_benchmarks_rules_read_as_instructions() {
        let rules = [
            "Standing rule for every answer from now on: at most three bullet points, each line starting with '- '.",
            "Standing rule: never mention project BIRCH-204 or Ana in any answer; that project is confidential.",
            "Standing rule: end every answer with this exact line: Ref: CEDAR-731",
            "Standing rule from now on: write every date as DD.MM.YYYY, for example 18.02.2027.",
            "Rule change: from now on end every answer with this exact line instead: Ref: CEDAR-731/B",
        ];
        let others = [
            "The project identifier is CEDAR-731.",
            "Mira owns this project. The delivery city is Oslo.",
            "External uploads are forbidden, including diagnostic logs.",
            "Workshop note 07: the team reviewed the packaging checklist, discussed the order of verification steps, and filed the meeting notes. This discussion introduced no changes to the project requirements.",
            "Budget correction: the ceiling is now 1800 USD; 2400 USD is void.",
            "Ownership and destination correction: Noor replaces Mira, and delivery moves to Bergen.",
            "Correction: staging now listens on 8444, not 8443.",
            "Unrelated: project BIRCH-204 is budgeted at 7200 USD and owned by Ana.",
            "BIRCH-204 note: do not merge its 7200 USD budget with the delivery project figures.",
            "Progress note 61: packaging review continues; no requirements changed.",
            "Correction: owner is now Ines, superseding the earlier value.",
            "New recovery phrase: opal-harbor. Record it for the release.",
        ];
        for rule in rules {
            assert_eq!(instruction_sentences(rule), [rule], "{rule}");
        }
        for other in others {
            assert!(instruction_sentences(other).is_empty(), "{other}");
        }
    }

    #[test]
    fn a_reply_parses_with_or_without_a_code_fence() {
        let plain = r#"{"updates":[{"field":"owner","value":"Mira","evidence":"Mira owns this project."}],"context":"c"}"#;
        assert_eq!(parse_reply(plain).unwrap().updates.len(), 1);
        assert_eq!(parse_reply(&format!("```json\n{plain}\n```")).unwrap().context, "c");
        assert_eq!(parse_reply("Mira owns it.").unwrap_err(), "format: the reply is not the JSON object asked for");
        assert!(parse_reply(r#"{"context": "only context"}"#).unwrap().updates.is_empty());
    }

    #[test]
    fn verified_updates_apply_in_the_conversations_order() {
        let turns = [
            user("Mira owns this project. Production listens on port 9443."),
            user("Ownership correction: Noor replaces Mira, and delivery moves to Bergen."),
            user("Correction: production now listens on 9445."),
        ];
        // Listed out of order: the later turn still wins.
        let r = reply(
            r#"{"updates":[
                {"field":"production_port","value":9445,"evidence":"Correction: production now listens on 9445."},
                {"field":"owner","value":"Noor","evidence":"Ownership correction: Noor replaces Mira, and delivery moves to Bergen"},
                {"field":"owner","value":"Mira","evidence":"Mira owns this project."},
                {"field":"production_port","value":"9443","evidence":"Production listens on port 9443."}
            ],"context":"Packaging review continues."}"#,
        );
        let ledger = apply(&Ledger::default(), &r, &turns, 10).unwrap();
        assert_eq!(values(&ledger), [pair("owner", "Noor"), pair("production_port", "9445")]);
        assert_eq!(ledger.facts[0].history, [Replaced { value: "Mira".into(), turn: 10 }]);
        assert_eq!(ledger.facts[0].turn, 11);
        assert_eq!(ledger.facts[1].evidence, "Correction: production now listens on 9445.");
        assert_eq!(ledger.context, "Packaging review continues.");
    }

    #[test]
    fn a_backwards_reading_is_refused() {
        let turns = [user("Ownership correction: Noor replaces Mira, and delivery moves to Bergen.")];
        let r = reply(r#"{"updates":[{"field":"owner","value":"Mira","evidence":"Noor replaces Mira"}]}"#);
        assert_eq!(
            apply(&Ledger::default(), &r, &turns, 0).unwrap_err(),
            ["ledger_backwards: 'Mira' for owner is the value its own evidence replaces"]
        );
    }

    #[test]
    fn evidence_is_one_sentence_naming_its_fact() {
        let turns = [user("Staging listens on port 8443. Production listens on port 9443.")];
        let both = reply(r#"{"updates":[{"field":"staging_port","value":"8443","evidence":"Staging listens on port 8443. Production listens on port 9443."}]}"#);
        assert_eq!(
            apply(&Ledger::default(), &both, &turns, 0).unwrap_err(),
            ["ledger_evidence: the evidence for staging_port is more than one sentence; quote only the one that states it"]
        );
        let misfiled = reply(r#"{"updates":[{"field":"staging_port","value":"9443","evidence":"Production listens on port 9443."}]}"#);
        assert_eq!(
            apply(&Ledger::default(), &misfiled, &turns, 0).unwrap_err(),
            ["ledger_misfiled: the evidence for staging_port names none of staging"]
        );
        let right = reply(r#"{"updates":[
            {"field":"staging_port","value":"8443","evidence":"Staging listens on port 8443."},
            {"field":"production_port","value":"9443","evidence":"Production listens on port 9443."}
        ]}"#);
        assert_eq!(apply(&Ledger::default(), &right, &turns, 0).unwrap().facts.len(), 2);
        // A short field word matches on three letters, a longer one on four.
        assert!(names_word("mira owns this project", "owner"));
        assert!(!names_word("staging stays on port 8443", "stations"));
        assert!(names_word("a version 1.2 note", "version"));
        assert!(!more_than_one_sentence("the ceiling is now 1.5 usd; 2400 usd is void"));
    }

    #[test]
    fn evidence_must_come_from_the_new_turns_and_hold_the_value() {
        let turns = [user("Correction: staging now listens on 8444, not 8443.")];
        let r = reply(
            r#"{"updates":[
                {"field":"staging_port","value":"8445","evidence":"staging now listens on 8444"},
                {"field":"owner","value":"Ines","evidence":"The owner is now Ines."},
                {"field":"Staging Port!","value":"8444","evidence":"staging now listens on 8444"},
                {"field":"staging_port","value":"","evidence":"staging now listens on 8444"}
            ]}"#,
        );
        assert_eq!(
            apply(&Ledger::default(), &r, &turns, 0).unwrap_err(),
            [
                "ledger_evidence: the evidence for staging_port does not contain '8445'",
                "ledger_evidence: the evidence for owner is not a sentence from the new turns",
                "ledger_field: 'Staging Port!' is not a field name (snake_case, at most 48 chars)",
                "ledger_field: staging_port has no usable value",
            ]
        );
        // Spacing, case and a missing full stop do not matter.
        let r = reply(r#"{"updates":[{"field":"Staging port","value":"8444","evidence":"CORRECTION:  staging now listens on 8444"}]}"#);
        assert_eq!(values(&apply(&Ledger::default(), &r, &turns, 0).unwrap()), [pair("staging_port", "8444")]);
    }

    #[test]
    fn facts_carry_forward_and_an_older_update_never_wins() {
        let first = apply(
            &Ledger::default(),
            &reply(r#"{"updates":[{"field":"owner","value":"Ines","evidence":"owner is now Ines"}]}"#),
            &[user("Correction: owner is now Ines.")],
            40,
        )
        .unwrap();
        // A later fold that mentions nothing keeps the fact.
        let quiet = apply(&first, &reply(r#"{"updates":[],"context":"Nothing changed."}"#), &[user("Progress note 51.")], 50).unwrap();
        assert_eq!(values(&quiet), [pair("owner", "Ines")]);
        // A fold whose turns come before the fact's turn cannot replace it.
        let stale = apply(&first, &reply(r#"{"updates":[{"field":"owner","value":"Noor","evidence":"Noor replaces Mira as owner"}]}"#), &[user("Noor replaces Mira as owner.")], 10).unwrap();
        assert_eq!(values(&stale), [pair("owner", "Ines")]);
        // The same value again changes nothing.
        let again = apply(&first, &reply(r#"{"updates":[{"field":"owner","value":"ines","evidence":"owner is Ines"}]}"#), &[user("The owner is Ines.")], 60).unwrap();
        assert!(again.facts[0].history.is_empty());
        // A value can come back.
        let back = apply(&first, &reply(r#"{"updates":[{"field":"owner","value":"Mira","evidence":"Mira replaces Ines as owner"}]}"#), &[user("Mira replaces Ines as owner.")], 70).unwrap();
        assert_eq!(values(&back), [pair("owner", "Mira")]);
        assert_eq!(back.facts[0].history, [Replaced { value: "Ines".into(), turn: 40 }]);
    }

    fn fact(field: &str, value: &str, turn: usize) -> Fact {
        Fact { field: field.into(), value: value.into(), evidence: String::new(), turn, history: vec![] }
    }

    #[test]
    fn a_value_goes_to_the_fact_its_evidence_names() {
        let prior = Ledger { facts: vec![fact("staging_port", "8443", 1), fact("production_port", "9443", 1)], context: String::new(), ..Default::default() };
        let turns = [user("Correction: staging now listens on 8444.")];
        let misfiled = reply(r#"{"updates":[{"field":"production_port","value":"8444","evidence":"staging now listens on 8444"}]}"#);
        assert_eq!(
            apply(&prior, &misfiled, &turns, 5).unwrap_err(),
            ["ledger_misfiled: the evidence for production_port sets staging_port; use that field name"]
        );
        let right = reply(r#"{"updates":[{"field":"staging_port","value":"8444","evidence":"staging now listens on 8444"}]}"#);
        assert_eq!(apply(&prior, &right, &turns, 5).unwrap().facts[0].value, "8444");
        // A name of the summarizer's own is fine when no kept fact is named.
        let fresh = Ledger::default();
        let turns = [user("Budget correction: the ceiling is now 1250 USD.")];
        let named = reply(r#"{"updates":[{"field":"budget_usd","value":"1250 USD","evidence":"the ceiling is now 1250 USD"}]}"#);
        assert_eq!(values(&apply(&fresh, &named, &turns, 0).unwrap()), [pair("budget_usd", "1250 USD")]);
    }

    /// The folding benchmark's mutating scenario, read correctly: every
    /// update is accepted and the latest values win.
    #[test]
    fn the_benchmark_scenario_read_correctly_is_accepted() {
        let turns: Vec<ChatMessage> = [
            "The project identifier is CEDAR-731.",
            "Mira owns this project. The delivery city is Oslo.",
            "The spending ceiling is 2400 USD.",
            "External uploads are forbidden, including diagnostic logs.",
            "Staging listens on port 8443. Production listens on port 9443.",
            "The release date is 2027-02-18.",
            "Budget correction: the ceiling is now 1800 USD; 2400 USD is void.",
            "Ownership and destination correction: Noor replaces Mira, and delivery moves to Bergen.",
            "Correction: staging now listens on 8444, not 8443.",
            "Correction: external uploads are now permitted, including diagnostic logs.",
            "Unrelated: project BIRCH-204 is budgeted at 7200 USD and owned by Ana.",
            "Correction: release slipped to 2027-03-04.",
            "Budget correction again: the ceiling is now 1250 USD; 1800 USD is void.",
            "Correction: delivery moves to Tromso, superseding Bergen.",
            "Correction: production now listens on 9445.",
            "Correction: owner is now Ines, superseding the earlier value.",
        ]
        .into_iter()
        .map(user)
        .collect();
        let r = reply(r#"{"updates":[
            {"field":"project_identifier","value":"CEDAR-731","evidence":"The project identifier is CEDAR-731."},
            {"field":"owner","value":"Mira","evidence":"Mira owns this project."},
            {"field":"delivery_city","value":"Oslo","evidence":"The delivery city is Oslo."},
            {"field":"budget_usd","value":"2400 USD","evidence":"The spending ceiling is 2400 USD."},
            {"field":"external_uploads_allowed","value":"forbidden","evidence":"External uploads are forbidden, including diagnostic logs."},
            {"field":"staging_port","value":"8443","evidence":"Staging listens on port 8443."},
            {"field":"production_port","value":"9443","evidence":"Production listens on port 9443."},
            {"field":"release_date","value":"2027-02-18","evidence":"The release date is 2027-02-18."},
            {"field":"budget_usd","value":"1800 USD","evidence":"Budget correction: the ceiling is now 1800 USD; 2400 USD is void."},
            {"field":"owner","value":"Noor","evidence":"Ownership and destination correction: Noor replaces Mira, and delivery moves to Bergen."},
            {"field":"delivery_city","value":"Bergen","evidence":"Ownership and destination correction: Noor replaces Mira, and delivery moves to Bergen."},
            {"field":"staging_port","value":"8444","evidence":"Correction: staging now listens on 8444, not 8443."},
            {"field":"external_uploads_allowed","value":"permitted","evidence":"Correction: external uploads are now permitted, including diagnostic logs."},
            {"field":"birch_204_budget","value":"7200 USD","evidence":"Unrelated: project BIRCH-204 is budgeted at 7200 USD and owned by Ana."},
            {"field":"birch_204_owner","value":"Ana","evidence":"Unrelated: project BIRCH-204 is budgeted at 7200 USD and owned by Ana."},
            {"field":"release_date","value":"2027-03-04","evidence":"Correction: release slipped to 2027-03-04."},
            {"field":"budget_usd","value":"1250 USD","evidence":"Budget correction again: the ceiling is now 1250 USD; 1800 USD is void."},
            {"field":"delivery_city","value":"Tromso","evidence":"Correction: delivery moves to Tromso, superseding Bergen."},
            {"field":"production_port","value":"9445","evidence":"Correction: production now listens on 9445."},
            {"field":"owner","value":"Ines","evidence":"Correction: owner is now Ines, superseding the earlier value."}
        ]}"#);
        let ledger = apply(&Ledger::default(), &r, &turns, 0).unwrap();
        assert_eq!(
            values(&ledger),
            [
                pair("project_identifier", "CEDAR-731"),
                pair("owner", "Ines"),
                pair("delivery_city", "Tromso"),
                pair("budget_usd", "1250 USD"),
                pair("external_uploads_allowed", "permitted"),
                pair("staging_port", "8444"),
                pair("production_port", "9445"),
                pair("release_date", "2027-03-04"),
                pair("birch_204_budget", "7200 USD"),
                pair("birch_204_owner", "Ana"),
            ]
        );
    }

    #[test]
    fn a_correction_cannot_start_a_new_fact_beside_the_one_it_corrects() {
        let prior = Ledger { facts: vec![fact("spending_ceiling_usd", "2400", 1)], context: String::new(), ..Default::default() };
        let turns = [user("Budget correction: the ceiling is now 1800 USD; 2400 USD is obsolete.")];
        let beside = reply(r#"{"updates":[{"field":"ceiling","value":"1800 USD","evidence":"Budget correction: the ceiling is now 1800 USD; 2400 USD is obsolete."}]}"#);
        assert_eq!(
            apply(&prior, &beside, &turns, 5).unwrap_err(),
            ["ledger_misfiled: the evidence for ceiling sets spending_ceiling_usd; use that field name"]
        );
        let right = reply(r#"{"updates":[{"field":"spending_ceiling_usd","value":"1800","evidence":"the ceiling is now 1800 USD; 2400 USD is obsolete"}]}"#);
        assert_eq!(values(&apply(&prior, &right, &turns, 5).unwrap()), [pair("spending_ceiling_usd", "1800")]);
    }

    #[test]
    fn a_facts_history_keeps_its_latest_replaced_values() {
        let mut ledger = Ledger { facts: vec![fact("port", "1000", 0)], context: String::new(), ..Default::default() };
        for turn in 1..=10 {
            let port = 1000 + turn;
            let text = format!("Correction: port is now {port}.");
            let update = reply(&format!(r#"{{"updates":[{{"field":"port","value":"{port}","evidence":"port is now {port}"}}]}}"#));
            ledger = apply(&ledger, &update, &[user(&text)], turn).unwrap();
        }
        let history: Vec<&str> = ledger.facts[0].history.iter().map(|old| old.value.as_str()).collect();
        assert_eq!(ledger.facts[0].value, "1010");
        assert_eq!(history, ["1002", "1003", "1004", "1005", "1006", "1007", "1008", "1009"]);
    }

    #[test]
    fn a_second_name_for_a_kept_fact_is_refused() {
        let prior = Ledger {
            facts: vec![Fact {
                field: "owner".into(),
                value: "Noor".into(),
                evidence: "Noor replaces Mira".into(),
                turn: 5,
                history: vec![Replaced { value: "Mira".into(), turn: 2 }],
            }],
            context: String::new(),
            ..Default::default()
        };
        let turns = [user("Noor owns the project and leads the team. Mira used to own it.")];
        let duplicate = reply(r#"{"updates":[{"field":"project_owner","value":"Noor","evidence":"Noor owns the project and leads the team"}]}"#);
        assert_eq!(
            apply(&prior, &duplicate, &turns, 10).unwrap_err(),
            ["ledger_duplicate: project_owner is the same fact as owner; use the field name owner"]
        );
        let replaced = reply(r#"{"updates":[{"field":"former_owner","value":"Mira","evidence":"Mira used to own it"}]}"#);
        assert_eq!(
            apply(&prior, &replaced, &turns, 10).unwrap_err(),
            ["ledger_duplicate: 'Mira' for former_owner was replaced in owner by 'Noor'"]
        );
        // Another project's owner is another fact.
        let other_project = reply(r#"{"updates":[{"field":"birch_204_owner","value":"Ana","evidence":"Ana owns BIRCH-204"}]}"#);
        let turns_birch = [user("Ana owns BIRCH-204.")];
        assert_eq!(apply(&prior, &other_project, &turns_birch, 10).unwrap().facts.len(), 2);
        assert!(same_fact("staging", "staging_port"));
        assert!(same_fact("release_date", "release"));
        assert!(!same_fact("owner", "birch_204_owner"));
        assert!(!same_fact("staging_port", "staging_host"));
        // A different fact with the same value is fine.
        let other = reply(r#"{"updates":[{"field":"team_lead","value":"Noor","evidence":"Noor owns the project and leads the team"}]}"#);
        assert_eq!(apply(&prior, &other, &turns, 10).unwrap().facts.len(), 2);
    }

    #[test]
    fn a_value_another_fact_replaced_is_refused_under_a_new_name() {
        let prior = Ledger {
            facts: vec![Fact {
                field: "staging_port".into(),
                value: "8444".into(),
                evidence: String::new(),
                turn: 5,
                history: vec![Replaced { value: "8443".into(), turn: 1 }],
            }],
            context: String::new(),
            ..Default::default()
        };
        let turns = [user("The staging host listens on 8443 for health checks.")];
        let replaced = reply(r#"{"updates":[{"field":"staging_host_port","value":"8443","evidence":"The staging host listens on 8443 for health checks"}]}"#);
        assert_eq!(
            apply(&prior, &replaced, &turns, 10).unwrap_err(),
            ["ledger_duplicate: '8443' for staging_host_port was replaced in staging_port by '8444'"]
        );
        let host = reply(r#"{"updates":[{"field":"health_check_host","value":"8443","evidence":"The staging host listens on 8443 for health checks"}]}"#);
        assert_eq!(apply(&prior, &host, &turns, 10).unwrap().facts.len(), 2);
        let mixed = reply(r#"{"updates":[{"field":"staging_health","value":"8443","evidence":"The staging host listens on 8443 for health checks"}]}"#);
        assert_eq!(
            apply(&prior, &mixed, &turns, 10).unwrap_err(),
            ["ledger_duplicate: '8443' for staging_health was replaced in staging_port by '8444'"]
        );
    }

    #[test]
    fn the_facts_block_is_one_line_per_field_and_never_cut() {
        let ledger = Ledger {
            facts: vec![
                Fact { field: "owner".into(), value: "Ines".into(), evidence: String::new(), turn: 9, history: vec![
                    Replaced { value: "Mira".into(), turn: 1 },
                    Replaced { value: "Noor".into(), turn: 5 },
                ] },
                Fact { field: "production_port".into(), value: "9445".into(), evidence: String::new(), turn: 3, history: vec![] },
            ],
            context: "A long note. ".repeat(20),
            ..Default::default()
        };
        let rendered = ledger.render(120).unwrap();
        assert!(rendered.starts_with("Current facts:\n- owner: Ines (was Noor)\n- production port: 9445\n\nContext:\nA long note."), "{rendered}");
        assert_eq!(rendered.chars().count(), 120);
        assert_eq!(ledger.render(20).unwrap_err(), "within_length_bound: the instructions and facts alone are 63 chars, over the 20 allowed");
        let bare = Ledger { facts: vec![], context: "Only context.".into(), ..Default::default() };
        assert_eq!(bare.render(100).unwrap(), "Context:\nOnly context.");
    }

    #[test]
    fn a_field_pin_is_stated_by_name_or_by_evidence() {
        let ledger = Ledger {
            facts: vec![Fact {
                field: "budget_usd".into(),
                value: "1250 USD".into(),
                evidence: "Budget correction again: the ceiling is now 1250 USD".into(),
                turn: 3,
                history: vec![],
            }],
            context: String::new(),
            ..Default::default()
        };
        let pin = |field: &str, value: &str| checks::FieldPin { field: field.into(), value: value.into() };
        assert!(states_pin(&ledger, &pin("ceiling", "1250")));
        assert!(states_pin(&ledger, &pin("budget", "1250")));
        assert!(!states_pin(&ledger, &pin("ceiling", "1800")));
        assert!(!states_pin(&ledger, &pin("owner", "1250")));
    }
}
