# Plan: folding retention benchmark (real models)

**Branch:** `opencode/folding-retention-benchmark`, off `master` (PR 51 merged).
**Builder:** OpenCode. **Reviewer:** Claude Code, after handoff (section 6).
**Goal:** find out, against real models, whether conversation folding keeps
the facts a conversation depends on, and what it costs once the summarizer is
paid at its real price. Two numbers decide whether folding is turned on for
anyone: fields still answered correctly with folding on, and total spend with
folding on versus off.

Read this whole file before writing code. Section 2 is the contract.

---

## 1. Why, and what is out of scope

Folding is merged (PR 51) and off by default. Everything measured so far is a
mock. `fold_economics` (`tests/integration.rs`) reports 92.9M → 7.3M nano-USD
over 10 requests, but its summarizer:
- costs 1/200 of the main model;
- returns a 3-token summary;
- reads a conversation with nothing worth keeping.

That is an upper bound, not a result. Whether facts survive a fold has never
been measured at all.

5-fold-engine already has the right experiment:
`benchmark_context_retention.py` in `warheart1984-ctrl/5-fold-engine`. This
plan ports its scenarios and scoring to drive this gateway over HTTP.

**Out of scope:**
- Any change to `src/`. If the benchmark finds a bug, write it up in the PR
  and stop; the fix is a separate change.
- Folding with receipts (separate plan, `docs/plans/folding-receipts.md`).
- Real user conversations. Synthetic scenarios only, so artifacts can be
  committed.
- Running real-model calls in CI. They cost money and need vendor keys.

---

## 2. Rules the benchmark must follow

1. **Paired arms.** Every call is sent twice, to tenant `plain` (folding off)
   and tenant `folded` (folding on), with the same main model, the same
   messages and the same parameters (`temperature: 0`). Folding is the only
   difference.
2. **The question never enters history.** Each call sends the history plus the
   question as the last user turn. Neither the question nor the answer is added
   to later history, as in 5-fold-engine.
3. **Spend comes from the gateway's own ledger.** Use `/v1/usage`
   `spent_nano_usd` per tenant, read before and after each call. Do not
   recompute cost from token counts.
4. **Retention counts only facts a fold could have lost.** A field is eligible
   only when:
   - its latest source turn is inside the replaced prefix (`x-gateway-folded`
     ≥ the turn's one-based position);
   - and the `plain` arm answered it correctly on the same call.
5. **Honest labels.** The lifecycle run (lowered thresholds) is labelled
   "lifecycle, not economics" in its artifact and in any summary of it.
6. **No secrets in output.** Keys come from the environment and are never
   printed or written to the artifact.

---

## 3. Design

### 3.1 The harness: `tools/fold_retention.py`

Standard library only (`urllib.request`, `json`, `argparse`), like
`tools/anchor_publisher.py`. Port from 5-fold-engine's
`benchmark_context_retention.py`, keeping names and comments where they
explain a choice:

- **Prompts:** `SYSTEM`, `QUESTION`, `INITIAL`.
- **Scenarios:** `Scenario` (`fixed`), `MutatingScenario` (`mutating`), with
  `MUTATIONS`, `SEED_MUTATIONS`, `LATE_MUTATIONS`, `DISTRACTOR`,
  `CAP_BOUNDARY_MUTATION`, and the distractor/correction overlap check.
- **Scoring:** `score_answer`, unchanged in behaviour:
  - strict typed equality, plus containment recorded separately;
  - dash normalization;
  - duplicate JSON keys refused;
  - `finish_reason` must be `stop`;
  - extra keys recorded.

`advance(call, folds, fold_cap)` needs the fold count. Read it from `/metrics`
`gw_folds_total{tenant="folded",outcome="committed"}` with the admin key.

Requests go to `POST /v1/chat/completions` (OpenAI format), with
`max_tokens` set (the gateway requires it) and `stream: false`.

### 3.2 Bench configuration: `bench/folding/`

Pointed at with `LLM_GATEWAY_CONFIG_DIR`.

- **`models.yaml`:**
  - a main model and a summarizer from one provider the user has a key for
    (Groq first: `live.yml` already covers it);
  - `cost` set to that vendor's real published prices, with the date and the
    source URL in a comment.
- **`tenants.yaml`:**
  - `plain` with folding off;
  - `folded` with folding on and `summarizer_model` set;
  - one admin-scoped key for `/metrics`.

  Keys are read from environment variables, never written in the file.
- **Two folding profiles,** selected by `--profile`:
  - **`lifecycle`:** `min_new_turns: 1`, `min_new_tokens: 0`, `max_folds: 5`,
    `refold_threshold: 8`. Exercises fold, cap, freeze and re-fold within one
    run. Not an economics measurement.
  - **`economics`:** the shipped defaults from `docs/plans/conversation-folding.md`
    section 3.1.

### 3.3 Per-call record and the artifact

One JSON object per call:
- call number;
- per arm: correct fields, containment-correct fields, `finish_reason`, spend
  delta;
- for `folded`: `x-gateway-folded`, fold count, and summarizer prompt and
  completion tokens (deltas of `gw_fold_summarizer_tokens_total`);
- per field: expected value, whether it was eligible (2.4), and whether each
  arm got it right.

The artifact goes to `benchmark_results/fold_retention_<scenario>_<profile>_<UTC timestamp>.json`.
It also carries:
- run metadata: models, prices, profile, git commit;
- aggregates:
  - `retention_rate`: eligible fields the `folded` arm got right ÷ eligible
    fields;
  - accuracy per arm;
  - total spend per arm and net saving;
  - folds committed, rejected and failed;
  - how far the scenario reached: warm, frozen, re-folded.

Scenario messages are synthetic, so they may be stored. Never store keys.

**Exit codes:** `0` all phases covered and no regression, `1` the `folded`
arm got wrong a field the `plain` arm got right, `2` the run was incomplete
(provider error, a phase never reached).

### 3.4 CI and manual runs

- Unit tests in `tools/tests/test_fold_retention.py`, written with
  `unittest` like the publisher's: scenario schedules, `score_answer` cases,
  eligibility. No network. CI already runs `python -m unittest discover -s
  tools/tests` in the `anchor-publisher` job; rename that job (or add one) so
  its name says it covers both.
- Real runs are manual: a `workflow_dispatch` job in a new
  `.github/workflows/fold-retention.yml`, modelled on `live.yml`. It takes
  scenario and profile inputs and uploads the artifact. It never runs on push.

---

## 4. Phases and acceptance tests

Each phase ends with:
- every existing test passing on Windows and Linux;
- `cargo clippy --locked --all-targets -- -D warnings` clean;
- `python -m unittest discover -s tools/tests` passing.

Commit per phase.

### Phase 1: harness, scenarios, scoring (no network)
- [ ] `score_answer`, with one test each for:
  - an exact match;
  - a type mismatch (`"8443"` vs `8443`);
  - a duplicate key;
  - `finish_reason: "length"`;
  - a containment-only match;
  - a Unicode dash normalized;
  - an extra key.
- [ ] Both scenarios produce the same message lists and expected values as
      5-fold-engine for calls 1–50 (`fold_cap` 5). Check against a fixture
      generated once from the Python original, recorded with its commit.
- [ ] Eligibility: a field introduced after the replaced prefix, or one the
      `plain` arm got wrong, is not eligible.
- [ ] A harness run against the mock upstream (`config/benchmark-mock`, or a
      stub HTTP server in the test) completes, writes an artifact with every
      field in 3.3, and exits `0`.

### Phase 2: bench config and the manual workflow
- [ ] `bench/folding/` loads in the gateway (`cargo run` with
      `LLM_GATEWAY_CONFIG_DIR`). One test boots it with the keys stubbed.
- [ ] `fold-retention.yml` runs on `workflow_dispatch` only, and no key value
      appears in its log (keys are masked secrets).

### Phase 3: real runs and the write-up
- [ ] Run `fixed` and `mutating` with `lifecycle`, and `fixed` with
      `economics`, against real models. Commit the three artifacts.
- [ ] A README section "Folding: measured", stating for each run:
      - retention rate;
      - accuracy per arm;
      - total spend per arm;
      - net saving;
      - how far it got: warm, frozen, re-folded.

      Say which numbers are lifecycle runs and therefore not economics. If
      `folded` lost facts or cost more, say so plainly.

---

## 5. Process rules for the builder

- One commit per phase, message saying what and why. Never `--no-verify`,
  never force-push, never edit `vendor/`, never print a key.
- If a rule in section 2 turns out impossible, implement the closest safe
  thing and list it under "Deviations" in the PR description.
- **"Passing" means on both platforms:** read `gh pr checks` before handing
  over.
- An acceptance box is ticked only by a test that fails when the behaviour it
  names is removed.

---

## 6. Handoff to Claude Code

Open one draft PR for the branch. In its description, list per phase:
- what was done;
- the tests that prove it;
- anything skipped and every deviation;
- the three runs' headline numbers.

The review will check:
1. Scoring and scenarios against 5-fold-engine, line by line. The reviewer
   will run the Python original on the same calls and compare.
2. Pairing: that the two arms differ only in folding.
3. Eligibility and retention arithmetic against one artifact, by hand.
4. That spend comes from the ledger and no key is in any log or artifact.
5. CI on both platforms.
