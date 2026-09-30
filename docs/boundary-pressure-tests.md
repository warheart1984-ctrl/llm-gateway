# Boundary pressure tests

**The claim:** the gateway prevents an unauthorised or financially
inadmissible AI request from reaching the provider.

**How it is tested:** `tests/pressure.rs`. Each test applies one kind of
pressure and checks the claim *at the provider*. The mock provider counts
every request it receives, so "refused" means the provider provably never saw
the request, not merely that the gateway returned an error code.

```bash
cargo test --test pressure
```

**Legend**
- **HOLDS:** the boundary held under this pressure, and the test proves it.
- **GAP:** the boundary does not hold today. The test asserts the actual
  fail-open behaviour, so it passes while the gap exists and fails the day
  the gap is closed. The fix then has to flip the assertion, so the claim
  can't change silently.
- **NOT BUILT:** no mechanism exists, so there is nothing to test yet.

Scope: both entry points and every ledger backend. Sections 1–6 exercise
`POST /v1/chat/stream`. Section 7 checks that the second entry point,
`POST /v1/chat/complete`, cannot be used to get around the boundary.
Section 8 covers replays, section 9 the shared Postgres ledger (restarts,
replicas, a dead database, answers at rest), section 10 the local SQLite
ledger and quota-split replicas, section 11 real process kills, and section
12 what still fails open.

## 1. Identity: who is asking?

| Pressure | Must happen | Test | Result |
|---|---|---|---|
| No credential | 401; provider receives nothing | `identity_no_credential_never_reaches_the_provider` | HOLDS |
| Forged key: near-misses, wrong case, empty, wrong auth scheme | 401; provider receives nothing | `identity_a_forged_key_never_reaches_the_provider` | HOLDS |
| Credential switched off in config | refused; provider receives nothing | `identity_a_switched_off_credential_never_reaches_the_provider` | HOLDS |
| Key revoked while the gateway runs | 401 from the next request; no restart needed | `identity_a_revoked_key_stops_at_the_next_request` | HOLDS |
| Tenant disabled | refused; provider receives nothing | `identity_a_disabled_tenant_never_reaches_the_provider` | HOLDS |

## 2. Authority: are they allowed?

| Pressure | Must happen | Test | Result |
|---|---|---|---|
| Valid key without the spend scope | 403; provider receives nothing | `authority_a_key_without_the_spend_scope_never_reaches_the_provider` | HOLDS |
| Application key attempts operator actions (reload, metrics) | 403; only an `admin` key succeeds | `authority_operator_actions_need_the_admin_scope` | HOLDS |
| Tenant asks for another tenant's spend | answered with its own ledger only | `authority_a_tenant_sees_only_its_own_spend` | HOLDS |

## 3. Model access: which provider and model?

| Pressure | Must happen | Test | Result |
|---|---|---|---|
| Model outside the tenant's allowlist | 403; provider receives nothing | `model_outside_the_allowlist_never_reaches_the_provider` | HOLDS |
| Denied model, requested under a `mock/*` wildcard | 403: deny beats allow | `model_denied_under_a_wildcard_never_reaches_the_provider` | HOLDS |
| Unknown or wildcard model names | 4xx; provider receives nothing | `model_unknown_never_reaches_the_provider` | HOLDS |
| Model or transport smuggled through `params` | the registry model and gateway transport are what the provider sees | `model_and_transport_cannot_be_redirected_through_parameters` | HOLDS |

## 4. Budget and pre-flight cost

| Pressure | Must happen | Test | Result |
|---|---|---|---|
| Worst-case exposure over the remaining budget, even though the real answer would be cheap | 402 before execution; costs nothing | `budget_worst_case_exposure_over_budget_is_refused_up_front` | HOLDS |
| No output cap anywhere, so exposure is unbounded | 400 before execution | `budget_an_uncapped_request_is_refused` | HOLDS |
| Output cap above the tenant ceiling | the provider receives the ceiling | `budget_the_output_ceiling_is_what_the_provider_receives` | HOLDS |
| 20 simultaneous requests against room for 3 | exactly 3 reach the provider, 17 refused | `budget_a_concurrent_burst_admits_exactly_what_fits` | HOLDS |
| One tenant pinned at its ceiling | other tenants unaffected | `budget_one_tenants_exhaustion_does_not_touch_another` | HOLDS |

## 5. Failure: provider or client misbehaving

| Pressure | Must happen | Test | Result |
|---|---|---|---|
| Provider unreachable | 502; costs nothing; slot released | `failure_an_unreachable_provider_costs_nothing_and_frees_the_slot` | HOLDS |
| Provider refuses (429) | 502, marked retryable; costs nothing | `failure_a_refusing_provider_costs_nothing` | HOLDS |
| Provider accepts and never answers | released within the read timeout; costs nothing; slot freed | `failure_a_hung_provider_cannot_pin_budget_or_slots` | HOLDS |
| Provider dies mid-answer | in-band error, stream still terminates; only delivered output billed | `failure_a_provider_dying_mid_answer_bills_only_what_was_delivered` | HOLDS |
| 100 clients give up at once | no concurrency slot leaks; next request served | `failure_a_disconnect_storm_leaks_no_slots` | HOLDS |
| 64 MiB body declared; 500-header flood | 413 / 431 before the body is read | `failure_oversized_input_is_refused_before_it_is_read` | HOLDS |

## 6. Accountability: what was refused, and why?

| Pressure | Must happen | Test | Result |
|---|---|---|---|
| A refused request | structured record with request id, tenant, reason | `ledger_every_refusal_is_recorded_with_request_tenant_and_reason` | HOLDS as a log line only |
| A known caller's refusal | on the decision record with request, tenant, key, code, endpoint and model; queryable by an admin | `ledger_a_refusal_is_on_the_decision_record` | HOLDS |
| An anonymous (401) refusal | never persisted: it would let anyone write to the database | `ledger_anonymous_refusals_are_never_recorded` | HOLDS |
| A prompt in a refused request, or quoted by a parser error | never on the decision record | `ledger_the_decision_record_holds_no_prompt_text` | HOLDS |
| A restart, on the SQLite ledger | the decision record survives | `ledger_decisions_survive_a_restart` | HOLDS |
| An operator reloads the registry | recorded as an admin action; decisions need the admin scope | `ledger_an_operator_action_is_recorded` | HOLDS |
| An operator action whose record cannot be written (database cut) | the action does not happen: 503, and the key it would revoke still works | `shared_ledger_an_operator_action_that_cannot_be_recorded_does_not_happen` | HOLDS |

## 7. The second door: `POST /v1/chat/complete`

| Pressure | Must happen | Test | Result |
|---|---|---|---|
| No key, forged key, switched-off key, key without the spend scope, disabled tenant | 401/403; provider receives nothing | `second_door_identity_and_authority_hold` | HOLDS |
| Off-allowlist, denied or unknown model; model or `stream: true` smuggled through `params` | refused; provider receives the registry model and `stream: false` | `second_door_model_access_holds` | HOLDS |
| Worst case over budget; no output cap | 402 / 400 before execution | `second_door_worst_case_exposure_is_refused_up_front` | HOLDS |
| 20 simultaneous requests alternating between both doors, room for 3 | exactly 3 reach the provider: one budget, not two | `second_door_both_doors_draw_on_one_budget` | HOLDS |
| Provider accepts and never answers | released within the read timeout; costs nothing; slot freed | `second_door_a_hung_provider_cannot_pin_budget_or_slots` | HOLDS |
| Provider dies mid-answer | 502; prompt billed, output reservation refunded | `second_door_a_provider_dying_mid_answer_bills_the_prompt_only` | HOLDS |
| 20 clients give up while waiting | no slot leaks; each billed its reservation, because the provider finishes unseen | `second_door_a_disconnect_storm_leaks_no_slots_and_bills_each_reservation` | HOLDS |

## 8. Replays: one execution and one charge per `Idempotency-Key`

Both ledger backends. The key and the charge are claimed in one atomic
step, together with a fingerprint of the request.

| Pressure | Must happen | Test | Result |
|---|---|---|---|
| Same key, same streamed request, sent twice | 409 `duplicate_request` naming the original and its bill; provider called once, billed once | `replay_a_repeated_stream_is_recognised_not_executed` | HOLDS |
| Same key, same completion, sent twice | the stored answer returned byte for byte; provider called once, billed once | `replay_a_repeated_completion_is_served_from_the_ledger` | HOLDS |
| Same key, different request | 422 `idempotency_key_reused` | `replay_the_same_key_for_a_different_request_is_refused` | HOLDS |
| Retry while the first attempt is still running | 409 `request_in_progress`, `retry-after: 1` | `replay_a_retry_while_the_first_is_running_is_told_to_wait` | HOLDS |
| Retry after the provider refused (nothing executed, nothing billed) | executed again, not refused as a duplicate | `replay_a_refused_attempt_can_be_retried_under_its_key` | HOLDS |
| Tenant configured to require the key, request without one | 400 `idempotency_key_required`; provider receives nothing | `replay_a_tenant_can_require_the_key` | HOLDS |
| Two identical requests with no key | both executed: they may be two real requests | `replay_without_a_key_cannot_be_recognised` | LIMIT (by design) |

## 9. The shared ledger: restarts, replicas, a dead database

`[ledger] backend = "postgres"`, tested against a real Postgres. CI sets
`LLM_GATEWAY_REQUIRE_TEST_DATABASE`, so a missing database fails these tests
there instead of skipping them.

| Pressure | Must happen | Test | Result |
|---|---|---|---|
| Crash and restart | spend survives; an exhausted tenant stays refused | `shared_ledger_a_restart_keeps_todays_spend` | HOLDS |
| Two replicas, sequential | the second sees the first one's spend and refuses | `shared_ledger_replicas_draw_on_one_budget` | HOLDS |
| 20 simultaneous requests across two replicas, room for 3 | exactly 3 reach the provider | `shared_ledger_a_burst_across_replicas_admits_exactly_what_fits` | HOLDS |
| Same key sent to two different replicas | executed once, the second served from the ledger | `shared_ledger_a_repeated_key_is_recognised_across_replicas` | HOLDS |
| Database connection cut mid-run | 503 `ledger_unavailable`; provider receives nothing; readiness fails | `shared_ledger_a_dead_ledger_fails_closed` | HOLDS |
| 20 simultaneous requests across two replicas, concurrency cap 3 | exactly 3 reach the provider, the rest 429 `concurrency_limited`: one cap, not one per replica | `shared_ledger_concurrency_is_one_cap_across_replicas` | HOLDS |
| 14 requests alternating between two replicas, 6 allowed per minute | exactly 6 admitted: one window, not one per replica | `shared_ledger_the_rate_limit_is_one_window_across_replicas` | HOLDS |
| 40 completions racing on two replicas: settlements write the rate counters while admissions hold the day row | all 40 succeed; the database never has to abort a deadlocked transaction | `shared_ledger_settlements_racing_admissions_never_deadlock` | HOLDS |
| Ledger unreachable at startup | the gateway refuses to start; the error does not echo credentials | `shared_ledger_a_gateway_will_not_start_without_its_ledger` | HOLDS |
| Read the stored answers straight from the database | ciphertext only (AES-256-GCM); the replay still returns the real answer | `shared_ledger_stored_answers_are_sealed` | HOLDS |
| No sealing key configured | no answer stored at all; a repeat is recognised and billed once, with 409 instead of a replay | `shared_ledger_without_a_key_stores_no_answer` | HOLDS |
| Copy one request's sealed answer into another request's row | the copy does not open; 409, never the wrong answer; nothing re-executed | `shared_ledger_an_answer_moved_to_another_row_is_not_served` | HOLDS |

## 10. The local ledger (SQLite) and quota-split replicas

No database server, so these run on every platform, Windows included.

| Pressure | Must happen | Test | Result |
|---|---|---|---|
| Crash and restart on the SQLite ledger | spend survives; an exhausted tenant stays refused | `local_ledger_a_restart_keeps_todays_spend` | HOLDS |
| Same `Idempotency-Key` after a restart | recognised; executed once across the restart | `local_ledger_a_repeated_key_is_recognised_after_a_restart` | HOLDS |
| SQLite ledger cannot be opened | the gateway refuses to start | `local_ledger_a_gateway_will_not_start_without_its_ledger` | HOLDS |
| 20 simultaneous requests across two split replicas, no shared database, room for 3 | exactly one share per replica reaches the provider: 2, where unsplit replicas would admit 6 | `quota_split_a_burst_across_replicas_admits_exactly_the_shares` | HOLDS |
| A split replica's rate limit | each replica enforces its share of the minute | `quota_split_divides_the_rate_limit_too` | HOLDS |
| A stream held open for 4 s against a 1 s lease | its lease keeps renewing; never swept while live; billed its real usage | `local_ledger_a_long_stream_is_never_swept_while_live` | HOLDS |
| A split replica restarts | its spent share survives; `/v1/usage` reports the share and the tenant budget | `quota_split_a_restarted_replica_keeps_its_spent_share` | HOLDS |
| A split configured on a ledger that would fail open, or with a 0% margin | the gateway refuses to start | `quota_split_refuses_to_boot_where_it_would_fail_open` | HOLDS |
| A tenant budget whose share rounds down to zero | refused, never treated as "no ceiling" | `a_share_that_rounds_to_zero_refuses_instead_of_unlimiting` (unit) | HOLDS |

## 11. Crashes: the real binary, killed

`tests/crash.rs` starts the actual `llm-gateway` executable, kills it
outright (no destructors, no graceful shutdown), restarts it on the same
SQLite ledger, and reads the ledger file directly. Not covered: a kill after
the answer completes but before its settlement is written, a window of
milliseconds; its outcome (open, then swept and billed in full) is the same
code path as the kills below.

| Pressure | Must happen | Test | Result |
|---|---|---|---|
| Killed while the request body is still arriving | nothing reserved, nothing charged, provider untouched | `a_kill_before_admission_leaves_no_trace` | HOLDS |
| Killed after reserving, with the provider called but silent | the reservation survives; swept and billed in full; never re-executed | `a_kill_after_reserving_before_any_answer_bills_the_reservation` | HOLDS |
| Killed mid-stream, after the first token was delivered | same: swept and billed in full | `a_kill_mid_stream_bills_the_reservation` | HOLDS |
| Killed, then the client retries with the same idempotency key | 409 in progress, then duplicate; executed exactly once across the crash | `a_retry_across_a_kill_is_never_executed_twice` | HOLDS |
| Any deployment asks what its ledger guarantees | readiness names backend, durability and replica mode | `readiness_names_the_ledger_consistency_scope` | HOLDS |

## 12. Known gaps

| Pressure | What happens | Test | Result |
|---|---|---|---|
| Restart, on the in-memory ledger | spend is forgotten | `gap_memory_ledger_a_restart_forgets_todays_spend` | **GAP by design:** memory is opt-in; the default is `sqlite` |
| Two replicas, on the in-memory ledger | each enforces the full budget | `gap_memory_ledger_replicas_each_enforce_the_full_budget` | **GAP by design:** use `postgres`, or `sqlite` with quota split |
| HOLD (neither GO nor NO-GO: wait for approval) | no such decision exists | none | NOT BUILT |

The in-memory ledger is for tests and development, chosen explicitly, and
exact within one process. The reservation table records every admitted
request, and the decision table every refusal, failure and operator action.

## Evidence that the suite can fail

- **Budget check removed from the gateway:** 6 tests fail, including both
  `gap_*` budget tests, which shows they exercise the real budget.
- **Deny-list ignored:** exactly `model_denied_under_a_wildcard…` fails.
- **Mid-answer failure misclassified as "never accepted":** exactly
  `second_door_a_provider_dying_mid_answer…` fails, billing 0 where the
  prompt is owed.
- **Shared ledger's budget condition removed from the SQL:** exactly the
  three shared-ledger budget tests fail (restart, replicas, burst across
  replicas), which shows the database's check, not luck, holds the line.
- **Answer sealing:** removing the row binding fails exactly the
  moved-answer test (the copied answer is served); storing answers in the
  clear fails three tests, including the at-rest check.
- **SQLite budget condition removed from the SQL:** the exact-budget and
  two-processes-on-one-file ledger tests fail, and so does the restart
  pressure test.
- **Quota split ignored:** both split pressure tests fail, and the unit test
  for each replica's share.
- **Lease renewal disabled:** the held-reservation ledger test and the
  4-second stream test fail; the live reservation is swept.
- **Shared concurrency count or rate condition removed from the Postgres
  SQL:** the cross-replica cap and window tests each fail.
- **Closing's lock order reversed** (rate counters before the day row, as
  first written in this change): the racing-settlements test fails 5 runs
  in 5 with deadlock-aborted 503s; restored, it passes 5 in 5.
- **Crash tests:** switching idempotency off makes the retry across a kill
  execute twice; disabling the sweeper leaves the orphaned reservation open
  and unbilled. Both caught.
- **Decision record:** recording anonymous refusals fails exactly the
  anonymous test; letting a reload proceed when its record fails fails
  exactly the fail-closed reload test.
- **Flakiness, diagnosed:** the intermittent failures (about 1 run in 5)
  were ledger requests exceeding their deadline under the suite's own load,
  with dozens of gateways syncing every commit to one disk in parallel.
  They surfaced as a 503 on the SQLite ledger and as an uncounted refusal in
  the Postgres burst test. Fixed by opening every SQLite connection at boot,
  and by giving the tests a loaded machine's deadline, since they assert
  admission outcomes, not latency. The burst test now names any refusal
  that is not a budget refusal. After the fix: 25 consecutive full-suite
  runs on Postgres, 55/55 each time.
