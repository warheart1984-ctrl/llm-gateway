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

Scope: both entry points. Sections 1–6 exercise `POST /v1/chat/stream`.
Section 7 checks that the second entry point, `POST /v1/chat/complete`,
cannot be used to get around the boundary.

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
| Durable record of every decision (authorised, spent, refused) | queryable after the fact | none | NOT BUILT |

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

## 8. Known gaps

| Pressure | What happens today | Test | Result |
|---|---|---|---|
| Crash or restart | the ledger dies with the process; an exhausted tenant spends again | `gap_a_restart_forgets_todays_spend` | **GAP: fails open** |
| Two replicas | each enforces the full budget; N replicas admit N budgets | `gap_replicas_each_enforce_the_full_budget` | **GAP: fails open** |
| Replayed request (same request id and idempotency key) | executed and billed twice | `gap_a_replayed_request_is_executed_and_billed_again` | **GAP: not detected** |
| Replayed completion, as when a client times out and retries | executed and billed twice | `gap_a_replayed_completion_is_executed_and_billed_again` | **GAP: not detected** |
| HOLD (neither GO nor NO-GO: wait for approval) | no such decision exists; the gate is GO / NO-GO only | none | NOT BUILT |

The first two gaps are the subject of `docs/plans/durable-ledger.md`. That
plan's reservation table becomes the durable decision record once rejected
decisions are written to it as well. Replay protection needs an idempotency
key stored with the reservation.

## Evidence that the suite can fail

- **Budget check removed from the gateway:** 6 tests fail, including both
  `gap_*` budget tests, which shows they exercise the real budget.
- **Deny-list ignored:** exactly `model_denied_under_a_wildcard…` fails.
- **Mid-answer failure misclassified as "never accepted":** exactly
  `second_door_a_provider_dying_mid_answer…` fails, billing 0 where the
  prompt is owed.
- **Flakiness:** 10 consecutive runs, 35/35 each time.
