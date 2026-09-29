#!/bin/sh
# Scripted walkthrough of the gateway's claims, run against the demo stack.
# Every step prints what it is showing, then checks it; any failed check exits
# non-zero, which is what CI keys off.
#
#   GATEWAY=http://localhost:8080 OPS=http://localhost:9090 sh demo/client.sh
#
# POSIX sh plus curl only, so it runs in the stock curl image and on a laptop.

set -eu

GATEWAY="${GATEWAY:-http://gateway:8080}"
OPS="${OPS:-http://gateway:9090}"
DEMO_KEY="${DEMO_KEY:-$(cat "${DEMO_KEY_FILE:-/run/secrets/demo_key}")}"
SHOESTRING_KEY="${SHOESTRING_KEY:-$(cat "${SHOESTRING_KEY_FILE:-/run/secrets/shoestring_key}")}"
TMP="$(mktemp -d)"

step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
ok() { printf '   \033[32mok\033[0m  %s\n' "$*"; }
fail() { printf '   \033[31mFAIL\033[0m %s\n' "$*"; exit 1; }

# `status KEY PATH [curl args...]`: body to $TMP/body, HTTP status on stdout.
status() {
  key="$1"; path="$2"; shift 2
  curl -sS -o "$TMP/body" -w '%{http_code}' -H "x-api-key: $key" "$@" "$GATEWAY$path"
}

# First integer value of a JSON field, without jq.
field() {
  sed -n "s/.*\"$1\":\([0-9][0-9]*\).*/\1/p" "$2" | head -n1
}

chat_body='{"model":"mock/chat","messages":[{"role":"user","content":"Explain the gateway in one breath."}],"stream":true,"params":{"max_tokens":64}}'

step "Waiting for the gateway to report ready"
i=0
until curl -fsS "$GATEWAY/health/ready" >/dev/null 2>&1; do
  i=$((i + 1))
  [ "$i" -lt 60 ] || fail "gateway never became ready"
  sleep 1
done
ok "ready"

step "1. A streamed completion under the v1 SSE contract"
curl -sS -N -H "x-api-key: $DEMO_KEY" -H 'content-type: application/json' \
  -d "$chat_body" "$GATEWAY/v1/chat/stream" | tee "$TMP/stream"
grep -q '^event: start' "$TMP/stream" || fail "no start frame"
grep -q '^data: {"token":' "$TMP/stream" || fail "no token frames"
grep -q '^event: end' "$TMP/stream" || fail "no end frame"
grep -q '^data: \[DONE\]' "$TMP/stream" || fail "no [DONE]"
ok "start, tokens, end, [DONE]"

step "2. The tenant can see its own spend"
code=$(status "$DEMO_KEY" /v1/usage)
cat "$TMP/body"; echo
[ "$code" = 200 ] || fail "/v1/usage returned $code"
spent=$(field spent_nano_usd "$TMP/body")
[ "${spent:-0}" -gt 0 ] || fail "spend was not recorded"
ok "spent ${spent} nano-USD, settled from the usage on the final chunk"

step "3. A tiny budget runs out: 402 before any upstream connection"
admitted=0
refused=""
for n in 1 2 3 4 5 6 7 8 9 10; do
  code=$(status "$SHOESTRING_KEY" /v1/chat/stream -H 'content-type: application/json' -d "$chat_body")
  printf '   request %2d -> %s\n' "$n" "$code"
  if [ "$code" = 200 ]; then
    admitted=$((admitted + 1))
  elif [ "$code" = 402 ]; then
    refused="$n"
    break
  else
    cat "$TMP/body"; fail "unexpected status $code"
  fi
done
[ -n "$refused" ] || fail "the budget never ran out"
[ "$admitted" -ge 1 ] || fail "the first request should have fit"
cat "$TMP/body"; echo
grep -q '"budget_exhausted"' "$TMP/body" || fail "402 without the budget_exhausted code"
code=$(status "$SHOESTRING_KEY" /v1/usage)
spent=$(field spent_nano_usd "$TMP/body")
budget=$(field daily_budget_nano_usd "$TMP/body")
[ "$spent" -le "$budget" ] || fail "spent $spent exceeds budget $budget"
ok "$admitted streamed, then refused; spent $spent of $budget nano-USD, never over"

step "4. The other tenant is unaffected"
code=$(status "$DEMO_KEY" /v1/chat/stream -H 'content-type: application/json' -d "$chat_body")
[ "$code" = 200 ] || fail "demo tenant got $code while shoestring was exhausted"
ok "demo still streams (200)"

step "5. A client that hangs up mid-stream releases its slot"
curl -sS -N -m 0.3 -H "x-api-key: $DEMO_KEY" -H 'content-type: application/json' \
  -d "$chat_body" "$GATEWAY/v1/chat/stream" >/dev/null 2>&1 || true
sleep 1
code=$(status "$DEMO_KEY" /v1/usage)
inflight=$(field streams_in_flight "$TMP/body")
[ "$inflight" = 0 ] || fail "$inflight streams still in flight after the disconnect"
ok "streams_in_flight is 0; what was delivered before the hang-up was billed"

step "6. Metrics: admin-only on the public port, open on the ops port"
code=$(curl -sS -o /dev/null -w '%{http_code}' "$GATEWAY/metrics")
[ "$code" = 404 ] || fail "public /metrics should not exist with an ops port set, got $code"
ok "public /metrics: 404 (moved to the ops listener)"
curl -fsS "$OPS/metrics" > "$TMP/metrics" || fail "ops /metrics unreachable"
grep 'gw_tenant_cost_nano_usd_total' "$TMP/metrics"
grep -q 'gw_tenant_cost_nano_usd_total{tenant="shoestring"}' "$TMP/metrics" || fail "no per-tenant cost series"
ok "per-tenant spend is visible to the operator"

printf '\n\033[32mAll demo checks passed.\033[0m\n'
