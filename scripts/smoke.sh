#!/usr/bin/env bash
#
# Docker Compose smoke test: bring the stack up and prove it serves a real
# payment end to end.
set -euo pipefail

API="${LEDGER_API_URL:-http://localhost:8080}"
SUFFIX="$(date +%s)"

step() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
fail() { printf '\033[31msmoke: %s\033[0m\n' "$*" >&2; exit 1; }

step "Starting the stack"
docker compose up --build -d

step "Waiting for readiness"
for _ in $(seq 1 90); do
  if curl -fsS "${API}/health/ready" >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 2
done
[ "${ready:-0}" = "1" ] || { docker compose logs --tail=50 api; fail "API never became ready"; }
curl -fsS "${API}/health/live" >/dev/null || fail "liveness probe failed"
echo "ok: /health/live and /health/ready"

step "Checking metrics"
curl -fsS "${API}/metrics" | grep -q 'ledger_' || fail "no ledger metrics exposed"
echo "ok: /metrics exposes ledger metrics"

step "Running a payment through the stack"
sys="smoke-system-${SUFFIX}"; a="smoke-a-${SUFFIX}"; b="smoke-b-${SUFFIX}"

post() {
  curl -fsS -X POST "${API}$1" -H 'Content-Type: application/json' \
    -H "Idempotency-Key: $2" -d "$3"
}

post /v1/accounts "s-${SUFFIX}" "{\"id\":\"${sys}\",\"currency\":\"USD\",\"kind\":\"system\",\"allow_overdraft\":true}" >/dev/null
post /v1/accounts "a-${SUFFIX}" "{\"id\":\"${a}\",\"currency\":\"USD\",\"kind\":\"user\"}" >/dev/null
post /v1/accounts "b-${SUFFIX}" "{\"id\":\"${b}\",\"currency\":\"USD\",\"kind\":\"user\"}" >/dev/null
post /v1/transfers "f-${SUFFIX}" \
  "{\"source_account_id\":\"${sys}\",\"destination_account_id\":\"${a}\",\"amount\":5000,\"currency\":\"USD\"}" >/dev/null
post /v1/transfers "t-${SUFFIX}" \
  "{\"source_account_id\":\"${a}\",\"destination_account_id\":\"${b}\",\"amount\":2500,\"currency\":\"USD\"}" >/dev/null

balance=$(curl -fsS "${API}/v1/accounts/${b}/balance" | grep -o '"amount":[0-9-]*' | cut -d: -f2)
[ "${balance}" = "2500" ] || fail "balance = ${balance}, want 2500"
echo "ok: transfer settled, destination balance ${balance}"

step "Verifying reconciliation"
curl -fsS "${API}/v1/reconciliation" >/dev/null || fail "reconciliation reported inconsistencies"
echo "ok: ledger reconciles"

step "Smoke test passed"
