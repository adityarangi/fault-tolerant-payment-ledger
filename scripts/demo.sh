#!/usr/bin/env bash
#
# End-to-end demonstration of the fault-tolerant payment ledger.
#
# Walks through the thirteen scenarios the project claims to handle, against a
# running stack:
#
#   docker compose up --build -d
#   ./scripts/demo.sh
#
set -euo pipefail

API="${LEDGER_API_URL:-http://localhost:8080}"
WEBHOOK="${EXAMPLE_WEBHOOK_URL:-http://localhost:9090}"
SUFFIX="$(date +%s)"
SYSTEM="system-demo-${SUFFIX}"
ALICE="alice-${SUFFIX}"
BOB="bob-${SUFFIX}"

bold() { printf '\n\033[1m%s\033[0m\n' "$*"; }
info() { printf '  %s\n' "$*"; }
fail() { printf '\n\033[31mFAILED: %s\033[0m\n' "$*" >&2; exit 1; }

# python3 does the JSON extraction (always present); jq only pretty-prints.
json() { python3 -c "import sys,json;print(json.load(sys.stdin).get('$1',''))"; }
if command -v jq >/dev/null 2>&1; then pretty() { jq .; }; else pretty() { python3 -m json.tool; }; fi

# deliveries_of <event-id>: how many times the receiver has been handed it.
deliveries_of() {
  curl -sS "${WEBHOOK}/events" | python3 -c "
import sys, json
target = '$1'
data = json.load(sys.stdin)
print(next((e['deliveries'] for e in data['events'] if e['event_id'] == target), 0))
"
}

unique_events() { curl -sS "${WEBHOOK}/events" | json unique_events; }

# post <path> <idempotency-key> <body> [extra curl args...]
post() {
  local path="$1" key="$2" body="$3"; shift 3
  curl -sS -X POST "${API}${path}" \
    -H 'Content-Type: application/json' \
    -H "Idempotency-Key: ${key}" \
    "$@" -d "${body}"
}

status_of() {
  local path="$1" key="$2" body="$3"; shift 3
  curl -sS -o /dev/null -w '%{http_code}' -X POST "${API}${path}" \
    -H 'Content-Type: application/json' \
    -H "Idempotency-Key: ${key}" \
    "$@" -d "${body}"
}

balance_of() { curl -sS "${API}/v1/accounts/$1/balance" | json amount; }

bold "0. Waiting for the API to become ready"
for _ in $(seq 1 60); do
  if curl -fsS "${API}/health/ready" >/dev/null 2>&1; then break; fi
  sleep 1
done
curl -fsS "${API}/health/ready" >/dev/null || fail "API never became ready at ${API}"
info "ready"

bold "1. Create and fund accounts"
post /v1/accounts "acct-${SYSTEM}" \
  "{\"id\":\"${SYSTEM}\",\"currency\":\"USD\",\"kind\":\"system\",\"allow_overdraft\":true}" >/dev/null
post /v1/accounts "acct-${ALICE}" "{\"id\":\"${ALICE}\",\"currency\":\"USD\",\"kind\":\"user\"}" >/dev/null
post /v1/accounts "acct-${BOB}"   "{\"id\":\"${BOB}\",\"currency\":\"USD\",\"kind\":\"user\"}" >/dev/null
info "created ${SYSTEM}, ${ALICE}, ${BOB}"

# Funding is a balanced transaction from a system account, not a balance edit.
post /v1/transfers "fund-${ALICE}" \
  "{\"source_account_id\":\"${SYSTEM}\",\"destination_account_id\":\"${ALICE}\",\"amount\":100000,\"currency\":\"USD\",\"description\":\"Initial funding\"}" >/dev/null
info "funded ${ALICE} with 100000 minor units (balance: $(balance_of "${ALICE}"))"

bold "2. Execute a transfer"
KEY="transfer-${SUFFIX}"
BODY="{\"source_account_id\":\"${ALICE}\",\"destination_account_id\":\"${BOB}\",\"amount\":2500,\"currency\":\"USD\",\"description\":\"Invoice payment\",\"external_reference\":\"invoice-4821\"}"
TXN="$(post /v1/transfers "${KEY}" "${BODY}")"
TX_ID="$(printf '%s' "${TXN}" | json id)"
info "transaction ${TX_ID}"
info "alice=$(balance_of "${ALICE}") bob=$(balance_of "${BOB}")"

bold "3. Retry with the same idempotency key"
REPLAY="$(post /v1/transfers "${KEY}" "${BODY}")"
[ "$(printf '%s' "${REPLAY}" | json id)" = "${TX_ID}" ] \
  || fail "the retry returned a different transaction"
info "retry returned the original transaction ${TX_ID}"

bold "4. Prove only one ledger transaction exists"
info "alice=$(balance_of "${ALICE}") bob=$(balance_of "${BOB}")  (unchanged by the retry)"
[ "$(balance_of "${BOB}")" = "2500" ] || fail "the retry moved money twice"
# A different payload under the same key must be rejected outright.
CONFLICT="$(status_of /v1/transfers "${KEY}" \
  "{\"source_account_id\":\"${ALICE}\",\"destination_account_id\":\"${BOB}\",\"amount\":9999,\"currency\":\"USD\"}")"
[ "${CONFLICT}" = "409" ] || fail "expected 409 idempotency_conflict, got ${CONFLICT}"
info "reusing the key with a different payload -> 409 idempotency_conflict"

bold "5. Attempt concurrent overspending"
# Alice holds 97500; twenty concurrent 10000 transfers would need 200000.
for i in $(seq 1 20); do
  status_of /v1/transfers "overspend-${SUFFIX}-${i}" \
    "{\"source_account_id\":\"${ALICE}\",\"destination_account_id\":\"${BOB}\",\"amount\":10000,\"currency\":\"USD\"}" &
done > /tmp/ledger-demo-overspend-$$ 2>/dev/null
wait
ALICE_BAL="$(balance_of "${ALICE}")"
info "alice=${ALICE_BAL} bob=$(balance_of "${BOB}")"
[ "${ALICE_BAL}" -ge 0 ] || fail "INVARIANT 7 violated: the account went negative"
info "the account never went negative"
rm -f /tmp/ledger-demo-overspend-$$

bold "6. Simulate a lost response after commit"
LOST_KEY="lost-${SUFFIX}"
LOST_BODY="{\"source_account_id\":\"${ALICE}\",\"destination_account_id\":\"${BOB}\",\"amount\":1500,\"currency\":\"USD\",\"description\":\"Lost response\"}"
BEFORE="$(balance_of "${BOB}")"
LOST_STATUS="$(status_of /v1/transfers "${LOST_KEY}" "${LOST_BODY}" -H 'X-Failpoint: after_commit=error')"
info "the client saw HTTP ${LOST_STATUS} and does not know the outcome"
AFTER="$(balance_of "${BOB}")"
[ "${AFTER}" -eq "$((BEFORE + 1500))" ] || fail "the transaction did not actually commit"
info "but the ledger committed: bob ${BEFORE} -> ${AFTER}"

bold "7. Retry safely"
RETRIED="$(post /v1/transfers "${LOST_KEY}" "${LOST_BODY}")"
RETRIED_ID="$(printf '%s' "${RETRIED}" | json id)"
info "the retry returned the committed transaction ${RETRIED_ID}"
[ "$(balance_of "${BOB}")" -eq "${AFTER}" ] || fail "INVARIANT 6 violated: the retry moved money again"
info "balance unchanged by the retry: $(balance_of "${BOB}")"

bold "8. Publish the outbox event to Kafka"
# A dry-run replay is a convenient way to look up the event id the payment
# transaction wrote to the outbox, without publishing anything.
DRY="$(post /v1/replay "replay-dry-${SUFFIX}" "{\"transaction_id\":\"${TX_ID}\",\"dry_run\":true}")"
EVENT_ID="$(printf '%s' "${DRY}" | python3 -c "import sys,json;print(json.load(sys.stdin)['event_ids'][0])")"
info "transaction ${TX_ID} produced outbox event ${EVENT_ID}"

info "waiting for the outbox worker to publish it and the webhook worker to deliver it..."
for _ in $(seq 1 60); do
  [ "$(deliveries_of "${EVENT_ID}")" -ge 1 ] && break
  sleep 1
done
[ "$(deliveries_of "${EVENT_ID}")" -ge 1 ] \
  || fail "event ${EVENT_ID} never reached the webhook receiver"
info "delivered end to end: outbox -> Kafka -> webhook worker -> receiver"

bold "9. Simulate Kafka failure and recovery"
info "stopping kafka..."
docker compose stop kafka >/dev/null 2>&1 || info "(compose not available; skipping)"
DOWN_KEY="kafka-down-${SUFFIX}"
DOWN_STATUS="$(status_of /v1/transfers "${DOWN_KEY}" \
  "{\"source_account_id\":\"${ALICE}\",\"destination_account_id\":\"${BOB}\",\"amount\":700,\"currency\":\"USD\",\"description\":\"Kafka down\"}")"
[ "${DOWN_STATUS}" = "201" ] || fail "payments must keep working while Kafka is down (got ${DOWN_STATUS})"
info "payment accepted with Kafka down -> HTTP ${DOWN_STATUS}; the event queued in the outbox"
info "restarting kafka..."
docker compose start kafka >/dev/null 2>&1 || true
sleep 20
info "the outbox worker retries with exponential backoff and drains the queue"

bold "10. Replay the event"
# Snapshot immediately before replaying so step 9's payment cannot skew this.
UNIQUE_BEFORE="$(unique_events)"
BEFORE_REPLAY="$(deliveries_of "${EVENT_ID}")"

post /v1/replay "replay-${SUFFIX}" "{\"transaction_id\":\"${TX_ID}\"}" | pretty
info "replayed event ${EVENT_ID} under its ORIGINAL id"
sleep 5

AFTER_REPLAY="$(deliveries_of "${EVENT_ID}")"
UNIQUE_AFTER="$(unique_events)"
[ "${AFTER_REPLAY}" = "${BEFORE_REPLAY}" ] \
  || fail "the replay was reprocessed (${BEFORE_REPLAY} -> ${AFTER_REPLAY} deliveries)"
[ "${UNIQUE_AFTER}" = "${UNIQUE_BEFORE}" ] \
  || fail "the replay created a new event (${UNIQUE_BEFORE} -> ${UNIQUE_AFTER})"
info "the receiver deduplicated it: still ${AFTER_REPLAY} delivery, still ${UNIQUE_AFTER} unique events"

bold "11. Simulate webhook failure and retry"
curl -sS -X POST "${WEBHOOK}/fail?count=2" >/dev/null || true
info "the receiver will reject the next two deliveries with HTTP 500"
BEFORE_RETRY="$(unique_events)"
post /v1/transfers "webhook-retry-${SUFFIX}" \
  "{\"source_account_id\":\"${ALICE}\",\"destination_account_id\":\"${BOB}\",\"amount\":300,\"currency\":\"USD\",\"description\":\"Webhook retry\"}" >/dev/null
for _ in $(seq 1 40); do
  [ "$(unique_events)" -gt "${BEFORE_RETRY}" ] && break
  sleep 1
done
AFTER_RETRY="$(unique_events)"
[ "${AFTER_RETRY}" -gt "${BEFORE_RETRY}" ] \
  || fail "the webhook was never delivered after its transient failures"
info "delivered after two rejections and exponential backoff (${BEFORE_RETRY} -> ${AFTER_RETRY} unique events)"

bold "12. Reverse the payment"
post "/v1/transactions/${TX_ID}/reverse" "reverse-${SUFFIX}" '{"reason":"customer dispute"}' | pretty
info "a second reversal attempt is refused:"
SECOND="$(status_of "/v1/transactions/${TX_ID}/reverse" "reverse-again-${SUFFIX}" '{"reason":"again"}')"
[ "${SECOND}" = "409" ] || fail "expected 409 transaction_already_reversed, got ${SECOND}"
info "HTTP ${SECOND} transaction_already_reversed"

bold "13. Run reconciliation"
curl -sS "${API}/v1/reconciliation" | pretty
info "balances recomputed from the immutable entries"

bold "Demo complete"
info "alice=$(balance_of "${ALICE}") bob=$(balance_of "${BOB}") system=$(balance_of "${SYSTEM}")"
info "in a closed double-entry system every balance sums to zero"
