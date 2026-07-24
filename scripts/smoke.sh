#!/usr/bin/env bash
# Full-stack smoke test: boots Redis + all 7 services behind the gateway and
# drives real orders through the PUBLIC API, verifying auth, the happy path,
# a business rejection, distributed tracing, and ops aggregation.
#   scripts/smoke.sh
set -u
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT/bin"; DATA="$ROOT/data-smoke"; RP=6390
GW="http://127.0.0.1:7100"; SECRET="smoke-secret"
export REDIS_ADDR="127.0.0.1:$RP" COLLECTOR_URL="http://127.0.0.1:7107" AUTH_SECRET="$SECRET"

command -v "$BIN/gateway.exe" >/dev/null 2>&1 || go build -o "$BIN/" ./cmd/...
rm -rf "$DATA"; mkdir -p "$DATA"; PIDS=()
cleanup(){ for p in "${PIDS[@]}"; do kill "$p" 2>/dev/null; done
  "$ROOT/tools/redis/redis-cli.exe" -p $RP shutdown nosave 2>/dev/null; wait 2>/dev/null; }
trap cleanup EXIT

"$ROOT/tools/redis/redis-server.exe" --port $RP --save "" --appendonly no >/dev/null 2>&1 & PIDS+=($!)
sleep 1
"$BIN/catalog.exe"   -addr :7101 -db "$DATA/catalog.sqlite" >/dev/null 2>&1 & PIDS+=($!)
"$BIN/inventory.exe" -addr :7102 -db "$DATA/inventory.sqlite" >/dev/null 2>&1 & PIDS+=($!)
"$BIN/payment.exe"   -addr :7103 -db "$DATA/payment.sqlite" >/dev/null 2>&1 & PIDS+=($!)
"$BIN/auth.exe"      -addr :7105 -db "$DATA/auth.sqlite" -secret "$SECRET" >/dev/null 2>&1 & PIDS+=($!)
"$BIN/collector.exe" -addr :7107 -db "$DATA/collector.sqlite" >/dev/null 2>&1 & PIDS+=($!)
"$BIN/order.exe"     -addr :7104 -db "$DATA/order.sqlite" \
  -catalog http://127.0.0.1:7101 -inventory http://127.0.0.1:7102 -payment http://127.0.0.1:7103 \
  >/dev/null 2>&1 & PIDS+=($!)
"$BIN/notification.exe" -addr :7106 >/dev/null 2>&1 & PIDS+=($!)
export AUTH_URL=http://127.0.0.1:7105 CATALOG_URL=http://127.0.0.1:7101 \
  INVENTORY_URL=http://127.0.0.1:7102 ORDER_URL=http://127.0.0.1:7104 \
  NOTIFICATION_URL=http://127.0.0.1:7106 COLLECTOR_URL_HTTP=http://127.0.0.1:7107
"$BIN/gateway.exe" -addr :7100 -secret "$SECRET" >/dev/null 2>&1 & PIDS+=($!)

for i in $(seq 1 30); do curl -sf "$GW/health" >/dev/null 2>&1 && break; sleep 0.5; done
sleep 1
fail(){ echo "SMOKE FAIL: $1"; exit 1; }
oid(){ grep -oE '"id":"[^"]*"' | head -1 | sed 's/"id":"//;s/"//'; }
state(){ curl -s "$GW/api/orders/$1" | grep -oE '"state":"[^"]*"' | head -1 | sed 's/"state":"//;s/"//'; }

TOK=$(curl -s -X POST "$GW/api/auth/login" -d '{"id":"rahul@orderops.dev","password":"demo1234"}' \
  | grep -oE '"token":"[^"]*"' | sed 's/"token":"//;s/"//')
[ -n "$TOK" ] || fail "no token"; echo "1. login OK"

CODE=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$GW/api/orders" -d '{}')
[ "$CODE" = "401" ] || fail "anon order got $CODE"; echo "2. anonymous order blocked (401)"

O=$(curl -s -X POST "$GW/api/orders" -H "Authorization: Bearer $TOK" \
  -d '{"customer":"Rahul","items":[{"product_id":"margherita","qty":2}]}' | oid)
for i in $(seq 1 40); do S=$(state "$O"); [ "$S" = confirmed ] && break; [ "$S" = cancelled ] && break; sleep 0.25; done
[ "$S" = confirmed ] || fail "happy order = $S"; echo "3. happy path CONFIRMED"

O2=$(curl -s -X POST "$GW/api/orders" -H "Authorization: Bearer $TOK" \
  -d '{"customer":"Rahul","items":[{"product_id":"ramen-tonkotsu","qty":4}]}' | oid)
for i in $(seq 1 40); do S2=$(state "$O2"); [ "$S2" = cancelled ] && break; [ "$S2" = confirmed ] && break; sleep 0.25; done
[ "$S2" = cancelled ] || fail "insufficient-stock order = $S2"; echo "4. insufficient stock CANCELLED"

sleep 1
NT=$(curl -s "$GW/api/traces" | grep -oE '"trace_id"' | wc -l)
[ "$NT" -ge 1 ] || fail "no traces"; echo "5. $NT distributed traces captured"

NH=$(curl -s "$GW/api/ops/overview" | grep -oE '"healthy":true' | wc -l)
[ "$NH" -ge 6 ] || fail "only $NH healthy"; echo "6. $NH services healthy in ops overview"

echo; echo "====== SMOKE TEST PASSED ======"
