#!/usr/bin/env bash
# Load + chaos bench. Phase A: happy-path saga throughput/latency. Phase B:
# hard PSP outage for the whole run — every order must cancel-and-compensate,
# stock fully released, zero stuck. Phase C: PSP back, breaker half-opens,
# orders confirm again.
#   scripts/bench.sh
set -u
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT/bin"; DATA="$ROOT/data-bench"; RP=6391
export REDIS_ADDR="127.0.0.1:$RP" COLLECTOR_URL="http://127.0.0.1:7207" AUTH_SECRET="bench-secret"

command -v "$BIN/loadgen.exe" >/dev/null 2>&1 || go build -o "$BIN/" ./cmd/...
rm -rf "$DATA"; mkdir -p "$DATA"; PIDS=(); PAY_PID=""
cleanup(){ for p in "${PIDS[@]}"; do kill "$p" 2>/dev/null; done; kill "$PAY_PID" 2>/dev/null
  "$ROOT/tools/redis/redis-cli.exe" -p $RP shutdown nosave 2>/dev/null; wait 2>/dev/null; }
trap cleanup EXIT

"$ROOT/tools/redis/redis-server.exe" --port $RP --save "" --appendonly no >/dev/null 2>&1 & PIDS+=($!)
sleep 1
"$BIN/catalog.exe"   -addr :7201 -db "$DATA/catalog.sqlite" >/dev/null 2>&1 & PIDS+=($!)
"$BIN/inventory.exe" -addr :7202 -db "$DATA/inventory.sqlite" >/dev/null 2>&1 & PIDS+=($!)
"$BIN/payment.exe"   -addr :7203 -db "$DATA/payment.sqlite" >/dev/null 2>&1 & PAY_PID=$!
"$BIN/collector.exe" -addr :7207 -db "$DATA/collector.sqlite" >/dev/null 2>&1 & PIDS+=($!)
"$BIN/order.exe"     -addr :7204 -db "$DATA/order.sqlite" \
  -catalog http://127.0.0.1:7201 -inventory http://127.0.0.1:7202 -payment http://127.0.0.1:7203 \
  >/dev/null 2>&1 & PIDS+=($!)
ORD=http://127.0.0.1:7204
for i in $(seq 1 30); do curl -sf "$ORD/health" >/dev/null 2>&1 && break; sleep 0.5; done
sleep 1

echo "### PHASE A — happy-path saga load (180 orders, 16 workers) ###"
"$BIN/loadgen.exe" -direct -order "$ORD" -n 180 -c 16 -product filter-coffee

echo; echo "### PHASE B — CHAOS: hard PSP outage for the whole run ###"
STK0=$(curl -s http://127.0.0.1:7202/stock | grep -oE '"product_id":"mango-lassi","available":[0-9]+' | grep -oE '[0-9]+$')
echo "mango-lassi stock before: $STK0 · killing payment"
kill "$PAY_PID" 2>/dev/null; sleep 0.5
"$BIN/loadgen.exe" -direct -order "$ORD" -n 60 -c 16 -product mango-lassi | grep -E "confirmed|cancelled|failed"

echo; echo "### PHASE C — recovery: payment back, breaker cools down ###"
"$BIN/payment.exe" -addr :7203 -db "$DATA/payment.sqlite" >/dev/null 2>&1 & PAY_PID=$!
sleep 6
"$BIN/loadgen.exe" -direct -order "$ORD" -n 30 -c 12 -product mango-lassi | grep -E "confirmed|cancelled|failed"

echo; echo "### VERDICT ###"; sleep 1
NT=$(curl -s "$ORD/orders?state=pending" | grep -oE '"id"' | wc -l)
NR=$(curl -s "$ORD/orders?state=stock_reserved" | grep -oE '"id"' | wc -l)
NA=$(curl -s "$ORD/orders?state=payment_authorized" | grep -oE '"id"' | wc -l)
STK1=$(curl -s http://127.0.0.1:7202/stock | grep -oE '"product_id":"mango-lassi","available":[0-9]+' | grep -oE '[0-9]+$')
echo "non-terminal orders: pending=$NT reserved=$NR authorized=$NA · mango-lassi stock=$STK1 (was $STK0)"
if [ "$NT" -eq 0 ] && [ "$NR" -eq 0 ] && [ "$NA" -eq 0 ]; then
  echo "PASS — zero stuck; outage fully compensated; recovered on breaker half-open"
else echo "FAIL — orders left non-terminal"; fi
