#!/usr/bin/env bash
# Boot the whole stack locally (no Docker) for manual browsing / demos.
#   scripts/run_all.sh
#   open http://127.0.0.1:7000       (storefront — sign in rahul@orderops.dev / demo1234)
#   open http://127.0.0.1:7000/ops   (ops console)
# Ctrl-C tears everything down.
set -u
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT/bin"; DATA="$ROOT/data"
export REDIS_ADDR="127.0.0.1:6379" COLLECTOR_URL="http://127.0.0.1:7007" AUTH_SECRET="dev-secret-change-me"

command -v "$BIN/gateway.exe" >/dev/null 2>&1 || go build -o "$BIN/" ./cmd/...
mkdir -p "$DATA"
PIDS=()
cleanup(){ echo; echo "stopping…"; for p in "${PIDS[@]}"; do kill "$p" 2>/dev/null; done
  "$ROOT/tools/redis/redis-cli.exe" -p 6379 shutdown nosave 2>/dev/null; wait 2>/dev/null; }
trap cleanup EXIT INT TERM

"$ROOT/tools/redis/redis-server.exe" --port 6379 --save "" --appendonly no >/dev/null 2>&1 & PIDS+=($!)
sleep 1
"$BIN/catalog.exe"      >/dev/null 2>&1 & PIDS+=($!)
"$BIN/inventory.exe"    >/dev/null 2>&1 & PIDS+=($!)
"$BIN/payment.exe"      >/dev/null 2>&1 & PIDS+=($!)
"$BIN/auth.exe"         >/dev/null 2>&1 & PIDS+=($!)
"$BIN/collector.exe"    >/dev/null 2>&1 & PIDS+=($!)
"$BIN/order.exe"        >/dev/null 2>&1 & PIDS+=($!)
"$BIN/notification.exe" >/dev/null 2>&1 & PIDS+=($!)
export AUTH_URL=http://127.0.0.1:7005 CATALOG_URL=http://127.0.0.1:7001 \
  INVENTORY_URL=http://127.0.0.1:7002 ORDER_URL=http://127.0.0.1:7004 \
  NOTIFICATION_URL=http://127.0.0.1:7006 COLLECTOR_URL_HTTP=http://127.0.0.1:7007
echo "stack up → http://127.0.0.1:7000  (ops: /ops).  Ctrl-C to stop."
"$BIN/gateway.exe"   # foreground; Ctrl-C triggers cleanup
