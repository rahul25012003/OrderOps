# OrderOps — Load & Chaos Results

Reproduce: `scripts/bench.sh` (load + chaos), `scripts/smoke.sh` (public-API
end-to-end). Numbers below are from a Windows 11 laptop, Go 1.26.5, portable
Redis 5.0.14, every service on its own SQLite (single writer). Latencies are
**end-to-end saga times** — order placed → polled to a terminal state — not
ingest-ack times.

## Phase A — happy-path throughput (180 orders, 16 workers, direct to saga)

| metric | value |
|---|---|
| confirmed | **180 / 180** |
| failed / stuck | **0** |
| throughput | **~100–113 orders/s** end-to-end to terminal |
| saga latency p50 | **~130 ms** |
| saga latency p95 | **~210 ms** |
| saga latency p99 | **~230 ms** |
| max | ~285 ms |

Each confirmed order is a full saga: server-side pricing (→ catalog),
reserve (→ inventory), authorize (→ payment), confirm — four services, three
resilient HTTP hops, four outbox facts, all traced. The load generator hits
the **order service directly**, on purpose: the gateway's rate limiter is a
separately-tested edge concern (you don't throttle your own load test).

## Phase B — hard PSP outage for the ENTIRE run (60 orders, payment killed)

| metric | value |
|---|---|
| confirmed | 0 |
| **cancelled (compensated)** | **60 / 60** |
| failed / stuck | **0** |
| stock before → after | **100 → 100** (every hold released) |

With payment dead, each order retries under the breaker, the breaker opens,
and the saga **compensates** — releases the stock reservation and cancels
with reason `payment unavailable`. No order hangs; no stock is leaked. The
inventory count returning exactly to its starting value is the machine-checked
proof that every compensation ran.

## Phase C — recovery (payment restored, 30 orders)

| metric | value |
|---|---|
| confirmed | **30 / 30** |
| stock before → after | 100 → **70** |

After the 5-second breaker cooldown the breaker goes **half-open**, admits a
probe, sees payment healthy, and closes — orders confirm again. Stock drops by
exactly 30. The breaker's open → half-open → closed cycle is visible in the
run, not just asserted.

## The bug the load test earned

The first load run reported ~14% `place status 500`. Cause: order ids were
`ord-<UnixNano>` and Windows' coarse clock let concurrent workers mint the
**same id in one tick** → duplicate primary key → 500. Fix: a monotonic
per-process counter appended to the timestamp (`internal/order/order.go`),
plus a regression test (`TestConcurrentCreatesGetUniqueIDs`) that fires 60
concurrent creates and asserts unique ids and zero failures. Post-fix:
**0 failures.** This is exactly the class of bug unit tests miss and load
tests catch.

## Correctness suite

`go test ./...` — 43 tests. Highlights beyond the numbers above:

- **Trace propagation** (`httpkit`): a client call under a server span emits a
  client span parented on it, and the downstream server span parents on the
  client span — one connected trace across the hop, asserted directly.
- **Saga paths** (`order`): happy, declined-compensation, dead-PSP-compensation,
  insufficient-stock (never calls payment), crash-recovery, idempotent-replay.
- **Outbox** (`events`): rolled-back facts never publish; duplicate publishes
  absorbed by consumer dedupe; dead consumers' deliveries reclaimed.
- **Gateway**: anonymous order 401; valid JWT carries identity + injected
  traceparent to the upstream; client cannot spoof `X-User-ID`; rate limiter
  trips under burst; ops overview fans out to 7 services.
- **Collector**: waterfall reconstructs parent depths; trace annotated with
  business facts; latency percentiles.

CI (`.github/workflows/ci.yml`) runs `go vet` + `go test -race` against a
Redis 7 service container. `-race` needs cgo, which this machine lacks, so the
race detector runs in CI, not locally.
