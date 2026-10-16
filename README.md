# OrderOps — Event-Driven Microservices Platform

A food-ordering backend built the way a real engineering org builds one:
independent services with their own databases, a saga-orchestrated
distributed transaction with compensation, a transactional outbox on an
event bus, resilience patterns on every call, JWT auth at a single gateway,
**real distributed tracing** rendered as a live waterfall, and an ops console
that streams the system's events as they happen — every guarantee pinned by
tests that run real services over real HTTP, and by load + chaos runs.

> **Status: complete — 8 services, 2 UIs, 43 tests, 11 ADRs, load + chaos proven.**

No Docker on the dev machine, so the microservice *principles* are honored
under the constraint (honest ADRs, not hand-waving): every service is its own
binary with its own database, talking only over HTTP and a Redis-Streams event
bus. Docker and k8s manifests are authored; the operational proofs run on what
*can* run here — crash recovery, chaos compensation, and load numbers.

## The system

```
  browser ──► GATEWAY :7000  ── the only public surface (ADR-007)
              • serves 2 SPAs (storefront + ops console, //go:embed)
              • JWT verify · per-IP rate limit · mints the root trace
              • reverse-proxies /api/* and injects traceparent downstream
                 │
    ┌────────────┼───────────────────────────────┬──────────────┐
    ▼            ▼                                 ▼              ▼
 AUTH :7005   ORDER :7004  ── SAGA ORCHESTRATOR   CATALOG :7001   (each its
 HS256 JWT    pending→reserved→authorized→        products         own SQLite,
 pbkdf2       confirmed / cancelled(+compensate)                   no sharing)
 (ADR-008)       │            │                   INVENTORY :7002  PAYMENT :7003
                 │ commands (resilient HTTP:       optimistic      scripted PSP:
                 │ retry∘breaker∘timeout,          stock holds     99 decline ·
                 ▼ 4xx permanent/5xx transient)    idempotent      42 slow ·
              facts ──► TRANSACTIONAL OUTBOX ──► REDIS STREAMS (Kafka-shaped)
                                                     │        │
                              ┌──────────────────────┘        └───────────────┐
                              ▼ (consumer group A)                (group B) ▼
                       NOTIFICATION :7006                        COLLECTOR :7007
                       live SSE fan-out                          durable event log +
                       (storefront + ops)                        SPAN ingest → trace
                                                                 waterfall + latency

  W3C traceparent + real SPANS follow one order across every hop (ADR-004/009);
  spans export BEST-EFFORT — telemetry never blocks a real request.
```

Eight services: **gateway, auth, catalog, inventory, payment, order,
notification, collector** — plus `loadgen`. Two independent bus consumers
(notification, collector) off the same streams (ADR-010).

## Two UIs (embedded in the gateway binary)

- **Storefront** (`/`) — sign in, browse the menu, build a cart, place an order,
  and watch the **saga advance live** (ingredients reserved → payment authorized
  → confirmed) over SSE, or see it cancel with a reason if stock/payment fails.
  Demo login: `rahul@orderops.dev` / `demo1234`.
- **Ops console** (`/ops`) — service health, live circuit-breaker states,
  throughput + p50/p95/p99 latency, inventory levels, a **live event stream**,
  and **recent traces you can click to open a distributed-trace waterfall** with
  per-hop timings and the business facts overlaid.

## What's proven

**43 tests** (`go test ./...`) + load/chaos (`scripts/bench.sh`). Full numbers:
[`docs/bench_results.md`](docs/bench_results.md).

- **~100 orders/s** end-to-end (place → terminal), **p50 ~130 ms / p99 ~230 ms**,
  0 failures over 180 orders.
- **Hard PSP outage** (payment killed for a whole run): **60/60 orders
  cancelled-and-compensated, stock fully released (100→100), zero stuck.**
  Then the **breaker half-opens** on recovery and orders confirm again.
- **Distributed trace propagation** asserted directly: a client span parents on
  the caller's server span; the downstream server span parents on the client
  span — one connected trace across the hop.
- **Saga**: happy · declined→compensate · dead-PSP→compensate · insufficient-stock
  (never calls payment) · crash-recovery · idempotent-replay.
- **Outbox**: rolled-back facts never publish; duplicate publishes absorbed by
  consumer dedupe; dead consumers' deliveries reclaimed.
- **Edge**: anonymous order 401 · JWT identity propagated & un-spoofable ·
  rate-limit trips under burst · ops overview fans out to 7 services.

A concurrency bug the load test earned (id collisions under a coarse Windows
clock) is fixed and regression-tested — see ADR-011.

## Quickstart

**No Docker (Windows):**
```bash
scripts/get_redis.ps1          # one-time: fetch the portable Redis binary
bash scripts/run_all.sh        # boots redis + all 8 services
# → http://127.0.0.1:7000  (storefront)   http://127.0.0.1:7000/ops  (ops)
bash scripts/smoke.sh          # full public-API end-to-end check
bash scripts/bench.sh          # load + chaos numbers
```

**With Docker — the whole system, one command:**
```bash
docker compose -f deploy/docker-compose.yaml up --build
```

**Kubernetes** (authored, ADR-001): `kubectl apply -f deploy/k8s/orderops.yaml`.

## Tech

Go 1.26 · net/http (stdlib routing, `//go:embed` UIs) · Redis Streams
(consumer groups, `XPENDING`/`XCLAIM` reclaim, Lua) · modernc.org/sqlite
(pure-Go, no cgo) · transactional outbox + consumer dedupe · saga orchestration
with compensations · circuit breaker + jittered retry + failure classification ·
W3C traceparent + best-effort span export + waterfall reconstruction ·
hand-rolled HS256 JWT + PBKDF2 · token-bucket rate limiting · SSE · Docker
(distroless static) · Kubernetes (Deployments/Services/HPA/Ingress/Secret).

## Design decisions

[`docs/decisions.md`](docs/decisions.md) — **11 ADRs**, each stating the
constraint honestly and what it does/doesn't prove on a no-Docker machine.
