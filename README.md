# OrderOps — Event-Driven Microservices Platform

A food-ordering backend built the way a real engineering org builds it:
independent services with their own databases, saga-orchestrated
distributed transactions with compensation, a transactional outbox on an
event bus, resilience patterns on every call — and every guarantee pinned
by tests that run real services over real HTTP.

> Mission 2030 · Project 7 (build order #4). Plan and tracker:
> [`../PROJECT_MASTER_PLAN.md`](../PROJECT_MASTER_PLAN.md)
> **Status: Phases 0–2 complete (foundation + saga core). In progress.**

## Architecture (current slice)

```
                     ┌──────────► CATALOG :7001  products, own SQLite, cache
 client ─► (gateway  │
    + storefront UI, │  commands: resilient HTTP (retry ∘ breaker ∘ timeout,
      planned next)  │  4xx permanent / 5xx transient — ADR-006)
                     ▼
              ORDER :7004 — SAGA ORCHESTRATOR
              pending → stock_reserved → payment_authorized → confirmed
                 └── compensations (release stock) ──► cancelled(reason)
              persisted state machine · step timeline · crash recovery loop
                     │                       │
                     ▼                       ▼
              INVENTORY :7002          PAYMENT :7003 (mock PSP)
              optimistic stock         scripted failures: amounts ending
              holds, idempotent        99 decline · 42 slow · PAYMENT_DOWN=1
              by order id              dead — idempotent auth by order id

  FACTS (order.created/confirmed/cancelled, stock.*, payment.*) leave every
  service through a TRANSACTIONAL OUTBOX → Redis Streams event bus
  (Kafka-shaped seam, ADR-002) → consumers dedupe in-transaction
  (at-least-once delivery, exactly-once effect, ADR-003)

  W3C traceparent follows the order through every HTTP hop and event
  envelope (ADR-004)
```

## Proven so far (12 tests, all real-HTTP end-to-end)

- happy path confirms and decrements stock, facts in order
- declined payment **compensates the reservation** and cancels with reason
- dead PSP (chaos env) → retries → breaker → compensation → cancelled
- insufficient stock rejects **without ever calling payment**
- saga **crash recovery**: a pending order left by a dead process resumes
  to confirmed
- saga **replay is idempotent**: double-running never double-charges or
  double-decrements
- outbox is transactional (rolled-back facts never publish); duplicate
  publishes are absorbed by consumer dedupe; dead consumers' deliveries
  are reclaimed

## Quickstart (Windows, no Docker)

```powershell
.\scripts\get_redis.ps1
.\tools\redis\redis-server.exe --port 6379   # bus
go build -o bin/ ./cmd/...
bin\catalog.exe    # :7001
bin\inventory.exe  # :7002
bin\payment.exe    # :7003
bin\order.exe      # :7004

curl -X POST localhost:7004/orders -d '{"customer":"rahul","items":[{"product_id":"margherita","qty":2}]}'
curl localhost:7004/orders/<id>    # order + live saga timeline
```

## Coming next (per the master plan)

Gateway + auth (JWT) + storefront UI (live saga progress) · notification
service + SSE · trace collector + waterfall UI · ops dashboard · chaos
suite + load numbers · k8s manifests (authored) · README essay + video.

## Design decisions

[`docs/decisions.md`](docs/decisions.md) — 6 ADRs so far.
