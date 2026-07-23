# OrderOps — Decision Log

## ADR-001 — Real processes, real HTTP, real per-service databases

**Context.** No Docker/K8s on the dev machine, but "microservices" faked
inside one process would gut the project.

**Decision.** Every service is its own binary with its OWN SQLite database
(`data/<service>.sqlite`) — the no-shared-database rule survives the
constraint. Services interact only via HTTP APIs and the event bus.
Tests wire real services over real HTTP (httptest) with real breakers.

**Consequences.** K8s manifests/Helm are authored-not-verified; the
operational proofs shift to what CAN run here: crash tests, chaos
compensation, load numbers.

## ADR-002 — Redis Streams as the event backbone (Kafka-shaped seam)

**Context.** The plan calls for Kafka; no Docker means no Kafka.

**Decision.** The `events` package is the seam: envelope schema, topics by
domain, consumer groups, pending-reclaim. Redis Streams implements it
today (portable binary, pattern proven in JobHive); a Kafka implementation
slots behind the same surface.

**Consequences.** No partitioning semantics locally; trace-through-bus and
at-least-once + dedupe — the interview substance — are fully exercised.

## ADR-003 — Transactional outbox + consumer dedupe, everywhere

**Context.** Services must never dual-write DB + bus: a crash between the
two either loses a fact or invents one.

**Decision.** State change and outbox row commit in ONE SQLite
transaction; a relay drains outbox→stream (at-least-once); every consumer
records `processed_events` in the SAME transaction as its effect
(exactly-once effect). Both crash windows are pinned by tests.

**Consequences.** Every service carries two extra tables and a relay
goroutine — the price of never losing a fact.

## ADR-004 — Hand-propagated W3C traceparent (no OTel collector yet)

**Context.** Jaeger needs a container. Tracing is core to the project's
story.

**Decision.** W3C `traceparent` propagated through HTTP headers and event
envelopes by `httpkit`; one trace id follows an order from gateway click
to notification event. A lightweight in-repo collector + waterfall UI is
planned; OTel SDK export can replace the hand-rolled layer without
touching call sites.

**Consequences.** No spans/timings yet — trace continuity first, depth
next.

## ADR-005 — Saga: commands are synchronous HTTP, facts are async events

**Context.** Orchestration-based saga needs timeouts, breakers and
immediate business rejections (insufficient stock, card declined) —
awkward over fire-and-forget events. But downstream consumers
(notification, analytics) must not couple to the orchestrator.

**Decision.** The order service COMMANDS inventory/payment over resilient
HTTP (retry + breaker + Permanent-vs-transient classification) and
publishes FACTS (order.created/confirmed/cancelled, stock.*, payment.*)
through the outbox. Compensations (release stock, refund) are themselves
idempotent commands.

**Consequences.** Two integration styles to explain — deliberately: "sync
for control flow, async for propagation" is the defensible line.

## ADR-006 — Failure classification decides compensation, not retries alone

**Context.** "Payment failed" means three different things.

**Decision.** 402 declined → final, compensate + cancel. 5xx/transport →
retry with backoff under the breaker; if still failing, compensate + cancel
with reason "payment unavailable" (holding a customer's stock hostage to a
dead PSP is the worse outcome). 4xx → permanent, never retried, never
trips the breaker (the dependency answered; the request was wrong).

**Consequences.** The scripted PSP (amounts ending 99 decline, 42 are
slow, PAYMENT_DOWN=1 kills it) makes every path demoable on demand.
