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

**Consequences.** Continuity shipped first; **depth followed** — real spans
with timings now exist (ADR-009), rendered as a waterfall in the ops console.

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

## ADR-007 — One gateway owns every cross-cutting concern

**Context.** Auth, rate limiting, TLS termination, static hosting and the
tracing entry point could be duplicated across services — or centralized.

**Decision.** A single edge gateway (`internal/gateway`) is the ONLY public
surface. It serves the two SPAs, verifies JWTs, rate-limits per client IP
(token bucket), reverse-proxies `/api/*` to internal services, and mints the
root trace. Internal services trust the network and stay focused on their
domain. Identity is passed downstream as `X-User-ID`/`X-User-Role` headers
the gateway sets AFTER verification (and overwrites, so a client can't spoof
them).

**Consequences.** The gateway is a availability chokepoint — hence 2 replicas
+ HPA in k8s. Its concerns are unit-tested in isolation (auth gate, spoof
rejection, rate-limit trip, fan-out aggregation).

## ADR-008 — Stateless JWT, hand-rolled HS256, verified at the edge

**Context.** Auth needs to scale without a session store, and the token
format shouldn't be a black box in a portfolio project.

**Decision.** The auth service signs HS256 JWTs; the gateway verifies them
LOCALLY with the shared secret — no auth round-trip per request. The JWT is
~50 lines (`internal/platform/token`), only HS256 is accepted (sidesteps
`alg:none` and algorithm-confusion), and signatures are checked in constant
time before any claim is trusted. Passwords use stdlib `crypto/pbkdf2`
(120k iterations, per-user salt) — no bcrypt dependency.

**Consequences.** Secret rotation invalidates all tokens (acceptable at this
scale; a JWKS/kid scheme is the seam for rotation-without-logout). Tamper and
expiry are unit-tested, including a privilege-escalation forgery attempt.

## ADR-009 — Real spans exported best-effort to an in-repo collector

**Context.** ADR-004 gave trace CONTINUITY (one id across hops) but no
timings. A Jaeger/OTel-collector needs containers.

**Decision.** Every HTTP hop emits a span (server-side in the middleware,
client-side in the resilient client) carrying trace id, span id and parent
span id. Spans are exported BEST-EFFORT: buffered channel, batched flush,
dropped on overflow or collector-down — telemetry NEVER blocks or fails a
real request. The collector stores spans in SQLite and reconstructs the
parent/child tree into a Jaeger-style waterfall. The `Exporter` interface is
the seam: an OTLP exporter slots in behind it untouched.

**Consequences.** Trace depth is real (p50/p95/p99 from server spans, per-hop
timings in the waterfall) without a single container. The best-effort
contract is a feature, not a shortcut: observability that can take down the
system it observes is worse than none.

## ADR-010 — Notification and collector are INDEPENDENT bus consumers

**Context.** Live UI updates and durable observability are different needs
with different failure tolerances.

**Decision.** Two services consume the same streams under DIFFERENT consumer
groups: `notification` (live SSE fan-out, in-memory, losing a frame on crash
is fine) and `collector` (durable event log + trace store). Independent
offsets mean a slow/restarting collector never stalls the live feed, and vice
versa — the textbook reason an event backbone beats point-to-point calls.

**Consequences.** Two consumers to run, but each scales and fails on its own
terms. Notification is deliberately non-durable (a live view); the collector
is the system of record for events.

## ADR-011 — Order ids are timestamp + monotonic counter, not timestamp alone

**Context.** The load generator surfaced ~14% of creates failing with 500
under concurrency.

**Decision.** Root cause: `ord-<UnixNano>` collided because Windows' clock
resolution let concurrent workers share a tick → duplicate primary key. Fix:
append a process-monotonic atomic counter (`ord-<nanos>-<seq>`), keeping
time-ordering while guaranteeing uniqueness regardless of clock granularity.
A regression test fires 60 concurrent creates and asserts unique ids.

**Consequences.** Ids are unique per process (sufficient for the single
order-service topology); a multi-writer order tier would need a node id or
ULID — the same seam JobHive uses. The broader lesson is logged: this is the
bug class unit tests miss and load tests catch.
