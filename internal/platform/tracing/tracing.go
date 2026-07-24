// Package tracing gives OrderOps real distributed-trace SPANS with timings
// — the "depth" ADR-004 deferred. Every HTTP hop (server-side and
// client-side) emits a span carrying the W3C trace id, its own span id and
// its parent's, so the collector can rebuild the parent/child tree and
// render a Jaeger-style waterfall of one order's journey.
//
// The exporter is BEST-EFFORT by construction: spans go onto a buffered
// channel and are flushed in batches by a background goroutine. If the
// buffer is full or the collector is down, spans are dropped — never does
// telemetry block or fail a real request. This is the OTel export contract
// in miniature; an OTLP exporter slots in behind the same Exporter surface.
package tracing

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"time"
)

// Span is one timed operation in a trace.
type Span struct {
	TraceID  string `json:"trace_id"`
	SpanID   string `json:"span_id"`
	ParentID string `json:"parent_id,omitempty"`
	Service  string `json:"service"`
	Name     string `json:"name"` // e.g. "GET /products/{id}"
	Kind     string `json:"kind"` // server | client
	StartNS  int64  `json:"start_ns"`
	EndNS    int64  `json:"end_ns"`
	Status   int    `json:"status"`
	Err      string `json:"error,omitempty"`
}

// Exporter receives finished spans. Emit MUST NOT block.
type Exporter interface {
	Emit(Span)
}

// Nop discards spans (the default when tracing is disabled — tests, or any
// service started without a COLLECTOR_URL).
type Nop struct{}

// Emit does nothing.
func (Nop) Emit(Span) {}

// NewSpanID mints an 8-byte (16 hex) span id, per W3C traceparent.
func NewSpanID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// HTTPExporter batches spans to a collector over HTTP.
type HTTPExporter struct {
	url  string
	ch   chan Span
	http *http.Client
}

// NewHTTPExporter targets collectorURL (its /spans endpoint). Call Run in a
// goroutine to start flushing.
func NewHTTPExporter(collectorURL string) *HTTPExporter {
	return &HTTPExporter{
		url:  collectorURL + "/spans",
		ch:   make(chan Span, 2048),
		http: &http.Client{Timeout: 2 * time.Second},
	}
}

// Emit enqueues a span, dropping it if the buffer is full — telemetry never
// applies backpressure to the request path.
func (e *HTTPExporter) Emit(s Span) {
	select {
	case e.ch <- s:
	default: // buffer full: drop
	}
}

// Run flushes batched spans until ctx is cancelled.
func (e *HTTPExporter) Run(ctx context.Context) {
	t := time.NewTicker(250 * time.Millisecond)
	defer t.Stop()
	batch := make([]Span, 0, 128)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		body, err := json.Marshal(batch)
		if err == nil {
			if req, err := http.NewRequestWithContext(ctx, http.MethodPost,
				e.url, bytes.NewReader(body)); err == nil {
				req.Header.Set("Content-Type", "application/json")
				if resp, err := e.http.Do(req); err == nil {
					resp.Body.Close()
				}
			}
		}
		batch = batch[:0]
	}
	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case s := <-e.ch:
			batch = append(batch, s)
			if len(batch) >= 128 {
				flush()
			}
		case <-t.C:
			flush()
		}
	}
}
