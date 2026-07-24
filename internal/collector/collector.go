// Package collector is the observability backend. It does two things a
// Jaeger + a Kafka-sink would do in production, in one small Go service:
//
//	1. INGESTS spans (POST /spans) from every service's tracing exporter and
//	   reconstructs the parent/child tree into a waterfall for one trace.
//	2. CONSUMES the bus as its OWN group (durable event log) so the ops
//	   dashboard has a persistent tail and each trace can be annotated with
//	   the business facts that flowed while it ran.
//
// The waterfall the UI renders is a REAL distributed trace: a gateway click
// → order → inventory/payment spans, timed, nested by span parentage. This
// is the "depth" ADR-004 promised once continuity was in place.
package collector

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sort"
	"time"

	"github.com/rahul25012003/orderops/internal/platform/db"
	"github.com/rahul25012003/orderops/internal/platform/events"
	"github.com/rahul25012003/orderops/internal/platform/httpkit"
	"github.com/rahul25012003/orderops/internal/platform/tracing"
)

const schema = `
CREATE TABLE IF NOT EXISTS spans (
    span_id   TEXT PRIMARY KEY,
    trace_id  TEXT NOT NULL,
    parent_id TEXT NOT NULL DEFAULT '',
    service   TEXT NOT NULL,
    name      TEXT NOT NULL,
    kind      TEXT NOT NULL,
    start_ns  INTEGER NOT NULL,
    end_ns    INTEGER NOT NULL,
    status    INTEGER NOT NULL DEFAULT 0,
    err       TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_spans_trace ON spans(trace_id);
CREATE INDEX IF NOT EXISTS idx_spans_start ON spans(start_ns);
CREATE TABLE IF NOT EXISTS events (
    seq         INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id    TEXT NOT NULL UNIQUE,
    trace_id    TEXT NOT NULL DEFAULT '',
    type        TEXT NOT NULL,
    source      TEXT NOT NULL,
    order_id    TEXT NOT NULL DEFAULT '',
    occurred_at TEXT NOT NULL,
    data        TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_trace ON events(trace_id);
CREATE INDEX IF NOT EXISTS idx_events_seq ON events(seq);
`

var topics = []string{"orderops:order", "orderops:stock", "orderops:payment"}

const group = "collector"

// Service is the collector HTTP + consumer service.
type Service struct {
	DB     *sql.DB
	Bus    *events.Bus
	Log    *slog.Logger
	Tracer tracing.Exporter
}

// New opens the store around an already-connected bus.
func New(dbPath string, bus *events.Bus, log *slog.Logger) (*Service, error) {
	d, err := db.Open(dbPath, schema)
	if err != nil {
		return nil, err
	}
	return &Service{DB: d, Bus: bus, Log: log}, nil
}

func (s *Service) Close() error { return s.DB.Close() }

func (s *Service) Router() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		httpkit.WriteJSON(w, 200, map[string]string{"status": "ok",
			"service": "collector"})
	})
	mux.HandleFunc("POST /spans", s.ingestSpans)
	mux.HandleFunc("GET /traces", s.listTraces)
	mux.HandleFunc("GET /traces/{id}", s.getTrace)
	mux.HandleFunc("GET /events", s.recentEvents)
	mux.HandleFunc("GET /stats", s.stats)
	// the collector is never traced by itself — that would be a feedback loop
	return httpkit.Middleware("collector", s.Log, nil, mux)
}

// Run consumes the bus into the durable event log until ctx cancels.
func (s *Service) Run(ctx context.Context) {
	consumer := fmt.Sprintf("collector-%d", os.Getpid())
	for {
		if ctx.Err() != nil {
			return
		}
		d, err := s.Bus.Consume(ctx, topics, group, consumer, time.Second)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.logger().Warn("consume", "err", err)
			time.Sleep(200 * time.Millisecond)
			continue
		}
		if d == nil {
			continue
		}
		s.storeEvent(ctx, d)
		s.Bus.Ack(ctx, group, d)
	}
}

func (s *Service) storeEvent(ctx context.Context, d *events.Delivery) {
	var m map[string]any
	json.Unmarshal(d.Data, &m)
	orderID, _ := m["order_id"].(string)
	if orderID == "" {
		orderID, _ = m["id"].(string)
	}
	s.DB.ExecContext(ctx, `
        INSERT OR IGNORE INTO events
            (event_id, trace_id, type, source, order_id, occurred_at, data)
        VALUES (?, ?, ?, ?, ?, ?, ?)`,
		d.ID, d.TraceID, d.Type, d.Source, orderID,
		d.OccurredAt.Format(time.RFC3339Nano), string(d.Data))
}

// --- span ingest ------------------------------------------------------------

func (s *Service) ingestSpans(w http.ResponseWriter, r *http.Request) {
	var batch []tracing.Span
	if !httpkit.Decode(w, r, &batch) {
		return
	}
	for _, sp := range batch {
		s.DB.ExecContext(r.Context(), `
            INSERT OR IGNORE INTO spans (span_id, trace_id, parent_id, service,
                name, kind, start_ns, end_ns, status, err)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			sp.SpanID, sp.TraceID, sp.ParentID, sp.Service, sp.Name, sp.Kind,
			sp.StartNS, sp.EndNS, sp.Status, sp.Err)
	}
	httpkit.WriteJSON(w, 202, map[string]int{"ingested": len(batch)})
}

// --- trace queries ----------------------------------------------------------

type traceSummary struct {
	TraceID    string  `json:"trace_id"`
	Root       string  `json:"root"`
	Name       string  `json:"name"`
	StartNS    int64   `json:"start_ns"`
	DurationMS float64 `json:"duration_ms"`
	Spans      int     `json:"spans"`
	Errors     int     `json:"errors"`
}

func (s *Service) listTraces(w http.ResponseWriter, r *http.Request) {
	rows, err := s.DB.QueryContext(r.Context(), `
        SELECT trace_id, MIN(start_ns), MAX(end_ns), COUNT(*),
               SUM(CASE WHEN status >= 500 OR (status = 0 AND err <> '')
                        THEN 1 ELSE 0 END)
        FROM spans GROUP BY trace_id
        ORDER BY MIN(start_ns) DESC LIMIT 50`)
	if err != nil {
		httpkit.Error(w, 500, "storage failure")
		return
	}
	out := []traceSummary{}
	for rows.Next() {
		var ts traceSummary
		var start, end int64
		rows.Scan(&ts.TraceID, &start, &end, &ts.Spans, &ts.Errors)
		ts.StartNS = start
		ts.DurationMS = float64(end-start) / 1e6
		out = append(out, ts)
	}
	rows.Close() // free the single connection before per-trace enrichment
	// (SetMaxOpenConns(1): a query inside the open rows loop would deadlock)
	for i := range out {
		out[i].Root, out[i].Name = s.rootOf(r.Context(), out[i].TraceID)
	}
	httpkit.WriteJSON(w, 200, map[string]any{"traces": out})
}

// rootOf returns the (service, name) of a trace's earliest server span.
func (s *Service) rootOf(ctx context.Context, traceID string) (string, string) {
	var svcName, name string
	s.DB.QueryRowContext(ctx, `
        SELECT service, name FROM spans
        WHERE trace_id = ? ORDER BY start_ns LIMIT 1`, traceID).
		Scan(&svcName, &name)
	return svcName, name
}

type waterfallSpan struct {
	SpanID     string  `json:"span_id"`
	ParentID   string  `json:"parent_id"`
	Service    string  `json:"service"`
	Name       string  `json:"name"`
	Kind       string  `json:"kind"`
	Depth      int     `json:"depth"`
	OffsetMS   float64 `json:"offset_ms"`
	DurationMS float64 `json:"duration_ms"`
	Status     int     `json:"status"`
	Err        string  `json:"err,omitempty"`
}

type traceEvent struct {
	Type     string  `json:"type"`
	Source   string  `json:"source"`
	OrderID  string  `json:"order_id"`
	OffsetMS float64 `json:"offset_ms"`
}

func (s *Service) getTrace(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	rows, err := s.DB.QueryContext(r.Context(), `
        SELECT span_id, parent_id, service, name, kind, start_ns, end_ns,
               status, err
        FROM spans WHERE trace_id = ? ORDER BY start_ns`, id)
	if err != nil {
		httpkit.Error(w, 500, "storage failure")
		return
	}
	defer rows.Close()

	type raw struct {
		span, parent, service, name, kind, err string
		start, end                             int64
		status                                 int
	}
	var spans []raw
	var t0 int64
	byID := map[string]bool{}
	for rows.Next() {
		var x raw
		rows.Scan(&x.span, &x.parent, &x.service, &x.name, &x.kind, &x.start,
			&x.end, &x.status, &x.err)
		if t0 == 0 || x.start < t0 {
			t0 = x.start
		}
		byID[x.span] = true
		spans = append(spans, x)
	}
	if len(spans) == 0 {
		httpkit.Error(w, 404, "no such trace")
		return
	}

	depthOf := map[string]int{}
	var depth func(id, parent string) int
	depth = func(id, parent string) int {
		if d, ok := depthOf[id]; ok {
			return d
		}
		if parent == "" || !byID[parent] {
			depthOf[id] = 0
			return 0
		}
		// parent's depth is already computed (spans are start-ordered, and a
		// parent span always starts before its child)
		d := depthOf[parent] + 1
		depthOf[id] = d
		return d
	}

	out := make([]waterfallSpan, 0, len(spans))
	var maxEnd int64
	for _, x := range spans {
		if x.end > maxEnd {
			maxEnd = x.end
		}
		out = append(out, waterfallSpan{
			SpanID: x.span, ParentID: x.parent, Service: x.service,
			Name: x.name, Kind: x.kind, Depth: depth(x.span, x.parent),
			OffsetMS:   float64(x.start-t0) / 1e6,
			DurationMS: float64(x.end-x.start) / 1e6,
			Status:     x.status, Err: x.err,
		})
	}

	// annotate with the business facts of this trace
	evRows, _ := s.DB.QueryContext(r.Context(), `
        SELECT type, source, order_id, occurred_at FROM events
        WHERE trace_id = ? ORDER BY occurred_at`, id)
	var evs []traceEvent
	if evRows != nil {
		for evRows.Next() {
			var te traceEvent
			var at string
			evRows.Scan(&te.Type, &te.Source, &te.OrderID, &at)
			if ts, err := time.Parse(time.RFC3339Nano, at); err == nil {
				te.OffsetMS = float64(ts.UnixNano()-t0) / 1e6
			}
			evs = append(evs, te)
		}
		evRows.Close()
	}

	httpkit.WriteJSON(w, 200, map[string]any{
		"trace_id":    id,
		"start_ns":    t0,
		"duration_ms": float64(maxEnd-t0) / 1e6,
		"spans":       out,
		"events":      evs,
	})
}

func (s *Service) recentEvents(w http.ResponseWriter, r *http.Request) {
	rows, err := s.DB.QueryContext(r.Context(), `
        SELECT trace_id, type, source, order_id, occurred_at
        FROM events ORDER BY seq DESC LIMIT 50`)
	if err != nil {
		httpkit.Error(w, 500, "storage failure")
		return
	}
	defer rows.Close()
	type ev struct {
		TraceID string `json:"trace_id"`
		Type    string `json:"type"`
		Source  string `json:"source"`
		OrderID string `json:"order_id"`
		At      string `json:"at"`
	}
	out := []ev{}
	for rows.Next() {
		var e ev
		rows.Scan(&e.TraceID, &e.Type, &e.Source, &e.OrderID, &e.At)
		out = append(out, e)
	}
	httpkit.WriteJSON(w, 200, map[string]any{"events": out})
}

// --- stats ------------------------------------------------------------------

func (s *Service) stats(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var totalSpans, traces, totalEvents, errs int
	s.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM spans").Scan(&totalSpans)
	s.DB.QueryRowContext(ctx,
		"SELECT COUNT(DISTINCT trace_id) FROM spans").Scan(&traces)
	s.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM events").Scan(&totalEvents)
	// a real error is a 5xx or a transport failure (no response) — a 4xx is a
	// business rejection (insufficient stock, declined card), not a fault
	s.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM spans
        WHERE status >= 500 OR (status = 0 AND err <> '')`).Scan(&errs)

	// server-span latency percentiles over the recent window
	rows, _ := s.DB.QueryContext(ctx, `
        SELECT (end_ns - start_ns) FROM spans WHERE kind = 'server'
        ORDER BY start_ns DESC LIMIT 1000`)
	var durs []float64
	if rows != nil {
		for rows.Next() {
			var ns int64
			rows.Scan(&ns)
			durs = append(durs, float64(ns)/1e6)
		}
		rows.Close()
	}
	sort.Float64s(durs)

	perSvc := map[string]int{}
	svcRows, _ := s.DB.QueryContext(ctx,
		"SELECT service, COUNT(*) FROM spans GROUP BY service")
	if svcRows != nil {
		for svcRows.Next() {
			var name string
			var n int
			svcRows.Scan(&name, &n)
			perSvc[name] = n
		}
		svcRows.Close()
	}

	httpkit.WriteJSON(w, 200, map[string]any{
		"spans":       totalSpans,
		"traces":      traces,
		"events":      totalEvents,
		"errors":      errs,
		"per_service": perSvc,
		"latency_ms": map[string]float64{
			"p50": pct(durs, 0.50),
			"p95": pct(durs, 0.95),
			"p99": pct(durs, 0.99),
		},
	})
}

func pct(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	i := int(q * float64(len(sorted)-1))
	return sorted[i]
}

func (s *Service) logger() *slog.Logger {
	if s.Log != nil {
		return s.Log
	}
	return slog.Default()
}
