package collector_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rahul25012003/orderops/internal/collector"
	"github.com/rahul25012003/orderops/internal/platform/tracing"
)

const base = int64(1_700_000_000_000_000_000) // fixed ns epoch for determinism

func ms(n int64) int64 { return n * 1_000_000 }

func newCollector(t *testing.T) (*collector.Service, *httptest.Server) {
	t.Helper()
	quiet := slog.New(slog.NewTextHandler(os.Stderr,
		&slog.HandlerOptions{Level: slog.LevelError}))
	s, err := collector.New(filepath.Join(t.TempDir(), "col.sqlite"), nil, quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	srv := httptest.NewServer(s.Router())
	t.Cleanup(srv.Close)
	return s, srv
}

// one realistic trace: gateway → order(server) → order(client) → inventory.
// Span ids are globally unique (prefixed by trace id), as real span ids are.
func sampleTrace(traceID string) []tracing.Span {
	id := func(s string) string { return traceID + "-" + s }
	return []tracing.Span{
		{TraceID: traceID, SpanID: id("g1"), ParentID: "", Service: "gateway",
			Name: "POST /api/orders", Kind: "server",
			StartNS: base, EndNS: base + ms(100), Status: 201},
		{TraceID: traceID, SpanID: id("o1"), ParentID: id("g1"), Service: "order",
			Name: "POST /orders", Kind: "server",
			StartNS: base + ms(5), EndNS: base + ms(90), Status: 201},
		{TraceID: traceID, SpanID: id("oc1"), ParentID: id("o1"), Service: "order",
			Name: "POST /reserve", Kind: "client",
			StartNS: base + ms(10), EndNS: base + ms(40), Status: 200},
		{TraceID: traceID, SpanID: id("i1"), ParentID: id("oc1"), Service: "inventory",
			Name: "POST /reserve", Kind: "server",
			StartNS: base + ms(12), EndNS: base + ms(38), Status: 200},
	}
}

func ingest(t *testing.T, url string, spans []tracing.Span) {
	t.Helper()
	body, _ := json.Marshal(spans)
	resp, err := http.Post(url+"/spans", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
}

func TestWaterfallReconstructsParentDepths(t *testing.T) {
	_, srv := newCollector(t)
	ingest(t, srv.URL, sampleTrace("trace-abc"))

	resp, err := http.Get(srv.URL + "/traces/trace-abc")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	var out struct {
		DurationMS float64 `json:"duration_ms"`
		Spans      []struct {
			SpanID     string  `json:"span_id"`
			Depth      int     `json:"depth"`
			OffsetMS   float64 `json:"offset_ms"`
			DurationMS float64 `json:"duration_ms"`
		} `json:"spans"`
	}
	json.NewDecoder(resp.Body).Decode(&out)

	if len(out.Spans) != 4 {
		t.Fatalf("got %d spans, want 4", len(out.Spans))
	}
	wantDepth := map[string]int{"trace-abc-g1": 0, "trace-abc-o1": 1,
		"trace-abc-oc1": 2, "trace-abc-i1": 3}
	for _, s := range out.Spans {
		if wantDepth[s.SpanID] != s.Depth {
			t.Fatalf("span %s depth = %d, want %d", s.SpanID, s.Depth,
				wantDepth[s.SpanID])
		}
	}
	if out.DurationMS < 99 || out.DurationMS > 101 {
		t.Fatalf("trace duration = %.1fms, want ~100", out.DurationMS)
	}
}

func TestTraceAnnotatedWithEvents(t *testing.T) {
	s, srv := newCollector(t)
	ingest(t, srv.URL, sampleTrace("trace-ev"))
	// a business fact recorded on the same trace, 20ms in
	at := time.Unix(0, base+ms(20)).UTC().Format(time.RFC3339Nano)
	s.DB.Exec(`INSERT INTO events
        (event_id, trace_id, type, source, order_id, occurred_at, data)
        VALUES (?, ?, ?, ?, ?, ?, ?)`,
		"e1", "trace-ev", "stock.reserved", "inventory", "ord-1", at, "{}")

	resp, err := http.Get(srv.URL + "/traces/trace-ev")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out struct {
		Events []struct {
			Type     string  `json:"type"`
			OffsetMS float64 `json:"offset_ms"`
		} `json:"events"`
	}
	json.NewDecoder(resp.Body).Decode(&out)
	if len(out.Events) != 1 || out.Events[0].Type != "stock.reserved" {
		t.Fatalf("events on trace: %+v", out.Events)
	}
	if out.Events[0].OffsetMS < 19 || out.Events[0].OffsetMS > 21 {
		t.Fatalf("event offset = %.1fms, want ~20", out.Events[0].OffsetMS)
	}
}

func TestStatsCountAndPercentiles(t *testing.T) {
	_, srv := newCollector(t)
	ingest(t, srv.URL, sampleTrace("trace-1"))
	ingest(t, srv.URL, sampleTrace("trace-2"))

	resp, err := http.Get(srv.URL + "/stats")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var st struct {
		Spans     int            `json:"spans"`
		Traces    int            `json:"traces"`
		LatencyMS map[string]float64 `json:"latency_ms"`
	}
	json.NewDecoder(resp.Body).Decode(&st)
	if st.Traces != 2 {
		t.Fatalf("traces = %d, want 2", st.Traces)
	}
	if st.Spans != 8 {
		t.Fatalf("spans = %d, want 8", st.Spans)
	}
	if st.LatencyMS["p99"] <= 0 {
		t.Fatalf("p99 latency not computed: %v", st.LatencyMS)
	}
}

func TestListTracesReportsRoot(t *testing.T) {
	_, srv := newCollector(t)
	ingest(t, srv.URL, sampleTrace("trace-root"))
	resp, err := http.Get(srv.URL + "/traces")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out struct {
		Traces []struct {
			TraceID string `json:"trace_id"`
			Root    string `json:"root"`
			Spans   int    `json:"spans"`
		} `json:"traces"`
	}
	json.NewDecoder(resp.Body).Decode(&out)
	if len(out.Traces) != 1 || out.Traces[0].Root != "gateway" {
		t.Fatalf("trace list root: %+v", out.Traces)
	}
}
