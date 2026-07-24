package tracing_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/rahul25012003/orderops/internal/platform/tracing"
)

func TestNopNeverPanics(t *testing.T) {
	tracing.Nop{}.Emit(tracing.Span{}) // must be a no-op, no crash
}

func TestHTTPExporterBatchesToCollector(t *testing.T) {
	var mu sync.Mutex
	var got []tracing.Span
	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			var batch []tracing.Span
			json.Unmarshal(body, &batch)
			mu.Lock()
			got = append(got, batch...)
			mu.Unlock()
			w.WriteHeader(202)
		}))
	defer srv.Close()

	exp := tracing.NewHTTPExporter(srv.URL)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go exp.Run(ctx)

	for i := 0; i < 5; i++ {
		exp.Emit(tracing.Span{TraceID: "t", SpanID: "s", Service: "x",
			Kind: "server"})
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if n >= 5 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("collector never received all spans (got %d/5)", len(got))
}

func TestEmitNeverBlocksWhenFull(t *testing.T) {
	// no Run goroutine draining: the buffer fills and further Emits must drop,
	// not deadlock. If Emit blocked, this test would hang and time out.
	exp := tracing.NewHTTPExporter("http://127.0.0.1:0")
	done := make(chan struct{})
	go func() {
		for i := 0; i < 10_000; i++ {
			exp.Emit(tracing.Span{SpanID: "s"})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Emit blocked when buffer full — telemetry must never backpressure")
	}
}
