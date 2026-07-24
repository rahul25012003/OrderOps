package httpkit_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/rahul25012003/orderops/internal/platform/httpkit"
	"github.com/rahul25012003/orderops/internal/platform/tracing"
)

// capture is an Exporter that records spans for assertions.
type capture struct {
	mu    sync.Mutex
	spans []tracing.Span
}

func (c *capture) Emit(s tracing.Span) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.spans = append(c.spans, s)
}

func (c *capture) find(kind string) (tracing.Span, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, s := range c.spans {
		if s.Kind == kind {
			return s, true
		}
	}
	return tracing.Span{}, false
}

// A client call made under a server-span context must (1) emit a client span
// parented on that server span, and (2) cause the downstream server span to
// parent on the client span — one connected trace across the hop.
func TestTracePropagatesParentChainAcrossHop(t *testing.T) {
	cap := &capture{}
	downstream := httptest.NewServer(httpkit.Middleware("down", nil, cap,
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			httpkit.WriteJSON(w, 200, map[string]string{"ok": "yes"})
		})))
	defer downstream.Close()

	client := httpkit.NewClient(2 * time.Second)
	client.Service = "up"
	client.Tracer = cap

	// simulate being inside "up"'s request handler
	traceID := httpkit.NewTraceID()
	upSpan := tracing.NewSpanID()
	ctx := httpkit.WithSpan(httpkit.WithTrace(context.Background(), traceID), upSpan)

	if err := client.GetJSON(ctx, downstream.URL, "/", nil); err != nil {
		t.Fatalf("call: %v", err)
	}

	// let the downstream server span flush (Emit is synchronous here)
	var cs, ss tracing.Span
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		var okc, oks bool
		cs, okc = cap.find("client")
		ss, oks = cap.find("server")
		if okc && oks {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if cs.SpanID == "" || ss.SpanID == "" {
		t.Fatal("missing client or server span")
	}
	if cs.TraceID != traceID || ss.TraceID != traceID {
		t.Fatalf("trace id not shared: client=%s server=%s want=%s",
			cs.TraceID, ss.TraceID, traceID)
	}
	if cs.ParentID != upSpan {
		t.Fatalf("client span parent = %s, want caller span %s", cs.ParentID, upSpan)
	}
	if ss.ParentID != cs.SpanID {
		t.Fatalf("downstream span parent = %s, want client span %s",
			ss.ParentID, cs.SpanID)
	}
	if cs.Service != "up" || ss.Service != "down" {
		t.Fatalf("service names: client=%s server=%s", cs.Service, ss.Service)
	}
}
