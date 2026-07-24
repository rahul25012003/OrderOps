// Package httpkit: the shared HTTP plumbing every service uses — JSON
// helpers, W3C traceparent propagation, and a resilient inter-service
// client (timeout + retry + circuit breaker per target).
package httpkit

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/rahul25012003/orderops/internal/platform/resilience"
	"github.com/rahul25012003/orderops/internal/platform/tracing"
)

// --- tracing (W3C traceparent, hand-propagated — ADR-004) -------------------

type ctxKey int

const (
	traceKey ctxKey = iota
	spanKey
)

// TraceID returns the current trace id ("" if none).
func TraceID(ctx context.Context) string {
	v, _ := ctx.Value(traceKey).(string)
	return v
}

// WithTrace stores a trace id on the context.
func WithTrace(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, traceKey, traceID)
}

// SpanID returns the current (server) span id — the parent for any outbound
// call made while handling this request.
func SpanID(ctx context.Context) string {
	v, _ := ctx.Value(spanKey).(string)
	return v
}

// WithSpan stores the current span id on the context.
func WithSpan(ctx context.Context, spanID string) context.Context {
	return context.WithValue(ctx, spanKey, spanID)
}

// NewTraceID mints a 16-byte hex trace id.
func NewTraceID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// Middleware extracts/mints the trace context, records a SERVER span, logs
// the request, and serves. Pass a tracing.Exporter (nil = disabled).
func Middleware(service string, log *slog.Logger, exp tracing.Exporter,
	next http.Handler) http.Handler {
	if exp == nil {
		exp = tracing.Nop{}
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		traceID, parentSpan := parseTraceContext(r.Header.Get("traceparent"))
		if traceID == "" {
			traceID = NewTraceID()
		}
		span := tracing.NewSpanID()
		ctx := WithSpan(WithTrace(r.Context(), traceID), span)
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: 200}
		next.ServeHTTP(sw, r.WithContext(ctx))
		if log != nil {
			log.Info("http", "method", r.Method, "path", r.URL.Path,
				"status", sw.status, "ms", time.Since(start).Milliseconds(),
				"trace", traceID)
		}
		name := r.Pattern
		if name == "" {
			name = r.Method + " " + r.URL.Path
		}
		exp.Emit(tracing.Span{TraceID: traceID, SpanID: span,
			ParentID: parentSpan, Service: service, Name: name, Kind: "server",
			StartNS: start.UnixNano(), EndNS: time.Now().UnixNano(),
			Status: sw.status})
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

// Flush + Unwrap keep the wrapper transparent to SSE: without them the
// embedded interface hides the underlying Flusher, and every event stream
// (notification, the gateway's proxy passthrough) would buffer forever.
func (w *statusWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// parseTraceContext extracts (traceID, spanID) from a traceparent header:
// "00-<32 hex trace-id>-<16 hex span-id>-<flags>".
func parseTraceContext(h string) (traceID, spanID string) {
	if len(h) >= 55 && h[0:3] == "00-" {
		return h[3:35], h[36:52]
	}
	return "", ""
}

func formatTraceparent(traceID, spanID string) string {
	return fmt.Sprintf("00-%s-%s-01", traceID, spanID)
}

// Traceparent builds a W3C traceparent header value — exported so an edge
// proxy (the gateway) can inject the current trace context onto the request
// it forwards, stitching the whole hop chain into one trace.
func Traceparent(traceID, spanID string) string {
	return formatTraceparent(traceID, spanID)
}

// --- JSON helpers -----------------------------------------------------------

func WriteJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func Error(w http.ResponseWriter, code int, msg string) {
	WriteJSON(w, code, map[string]string{"error": msg})
}

func Decode(w http.ResponseWriter, r *http.Request, into any) bool {
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).
		Decode(into); err != nil {
		Error(w, http.StatusBadRequest, "invalid JSON body")
		return false
	}
	return true
}

// --- resilient inter-service client -----------------------------------------

// Client calls sibling services with per-target circuit breakers, retries
// with jittered backoff, hard timeouts, and traceparent propagation. Each
// logical call emits one CLIENT span parented on the caller's server span.
type Client struct {
	HTTP     *http.Client
	Attempts int           // per call (default 3)
	Backoff  time.Duration // base retry delay (default 100ms)
	Service  string        // caller name stamped on client spans
	Tracer   tracing.Exporter

	mu       sync.Mutex
	breakers map[string]*resilience.Breaker
}

func NewClient(timeout time.Duration) *Client {
	return &Client{
		HTTP:     &http.Client{Timeout: timeout},
		breakers: map[string]*resilience.Breaker{},
	}
}

func (c *Client) tracer() tracing.Exporter {
	if c.Tracer != nil {
		return c.Tracer
	}
	return tracing.Nop{}
}

func (c *Client) breaker(target string) *resilience.Breaker {
	c.mu.Lock()
	defer c.mu.Unlock()
	b, ok := c.breakers[target]
	if !ok {
		b = &resilience.Breaker{FailureThreshold: 5, OpenFor: 5 * time.Second}
		c.breakers[target] = b
	}
	return b
}

// BreakerStates exposes breaker health for dashboards.
func (c *Client) BreakerStates() map[string]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := map[string]string{}
	for target, b := range c.breakers {
		out[target] = b.State()
	}
	return out
}

// StatusError carries a non-2xx response.
type StatusError struct {
	Code int
	Body string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("HTTP %d: %s", e.Code, e.Body)
}

// PostJSON calls target+path with the request body, decoding 2xx into out
// (out may be nil). Breaker + retry + traceparent are automatic.
func (c *Client) PostJSON(ctx context.Context, target, path string,
	body, out any) error {
	return c.do(ctx, http.MethodPost, target, path, body, out)
}

// GetJSON fetches target+path into out.
func (c *Client) GetJSON(ctx context.Context, target, path string,
	out any) error {
	return c.do(ctx, http.MethodGet, target, path, nil, out)
}

func (c *Client) do(ctx context.Context, method, target, path string,
	body, out any) error {
	attempts := c.Attempts
	if attempts <= 0 {
		attempts = 3
	}
	backoff := c.Backoff
	if backoff <= 0 {
		backoff = 100 * time.Millisecond
	}
	var payload []byte
	if body != nil {
		var err error
		if payload, err = json.Marshal(body); err != nil {
			return err
		}
	}

	traceID := TraceID(ctx)
	clientSpan := tracing.NewSpanID() // downstream sees this as its parent
	parent := SpanID(ctx)
	start := time.Now()
	status := 0

	err := resilience.Do(ctx, c.breaker(target), attempts, backoff,
		func(ctx context.Context) error {
			req, err := http.NewRequestWithContext(ctx, method,
				target+path, bytes.NewReader(payload))
			if err != nil {
				return err
			}
			req.Header.Set("Content-Type", "application/json")
			if traceID != "" {
				req.Header.Set("traceparent",
					formatTraceparent(traceID, clientSpan))
			}
			resp, err := c.HTTP.Do(req)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			status = resp.StatusCode
			raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
			if resp.StatusCode < 200 || resp.StatusCode > 299 {
				se := &StatusError{Code: resp.StatusCode,
					Body: string(bytes.TrimSpace(raw))}
				if se.Code < 500 {
					// 4xx won't heal by retrying, and must not trip the
					// breaker — the dependency answered fine
					return resilience.Permanent(se)
				}
				return se
			}
			if out != nil {
				return json.Unmarshal(raw, out)
			}
			return nil
		})

	if traceID != "" {
		errStr := ""
		if err != nil {
			errStr = err.Error()
		}
		c.tracer().Emit(tracing.Span{TraceID: traceID, SpanID: clientSpan,
			ParentID: parent, Service: c.Service, Name: method + " " + path,
			Kind: "client", StartNS: start.UnixNano(),
			EndNS: time.Now().UnixNano(), Status: status, Err: errStr})
	}
	return err
}
