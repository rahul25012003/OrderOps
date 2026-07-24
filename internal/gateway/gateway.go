// Package gateway is OrderOps' single front door (:7000). It is the edge:
// it terminates the browser, serves the two single-page apps (storefront +
// ops console), enforces JWT auth and per-client rate limiting, and reverse-
// proxies /api/* to the right internal service — injecting the trace context
// so one customer click becomes one distributed trace end to end.
//
// Nothing downstream is internet-facing; the gateway is the only thing that
// is. That is the whole reason a gateway exists: cross-cutting concerns
// (auth, rate limit, tracing entry, static hosting) live here, once, instead
// of smeared across every service.
package gateway

import (
	"context"
	"embed"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/rahul25012003/orderops/internal/platform/httpkit"
	"github.com/rahul25012003/orderops/internal/platform/token"
	"github.com/rahul25012003/orderops/internal/platform/tracing"
)

//go:embed assets/*.html
var assets embed.FS

// Targets is the internal service map the gateway proxies to.
type Targets struct {
	Auth         string
	Catalog      string
	Inventory    string
	Order        string
	Notification string
	Collector    string
}

// Service is the gateway.
type Service struct {
	Secret  string
	Targets Targets
	Log     *slog.Logger
	Tracer  tracing.Exporter

	limiter    *limiter
	httpc      *http.Client
	storefront []byte
	ops        []byte
}

// New loads the embedded UIs and wires the gateway.
func New(secret string, targets Targets, log *slog.Logger) (*Service, error) {
	store, err := assets.ReadFile("assets/storefront.html")
	if err != nil {
		return nil, err
	}
	ops, err := assets.ReadFile("assets/ops.html")
	if err != nil {
		return nil, err
	}
	return &Service{
		Secret: secret, Targets: targets, Log: log,
		limiter:    newLimiter(25, 50), // 25 req/s, burst 50, per client IP
		httpc:      &http.Client{Timeout: 2 * time.Second},
		storefront: store, ops: ops,
	}, nil
}

func (s *Service) Router() http.Handler {
	mux := http.NewServeMux()

	// static single-page apps
	mux.HandleFunc("GET /", s.page(s.storefront))
	mux.HandleFunc("GET /ops", s.page(s.ops))
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		httpkit.WriteJSON(w, 200, map[string]string{"status": "ok",
			"service": "gateway"})
	})

	// auth (public): /api/auth/login, /api/auth/register
	authProxy := s.proxy(s.Targets.Auth, trimPrefix("/api/auth"))
	mux.Handle("POST /api/auth/login", authProxy)
	mux.Handle("POST /api/auth/register", authProxy)

	// catalog (public browse): /api/catalog/products...
	catProxy := s.proxy(s.Targets.Catalog, trimPrefix("/api/catalog"))
	mux.Handle("GET /api/catalog/", catProxy)

	// orders: creation + listing require a valid JWT; single-order read is
	// open (ids are unguessable and the storefront polls it as an SSE fallback)
	orderProxy := s.proxy(s.Targets.Order, trimPrefix("/api"))
	mux.Handle("POST /api/orders", s.requireAuth(orderProxy))
	mux.Handle("GET /api/orders", s.requireAuth(orderProxy))
	mux.Handle("GET /api/orders/{id}", orderProxy)

	// inventory stock view (ops): /api/inventory/stock
	invProxy := s.proxy(s.Targets.Inventory, trimPrefix("/api/inventory"))
	mux.Handle("GET /api/inventory/", invProxy)

	// live events (SSE) + recent tail: /api/events, /api/events/{id}, /api/recent
	notifProxy := s.proxy(s.Targets.Notification, trimPrefix("/api"))
	mux.Handle("GET /api/events", notifProxy)
	mux.Handle("GET /api/events/{id}", notifProxy)
	mux.Handle("GET /api/recent", notifProxy)

	// traces + stats (ops): /api/traces, /api/traces/{id}, /api/stats
	colProxy := s.proxy(s.Targets.Collector, trimPrefix("/api"))
	mux.Handle("GET /api/traces", colProxy)
	mux.Handle("GET /api/traces/{id}", colProxy)
	mux.Handle("GET /api/stats", colProxy)

	// aggregated ops snapshot (fan-out, gateway-owned)
	mux.HandleFunc("GET /api/ops/overview", s.opsOverview)

	// rate limit wraps everything under /api; the tracing middleware wraps all
	return httpkit.Middleware("gateway", s.Log, s.Tracer, s.rateLimited(mux))
}

func (s *Service) page(body []byte) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" && r.URL.Path != "/ops" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(body)
	}
}

// --- reverse proxy ----------------------------------------------------------

func trimPrefix(prefix string) func(string) string {
	return func(path string) string {
		out := strings.TrimPrefix(path, prefix)
		if out == "" {
			out = "/"
		}
		return out
	}
}

func (s *Service) proxy(targetBase string, rewrite func(string) string) http.Handler {
	base, _ := url.Parse(targetBase)
	rp := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(base)
			pr.Out.URL.Path = rewrite(pr.In.URL.Path)
			pr.Out.URL.RawQuery = pr.In.URL.RawQuery
			pr.SetXForwarded()
			// stitch the trace: downstream parents on the gateway's span
			if tid := httpkit.TraceID(pr.In.Context()); tid != "" {
				pr.Out.Header.Set("traceparent",
					httpkit.Traceparent(tid, httpkit.SpanID(pr.In.Context())))
			}
		},
		FlushInterval: -1, // flush immediately — required for SSE passthrough
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			httpkit.Error(w, http.StatusBadGateway, "upstream unavailable")
		},
	}
	return rp
}

// --- auth gate --------------------------------------------------------------

func (s *Service) requireAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		claims, err := token.Parse(token.Bearer(r.Header.Get("Authorization")),
			s.Secret, time.Now())
		if err != nil {
			httpkit.Error(w, 401, "authentication required")
			return
		}
		// hand identity to the upstream (never trusts client-set values —
		// these overwrite anything the browser sent)
		r.Header.Set("X-User-ID", claims.Subject)
		r.Header.Set("X-User-Role", claims.Role)
		next.ServeHTTP(w, r)
	})
}

// --- rate limiting ----------------------------------------------------------

func (s *Service) rateLimited(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// only throttle the API, and never long-lived SSE streams
		if strings.HasPrefix(r.URL.Path, "/api/") &&
			!strings.HasPrefix(r.URL.Path, "/api/events") {
			if !s.limiter.allow(clientIP(r)) {
				w.Header().Set("Retry-After", "1")
				httpkit.Error(w, http.StatusTooManyRequests, "rate limit exceeded")
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if i := strings.IndexByte(xff, ','); i >= 0 {
			return strings.TrimSpace(xff[:i])
		}
		return strings.TrimSpace(xff)
	}
	host := r.RemoteAddr
	if i := strings.LastIndexByte(host, ':'); i >= 0 {
		host = host[:i]
	}
	return host
}

type bucket struct {
	tokens float64
	last   time.Time
}

type limiter struct {
	mu      sync.Mutex
	buckets map[string]*bucket
	rate    float64 // tokens per second
	burst   float64 // bucket capacity
}

func newLimiter(rate, burst float64) *limiter {
	return &limiter{buckets: map[string]*bucket{}, rate: rate, burst: burst}
}

func (l *limiter) allow(key string) bool {
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	b := l.buckets[key]
	if b == nil {
		b = &bucket{tokens: l.burst, last: now}
		l.buckets[key] = b
	}
	b.tokens += now.Sub(b.last).Seconds() * l.rate
	if b.tokens > l.burst {
		b.tokens = l.burst
	}
	b.last = now
	if b.tokens >= 1 {
		b.tokens--
		return true
	}
	return false
}

// --- ops aggregation --------------------------------------------------------

type serviceHealth struct {
	Name    string `json:"name"`
	Healthy bool   `json:"healthy"`
	Detail  any    `json:"detail,omitempty"`
}

func (s *Service) opsOverview(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	checks := []struct{ name, url string }{
		{"gateway", ""},
		{"auth", s.Targets.Auth},
		{"catalog", s.Targets.Catalog},
		{"inventory", s.Targets.Inventory},
		{"order", s.Targets.Order},
		{"notification", s.Targets.Notification},
		{"collector", s.Targets.Collector},
	}
	health := make([]serviceHealth, len(checks))
	var wg sync.WaitGroup
	for i, c := range checks {
		if c.name == "gateway" {
			health[i] = serviceHealth{Name: "gateway", Healthy: true}
			continue
		}
		wg.Add(1)
		go func(i int, name, base string) {
			defer wg.Done()
			health[i] = s.health(ctx, name, base)
		}(i, c.name, c.url)
	}
	wg.Wait()

	// breaker states live on the order service's health payload
	breakers := map[string]any{}
	for _, h := range health {
		if h.name() == "order" {
			if m, ok := h.Detail.(map[string]any); ok {
				breakers, _ = m["breakers"].(map[string]any)
			}
		}
	}

	var stats any
	s.getJSON(ctx, s.Targets.Collector+"/stats", &stats)
	var stock any
	s.getJSON(ctx, s.Targets.Inventory+"/stock", &stock)

	httpkit.WriteJSON(w, 200, map[string]any{
		"services": health,
		"breakers": breakers,
		"stats":    stats,
		"stock":    stock,
	})
}

func (h serviceHealth) name() string { return h.Name }

func (s *Service) health(ctx context.Context, name, base string) serviceHealth {
	var detail map[string]any
	if err := s.getJSON(ctx, base+"/health", &detail); err != nil {
		return serviceHealth{Name: name, Healthy: false}
	}
	return serviceHealth{Name: name, Healthy: true, Detail: detail}
}

func (s *Service) getJSON(ctx context.Context, url string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := s.httpc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(out)
}
