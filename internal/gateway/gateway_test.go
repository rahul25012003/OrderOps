package gateway_test

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rahul25012003/orderops/internal/gateway"
	"github.com/rahul25012003/orderops/internal/platform/token"
)

const secret = "gw-secret"

// captured records what the order upstream actually received.
type captured struct {
	mu      sync.Mutex
	userID  string
	trace   string
	hitPath string
}

func fakeUpstream(name string, cap *captured) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			switch {
			case r.URL.Path == "/health":
				h := map[string]any{"status": "ok", "service": name}
				if name == "order" {
					h["breakers"] = map[string]any{"http://payment": "closed"}
				}
				writeJSON(w, 200, h)
			case r.URL.Path == "/stats":
				writeJSON(w, 200, map[string]any{"spans": 12, "traces": 3,
					"latency_ms": map[string]float64{"p50": 1.2, "p99": 9.9}})
			case r.URL.Path == "/stock":
				writeJSON(w, 200, map[string]any{"stock": []map[string]any{
					{"product_id": "margherita", "available": 50, "reserved": 0}}})
			case r.URL.Path == "/products":
				writeJSON(w, 200, map[string]any{"products": []any{}})
			case r.URL.Path == "/orders":
				if cap != nil {
					cap.mu.Lock()
					cap.userID = r.Header.Get("X-User-ID")
					cap.trace = r.Header.Get("traceparent")
					cap.hitPath = r.URL.Path
					cap.mu.Unlock()
				}
				writeJSON(w, 201, map[string]any{"id": "ord-1", "state": "pending"})
			default:
				writeJSON(w, 200, map[string]any{"ok": true})
			}
		}))
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func newGateway(t *testing.T, cap *captured) *httptest.Server {
	t.Helper()
	up := func(name string) string {
		s := fakeUpstream(name, cap)
		t.Cleanup(s.Close)
		return s.URL
	}
	order := fakeUpstream("order", cap)
	t.Cleanup(order.Close)

	targets := gateway.Targets{
		Auth: up("auth"), Catalog: up("catalog"), Inventory: up("inventory"),
		Order: order.URL, Notification: up("notification"), Collector: up("collector"),
	}
	quiet := slog.New(slog.NewTextHandler(os.Stderr,
		&slog.HandlerOptions{Level: slog.LevelError}))
	gw, err := gateway.New(secret, targets, quiet)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(gw.Router())
	t.Cleanup(srv.Close)
	return srv
}

func TestServesSinglePageApps(t *testing.T) {
	srv := newGateway(t, nil)
	for path, needle := range map[string]string{
		"/":    "OrderOps",
		"/ops": "operations console",
	} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if !strings.Contains(string(body), needle) {
			t.Fatalf("%s did not serve the SPA (missing %q)", path, needle)
		}
	}
}

func TestAnonymousOrderRejected(t *testing.T) {
	srv := newGateway(t, nil)
	resp, err := http.Post(srv.URL+"/api/orders", "application/json",
		strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Fatalf("anonymous order want 401, got %d", resp.StatusCode)
	}
}

func TestAuthedOrderCarriesIdentityAndTrace(t *testing.T) {
	cap := &captured{}
	srv := newGateway(t, cap)
	tok, _ := token.Sign(token.Claims{Subject: "rahul@orderops.dev",
		Role: "customer"}, secret, time.Now(), time.Hour)

	req, _ := http.NewRequest("POST", srv.URL+"/api/orders",
		bytes.NewReader([]byte(`{"customer":"rahul","items":[]}`)))
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 201 {
		t.Fatalf("authed order want 201, got %d", resp.StatusCode)
	}
	cap.mu.Lock()
	defer cap.mu.Unlock()
	if cap.hitPath != "/orders" {
		t.Fatalf("upstream path = %q, want /orders (prefix strip)", cap.hitPath)
	}
	if cap.userID != "rahul@orderops.dev" {
		t.Fatalf("identity not propagated: X-User-ID=%q", cap.userID)
	}
	if len(cap.trace) != 55 || !strings.HasPrefix(cap.trace, "00-") {
		t.Fatalf("traceparent not injected across proxy: %q", cap.trace)
	}
}

func TestClientCannotForgeIdentity(t *testing.T) {
	cap := &captured{}
	srv := newGateway(t, cap)
	tok, _ := token.Sign(token.Claims{Subject: "real@user", Role: "customer"},
		secret, time.Now(), time.Hour)
	req, _ := http.NewRequest("POST", srv.URL+"/api/orders",
		bytes.NewReader([]byte(`{}`)))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("X-User-ID", "admin@evil") // spoof attempt
	http.DefaultClient.Do(req)
	cap.mu.Lock()
	defer cap.mu.Unlock()
	if cap.userID != "real@user" {
		t.Fatalf("gateway let a client spoof identity: %q", cap.userID)
	}
}

func TestPublicCatalogNeedsNoAuth(t *testing.T) {
	srv := newGateway(t, nil)
	resp, err := http.Get(srv.URL + "/api/catalog/products")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("public catalog want 200, got %d", resp.StatusCode)
	}
}

func TestRateLimitTripsUnderBurst(t *testing.T) {
	srv := newGateway(t, nil)
	var got429 bool
	for i := 0; i < 90; i++ { // burst 50, so a tight loop must trip it
		resp, err := http.Get(srv.URL + "/api/catalog/products")
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode == 429 {
			got429 = true
			break
		}
	}
	if !got429 {
		t.Fatal("rate limiter never tripped under a 90-request burst")
	}
}

func TestOpsOverviewAggregatesFanOut(t *testing.T) {
	srv := newGateway(t, nil)
	resp, err := http.Get(srv.URL + "/api/ops/overview")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out struct {
		Services []struct {
			Name    string `json:"name"`
			Healthy bool   `json:"healthy"`
		} `json:"services"`
		Breakers map[string]any `json:"breakers"`
		Stats    map[string]any `json:"stats"`
	}
	json.NewDecoder(resp.Body).Decode(&out)
	if len(out.Services) != 7 {
		t.Fatalf("overview listed %d services, want 7", len(out.Services))
	}
	for _, s := range out.Services {
		if !s.Healthy {
			t.Fatalf("service %s reported unhealthy in aggregation", s.Name)
		}
	}
	if out.Stats["spans"] == nil {
		t.Fatal("collector stats not aggregated into overview")
	}
	if out.Breakers["http://payment"] != "closed" {
		t.Fatalf("order breaker states not surfaced: %+v", out.Breakers)
	}
}
