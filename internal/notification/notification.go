// Package notification is the live-events service. It joins the bus as its
// OWN consumer group (independent offset from the collector — fan-out, the
// whole point of an event backbone) and pushes every fact to browsers over
// Server-Sent Events. Two streams: a global feed for the ops dashboard and
// a per-order feed the storefront subscribes to so the customer watches
// their saga advance in real time.
//
// This is a LIVE view, not a system of record: an in-memory hub with a
// bounded ring buffer for late subscribers. Durability lives in the
// collector; losing a live frame on a crash is acceptable here.
package notification

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/rahul25012003/orderops/internal/platform/events"
	"github.com/rahul25012003/orderops/internal/platform/httpkit"
	"github.com/rahul25012003/orderops/internal/platform/tracing"
)

// consumed streams (one group, all domains).
var topics = []string{"orderops:order", "orderops:stock", "orderops:payment"}

const group = "notification"

// Event is the normalized fact the hub broadcasts.
type Event struct {
	Type    string          `json:"type"`
	Source  string          `json:"source"`
	OrderID string          `json:"order_id"`
	TraceID string          `json:"trace_id,omitempty"`
	At      time.Time       `json:"at"`
	Data    json.RawMessage `json:"data"`
}

// Service is the notification HTTP + consumer service.
type Service struct {
	Bus    *events.Bus
	Log    *slog.Logger
	Tracer tracing.Exporter
	hub    *hub
}

// New builds the service around an already-connected bus.
func New(bus *events.Bus, log *slog.Logger) *Service {
	return &Service{Bus: bus, Log: log, hub: newHub(200)}
}

func (s *Service) Router() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		httpkit.WriteJSON(w, 200, map[string]any{"status": "ok",
			"service": "notification", "subscribers": s.hub.count()})
	})
	mux.HandleFunc("GET /events", s.stream)          // global feed
	mux.HandleFunc("GET /events/{id}", s.stream)     // per-order feed
	mux.HandleFunc("GET /recent", s.recent)          // ops tail (JSON)
	return httpkit.Middleware("notification", s.Log, s.Tracer, mux)
}

// Run consumes the bus until ctx is cancelled, publishing to the hub.
func (s *Service) Run(ctx context.Context) {
	consumer := fmt.Sprintf("notif-%d", os.Getpid())
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
		s.hub.publish(toEvent(d))
		s.Bus.Ack(ctx, group, d)
	}
}

func toEvent(d *events.Delivery) Event {
	var m map[string]any
	json.Unmarshal(d.Data, &m)
	orderID, _ := m["order_id"].(string)
	if orderID == "" {
		orderID, _ = m["id"].(string) // order.created carries "id"
	}
	return Event{Type: d.Type, Source: d.Source, OrderID: orderID,
		TraceID: d.TraceID, At: d.OccurredAt, Data: d.Data}
}

// stream is the SSE handler for both /events and /events/{id}.
func (s *Service) stream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		httpkit.Error(w, 500, "streaming unsupported")
		return
	}
	orderID := r.PathValue("id")
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(200)

	ch := s.hub.subscribe(orderID)
	defer s.hub.unsubscribe(ch)

	// backfill so a late subscriber still sees the saga so far
	for _, e := range s.hub.backfill(orderID) {
		writeSSE(w, e)
	}
	flusher.Flush()

	ping := time.NewTicker(15 * time.Second)
	defer ping.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case e := <-ch:
			writeSSE(w, e)
			flusher.Flush()
		case <-ping.C:
			fmt.Fprint(w, ": ping\n\n") // comment frame keeps the socket warm
			flusher.Flush()
		}
	}
}

func (s *Service) recent(w http.ResponseWriter, r *http.Request) {
	httpkit.WriteJSON(w, 200, map[string]any{"events": s.hub.recent()})
}

func writeSSE(w http.ResponseWriter, e Event) {
	body, _ := json.Marshal(e)
	fmt.Fprintf(w, "data: %s\n\n", body)
}

func (s *Service) logger() *slog.Logger {
	if s.Log != nil {
		return s.Log
	}
	return slog.Default()
}

// --- hub --------------------------------------------------------------------

type sub struct {
	ch     chan Event
	filter string // "" = all orders
}

type hub struct {
	mu      sync.Mutex
	subs    map[*sub]struct{}
	ring    []Event
	ringCap int
}

func newHub(ringCap int) *hub {
	return &hub{subs: map[*sub]struct{}{}, ringCap: ringCap}
}

func (h *hub) publish(e Event) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.ring = append(h.ring, e)
	if len(h.ring) > h.ringCap {
		h.ring = h.ring[len(h.ring)-h.ringCap:]
	}
	for s := range h.subs {
		if s.filter != "" && s.filter != e.OrderID {
			continue
		}
		select {
		case s.ch <- e:
		default: // slow consumer: drop rather than stall the fan-out
		}
	}
}

func (h *hub) subscribe(filter string) chan Event {
	h.mu.Lock()
	defer h.mu.Unlock()
	s := &sub{ch: make(chan Event, 64), filter: filter}
	h.subs[s] = struct{}{}
	return s.ch
}

func (h *hub) unsubscribe(ch chan Event) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for s := range h.subs {
		if s.ch == ch {
			delete(h.subs, s)
			return
		}
	}
}

// backfill returns the ring entries a new subscriber should replay: all
// matching a specific order, or the last 25 for the global feed.
func (h *hub) backfill(filter string) []Event {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []Event
	if filter != "" {
		for _, e := range h.ring {
			if e.OrderID == filter {
				out = append(out, e)
			}
		}
		return out
	}
	start := 0
	if len(h.ring) > 25 {
		start = len(h.ring) - 25
	}
	return append(out, h.ring[start:]...)
}

func (h *hub) recent() []Event {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]Event, len(h.ring))
	copy(out, h.ring)
	return out
}

func (h *hub) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.subs)
}
