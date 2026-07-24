package notification

// White-box: exercises the fan-out hub directly and the SSE endpoint over
// real HTTP — no Redis needed, since the hub is the unit under test.

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHubFiltersByOrder(t *testing.T) {
	h := newHub(50)
	all := h.subscribe("")
	only1 := h.subscribe("ord-1")

	h.publish(Event{Type: "stock.reserved", OrderID: "ord-2"})
	h.publish(Event{Type: "order.confirmed", OrderID: "ord-1"})

	// the global sub sees both
	if got := drain(all, 2); len(got) != 2 {
		t.Fatalf("global sub saw %d events, want 2", len(got))
	}
	// the per-order sub sees only ord-1
	got := drain(only1, 2)
	if len(got) != 1 || got[0].OrderID != "ord-1" {
		t.Fatalf("filtered sub leaked: %+v", got)
	}
}

func TestBackfillReplaysOrderHistory(t *testing.T) {
	h := newHub(50)
	h.publish(Event{Type: "order.created", OrderID: "ord-9"})
	h.publish(Event{Type: "stock.reserved", OrderID: "ord-9"})
	h.publish(Event{Type: "order.created", OrderID: "ord-other"})

	back := h.backfill("ord-9")
	if len(back) != 2 {
		t.Fatalf("backfill for ord-9 = %d, want 2 (its own history only)", len(back))
	}
}

func TestUnsubscribeStopsDelivery(t *testing.T) {
	h := newHub(10)
	ch := h.subscribe("")
	h.unsubscribe(ch)
	h.publish(Event{Type: "x"})
	select {
	case <-ch:
		t.Fatal("received on an unsubscribed channel")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestSSEStreamDeliversLiveEvent(t *testing.T) {
	s := New(nil, nil) // bus unused: we publish to the hub directly
	srv := httptest.NewServer(s.Router())
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", srv.URL+"/events/ord-7", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("content-type %q", resp.Header.Get("Content-Type"))
	}

	// give the handler a moment to subscribe, then publish
	go func() {
		time.Sleep(100 * time.Millisecond)
		s.hub.publish(Event{Type: "order.confirmed", OrderID: "ord-7",
			At: time.Now()})
	}()

	sc := bufio.NewScanner(resp.Body)
	deadline := time.Now().Add(2 * time.Second)
	for sc.Scan() && time.Now().Before(deadline) {
		line := sc.Text()
		if strings.HasPrefix(line, "data: ") {
			var e Event
			if json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &e) == nil &&
				e.Type == "order.confirmed" && e.OrderID == "ord-7" {
				return // success
			}
		}
	}
	t.Fatal("never received the live SSE frame for ord-7")
}

func drain(ch chan Event, want int) []Event {
	var out []Event
	deadline := time.Now().Add(500 * time.Millisecond)
	for len(out) < want && time.Now().Before(deadline) {
		select {
		case e := <-ch:
			out = append(out, e)
		case <-time.After(50 * time.Millisecond):
			return out
		}
	}
	return out
}
