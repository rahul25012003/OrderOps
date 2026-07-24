package order

// End-to-end saga tests: REAL catalog, inventory and payment services over
// real HTTP (httptest), real per-service SQLite, real breakers/retries.
// Only Redis is absent — facts stay in each outbox (relay not running),
// which the tests assert directly.

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/rahul25012003/orderops/internal/catalog"
	"github.com/rahul25012003/orderops/internal/inventory"
	"github.com/rahul25012003/orderops/internal/payment"
	"github.com/rahul25012003/orderops/internal/platform/httpkit"
)

type world struct {
	order     *Service
	orderSrv  *httptest.Server
	invDB     *sql.DB
	payDB     *sql.DB
	catDB     *sql.DB
	payODown  *httptest.Server
}

func newWorld(t *testing.T) *world {
	t.Helper()
	dir := t.TempDir()
	quiet := slog.New(slog.NewTextHandler(os.Stderr,
		&slog.HandlerOptions{Level: slog.LevelError}))

	cat, err := catalog.New(filepath.Join(dir, "catalog.sqlite"), quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cat.Close() })
	catSrv := httptest.NewServer(cat.Router())
	t.Cleanup(catSrv.Close)
	// a product priced to trip the PSP's scripted decline (ends 99)
	cat.DB.Exec(`INSERT INTO products (id, name, price_cents) VALUES
                 ('decline-me', 'Declines', 9999)`)

	inv, err := inventory.New(filepath.Join(dir, "inventory.sqlite"), quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { inv.Close() })
	invSrv := httptest.NewServer(inv.Router())
	t.Cleanup(invSrv.Close)
	inv.DB.Exec(`INSERT INTO stock (product_id, available) VALUES
                 ('decline-me', 10)`)

	pay, err := payment.New(filepath.Join(dir, "payment.sqlite"), quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pay.Close() })
	paySrv := httptest.NewServer(pay.Router())
	t.Cleanup(paySrv.Close)

	client := httpkit.NewClient(2 * time.Second)
	client.Attempts = 2
	client.Backoff = 20 * time.Millisecond
	ord, err := New(filepath.Join(dir, "order.sqlite"), client,
		catSrv.URL, invSrv.URL, paySrv.URL, quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ord.Close() })
	ordSrv := httptest.NewServer(ord.Router())
	t.Cleanup(ordSrv.Close)

	return &world{order: ord, orderSrv: ordSrv, invDB: inv.DB,
		payDB: pay.DB, catDB: cat.DB}
}

func placeOrder(t *testing.T, w *world, body string) *Order {
	t.Helper()
	resp, err := http.Post(w.orderSrv.URL+"/orders", "application/json",
		bytes.NewReader([]byte(body)))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 201 {
		t.Fatalf("create order: status %d", resp.StatusCode)
	}
	var o Order
	json.NewDecoder(resp.Body).Decode(&o)
	return &o
}

func waitTerminal(t *testing.T, w *world, id string) *Order {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		o, err := w.order.loadOrder(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		if o.State == StateConfirmed || o.State == StateCancelled {
			return o
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("saga never reached a terminal state")
	return nil
}

func available(t *testing.T, d *sql.DB, product string) int {
	t.Helper()
	var n int
	d.QueryRow("SELECT available FROM stock WHERE product_id = ?",
		product).Scan(&n)
	return n
}

func outboxTypes(t *testing.T, d *sql.DB) []string {
	t.Helper()
	rows, _ := d.Query("SELECT type FROM outbox ORDER BY seq")
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		rows.Scan(&s)
		out = append(out, s)
	}
	return out
}

func TestHappyPathConfirmsAndDecrementsStock(t *testing.T) {
	w := newWorld(t)
	before := available(t, w.invDB, "margherita")

	o := placeOrder(t, w,
		`{"customer":"rahul","items":[{"product_id":"margherita","qty":2}]}`)
	if o.TotalCents != 2*29900 {
		t.Fatalf("server-side pricing wrong: %d", o.TotalCents)
	}
	final := waitTerminal(t, w, o.ID)
	if final.State != StateConfirmed {
		t.Fatalf("state %s (%s), want confirmed", final.State, final.Reason)
	}
	if got := available(t, w.invDB, "margherita"); got != before-2 {
		t.Fatalf("stock %d, want %d", got, before-2)
	}
	var payState string
	w.payDB.QueryRow("SELECT state FROM payments WHERE order_id = ?",
		o.ID).Scan(&payState)
	if payState != "authorized" {
		t.Fatalf("payment state %q", payState)
	}
	types := outboxTypes(t, w.order.DB)
	if len(types) != 2 || types[0] != "order.created" ||
		types[1] != "order.confirmed" {
		t.Fatalf("order outbox: %v", types)
	}
}

func TestDeclinedPaymentCompensatesStock(t *testing.T) {
	w := newWorld(t)
	before := available(t, w.invDB, "decline-me")

	o := placeOrder(t, w,
		`{"customer":"rahul","items":[{"product_id":"decline-me","qty":1}]}`)
	final := waitTerminal(t, w, o.ID)
	if final.State != StateCancelled || final.Reason != "card declined" {
		t.Fatalf("state=%s reason=%q", final.State, final.Reason)
	}
	// compensation returned the stock to the shelf
	if got := available(t, w.invDB, "decline-me"); got != before {
		t.Fatalf("stock not compensated: %d, want %d", got, before)
	}
	steps, _ := w.order.loadSteps(context.Background(), o.ID)
	var sawComp bool
	for _, s := range steps {
		if s.Step == "release_stock" && s.Status == "compensated" {
			sawComp = true
		}
	}
	if !sawComp {
		t.Fatalf("timeline missing compensation: %+v", steps)
	}
	types := outboxTypes(t, w.order.DB)
	if types[len(types)-1] != "order.cancelled" {
		t.Fatalf("order outbox: %v", types)
	}
}

func TestInsufficientStockCancelsWithoutPayment(t *testing.T) {
	w := newWorld(t)
	// ramen-tonkotsu is seeded with only 3
	o := placeOrder(t, w,
		`{"customer":"rahul","items":[{"product_id":"ramen-tonkotsu","qty":4}]}`)
	final := waitTerminal(t, w, o.ID)
	if final.State != StateCancelled || final.Reason != "insufficient stock" {
		t.Fatalf("state=%s reason=%q", final.State, final.Reason)
	}
	if got := available(t, w.invDB, "ramen-tonkotsu"); got != 3 {
		t.Fatalf("stock touched on rejection: %d", got)
	}
	var payments int
	w.payDB.QueryRow("SELECT COUNT(*) FROM payments").Scan(&payments)
	if payments != 0 {
		t.Fatal("payment must never be called for an unreservable order")
	}
}

func TestDeadPSPCompensatesAndCancels(t *testing.T) {
	w := newWorld(t)
	os.Setenv("PAYMENT_DOWN", "1")
	defer os.Unsetenv("PAYMENT_DOWN")
	before := available(t, w.invDB, "salad-caesar")

	o := placeOrder(t, w,
		`{"customer":"rahul","items":[{"product_id":"salad-caesar","qty":1}]}`)
	final := waitTerminal(t, w, o.ID)
	if final.State != StateCancelled || final.Reason != "payment unavailable" {
		t.Fatalf("state=%s reason=%q", final.State, final.Reason)
	}
	if got := available(t, w.invDB, "salad-caesar"); got != before {
		t.Fatalf("stock not compensated after PSP death: %d", got)
	}
}

func TestSagaRecoveryResumesCrashedOrder(t *testing.T) {
	w := newWorld(t)
	ctx := context.Background()
	// simulate: order row committed, then the service crashed before the
	// saga goroutine did anything
	items, _ := json.Marshal([]Item{{ProductID: "mango-lassi", Qty: 1,
		PriceCents: 9900}})
	w.order.DB.Exec(`
        INSERT INTO orders (id, customer, items, total_cents, state,
                            created_at, updated_at)
        VALUES ('ord-crashed', 'rahul', ?, 9900, 'pending', ?, ?)`,
		string(items), "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z")

	n, err := w.order.RecoverStuckSagas(ctx, time.Millisecond)
	if err != nil || n != 1 {
		t.Fatalf("recovered %d err=%v", n, err)
	}
	o, _ := w.order.loadOrder(ctx, "ord-crashed")
	if o.State != StateConfirmed {
		t.Fatalf("recovered saga state %s (%s)", o.State, o.Reason)
	}
}

func TestConcurrentCreatesGetUniqueIDs(t *testing.T) {
	w := newWorld(t)
	const n = 60
	ids := make(chan string, n)
	errs := make(chan int, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := http.Post(w.orderSrv.URL+"/orders", "application/json",
				bytes.NewReader([]byte(
					`{"customer":"load","items":[{"product_id":"filter-coffee","qty":1}]}`)))
			if err != nil {
				errs <- 0
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != 201 {
				errs <- resp.StatusCode
				return
			}
			var o Order
			json.NewDecoder(resp.Body).Decode(&o)
			ids <- o.ID
		}()
	}
	wg.Wait()
	close(ids)
	close(errs)
	if len(errs) > 0 {
		t.Fatalf("%d/%d concurrent creates failed (id collision → 500)", len(errs), n)
	}
	seen := map[string]bool{}
	for id := range ids {
		if seen[id] {
			t.Fatalf("duplicate order id under concurrency: %s", id)
		}
		seen[id] = true
	}
	if len(seen) != n {
		t.Fatalf("got %d unique ids, want %d", len(seen), n)
	}
}

func TestSagaReplayIsIdempotent(t *testing.T) {
	w := newWorld(t)
	ctx := context.Background()
	before := available(t, w.invDB, "filter-coffee")
	o := placeOrder(t, w,
		`{"customer":"rahul","items":[{"product_id":"filter-coffee","qty":1}]}`)
	waitTerminal(t, w, o.ID)

	// replay the whole saga (recovery double-fire, at-least-once world)
	w.order.RunSaga(ctx, o.ID)
	w.order.RunSaga(ctx, o.ID)

	if got := available(t, w.invDB, "filter-coffee"); got != before-1 {
		t.Fatalf("stock decremented more than once: %d, want %d",
			got, before-1)
	}
	var payments int
	w.payDB.QueryRow("SELECT COUNT(*) FROM payments WHERE order_id = ?",
		o.ID).Scan(&payments)
	if payments != 1 {
		t.Fatalf("%d payment rows, want 1", payments)
	}
}
