// Package order: the saga orchestrator. An order walks an explicit,
// persisted state machine —
//
//	pending → stock_reserved → payment_authorized → confirmed
//	   └────────┴──────────────┴──► cancelled (with compensations run)
//
// Every step is recorded in saga_steps (the timeline the UIs render);
// every state change is an optimistic-locked transition; every fact
// (order.created/confirmed/cancelled) leaves through the transactional
// outbox. Downstream calls (inventory, payment) are idempotent by order
// id, so the recovery loop can replay a crashed saga from its persisted
// state — kill this service mid-saga and it finishes the job on restart.
package order

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/rahul25012003/orderops/internal/platform/db"
	"github.com/rahul25012003/orderops/internal/platform/events"
	"github.com/rahul25012003/orderops/internal/platform/httpkit"
)

const schema = `
CREATE TABLE IF NOT EXISTS orders (
    id          TEXT PRIMARY KEY,
    customer    TEXT NOT NULL,
    items       TEXT NOT NULL,
    total_cents INTEGER NOT NULL,
    state       TEXT NOT NULL,
    reason      TEXT NOT NULL DEFAULT '',
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_orders_state ON orders(state);
CREATE TABLE IF NOT EXISTS saga_steps (
    seq      INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id TEXT NOT NULL,
    step     TEXT NOT NULL,
    status   TEXT NOT NULL, -- ok | failed | compensated
    note     TEXT NOT NULL DEFAULT '',
    at       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_steps_order ON saga_steps(order_id);
` + events.OutboxSchema

// Saga states.
const (
	StatePending    = "pending"
	StateReserved   = "stock_reserved"
	StateAuthorized = "payment_authorized"
	StateConfirmed  = "confirmed"
	StateCancelled  = "cancelled"
)

// Item is one order line (price captured at order time).
type Item struct {
	ProductID  string `json:"product_id"`
	Qty        int    `json:"qty"`
	PriceCents int64  `json:"price_cents"`
}

// Order is the aggregate.
type Order struct {
	ID         string    `json:"id"`
	Customer   string    `json:"customer"`
	Items      []Item    `json:"items"`
	TotalCents int64     `json:"total_cents"`
	State      string    `json:"state"`
	Reason     string    `json:"reason,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// Step is one saga timeline row.
type Step struct {
	Seq    int64     `json:"seq"`
	Step   string    `json:"step"`
	Status string    `json:"status"`
	Note   string    `json:"note,omitempty"`
	At     time.Time `json:"at"`
}

// Service is the order HTTP service + saga runner.
type Service struct {
	DB           *sql.DB
	Client       *httpkit.Client
	CatalogURL   string
	InventoryURL string
	PaymentURL   string
	Log          *slog.Logger
}

func New(dbPath string, client *httpkit.Client, catalogURL, inventoryURL,
	paymentURL string, log *slog.Logger) (*Service, error) {
	d, err := db.Open(dbPath, schema)
	if err != nil {
		return nil, err
	}
	return &Service{DB: d, Client: client, CatalogURL: catalogURL,
		InventoryURL: inventoryURL, PaymentURL: paymentURL, Log: log}, nil
}

func (s *Service) Close() error { return s.DB.Close() }

func (s *Service) Router() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		httpkit.WriteJSON(w, 200, map[string]any{"status": "ok",
			"service": "order", "breakers": s.Client.BreakerStates()})
	})
	mux.HandleFunc("POST /orders", s.createOrder)
	mux.HandleFunc("GET /orders/{id}", s.getOrder)
	mux.HandleFunc("GET /orders", s.listOrders)
	return httpkit.Middleware(s.Log, mux)
}

// --- API --------------------------------------------------------------------

type createRequest struct {
	Customer string `json:"customer"`
	Items    []struct {
		ProductID string `json:"product_id"`
		Qty       int    `json:"qty"`
	} `json:"items"`
}

func (s *Service) createOrder(w http.ResponseWriter, r *http.Request) {
	var req createRequest
	if !httpkit.Decode(w, r, &req) {
		return
	}
	if req.Customer == "" || len(req.Items) == 0 {
		httpkit.Error(w, 422, "customer and items are required")
		return
	}
	// price server-side against the catalog — never trust client prices
	var items []Item
	var total int64
	for _, line := range req.Items {
		if line.Qty <= 0 || line.Qty > 20 {
			httpkit.Error(w, 422, "qty must be 1-20")
			return
		}
		var p struct {
			PriceCents int64 `json:"price_cents"`
		}
		err := s.Client.GetJSON(r.Context(), s.CatalogURL,
			"/products/"+line.ProductID, &p)
		var se *httpkit.StatusError
		if errors.As(err, &se) && se.Code == 404 {
			httpkit.Error(w, 422, "unknown product "+line.ProductID)
			return
		}
		if err != nil {
			httpkit.Error(w, 503, "catalog unavailable")
			return
		}
		items = append(items, Item{ProductID: line.ProductID, Qty: line.Qty,
			PriceCents: p.PriceCents})
		total += p.PriceCents * int64(line.Qty)
	}

	o := &Order{
		ID: newOrderID(), Customer: req.Customer, Items: items,
		TotalCents: total, State: StatePending,
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}
	itemsJSON, _ := json.Marshal(items)
	tx, err := s.DB.BeginTx(r.Context(), nil)
	if err != nil {
		httpkit.Error(w, 500, "storage failure")
		return
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`
        INSERT INTO orders (id, customer, items, total_cents, state,
                            created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)`,
		o.ID, o.Customer, string(itemsJSON), o.TotalCents, o.State,
		o.CreatedAt.Format(time.RFC3339Nano),
		o.UpdatedAt.Format(time.RFC3339Nano)); err != nil {
		httpkit.Error(w, 500, "storage failure")
		return
	}
	events.Stage(tx, o.ID+":order.created", "order.created",
		httpkit.TraceID(r.Context()), o)
	if err := tx.Commit(); err != nil {
		httpkit.Error(w, 500, "storage failure")
		return
	}

	// run the saga async — the API answers fast, the timeline streams live
	traceCtx := httpkit.WithTrace(context.Background(),
		httpkit.TraceID(r.Context()))
	go s.RunSaga(traceCtx, o.ID)

	httpkit.WriteJSON(w, 201, o)
}

func (s *Service) getOrder(w http.ResponseWriter, r *http.Request) {
	o, err := s.loadOrder(r.Context(), r.PathValue("id"))
	if err == sql.ErrNoRows {
		httpkit.Error(w, 404, "no such order")
		return
	}
	if err != nil {
		httpkit.Error(w, 500, "storage failure")
		return
	}
	steps, _ := s.loadSteps(r.Context(), o.ID)
	httpkit.WriteJSON(w, 200, map[string]any{"order": o, "saga": steps})
}

func (s *Service) listOrders(w http.ResponseWriter, r *http.Request) {
	limit := 50
	fmt.Sscanf(r.URL.Query().Get("limit"), "%d", &limit)
	if limit <= 0 || limit > 500 {
		limit = 50
	}
	state := r.URL.Query().Get("state")
	rows, err := s.DB.QueryContext(r.Context(), `
        SELECT id, customer, items, total_cents, state, reason,
               created_at, updated_at
        FROM orders WHERE (? = '' OR state = ?)
        ORDER BY id DESC LIMIT ?`, state, state, limit)
	if err != nil {
		httpkit.Error(w, 500, "storage failure")
		return
	}
	defer rows.Close()
	out := []*Order{}
	for rows.Next() {
		if o, err := scanOrder(rows); err == nil {
			out = append(out, o)
		}
	}
	httpkit.WriteJSON(w, 200, map[string]any{"orders": out})
}

// --- saga -------------------------------------------------------------------

// RunSaga advances an order from its CURRENT persisted state to a terminal
// one. Safe to call repeatedly and concurrently: transitions are
// optimistic-locked and downstream calls are idempotent by order id.
func (s *Service) RunSaga(ctx context.Context, orderID string) {
	log := s.logger().With("order_id", orderID)
	for {
		o, err := s.loadOrder(ctx, orderID)
		if err != nil {
			log.Error("saga load", "err", err)
			return
		}
		switch o.State {
		case StatePending:
			err = s.stepReserve(ctx, o)
		case StateReserved:
			err = s.stepAuthorize(ctx, o)
		case StateAuthorized:
			err = s.stepConfirm(ctx, o)
		default:
			return // terminal
		}
		if err != nil {
			log.Warn("saga halted", "state", o.State, "err", err)
			return // recovery loop retries or a compensation already ran
		}
	}
}

func (s *Service) stepReserve(ctx context.Context, o *Order) error {
	items := make([]map[string]any, len(o.Items))
	for i, it := range o.Items {
		items[i] = map[string]any{"product_id": it.ProductID, "qty": it.Qty}
	}
	err := s.Client.PostJSON(ctx, s.InventoryURL, "/reserve",
		map[string]any{"order_id": o.ID, "items": items}, nil)
	var se *httpkit.StatusError
	if errors.As(err, &se) && se.Code == 409 {
		// business rejection: no stock — nothing to compensate
		s.recordStep(ctx, o.ID, "reserve_stock", "failed", se.Body)
		return s.cancel(ctx, o, StatePending, "insufficient stock", nil)
	}
	if err != nil {
		s.recordStep(ctx, o.ID, "reserve_stock", "failed", err.Error())
		return err // transient: recovery loop will resume from pending
	}
	s.recordStep(ctx, o.ID, "reserve_stock", "ok", "")
	return s.transition(ctx, o.ID, StatePending, StateReserved, "")
}

func (s *Service) stepAuthorize(ctx context.Context, o *Order) error {
	err := s.Client.PostJSON(ctx, s.PaymentURL, "/authorize",
		map[string]any{"order_id": o.ID, "amount_cents": o.TotalCents}, nil)
	var se *httpkit.StatusError
	if errors.As(err, &se) && se.Code == 402 {
		// declined is FINAL: compensate the reservation, cancel the order
		s.recordStep(ctx, o.ID, "authorize_payment", "failed", "card declined")
		return s.cancel(ctx, o, StateReserved, "card declined",
			[]compensation{s.compReleaseStock})
	}
	if err != nil {
		// PSP unreachable past retries+breaker: compensate and cancel —
		// holding a customer's stock hostage to a dead PSP is worse
		s.recordStep(ctx, o.ID, "authorize_payment", "failed", err.Error())
		return s.cancel(ctx, o, StateReserved, "payment unavailable",
			[]compensation{s.compReleaseStock})
	}
	s.recordStep(ctx, o.ID, "authorize_payment", "ok", "")
	return s.transition(ctx, o.ID, StateReserved, StateAuthorized, "")
}

func (s *Service) stepConfirm(ctx context.Context, o *Order) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	res, err := tx.Exec(`UPDATE orders SET state = ?, updated_at = ?
                         WHERE id = ? AND state = ?`,
		StateConfirmed, now(), o.ID, StateAuthorized)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		tx.Commit()
		return nil // someone else finished it
	}
	events.Stage(tx, o.ID+":order.confirmed", "order.confirmed",
		httpkit.TraceID(ctx), map[string]any{"order_id": o.ID,
			"customer": o.Customer, "total_cents": o.TotalCents})
	if err := tx.Commit(); err != nil {
		return err
	}
	s.recordStep(ctx, o.ID, "confirm", "ok", "")
	s.logger().Info("order confirmed", "order_id", o.ID)
	return nil
}

type compensation func(ctx context.Context, o *Order) error

func (s *Service) compReleaseStock(ctx context.Context, o *Order) error {
	err := s.Client.PostJSON(ctx, s.InventoryURL, "/release",
		map[string]any{"order_id": o.ID}, nil)
	status := "compensated"
	note := "stock released"
	if err != nil {
		status, note = "failed", "release failed: "+err.Error()
	}
	s.recordStep(ctx, o.ID, "release_stock", status, note)
	return err
}

// cancel runs compensations then moves the order to cancelled with reason.
func (s *Service) cancel(ctx context.Context, o *Order, fromState,
	reason string, comps []compensation) error {
	for _, comp := range comps {
		if err := comp(ctx, o); err != nil {
			// compensation failed (e.g. inventory down): keep the order in
			// its current state; the recovery loop retries the whole path
			return err
		}
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	res, err := tx.Exec(`UPDATE orders SET state = ?, reason = ?, updated_at = ?
                         WHERE id = ? AND state = ?`,
		StateCancelled, reason, now(), o.ID, fromState)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		tx.Commit()
		return nil
	}
	events.Stage(tx, o.ID+":order.cancelled", "order.cancelled",
		httpkit.TraceID(ctx), map[string]any{"order_id": o.ID,
			"customer": o.Customer, "reason": reason})
	if err := tx.Commit(); err != nil {
		return err
	}
	s.logger().Warn("order cancelled", "order_id", o.ID, "reason", reason)
	return nil
}

// RecoverStuckSagas resumes non-terminal orders untouched for minAge —
// the crash-recovery loop (kill the service mid-saga; it finishes here).
func (s *Service) RecoverStuckSagas(ctx context.Context,
	minAge time.Duration) (int, error) {
	cutoff := time.Now().UTC().Add(-minAge).Format(time.RFC3339Nano)
	rows, err := s.DB.QueryContext(ctx, `
        SELECT id FROM orders
        WHERE state IN (?, ?, ?) AND updated_at < ? LIMIT 50`,
		StatePending, StateReserved, StateAuthorized, cutoff)
	if err != nil {
		return 0, err
	}
	var ids []string
	for rows.Next() {
		var id string
		rows.Scan(&id)
		ids = append(ids, id)
	}
	rows.Close()
	for _, id := range ids {
		s.recordStep(ctx, id, "recovery", "ok", "saga resumed after restart/stall")
		s.RunSaga(ctx, id)
	}
	return len(ids), nil
}

// RunRecovery ticks RecoverStuckSagas until ctx is cancelled.
func (s *Service) RunRecovery(ctx context.Context, interval,
	minAge time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if n, err := s.RecoverStuckSagas(ctx, minAge); err != nil {
				s.logger().Error("saga recovery", "err", err)
			} else if n > 0 {
				s.logger().Info("sagas recovered", "count", n)
			}
		}
	}
}

// --- plumbing ---------------------------------------------------------------

func (s *Service) transition(ctx context.Context, id, from, to,
	reason string) error {
	res, err := s.DB.ExecContext(ctx, `
        UPDATE orders SET state = ?, reason = ?, updated_at = ?
        WHERE id = ? AND state = ?`, to, reason, now(), id, from)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("order %s not in %s", id, from)
	}
	return nil
}

func (s *Service) recordStep(ctx context.Context, orderID, step, status,
	note string) {
	s.DB.ExecContext(ctx, `
        INSERT INTO saga_steps (order_id, step, status, note, at)
        VALUES (?, ?, ?, ?, ?)`, orderID, step, status, note, now())
}

func (s *Service) loadOrder(ctx context.Context, id string) (*Order, error) {
	row := s.DB.QueryRowContext(ctx, `
        SELECT id, customer, items, total_cents, state, reason,
               created_at, updated_at
        FROM orders WHERE id = ?`, id)
	return scanOrder(row)
}

func scanOrder(row interface{ Scan(...any) error }) (*Order, error) {
	var o Order
	var items, created, updated string
	if err := row.Scan(&o.ID, &o.Customer, &items, &o.TotalCents, &o.State,
		&o.Reason, &created, &updated); err != nil {
		return nil, err
	}
	json.Unmarshal([]byte(items), &o.Items)
	o.CreatedAt, _ = time.Parse(time.RFC3339Nano, created)
	o.UpdatedAt, _ = time.Parse(time.RFC3339Nano, updated)
	return &o, nil
}

func (s *Service) loadSteps(ctx context.Context, orderID string) ([]Step, error) {
	rows, err := s.DB.QueryContext(ctx, `
        SELECT seq, step, status, note, at FROM saga_steps
        WHERE order_id = ? ORDER BY seq`, orderID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Step{}
	for rows.Next() {
		var st Step
		var at string
		if err := rows.Scan(&st.Seq, &st.Step, &st.Status, &st.Note,
			&at); err != nil {
			return nil, err
		}
		st.At, _ = time.Parse(time.RFC3339Nano, at)
		out = append(out, st)
	}
	return out, rows.Err()
}

func (s *Service) logger() *slog.Logger {
	if s.Log != nil {
		return s.Log
	}
	return slog.Default()
}

func now() string { return time.Now().UTC().Format(time.RFC3339Nano) }

func newOrderID() string {
	return fmt.Sprintf("ord-%d", time.Now().UnixNano())
}
