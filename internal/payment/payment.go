// Package payment: the mock PSP. Deterministic, scriptable failure modes
// (amount-coded, like Stripe test cards) so demos and chaos tests can
// trigger every saga path on demand:
//
//	amount ending 99  -> card declined            (saga compensates)
//	amount ending 42  -> slow authorization (4s)  (timeout/breaker demo)
//	PAYMENT_DOWN=1    -> every call 503           (kill-the-PSP chaos)
//
// Authorization is idempotent by order id; refunds are the compensation.
package payment

import (
	"database/sql"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/rahul25012003/orderops/internal/platform/db"
	"github.com/rahul25012003/orderops/internal/platform/events"
	"github.com/rahul25012003/orderops/internal/platform/httpkit"
	"github.com/rahul25012003/orderops/internal/platform/tracing"
)

const schema = `
CREATE TABLE IF NOT EXISTS payments (
    order_id     TEXT PRIMARY KEY,
    amount_cents INTEGER NOT NULL,
    state        TEXT NOT NULL, -- authorized | declined | refunded
    auth_code    TEXT NOT NULL DEFAULT '',
    updated_at   TEXT NOT NULL
);
` + events.OutboxSchema

// Service is the payment HTTP service.
type Service struct {
	DB     *sql.DB
	Log    *slog.Logger
	Tracer tracing.Exporter
}

func New(dbPath string, log *slog.Logger) (*Service, error) {
	d, err := db.Open(dbPath, schema)
	if err != nil {
		return nil, err
	}
	return &Service{DB: d, Log: log}, nil
}

func (s *Service) Close() error { return s.DB.Close() }

func (s *Service) Router() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		httpkit.WriteJSON(w, 200, map[string]string{"status": "ok",
			"service": "payment"})
	})
	mux.HandleFunc("POST /authorize", s.authorize)
	mux.HandleFunc("POST /refund", s.refund)
	return httpkit.Middleware("payment", s.Log, s.Tracer, mux)
}

type authRequest struct {
	OrderID     string `json:"order_id"`
	AmountCents int64  `json:"amount_cents"`
}

func (s *Service) authorize(w http.ResponseWriter, r *http.Request) {
	if os.Getenv("PAYMENT_DOWN") == "1" {
		httpkit.Error(w, 503, "PSP unavailable (chaos mode)")
		return
	}
	var req authRequest
	if !httpkit.Decode(w, r, &req) {
		return
	}
	if req.OrderID == "" || req.AmountCents <= 0 {
		httpkit.Error(w, 422, "order_id and positive amount_cents required")
		return
	}
	if req.AmountCents%100 == 42 {
		time.Sleep(4 * time.Second) // scripted slow PSP
	}

	tx, err := s.DB.BeginTx(r.Context(), nil)
	if err != nil {
		httpkit.Error(w, 500, "storage failure")
		return
	}
	defer tx.Rollback()

	// idempotency: same order re-authorized = replay of the recorded outcome
	var state, code string
	err = tx.QueryRow(`SELECT state, auth_code FROM payments
                       WHERE order_id = ?`, req.OrderID).Scan(&state, &code)
	if err == nil {
		tx.Commit()
		s.respond(w, state, code, true)
		return
	}
	if err != sql.ErrNoRows {
		httpkit.Error(w, 500, "storage failure")
		return
	}

	declined := req.AmountCents%100 == 99 // scripted decline
	state = "authorized"
	if declined {
		state, code = "declined", ""
	} else {
		code = "AUTH-" + req.OrderID[:min(8, len(req.OrderID))]
	}
	if _, err := tx.Exec(`
        INSERT INTO payments (order_id, amount_cents, state, auth_code, updated_at)
        VALUES (?, ?, ?, ?, ?)`,
		req.OrderID, req.AmountCents, state, code,
		time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		httpkit.Error(w, 500, "storage failure")
		return
	}
	eventType := "payment.authorized"
	if declined {
		eventType = "payment.declined"
	}
	events.Stage(tx, req.OrderID+":"+eventType, eventType,
		httpkit.TraceID(r.Context()), map[string]any{
			"order_id": req.OrderID, "amount_cents": req.AmountCents})
	if err := tx.Commit(); err != nil {
		httpkit.Error(w, 500, "storage failure")
		return
	}
	s.respond(w, state, code, false)
}

func (s *Service) respond(w http.ResponseWriter, state, code string,
	replay bool) {
	if state == "declined" {
		httpkit.Error(w, 402, "card declined")
		return
	}
	httpkit.WriteJSON(w, 200, map[string]any{"status": state,
		"auth_code": code, "replay": replay})
}

type refundRequest struct {
	OrderID string `json:"order_id"`
}

// refund is the compensation. Idempotent: refunding an unknown or already-
// refunded order succeeds as a replay.
func (s *Service) refund(w http.ResponseWriter, r *http.Request) {
	var req refundRequest
	if !httpkit.Decode(w, r, &req) {
		return
	}
	tx, err := s.DB.BeginTx(r.Context(), nil)
	if err != nil {
		httpkit.Error(w, 500, "storage failure")
		return
	}
	defer tx.Rollback()
	res, err := tx.Exec(`UPDATE payments SET state = 'refunded', updated_at = ?
                         WHERE order_id = ? AND state = 'authorized'`,
		time.Now().UTC().Format(time.RFC3339Nano), req.OrderID)
	if err != nil {
		httpkit.Error(w, 500, "storage failure")
		return
	}
	if n, _ := res.RowsAffected(); n == 1 {
		events.Stage(tx, req.OrderID+":payment.refunded", "payment.refunded",
			httpkit.TraceID(r.Context()),
			map[string]any{"order_id": req.OrderID})
	}
	if err := tx.Commit(); err != nil {
		httpkit.Error(w, 500, "storage failure")
		return
	}
	httpkit.WriteJSON(w, 200, map[string]any{"status": "refunded"})
}
