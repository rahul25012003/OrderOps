// Package inventory: stock reservation with optimistic locking and
// order-scoped idempotency — reserving twice for the same order is a
// replay, not a double-decrement. Facts (stock.reserved / stock.released /
// stock.rejected) go out through the transactional outbox.
package inventory

import (
	"database/sql"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/rahul25012003/orderops/internal/platform/db"
	"github.com/rahul25012003/orderops/internal/platform/events"
	"github.com/rahul25012003/orderops/internal/platform/httpkit"
	"github.com/rahul25012003/orderops/internal/platform/tracing"
)

const schema = `
CREATE TABLE IF NOT EXISTS stock (
    product_id TEXT PRIMARY KEY,
    available  INTEGER NOT NULL,
    reserved   INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS reservations (
    order_id   TEXT NOT NULL,
    product_id TEXT NOT NULL,
    qty        INTEGER NOT NULL,
    state      TEXT NOT NULL DEFAULT 'held', -- held | released | consumed
    PRIMARY KEY (order_id, product_id)
);
` + events.OutboxSchema

var seedStock = map[string]int{
	"margherita": 50, "truffle-fries": 80, "paneer-bowl": 60,
	"ramen-tonkotsu": 3, "salad-caesar": 40, "mango-lassi": 100,
	"filter-coffee": 200, "gulab-jamun": 90,
}

// Item is one line of a reservation request.
type Item struct {
	ProductID string `json:"product_id"`
	Qty       int    `json:"qty"`
}

// Service is the inventory HTTP service.
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
	var n int
	d.QueryRow("SELECT COUNT(*) FROM stock").Scan(&n)
	if n == 0 {
		for id, qty := range seedStock {
			d.Exec("INSERT INTO stock (product_id, available) VALUES (?, ?)",
				id, qty)
		}
	}
	return &Service{DB: d, Log: log}, nil
}

func (s *Service) Close() error { return s.DB.Close() }

func (s *Service) Router() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		httpkit.WriteJSON(w, 200, map[string]string{"status": "ok",
			"service": "inventory"})
	})
	mux.HandleFunc("GET /stock", s.listStock)
	mux.HandleFunc("POST /reserve", s.reserve)
	mux.HandleFunc("POST /release", s.release)
	return httpkit.Middleware("inventory", s.Log, s.Tracer, mux)
}

type reserveRequest struct {
	OrderID string `json:"order_id"`
	Items   []Item `json:"items"`
}

// reserve holds stock for an order — all lines or nothing, idempotent by
// order id (a saga retry replays cleanly).
func (s *Service) reserve(w http.ResponseWriter, r *http.Request) {
	var req reserveRequest
	if !httpkit.Decode(w, r, &req) {
		return
	}
	if req.OrderID == "" || len(req.Items) == 0 {
		httpkit.Error(w, 422, "order_id and items are required")
		return
	}
	tx, err := s.DB.BeginTx(r.Context(), nil)
	if err != nil {
		httpkit.Error(w, 500, "storage failure")
		return
	}
	defer tx.Rollback()

	// idempotency: an existing held reservation for this order = replay
	var held int
	tx.QueryRow(`SELECT COUNT(*) FROM reservations
                 WHERE order_id = ? AND state = 'held'`, req.OrderID).Scan(&held)
	if held > 0 {
		tx.Commit()
		httpkit.WriteJSON(w, 200, map[string]any{"status": "reserved",
			"replay": true})
		return
	}

	for _, item := range req.Items {
		if item.Qty <= 0 {
			httpkit.Error(w, 422, "qty must be positive")
			return
		}
		// optimistic decrement: succeeds only if enough stock remains
		res, err := tx.Exec(`
            UPDATE stock SET available = available - ?, reserved = reserved + ?
            WHERE product_id = ? AND available >= ?`,
			item.Qty, item.Qty, item.ProductID, item.Qty)
		if err != nil {
			httpkit.Error(w, 500, "storage failure")
			return
		}
		if n, _ := res.RowsAffected(); n == 0 {
			// insufficient (or unknown product) — reject the whole order
			events.Stage(tx, req.OrderID+":stock.rejected", "stock.rejected",
				httpkit.TraceID(r.Context()), map[string]any{
					"order_id": req.OrderID, "product_id": item.ProductID,
					"reason": "insufficient stock"})
			tx.Commit() // commit ONLY the rejection fact
			httpkit.Error(w, 409, fmt.Sprintf(
				"insufficient stock for %s", item.ProductID))
			return
		}
		if _, err := tx.Exec(`
            INSERT INTO reservations (order_id, product_id, qty)
            VALUES (?, ?, ?)`,
			req.OrderID, item.ProductID, item.Qty); err != nil {
			httpkit.Error(w, 500, "storage failure")
			return
		}
	}
	if err := events.Stage(tx, req.OrderID+":stock.reserved", "stock.reserved",
		httpkit.TraceID(r.Context()), map[string]any{
			"order_id": req.OrderID, "items": req.Items}); err != nil {
		httpkit.Error(w, 500, "storage failure")
		return
	}
	if err := tx.Commit(); err != nil {
		httpkit.Error(w, 500, "storage failure")
		return
	}
	httpkit.WriteJSON(w, 200, map[string]any{"status": "reserved"})
}

type releaseRequest struct {
	OrderID string `json:"order_id"`
}

// release is the compensation: return held stock to the shelf. Idempotent —
// releasing an unknown or already-released order is a no-op success.
func (s *Service) release(w http.ResponseWriter, r *http.Request) {
	var req releaseRequest
	if !httpkit.Decode(w, r, &req) {
		return
	}
	tx, err := s.DB.BeginTx(r.Context(), nil)
	if err != nil {
		httpkit.Error(w, 500, "storage failure")
		return
	}
	defer tx.Rollback()

	rows, err := tx.Query(`SELECT product_id, qty FROM reservations
                           WHERE order_id = ? AND state = 'held'`, req.OrderID)
	if err != nil {
		httpkit.Error(w, 500, "storage failure")
		return
	}
	type line struct {
		product string
		qty     int
	}
	var lines []line
	for rows.Next() {
		var l line
		rows.Scan(&l.product, &l.qty)
		lines = append(lines, l)
	}
	rows.Close()
	if len(lines) == 0 {
		tx.Commit()
		httpkit.WriteJSON(w, 200, map[string]any{"status": "released",
			"replay": true})
		return
	}
	for _, l := range lines {
		tx.Exec(`UPDATE stock SET available = available + ?,
                 reserved = reserved - ? WHERE product_id = ?`,
			l.qty, l.qty, l.product)
	}
	tx.Exec(`UPDATE reservations SET state = 'released'
             WHERE order_id = ? AND state = 'held'`, req.OrderID)
	events.Stage(tx, req.OrderID+":stock.released", "stock.released",
		httpkit.TraceID(r.Context()),
		map[string]any{"order_id": req.OrderID})
	if err := tx.Commit(); err != nil {
		httpkit.Error(w, 500, "storage failure")
		return
	}
	httpkit.WriteJSON(w, 200, map[string]any{"status": "released"})
}

func (s *Service) listStock(w http.ResponseWriter, r *http.Request) {
	rows, err := s.DB.QueryContext(r.Context(),
		"SELECT product_id, available, reserved FROM stock ORDER BY product_id")
	if err != nil {
		httpkit.Error(w, 500, "storage failure")
		return
	}
	defer rows.Close()
	type row struct {
		ProductID string `json:"product_id"`
		Available int    `json:"available"`
		Reserved  int    `json:"reserved"`
	}
	var out []row
	for rows.Next() {
		var x row
		rows.Scan(&x.ProductID, &x.Available, &x.Reserved)
		out = append(out, x)
	}
	httpkit.WriteJSON(w, 200, map[string]any{"stock": out})
}
