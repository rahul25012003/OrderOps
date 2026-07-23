// Package catalog: the product service. Read-heavy; serves from its own
// SQLite with an in-process cache the storefront can hammer. Also the
// graceful-degradation demo target — the gateway serves its stale cache
// when catalog is down.
package catalog

import (
	"database/sql"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/rahul25012003/orderops/internal/platform/db"
	"github.com/rahul25012003/orderops/internal/platform/httpkit"
)

const schema = `
CREATE TABLE IF NOT EXISTS products (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    price_cents INTEGER NOT NULL,
    image_hue   INTEGER NOT NULL DEFAULT 200
);
`

// Product is the catalog entry.
type Product struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
	PriceCents  int64  `json:"price_cents"`
	ImageHue    int    `json:"image_hue"` // storefront renders a hue tile
}

var seed = []Product{
	{"margherita", "Margherita Pizza", "San Marzano tomatoes, fior di latte, basil", 29900, 6},
	{"truffle-fries", "Truffle Fries", "Hand-cut, parmesan, truffle oil", 19900, 36},
	{"paneer-bowl", "Paneer Tikka Bowl", "Charred paneer, jeera rice, mint chutney", 24900, 16},
	{"ramen-tonkotsu", "Tonkotsu Ramen", "18-hour broth, chashu, ajitama egg", 34900, 350},
	{"salad-caesar", "Caesar Salad", "Romaine, sourdough croutons, aged parmesan", 21900, 110},
	{"mango-lassi", "Mango Lassi", "Alphonso mango, thick-set curd", 9900, 45},
	{"filter-coffee", "Filter Coffee", "South-Indian style, brass tumbler", 7900, 25},
	{"gulab-jamun", "Gulab Jamun (2 pc)", "Warm, rose syrup, pistachio dust", 12900, 330},
}

// Service is the catalog HTTP service.
type Service struct {
	DB  *sql.DB
	Log *slog.Logger

	mu     sync.RWMutex
	cache  []Product
	cached time.Time
}

// New opens the store and seeds it if empty.
func New(dbPath string, log *slog.Logger) (*Service, error) {
	d, err := db.Open(dbPath, schema)
	if err != nil {
		return nil, err
	}
	var n int
	d.QueryRow("SELECT COUNT(*) FROM products").Scan(&n)
	if n == 0 {
		for _, p := range seed {
			d.Exec(`INSERT INTO products (id, name, description, price_cents,
                    image_hue) VALUES (?, ?, ?, ?, ?)`,
				p.ID, p.Name, p.Description, p.PriceCents, p.ImageHue)
		}
	}
	return &Service{DB: d, Log: log}, nil
}

func (s *Service) Close() error { return s.DB.Close() }

// Router builds the HTTP handler.
func (s *Service) Router() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		httpkit.WriteJSON(w, 200, map[string]string{"status": "ok",
			"service": "catalog"})
	})
	mux.HandleFunc("GET /products", s.listProducts)
	mux.HandleFunc("GET /products/{id}", s.getProduct)
	return httpkit.Middleware(s.Log, mux)
}

func (s *Service) listProducts(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	if time.Since(s.cached) < 5*time.Second && s.cache != nil {
		out := s.cache
		s.mu.RUnlock()
		httpkit.WriteJSON(w, 200, map[string]any{"products": out})
		return
	}
	s.mu.RUnlock()

	rows, err := s.DB.QueryContext(r.Context(), `
        SELECT id, name, description, price_cents, image_hue
        FROM products ORDER BY name`)
	if err != nil {
		httpkit.Error(w, 500, "storage failure")
		return
	}
	defer rows.Close()
	var out []Product
	for rows.Next() {
		var p Product
		if err := rows.Scan(&p.ID, &p.Name, &p.Description, &p.PriceCents,
			&p.ImageHue); err != nil {
			httpkit.Error(w, 500, "storage failure")
			return
		}
		out = append(out, p)
	}
	s.mu.Lock()
	s.cache, s.cached = out, time.Now()
	s.mu.Unlock()
	httpkit.WriteJSON(w, 200, map[string]any{"products": out})
}

func (s *Service) getProduct(w http.ResponseWriter, r *http.Request) {
	var p Product
	err := s.DB.QueryRowContext(r.Context(), `
        SELECT id, name, description, price_cents, image_hue
        FROM products WHERE id = ?`, r.PathValue("id")).
		Scan(&p.ID, &p.Name, &p.Description, &p.PriceCents, &p.ImageHue)
	if err == sql.ErrNoRows {
		httpkit.Error(w, 404, "no such product")
		return
	}
	if err != nil {
		httpkit.Error(w, 500, "storage failure")
		return
	}
	httpkit.WriteJSON(w, 200, p)
}
