// Package auth is the identity service. It stores users with PBKDF2-hashed
// passwords (stdlib crypto/pbkdf2 — no bcrypt dependency) and mints HS256
// JWTs the gateway verifies statelessly. Small on purpose: register, login,
// and a seeded demo account are all a portfolio auth service needs to show
// the pattern (hash-on-write, constant-time verify, signed short-lived
// tokens) without pretending to be an IdP.
package auth

import (
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/rahul25012003/orderops/internal/platform/db"
	"github.com/rahul25012003/orderops/internal/platform/httpkit"
	"github.com/rahul25012003/orderops/internal/platform/token"
	"github.com/rahul25012003/orderops/internal/platform/tracing"
)

const schema = `
CREATE TABLE IF NOT EXISTS users (
    id         TEXT PRIMARY KEY,   -- email / handle
    name       TEXT NOT NULL,
    role       TEXT NOT NULL DEFAULT 'customer',
    salt       TEXT NOT NULL,
    hash       TEXT NOT NULL,
    created_at TEXT NOT NULL
);
`

const (
	pbkdf2Iter = 120_000 // deliberate cost; tune with hardware
	pbkdf2Len  = 32
	tokenTTL   = 12 * time.Hour
)

// Service is the auth HTTP service.
type Service struct {
	DB     *sql.DB
	Secret string // HMAC signing secret, shared with the gateway
	Log    *slog.Logger
	Tracer tracing.Exporter
}

// New opens the store, seeds two demo accounts, and returns the service.
func New(dbPath, secret string, log *slog.Logger) (*Service, error) {
	d, err := db.Open(dbPath, schema)
	if err != nil {
		return nil, err
	}
	s := &Service{DB: d, Secret: secret, Log: log}
	// demo seed so the storefront works out of the box (documented creds)
	s.seed("rahul@orderops.dev", "Rahul", "customer", "demo1234")
	s.seed("admin@orderops.dev", "Ops Admin", "admin", "admin1234")
	return s, nil
}

func (s *Service) seed(id, name, role, password string) {
	var n int
	s.DB.QueryRow("SELECT COUNT(*) FROM users WHERE id = ?", id).Scan(&n)
	if n == 0 {
		s.create(id, name, role, password)
	}
}

func (s *Service) Close() error { return s.DB.Close() }

func (s *Service) Router() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		httpkit.WriteJSON(w, 200, map[string]string{"status": "ok",
			"service": "auth"})
	})
	mux.HandleFunc("POST /register", s.register)
	mux.HandleFunc("POST /login", s.login)
	return httpkit.Middleware("auth", s.Log, s.Tracer, mux)
}

type credentials struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Password string `json:"password"`
}

type tokenResponse struct {
	Token string `json:"token"`
	Name  string `json:"name"`
	Role  string `json:"role"`
	ID    string `json:"id"`
}

func (s *Service) register(w http.ResponseWriter, r *http.Request) {
	var c credentials
	if !httpkit.Decode(w, r, &c) {
		return
	}
	c.ID = strings.TrimSpace(strings.ToLower(c.ID))
	if !strings.Contains(c.ID, "@") || len(c.Password) < 8 {
		httpkit.Error(w, 422, "valid email and 8+ char password required")
		return
	}
	if c.Name == "" {
		c.Name = c.ID
	}
	role, err := s.create(c.ID, c.Name, "customer", c.Password)
	if err != nil {
		httpkit.Error(w, 409, "account already exists")
		return
	}
	s.issue(w, c.ID, c.Name, role)
}

func (s *Service) login(w http.ResponseWriter, r *http.Request) {
	var c credentials
	if !httpkit.Decode(w, r, &c) {
		return
	}
	c.ID = strings.TrimSpace(strings.ToLower(c.ID))
	var name, role, salt, want string
	err := s.DB.QueryRowContext(r.Context(),
		"SELECT name, role, salt, hash FROM users WHERE id = ?", c.ID).
		Scan(&name, &role, &salt, &want)
	if err == sql.ErrNoRows {
		// same generic error + do a throwaway hash to blunt user enumeration
		verify(c.Password, "deadbeef", "")
		httpkit.Error(w, 401, "invalid credentials")
		return
	}
	if err != nil {
		httpkit.Error(w, 500, "storage failure")
		return
	}
	if !verify(c.Password, salt, want) {
		httpkit.Error(w, 401, "invalid credentials")
		return
	}
	s.issue(w, c.ID, name, role)
}

func (s *Service) issue(w http.ResponseWriter, id, name, role string) {
	tok, err := token.Sign(token.Claims{Subject: id, Name: name, Role: role},
		s.Secret, time.Now(), tokenTTL)
	if err != nil {
		httpkit.Error(w, 500, "could not mint token")
		return
	}
	httpkit.WriteJSON(w, 200, tokenResponse{Token: tok, Name: name,
		Role: role, ID: id})
}

// create inserts a user with a fresh random salt. Returns the role.
func (s *Service) create(id, name, role, password string) (string, error) {
	salt := make([]byte, 16)
	rand.Read(salt)
	saltHex := hex.EncodeToString(salt)
	_, err := s.DB.Exec(`
        INSERT INTO users (id, name, role, salt, hash, created_at)
        VALUES (?, ?, ?, ?, ?, ?)`,
		id, name, role, saltHex, hash(password, saltHex),
		time.Now().UTC().Format(time.RFC3339Nano))
	return role, err
}

func hash(password, saltHex string) string {
	salt, _ := hex.DecodeString(saltHex)
	dk, _ := pbkdf2.Key(sha256.New, password, salt, pbkdf2Iter, pbkdf2Len)
	return hex.EncodeToString(dk)
}

func verify(password, saltHex, wantHex string) bool {
	got := hash(password, saltHex)
	return subtle.ConstantTimeCompare([]byte(got), []byte(wantHex)) == 1
}
