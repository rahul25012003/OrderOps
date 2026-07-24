package auth_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rahul25012003/orderops/internal/auth"
	"github.com/rahul25012003/orderops/internal/platform/token"
)

const secret = "test-secret"

func newAuth(t *testing.T) *httptest.Server {
	t.Helper()
	quiet := slog.New(slog.NewTextHandler(os.Stderr,
		&slog.HandlerOptions{Level: slog.LevelError}))
	s, err := auth.New(filepath.Join(t.TempDir(), "auth.sqlite"), secret, quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	srv := httptest.NewServer(s.Router())
	t.Cleanup(srv.Close)
	return srv
}

func post(t *testing.T, url, body string) (int, map[string]any) {
	t.Helper()
	resp, err := http.Post(url, "application/json", bytes.NewReader([]byte(body)))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func TestSeededDemoLoginMintsValidToken(t *testing.T) {
	srv := newAuth(t)
	code, out := post(t, srv.URL+"/login",
		`{"id":"rahul@orderops.dev","password":"demo1234"}`)
	if code != 200 {
		t.Fatalf("login status %d: %v", code, out)
	}
	tok, _ := out["token"].(string)
	claims, err := token.Parse(tok, secret, time.Now())
	if err != nil {
		t.Fatalf("issued token invalid: %v", err)
	}
	if claims.Subject != "rahul@orderops.dev" || claims.Role != "customer" {
		t.Fatalf("claims: %+v", claims)
	}
}

func TestWrongPasswordRejected(t *testing.T) {
	srv := newAuth(t)
	code, _ := post(t, srv.URL+"/login",
		`{"id":"rahul@orderops.dev","password":"nope"}`)
	if code != 401 {
		t.Fatalf("want 401, got %d", code)
	}
}

func TestUnknownUserRejected(t *testing.T) {
	srv := newAuth(t)
	code, _ := post(t, srv.URL+"/login",
		`{"id":"ghost@orderops.dev","password":"whatever"}`)
	if code != 401 {
		t.Fatalf("want 401 for unknown user, got %d", code)
	}
}

func TestRegisterThenLogin(t *testing.T) {
	srv := newAuth(t)
	code, out := post(t, srv.URL+"/register",
		`{"id":"new@orderops.dev","name":"New","password":"hunter2pass"}`)
	if code != 200 {
		t.Fatalf("register status %d: %v", code, out)
	}
	if _, ok := out["token"].(string); !ok {
		t.Fatal("register returned no token")
	}
	// same creds now log in
	code, _ = post(t, srv.URL+"/login",
		`{"id":"new@orderops.dev","password":"hunter2pass"}`)
	if code != 200 {
		t.Fatalf("login after register: %d", code)
	}
	// duplicate registration is refused
	code, _ = post(t, srv.URL+"/register",
		`{"id":"new@orderops.dev","name":"Dup","password":"hunter2pass"}`)
	if code != 409 {
		t.Fatalf("duplicate register want 409, got %d", code)
	}
}

func TestWeakInputRejected(t *testing.T) {
	srv := newAuth(t)
	code, _ := post(t, srv.URL+"/register",
		`{"id":"notanemail","password":"short"}`)
	if code != 422 {
		t.Fatalf("want 422 for weak input, got %d", code)
	}
}
