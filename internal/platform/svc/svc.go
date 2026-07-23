// Package svc: shared service runtime — signal handling, graceful HTTP
// shutdown, and outbox-relay wiring. Keeps every cmd/<service> main thin.
package svc

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/rahul25012003/orderops/internal/platform/events"
)

// SignalContext returns a context cancelled on Ctrl-C.
func SignalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt)
}

// Serve runs the HTTP server until ctx cancels, then drains connections.
func Serve(ctx context.Context, addr string, h http.Handler,
	log *slog.Logger) error {
	srv := &http.Server{Addr: addr, Handler: h}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()
	log.Info("listening", "addr", addr)
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		shutCtx, cancel := context.WithTimeout(context.Background(),
			5*time.Second)
		defer cancel()
		srv.Shutdown(shutCtx)
		log.Info("stopped")
		return nil
	}
}

// StartRelay connects the bus and starts this service's outbox relay.
// Returns the bus (close it on exit). Fail-soft: if Redis is down the
// relay keeps retrying — the service still serves, facts queue up in the
// outbox (that is the point of having one).
func StartRelay(ctx context.Context, db *sql.DB, source, redisAddr string,
	log *slog.Logger) *events.Bus {
	bus := events.NewBus(redisAddr)
	relay := &events.Relay{DB: db, Bus: bus, Source: source, Log: log}
	go relay.Run(ctx, 300*time.Millisecond)
	return bus
}

// EnvOr reads an env var with a fallback.
func EnvOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// Fatal logs and exits when err is a real failure.
func Fatal(log *slog.Logger, what string, err error) {
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error(what, "err", err)
		os.Exit(1)
	}
}
