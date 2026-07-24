// Command auth runs the identity service (:7005).
package main

import (
	"flag"
	"log/slog"
	"os"

	"github.com/rahul25012003/orderops/internal/auth"
	"github.com/rahul25012003/orderops/internal/platform/svc"
)

func main() {
	addr := flag.String("addr", ":7005", "listen address")
	dbPath := flag.String("db", "data/auth.sqlite", "database path")
	secret := flag.String("secret", svc.EnvOr("AUTH_SECRET",
		"dev-secret-change-me"), "JWT signing secret (shared with gateway)")
	collectorURL := flag.String("collector", svc.EnvOr("COLLECTOR_URL", ""),
		"trace collector URL (optional)")
	flag.Parse()
	log := slog.New(slog.NewTextHandler(os.Stderr, nil)).With("svc", "auth")

	s, err := auth.New(*dbPath, *secret, log)
	svc.Fatal(log, "init", err)
	defer s.Close()

	ctx, stop := svc.SignalContext()
	defer stop()
	s.Tracer = svc.StartTracer(ctx, *collectorURL)
	svc.Fatal(log, "serve", svc.Serve(ctx, *addr, s.Router(), log))
}
