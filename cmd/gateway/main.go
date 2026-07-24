// Command gateway runs the edge: static UIs, auth, rate limiting, and the
// reverse proxy to every internal service (:7000).
package main

import (
	"flag"
	"log/slog"
	"os"

	"github.com/rahul25012003/orderops/internal/gateway"
	"github.com/rahul25012003/orderops/internal/platform/svc"
)

func main() {
	addr := flag.String("addr", ":7000", "listen address")
	secret := flag.String("secret", svc.EnvOr("AUTH_SECRET",
		"dev-secret-change-me"), "JWT verification secret (shared with auth)")
	collectorURL := flag.String("collector", svc.EnvOr("COLLECTOR_URL", ""),
		"trace collector URL (optional)")
	flag.Parse()
	log := slog.New(slog.NewTextHandler(os.Stderr, nil)).With("svc", "gateway")

	targets := gateway.Targets{
		Auth:         svc.EnvOr("AUTH_URL", "http://127.0.0.1:7005"),
		Catalog:      svc.EnvOr("CATALOG_URL", "http://127.0.0.1:7001"),
		Inventory:    svc.EnvOr("INVENTORY_URL", "http://127.0.0.1:7002"),
		Order:        svc.EnvOr("ORDER_URL", "http://127.0.0.1:7004"),
		Notification: svc.EnvOr("NOTIFICATION_URL", "http://127.0.0.1:7006"),
		Collector:    svc.EnvOr("COLLECTOR_URL_HTTP", "http://127.0.0.1:7007"),
	}
	s, err := gateway.New(*secret, targets, log)
	svc.Fatal(log, "init", err)

	ctx, stop := svc.SignalContext()
	defer stop()
	s.Tracer = svc.StartTracer(ctx, *collectorURL)
	svc.Fatal(log, "serve", svc.Serve(ctx, *addr, s.Router(), log))
}
