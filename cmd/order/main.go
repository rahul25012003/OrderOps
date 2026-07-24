// Command order runs the saga orchestrator (:7004).
package main

import (
	"flag"
	"log/slog"
	"os"
	"time"

	"github.com/rahul25012003/orderops/internal/order"
	"github.com/rahul25012003/orderops/internal/platform/httpkit"
	"github.com/rahul25012003/orderops/internal/platform/svc"
)

func main() {
	addr := flag.String("addr", ":7004", "listen address")
	dbPath := flag.String("db", "data/order.sqlite", "database path")
	redisAddr := flag.String("redis", svc.EnvOr("REDIS_ADDR", "127.0.0.1:6379"),
		"redis address (event bus)")
	catalogURL := flag.String("catalog", svc.EnvOr("CATALOG_URL",
		"http://127.0.0.1:7001"), "catalog service URL")
	inventoryURL := flag.String("inventory", svc.EnvOr("INVENTORY_URL",
		"http://127.0.0.1:7002"), "inventory service URL")
	paymentURL := flag.String("payment", svc.EnvOr("PAYMENT_URL",
		"http://127.0.0.1:7003"), "payment service URL")
	collectorURL := flag.String("collector", svc.EnvOr("COLLECTOR_URL", ""),
		"trace collector URL (optional)")
	flag.Parse()
	log := slog.New(slog.NewTextHandler(os.Stderr, nil)).With("svc", "order")

	client := httpkit.NewClient(3 * time.Second)
	client.Service = "order"
	s, err := order.New(*dbPath, client, *catalogURL, *inventoryURL,
		*paymentURL, log)
	svc.Fatal(log, "init", err)
	defer s.Close()

	ctx, stop := svc.SignalContext()
	defer stop()
	tracer := svc.StartTracer(ctx, *collectorURL)
	s.Tracer = tracer
	client.Tracer = tracer
	bus := svc.StartRelay(ctx, s.DB, "order", *redisAddr, log)
	defer bus.Close()
	go s.RunRecovery(ctx, 5*time.Second, 15*time.Second)
	svc.Fatal(log, "serve", svc.Serve(ctx, *addr, s.Router(), log))
}
