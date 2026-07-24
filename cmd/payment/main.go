// Command payment runs the mock PSP (:7003).
package main

import (
	"flag"
	"log/slog"
	"os"

	"github.com/rahul25012003/orderops/internal/payment"
	"github.com/rahul25012003/orderops/internal/platform/svc"
)

func main() {
	addr := flag.String("addr", ":7003", "listen address")
	dbPath := flag.String("db", "data/payment.sqlite", "database path")
	redisAddr := flag.String("redis", svc.EnvOr("REDIS_ADDR", "127.0.0.1:6379"),
		"redis address (event bus)")
	collectorURL := flag.String("collector", svc.EnvOr("COLLECTOR_URL", ""),
		"trace collector URL (optional)")
	flag.Parse()
	log := slog.New(slog.NewTextHandler(os.Stderr, nil)).With("svc", "payment")

	s, err := payment.New(*dbPath, log)
	svc.Fatal(log, "init", err)
	defer s.Close()

	ctx, stop := svc.SignalContext()
	defer stop()
	s.Tracer = svc.StartTracer(ctx, *collectorURL)
	bus := svc.StartRelay(ctx, s.DB, "payment", *redisAddr, log)
	defer bus.Close()
	svc.Fatal(log, "serve", svc.Serve(ctx, *addr, s.Router(), log))
}
