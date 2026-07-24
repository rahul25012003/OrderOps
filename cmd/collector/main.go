// Command collector runs the trace + event observability service (:7007).
package main

import (
	"flag"
	"log/slog"
	"os"

	"github.com/rahul25012003/orderops/internal/collector"
	"github.com/rahul25012003/orderops/internal/platform/events"
	"github.com/rahul25012003/orderops/internal/platform/svc"
)

func main() {
	addr := flag.String("addr", ":7007", "listen address")
	dbPath := flag.String("db", "data/collector.sqlite", "database path")
	redisAddr := flag.String("redis", svc.EnvOr("REDIS_ADDR", "127.0.0.1:6379"),
		"redis address (event bus)")
	flag.Parse()
	log := slog.New(slog.NewTextHandler(os.Stderr, nil)).With("svc", "collector")

	ctx, stop := svc.SignalContext()
	defer stop()
	bus := events.NewBus(*redisAddr)
	defer bus.Close()

	s, err := collector.New(*dbPath, bus, log)
	svc.Fatal(log, "init", err)
	defer s.Close()

	go s.Run(ctx)
	svc.Fatal(log, "serve", svc.Serve(ctx, *addr, s.Router(), log))
}
