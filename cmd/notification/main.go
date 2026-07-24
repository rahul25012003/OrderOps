// Command notification runs the live SSE events service (:7006).
package main

import (
	"flag"
	"log/slog"
	"os"

	"github.com/rahul25012003/orderops/internal/notification"
	"github.com/rahul25012003/orderops/internal/platform/events"
	"github.com/rahul25012003/orderops/internal/platform/svc"
)

func main() {
	addr := flag.String("addr", ":7006", "listen address")
	redisAddr := flag.String("redis", svc.EnvOr("REDIS_ADDR", "127.0.0.1:6379"),
		"redis address (event bus)")
	collectorURL := flag.String("collector", svc.EnvOr("COLLECTOR_URL", ""),
		"trace collector URL (optional)")
	flag.Parse()
	log := slog.New(slog.NewTextHandler(os.Stderr, nil)).With("svc", "notification")

	ctx, stop := svc.SignalContext()
	defer stop()
	bus := events.NewBus(*redisAddr)
	defer bus.Close()

	s := notification.New(bus, log)
	s.Tracer = svc.StartTracer(ctx, *collectorURL)
	go s.Run(ctx)
	svc.Fatal(log, "serve", svc.Serve(ctx, *addr, s.Router(), log))
}
