// Command inventory runs the stock service (:7002).
package main

import (
	"flag"
	"log/slog"
	"os"

	"github.com/rahul25012003/orderops/internal/inventory"
	"github.com/rahul25012003/orderops/internal/platform/svc"
)

func main() {
	addr := flag.String("addr", ":7002", "listen address")
	dbPath := flag.String("db", "data/inventory.sqlite", "database path")
	redisAddr := flag.String("redis", svc.EnvOr("REDIS_ADDR", "127.0.0.1:6379"),
		"redis address (event bus)")
	flag.Parse()
	log := slog.New(slog.NewTextHandler(os.Stderr, nil)).With("svc", "inventory")

	s, err := inventory.New(*dbPath, log)
	svc.Fatal(log, "init", err)
	defer s.Close()

	ctx, stop := svc.SignalContext()
	defer stop()
	bus := svc.StartRelay(ctx, s.DB, "inventory", *redisAddr, log)
	defer bus.Close()
	svc.Fatal(log, "serve", svc.Serve(ctx, *addr, s.Router(), log))
}
