// Command catalog runs the product service (:7001).
package main

import (
	"flag"
	"log/slog"
	"os"

	"github.com/rahul25012003/orderops/internal/catalog"
	"github.com/rahul25012003/orderops/internal/platform/svc"
)

func main() {
	addr := flag.String("addr", ":7001", "listen address")
	dbPath := flag.String("db", "data/catalog.sqlite", "database path")
	flag.Parse()
	log := slog.New(slog.NewTextHandler(os.Stderr, nil)).With("svc", "catalog")

	s, err := catalog.New(*dbPath, log)
	svc.Fatal(log, "init", err)
	defer s.Close()

	ctx, stop := svc.SignalContext()
	defer stop()
	svc.Fatal(log, "serve", svc.Serve(ctx, *addr, s.Router(), log))
}
