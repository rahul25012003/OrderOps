// Package db opens each service's OWN SQLite database — the "no shared
// database" microservice rule survives the no-Docker constraint (ADR-003):
// services share nothing but the bus and each other's APIs.
package db

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite" // pure-Go driver
)

// Open creates/opens the database at path and applies the schema.
func Open(path, schema string) (*sql.DB, error) {
	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	d, err := sql.Open("sqlite",
		path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, err
	}
	d.SetMaxOpenConns(1) // single writer; avoids SQLITE_BUSY storms
	if _, err := d.Exec(schema); err != nil {
		d.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	return d, nil
}
