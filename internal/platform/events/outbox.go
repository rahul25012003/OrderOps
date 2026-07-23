package events

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"time"
)

// OutboxSchema is embedded into every service's SQLite schema: state change
// and outbox row commit in the SAME transaction, so a fact is recorded iff
// the state change happened. The relay then delivers at-least-once; the
// consumer's dedupe table makes the effect exactly-once.
const OutboxSchema = `
CREATE TABLE IF NOT EXISTS outbox (
    seq        INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id   TEXT NOT NULL UNIQUE,
    type       TEXT NOT NULL,
    trace_id   TEXT NOT NULL DEFAULT '',
    data       TEXT NOT NULL,
    created_at TEXT NOT NULL,
    published  INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_outbox_unpublished ON outbox(published, seq);
CREATE TABLE IF NOT EXISTS processed_events (
    event_id     TEXT PRIMARY KEY,
    processed_at TEXT NOT NULL
);
`

// Stage writes a fact into the outbox WITHIN the caller's transaction.
func Stage(tx *sql.Tx, eventID, eventType, traceID string, data any) error {
	body, err := json.Marshal(data)
	if err != nil {
		return err
	}
	_, err = tx.Exec(`
        INSERT INTO outbox (event_id, type, trace_id, data, created_at)
        VALUES (?, ?, ?, ?, ?)`,
		eventID, eventType, traceID, string(body),
		time.Now().UTC().Format(time.RFC3339Nano))
	return err
}

// AlreadyProcessed checks-and-records the event id WITHIN the caller's
// transaction. Returns true if this event's effect was already applied —
// the caller must then commit nothing but the check itself (no-op replay).
func AlreadyProcessed(tx *sql.Tx, eventID string) (bool, error) {
	res, err := tx.Exec(`
        INSERT OR IGNORE INTO processed_events (event_id, processed_at)
        VALUES (?, ?)`,
		eventID, time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 0, err
}

// Relay drains a service's outbox to the bus. Crash-safe: a row is marked
// published only after the XADD succeeds; a crash in between causes a
// duplicate publish, which consumer dedupe absorbs.
type Relay struct {
	DB     *sql.DB
	Bus    *Bus
	Source string // service name stamped on envelopes
	Log    *slog.Logger
}

// Run polls until ctx is cancelled.
func (r *Relay) Run(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if _, err := r.Drain(ctx); err != nil && ctx.Err() == nil {
				r.logger().Error("outbox drain", "err", err)
			}
		}
	}
}

// Drain publishes every unpublished row in order. Returns rows published.
func (r *Relay) Drain(ctx context.Context) (int, error) {
	rows, err := r.DB.QueryContext(ctx, `
        SELECT seq, event_id, type, trace_id, data, created_at
        FROM outbox WHERE published = 0 ORDER BY seq LIMIT 100`)
	if err != nil {
		return 0, err
	}
	type row struct {
		seq                       int64
		id, typ, traceID, data, at string
	}
	var batch []row
	for rows.Next() {
		var x row
		if err := rows.Scan(&x.seq, &x.id, &x.typ, &x.traceID, &x.data,
			&x.at); err != nil {
			rows.Close()
			return 0, err
		}
		batch = append(batch, x)
	}
	rows.Close()

	n := 0
	for _, x := range batch {
		occurred, _ := time.Parse(time.RFC3339Nano, x.at)
		env := &Envelope{ID: x.id, Type: x.typ, Source: r.Source,
			OccurredAt: occurred, TraceID: x.traceID,
			Data: json.RawMessage(x.data)}
		if err := r.Bus.Publish(ctx, env); err != nil {
			return n, err // bus down: stop, keep order, retry next tick
		}
		if _, err := r.DB.ExecContext(ctx,
			"UPDATE outbox SET published = 1 WHERE seq = ?", x.seq); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

func (r *Relay) logger() *slog.Logger {
	if r.Log != nil {
		return r.Log
	}
	return slog.Default()
}
