package events

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/rahul25012003/orderops/internal/platform/db"
	"github.com/rahul25012003/orderops/internal/platform/testinfra"
)

func setup(t *testing.T) (*sql.DB, *Bus) {
	t.Helper()
	d, err := db.Open(filepath.Join(t.TempDir(), "svc.sqlite"), OutboxSchema)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	bus := NewBus(testinfra.RedisAddr(t))
	t.Cleanup(func() { bus.Close() })
	return d, bus
}

func TestOutboxIsTransactional(t *testing.T) {
	d, bus := setup(t)
	ctx := context.Background()
	relay := &Relay{DB: d, Bus: bus, Source: "test"}

	// staged but ROLLED BACK: the fact must never exist
	tx, _ := d.Begin()
	Stage(tx, "e1", "order.created", "", map[string]any{"x": 1})
	tx.Rollback()
	if n, _ := relay.Drain(ctx); n != 0 {
		t.Fatalf("rolled-back fact published: %d", n)
	}

	// staged and committed: published exactly once
	tx, _ = d.Begin()
	Stage(tx, "e2", "order.created", "trace-1", map[string]any{"x": 2})
	tx.Commit()
	if n, _ := relay.Drain(ctx); n != 1 {
		t.Fatal("committed fact not published")
	}
	if n, _ := relay.Drain(ctx); n != 0 {
		t.Fatal("fact published twice by relay")
	}

	d2, err := bus.Consume(ctx, []string{Topic("order.created")}, "g1", "c1",
		time.Second)
	if err != nil || d2 == nil {
		t.Fatalf("consume: %v %v", d2, err)
	}
	if d2.ID != "e2" || d2.Type != "order.created" || d2.TraceID != "trace-1" {
		t.Fatalf("bad envelope: %+v", d2.Envelope)
	}
	bus.Ack(ctx, "g1", d2)
}

func TestConsumerDedupeAbsorbsDuplicatePublish(t *testing.T) {
	d, bus := setup(t)
	ctx := context.Background()
	// simulate a relay crash between XADD and the published-mark: the same
	// envelope hits the stream twice
	env := &Envelope{ID: "dup-1", Type: "stock.reserved", Source: "test",
		OccurredAt: time.Now(), Data: []byte(`{}`)}
	bus.Publish(ctx, env)
	bus.Publish(ctx, env)

	seen := 0
	for i := 0; i < 2; i++ {
		del, err := bus.Consume(ctx, []string{Topic("stock.reserved")},
			"g1", "c1", time.Second)
		if err != nil || del == nil {
			t.Fatalf("consume %d: %v %v", i, del, err)
		}
		tx, _ := d.Begin()
		already, err := AlreadyProcessed(tx, del.ID)
		if err != nil {
			t.Fatal(err)
		}
		if !already {
			seen++ // apply the effect exactly here
		}
		tx.Commit()
		bus.Ack(ctx, "g1", del)
	}
	if seen != 1 {
		t.Fatalf("effect applied %d times, want exactly 1", seen)
	}
}

func TestReclaimHandsBackDeadConsumersDeliveries(t *testing.T) {
	d, bus := setup(t)
	_ = d
	ctx := context.Background()
	bus.Publish(ctx, &Envelope{ID: "r1", Type: "order.created",
		OccurredAt: time.Now(), Data: []byte(`{}`)})

	// consumer A takes the delivery and dies (never acks)
	del, err := bus.Consume(ctx, []string{Topic("order.created")}, "g1",
		"doomed", time.Second)
	if err != nil || del == nil {
		t.Fatalf("doomed consume: %v %v", del, err)
	}

	time.Sleep(150 * time.Millisecond)
	got, err := bus.Reclaim(ctx, Topic("order.created"), "g1", "survivor",
		100*time.Millisecond)
	if err != nil || len(got) != 1 || got[0].ID != "r1" {
		t.Fatalf("reclaim: %+v err=%v", got, err)
	}
	bus.Ack(ctx, "g1", got[0])
}
