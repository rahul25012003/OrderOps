// Package events is OrderOps' event backbone: versioned fact envelopes on
// Redis Streams (ADR-002: Kafka-shaped seam, no Kafka on the dev machine),
// a transactional OUTBOX so services never dual-write DB+bus, and
// processed-event DEDUPE so consumers are idempotent under redelivery.
//
// The contract every service lives by:
//
//	publish:  state change + outbox row in ONE SQLite transaction;
//	          a relay goroutine drains the outbox to the stream (at-least-once)
//	consume:  effect + processed_events row in ONE transaction;
//	          duplicates are detected there and acked without effect
package events

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Envelope is the wire format of one fact.
type Envelope struct {
	ID         string          `json:"id"`   // producer-side unique id
	Type       string          `json:"type"` // e.g. "order.created"
	Source     string          `json:"source"`
	OccurredAt time.Time       `json:"occurred_at"`
	TraceID    string          `json:"trace_id,omitempty"` // W3C trace-id
	Data       json.RawMessage `json:"data"`
}

// Topic maps an event type to its stream ("order.created" -> "orderops:order").
func Topic(eventType string) string {
	domain, _, ok := strings.Cut(eventType, ".")
	if !ok {
		domain = eventType
	}
	return "orderops:" + domain
}

// Delivery is one consumed message.
type Delivery struct {
	Envelope
	stream string
	msgID  string
}

// Bus publishes and subscribes envelopes. Redis Streams today; the
// interface is the Kafka seam.
type Bus struct {
	rdb *redis.Client
}

func NewBus(addr string) *Bus {
	return &Bus{rdb: redis.NewClient(&redis.Options{Addr: addr})}
}

func (b *Bus) Close() error                   { return b.rdb.Close() }
func (b *Bus) Ping(ctx context.Context) error { return b.rdb.Ping(ctx).Err() }

// Publish appends the envelope to its topic stream.
func (b *Bus) Publish(ctx context.Context, env *Envelope) error {
	body, err := json.Marshal(env)
	if err != nil {
		return err
	}
	return b.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: Topic(env.Type),
		Values: map[string]any{"envelope": string(body)},
	}).Err()
}

// ensureGroup creates the consumer group if missing.
func (b *Bus) ensureGroup(ctx context.Context, stream, group string) error {
	err := b.rdb.XGroupCreateMkStream(ctx, stream, group, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}
	return nil
}

// Consume blocks up to `block` for one delivery from any of the topics.
// Returns (nil, nil) on an idle tick.
func (b *Bus) Consume(ctx context.Context, topics []string, group,
	consumer string, block time.Duration) (*Delivery, error) {
	for _, t := range topics {
		if err := b.ensureGroup(ctx, t, group); err != nil {
			return nil, err
		}
	}
	args := make([]string, 0, len(topics)*2)
	args = append(args, topics...)
	for range topics {
		args = append(args, ">")
	}
	res, err := b.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: group, Consumer: consumer, Streams: args,
		Count: 1, Block: block,
	}).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	for _, s := range res {
		for _, m := range s.Messages {
			raw, _ := m.Values["envelope"].(string)
			var env Envelope
			if err := json.Unmarshal([]byte(raw), &env); err != nil {
				// poison frame: ack it away, surface the problem
				b.rdb.XAck(ctx, s.Stream, group, m.ID)
				return nil, fmt.Errorf("poison event on %s: %w", s.Stream, err)
			}
			return &Delivery{Envelope: env, stream: s.Stream, msgID: m.ID}, nil
		}
	}
	return nil, nil
}

// Ack marks the delivery processed for this group.
func (b *Bus) Ack(ctx context.Context, group string, d *Delivery) error {
	return b.rdb.XAck(ctx, d.stream, group, d.msgID).Err()
}

// Reclaim hands back deliveries stuck pending longer than minIdle (a
// consumer died mid-handle) — same XPENDING+XCLAIM pattern proven in
// JobHive; idle filtered client-side (Redis 5.0).
func (b *Bus) Reclaim(ctx context.Context, topic, group, consumer string,
	minIdle time.Duration) ([]*Delivery, error) {
	if err := b.ensureGroup(ctx, topic, group); err != nil {
		return nil, err
	}
	pend, err := b.rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: topic, Group: group, Start: "-", End: "+", Count: 50,
	}).Result()
	if err != nil {
		return nil, err
	}
	var out []*Delivery
	for _, p := range pend {
		if p.Idle < minIdle {
			continue
		}
		msgs, err := b.rdb.XClaim(ctx, &redis.XClaimArgs{
			Stream: topic, Group: group, Consumer: consumer,
			MinIdle: minIdle, Messages: []string{p.ID},
		}).Result()
		if err != nil && err != redis.Nil {
			return out, err
		}
		for _, m := range msgs {
			raw, _ := m.Values["envelope"].(string)
			var env Envelope
			if json.Unmarshal([]byte(raw), &env) == nil {
				out = append(out, &Delivery{Envelope: env, stream: topic,
					msgID: m.ID})
			} else {
				b.rdb.XAck(ctx, topic, group, m.ID)
			}
		}
	}
	return out, nil
}
