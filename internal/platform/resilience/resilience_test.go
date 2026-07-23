package resilience

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestBreakerTripsAndRecovers(t *testing.T) {
	b := &Breaker{FailureThreshold: 3, OpenFor: 100 * time.Millisecond}
	boom := errors.New("boom")
	ctx := context.Background()

	// three failures trip it
	for i := 0; i < 3; i++ {
		Do(ctx, b, 1, time.Millisecond, func(ctx context.Context) error {
			return boom
		})
	}
	if b.State() != "open" {
		t.Fatalf("state %s, want open", b.State())
	}
	// open = instant fail, fn never runs
	ran := false
	err := Do(ctx, b, 1, time.Millisecond, func(ctx context.Context) error {
		ran = true
		return nil
	})
	if !errors.Is(err, ErrOpen) || ran {
		t.Fatalf("open breaker admitted a call: err=%v ran=%v", err, ran)
	}

	// after OpenFor, one half-open probe heals it
	time.Sleep(120 * time.Millisecond)
	if err := Do(ctx, b, 1, time.Millisecond,
		func(ctx context.Context) error { return nil }); err != nil {
		t.Fatalf("half-open probe: %v", err)
	}
	if b.State() != "closed" {
		t.Fatalf("state %s, want closed after successful probe", b.State())
	}
}

func TestRetriesStopOnPermanent(t *testing.T) {
	b := &Breaker{FailureThreshold: 2}
	inner := errors.New("422 semantically wrong")
	calls := 0
	err := Do(context.Background(), b, 5, time.Millisecond,
		func(ctx context.Context) error {
			calls++
			return Permanent(inner)
		})
	if calls != 1 {
		t.Fatalf("permanent error retried: %d calls", calls)
	}
	if !errors.Is(err, inner) {
		t.Fatalf("caller must see the inner error, got %v", err)
	}
	// permanent failures are the DEPENDENCY answering fine: never trip
	if b.State() != "closed" {
		t.Fatalf("breaker tripped by permanent error: %s", b.State())
	}
}

func TestRetryEventuallySucceeds(t *testing.T) {
	calls := 0
	err := Do(context.Background(), nil, 3, time.Millisecond,
		func(ctx context.Context) error {
			calls++
			if calls < 3 {
				return errors.New("flaky")
			}
			return nil
		})
	if err != nil || calls != 3 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
}
