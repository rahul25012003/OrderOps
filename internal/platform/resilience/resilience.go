// Package resilience: circuit breaker + retry with jittered backoff —
// applied to every inter-service call (each failure mode is chaos-tested).
package resilience

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

// ErrOpen is returned instantly while a breaker is open (failing fast is
// the point: a dead dependency shouldn't consume your goroutines/timeouts).
var ErrOpen = errors.New("circuit breaker open")

// Permanent marks err as non-retryable: Do returns it immediately and the
// breaker records a SUCCESS (the dependency answered; the request was just
// wrong — a 422 must never trip a breaker).
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err}
}

type permanentError struct{ err error }

func (p *permanentError) Error() string { return p.err.Error() }
func (p *permanentError) Unwrap() error { return p.err }

// Breaker is a classic closed -> open -> half-open circuit breaker.
type Breaker struct {
	FailureThreshold int           // consecutive failures to trip (default 5)
	OpenFor          time.Duration // how long to fail fast (default 10s)

	mu        sync.Mutex
	failures  int
	openUntil time.Time
	halfOpen  bool
}

func (b *Breaker) threshold() int {
	if b.FailureThreshold > 0 {
		return b.FailureThreshold
	}
	return 5
}

func (b *Breaker) openFor() time.Duration {
	if b.OpenFor > 0 {
		return b.OpenFor
	}
	return 10 * time.Second
}

// Allow reports whether a call may proceed right now.
func (b *Breaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failures < b.threshold() {
		return true
	}
	if time.Now().After(b.openUntil) {
		if b.halfOpen {
			return false // one probe already in flight
		}
		b.halfOpen = true // half-open: admit exactly one probe
		return true
	}
	return false
}

// Report records the outcome of an admitted call.
func (b *Breaker) Report(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err == nil {
		b.failures, b.halfOpen = 0, false
		return
	}
	b.failures++
	b.halfOpen = false
	if b.failures >= b.threshold() {
		b.openUntil = time.Now().Add(b.openFor())
	}
}

// State is for dashboards: closed | open | half-open.
func (b *Breaker) State() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch {
	case b.failures < b.threshold():
		return "closed"
	case time.Now().After(b.openUntil):
		return "half-open"
	default:
		return "open"
	}
}

// Do runs fn under the breaker with up to `attempts` tries and jittered
// exponential backoff between them. Context cancellation stops retries.
func Do(ctx context.Context, b *Breaker, attempts int, baseDelay time.Duration,
	fn func(ctx context.Context) error) error {
	if attempts < 1 {
		attempts = 1
	}
	var lastErr error
	for i := 0; i < attempts; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if b != nil && !b.Allow() {
			return fmt.Errorf("%w (last: %v)", ErrOpen, lastErr)
		}
		err := fn(ctx)
		var perm *permanentError
		isPerm := errors.As(err, &perm)
		if b != nil {
			if isPerm {
				b.Report(nil) // dependency healthy; request was just wrong
			} else {
				b.Report(err)
			}
		}
		if err == nil {
			return nil
		}
		if isPerm {
			return perm.Unwrap() // no retry; caller sees the inner error
		}
		lastErr = err
		if i < attempts-1 {
			d := baseDelay << i
			d = time.Duration(float64(d) * (0.5 + rand.Float64())) // jitter
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(d):
			}
		}
	}
	return lastErr
}
