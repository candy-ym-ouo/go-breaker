package breaker

import (
	"context"
	"fmt"
	"time"
)

// RetryPredicate decides whether a failed attempt is safe to retry.
// Callers should only retry operations that are idempotent or otherwise safe
// to execute more than once.
type RetryPredicate func(error) bool

// RetryPolicy controls retries within one breaker execution. MaxAttempts
// includes the initial call, so the default policy performs no retries.
type RetryPolicy struct {
	MaxAttempts  int
	InitialDelay time.Duration
	MaxDelay     time.Duration
	Multiplier   float64
	Retryable    RetryPredicate
}

func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{MaxAttempts: 1, Multiplier: 2}
}

func (p RetryPolicy) Validate() error {
	if p.MaxAttempts < 0 || p.MaxAttempts > 10 {
		return fmt.Errorf("%w: retry attempts must be between 0 and 10", ErrConfigInvalid)
	}
	if p.MaxAttempts <= 1 {
		return nil
	}
	if p.InitialDelay < 0 || p.MaxDelay < 0 || p.MaxDelay > 0 && p.MaxDelay < p.InitialDelay {
		return fmt.Errorf("%w: retry delay is invalid", ErrConfigInvalid)
	}
	if p.Multiplier < 1 {
		return fmt.Errorf("%w: retry multiplier must be at least 1", ErrConfigInvalid)
	}
	if p.MaxAttempts > 1 && p.Retryable == nil {
		return fmt.Errorf("%w: retry predicate is required", ErrConfigInvalid)
	}
	return nil
}

func (p RetryPolicy) delay(retry int) time.Duration {
	if retry < 1 || p.InitialDelay == 0 {
		return 0
	}
	delay := p.InitialDelay
	for attempt := 1; attempt < retry; attempt++ {
		next := time.Duration(float64(delay) * p.Multiplier)
		if next <= delay {
			return delay
		}
		if p.MaxDelay > 0 && next >= p.MaxDelay {
			return p.MaxDelay
		}
		delay = next
	}
	if p.MaxDelay > 0 && delay > p.MaxDelay {
		return p.MaxDelay
	}
	return delay
}

func (p RetryPolicy) attempts() int {
	if p.MaxAttempts < 1 {
		return 1
	}
	return p.MaxAttempts
}

func (p RetryPolicy) wait(ctx context.Context, retry int) error {
	delay := p.delay(retry)
	if delay == 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
