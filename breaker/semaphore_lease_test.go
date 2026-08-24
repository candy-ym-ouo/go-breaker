package breaker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestSemaphoreLeaseReleasedOnEveryOutcome guards the regression where a permit
// acquired for a guarded call was only released on success. Under MaxConcurrency=1
// that first failing call left in_flight pinned at 1 and every later call was
// rejected with ErrConcurrencyLimit. Each sub-test runs one failing outcome and
// then asserts the slot is free by making a second call that must run (not be
// rejected) and leave in_flight at 0.
func TestSemaphoreLeaseReleasedOnEveryOutcome(t *testing.T) {
	t.Run("business_error", func(t *testing.T) {
		instance := mustNew(t, "lease-err", WithMaxConcurrency(1), WithCallTimeout(time.Second))
		_, _, _ = instance.ExecuteWithResult(context.Background(), func(context.Context) (interface{}, error) {
			return nil, errors.New("boom")
		})
		if got := instance.Snapshot().Metrics.InFlight; got != 0 {
			t.Fatalf("in_flight after failed call = %d, want 0", got)
		}
		assertNextCallRuns(t, instance)
	})

	t.Run("timeout", func(t *testing.T) {
		instance := mustNew(t, "lease-timeout", WithMaxConcurrency(1), WithCallTimeout(15*time.Millisecond))
		_, _, _ = instance.ExecuteWithResult(context.Background(), func(context.Context) (interface{}, error) {
			time.Sleep(50 * time.Millisecond)
			return "late", nil
		})
		if got := instance.Snapshot().Metrics.InFlight; got != 0 {
			t.Fatalf("in_flight after timeout = %d, want 0", got)
		}
		assertNextCallRuns(t, instance)
	})

	t.Run("panic", func(t *testing.T) {
		instance := mustNew(t, "lease-panic", WithMaxConcurrency(1), WithCallTimeout(time.Second), WithFallbackValue("fallback"))
		_, _, _ = instance.ExecuteWithResult(context.Background(), func(context.Context) (interface{}, error) {
			panic("boom")
		})
		if got := instance.Snapshot().Metrics.InFlight; got != 0 {
			t.Fatalf("in_flight after panic = %d, want 0", got)
		}
		assertNextCallRuns(t, instance)
	})

	t.Run("context_cancelled", func(t *testing.T) {
		instance := mustNew(t, "lease-cancel", WithMaxConcurrency(1), WithCallTimeout(time.Second))
		ctx, cancel := context.WithCancel(context.Background())
		entered := make(chan struct{})
		release := make(chan struct{})
		go func() {
			_, _, _ = instance.ExecuteWithResult(ctx, func(context.Context) (interface{}, error) {
				close(entered)
				<-release
				return "ok", nil
			})
		}()
		<-entered
		cancel()
		close(release)
		// Give the goroutine time to observe the cancellation and return.
		deadline := time.Now().Add(time.Second)
		for time.Now().Before(deadline) {
			if instance.Snapshot().Metrics.InFlight == 0 {
				break
			}
			time.Sleep(time.Millisecond)
		}
		if got := instance.Snapshot().Metrics.InFlight; got != 0 {
			t.Fatalf("in_flight after context cancel = %d, want 0", got)
		}
		assertNextCallRuns(t, instance)
	})

	t.Run("success", func(t *testing.T) {
		instance := mustNew(t, "lease-success", WithMaxConcurrency(1), WithCallTimeout(time.Second))
		_, _, _ = instance.ExecuteWithResult(context.Background(), func(context.Context) (interface{}, error) {
			return "ok", nil
		})
		if got := instance.Snapshot().Metrics.InFlight; got != 0 {
			t.Fatalf("in_flight after success = %d, want 0", got)
		}
		assertNextCallRuns(t, instance)
	})
}

// assertNextCallRuns makes one more call against a MaxConcurrency=1 breaker and
// verifies the permit is genuinely available: the call must execute (not be
// rejected with ErrConcurrencyLimit) and in_flight must return to 0 afterwards.
func assertNextCallRuns(t *testing.T, instance *Breaker) {
	t.Helper()
	value, err := instance.Execute(context.Background(), func(context.Context) (interface{}, error) {
		return "after", nil
	})
	if err != nil {
		t.Fatalf("follow-up call rejected: %v", err)
	}
	if value != "after" {
		t.Fatalf("follow-up call value = %v, want %q", value, "after")
	}
	if got := instance.Snapshot().Metrics.InFlight; got != 0 {
		t.Fatalf("in_flight after follow-up = %d, want 0", got)
	}
}

// TestSemaphoreLeaseNoStarvationAfterRepeatedFailures drives many consecutive
// failing calls through a MaxConcurrency=1 breaker and asserts none is rejected
// by the concurrency limiter and in_flight never sticks above 1.
func TestSemaphoreLeaseNoStarvationAfterRepeatedFailures(t *testing.T) {
	instance := mustNew(t, "starvation", WithMaxConcurrency(1), WithCallTimeout(time.Second))
	var rejected int64
	for i := 0; i < 20; i++ {
		_, err := instance.Execute(context.Background(), func(context.Context) (interface{}, error) {
			return nil, errors.New("boom")
		})
		if errors.Is(err, ErrConcurrencyLimit) {
			atomic.AddInt64(&rejected, 1)
		}
		if got := instance.Snapshot().Metrics.InFlight; got > 1 {
			t.Fatalf("iteration %d: in_flight = %d, want <= 1", i, got)
		}
	}
	if rejected != 0 {
		t.Fatalf("expected no concurrency-limit rejections across 20 failures, got %d", rejected)
	}
	if got := instance.Snapshot().Metrics.InFlight; got != 0 {
		t.Fatalf("in_flight after 20 failures = %d, want 0", got)
	}
}

// TestProbeDoesNotConsumeRegularConcurrencySlot verifies that a probe (half-open)
// call does not occupy the regular concurrency semaphore: after a probe runs to
// completion the slot count must still be 0 so a subsequent normal call is not
// rejected.
func TestProbeDoesNotConsumeRegularConcurrencySlot(t *testing.T) {
	instance := mustNew(t, "probe-slot",
		WithMaxConcurrency(1),
		WithCallTimeout(time.Second),
		WithMinRequests(1),
		WithErrorThreshold(0.5),
		WithSleepWindow(20*time.Millisecond),
	)
	// Trip the breaker into Open, then let the sleep window elapse so the next
	// call becomes a probe.
	_, _ = instance.Execute(context.Background(), func(context.Context) (interface{}, error) {
		return nil, errors.New("boom")
	})
	if instance.State() != StateOpen {
		t.Fatalf("expected open, got %s", instance.State())
	}
	time.Sleep(25 * time.Millisecond)

	// This call is a probe: it must succeed without touching the semaphore, so
	// in_flight stays 0 throughout and afterwards.
	value, _, err := instance.ExecuteWithResult(context.Background(), func(context.Context) (interface{}, error) {
		return "probe-ok", nil
	})
	if err != nil || value != "probe-ok" {
		t.Fatalf("probe call: value=%v err=%v", value, err)
	}
	if got := instance.Snapshot().Metrics.InFlight; got != 0 {
		t.Fatalf("probe consumed a concurrency slot: in_flight = %d, want 0", got)
	}
	waitState(t, instance, StateClosed)

	// After closing, a normal call must still find the slot free.
	assertNextCallRuns(t, instance)
}

// TestSemaphoreLeaseConcurrentReleaseStress runs many concurrent calls under
// MaxConcurrency=1 with a non-zero acquire timeout so every caller must wait for
// the in-flight call to release its slot. It confirms the permit is handed off
// correctly under contention: in_flight never exceeds 1, every call eventually
// runs (none is starved by a leaked slot), and the count ends at 0.
func TestSemaphoreLeaseConcurrentReleaseStress(t *testing.T) {
	instance := mustNew(t, "stress",
		WithMaxConcurrency(1),
		WithAcquireTimeout(5*time.Second),
		WithCallTimeout(time.Second),
	)
	var wg sync.WaitGroup
	var done int64
	var rejected int64
	start := make(chan struct{})
	const n = 50
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			<-start
			_, err := instance.Execute(context.Background(), func(context.Context) (interface{}, error) {
				return "ok", nil
			})
			switch {
			case err == nil:
				atomic.AddInt64(&done, 1)
			case errors.Is(err, ErrConcurrencyLimit):
				atomic.AddInt64(&rejected, 1)
			}
			if got := instance.Snapshot().Metrics.InFlight; got > 1 {
				t.Errorf("in_flight = %d, want <= 1", got)
			}
		}()
	}
	close(start)
	wg.Wait()
	if rejected != 0 {
		t.Fatalf("expected no concurrency-limit rejections under release pressure, got %d (done=%d)", rejected, done)
	}
	if done != n {
		t.Fatalf("expected all %d calls to complete, got %d", n, done)
	}
	if got := instance.Snapshot().Metrics.InFlight; got != 0 {
		t.Fatalf("in_flight after stress = %d, want 0", got)
	}
}
