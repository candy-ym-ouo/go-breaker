package breaker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestRetryPolicyRetriesFinalSuccessfulResult(t *testing.T) {
	var calls atomic.Int32
	instance := mustNew(t, "retry-success", WithRetryPolicy(RetryPolicy{
		MaxAttempts: 3, Multiplier: 2,
		Retryable: func(error) bool { return true },
	}))
	value, err := instance.Execute(context.Background(), func(context.Context) (interface{}, error) {
		if calls.Add(1) < 3 {
			return nil, errors.New("temporary")
		}
		return "ok", nil
	})
	if err != nil || value != "ok" || calls.Load() != 3 {
		t.Fatalf("value=%v err=%v calls=%d", value, err, calls.Load())
	}
	metrics := instance.Snapshot().Metrics
	if metrics.TotalRequests != 1 || metrics.Succeeded != 1 || metrics.Failed != 0 || metrics.Retries != 2 {
		t.Fatalf("metrics=%+v", metrics)
	}
}

func TestRetryPolicyRespectsPredicate(t *testing.T) {
	var calls atomic.Int32
	instance := mustNew(t, "retry-predicate", WithRetryPolicy(RetryPolicy{
		MaxAttempts: 3, Multiplier: 2,
		Retryable: func(err error) bool { return errors.Is(err, ErrTimeout) },
	}))
	_, err := instance.Execute(context.Background(), func(context.Context) (interface{}, error) {
		calls.Add(1)
		return nil, errors.New("do not retry")
	})
	if !errors.Is(err, ErrCallFailed) || calls.Load() != 1 {
		t.Fatalf("err=%v calls=%d", err, calls.Load())
	}
}

func TestRetryPolicyStopsWaitingWhenContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	instance := mustNew(t, "retry-cancel", WithRetryPolicy(RetryPolicy{
		MaxAttempts: 2, InitialDelay: time.Second, Multiplier: 2,
		Retryable: func(error) bool { return true },
	}))
	done := make(chan error, 1)
	go func() {
		_, err := instance.Execute(ctx, func(context.Context) (interface{}, error) {
			return nil, errors.New("temporary")
		})
		done <- err
	}()
	time.Sleep(10 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, ErrCallFailed) {
			t.Fatalf("err=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("retry wait did not stop after cancellation")
	}
}

func TestRetryPolicyValidation(t *testing.T) {
	if err := (RetryPolicy{MaxAttempts: -1}).Validate(); !errors.Is(err, ErrConfigInvalid) {
		t.Fatalf("err=%v", err)
	}
}

func TestRetryPolicyDelayWithFixedMultiplier(t *testing.T) {
	policy := RetryPolicy{InitialDelay: 5 * time.Millisecond, Multiplier: 1}
	if delay := policy.delay(3); delay != 5*time.Millisecond {
		t.Fatalf("delay=%s", delay)
	}
}
