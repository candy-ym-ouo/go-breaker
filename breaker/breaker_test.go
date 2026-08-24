package breaker

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestBreakerOpensAndRecovers(t *testing.T) {
	instance := mustNew(t, "test",
		WithMinRequests(2),
		WithErrorThreshold(0.5),
		WithSleepWindow(20*time.Millisecond),
		WithCallTimeout(time.Second),
	)
	call := func(context.Context) (interface{}, error) { return nil, errors.New("boom") }
	_, _ = instance.Execute(context.Background(), call)
	_, _ = instance.Execute(context.Background(), call)
	if instance.State() != StateOpen {
		t.Fatalf("state = %s", instance.State())
	}
	if _, err := instance.Execute(context.Background(), call); !IsBreakerOpen(err) {
		t.Fatalf("expected breaker open, got %v", err)
	}
	time.Sleep(25 * time.Millisecond)
	value, err := instance.Execute(context.Background(), func(context.Context) (interface{}, error) {
		return "ok", nil
	})
	if err != nil || value != "ok" {
		t.Fatalf("probe result: %v, %v", value, err)
	}
	waitState(t, instance, StateClosed)
}

func TestBreakerFallbackTimeoutAndPanic(t *testing.T) {
	instance := mustNew(t, "fallback",
		WithCallTimeout(15*time.Millisecond),
		WithFallbackValue("fallback"),
	)
	value, _, err := instance.ExecuteWithResult(context.Background(), func(context.Context) (interface{}, error) {
		time.Sleep(50 * time.Millisecond)
		return "late", nil
	})
	if err != nil || value != "fallback" {
		t.Fatalf("timeout fallback: %v, %v", value, err)
	}
	value, err = instance.Execute(context.Background(), func(context.Context) (interface{}, error) {
		panic("boom")
	})
	if err != nil || value != "fallback" {
		t.Fatalf("panic fallback: %v, %v", value, err)
	}
	metrics := instance.Snapshot().Metrics
	if metrics.Timeouts != 1 || metrics.Failed != 1 {
		t.Fatalf("metrics: %+v", metrics)
	}
}

func TestBreakerTimeoutDoesNotLeakLateGoroutine(t *testing.T) {
	// CallTimeout (1ms) shorter than the business call (3ms). Fire 200 calls
	// concurrently: they all time out and the caller returns, but each late
	// goroutine keeps running fn and must still be able to deliver its result
	// and exit. With an unbuffered result channel that send blocks forever
	// (no receiver is ever coming), leaking one goroutine per timed-out call.
	// MaxConcurrency is lifted so every call reaches the timeout path. We
	// only assert the leak here; timeout accounting is covered by
	// TestBreakerFallbackTimeoutAndPanic.
	const n = 200
	instance := mustNew(t, "timeout-leak",
		WithCallTimeout(time.Millisecond),
		WithMaxConcurrency(n+1),
	)
	before := runtime.NumGoroutine()
	var group sync.WaitGroup
	group.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer group.Done()
			_, _, _ = instance.ExecuteWithResult(context.Background(), func(context.Context) (interface{}, error) {
				time.Sleep(3 * time.Millisecond)
				return "late", nil
			})
		}()
	}
	group.Wait()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= before+1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if delta := runtime.NumGoroutine() - before; delta > 5 {
		t.Fatalf("timeout leaked goroutines: before=%d after=%d delta=%d", before, runtime.NumGoroutine(), delta)
	}
}

func TestBreakerConcurrencyLimit(t *testing.T) {
	instance := mustNew(t, "concurrency", WithMaxConcurrency(1), WithCallTimeout(time.Second))
	entered := make(chan struct{})
	release := make(chan struct{})
	var group sync.WaitGroup
	group.Add(1)
	go func() {
		defer group.Done()
		_, _ = instance.Execute(context.Background(), func(context.Context) (interface{}, error) {
			close(entered)
			<-release
			return nil, nil
		})
	}()
	<-entered
	_, err := instance.Execute(context.Background(), func(context.Context) (interface{}, error) { return nil, nil })
	if !errors.Is(err, ErrConcurrencyLimit) {
		t.Fatalf("expected concurrency limit, got %v", err)
	}
	close(release)
	group.Wait()
}

func waitState(t *testing.T, instance *Breaker, target State) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if instance.State() == target {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("state did not become %s", target)
}

func mustNew(t *testing.T, name string, opts ...Option) *Breaker {
	t.Helper()
	instance, err := New(name, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return instance
}

func TestStateListenerCanForceAnotherTransition(t *testing.T) {
	instance := mustNew(t, "listener", WithMinRequests(1))
	instance.SubscribeType(EventStateChanged, func(event Event) {
		change := event.Data.(StateChange)
		if change.To == StateOpen {
			instance.ForceState(StateClosed)
		}
	})
	done := make(chan struct{})
	go func() {
		_, _ = instance.Execute(context.Background(), func(context.Context) (interface{}, error) {
			return nil, errors.New("boom")
		})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("state listener deadlocked during a reentrant transition")
	}
	if instance.State() != StateClosed {
		t.Fatalf("state = %s", instance.State())
	}
}

func TestProbeTransitionCompletesBeforeExecuteReturns(t *testing.T) {
	instance := mustNew(t, "probe-sync")
	instance.ForceState(StateHalfOpen)
	locked := true
	defer func() {
		if locked {
			instance.transitionMu.Unlock()
		}
	}()
	done := make(chan error, 1)
	entered := make(chan struct{})
	go func() {
		_, err := instance.Execute(context.Background(), func(context.Context) (interface{}, error) {
			instance.transitionMu.Lock()
			close(entered)
			return nil, nil
		})
		done <- err
	}()
	<-entered
	returnedEarly := false
	select {
	case <-done:
		returnedEarly = true
	case <-time.After(50 * time.Millisecond):
	}
	instance.transitionMu.Unlock()
	locked = false
	if returnedEarly {
		t.Fatal("probe execution returned before its state transition completed")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("probe execution did not complete")
	}
	if instance.State() != StateClosed {
		t.Fatalf("state = %s", instance.State())
	}
}

func TestStaleProbeTransitionDoesNotOverrideState(t *testing.T) {
	instance := mustNew(t, "stale-probe")
	instance.ForceState(StateOpen)
	instance.transitionFrom(StateHalfOpen, StateClosed, "stale_probe", time.Now().UTC())
	if instance.State() != StateOpen {
		t.Fatalf("stale probe changed state to %s", instance.State())
	}
}

func TestForceClosedResetsWindow(t *testing.T) {
	instance := mustNew(t, "force-closed")
	_, _ = instance.Execute(context.Background(), func(context.Context) (interface{}, error) { return nil, nil })
	if instance.Snapshot().Window.Total != 1 {
		t.Fatal("request was not recorded")
	}
	instance.ForceState(StateClosed)
	if instance.Snapshot().Window.Total != 0 {
		t.Fatal("forcing closed did not reset the window")
	}
}

type blockingWindow struct {
	recording chan struct{}
	release   chan struct{}
	resetting chan struct{}
}

func (w *blockingWindow) Record(ResultType)        { close(w.recording); <-w.release }
func (w *blockingWindow) Snapshot() WindowSnapshot { return WindowSnapshot{} }
func (w *blockingWindow) Reset()                   { close(w.resetting) }

func TestResetWaitsForWindowRecord(t *testing.T) {
	instance := mustNew(t, "reset-lock")
	window := &blockingWindow{make(chan struct{}), make(chan struct{}), make(chan struct{})}
	instance.window = window
	recorded := make(chan struct{})
	go func() {
		instance.recordRegular(&Result{Type: ResultSucceeded})
		close(recorded)
	}()
	<-window.recording
	reset := make(chan struct{})
	go func() {
		instance.Reset()
		close(reset)
	}()
	resetStartedEarly := false
	select {
	case <-window.resetting:
		resetStartedEarly = true
	case <-time.After(50 * time.Millisecond):
	}
	close(window.release)
	<-recorded
	<-reset
	if resetStartedEarly {
		t.Fatal("window reset ran concurrently with a record")
	}
}

func TestProbeFailureSuppressesConcurrentSuccess(t *testing.T) {
	instance := mustNew(t, "probe-failure", WithProbeCount(2), WithSuccessThreshold(1))
	instance.ForceState(StateHalfOpen)
	epoch := instance.probeEpoch.Load()
	instance.transitionMu.Lock()
	failed := make(chan struct{})
	go func() {
		instance.finishProbe(false, epoch)
		close(failed)
	}()
	deadline := time.Now().Add(time.Second)
	for {
		instance.probeMu.Lock()
		failureSeen := instance.probeFailed
		instance.probeMu.Unlock()
		if failureSeen {
			break
		}
		if time.Now().After(deadline) {
			instance.transitionMu.Unlock()
			t.Fatal("failed probe did not register")
		}
		time.Sleep(time.Millisecond)
	}
	instance.finishProbe(true, epoch)
	instance.probeMu.Lock()
	succeeded := instance.probeSucceeded
	instance.probeMu.Unlock()
	instance.transitionMu.Unlock()
	<-failed
	if succeeded != 0 || instance.State() != StateOpen {
		t.Fatalf("success overrode failure: succeeded=%d state=%s", succeeded, instance.State())
	}
}

func TestConcurrentProbeFailureReopensAfterEarlySuccess(t *testing.T) {
	instance := mustNew(t, "late-probe-failure", WithProbeCount(2), WithSuccessThreshold(1))
	instance.ForceState(StateHalfOpen)
	successRelease := make(chan struct{})
	failureRelease := make(chan struct{})
	entered := make(chan struct{}, 2)
	results := make(chan error, 2)
	go func() {
		_, err := instance.Execute(context.Background(), func(context.Context) (interface{}, error) {
			entered <- struct{}{}
			<-successRelease
			return nil, nil
		})
		results <- err
	}()
	go func() {
		_, err := instance.Execute(context.Background(), func(context.Context) (interface{}, error) {
			entered <- struct{}{}
			<-failureRelease
			return nil, errors.New("boom")
		})
		results <- err
	}()
	<-entered
	<-entered
	close(successRelease)
	if err := <-results; err != nil {
		t.Fatal(err)
	}
	if instance.State() != StateClosed {
		t.Fatalf("successful probe did not close breaker: %s", instance.State())
	}
	close(failureRelease)
	if err := <-results; err == nil {
		t.Fatal("failed probe unexpectedly succeeded")
	}
	if instance.State() != StateOpen {
		t.Fatalf("late failed probe left breaker %s", instance.State())
	}
}
