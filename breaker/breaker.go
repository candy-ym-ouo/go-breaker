package breaker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type Snapshot struct {
	Resource       string          `json:"resource"`
	State          State           `json:"state"`
	Config         Config          `json:"config"`
	Window         WindowSnapshot  `json:"window"`
	Metrics        MetricsSnapshot `json:"metrics"`
	OpenedAt       time.Time       `json:"opened_at,omitempty"`
	StateChangedAt time.Time       `json:"state_changed_at"`
}

type Breaker struct {
	resource                            string
	config                              atomic.Pointer[Config]
	state                               atomic.Int32
	openedAt, openUntil, stateChangedAt atomic.Int64
	transitionMu                        sync.Mutex
	probeMu                             sync.Mutex
	probeTaken, probeSucceeded          int
	probeFailed                         bool
	probeEpoch                          atomic.Int64
	windowMu                            sync.RWMutex
	window                              Window
	semaphoreMu                         sync.RWMutex
	semaphore                           *Semaphore
	metrics                             metrics
	events                              *eventBus
}

type callOutput struct {
	value interface{}
	err   error
}

func New(name string, opts ...Option) (*Breaker, error) {
	settings := options{config: DefaultConfig()}
	for _, option := range opts {
		if option != nil {
			option(&settings)
		}
	}
	if err := settings.config.Validate(); err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	breaker := &Breaker{
		resource:  name,
		window:    newSlidingWindow(settings.config.WindowSize, settings.config.BucketDuration, time.Now),
		semaphore: NewSemaphore(settings.config.MaxConcurrency),
		events:    newEventBus(500),
	}
	breaker.config.Store(cloneConfig(settings.config))
	breaker.state.Store(int32(StateClosed))
	breaker.stateChangedAt.Store(now.UnixNano())
	for _, listener := range settings.listeners {
		breaker.Subscribe(listener)
	}
	return breaker, nil
}

func (b *Breaker) Name() string { return b.resource }

func (b *Breaker) Execute(ctx context.Context, fn func(context.Context) (interface{}, error)) (interface{}, error) {
	value, _, err := b.ExecuteWithResult(ctx, fn)
	return value, err
}

func (b *Breaker) ExecuteWithResult(ctx context.Context, fn func(context.Context) (interface{}, error)) (interface{}, *Result, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	started := time.Now().UTC()
	b.metrics.begin()
	probe, rejected := b.admit(started)
	if rejected != nil {
		return b.reject(rejected, started)
	}
	if probe == 0 {
		semaphore := b.currentSemaphore()
		if !semaphore.Acquire(b.Config().AcquireTimeout) {
			reason := ReasonConcurrencyLimit
			return b.reject(&reason, started)
		}
		defer semaphore.Release()
	}
	value, err, timedOut := b.invoke(ctx, fn)
	latency := time.Since(started).Milliseconds()
	if timedOut {
		result := &Result{Type: ResultTimeout, Err: ErrTimeout, LatencyMs: latency, StartedAt: started}
		b.complete(result, probe)
		fallback, fallbackErr := b.degrade(ReasonTimeout, result)
		return fallback, result, fallbackErr
	}
	if err != nil {
		wrapped := fmt.Errorf("%w: %v", ErrCallFailed, err)
		result := &Result{Type: ResultFailed, Err: wrapped, LatencyMs: latency, StartedAt: started}
		b.complete(result, probe)
		fallback, fallbackErr := b.degrade(ReasonCallFailed, result)
		return fallback, result, fallbackErr
	}
	result := &Result{Type: ResultSucceeded, LatencyMs: latency, StartedAt: started}
	b.complete(result, probe)
	return value, result, nil
}

func (b *Breaker) admit(now time.Time) (int64, *Reason) {
	state := b.State()
	if state == StateOpen {
		if now.UnixNano() < b.openUntil.Load() {
			reason := ReasonBreakerOpen
			return 0, &reason
		}
		b.transitionFrom(StateOpen, StateHalfOpen, "sleep_window_elapsed", now)
		state = b.State()
	}
	if state != StateHalfOpen {
		return 0, nil
	}
	b.transitionMu.Lock()
	defer b.transitionMu.Unlock()
	b.probeMu.Lock()
	defer b.probeMu.Unlock()
	if b.State() != StateHalfOpen {
		return 0, nil
	}
	if b.probeTaken >= b.Config().ProbeCount {
		reason := ReasonBreakerOpen
		return 0, &reason
	}
	b.probeTaken++
	return b.probeEpoch.Load(), nil
}

func (b *Breaker) reject(reason *Reason, started time.Time) (interface{}, *Result, error) {
	resultType := ResultRejectedByBreaker
	if *reason == ReasonConcurrencyLimit {
		resultType = ResultRejectedByConcurrency
	}
	result := &Result{
		Type:      resultType,
		Err:       reason.Error(),
		LatencyMs: time.Since(started).Milliseconds(),
		StartedAt: started,
	}
	b.recordRegular(result)
	value, err := b.degrade(*reason, result)
	return value, result, err
}

func (b *Breaker) invoke(ctx context.Context, fn func(context.Context) (interface{}, error)) (interface{}, error, bool) {
	if fn == nil {
		return nil, fmt.Errorf("nil call function"), false
	}
	callCtx, cancel := context.WithTimeout(ctx, b.Config().CallTimeout)
	defer cancel()
	output := make(chan callOutput, 1)
	go func() {
		var result callOutput
		defer func() {
			if recovered := recover(); recovered != nil {
				result.err = fmt.Errorf("panic: %v", recovered)
			}
			output <- result
		}()
		result.value, result.err = fn(callCtx)
	}()
	select {
	case result := <-output:
		return result.value, result.err, false
	case <-callCtx.Done():
		return nil, callCtx.Err(), true
	}
}

func (b *Breaker) complete(result *Result, probe int64) {
	if probe != 0 {
		success := result.Type == ResultSucceeded
		b.metrics.recordProbe(success, result.LatencyMs)
		b.publishResult(result)
		b.finishProbe(success, probe)
		return
	}
	b.recordRegular(result)
	if result.Type == ResultFailed || result.Type == ResultTimeout {
		b.maybeOpen()
	}
}

func (b *Breaker) recordRegular(result *Result) {
	b.windowMu.RLock()
	b.window.Record(result.Type)
	b.windowMu.RUnlock()
	b.metrics.record(result.Type, result.LatencyMs)
	b.publishResult(result)
}

func (b *Breaker) publishResult(result *Result) {
	if !b.Config().EnableResultEvent {
		return
	}
	b.events.publish(Event{
		Type:     EventRequestResult,
		Resource: b.resource,
		Data:     result,
	})
}

func (b *Breaker) finishProbe(success bool, epoch int64) {
	b.probeMu.Lock()
	state := b.State()
	if b.probeEpoch.Load() != epoch || success && state != StateHalfOpen || !success && state != StateHalfOpen && state != StateClosed {
		b.probeMu.Unlock()
		return
	}
	target := State(-1)
	reason := ""
	if !success {
		b.probeSucceeded = 0
		b.probeFailed = true
		target, reason = StateOpen, "probe_failed"
	} else if !b.probeFailed {
		b.probeSucceeded++
		if b.probeSucceeded >= b.Config().SuccessThreshold {
			target, reason = StateClosed, "probe_succeeded"
		}
	}
	b.probeMu.Unlock()
	if target >= StateClosed {
		b.transitionProbe(epoch, target, reason, time.Now().UTC())
	}
}

func (b *Breaker) maybeOpen() {
	if b.State() != StateClosed {
		return
	}
	b.windowMu.RLock()
	snapshot := b.window.Snapshot()
	b.windowMu.RUnlock()
	config := b.Config()
	if snapshot.Total < config.MinRequests {
		return
	}
	if snapshot.ErrorRate < config.ErrorThreshold {
		return
	}
	b.transitionFrom(StateClosed, StateOpen, "error_rate_exceeded", time.Now().UTC())
}

func (b *Breaker) transition(target State, reason string, now time.Time) {
	b.transitionFrom(State(-1), target, reason, now)
}

func (b *Breaker) transitionFrom(expected, target State, reason string, now time.Time) bool {
	return b.transitionChecked(expected, 0, target, reason, now)
}

func (b *Breaker) transitionProbe(epoch int64, target State, reason string, now time.Time) bool {
	return b.transitionChecked(State(-1), epoch, target, reason, now)
}

func (b *Breaker) transitionChecked(expected State, epoch int64, target State, reason string, now time.Time) bool {
	if target < StateClosed || target > StateHalfOpen {
		return false
	}
	b.transitionMu.Lock()
	current := b.State()
	invalidProbe := epoch != 0 && (b.probeEpoch.Load() != epoch || target == StateClosed && current != StateHalfOpen || target == StateOpen && current != StateHalfOpen && current != StateClosed)
	if expected >= StateClosed && current != expected || invalidProbe {
		b.transitionMu.Unlock()
		return false
	}
	changed := current != target
	b.probeMu.Lock()
	b.probeTaken, b.probeSucceeded = 0, 0
	b.probeFailed = false
	if target == StateHalfOpen || epoch == 0 || target == StateOpen {
		b.probeEpoch.Add(1)
	}
	b.probeMu.Unlock()
	switch target {
	case StateOpen:
		b.openedAt.Store(now.UnixNano())
		b.openUntil.Store(now.Add(b.Config().SleepWindow).UnixNano())
	case StateClosed:
		b.openUntil.Store(0)
		b.windowMu.Lock()
		b.window.Reset()
		b.windowMu.Unlock()
	case StateHalfOpen:
		b.openUntil.Store(0)
	}
	if changed {
		b.state.Store(int32(target))
		b.stateChangedAt.Store(now.UnixNano())
	}
	b.transitionMu.Unlock()
	if changed {
		b.events.publish(Event{Type: EventStateChanged, Resource: b.resource, Time: now, Data: StateChange{From: current, To: target, Reason: reason}})
	}
	return true
}

func (b *Breaker) degrade(reason Reason, result *Result) (interface{}, error) {
	value, err := b.Config().Fallback.Execute(reason, result)
	if err != nil && !isKnownExecutionError(err) {
		return nil, fmt.Errorf("%w: %v", ErrFallbackFailed, err)
	}
	return value, err
}

func isKnownExecutionError(err error) bool {
	return errors.Is(err, ErrTimeout) ||
		errors.Is(err, ErrCallFailed) ||
		errors.Is(err, ErrBreakerOpen) ||
		errors.Is(err, ErrConcurrencyLimit)
}

func (b *Breaker) State() State {
	return State(b.state.Load())
}

func (b *Breaker) Config() Config {
	return *b.config.Load()
}

func (b *Breaker) Snapshot() Snapshot {
	b.windowMu.RLock()
	window := b.window.Snapshot()
	b.windowMu.RUnlock()
	semaphore := b.currentSemaphore()
	return Snapshot{
		Resource:       b.resource,
		State:          b.State(),
		Config:         b.Config(),
		Window:         window,
		Metrics:        b.metrics.snapshot(semaphore.Count()),
		OpenedAt:       unixTime(b.openedAt.Load()),
		StateChangedAt: unixTime(b.stateChangedAt.Load()),
	}
}

func (b *Breaker) UpdateConfig(config Config) error {
	if err := config.Validate(); err != nil {
		return err
	}
	previous := b.Config()
	if previous.WindowSize != config.WindowSize || previous.BucketDuration != config.BucketDuration {
		b.window = newSlidingWindow(config.WindowSize, config.BucketDuration, time.Now)
	}
	if previous.MaxConcurrency != config.MaxConcurrency {
		b.semaphore = NewSemaphore(config.MaxConcurrency)
	}
	b.config.Store(cloneConfig(config))
	b.events.publish(Event{
		Type:     EventConfigChanged,
		Resource: b.resource,
		Data:     map[string]interface{}{"updated": true},
	})
	return nil
}

func (b *Breaker) Reset() {
	b.windowMu.Lock()
	b.window.Reset()
	b.windowMu.Unlock()
	b.metrics.reset()
	b.probeMu.Lock()
	b.probeTaken, b.probeSucceeded = 0, 0
	b.probeFailed = false
	b.probeMu.Unlock()
}

func (b *Breaker) ForceState(state State) {
	b.transition(state, "forced", time.Now().UTC())
}

func (b *Breaker) TriggerProbe() bool {
	state := b.State()
	if state == StateOpen {
		return b.transitionFrom(StateOpen, StateHalfOpen, "manual_probe", time.Now().UTC())
	}
	return state == StateHalfOpen
}

func (b *Breaker) Subscribe(listener Listener) {
	b.events.subscribe(listener)
}

func (b *Breaker) SubscribeType(eventType EventType, listener Listener) {
	b.events.subscribeType(eventType, listener)
}

func (b *Breaker) RecentEvents(limit int) []Event {
	return b.events.recent(limit)
}

func (b *Breaker) currentSemaphore() *Semaphore {
	b.semaphoreMu.RLock()
	value := b.semaphore
	b.semaphoreMu.RUnlock()
	return value
}

func unixTime(stamp int64) time.Time {
	if stamp <= 0 {
		return time.Time{}
	}
	return time.Unix(0, stamp).UTC()
}
