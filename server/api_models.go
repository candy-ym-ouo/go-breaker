package server

import (
	"time"

	"go-breaker/breaker"
)

type ConfigView struct {
	WindowSize        *int     `json:"window_size,omitempty"`
	BucketDurationMs  *int64   `json:"bucket_duration_ms,omitempty"`
	ErrorThreshold    *float64 `json:"error_threshold,omitempty"`
	MinRequests       *int64   `json:"min_requests,omitempty"`
	SleepWindowMs     *int64   `json:"sleep_window_ms,omitempty"`
	ProbeCount        *int     `json:"probe_count,omitempty"`
	SuccessThreshold  *int     `json:"success_threshold,omitempty"`
	MaxConcurrency    *int     `json:"max_concurrency,omitempty"`
	AcquireTimeoutMs  *int64   `json:"acquire_timeout_ms,omitempty"`
	CallTimeoutMs     *int64   `json:"call_timeout_ms,omitempty"`
	EnableResultEvent *bool    `json:"enable_result_event,omitempty"`
	MetricSnapshotSec *int     `json:"metric_snapshot_sec,omitempty"`
}

type ConfigResponse struct {
	WindowSize        int     `json:"window_size"`
	BucketDurationMs  int64   `json:"bucket_duration_ms"`
	ErrorThreshold    float64 `json:"error_threshold"`
	MinRequests       int64   `json:"min_requests"`
	SleepWindowMs     int64   `json:"sleep_window_ms"`
	ProbeCount        int     `json:"probe_count"`
	SuccessThreshold  int     `json:"success_threshold"`
	MaxConcurrency    int     `json:"max_concurrency"`
	AcquireTimeoutMs  int64   `json:"acquire_timeout_ms"`
	CallTimeoutMs     int64   `json:"call_timeout_ms"`
	EnableResultEvent bool    `json:"enable_result_event"`
	MetricSnapshotSec int     `json:"metric_snapshot_sec"`
}

type BreakerSummary struct {
	Resource       string    `json:"resource"`
	State          string    `json:"state"`
	StateChangedAt time.Time `json:"state_changed_at"`
	ErrorRate      float64   `json:"error_rate"`
	Total          int64     `json:"total"`
	Succeeded      int64     `json:"succeeded"`
	Failed         int64     `json:"failed"`
	Rejected       int64     `json:"rejected"`
	InFlight       int64     `json:"in_flight"`
	SleepWindowMs  int64     `json:"sleep_window_ms"`
}

type BreakerDetail struct {
	Resource       string                  `json:"resource"`
	State          string                  `json:"state"`
	Config         ConfigResponse          `json:"config"`
	Window         breaker.WindowSnapshot  `json:"window"`
	Metrics        breaker.MetricsSnapshot `json:"metrics"`
	OpenedAt       *time.Time              `json:"opened_at,omitempty"`
	StateChangedAt time.Time               `json:"state_changed_at"`
}

type GlobalMetrics struct {
	Breakers  int     `json:"breakers"`
	Total     int64   `json:"total"`
	Succeeded int64   `json:"succeeded"`
	Failed    int64   `json:"failed"`
	Timeouts  int64   `json:"timeouts"`
	Rejected  int64   `json:"rejected"`
	InFlight  int64   `json:"in_flight"`
	ErrorRate float64 `json:"error_rate"`
}

type EventView struct {
	Type     string      `json:"type"`
	Resource string      `json:"resource"`
	Time     time.Time   `json:"time"`
	Data     interface{} `json:"data,omitempty"`
}

type stateChangeView struct {
	From   string `json:"from"`
	To     string `json:"to"`
	Reason string `json:"reason"`
}

func configResponse(config breaker.Config) ConfigResponse {
	return ConfigResponse{
		WindowSize:        config.WindowSize,
		BucketDurationMs:  config.BucketDuration.Milliseconds(),
		ErrorThreshold:    config.ErrorThreshold,
		MinRequests:       config.MinRequests,
		SleepWindowMs:     config.SleepWindow.Milliseconds(),
		ProbeCount:        config.ProbeCount,
		SuccessThreshold:  config.SuccessThreshold,
		MaxConcurrency:    config.MaxConcurrency,
		AcquireTimeoutMs:  config.AcquireTimeout.Milliseconds(),
		CallTimeoutMs:     config.CallTimeout.Milliseconds(),
		EnableResultEvent: config.EnableResultEvent,
		MetricSnapshotSec: config.MetricSnapshotSec,
	}
}

func applyConfigView(config breaker.Config, view ConfigView) breaker.Config {
	setValue(&config.WindowSize, view.WindowSize)
	setDuration(&config.BucketDuration, view.BucketDurationMs)
	setValue(&config.ErrorThreshold, view.ErrorThreshold)
	setValue(&config.MinRequests, view.MinRequests)
	setDuration(&config.SleepWindow, view.SleepWindowMs)
	setValue(&config.ProbeCount, view.ProbeCount)
	setValue(&config.SuccessThreshold, view.SuccessThreshold)
	setValue(&config.MaxConcurrency, view.MaxConcurrency)
	setDuration(&config.AcquireTimeout, view.AcquireTimeoutMs)
	setDuration(&config.CallTimeout, view.CallTimeoutMs)
	setValue(&config.EnableResultEvent, view.EnableResultEvent)
	setValue(&config.MetricSnapshotSec, view.MetricSnapshotSec)
	return config
}

func setValue[T any](target *T, value *T) {
	if value != nil {
		*target = *value
	}
}

func setDuration(target *time.Duration, milliseconds *int64) {
	if milliseconds != nil {
		*target = time.Duration(*milliseconds) * time.Millisecond
	}
}

func summary(snapshot breaker.Snapshot) BreakerSummary {
	return BreakerSummary{
		Resource:       snapshot.Resource,
		State:          snapshot.State.String(),
		StateChangedAt: snapshot.StateChangedAt,
		ErrorRate:      snapshot.Window.ErrorRate,
		Total:          snapshot.Window.Total,
		Succeeded:      snapshot.Window.Succeeded,
		Failed:         snapshot.Window.Failed + snapshot.Window.Timeouts,
		Rejected:       snapshot.Window.Rejected(),
		InFlight:       snapshot.Metrics.InFlight,
		SleepWindowMs:  snapshot.Config.SleepWindow.Milliseconds(),
	}
}

func detail(snapshot breaker.Snapshot) BreakerDetail {
	value := BreakerDetail{
		Resource:       snapshot.Resource,
		State:          snapshot.State.String(),
		Config:         configResponse(snapshot.Config),
		Window:         snapshot.Window,
		Metrics:        snapshot.Metrics,
		StateChangedAt: snapshot.StateChangedAt,
	}
	if !snapshot.OpenedAt.IsZero() {
		openedAt := snapshot.OpenedAt
		value.OpenedAt = &openedAt
	}
	return value
}

func eventView(event breaker.Event) EventView {
	data := event.Data
	if change, ok := event.Data.(breaker.StateChange); ok {
		data = stateChangeView{
			From:   change.From.String(),
			To:     change.To.String(),
			Reason: change.Reason,
		}
	} else if change, ok := event.Data.(*breaker.ConfigChange); ok {
		data = map[string]bool{"window_changed": change.WindowChanged}
	}
	return EventView{
		Type:     event.Type.String(),
		Resource: event.Resource,
		Time:     event.Time,
		Data:     data,
	}
}
