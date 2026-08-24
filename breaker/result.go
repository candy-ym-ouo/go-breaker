package breaker

import (
	"context"
	"time"
)

type invocationContext struct {
	context.Context
	cancel context.CancelFunc
}

func newInvocationContext(parent context.Context, timeout time.Duration) *invocationContext {
	ctx, cancel := context.WithTimeout(parent, timeout)
	return &invocationContext{Context: ctx, cancel: cancel}
}

// ResultType classifies one request handled by a breaker.
type ResultType int32

const (
	ResultSucceeded ResultType = iota
	ResultFailed
	ResultTimeout
	ResultRejectedByBreaker
	ResultRejectedByConcurrency
)

func (t ResultType) String() string {
	switch t {
	case ResultSucceeded:
		return "succeeded"
	case ResultFailed:
		return "failed"
	case ResultTimeout:
		return "timeout"
	case ResultRejectedByBreaker:
		return "rejected_by_breaker"
	case ResultRejectedByConcurrency:
		return "rejected_by_concurrency"
	default:
		return "unknown"
	}
}

// Result describes an execution or rejection.
type Result struct {
	Type      ResultType `json:"type"`
	Err       error      `json:"-"`
	LatencyMs int64      `json:"latency_ms"`
	StartedAt time.Time  `json:"started_at"`
}
