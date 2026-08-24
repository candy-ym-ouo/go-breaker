package breaker

import "time"

type callOutput struct {
	value interface{}
	err   error
}

// newCallOutputChannel returns the channel used to hand a business call's
// result back to ExecuteWithResult. It carries one buffered slot so that a
// goroutine whose call finishes after the caller has already timed out can
// complete its send and exit instead of blocking forever on an unbuffered
// channel with no receiver. The buffer is never closed: the late result is
// simply discarded, preserving the timeout/panic/normal-return semantics
// while avoiding "send on closed channel".
func newCallOutputChannel() chan callOutput { return make(chan callOutput, 1) }

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
