package breaker

import "errors"

var (
	ErrBreakerOpen      = errors.New("breaker: circuit breaker is open")
	ErrConcurrencyLimit = errors.New("breaker: concurrency limit exceeded")
	ErrTimeout          = errors.New("breaker: call timeout")
	ErrCallFailed       = errors.New("breaker: call failed")
	ErrConfigInvalid    = errors.New("breaker: invalid config")
	ErrBreakerNotFound  = errors.New("breaker: not found")
	ErrFallbackFailed   = errors.New("breaker: fallback failed")
)

func IsBreakerOpen(err error) bool {
	return errors.Is(err, ErrBreakerOpen)
}

func IsRejected(err error) bool {
	return errors.Is(err, ErrBreakerOpen) || errors.Is(err, ErrConcurrencyLimit)
}
