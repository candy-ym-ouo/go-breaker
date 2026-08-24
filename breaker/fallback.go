package breaker

import "fmt"

type FallbackType int32

const (
	FallbackReturnErr FallbackType = iota
	FallbackDefaultValue
	FallbackCustomFunc
)

type Reason int32

const (
	ReasonBreakerOpen Reason = iota
	ReasonConcurrencyLimit
	ReasonTimeout
	ReasonCallFailed
)

func (r Reason) Error() error {
	switch r {
	case ReasonBreakerOpen:
		return ErrBreakerOpen
	case ReasonConcurrencyLimit:
		return ErrConcurrencyLimit
	case ReasonTimeout:
		return ErrTimeout
	default:
		return ErrCallFailed
	}
}

type FallbackFunc func(reason Reason, result *Result) (interface{}, error)

type Fallback struct {
	Type  FallbackType
	Value interface{}
	Func  FallbackFunc
}

func (f Fallback) Execute(reason Reason, result *Result) (value interface{}, err error) {
	original := reason.Error()
	if result != nil && result.Err != nil {
		original = result.Err
	}
	switch f.Type {
	case FallbackDefaultValue:
		return f.Value, nil
	case FallbackCustomFunc:
		if f.Func == nil {
			return nil, fmt.Errorf("%w: custom function is nil", ErrFallbackFailed)
		}
		defer func() {
			if recovered := recover(); recovered != nil {
				value = nil
				err = fmt.Errorf("%w: panic: %v", ErrFallbackFailed, recovered)
			}
		}()
		return f.Func(reason, result)
	case FallbackReturnErr:
		fallthrough
	default:
		return nil, fmt.Errorf("fallback execution: %w", original)
	}
}
