package breaker

import "fmt"

// State is the lifecycle state of a circuit breaker.
type State int32

const (
	StateClosed State = iota
	StateOpen
	StateHalfOpen
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half_open"
	default:
		return "unknown"
	}
}

func (s State) Color() string {
	switch s {
	case StateClosed:
		return "green"
	case StateOpen:
		return "red"
	case StateHalfOpen:
		return "yellow"
	default:
		return "gray"
	}
}

func ParseState(value string) (State, error) {
	switch value {
	case "closed":
		return StateClosed, nil
	case "open":
		return StateOpen, nil
	case "half_open", "half-open":
		return StateHalfOpen, nil
	default:
		return StateClosed, fmt.Errorf("%w: unknown state %q", ErrConfigInvalid, value)
	}
}

type StateChange struct {
	From   State  `json:"from"`
	To     State  `json:"to"`
	Reason string `json:"reason"`
}
