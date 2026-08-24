package breaker

import (
	"context"
	"errors"
	"testing"
)

func TestBug03BreakerOpenErrorRemainsDiscoverable(t *testing.T) {
	instance := mustNew(t, "service")
	instance.ForceState(StateOpen)
	_, err := instance.Execute(context.Background(), func(context.Context) (interface{}, error) {
		t.Fatal("open breaker executed the protected call")
		return nil, nil
	})
	if !errors.Is(err, ErrBreakerOpen) {
		t.Fatalf("errors.Is could not find ErrBreakerOpen in %v", err)
	}
	if !IsBreakerOpen(err) {
		t.Fatalf("IsBreakerOpen rejected %v", err)
	}
}
