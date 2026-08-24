package breaker

import (
	"context"
	"testing"
)

func TestBug06EachExecutionGetsFreshContext(t *testing.T) {
	instance := mustNew(t, "service")
	if _, err := instance.Execute(context.Background(), func(context.Context) (interface{}, error) {
		return "first", nil
	}); err != nil {
		t.Fatal(err)
	}
	observed := make(chan error, 1)
	value, err := instance.Execute(context.Background(), func(ctx context.Context) (interface{}, error) {
		observed <- ctx.Err()
		return "second", nil
	})
	if contextErr := <-observed; contextErr != nil {
		t.Fatalf("second execution received a stale context: %v", contextErr)
	}
	if err != nil || value != "second" {
		t.Fatalf("second execution returned value=%v err=%v", value, err)
	}
}
