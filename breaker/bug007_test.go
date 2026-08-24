package breaker

import (
	"context"
	"runtime"
	"testing"
	"time"
)

func TestBug07TimedOutCallsDoNotLeakGoroutines(t *testing.T) {
	instance := mustNew(t, "service", WithCallTimeout(2*time.Millisecond), WithMinRequests(1000))
	runtime.GC()
	baseline := runtime.NumGoroutine()
	for index := 0; index < 12; index++ {
		_, _ = instance.Execute(context.Background(), func(context.Context) (interface{}, error) {
			time.Sleep(8 * time.Millisecond)
			return nil, nil
		})
	}
	time.Sleep(30 * time.Millisecond)
	runtime.GC()
	if leaked := runtime.NumGoroutine() - baseline; leaked > 4 {
		t.Fatalf("timed-out calls left %d goroutines blocked", leaked)
	}
}
