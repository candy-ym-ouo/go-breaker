package breaker

import (
	"testing"
	"time"
)

func TestBug01RegistryCreationListenerCanReenter(t *testing.T) {
	registry := NewRegistry()
	registry.Subscribe(func(event Event) {
		if event.Type == EventBreakerCreated {
			registry.List()
		}
	})
	done := make(chan struct{})
	go func() {
		registry.GetOrCreate("service")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("GetOrCreate deadlocked while the creation listener reentered the registry")
	}
}
