package breaker

import (
	"testing"
	"time"
)

func TestRegistryLifecycle(t *testing.T) {
	registry := NewRegistry()
	created := 0
	registry.Subscribe(func(event Event) {
		if event.Type == EventBreakerCreated {
			created++
		}
	})
	first := registry.GetOrCreate("a")
	second := registry.GetOrCreate("a")
	registry.GetOrCreate("b")
	if first != second || created != 2 || len(registry.List()) != 2 {
		t.Fatalf("registry mismatch: created=%d list=%d", created, len(registry.List()))
	}
	if !registry.Remove("a") || registry.Remove("a") {
		t.Fatal("remove semantics failed")
	}
}

func TestRegistryCreationListenerCanReadRegistry(t *testing.T) {
	registry := NewRegistry()
	registry.Subscribe(func(event Event) {
		if event.Type == EventBreakerCreated {
			registry.List()
		}
	})
	done := make(chan struct{})
	go func() {
		registry.GetOrCreate("reentrant")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("creation listener deadlocked while reading registry")
	}
}
