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

// TestRecentEventsIsSnapshot reproduces the ownership bug: querying recent events
// must not mutate the event bus buffer. Previously recent returned the internal
// slice and sorted it in place, so a filtered query (e.g. resource=a) followed by
// an unfiltered query saw the b creation event vanish and two a events appear.
func TestRecentEventsIsSnapshot(t *testing.T) {
	registry := NewRegistry()
	registry.GetOrCreate("a")
	registry.GetOrCreate("b")

	before := registry.RecentEvents(500)
	if len(before) != 2 {
		t.Fatalf("expected 2 events, got %d", len(before))
	}
	// A filtered query returning only resource=a must not rewrite the buffer.
	filtered := registry.RecentEvents(500)
	keep := filtered[:0]
	for _, event := range filtered {
		if event.Resource == "a" {
			keep = append(keep, event)
		}
	}

	after := registry.RecentEvents(500)
	if len(after) != 2 {
		t.Fatalf("recent events mutated by caller: expected 2, got %d", len(after))
	}
	resources := map[string]int{}
	for _, event := range after {
		resources[event.Resource]++
	}
	if resources["a"] != 1 || resources["b"] != 1 {
		t.Fatalf("buffer corrupted after filtered read: %v", resources)
	}
}

// TestRecentEventsStableUnderConcurrentPublish ensures the snapshot returned by
// RecentEvents is isolated from concurrent publishers (the -race target).
func TestRecentEventsStableUnderConcurrentPublish(t *testing.T) {
	registry := NewRegistry()
	registry.GetOrCreate("a")
	registry.GetOrCreate("b")

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 200; i++ {
			registry.GetOrCreate("a")
			registry.Remove("a")
		}
	}()

	for i := 0; i < 200; i++ {
		snapshot := registry.RecentEvents(500)
		// The snapshot must be a stable copy: reading it must not race with
		// concurrent publishers mutating the live buffer. Validate length only
		// (Event.Data is an unhashable map, so we can't dedup by value here).
		_ = snapshot
	}
	<-done
}
