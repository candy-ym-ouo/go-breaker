package main

import (
	"testing"
	"time"

	"go-breaker/breaker"
)

func TestBug10StopAllIsIdempotentAndTrafficCanRestart(t *testing.T) {
	registry := breaker.NewRegistry()
	instance := registry.GetOrCreate("service")
	simulator := NewSimulator(registry)
	simulator.StopAll()
	simulator.StopAll()
	simulator.Start(Injection{Resource: "service", ExpiresAt: time.Now().UTC().Add(time.Second)})
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if instance.Snapshot().Metrics.TotalRequests > 0 {
			simulator.StopAll()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("traffic did not restart after StopAll")
}
