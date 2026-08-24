package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go-breaker/breaker"
)

func TestStopAllStopsGeneratedTraffic(t *testing.T) {
	registry := breaker.NewRegistry()
	instance := registry.GetOrCreate("service")
	simulator := NewSimulator(registry)
	simulator.Start(Injection{Resource: "service", ExpiresAt: time.Now().Add(3 * time.Second)})
	deadline := time.Now().Add(time.Second)
	for instance.Snapshot().Metrics.TotalRequests == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if instance.Snapshot().Metrics.TotalRequests == 0 {
		t.Fatal("traffic generator did not start")
	}
	simulator.StopAll()
	time.Sleep(250 * time.Millisecond)
	stoppedAt := instance.Snapshot().Metrics.TotalRequests
	time.Sleep(500 * time.Millisecond)
	if got := instance.Snapshot().Metrics.TotalRequests; got != stoppedAt {
		t.Fatalf("traffic continued after stop: before=%d after=%d", stoppedAt, got)
	}
}

func TestSimulateHandlerReturnsNormalizedInjection(t *testing.T) {
	registry := breaker.NewRegistry()
	registry.GetOrCreate("service")
	simulator := NewSimulator(registry)
	defer simulator.StopAll()
	request := httptest.NewRequest(http.MethodPost, "/api/demo/simulate", strings.NewReader(
		`{"resource":"service","fail_rate":0,"latency_ms":0,"seconds":1}`,
	))
	response := httptest.NewRecorder()
	simulator.SimulateHandler(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var injection Injection
	if err := json.Unmarshal(response.Body.Bytes(), &injection); err != nil {
		t.Fatal(err)
	}
	if injection.StartedAt.IsZero() || !injection.ExpiresAt.After(injection.StartedAt) {
		t.Fatalf("invalid normalized injection: %+v", injection)
	}
}

func TestSimulateHandlerRejectsTrailingJSON(t *testing.T) {
	registry := breaker.NewRegistry()
	registry.GetOrCreate("service")
	simulator := NewSimulator(registry)
	request := httptest.NewRequest(http.MethodPost, "/api/demo/simulate", strings.NewReader(
		`{"resource":"service","fail_rate":0,"latency_ms":0,"seconds":1} {}`,
	))
	response := httptest.NewRecorder()
	simulator.SimulateHandler(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestStopAllIsIdempotent(t *testing.T) {
	registry := breaker.NewRegistry()
	instance := registry.GetOrCreate("service")
	simulator := NewSimulator(registry)
	simulator.Start(Injection{Resource: "service", ExpiresAt: time.Now().Add(3 * time.Second)})
	waitForTraffic(t, instance, time.Second)

	// Calling StopAll more than once must not panic ("close of closed channel").
	simulator.StopAll()
	simulator.StopAll()
	simulator.StopAll()
}

func TestReinjectAfterStopProducesFreshTraffic(t *testing.T) {
	registry := breaker.NewRegistry()
	instance := registry.GetOrCreate("service")
	simulator := NewSimulator(registry)

	simulator.Start(Injection{Resource: "service", ExpiresAt: time.Now().Add(3 * time.Second)})
	waitForTraffic(t, instance, time.Second)

	simulator.StopAll()
	stoppedAt := instance.Snapshot().Metrics.TotalRequests
	// Give lingering goroutines a chance to flush in-flight requests.
	time.Sleep(300 * time.Millisecond)
	if got := instance.Snapshot().Metrics.TotalRequests; got > stoppedAt+1 {
		t.Fatalf("traffic continued after stop: before=%d after=%d", stoppedAt, got)
	}

	// A new round of injection must obtain a usable stop signal and produce traffic again.
	simulator.Start(Injection{Resource: "service", ExpiresAt: time.Now().Add(3 * time.Second)})
	waitForTraffic(t, instance, time.Second)
	simulator.StopAll()
}

func waitForTraffic(t *testing.T, instance *breaker.Breaker, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for instance.Snapshot().Metrics.TotalRequests == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if instance.Snapshot().Metrics.TotalRequests == 0 {
		t.Fatal("traffic generator did not start")
	}
}

