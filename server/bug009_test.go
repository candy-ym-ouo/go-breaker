package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"go-breaker/breaker"
)

func TestBug09FilteredEventsDoNotPolluteHistory(t *testing.T) {
	registry := breaker.NewRegistry()
	registry.GetOrCreate("a")
	registry.GetOrCreate("b")
	handler := New(registry).Handler()
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/api/events?resource=a", nil))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/events", nil))
	var events []EventView
	if err := json.Unmarshal(response.Body.Bytes(), &events); err != nil {
		t.Fatal(err)
	}
	resources := map[string]int{}
	for _, event := range events {
		if event.Type == breaker.EventBreakerCreated.String() {
			resources[event.Resource]++
		}
	}
	if resources["a"] != 1 || resources["b"] != 1 {
		t.Fatalf("creation event history was polluted: %v", resources)
	}
}
