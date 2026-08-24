package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"go-breaker/breaker"
)

func TestBug04ConfigEventWithUnchangedWindowIsSafe(t *testing.T) {
	registry := breaker.NewRegistry()
	registry.GetOrCreate("service")
	handler := New(registry).Handler()
	update := httptest.NewRequest(http.MethodPut, "/api/breakers/service/config", bytes.NewBufferString(`{"error_threshold":0.7}`))
	updateResponse := httptest.NewRecorder()
	handler.ServeHTTP(updateResponse, update)
	if updateResponse.Code != http.StatusOK {
		t.Fatalf("config update status=%d body=%s", updateResponse.Code, updateResponse.Body.String())
	}
	eventsResponse := httptest.NewRecorder()
	handler.ServeHTTP(eventsResponse, httptest.NewRequest(http.MethodGet, "/api/events", nil))
	if eventsResponse.Code != http.StatusOK {
		t.Fatalf("events status=%d body=%s", eventsResponse.Code, eventsResponse.Body.String())
	}
	var events []EventView
	if err := json.Unmarshal(eventsResponse.Body.Bytes(), &events); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, event := range events {
		found = found || event.Type == breaker.EventConfigChanged.String()
	}
	if !found {
		t.Fatal("config change event was not returned")
	}
}
