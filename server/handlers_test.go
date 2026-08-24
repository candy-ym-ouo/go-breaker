package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go-breaker/breaker"
)

func TestBreakerAPI(t *testing.T) {
	registry := breaker.NewRegistry()
	registry.GetOrCreate("payOrder")
	app := New(registry)

	response := request(t, app.Handler(), http.MethodGet, "/api/breakers", nil)
	if response.Code != http.StatusOK {
		t.Fatalf("list status=%d", response.Code)
	}
	var list []BreakerSummary
	if err := json.Unmarshal(response.Body.Bytes(), &list); err != nil || len(list) != 1 {
		t.Fatalf("list: %v %s", err, response.Body.String())
	}

	body := bytes.NewBufferString(`{"error_threshold":0.7,"sleep_window_ms":100}`)
	response = request(t, app.Handler(), http.MethodPut, "/api/breakers/payOrder/config", body)
	if response.Code != http.StatusOK {
		t.Fatalf("update status=%d body=%s", response.Code, response.Body.String())
	}
	if registry.List()[0].Config().ErrorThreshold != 0.7 {
		t.Fatal("config was not updated")
	}

	body = bytes.NewBufferString(`{"state":"open"}`)
	response = request(t, app.Handler(), http.MethodPost, "/api/breakers/payOrder/state", body)
	if response.Code != http.StatusOK || registry.List()[0].State() != breaker.StateOpen {
		t.Fatalf("state update failed: %d", response.Code)
	}
}

func TestBreakerAPIErrors(t *testing.T) {
	app := New(breaker.NewRegistry())
	response := request(t, app.Handler(), http.MethodGet, "/api/breakers/missing", nil)
	if response.Code != http.StatusNotFound {
		t.Fatalf("status=%d", response.Code)
	}
}

func request(t *testing.T, handler http.Handler, method string, path string, body *bytes.Buffer) *httptest.ResponseRecorder {
	t.Helper()
	var request *http.Request
	if body == nil {
		request = httptest.NewRequest(method, path, nil)
	} else {
		request = httptest.NewRequest(method, path, body)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func TestServerPublishesMetricSnapshots(t *testing.T) {
	registry := breaker.NewRegistry()
	registry.GetOrCreate("service")
	events := make(chan breaker.Event, 1)
	registry.Subscribe(func(event breaker.Event) {
		if event.Type == breaker.EventMetricSnapshot {
			select {
			case events <- event:
			default:
			}
		}
	})
	app := New(registry, WithAddr("127.0.0.1:0"), WithMetricsInterval(10*time.Millisecond))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- app.Run(ctx) }()
	select {
	case event := <-events:
		if event.Resource != "service" {
			t.Fatalf("resource=%q", event.Resource)
		}
	case <-time.After(time.Second):
		t.Fatal("metric snapshot event was not published")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
