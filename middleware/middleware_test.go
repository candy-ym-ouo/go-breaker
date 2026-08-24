package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"go-breaker/breaker"
)

func TestMiddlewareClassifiesResponses(t *testing.T) {
	registry := breaker.NewRegistry()
	guard := New(registry, WithResourceFunc(func(*http.Request) string { return "service" }))
	next := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Query().Get("fail") == "1" {
			writer.WriteHeader(http.StatusInternalServerError)
			return
		}
		writer.WriteHeader(http.StatusNoContent)
	})
	recorder := httptest.NewRecorder()
	guard.Wrap(next).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status=%d", recorder.Code)
	}
	recorder = httptest.NewRecorder()
	guard.Wrap(next).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/?fail=1", nil))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d", recorder.Code)
	}
	if registry.List()[0].Snapshot().Metrics.Failed != 1 {
		t.Fatal("failure was not recorded")
	}
}

func TestMiddlewareOpenResponse(t *testing.T) {
	registry := breaker.NewRegistry()
	instance := registry.GetOrCreate("service")
	instance.ForceState(breaker.StateOpen)
	guard := New(registry, WithResourceFunc(func(*http.Request) string { return "service" }))
	recorder := httptest.NewRecorder()
	guard.Wrap(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("downstream should not run")
	})).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
	if recorder.Code != http.StatusServiceUnavailable || recorder.Header().Get("X-Breaker-State") != "open" {
		t.Fatalf("response: %d %v", recorder.Code, recorder.Header())
	}
}
