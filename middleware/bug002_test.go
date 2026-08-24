package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go-breaker/breaker"
)

func TestBug02RequestCancellationReachesDownstream(t *testing.T) {
	registry := breaker.NewRegistry()
	registry.GetOrCreate("service", breaker.WithCallTimeout(300*time.Millisecond))
	downstreamCanceled := make(chan struct{})
	next := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	next = func(writer http.ResponseWriter, request *http.Request) {
		<-request.Context().Done()
		close(downstreamCanceled)
	}
	handler := New(registry, WithResourceFunc(func(*http.Request) string { return "service" })).Wrap(next)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	request := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx)
	started := time.Now()
	handler.ServeHTTP(httptest.NewRecorder(), request)
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("canceled request returned after %s", elapsed)
	}
	select {
	case <-downstreamCanceled:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("downstream handler did not observe request cancellation")
	}
}
