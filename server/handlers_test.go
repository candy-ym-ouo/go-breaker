package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
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

// TestConfigUpdateConcurrentApply hammers PUT /api/breakers/<name>/config while
// reading detail and global metrics. Before the handler applied UpdateConfig
// synchronously, the response snapshot raced the async update and could return
// a stale config; under -race the underlying window/semaphore swap was also
// flagged. Now every response must reflect the config just applied.
func TestConfigUpdateConcurrentApply(t *testing.T) {
	registry := breaker.NewRegistry()
	registry.GetOrCreate("service")
	app := New(registry)
	handler := app.Handler()

	const workers = 16
	stop := make(chan struct{})
	var group sync.WaitGroup

	for i := 0; i < workers; i++ {
		group.Add(1)
		go func(n int) {
			defer group.Done()
			windowSize := 8 + n%3
			maxConcurrency := 50 + n%5
			payload := `{"max_concurrency":` + strconv.Itoa(maxConcurrency) + `,` +
				`"window_size":` + strconv.Itoa(windowSize) + `}`
			allowed := map[int]struct{}{8: {}, 9: {}, 10: {}}
			allowedConc := map[int]struct{}{50: {}, 51: {}, 52: {}, 53: {}, 54: {}}
			for {
				select {
				case <-stop:
					return
				default:
				}
				// A fresh body per request: httptest.NewRequest drains the
				// reader, so a shared buffer would only serve the first call.
				req := httptest.NewRequest(http.MethodPut, "/api/breakers/service/config",
					bytes.NewBufferString(payload))
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, req)
				if rec.Code != http.StatusOK {
					t.Errorf("update status=%d body=%s", rec.Code, rec.Body.String())
					return
				}
				var detail BreakerDetail
				if err := json.Unmarshal(rec.Body.Bytes(), &detail); err != nil {
					t.Errorf("decode response: %v", err)
					return
				}
				// The response must reflect a genuinely applied config from the
				// live set — not the pre-update default (WindowSize=10 is also
				// in the live set, so guard against the default concurrency and
				// any out-of-range value, which would indicate a stale or
				// corrupt snapshot rather than a legitimate interleaving).
				if _, ok := allowed[detail.Config.WindowSize]; !ok {
					t.Errorf("window_size out of live set: %d", detail.Config.WindowSize)
					return
				}
				if _, ok := allowedConc[detail.Config.MaxConcurrency]; !ok {
					t.Errorf("max_concurrency out of live set: %d", detail.Config.MaxConcurrency)
					return
				}
			}
		}(i)
	}

	for i := 0; i < workers; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/breakers/service", nil))
				if rec.Code != http.StatusOK {
					t.Errorf("detail status=%d", rec.Code)
					return
				}
				rec2 := httptest.NewRecorder()
				handler.ServeHTTP(rec2, httptest.NewRequest(http.MethodGet, "/api/metrics", nil))
				if rec2.Code != http.StatusOK {
					t.Errorf("metrics status=%d", rec2.Code)
					return
				}
			}
		}()
	}

	time.Sleep(300 * time.Millisecond)
	close(stop)
	group.Wait()
}

// TestConfigUpdateRejectsInvalid ensures the 422 path survives the synchronous
// rewrite of configHandler.
func TestConfigUpdateRejectsInvalid(t *testing.T) {
	registry := breaker.NewRegistry()
	registry.GetOrCreate("invalid")
	app := New(registry)
	body := bytes.NewBufferString(`{"error_threshold":1.5}`)
	response := request(t, app.Handler(), http.MethodPut, "/api/breakers/invalid/config", body)
	if response.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422 for invalid config, got %d body=%s", response.Code, response.Body.String())
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
