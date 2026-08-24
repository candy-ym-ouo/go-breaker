package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"go-breaker/breaker"
)

type Injection struct {
	Resource  string    `json:"resource"`
	FailRate  float64   `json:"fail_rate"`
	LatencyMs int64     `json:"latency_ms"`
	StartedAt time.Time `json:"started_at"`
	ExpiresAt time.Time `json:"expires_at"`
}

type Simulator struct {
	mu         sync.RWMutex
	injections map[string]Injection
	registry   *breaker.Registry
	randomMu   sync.Mutex
	random     *rand.Rand
}

func NewSimulator(registry *breaker.Registry) *Simulator {
	return &Simulator{
		injections: make(map[string]Injection),
		registry:   registry,
		random:     rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (s *Simulator) Call(ctx context.Context, resource string) (map[string]interface{}, error) {
	injection, active := s.active(resource)
	if active && injection.LatencyMs > 0 {
		timer := time.NewTimer(time.Duration(injection.LatencyMs) * time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	if active && s.randomFloat() < injection.FailRate {
		return nil, errors.New("simulated upstream failure")
	}
	return map[string]interface{}{
		"resource": resource,
		"ok":       true,
		"time":     time.Now().UTC(),
	}, nil
}

func (s *Simulator) Start(injection Injection) Injection {
	now := time.Now().UTC()
	injection.StartedAt = now
	if injection.ExpiresAt.IsZero() {
		injection.ExpiresAt = now.Add(15 * time.Second)
	}
	s.mu.Lock()
	s.injections[injection.Resource] = injection
	s.mu.Unlock()
	go s.generateTraffic(injection.Resource, injection.ExpiresAt)
	return injection
}

func (s *Simulator) StopAll() {
	s.mu.Lock()
	s.injections = make(map[string]Injection)
	s.mu.Unlock()
}

func (s *Simulator) Status() []Injection {
	now := time.Now().UTC()
	s.mu.Lock()
	values := make([]Injection, 0, len(s.injections))
	for name, injection := range s.injections {
		if !injection.ExpiresAt.After(now) {
			delete(s.injections, name)
			continue
		}
		values = append(values, injection)
	}
	s.mu.Unlock()
	return values
}

func (s *Simulator) active(resource string) (Injection, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	injection, ok := s.injections[resource]
	if !ok || !injection.ExpiresAt.After(time.Now().UTC()) {
		delete(s.injections, resource)
		return Injection{}, false
	}
	return injection, true
}

func (s *Simulator) randomFloat() float64 {
	s.randomMu.Lock()
	value := s.random.Float64()
	s.randomMu.Unlock()
	return value
}

func (s *Simulator) generateTraffic(resource string, expiresAt time.Time) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case now := <-ticker.C:
			injection, active := s.active(resource)
			if !active || !expiresAt.After(now) || !injection.ExpiresAt.Equal(expiresAt) {
				return
			}
			instance, ok := s.registry.Get(resource)
			if !ok {
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_, _ = instance.Execute(ctx, func(callCtx context.Context) (interface{}, error) {
				return s.Call(callCtx, resource)
			})
			cancel()
		}
	}
}

func (s *Simulator) SimulateHandler(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		writeDemoError(writer, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var body struct {
		Resource  string  `json:"resource"`
		FailRate  float64 `json:"fail_rate"`
		LatencyMs int64   `json:"latency_ms"`
		Seconds   int     `json:"seconds"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(writer, request.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&body); err != nil {
		writeDemoError(writer, http.StatusBadRequest, err.Error())
		return
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		writeDemoError(writer, http.StatusBadRequest, "request body must contain one JSON object")
		return
	}
	if _, ok := s.registry.Get(body.Resource); !ok {
		writeDemoError(writer, http.StatusNotFound, "breaker not found")
		return
	}
	if body.FailRate < 0 || body.FailRate > 1 {
		writeDemoError(writer, http.StatusBadRequest, "fail_rate must be between 0 and 1")
		return
	}
	if body.LatencyMs < 0 || body.LatencyMs > 60000 {
		writeDemoError(writer, http.StatusBadRequest, "latency_ms is invalid")
		return
	}
	if body.Seconds < 1 || body.Seconds > 3600 {
		writeDemoError(writer, http.StatusBadRequest, "seconds must be between 1 and 3600")
		return
	}
	injection := Injection{
		Resource:  body.Resource,
		FailRate:  body.FailRate,
		LatencyMs: body.LatencyMs,
		ExpiresAt: time.Now().UTC().Add(time.Duration(body.Seconds) * time.Second),
	}
	injection = s.Start(injection)
	writeDemoJSON(writer, http.StatusOK, injection)
}

func (s *Simulator) StopHandler(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		writeDemoError(writer, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	s.StopAll()
	writeDemoJSON(writer, http.StatusOK, map[string]bool{"stopped": true})
}

func (s *Simulator) StatusHandler(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writeDemoError(writer, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeDemoJSON(writer, http.StatusOK, s.Status())
}

func writeDemoJSON(writer http.ResponseWriter, status int, value interface{}) {
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}

func writeDemoError(writer http.ResponseWriter, status int, message string) {
	writeDemoJSON(writer, status, map[string]string{"error": message})
}
