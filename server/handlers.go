package server

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"go-breaker/breaker"
)

func (s *Server) registerAPI() {
	s.mux.HandleFunc("/api/health", s.healthHandler)
	s.mux.HandleFunc("/api/metrics", s.metricsHandler)
	s.mux.HandleFunc("/api/events", s.eventsHandler)
	s.mux.HandleFunc("/api/breakers", s.breakersHandler)
	s.mux.HandleFunc("/api/breakers/", s.breakerHandler)
}

func (s *Server) healthHandler(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		methodNotAllowed(writer, http.MethodGet)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) metricsHandler(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		methodNotAllowed(writer, http.MethodGet)
		return
	}
	writeJSON(writer, http.StatusOK, aggregateMetrics(s.registry))
}

func (s *Server) breakersHandler(writer http.ResponseWriter, request *http.Request) {
	switch request.Method {
	case http.MethodGet:
		items := make([]BreakerSummary, 0)
		for _, instance := range s.registry.List() {
			items = append(items, summary(instance.Snapshot()))
		}
		writeJSON(writer, http.StatusOK, items)
	default:
		methodNotAllowed(writer, http.MethodGet)
	}
}

func (s *Server) breakerHandler(writer http.ResponseWriter, request *http.Request) {
	name, action, ok := parseBreakerPath(request.URL.Path)
	if !ok {
		writeError(writer, http.StatusNotFound, breaker.ErrBreakerNotFound)
		return
	}
	instance, exists := s.registry.Get(name)
	if !exists {
		writeError(writer, http.StatusNotFound, breaker.ErrBreakerNotFound)
		return
	}
	if action == "" {
		s.handleBreakerResource(writer, request, name, instance)
		return
	}
	switch action {
	case "config":
		s.configHandler(writer, request, instance)
	case "reset":
		s.resetHandler(writer, request, instance)
	case "state":
		s.stateHandler(writer, request, instance)
	case "probe":
		s.probeHandler(writer, request, instance)
	default:
		writeError(writer, http.StatusNotFound, breaker.ErrBreakerNotFound)
	}
}

func (s *Server) handleBreakerResource(writer http.ResponseWriter, request *http.Request, name string, instance *breaker.Breaker) {
	if request.Method != http.MethodGet {
		methodNotAllowed(writer, http.MethodGet)
		return
	}
	writeJSON(writer, http.StatusOK, detail(instance.Snapshot()))
}

func (s *Server) configHandler(writer http.ResponseWriter, request *http.Request, instance *breaker.Breaker) {
	if request.Method != http.MethodPut {
		methodNotAllowed(writer, http.MethodPut)
		return
	}
	var view ConfigView
	if err := decodeJSON(request, &view); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	config := applyConfigView(instance.Config(), view)
	if err := instance.UpdateConfig(config); err != nil {
		writeError(writer, http.StatusUnprocessableEntity, err)
		return
	}
	writeJSON(writer, http.StatusOK, detail(instance.Snapshot()))
}

func (s *Server) resetHandler(writer http.ResponseWriter, request *http.Request, instance *breaker.Breaker) {
	if request.Method != http.MethodPost {
		methodNotAllowed(writer, http.MethodPost)
		return
	}
	instance.Reset()
	writeJSON(writer, http.StatusOK, detail(instance.Snapshot()))
}

func (s *Server) stateHandler(writer http.ResponseWriter, request *http.Request, instance *breaker.Breaker) {
	if request.Method != http.MethodPost {
		methodNotAllowed(writer, http.MethodPost)
		return
	}
	var body struct {
		State string `json:"state"`
	}
	if err := decodeJSON(request, &body); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	state, err := breaker.ParseState(body.State)
	if err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	instance.ForceState(state)
	writeJSON(writer, http.StatusOK, detail(instance.Snapshot()))
}

func (s *Server) probeHandler(writer http.ResponseWriter, request *http.Request, instance *breaker.Breaker) {
	if request.Method != http.MethodPost {
		methodNotAllowed(writer, http.MethodPost)
		return
	}
	triggered := instance.TriggerProbe()
	writeJSON(writer, http.StatusOK, map[string]interface{}{
		"triggered": triggered,
		"state":     instance.State().String(),
	})
}

func (s *Server) eventsHandler(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		methodNotAllowed(writer, http.MethodGet)
		return
	}
	limit := 100
	if raw := request.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > 500 {
			writeError(writer, http.StatusBadRequest, errors.New("limit must be between 1 and 500"))
			return
		}
		limit = parsed
	}
	var since time.Time
	if raw := request.URL.Query().Get("since"); raw != "" {
		parsed, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			writeError(writer, http.StatusBadRequest, errors.New("since must be RFC3339"))
			return
		}
		since = parsed
	}
	resource := request.URL.Query().Get("resource")
	events := s.registry.RecentEvents(500)
	filtered := events[:0]
	for _, event := range events {
		if !since.IsZero() && !event.Time.After(since) {
			continue
		}
		if resource != "" && event.Resource != resource {
			continue
		}
		filtered = append(filtered, event)
		if len(filtered) == limit {
			break
		}
	}
	views := make([]EventView, 0, len(filtered))
	for _, event := range filtered {
		views = append(views, eventView(event))
	}
	writeJSON(writer, http.StatusOK, views)
}

func parseBreakerPath(path string) (string, string, bool) {
	value := strings.TrimPrefix(path, "/api/breakers/")
	value = strings.Trim(value, "/")
	if value == "" {
		return "", "", false
	}
	parts := strings.Split(value, "/")
	if len(parts) > 2 {
		return "", "", false
	}
	if len(parts) == 1 {
		return parts[0], "", true
	}
	return parts[0], parts[1], true
}

func decodeJSON(request *http.Request, target interface{}) error {
	decoder := json.NewDecoder(io.LimitReader(request.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return errors.New("request body must contain one JSON object")
	}
	return nil
}

func writeJSON(writer http.ResponseWriter, status int, value interface{}) {
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}

func writeError(writer http.ResponseWriter, status int, err error) {
	writeJSON(writer, status, map[string]string{"error": err.Error()})
}

func methodNotAllowed(writer http.ResponseWriter, methods ...string) {
	writer.Header().Set("Allow", strings.Join(methods, ", "))
	writeError(writer, http.StatusMethodNotAllowed, errors.New("method not allowed"))
}
