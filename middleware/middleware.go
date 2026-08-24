package middleware

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"go-breaker/breaker"
)

type Option func(*options)

type options struct {
	resourceFunc  func(*http.Request) string
	callTimeout   time.Duration
	fallback      breaker.Fallback
	defaultBody   []byte
	degradeStatus int
	stateHeader   bool
}

type Handler struct {
	registry *breaker.Registry
	next     http.Handler
	options  options
}

func defaultOptions() options {
	return options{
		resourceFunc:  normalizedResource,
		defaultBody:   []byte("service degraded"),
		degradeStatus: http.StatusServiceUnavailable,
		stateHeader:   true,
		fallback:      breaker.Fallback{Type: breaker.FallbackReturnErr},
	}
}

func New(registry *breaker.Registry, opts ...Option) *Handler {
	if registry == nil {
		registry = breaker.NewRegistry()
	}
	settings := defaultOptions()
	for _, option := range opts {
		if option != nil {
			option(&settings)
		}
	}
	return &Handler{registry: registry, options: settings}
}

func WithResourceFunc(fn func(*http.Request) string) Option {
	return func(o *options) {
		if fn != nil {
			o.resourceFunc = fn
		}
	}
}

func WithCallTimeout(duration time.Duration) Option {
	return func(o *options) { o.callTimeout = duration }
}

func WithFallback(fallback breaker.Fallback) Option {
	return func(o *options) { o.fallback = fallback }
}

func WithDefaultBody(body []byte) Option {
	return func(o *options) { o.defaultBody = append([]byte(nil), body...) }
}

func WithStatusOnDegrade(status int) Option {
	return func(o *options) {
		if status >= 100 && status <= 999 {
			o.degradeStatus = status
		}
	}
}

func WithStateHeader(enabled bool) Option {
	return func(o *options) { o.stateHeader = enabled }
}

func (h *Handler) Wrap(next http.Handler) http.Handler {
	copy := *h
	copy.next = next
	return &copy
}

func (h *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if h.next == nil {
		http.NotFound(writer, request)
		return
	}
	resource := h.options.resourceFunc(request)
	if resource == "" {
		resource = normalizedResource(request)
	}
	instance := h.registry.GetOrCreate(resource, breaker.WithFallback(h.options.fallback))
	ctx, cancel := h.requestContext(request.Context())
	defer cancel()
	capture := newResponseCapture()
	value, result, err := instance.ExecuteWithResult(ctx, func(callCtx context.Context) (interface{}, error) {
		nextRequest := request.Clone(context.Background())
		h.next.ServeHTTP(capture, nextRequest)
		if !capture.Successful() {
			return nil, fmt.Errorf("downstream returned HTTP %d", capture.Status())
		}
		return capture, nil
	})
	if err == nil && result != nil && result.Type == breaker.ResultSucceeded {
		if h.options.stateHeader {
			writer.Header().Set("X-Breaker-State", instance.State().String())
		}
		copyResponse(writer, capture)
		return
	}
	h.writeDegraded(writer, instance, value, result)
}

func (h *Handler) requestContext(parent context.Context) (context.Context, context.CancelFunc) {
	if h.options.callTimeout <= 0 {
		return context.WithCancel(parent)
	}
	return context.WithTimeout(parent, h.options.callTimeout)
}

func (h *Handler) writeDegraded(writer http.ResponseWriter, instance *breaker.Breaker, value interface{}, result *breaker.Result) {
	if h.options.stateHeader {
		writer.Header().Set("X-Breaker-State", instance.State().String())
	}
	if result != nil {
		writer.Header().Set("X-Breaker-Reason", result.Type.String())
	}
	writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
	writer.WriteHeader(h.options.degradeStatus)
	switch typed := value.(type) {
	case []byte:
		_, _ = writer.Write(typed)
	case string:
		_, _ = writer.Write([]byte(typed))
	case nil:
		_, _ = writer.Write(h.options.defaultBody)
	default:
		_, _ = fmt.Fprint(writer, typed)
	}
}
