package breaker

import (
	"sort"
	"sync"
	"time"
)

type EventType int32

const (
	EventStateChanged EventType = iota
	EventRequestResult
	EventBreakerCreated
	EventBreakerRemoved
	EventConfigChanged
	EventMetricSnapshot
)

func (t EventType) String() string {
	switch t {
	case EventStateChanged:
		return "state_changed"
	case EventRequestResult:
		return "request_result"
	case EventBreakerCreated:
		return "breaker_created"
	case EventBreakerRemoved:
		return "breaker_removed"
	case EventConfigChanged:
		return "config_changed"
	case EventMetricSnapshot:
		return "metric_snapshot"
	default:
		return "unknown"
	}
}

type Event struct {
	Type     EventType   `json:"type"`
	Resource string      `json:"resource"`
	Time     time.Time   `json:"time"`
	Data     interface{} `json:"data,omitempty"`
}

type Listener func(Event)

type subscription struct {
	typeFilter *EventType
	listener   Listener
}

type eventBus struct {
	mu        sync.RWMutex
	listeners []subscription
	events    []Event
	capacity  int
}

func newEventBus(capacity int) *eventBus {
	if capacity < 1 {
		capacity = 1
	}
	return &eventBus{capacity: capacity}
}

func (b *eventBus) subscribe(listener Listener) {
	if listener == nil {
		return
	}
	b.mu.Lock()
	b.listeners = append(b.listeners, subscription{listener: listener})
	b.mu.Unlock()
}

func (b *eventBus) subscribeType(eventType EventType, listener Listener) {
	if listener == nil {
		return
	}
	filter := eventType
	b.mu.Lock()
	b.listeners = append(b.listeners, subscription{typeFilter: &filter, listener: listener})
	b.mu.Unlock()
}

func (b *eventBus) publish(event Event) {
	if event.Time.IsZero() {
		event.Time = time.Now().UTC()
	}
	b.mu.Lock()
	b.events = append(b.events, event)
	if extra := len(b.events) - b.capacity; extra > 0 {
		copy(b.events, b.events[extra:])
		b.events = b.events[:b.capacity]
	}
	listeners := append([]subscription(nil), b.listeners...)
	b.mu.Unlock()
	for _, item := range listeners {
		if item.typeFilter != nil && *item.typeFilter != event.Type {
			continue
		}
		callListener(item.listener, event)
	}
}

func callListener(listener Listener, event Event) {
	defer func() { _ = recover() }()
	listener(event)
}

func (b *eventBus) recent(limit int) []Event {
	b.mu.RLock()
	// Copy into a fresh slice so callers receive an isolated snapshot. The
	// internal buffer must not escape (callers would otherwise alias it) and
	// must not be reordered, since concurrent publishers rely on the events
	// staying in append order for compaction.
	values := make([]Event, len(b.events))
	copy(values, b.events)
	b.mu.RUnlock()
	sort.SliceStable(values, func(i, j int) bool {
		return values[i].Time.After(values[j].Time)
	})
	if limit <= 0 || limit > len(values) {
		limit = len(values)
	}
	return values[:limit]
}
