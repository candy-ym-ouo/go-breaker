package breaker

import (
	"sort"
	"sync"
	"time"
)

type Registry struct {
	mu       sync.RWMutex
	breakers map[string]*Breaker
	events   *eventBus
}

func NewRegistry() *Registry {
	return &Registry{
		breakers: make(map[string]*Breaker),
		events:   newEventBus(500),
	}
}

func (r *Registry) Get(name string) (*Breaker, bool) {
	r.mu.RLock()
	breaker, ok := r.breakers[name]
	r.mu.RUnlock()
	return breaker, ok
}

func (r *Registry) GetOrCreate(name string, opts ...Option) *Breaker {
	if existing, ok := r.Get(name); ok {
		return existing
	}
	r.mu.Lock()
	if existing, ok := r.breakers[name]; ok {
		r.mu.Unlock()
		return existing
	}
	breaker, err := New(name, opts...)
	if err != nil {
		breaker, _ = New(name)
	}
	breaker.Subscribe(r.events.publish)
	r.breakers[name] = breaker
	r.events.publish(Event{
		Type:     EventBreakerCreated,
		Resource: name,
		Data:     map[string]string{"name": name},
	})
	r.mu.Unlock()
	return breaker
}

func (r *Registry) List() []*Breaker {
	r.mu.RLock()
	values := make([]*Breaker, 0, len(r.breakers))
	for _, breaker := range r.breakers {
		values = append(values, breaker)
	}
	r.mu.RUnlock()
	sort.Slice(values, func(i, j int) bool {
		return values[i].Name() < values[j].Name()
	})
	return values
}

func (r *Registry) Remove(name string) bool {
	r.mu.Lock()
	_, exists := r.breakers[name]
	if exists {
		delete(r.breakers, name)
	}
	r.mu.Unlock()
	if exists {
		r.events.publish(Event{
			Type:     EventBreakerRemoved,
			Resource: name,
			Data:     map[string]string{"name": name},
		})
	}
	return exists
}

func (r *Registry) ResetAll() {
	for _, breaker := range r.List() {
		breaker.Reset()
	}
}

func (r *Registry) UpdateConfigAll(config Config) error {
	if err := config.Validate(); err != nil {
		return err
	}
	for _, breaker := range r.List() {
		if err := breaker.UpdateConfig(config); err != nil {
			return err
		}
	}
	return nil
}

func (r *Registry) Subscribe(listener func(Event)) {
	r.events.subscribe(Listener(listener))
}

func (r *Registry) RecentEvents(limit int) []Event {
	return r.events.recent(limit)
}

func (r *Registry) PublishMetricSnapshots(now time.Time) {
	for _, instance := range r.List() {
		snapshot := instance.Snapshot()
		instance.events.publish(Event{Type: EventMetricSnapshot, Resource: instance.Name(), Time: now, Data: snapshot.Metrics})
	}
}
