package server

import "go-breaker/breaker"

func aggregateMetrics(registry *breaker.Registry) GlobalMetrics {
	values := registry.List()
	result := GlobalMetrics{Breakers: len(values)}
	for _, instance := range values {
		snapshot := instance.Snapshot()
		result.Total += snapshot.Metrics.TotalRequests
		result.Succeeded += snapshot.Metrics.Succeeded
		result.Failed += snapshot.Metrics.Failed
		result.Timeouts += snapshot.Metrics.Timeouts
		result.Rejected += snapshot.Metrics.RejectedByBreaker
		result.Rejected += snapshot.Metrics.RejectedByConcurrency
		result.InFlight += snapshot.Metrics.InFlight
	}
	completed := result.Succeeded + result.Failed + result.Timeouts
	if completed > 0 {
		result.ErrorRate = float64(result.Failed+result.Timeouts) / float64(completed)
	}
	return result
}
