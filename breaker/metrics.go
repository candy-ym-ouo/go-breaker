package breaker

import "sync/atomic"

const (
	latencyBucketWidth = int64(10)
	latencyBucketCount = 128
)

type metrics struct {
	totalRequests, succeeded, failed, timeouts, rejectedB, rejectedC, retries atomic.Int64
	probeSuccess, probeFailed, latencyTotalMs, latencyCount                   atomic.Int64
	latency                                                                   [latencyBucketCount]atomic.Int64
}

type MetricsSnapshot struct {
	TotalRequests         int64   `json:"total_requests"`
	Succeeded             int64   `json:"succeeded"`
	Failed                int64   `json:"failed"`
	Timeouts              int64   `json:"timeouts"`
	RejectedByBreaker     int64   `json:"rejected_by_breaker"`
	RejectedByConcurrency int64   `json:"rejected_by_concurrency"`
	Retries               int64   `json:"retries"`
	ProbeSuccess          int64   `json:"probe_success"`
	ProbeFailed           int64   `json:"probe_failed"`
	InFlight              int64   `json:"in_flight"`
	AvgLatencyMs          float64 `json:"avg_latency_ms"`
	P95LatencyMs          int64   `json:"p95_latency_ms"`
}

func (m *metrics) begin() {
	m.totalRequests.Add(1)
}

func (m *metrics) retry() { m.retries.Add(1) }

func (m *metrics) record(result ResultType, latencyMs int64) {
	switch result {
	case ResultSucceeded:
		m.succeeded.Add(1)
	case ResultFailed:
		m.failed.Add(1)
	case ResultTimeout:
		m.timeouts.Add(1)
	case ResultRejectedByBreaker:
		m.rejectedB.Add(1)
	case ResultRejectedByConcurrency:
		m.rejectedC.Add(1)
	}
	if result == ResultSucceeded || result == ResultFailed || result == ResultTimeout {
		m.recordLatency(latencyMs)
	}
}

func (m *metrics) recordProbe(success bool, latencyMs int64) {
	if success {
		m.probeSuccess.Add(1)
	} else {
		m.probeFailed.Add(1)
	}
	m.recordLatency(latencyMs)
}

func (m *metrics) recordLatency(latencyMs int64) {
	if latencyMs < 0 {
		latencyMs = 0
	}
	m.latencyTotalMs.Add(latencyMs)
	m.latencyCount.Add(1)
	index := latencyMs / latencyBucketWidth
	if index >= latencyBucketCount {
		index = latencyBucketCount - 1
	}
	m.latency[index].Add(1)
}

func (m *metrics) snapshot(inFlight int64) MetricsSnapshot {
	value := MetricsSnapshot{
		TotalRequests:         m.totalRequests.Load(),
		Succeeded:             m.succeeded.Load(),
		Failed:                m.failed.Load(),
		Timeouts:              m.timeouts.Load(),
		RejectedByBreaker:     m.rejectedB.Load(),
		RejectedByConcurrency: m.rejectedC.Load(),
		Retries:               m.retries.Load(),
		ProbeSuccess:          m.probeSuccess.Load(),
		ProbeFailed:           m.probeFailed.Load(),
		InFlight:              inFlight,
	}
	count := m.latencyCount.Load()
	if count > 0 {
		value.AvgLatencyMs = float64(m.latencyTotalMs.Load()) / float64(count)
		value.P95LatencyMs = m.percentile(count, 0.95)
	}
	return value
}

func (m *metrics) percentile(total int64, percentile float64) int64 {
	target := int64(float64(total)*percentile + 0.999999)
	if target < 1 {
		target = 1
	}
	var seen int64
	for index := range m.latency {
		seen += m.latency[index].Load()
		if seen >= target {
			return int64(index) * latencyBucketWidth
		}
	}
	return int64(latencyBucketCount-1) * latencyBucketWidth
}

func (m *metrics) reset() {
	m.totalRequests.Store(0)
	m.succeeded.Store(0)
	m.failed.Store(0)
	m.timeouts.Store(0)
	m.rejectedB.Store(0)
	m.rejectedC.Store(0)
	m.retries.Store(0)
	m.probeSuccess.Store(0)
	m.probeFailed.Store(0)
	m.latencyTotalMs.Store(0)
	m.latencyCount.Store(0)
	for index := range m.latency {
		m.latency[index].Store(0)
	}
}
