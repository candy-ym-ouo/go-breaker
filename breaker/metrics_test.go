package breaker

import "testing"

func TestMetricsP95UsesNearestRank(t *testing.T) {
	var metrics metrics
	metrics.recordLatency(10)
	metrics.recordLatency(100)
	if got := metrics.snapshot(0).P95LatencyMs; got != 100 {
		t.Fatalf("P95 = %d, want 100", got)
	}
}
