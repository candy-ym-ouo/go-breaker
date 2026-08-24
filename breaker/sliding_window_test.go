package breaker

import (
	"sync"
	"testing"
	"time"
)

func TestSlidingWindowAggregationAndExpiry(t *testing.T) {
	now := time.Unix(100, 0)
	window := newSlidingWindow(3, time.Second, func() time.Time { return now })
	window.Record(ResultSucceeded)
	window.Record(ResultSucceeded)
	window.Record(ResultFailed)
	window.Record(ResultTimeout)
	window.Record(ResultRejectedByBreaker)
	snapshot := window.Snapshot()
	if snapshot.Total != 4 || snapshot.Failed+snapshot.Timeouts != 2 || snapshot.RejectedB != 1 {
		t.Fatalf("unexpected snapshot: %+v", snapshot)
	}
	if snapshot.ErrorRate != 0.5 {
		t.Fatalf("error rate = %v", snapshot.ErrorRate)
	}
	now = now.Add(4 * time.Second)
	if got := window.Snapshot(); got.Total != 0 || got.Rejected() != 0 {
		t.Fatalf("expired data remained: %+v", got)
	}
}

func TestSlidingWindowConcurrentRecord(t *testing.T) {
	window := newSlidingWindow(10, time.Second, time.Now)
	var group sync.WaitGroup
	for i := 0; i < 20; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for j := 0; j < 500; j++ {
				window.Record(ResultSucceeded)
			}
		}()
	}
	group.Wait()
	if total := window.Snapshot().Total; total != 10000 {
		t.Fatalf("total = %d", total)
	}
	window.Reset()
	if window.Snapshot().Total != 0 || window.Snapshot().Rejected() != 0 {
		t.Fatal("reset did not clear window")
	}
}
