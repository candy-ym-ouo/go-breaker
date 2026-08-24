package breaker

import (
	"sort"
	"sync"
	"time"
)

type slidingWindow struct {
	mu             sync.Mutex
	buckets        []Bucket
	bucketDuration time.Duration
	windowDuration time.Duration
	now            func() time.Time
}

func newSlidingWindow(size int, duration time.Duration, now func() time.Time) *slidingWindow {
	if size < 1 {
		size = 1
	}
	if duration <= 0 {
		duration = time.Second
	}
	if now == nil {
		now = time.Now
	}
	return &slidingWindow{
		buckets:        make([]Bucket, size),
		bucketDuration: duration,
		windowDuration: time.Duration(size) * duration,
		now:            now,
	}
}

func (w *slidingWindow) Record(result ResultType) {
	w.recordAt(result, w.now())
}

func (w *slidingWindow) recordAt(result ResultType, now time.Time) {
	start := w.align(now.UnixNano())
	index := w.index(start)
	bucket := &w.buckets[index]
	if bucket.startAt.Load() != start {
		w.mu.Lock()
		if bucket.startAt.Load() != start {
			bucket.reset(start)
		}
		w.mu.Unlock()
	}
	bucket.record(result)
}

func (w *slidingWindow) Snapshot() WindowSnapshot {
	return w.snapshotAt(w.now())
}

func (w *slidingWindow) snapshotAt(now time.Time) WindowSnapshot {
	cutoff := now.Add(-w.windowDuration).UnixNano()
	future := now.Add(w.bucketDuration).UnixNano()
	result := WindowSnapshot{
		Buckets: make([]BucketSnapshot, 0, len(w.buckets)),
	}
	for index := range w.buckets {
		value := w.buckets[index].snapshot()
		if value.StartAt.IsZero() {
			continue
		}
		stamp := value.StartAt.UnixNano()
		if stamp <= cutoff || stamp >= future {
			continue
		}
		result.Buckets = append(result.Buckets, value)
		result.Succeeded += value.Succeeded
		result.Failed += value.Failed
		result.Timeouts += value.Timeouts
		result.RejectedC += value.RejectedC
		result.RejectedB += value.RejectedB
	}
	sort.Slice(result.Buckets, func(i, j int) bool {
		return result.Buckets[i].StartAt.Before(result.Buckets[j].StartAt)
	})
	result.Total = result.Succeeded + result.Failed + result.Timeouts
	if result.Total > 0 {
		result.ErrorRate = float64(result.Failed+result.Timeouts) / float64(result.Total)
	}
	return result
}

func (w *slidingWindow) Reset() {
	w.mu.Lock()
	defer w.mu.Unlock()
	for index := range w.buckets {
		w.buckets[index].clear()
	}
}

func (w *slidingWindow) align(stamp int64) int64 {
	width := w.bucketDuration.Nanoseconds()
	return stamp - stamp%width
}

func (w *slidingWindow) index(start int64) int {
	sequence := start / w.bucketDuration.Nanoseconds()
	index := sequence % int64(len(w.buckets))
	if index < 0 {
		index += int64(len(w.buckets))
	}
	return int(index)
}
