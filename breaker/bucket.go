package breaker

import (
	"sync/atomic"
	"time"
)

type Bucket struct {
	startAt   atomic.Int64
	succeeded atomic.Int64
	failed    atomic.Int64
	timeouts  atomic.Int64
	rejectedC atomic.Int64
	rejectedB atomic.Int64
}

type BucketSnapshot struct {
	StartAt   time.Time `json:"start"`
	Succeeded int64     `json:"succeeded"`
	Failed    int64     `json:"failed"`
	Timeouts  int64     `json:"timeouts"`
	RejectedC int64     `json:"rejected_c"`
	RejectedB int64     `json:"rejected_b"`
}

func (b *Bucket) reset(start int64) {
	b.succeeded.Store(0)
	b.failed.Store(0)
	b.timeouts.Store(0)
	b.rejectedC.Store(0)
	b.rejectedB.Store(0)
	b.startAt.Store(start)
}

func (b *Bucket) clear() {
	b.reset(0)
}

func (b *Bucket) record(result ResultType) {
	switch result {
	case ResultSucceeded:
		b.succeeded.Add(1)
	case ResultFailed:
		b.failed.Add(1)
	case ResultTimeout:
		b.timeouts.Add(1)
	case ResultRejectedByConcurrency:
		b.rejectedC.Add(1)
	case ResultRejectedByBreaker:
		b.rejectedB.Add(1)
	}
}

func (b *Bucket) snapshot() BucketSnapshot {
	start := b.startAt.Load()
	value := BucketSnapshot{
		Succeeded: b.succeeded.Load(),
		Failed:    b.failed.Load(),
		Timeouts:  b.timeouts.Load(),
		RejectedC: b.rejectedC.Load(),
		RejectedB: b.rejectedB.Load(),
	}
	if start > 0 {
		value.StartAt = time.Unix(0, start).UTC()
	}
	return value
}
