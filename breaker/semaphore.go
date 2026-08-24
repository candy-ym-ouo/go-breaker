package breaker

import (
	"sync/atomic"
	"time"
)

type Semaphore struct {
	ch    chan struct{}
	count atomic.Int64
	max   int
}

func NewSemaphore(max int) *Semaphore {
	if max < 1 {
		max = 1
	}
	return &Semaphore{
		ch:  make(chan struct{}, max),
		max: max,
	}
}

func (s *Semaphore) Acquire(timeout time.Duration) bool {
	if timeout <= 0 {
		select {
		case s.ch <- struct{}{}:
			s.count.Add(1)
			return true
		default:
			return false
		}
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case s.ch <- struct{}{}:
		s.count.Add(1)
		return true
	case <-timer.C:
		return false
	}
}

func (s *Semaphore) TryAcquire() bool {
	return s.Acquire(0)
}

func (s *Semaphore) Release() {
	select {
	case <-s.ch:
		s.count.Add(-1)
	default:
	}
}

// Complete releases one acquired permit back to the semaphore.
//
// A permit is a lease: once Acquire succeeds it must be released exactly once,
// regardless of whether the work it guarded succeeded, returned a business error,
// timed out, panicked, or was cancelled by a context. Failing to release on any
// outcome leaks the slot and, under MaxConcurrency=1, permanently starves all
// later callers with ErrConcurrencyLimit. Release is idempotent, so an extra
// release is a harmless no-op rather than a double-free.
func (s *Semaphore) Complete(success bool) {
	s.Release()
}

func (s *Semaphore) Count() int64 {
	return s.count.Load()
}

func (s *Semaphore) Max() int {
	return s.max
}
