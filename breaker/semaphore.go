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

func (s *Semaphore) Complete(success bool) {
	if success {
		s.Release()
	}
}

func (s *Semaphore) Count() int64 {
	return s.count.Load()
}

func (s *Semaphore) Max() int {
	return s.max
}
