package breaker

import (
	"testing"
	"time"
)

func TestSemaphore(t *testing.T) {
	semaphore := NewSemaphore(2)
	if !semaphore.TryAcquire() || !semaphore.TryAcquire() {
		t.Fatal("expected permits")
	}
	if semaphore.TryAcquire() {
		t.Fatal("unexpected extra permit")
	}
	started := time.Now()
	if semaphore.Acquire(30 * time.Millisecond) {
		t.Fatal("unexpected timed permit")
	}
	if time.Since(started) < 20*time.Millisecond {
		t.Fatal("acquire returned too early")
	}
	semaphore.Release()
	semaphore.Release()
	if semaphore.Count() != 0 || semaphore.Max() != 2 {
		t.Fatalf("count=%d max=%d", semaphore.Count(), semaphore.Max())
	}
}
