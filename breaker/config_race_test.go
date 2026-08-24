package breaker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// TestUpdateConfigConcurrentSnapshot exercises the window/semaphore replacement
// path under concurrent readers. It fails with DATA RACE on b.window /
// b.semaphore before UpdateConfig took the write locks.
func TestUpdateConfigConcurrentSnapshot(t *testing.T) {
	instance := mustNew(t, "config-race")
	base := DefaultConfig()

	const writers = 8
	const readers = 8
	stop := make(chan struct{})
	var group sync.WaitGroup

	for i := 0; i < writers; i++ {
		group.Add(1)
		go func(n int) {
			defer group.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				cfg := base
				// Fluctuate the two fields that trigger pointer replacement;
				// a constant config never rebuilds the window/semaphore and
				// would not reproduce the race.
				cfg.WindowSize = 8 + (n % 3)
				cfg.MaxConcurrency = 50 + (n % 5)
				if err := instance.UpdateConfig(cfg); err != nil {
					t.Errorf("update failed: %v", err)
					return
				}
			}
		}(i)
	}

	for i := 0; i < readers; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_ = instance.Snapshot()
			}
		}()
	}

	// A second reader population drives recordRegular / maybeOpen /
	// currentSemaphore so the read side of the swapped pointers is stressed
	// alongside Snapshot.
	for i := 0; i < readers; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			call := func(context.Context) (interface{}, error) {
				return nil, errors.New("boom")
			}
			for {
				select {
				case <-stop:
					return
				default:
				}
				_, _ = instance.Execute(context.Background(), call)
			}
		}()
	}

	time.Sleep(300 * time.Millisecond)
	close(stop)
	group.Wait()

	// After the storm settles the breaker must report a single internally
	// consistent, valid config (Config contains a func, so it is not
	// comparable with ==; Validate is the meaningful invariant here).
	final := instance.Snapshot().Config
	if err := final.Validate(); err != nil {
		t.Fatalf("final config invalid: %v", err)
	}
}
