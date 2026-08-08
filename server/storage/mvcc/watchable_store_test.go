package mvcc

import (
	"sync"
	"testing"
	"time"
)

func TestWatchableStoreCompactionRaceCondition(t *testing.T) {
	st := &store{compactMainRev: 0, currentRev: 10}
	ws := &watchableStore{
		store:    st,
		synced:   watcherGroup{watchers: make(map[*watcher]struct{})},
		unsynced: watcherGroup{watchers: make(map[*watcher]struct{})},
	}

	var wg sync.WaitGroup
	wg.Add(2)

	// Goroutine 1: Continuous compaction up to revision 5
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			st.compact(5)
			time.Sleep(100 * time.Microsecond)
		}
	}()

	// Goroutine 2: Attempt to create watch starting at compacted revision 5
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			ch := make(chan WatchResponse, 1)
			w, err := ws.watch([]byte("foo"), nil, 5, WatchID(i+1), ch)
			if err == nil && w == nil {
				t.Errorf("watch returned nil watcher without error")
			}
			if err == ErrCompacted {
				// Expected behavior when revision is compacted
				continue
			}
			time.Sleep(100 * time.Microsecond)
		}
	}()

	wg.Wait()
}
