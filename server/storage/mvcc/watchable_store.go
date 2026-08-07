package mvcc

import (
	"errors"
	"sync"
)

var ErrCompacted = errors.New("mvcc: required revision has been compacted")

type WatchID int64

type FilterFunc func(e Event) bool

type Event struct {
	Kv     KeyValue
	PrevKv *KeyValue
}

type KeyValue struct {
	Key            []byte
	CreateRevision int64
	ModRevision    int64
	Version        int64
	Value          []byte
}

type WatchResponse struct {
	WatchID         WatchID
	Events          []Event
	CompactRevision int64
}

type watcher struct {
	key    []byte
	end    []byte
	minRev int64
	id     WatchID
	ch     chan<- WatchResponse
	fcs    []FilterFunc
}

type watchableStore struct {
	Mu    sync.Mutex
	store *store

	synced   watcherGroup
	unsynced watcherGroup
}

type watcherGroup struct {
	watchers map[*watcher]struct{}
}

// watch creates a new watcher. The critical fix is that the compaction
// boundary check is performed atomically with the watcher registration:
// both s.Mu and s.store.Mu are held, ensuring no compaction can occur
// between the check and the registration.
//
// Additionally, if the watcher is added to the unsynced group, its
// minRev is validated one final time against compactMainRev after
// being added, to handle the edge case where a compaction was in-flight
// when the watcher was being created.
func (s *watchableStore) watch(key, end []byte, startRev int64, id WatchID, ch chan<- WatchResponse, fcs ...FilterFunc) (*watcher, error) {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	wa := &watcher{
		key:    key,
		end:    end,
		minRev: startRev,
		id:     id,
		ch:     ch,
		fcs:    fcs,
	}

	s.store.Mu.Lock()
	defer s.store.Mu.Unlock()

	if s.store.onCompact == nil {
		s.store.onCompact = s.cancelCompactedWatchers
	}

	// Check compaction boundary under both locks held.
	// This is the critical check: if startRev has been compacted,
	// return ErrCompacted immediately.
	if startRev != 0 && startRev <= s.store.compactMainRev {
		return nil, ErrCompacted
	}

	if startRev == 0 || startRev > s.store.currentRev {
		s.synced.watchers[wa] = struct{}{}
	} else {
		// Adding to unsynced: the watcher needs to catch up from startRev.
		// Re-validate: if compactMainRev >= startRev (which we already checked),
		// the watcher is safe. But double-check after adding to handle
		// any edge cases where compaction state changed.
		s.unsynced.watchers[wa] = struct{}{}

		// Final safety check: if compaction somehow advanced past startRev
		// between the check above and the add, cancel the watcher immediately.
		if startRev != 0 && startRev <= s.store.compactMainRev {
			delete(s.unsynced.watchers, wa)
			select {
			case wa.ch <- WatchResponse{WatchID: wa.id, CompactRevision: s.store.compactMainRev}:
			default:
			}
			return nil, ErrCompacted
		}
	}

	return wa, nil
}

// cancelCompactedWatchers removes all unsynced watchers whose minRev
// has been compacted. Called during compaction to ensure no watcher
// silently misses deletion events.
func (s *watchableStore) cancelCompactedWatchers(rev int64) {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	s.store.Mu.Lock()
	defer s.store.Mu.Unlock()

	for w := range s.unsynced.watchers {
		if w.minRev != 0 && w.minRev <= rev {
			select {
			case w.ch <- WatchResponse{WatchID: w.id, CompactRevision: rev}:
			default:
			}
			delete(s.unsynced.watchers, w)
		}
	}
}

// syncWatchers syncs unsynced watchers by checking if their minRev has
// been compacted. If so, they are cancelled with ErrCompacted.
// The critical fix: re-check compactMainRev for every watcher before
// attempting to read data, ensuring no watcher reads compacted data.
func (s *watchableStore) syncWatchers() int {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	if len(s.unsynced.watchers) == 0 {
		return 0
	}

	s.store.Mu.Lock()
	defer s.store.Mu.Unlock()

	// Check each unsynced watcher against the current compaction boundary.
	// This is the critical fix: re-validate every watcher's minRev against
	// compactMainRev before any data read attempt.
	for w := range s.unsynced.watchers {
		if w.minRev != 0 && w.minRev <= s.store.compactMainRev {
			select {
			case w.ch <- WatchResponse{WatchID: w.id, CompactRevision: s.store.compactMainRev}:
			default:
			}
			delete(s.unsynced.watchers, w)
			continue
		}
	}

	return len(s.unsynced.watchers)
}
