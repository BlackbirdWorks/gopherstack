package gcplogging

import (
	"time"

	"cloud.google.com/go/logging/apiv2/loggingpb"
)

// SetClock replaces the backend clock.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	defer b.mu.Unlock()

	b.now = now
}

// MatchFilter compiles src and evaluates it against e.
func MatchFilter(src string, e *loggingpb.LogEntry) (bool, error) {
	f, err := compileFilter(src)
	if err != nil {
		return false, err
	}

	return f.matches(newDoc(e)), nil
}

// OpenTail registers a tail session over the given resources, as TailLogEntries does.
func (b *InMemoryBackend) OpenTail(names ...string) error {
	_, err := b.newTail(&loggingpb.TailLogEntriesRequest{ResourceNames: names})

	return err
}

// TailAllowed charges n entries to a parent at now and returns how many the quota admitted.
func (b *InMemoryBackend) TailAllowed(parent string, n int, now time.Time) int {
	b.tails.mu.Lock("TailAllowed")
	defer b.tails.mu.Unlock()

	admitted := 0

	for range n {
		if b.tails.allow(parent, now) {
			admitted++
		}
	}

	return admitted
}

// AdmitList charges one ListLogEntries call to the parents of names.
func (b *InMemoryBackend) AdmitList(names ...string) error { return b.admitList(names) }

// QuotaParents reports how many parents the list and tail quotas currently track.
func (b *InMemoryBackend) QuotaParents() (int, int) {
	b.mu.RLock("QuotaParents")
	lists := len(b.listCalls)
	b.mu.RUnlock()

	b.tails.mu.RLock("QuotaParents")
	defer b.tails.mu.RUnlock()

	return lists, len(b.tails.rates)
}

// TailSessions reports the open tail sessions.
func (b *InMemoryBackend) TailSessions() int {
	b.tails.mu.RLock("TailSessions")
	defer b.tails.mu.RUnlock()

	return len(b.tails.subs)
}
