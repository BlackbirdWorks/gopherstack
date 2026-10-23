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
