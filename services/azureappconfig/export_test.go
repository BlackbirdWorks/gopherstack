package azureappconfig

import "time"

// SetNow replaces the backend clock.
func SetNow(b *InMemoryBackend, fn func() time.Time) { b.now = fn }

// ParseFilterMatch reports whether value matches the parsed filter expression.
func ParseFilterMatch(expr, value string) (bool, error) {
	f, err := parseFilter(expr)
	if err != nil {
		return false, err
	}

	return f.matches(value), nil
}

// HistoryLen returns the number of recorded history entries of a store.
func HistoryLen(b *InMemoryBackend, name string) int {
	b.mu.RLock("HistoryLen")
	defer b.mu.RUnlock()

	s, err := b.storeLocked(name)
	if err != nil {
		return -1
	}

	return len(s.History)
}
