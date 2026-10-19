package gcpsecretmanager

import "time"

// SetClock replaces the backend clock.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	defer b.mu.Unlock()

	b.now = now
}

// CompileFilter exposes the filter compiler for tests against a flat item.
func CompileFilter(s string, fields map[string][]string) (bool, error) {
	fn, err := compileFilter(s)
	if err != nil {
		return false, err
	}

	return fn(item(fields)), nil
}
