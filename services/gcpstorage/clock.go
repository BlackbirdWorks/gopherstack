package gcpstorage

import (
	"time"
)

type clockFn func() time.Time

func (b *InMemoryBackend) now() time.Time {
	if fn := b.clock.Load(); fn != nil {
		return (*fn)()
	}

	return time.Now()
}

// SetClock replaces the time source used for timestamps, retention, lifecycle and signed-URL expiry.
func (b *InMemoryBackend) SetClock(fn func() time.Time) {
	if fn == nil {
		b.clock.Store(nil)

		return
	}

	f := clockFn(fn)
	b.clock.Store(&f)
}
