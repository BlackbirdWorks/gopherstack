package lambdaweb

import "time"

// SetClock replaces the wall clock used for lifecycle deadlines and rate windows; nil restores time.Now.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	defer b.mu.Unlock()

	b.clock = now

	if now == nil {
		b.run.clock.Store(nil)
	} else {
		b.run.clock.Store(&now)
	}
}

// SetLifecycleDelay overrides every Pending/Deleting window; 0 keeps the built-in values.
func (b *InMemoryBackend) SetLifecycleDelay(d time.Duration) {
	b.mu.Lock("SetLifecycleDelay")
	defer b.mu.Unlock()

	b.lifecycleDelay = d
}

// now returns the current time; callers hold b.mu.
func (b *InMemoryBackend) now() time.Time {
	if b.clock != nil {
		return b.clock()
	}

	return time.Now()
}

// dwell returns the configured transition window or def; callers hold b.mu.
func (b *InMemoryBackend) dwell(def time.Duration) time.Duration {
	if b.lifecycleDelay > 0 {
		return b.lifecycleDelay
	}

	return def
}
