package gcpredis

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/redisengine"
)

// SetClock replaces the backend clock.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	defer b.mu.Unlock()

	b.now = now
}

// SetJobGate makes every background operation call f before it starts working.
func (b *InMemoryBackend) SetJobGate(f func()) { b.gate = f }

// SetMemoryUnit changes how many bytes one memory_size_gb gigabyte allows, so tests can reach the limit.
func (b *InMemoryBackend) SetMemoryUnit(n int64) {
	b.mu.Lock("SetMemoryUnit")
	defer b.mu.Unlock()

	b.unit = n
}

// EngineSettings returns the tuning currently applied to an instance's Redis server.
func (b *InMemoryBackend) EngineSettings(name string) redisengine.Settings {
	b.mu.RLock("EngineSettings")
	defer b.mu.RUnlock()

	return b.instances[name].engine.Settings()
}

// LiveEngines counts instances that still own a running Redis server.
func (b *InMemoryBackend) LiveEngines() int {
	b.mu.RLock("LiveEngines")
	defer b.mu.RUnlock()

	n := 0

	for _, r := range b.instances {
		if r.engine != nil {
			n++
		}
	}

	return n
}
