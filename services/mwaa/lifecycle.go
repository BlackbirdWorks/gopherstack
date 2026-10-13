package mwaa

import "time"

// SetTransitionDelay makes environments stay CREATING/UPDATING/DELETING for d before settling;
// zero keeps the observe-once default.
func (b *InMemoryBackend) SetTransitionDelay(d time.Duration) { b.transitionNanos.Store(int64(d)) }

func (b *InMemoryBackend) timedTransitions() bool { return b.transitionNanos.Load() > 0 }

func (b *InMemoryBackend) afterTransition(fn func()) {
	time.AfterFunc(time.Duration(b.transitionNanos.Load()), func() {
		b.mu.Lock("lifecycle")
		defer b.mu.Unlock()

		fn()
	})
}

// settleEnvironment moves key from the transient status to AVAILABLE after the delay.
func (b *InMemoryBackend) settleEnvironment(key, from string) {
	if !b.timedTransitions() {
		return
	}

	b.afterTransition(func() {
		env, ok := b.environments.Get(key)
		if !ok || env.Status != from {
			return
		}

		env.Status = envStatusAvailable

		if env.LastUpdate != nil && env.LastUpdate.Status == lastUpdatePending {
			env.LastUpdate.Status = lastUpdateSuccess
		}
	})
}

func (b *InMemoryBackend) removeEnvironmentAfterDelay(region, name string) {
	key := regionKey(region, name)

	b.afterTransition(func() {
		if env, ok := b.environments.Get(key); ok && env.Status == envStatusDeleting {
			b.environments.Delete(key)
			delete(b.metricsStore(region), name)
		}
	})
}
