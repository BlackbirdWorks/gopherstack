package resourcegroups

import (
	"context"
	"time"
)

const (
	configStatusUpdating       = "UPDATING"
	configStatusUpdateComplete = "UPDATE_COMPLETE"
)

// pendingConfig tracks an in-flight group configuration update. It is transient:
// after a restore the update counts as complete.
type pendingConfig struct {
	at       time.Time
	previous []GroupConfigurationItem
}

// SetLifecycleDelay keeps group configuration updates in UPDATING and account-settings
// changes in IN_PROGRESS for d (default 0: immediately complete).
func (b *InMemoryBackend) SetLifecycleDelay(d time.Duration) {
	b.mu.Lock("SetLifecycleDelay")
	defer b.mu.Unlock()

	b.lifecycleDelay = d
}

// markConfigUpdating records an update that replaced previous. Caller holds the write lock.
func (b *InMemoryBackend) markConfigUpdating(region, name string, previous []GroupConfigurationItem) {
	if b.lifecycleDelay <= 0 {
		return
	}

	b.pendingConfigs[regionKey(region, name)] = pendingConfig{at: time.Now(), previous: cloneConfigItems(previous)}
}

// GroupConfigurationState reports the configuration status of a group and, while it
// is UPDATING, the configuration that is still in effect.
func (b *InMemoryBackend) GroupConfigurationState(
	ctx context.Context,
	nameOrARN string,
) (string, []GroupConfigurationItem) {
	b.mu.RLock("GroupConfigurationState")
	defer b.mu.RUnlock()

	p, ok := b.pendingConfigs[regionKey(getRegion(ctx, b.region), resolveGroupName(nameOrARN))]
	if !ok || b.lifecycleDelay <= 0 || time.Since(p.at) >= b.lifecycleDelay {
		return configStatusUpdateComplete, nil
	}

	return configStatusUpdating, cloneConfigItems(p.previous)
}
