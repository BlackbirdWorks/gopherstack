package resourcegroups

import (
	"fmt"
	"time"
)

// AccountLifecycleEventStatus constants.
const (
	accountLifecycleEventsActive     = "ACTIVE"
	accountLifecycleEventsInactive   = "INACTIVE"
	accountLifecycleEventsInProgress = "IN_PROGRESS"
)

// GetAccountSettings returns the account-level settings.
func (b *InMemoryBackend) GetAccountSettings() AccountSettings {
	b.mu.RLock("GetAccountSettings")
	defer b.mu.RUnlock()

	settings := b.accountSettings
	if b.lifecycleDelay > 0 && time.Since(b.accountSettingsAt) < b.lifecycleDelay {
		settings.GroupLifecycleEventsStatus = accountLifecycleEventsInProgress
	}

	return settings
}

// UpdateAccountSettings updates the account-level lifecycle event desired status.
func (b *InMemoryBackend) UpdateAccountSettings(desiredStatus string) error {
	if desiredStatus != accountLifecycleEventsActive &&
		desiredStatus != accountLifecycleEventsInactive {
		return fmt.Errorf(
			"%w: GroupLifecycleEventsDesiredStatus must be %s or %s",
			ErrValidation,
			accountLifecycleEventsActive,
			accountLifecycleEventsInactive,
		)
	}

	b.mu.Lock("UpdateAccountSettings")
	defer b.mu.Unlock()

	b.accountSettings.GroupLifecycleEventsDesiredStatus = desiredStatus
	b.accountSettings.GroupLifecycleEventsStatus = desiredStatus
	b.accountSettingsAt = time.Now()

	return nil
}
