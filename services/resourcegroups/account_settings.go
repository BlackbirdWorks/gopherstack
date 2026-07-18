package resourcegroups

import "fmt"

// AccountLifecycleEventStatus constants.
const (
	accountLifecycleEventsActive   = "ACTIVE"
	accountLifecycleEventsInactive = "INACTIVE"
)

// GetAccountSettings returns the account-level settings.
func (b *InMemoryBackend) GetAccountSettings() AccountSettings {
	b.mu.RLock("GetAccountSettings")
	defer b.mu.RUnlock()

	return b.accountSettings
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

	return nil
}
