package appconfig

// GetAccountSettings returns the account-level AppConfig settings.
func (b *InMemoryBackend) GetAccountSettings() (*AccountSettings, error) {
	b.mu.RLock("GetAccountSettings")
	defer b.mu.RUnlock()

	cp := b.accountSettings

	return &cp, nil
}

// UpdateAccountSettings updates account-level AppConfig settings.
func (b *InMemoryBackend) UpdateAccountSettings(
	deletionProtection *DeletionProtectionSettings,
) (*AccountSettings, error) {
	b.mu.Lock("UpdateAccountSettings")
	defer b.mu.Unlock()

	if deletionProtection != nil {
		b.accountSettings.DeletionProtection = deletionProtection
	}

	cp := b.accountSettings

	return &cp, nil
}
