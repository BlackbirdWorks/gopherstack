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
	vendedMetrics *VendedMetricsSettings,
) (*AccountSettings, error) {
	b.mu.Lock("UpdateAccountSettings")
	defer b.mu.Unlock()

	// DeletionProtection/VendedMetrics are themselves pointer-scalar structs
	// (gopherstack-c8ge): a client updating only Enabled and omitting
	// ProtectionPeriodInMinutes must not wipe a previously-set
	// ProtectionPeriodInMinutes, so merge field by field rather than
	// swapping the sub-struct pointer wholesale.
	if deletionProtection != nil {
		if b.accountSettings.DeletionProtection == nil {
			b.accountSettings.DeletionProtection = &DeletionProtectionSettings{}
		}
		if deletionProtection.Enabled != nil {
			b.accountSettings.DeletionProtection.Enabled = deletionProtection.Enabled
		}
		if deletionProtection.ProtectionPeriodInMinutes != nil {
			b.accountSettings.DeletionProtection.ProtectionPeriodInMinutes = deletionProtection.ProtectionPeriodInMinutes
		}
	}

	if vendedMetrics != nil {
		if b.accountSettings.VendedMetrics == nil {
			b.accountSettings.VendedMetrics = &VendedMetricsSettings{}
		}
		if vendedMetrics.Enabled != nil {
			b.accountSettings.VendedMetrics.Enabled = vendedMetrics.Enabled
		}
	}

	cp := b.accountSettings

	return &cp, nil
}
