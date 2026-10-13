package sesv2

// VdmOptions captures the VDM (Virtual Deliverability Manager) configuration of a
// configuration set.
type VdmOptions struct {
	DashboardOptions map[string]any `json:"dashboardOptions,omitempty"`
	GuardianOptions  map[string]any `json:"guardianOptions,omitempty"`
}

// PutConfigurationSetVdmOptions stores the VDM options on the config set.
func (b *InMemoryBackend) PutConfigurationSetVdmOptions(
	name string,
	dashboardOptions, guardianOptions map[string]any,
) error {
	b.mu.Lock("PutConfigurationSetVdmOptions")
	defer b.mu.Unlock()

	cs, ok := b.configurationSets.Get(name)
	if !ok {
		return configSetMissing(name)
	}

	cs.VdmOptions = &VdmOptions{
		DashboardOptions: dashboardOptions,
		GuardianOptions:  guardianOptions,
	}

	return nil
}

func (b *InMemoryBackend) PutAccountVdmAttributes(vdmAttributes map[string]any) error {
	b.mu.Lock("PutAccountVdmAttributes")
	defer b.mu.Unlock()

	b.accountLocked().VdmAttributes = vdmAttributes

	return nil
}
