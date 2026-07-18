package sesv2

import "fmt"

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
		return fmt.Errorf("%w: configuration set %s not found", ErrNotFound, name)
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

	if b.accountDetails == nil {
		b.accountDetails = &AccountDetails{}
	}

	b.accountDetails.VdmAttributes = vdmAttributes

	return nil
}
