package workmail

import (
	"fmt"
)

// --- Email Monitoring Configuration ---

// PutEmailMonitoringConfiguration sets email monitoring config for an org.
func (b *InMemoryBackend) PutEmailMonitoringConfiguration(
	orgID, roleARN, logGroupARN string,
) error {
	b.mu.Lock("PutEmailMonitoringConfiguration")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrOrganizationNotFound, orgID)
	}
	b.emailMonitoring.Put(&EmailMonitoringConfiguration{
		RoleARN:     roleARN,
		LogGroupARN: logGroupARN,
		orgID:       orgID,
	})

	return nil
}

// DeleteEmailMonitoringConfiguration removes email monitoring config for an org.
func (b *InMemoryBackend) DeleteEmailMonitoringConfiguration(orgID string) error {
	b.mu.Lock("DeleteEmailMonitoringConfiguration")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrOrganizationNotFound, orgID)
	}
	b.emailMonitoring.Delete(orgID)

	return nil
}

// DescribeEmailMonitoringConfiguration returns email monitoring config for an org.
func (b *InMemoryBackend) DescribeEmailMonitoringConfiguration(
	orgID string,
) (*EmailMonitoringConfiguration, error) {
	b.mu.RLock("DescribeEmailMonitoringConfiguration")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrOrganizationNotFound, orgID)
	}
	cfg, ok := b.emailMonitoring.Get(orgID)
	if !ok {
		return &EmailMonitoringConfiguration{}, nil
	}

	return cfg, nil
}
