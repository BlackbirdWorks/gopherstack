package opensearch

import (
	"fmt"
	"time"
)

// CancelDomainConfigChange cancels a pending configuration change on a domain.
func (b *InMemoryBackend) CancelDomainConfigChange(
	domainName string,
	dryRun bool,
) ([]string, bool, error) {
	if domainName == "" {
		return nil, false, fmt.Errorf("%w: DomainName is required", ErrInvalidParameter)
	}

	b.mu.Lock("CancelDomainConfigChange")
	defer b.mu.Unlock()

	d, exists := b.domains.Get(domainName)
	if !exists {
		return nil, false, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	var cancelledChangeIDs []string

	if d.LastChangeID != "" {
		cancelledChangeIDs = append(cancelledChangeIDs, d.LastChangeID)

		// DryRun reports what WOULD be cancelled without actually cancelling
		// it -- mirrors the UpdateDomainConfig DryRun fix (see PARITY.md).
		if !dryRun {
			d.LastChangeID = ""
		}
	}

	return cancelledChangeIDs, dryRun, nil
}

func applyClusterConfig(d *Domain, input UpdateDomainConfigInput) {
	if input.ClusterConfig != nil {
		d.ClusterConfig = *input.ClusterConfig
	}

	if input.EngineVersion != "" {
		d.EngineVersion = input.EngineVersion
	}
}

func applyStorageConfig(d *Domain, input UpdateDomainConfigInput) {
	if input.EBSOptions != nil {
		d.EBSOptions = input.EBSOptions
	}

	if input.SnapshotOptions != nil {
		d.SnapshotOptions = input.SnapshotOptions
	}
}

func applySecurityConfig(d *Domain, input UpdateDomainConfigInput) {
	if input.EncryptionAtRestOptions != nil {
		d.EncryptionAtRestOptions = input.EncryptionAtRestOptions
	}

	if input.NodeToNodeEncryptionOptions != nil {
		d.NodeToNodeEncryptionOptions = input.NodeToNodeEncryptionOptions
	}

	if input.DomainEndpointOptions != nil {
		d.DomainEndpointOptions = input.DomainEndpointOptions
	}

	if input.AdvancedSecurityOptions != nil {
		d.AdvancedSecurityOptions = input.AdvancedSecurityOptions
	}
}

func applyNetworkConfig(d *Domain, input UpdateDomainConfigInput) {
	if input.VPCOptions != nil {
		d.VPCOptions = input.VPCOptions
	}

	if input.CognitoOptions != nil {
		d.CognitoOptions = input.CognitoOptions
	}
}

func applyOperationalConfig(d *Domain, input UpdateDomainConfigInput) {
	if input.OffPeakWindowOptions != nil {
		d.OffPeakWindowOptions = input.OffPeakWindowOptions
	}

	if input.IdentityCenterOptions != nil {
		d.IdentityCenterOptions = input.IdentityCenterOptions
	}

	if input.EnableSoftwareUpdateOptions != nil {
		d.EnableSoftwareUpdateOptions = input.EnableSoftwareUpdateOptions
	}

	if input.LogPublishingOptions != nil {
		d.LogPublishingOptions = input.LogPublishingOptions
	}

	if input.AccessPolicies != "" {
		d.AccessPolicies = input.AccessPolicies
	}
}

// UpdateDomainConfig updates mutable fields on a domain and records a change ID.
func (b *InMemoryBackend) UpdateDomainConfig(
	name string,
	input UpdateDomainConfigInput,
) (*Domain, error) {
	b.mu.Lock("UpdateDomainConfig")
	defer b.mu.Unlock()

	d, exists := b.domains.Get(name)
	if !exists || deleteWindowElapsed(d, b.clock()) {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, name)
	}

	applyClusterConfig(d, input)
	applyStorageConfig(d, input)
	applySecurityConfig(d, input)
	applyNetworkConfig(d, input)
	applyOperationalConfig(d, input)

	changeID := fmt.Sprintf("change-%s-%d", name, time.Now().UnixNano())
	d.LastChangeID = changeID
	b.beginProcessing(d, dpsModifying)

	cp := *d

	return &cp, nil
}

// PreviewDomainConfig computes the domain configuration that UpdateDomainConfig
// would produce for input, without mutating stored state or advancing the
// processing/change-ID bookkeeping. This backs UpdateDomainConfig's
// DryRun=true mode (aws-sdk-go-v2 UpdateDomainConfigInput.DryRun): AWS
// validates and previews the change but never applies it.
func (b *InMemoryBackend) PreviewDomainConfig(
	name string,
	input UpdateDomainConfigInput,
) (*Domain, error) {
	b.mu.RLock("PreviewDomainConfig")
	defer b.mu.RUnlock()

	d, exists := b.domains.Get(name)
	if !exists || deleteWindowElapsed(d, b.clock()) {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, name)
	}

	cp := *d
	applyClusterConfig(&cp, input)
	applyStorageConfig(&cp, input)
	applySecurityConfig(&cp, input)
	applyNetworkConfig(&cp, input)
	applyOperationalConfig(&cp, input)

	return &cp, nil
}

// GetDefaultApplicationSetting returns the account's default OpenSearch
// application ARN, or "" if none is set (types.GetDefaultApplicationSettingOutput).
func (b *InMemoryBackend) GetDefaultApplicationSetting() string {
	b.mu.RLock("GetDefaultApplicationSetting")
	defer b.mu.RUnlock()

	return b.defaultApplicationArn
}

// PutDefaultApplicationSetting sets or clears the account's default OpenSearch
// application ARN (types.PutDefaultApplicationSettingInput: ApplicationArn is
// the ARN to set, SetAsDefault true sets it as default, false clears it) and
// returns the resulting default ARN.
func (b *InMemoryBackend) PutDefaultApplicationSetting(
	applicationArn string,
	setAsDefault bool,
) (string, error) {
	if applicationArn == "" {
		return "", fmt.Errorf("%w: ApplicationArn is required", ErrInvalidParameter)
	}

	b.mu.Lock("PutDefaultApplicationSetting")
	defer b.mu.Unlock()

	if setAsDefault {
		b.defaultApplicationArn = applicationArn
	} else {
		b.defaultApplicationArn = ""
	}

	return b.defaultApplicationArn, nil
}
