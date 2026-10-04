package elasticsearch

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// UpdateDomainConfig updates the cluster configuration and/or EBS options for a domain.
func (b *InMemoryBackend) UpdateDomainConfig(ctx context.Context, name string, cfg UpdateConfig) (*Domain, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("UpdateDomainConfig")
	defer b.mu.Unlock()

	d, exists := b.domainGet(region, name)
	if !exists {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, name)
	}

	if applyDomainConfigUpdate(d, cfg) {
		d.ConfigUpdatedAt = time.Now()
		d.ConfigVersion++
	}

	return domainCopy(d), nil
}

// applyDomainConfigUpdate mutates d in place from every field cfg sets and
// reports whether at least one field changed. Factored out of
// UpdateDomainConfig to keep its cognitive complexity low. Caller must hold
// b.mu.Lock.
func applyDomainConfigUpdate(d *Domain, cfg UpdateConfig) bool {
	changed := false

	if cfg.ClusterConfig != nil {
		d.ClusterConfig = *cfg.ClusterConfig
		changed = true
	}

	if cfg.EBSOptions != nil {
		d.EBSOptions = *cfg.EBSOptions
		changed = true
	}

	if cfg.SnapshotOptions != nil {
		d.SnapshotOptions = *cfg.SnapshotOptions
		changed = true
	}

	if cfg.AdvancedOptions != nil {
		d.AdvancedOptions = cfg.AdvancedOptions
		changed = true
	}

	if cfg.AccessPolicies != nil {
		d.AccessPolicies = *cfg.AccessPolicies
		changed = true
	}

	if cfg.EncryptionAtRestEnabled != nil {
		d.EncryptionAtRestEnabled = *cfg.EncryptionAtRestEnabled
		changed = true
	}

	if cfg.NodeToNodeEncryptionEnabled != nil {
		d.NodeToNodeEncryptionEnabled = *cfg.NodeToNodeEncryptionEnabled
		changed = true
	}

	if cfg.EnforceHTTPS != nil {
		d.EnforceHTTPS = *cfg.EnforceHTTPS
		changed = true
	}

	if cfg.TLSSecurityPolicy != nil {
		d.TLSSecurityPolicy = *cfg.TLSSecurityPolicy
		changed = true
	}

	return applyDomainConfigUpdateExtended(d, cfg) || changed
}

// applyDomainConfigUpdateExtended applies the VPC/Cognito/AdvancedSecurity/
// AutoTune/LogPublishing fields, split out of applyDomainConfigUpdate to
// keep both functions' cognitive complexity low.
func applyDomainConfigUpdateExtended(d *Domain, cfg UpdateConfig) bool {
	changed := false

	if cfg.VPCOptions != nil {
		d.VPCOptions = cloneVPCOptions(cfg.VPCOptions)
		changed = true
	}

	if cfg.CognitoOptions != nil {
		d.CognitoOptions = cloneCognitoOptions(cfg.CognitoOptions)
		changed = true
	}

	if cfg.AdvancedSecurityOptions != nil {
		d.AdvancedSecurityOptions = cloneAdvancedSecurityOptions(cfg.AdvancedSecurityOptions)
		changed = true
	}

	if cfg.AutoTuneOptions != nil {
		prevRollback := ""
		if d.AutoTuneOptions != nil {
			prevRollback = d.AutoTuneOptions.RollbackOnDisable
		}

		d.AutoTuneOptions = cloneAutoTuneOptions(cfg.AutoTuneOptions)
		if d.AutoTuneOptions.RollbackOnDisable == "" {
			d.AutoTuneOptions.RollbackOnDisable = prevRollback
		}

		changed = true
	}

	if cfg.DeploymentStrategyOptions != nil {
		d.DeploymentStrategyOptions = cloneDeploymentStrategyOptions(cfg.DeploymentStrategyOptions)
		changed = true
	}

	if cfg.LogPublishingOptions != nil {
		d.LogPublishingOptions = cloneLogPublishingOptions(cfg.LogPublishingOptions)
		changed = true
	}

	return changed
}

// CancelDomainConfigChange cancels any in-progress configuration change for a domain.
// Because the in-memory backend applies changes synchronously this is a no-op.
func (b *InMemoryBackend) CancelDomainConfigChange(ctx context.Context, domainName string) (*Domain, error) {
	region := getRegion(ctx, b.region)
	b.mu.RLock("CancelDomainConfigChange")
	defer b.mu.RUnlock()

	d, exists := b.domainGet(region, domainName)
	if !exists {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	return domainCopy(d), nil
}

// DescribeDomainAutoTunes validates a domain exists and returns (the in-memory backend has no auto-tune state).
func (b *InMemoryBackend) DescribeDomainAutoTunes(ctx context.Context, domainName string) error {
	region := getRegion(ctx, b.region)
	b.mu.RLock("DescribeDomainAutoTunes")
	defer b.mu.RUnlock()

	if _, exists := b.domainGet(region, domainName); !exists {
		return fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	return nil
}

// maxChangeProgressHistory bounds how many past configuration changes
// DescribeDomainChangeProgress can resolve by ChangeId.
const maxChangeProgressHistory = 100

// ChangeProgress is one configuration change of a domain.
type ChangeProgress struct {
	StartTime time.Time
	ChangeID  string
}

// changeIDForVersion derives the stable ChangeId of a domain's configuration
// version, so the history needs no stored state.
func changeIDForVersion(region string, d *Domain, version int) string {
	key := fmt.Sprintf("%s|%s|%d|%d", region, d.Name, d.CreatedAt.UnixNano(), version)

	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(key)).String()
}

// DescribeDomainChangeProgress returns the change named by changeID, or the
// latest when empty; changes apply synchronously so all are complete.
func (b *InMemoryBackend) DescribeDomainChangeProgress(
	ctx context.Context, domainName, changeID string,
) (*ChangeProgress, error) {
	region := getRegion(ctx, b.region)
	b.mu.RLock("DescribeDomainChangeProgress")
	defer b.mu.RUnlock()

	d, exists := b.domainGet(region, domainName)
	if !exists {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	latest := ChangeProgress{ChangeID: changeIDForVersion(region, d, d.ConfigVersion), StartTime: d.CreatedAt}
	if !d.ConfigUpdatedAt.IsZero() {
		latest.StartTime = d.ConfigUpdatedAt
	}

	if changeID == "" || changeID == latest.ChangeID {
		return &latest, nil
	}

	for v := d.ConfigVersion - 1; v >= 1 && v > d.ConfigVersion-maxChangeProgressHistory; v-- {
		if changeIDForVersion(region, d, v) == changeID {
			return &ChangeProgress{ChangeID: changeID}, nil
		}
	}

	return nil, fmt.Errorf("%w: change %s not found for domain %s", ErrChangeNotFound, changeID, domainName)
}
