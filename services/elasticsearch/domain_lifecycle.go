package elasticsearch

import (
	"context"
	"fmt"
	"slices"
	"time"
)

// CancelElasticsearchServiceSoftwareUpdate cancels a scheduled software update.
// Because the in-memory backend never schedules updates this is a no-op.
func (b *InMemoryBackend) CancelElasticsearchServiceSoftwareUpdate(
	ctx context.Context, domainName string,
) (*Domain, error) {
	region := getRegion(ctx, b.region)
	b.mu.RLock("CancelElasticsearchServiceSoftwareUpdate")
	defer b.mu.RUnlock()

	d, exists := b.domainGet(region, domainName)
	if !exists {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	return domainCopy(d), nil
}

// DeleteElasticsearchServiceRole deletes the Elasticsearch service-linked
// IAM role. Real AWS: "Role deletion will fail if any existing VPC domains
// use the role. You must delete any such Elasticsearch domains before
// deleting the role." The in-memory backend has no IAM state, so this only
// enforces that precondition against tracked domains.
func (b *InMemoryBackend) DeleteElasticsearchServiceRole() error {
	b.mu.RLock("DeleteElasticsearchServiceRole")
	defer b.mu.RUnlock()

	for _, d := range b.domains.All() {
		if d.VPCOptions != nil {
			return fmt.Errorf(
				"%w: domain %s uses a VPC and still uses the service-linked role",
				ErrServiceRoleInUse, d.Name,
			)
		}
	}

	return nil
}

// GetUpgradeHistory returns the domain's recorded upgrades, newest first.
func (b *InMemoryBackend) GetUpgradeHistory(ctx context.Context, domainName string) ([]UpgradeRecord, error) {
	region := getRegion(ctx, b.region)
	b.mu.RLock("GetUpgradeHistory")
	defer b.mu.RUnlock()

	d, exists := b.domainGet(region, domainName)
	if !exists {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	out := make([]UpgradeRecord, 0, len(d.Upgrades))
	for _, rec := range slices.Backward(d.Upgrades) {
		rec.Steps = slices.Clone(rec.Steps)
		out = append(out, rec)
	}

	return out, nil
}

// GetUpgradeStatus returns the most recent upgrade record; ok is false when none exists.
func (b *InMemoryBackend) GetUpgradeStatus(ctx context.Context, domainName string) (UpgradeRecord, bool, error) {
	region := getRegion(ctx, b.region)
	b.mu.RLock("GetUpgradeStatus")
	defer b.mu.RUnlock()

	d, exists := b.domainGet(region, domainName)
	if !exists {
		return UpgradeRecord{}, false, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	if len(d.Upgrades) == 0 {
		return UpgradeRecord{}, false, nil
	}

	rec := d.Upgrades[len(d.Upgrades)-1]
	rec.Steps = slices.Clone(rec.Steps)

	return rec, true, nil
}

// StartElasticsearchServiceSoftwareUpdate schedules a software update (no-op in-memory).
func (b *InMemoryBackend) StartElasticsearchServiceSoftwareUpdate(
	ctx context.Context, domainName string,
) (*Domain, error) {
	region := getRegion(ctx, b.region)
	b.mu.RLock("StartElasticsearchServiceSoftwareUpdate")
	defer b.mu.RUnlock()

	d, exists := b.domainGet(region, domainName)
	if !exists {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	return domainCopy(d), nil
}

// UpgradeElasticsearchDomain upgrades a domain and records the upgrade in its history.
func (b *InMemoryBackend) UpgradeElasticsearchDomain(
	ctx context.Context, domainName, targetVersion string,
) (*Domain, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("UpgradeElasticsearchDomain")
	defer b.mu.Unlock()

	d, exists := b.domainGet(region, domainName)
	if !exists {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	b.recordUpgrade(d, targetVersion, false)

	if targetVersion != "" {
		d.ElasticsearchVersion = targetVersion
	}

	return domainCopy(d), nil
}

// CheckElasticsearchDomainUpgrade records an upgrade eligibility check without changing the domain.
func (b *InMemoryBackend) CheckElasticsearchDomainUpgrade(
	ctx context.Context, domainName, targetVersion string,
) error {
	region := getRegion(ctx, b.region)
	b.mu.Lock("CheckElasticsearchDomainUpgrade")
	defer b.mu.Unlock()

	d, exists := b.domainGet(region, domainName)
	if !exists {
		return fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	b.recordUpgrade(d, targetVersion, true)

	return nil
}

// recordUpgrade appends a bounded upgrade-history entry; the caller holds the write lock.
func (b *InMemoryBackend) recordUpgrade(d *Domain, targetVersion string, checkOnly bool) {
	name := "Upgrade from " + d.ElasticsearchVersion + " to " + targetVersion
	steps := []string{upgradeStepPreCheck, upgradeStepSnapshot, upgradeStepUpgrade}

	if checkOnly {
		name = "Upgrade eligibility check from " + d.ElasticsearchVersion + " to " + targetVersion
		steps = steps[:1]
	}

	d.Upgrades = append(d.Upgrades, UpgradeRecord{
		Name:           name,
		StartTimestamp: time.Now(),
		Steps:          steps,
	})

	if len(d.Upgrades) > maxUpgradeHistoryPerDomain {
		d.Upgrades = slices.Clone(d.Upgrades[len(d.Upgrades)-maxUpgradeHistoryPerDomain:])
	}
}
