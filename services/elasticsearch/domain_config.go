package elasticsearch

import (
	"context"
	"fmt"
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

	if cfg.ClusterConfig != nil {
		d.ClusterConfig = *cfg.ClusterConfig
	}

	if cfg.EBSOptions != nil {
		d.EBSOptions = *cfg.EBSOptions
	}

	if cfg.SnapshotOptions != nil {
		d.SnapshotOptions = *cfg.SnapshotOptions
	}

	if cfg.AdvancedOptions != nil {
		d.AdvancedOptions = cfg.AdvancedOptions
	}

	if cfg.AccessPolicies != nil {
		d.AccessPolicies = *cfg.AccessPolicies
	}

	if cfg.EncryptionAtRestEnabled != nil {
		d.EncryptionAtRestEnabled = *cfg.EncryptionAtRestEnabled
	}

	if cfg.NodeToNodeEncryptionEnabled != nil {
		d.NodeToNodeEncryptionEnabled = *cfg.NodeToNodeEncryptionEnabled
	}

	if cfg.EnforceHTTPS != nil {
		d.EnforceHTTPS = *cfg.EnforceHTTPS
	}

	if cfg.TLSSecurityPolicy != nil {
		d.TLSSecurityPolicy = *cfg.TLSSecurityPolicy
	}

	return domainCopy(d), nil
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

// DescribeDomainChangeProgress validates a domain exists and returns (changes are synchronous in-memory).
func (b *InMemoryBackend) DescribeDomainChangeProgress(ctx context.Context, domainName string) error {
	region := getRegion(ctx, b.region)
	b.mu.RLock("DescribeDomainChangeProgress")
	defer b.mu.RUnlock()

	if _, exists := b.domainGet(region, domainName); !exists {
		return fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	return nil
}
