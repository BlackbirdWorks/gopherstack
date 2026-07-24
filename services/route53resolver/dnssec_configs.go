package route53resolver

import (
	"context"
	"fmt"
	"sort"

	"github.com/google/uuid"
)

// GetResolverDnssecConfig returns or lazily creates the DNSSEC config for a resource.
func (b *InMemoryBackend) GetResolverDnssecConfig(ctx context.Context, resourceID string) *ResolverDnssecConfig {
	b.mu.Lock("GetResolverDnssecConfig")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	if cfg, ok := b.resolverDnssecConfigs.Get(regionalKey(region, resourceID)); ok {
		cp := *cfg

		return &cp
	}
	id := "rslvr-dnssec-" + uuid.New().String()[:8]
	cfg := &ResolverDnssecConfig{
		ID:               id,
		OwnerID:          b.accountID,
		ResourceID:       resourceID,
		ValidationStatus: validationStatusDisabled,
		Region:           region,
	}
	b.resolverDnssecConfigs.Put(cfg)
	cp := *cfg

	return &cp
}

// UpdateResolverDnssecConfig updates DNSSEC validation for a resource.
func (b *InMemoryBackend) UpdateResolverDnssecConfig(
	ctx context.Context,
	resourceID, validation string,
) (*ResolverDnssecConfig, error) {
	b.mu.Lock("UpdateResolverDnssecConfig")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	switch validation {
	case dnssecValidationEnable, dnssecValidationDisable, dnssecValidationUseLocal:
		// valid
	default:
		return nil, fmt.Errorf(
			"%w: Validation must be %s, %s, or %s",
			ErrValidation,
			dnssecValidationEnable,
			dnssecValidationDisable,
			dnssecValidationUseLocal,
		)
	}

	cfg, ok := b.resolverDnssecConfigs.Get(regionalKey(region, resourceID))
	if !ok {
		id := "rslvr-dnssec-" + uuid.New().String()[:8]
		cfg = &ResolverDnssecConfig{
			ID:         id,
			OwnerID:    b.accountID,
			ResourceID: resourceID,
			Region:     region,
		}
		b.resolverDnssecConfigs.Put(cfg)
	}
	switch validation {
	case dnssecValidationEnable:
		cfg.ValidationStatus = validationStatusEnabling
	case dnssecValidationUseLocal:
		cfg.ValidationStatus = validationStatusUpdatingToUseLocal
	default:
		cfg.ValidationStatus = validationStatusDisabling
	}
	cp := *cfg

	return &cp, nil
}

// ListResolverDnssecConfigs lists all DNSSEC configs.
func (b *InMemoryBackend) ListResolverDnssecConfigs(ctx context.Context) []*ResolverDnssecConfig {
	b.mu.RLock("ListResolverDnssecConfigs")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	regionConfigs := b.resolverDnssecConfigsByRegion.Get(region)
	list := make([]*ResolverDnssecConfig, 0, len(regionConfigs))
	for _, cfg := range regionConfigs {
		cp := *cfg
		list = append(list, &cp)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].ResourceID < list[j].ResourceID })

	return list
}

// --- Outpost Resolver operations ---
