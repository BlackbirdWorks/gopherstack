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

	if validation != dnssecValidationEnable && validation != dnssecValidationDisable {
		return nil, fmt.Errorf(
			"%w: Validation must be %s or %s",
			ErrValidation,
			dnssecValidationEnable,
			dnssecValidationDisable,
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
	if validation == dnssecValidationEnable {
		cfg.ValidationStatus = validationStatusEnabling
	} else {
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
