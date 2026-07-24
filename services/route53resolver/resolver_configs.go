package route53resolver

import (
	"context"
	"fmt"
	"sort"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// GetResolverConfig returns or lazily creates the resolver config for a resource (VPC).
func (b *InMemoryBackend) GetResolverConfig(ctx context.Context, resourceID string) *ResolverConfig {
	b.mu.Lock("GetResolverConfig")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	if cfg, ok := b.resolverConfigs.Get(regionalKey(region, resourceID)); ok {
		cp := *cfg

		return &cp
	}
	id := "rslvr-rc-" + uuid.New().String()[:8]
	cfgARN := arn.Build("route53resolver", region, b.accountID, "resolver-config/"+id)
	cfg := &ResolverConfig{
		ID:                 id,
		ARN:                cfgARN,
		OwnerID:            b.accountID,
		ResourceID:         resourceID,
		AutodefinedReverse: "DISABLED",
		Region:             region,
	}
	b.resolverConfigs.Put(cfg)
	cp := *cfg

	return &cp
}

// UpdateResolverConfig updates the AutodefinedReverse setting for a resource.
func (b *InMemoryBackend) UpdateResolverConfig(
	ctx context.Context,
	resourceID, autodefinedReverse string,
) (*ResolverConfig, error) {
	b.mu.Lock("UpdateResolverConfig")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	switch autodefinedReverse {
	case autodefinedReverseEnabled, autodefinedReverseDisabled, autodefinedReverseUseLocal:
		// valid
	default:
		return nil, fmt.Errorf(
			"%w: AutodefinedReverse must be %s, %s, or %s",
			ErrValidation,
			autodefinedReverseEnabled,
			autodefinedReverseDisabled,
			autodefinedReverseUseLocal,
		)
	}

	cfg, ok := b.resolverConfigs.Get(regionalKey(region, resourceID))
	if !ok {
		id := "rslvr-rc-" + uuid.New().String()[:8]
		cfgARN := arn.Build("route53resolver", region, b.accountID, "resolver-config/"+id)
		cfg = &ResolverConfig{
			ID:         id,
			ARN:        cfgARN,
			OwnerID:    b.accountID,
			ResourceID: resourceID,
			Region:     region,
		}
		b.resolverConfigs.Put(cfg)
	}
	switch autodefinedReverse {
	case autodefinedReverseEnabled:
		cfg.AutodefinedReverse = "ENABLED"
	case autodefinedReverseUseLocal:
		cfg.AutodefinedReverse = autodefinedReverseUseLocal
	default:
		cfg.AutodefinedReverse = "DISABLED"
	}
	cp := *cfg

	return &cp, nil
}

// ListResolverConfigs lists all resolver configs.
func (b *InMemoryBackend) ListResolverConfigs(ctx context.Context) []*ResolverConfig {
	b.mu.RLock("ListResolverConfigs")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	regionConfigs := b.resolverConfigsByRegion.Get(region)
	list := make([]*ResolverConfig, 0, len(regionConfigs))
	for _, cfg := range regionConfigs {
		cp := *cfg
		list = append(list, &cp)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].ResourceID < list[j].ResourceID })

	return list
}

// --- Resolver DNSSEC Config operations ---
