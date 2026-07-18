package route53resolver

import (
	"context"
	"fmt"
	"sort"

	"github.com/google/uuid"
)

// GetFirewallConfig returns or lazily creates the firewall config for a resource (VPC).
func (b *InMemoryBackend) GetFirewallConfig(ctx context.Context, resourceID string) *FirewallConfig {
	b.mu.Lock("GetFirewallConfig")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	if cfg, ok := b.firewallConfigs.Get(regionalKey(region, resourceID)); ok {
		cp := *cfg

		return &cp
	}
	id := "fwc-" + uuid.New().String()[:8]
	cfg := &FirewallConfig{
		ID:               id,
		OwnerID:          b.accountID,
		ResourceID:       resourceID,
		FirewallFailOpen: firewallFailOpenDisabled,
		Region:           region,
	}
	b.firewallConfigs.Put(cfg)
	cp := *cfg

	return &cp
}

// UpdateFirewallConfig updates the firewall fail-open setting for a resource.
func (b *InMemoryBackend) UpdateFirewallConfig(
	ctx context.Context,
	resourceID, firewallFailOpen string,
) (*FirewallConfig, error) {
	b.mu.Lock("UpdateFirewallConfig")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if firewallFailOpen != firewallFailOpenEnabled && firewallFailOpen != firewallFailOpenDisabled {
		return nil, fmt.Errorf(
			"%w: FirewallFailOpen must be %s or %s",
			ErrValidation,
			firewallFailOpenEnabled,
			firewallFailOpenDisabled,
		)
	}

	cfg, ok := b.firewallConfigs.Get(regionalKey(region, resourceID))
	if !ok {
		id := "fwc-" + uuid.New().String()[:8]
		cfg = &FirewallConfig{
			ID:         id,
			OwnerID:    b.accountID,
			ResourceID: resourceID,
			Region:     region,
		}
		b.firewallConfigs.Put(cfg)
	}
	cfg.FirewallFailOpen = firewallFailOpen
	cp := *cfg

	return &cp, nil
}

// ListFirewallConfigs lists all firewall configs.
func (b *InMemoryBackend) ListFirewallConfigs(ctx context.Context) []*FirewallConfig {
	b.mu.RLock("ListFirewallConfigs")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	regionConfigs := b.firewallConfigsByRegion.Get(region)
	list := make([]*FirewallConfig, 0, len(regionConfigs))
	for _, cfg := range regionConfigs {
		cp := *cfg
		list = append(list, &cp)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].ResourceID < list[j].ResourceID })

	return list
}

// --- Resolver Config operations ---
