package route53resolver

import (
	"context"
	"fmt"
	"sort"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// CreateFirewallDomainList creates a new DNS Firewall domain list.
func (b *InMemoryBackend) CreateFirewallDomainList(
	ctx context.Context,
	name, creatorRequestID string,
) (*FirewallDomainList, error) {
	b.mu.Lock("CreateFirewallDomainList")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	now := currentTime()
	id := "rslvr-fdl-" + uuid.New().String()[:8]
	listARN := arn.Build("route53resolver", region, b.accountID, "firewall-domain-list/"+id)
	dl := &FirewallDomainList{
		ID:               id,
		ARN:              listARN,
		Name:             name,
		CreatorRequestID: creatorRequestID,
		Status:           statusComplete,
		CreationTime:     now,
		ModificationTime: now,
		Region:           region,
	}
	b.firewallDomainLists.Put(dl)
	cp := *dl

	return &cp, nil
}

// DeleteFirewallDomainList deletes a DNS Firewall domain list.
func (b *InMemoryBackend) DeleteFirewallDomainList(ctx context.Context, id string) (*FirewallDomainList, error) {
	b.mu.Lock("DeleteFirewallDomainList")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	dl, ok := b.firewallDomainLists.Get(regionalKey(region, id))
	if !ok {
		return nil, fmt.Errorf("%w: firewall domain list %s not found", ErrNotFound, id)
	}
	cp := cloneFirewallDomainList(dl)
	delete(b.tagsStore(region), dl.ARN)
	b.firewallDomainLists.Delete(regionalKey(region, id))

	return cp, nil
}

// AddFirewallDomainListInternal adds a firewall domain list directly to the backend (test seed helper).
func (b *InMemoryBackend) AddFirewallDomainListInternal(name string) *FirewallDomainList {
	b.mu.Lock("AddFirewallDomainListInternal")
	defer b.mu.Unlock()

	id := "rslvr-fdl-" + uuid.New().String()[:8]
	listARN := arn.Build("route53resolver", b.region, b.accountID, "firewall-domain-list/"+id)
	dl := &FirewallDomainList{
		ID:     id,
		ARN:    listARN,
		Name:   name,
		Status: statusComplete,
		Region: b.region,
	}
	b.firewallDomainLists.Put(dl)
	cp := *dl

	return &cp
}

// GetFirewallDomainList retrieves a domain list by ID.
func (b *InMemoryBackend) GetFirewallDomainList(ctx context.Context, id string) (*FirewallDomainList, error) {
	b.mu.RLock("GetFirewallDomainList")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	dl, ok := b.firewallDomainLists.Get(regionalKey(region, id))
	if !ok {
		return nil, fmt.Errorf("%w: firewall domain list %s not found", ErrNotFound, id)
	}
	cp := cloneFirewallDomainList(dl)

	return cp, nil
}

// ListFirewallDomainLists lists all firewall domain lists.
func (b *InMemoryBackend) ListFirewallDomainLists(ctx context.Context) []*FirewallDomainList {
	b.mu.RLock("ListFirewallDomainLists")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	regionLists := b.firewallDomainListsByRegion.Get(region)
	list := make([]*FirewallDomainList, 0, len(regionLists))
	for _, dl := range regionLists {
		list = append(list, cloneFirewallDomainList(dl))
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	return list
}

// ListFirewallDomains returns the domains stored in a domain list.
func (b *InMemoryBackend) ListFirewallDomains(ctx context.Context, id string) ([]string, error) {
	b.mu.RLock("ListFirewallDomains")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	dl, ok := b.firewallDomainLists.Get(regionalKey(region, id))
	if !ok {
		return nil, fmt.Errorf("%w: firewall domain list %s not found", ErrNotFound, id)
	}
	cp := make([]string, len(dl.Domains))
	copy(cp, dl.Domains)

	return cp, nil
}

// UpdateFirewallDomains replaces, adds, or removes domains in a domain list.
func (b *InMemoryBackend) UpdateFirewallDomains(
	ctx context.Context,
	id, operation string,
	domains []string,
) (*FirewallDomainList, error) {
	b.mu.Lock("UpdateFirewallDomains")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	dl, ok := b.firewallDomainLists.Get(regionalKey(region, id))
	if !ok {
		return nil, fmt.Errorf("%w: firewall domain list %s not found", ErrNotFound, id)
	}

	switch operation {
	case domainUpdateOpReplace:
		dl.Domains = make([]string, len(domains))
		copy(dl.Domains, domains)
	case domainUpdateOpAdd:
		existing := make(map[string]bool, len(dl.Domains))
		for _, d := range dl.Domains {
			existing[d] = true
		}
		for _, d := range domains {
			if !existing[d] {
				dl.Domains = append(dl.Domains, d)
			}
		}
	case domainUpdateOpRemove:
		toRemove := make(map[string]bool, len(domains))
		for _, d := range domains {
			toRemove[d] = true
		}
		remaining := make([]string, 0, len(dl.Domains))
		for _, d := range dl.Domains {
			if !toRemove[d] {
				remaining = append(remaining, d)
			}
		}
		dl.Domains = remaining
	default:
		return nil, fmt.Errorf(
			"%w: Operation must be %s, %s, or %s",
			ErrValidation,
			domainUpdateOpReplace,
			domainUpdateOpAdd,
			domainUpdateOpRemove,
		)
	}
	dl.DomainCount = domainCount(dl.Domains)
	dl.ModificationTime = currentTime()
	cp := cloneFirewallDomainList(dl)

	return cp, nil
}

// ImportFirewallDomains simulates importing domains from a URL into a domain list.
func (b *InMemoryBackend) ImportFirewallDomains(
	ctx context.Context,
	id, operation, domainFileURL string,
) (*FirewallDomainList, error) {
	b.mu.Lock("ImportFirewallDomains")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	dl, ok := b.firewallDomainLists.Get(regionalKey(region, id))
	if !ok {
		return nil, fmt.Errorf("%w: firewall domain list %s not found", ErrNotFound, id)
	}

	// Simulate: clear domains on REPLACE, leave intact for ADD/REMOVE (no HTTP in mock).
	if operation == domainUpdateOpReplace {
		dl.Domains = []string{}
		dl.DomainCount = 0
	}
	dl.Status = statusComplete
	dl.ModificationTime = currentTime()
	_ = domainFileURL
	cp := cloneFirewallDomainList(dl)

	return cp, nil
}

// cloneFirewallDomainList returns a deep copy of a FirewallDomainList.
func cloneFirewallDomainList(dl *FirewallDomainList) *FirewallDomainList {
	cp := *dl
	if dl.Domains != nil {
		cp.Domains = make([]string, len(dl.Domains))
		copy(cp.Domains, dl.Domains)
	}

	return &cp
}

// domainCount returns the number of domains as int32, capping at MaxInt32.
func domainCount(domains []string) int32 {
	const maxInt32 = 1<<31 - 1
	if len(domains) > maxInt32 {
		return maxInt32
	}

	return int32(len(domains)) //nolint:gosec // guarded above
}

// --- Firewall Config operations ---
