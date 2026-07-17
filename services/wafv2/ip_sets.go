package wafv2

import (
	"context"
	"fmt"
	"maps"
	"net/netip"
	"sort"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func (b *InMemoryBackend) buildIPSetARN(name, id, scope, region string) string {
	prefix := scopePrefix(scope)

	return arn.Build("wafv2", arnRegionForScope(scope, region), b.accountID, prefix+"/ipset/"+name+"/"+id)
}

// IPSetARN builds a public ARN for an IPSet.
func (b *InMemoryBackend) IPSetARN(name, id, scope string) string {
	prefix := scopePrefix(scope)

	return arn.Build("wafv2", b.arnRegion(scope), b.accountID, prefix+"/ipset/"+name+"/"+id)
}

// validateCIDRs validates a list of CIDRs against the given IP version.
func validateCIDRs(addresses []string, ipVersion string) error {
	if len(addresses) > maxIPSetEntries {
		return fmt.Errorf(
			"%w: IP set exceeds maximum of %d addresses",
			ErrLimitsExceeded,
			maxIPSetEntries,
		)
	}

	for _, addr := range addresses {
		prefix, err := netip.ParsePrefix(addr)
		if err != nil {
			return fmt.Errorf("%w: invalid CIDR %q: %s", errInvalidRequest, addr, err.Error())
		}

		if ipVersion == IPVersionIPv4 && !prefix.Addr().Is4() {
			return fmt.Errorf(
				"%w: CIDR %q is not a valid IPv4 address for IPV4 set",
				errInvalidRequest,
				addr,
			)
		}

		if ipVersion == IPVersionIPv6 && !prefix.Addr().Is6() {
			return fmt.Errorf(
				"%w: CIDR %q is not a valid IPv6 address for IPV6 set",
				errInvalidRequest,
				addr,
			)
		}
	}

	return nil
}

// lookupIPSetByID finds an IPSet with the same CLOUDFRONT fallback logic.
func (b *InMemoryBackend) lookupIPSetByID(requestRegion, id string) (*IPSet, bool) {
	if s, ok := b.ipSets.Get(regionKey(requestRegion, id)); ok {
		return s, true
	}

	if requestRegion != "" {
		if s, ok := b.ipSets.Get(regionKey("", id)); ok {
			return s, true
		}
	}

	return nil, false
}

// CreateIPSet creates a new IPSet.
func (b *InMemoryBackend) CreateIPSet(
	ctx context.Context,
	name, scope, description, ipAddressVersion string,
	addresses []string,
	tags map[string]string,
) (*IPSet, error) {
	b.mu.Lock("CreateIPSet")
	defer b.mu.Unlock()

	region := storeRegion(scope, getRegion(ctx, b.region))

	if len(b.ipSetsByNameScope.Get(regionKey(region, nameScope(name, scope)))) > 0 {
		return nil, fmt.Errorf("%w: IP set %q already exists in scope %s", ErrIPSetAlreadyExists, name, scope)
	}

	id := uuid.NewString()
	arnStr := b.buildIPSetARN(name, id, scope, region)
	s := &IPSet{
		ARN:              arnStr,
		ID:               id,
		Name:             name,
		Scope:            scope,
		Description:      description,
		IPAddressVersion: ipAddressVersion,
		Addresses:        cloneAddresses(addresses),
		LockToken:        uuid.NewString(),
		Tags:             cloneTags(tags),
	}
	b.ipSets.Put(s)

	return cloneIPSet(s), nil
}

// GetIPSet returns an IPSet by ID.
func (b *InMemoryBackend) GetIPSet(ctx context.Context, id string) (*IPSet, error) {
	b.mu.RLock("GetIPSet")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	s, ok := b.lookupIPSetByID(region, id)
	if !ok {
		return nil, fmt.Errorf("%w: IP set %q not found", ErrIPSetNotFound, id)
	}

	return cloneIPSet(s), nil
}

// UpdateIPSet updates an IPSet by ID.
func (b *InMemoryBackend) UpdateIPSet(
	ctx context.Context,
	id, description, lockToken string,
	addresses []string,
) (*IPSet, error) {
	b.mu.Lock("UpdateIPSet")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	s, ok := b.lookupIPSetByID(region, id)
	if !ok {
		return nil, fmt.Errorf("%w: IP set %q not found", ErrIPSetNotFound, id)
	}

	if lockToken != "" && lockToken != s.LockToken {
		return nil, fmt.Errorf("%w: lock token mismatch for IP set %q", ErrOptimisticLock, id)
	}

	if description != "" {
		s.Description = description
	}

	if addresses != nil {
		s.Addresses = cloneAddresses(addresses)
	}

	s.LockToken = uuid.NewString()

	return cloneIPSet(s), nil
}

// DeleteIPSet deletes an IPSet by ID.
func (b *InMemoryBackend) DeleteIPSet(ctx context.Context, id, lockToken string) error {
	b.mu.Lock("DeleteIPSet")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	s, ok := b.lookupIPSetByID(region, id)
	if !ok {
		return fmt.Errorf("%w: IP set %q not found", ErrIPSetNotFound, id)
	}

	storeReg := regionFromARN(s.ARN)

	if lockToken != "" && lockToken != s.LockToken {
		return fmt.Errorf("%w: lock token mismatch for IP set %q", ErrOptimisticLock, id)
	}

	b.ipSets.Delete(regionKey(storeReg, id))

	return nil
}

// ListIPSets returns all IPSets sorted by name.
func (b *InMemoryBackend) ListIPSets(ctx context.Context) []*IPSet {
	b.mu.RLock("ListIPSets")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	list := make([]*IPSet, 0)

	for _, s := range b.ipSetsByRegion.Get(region) {
		list = append(list, cloneIPSet(s))
	}

	if region != "" {
		for _, s := range b.ipSetsByRegion.Get("") {
			list = append(list, cloneIPSet(s))
		}
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].Name < list[j].Name
	})

	return list
}
func cloneIPSet(s *IPSet) *IPSet {
	cp := *s
	cp.Tags = maps.Clone(s.Tags)
	cp.Addresses = cloneAddresses(s.Addresses)

	return &cp
}
