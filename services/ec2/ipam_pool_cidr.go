package ec2

import (
	"fmt"
	"net"
	"sort"

	"github.com/google/uuid"
)

// ---- IPAM Pool CIDR provisioning ----

// ProvisionIpamPoolCidr adds a CIDR range to an IPAM pool's provisioned space.
func (b *InMemoryBackend) ProvisionIpamPoolCidr(poolID, cidr string) (*IpamPoolCidr, error) {
	if poolID == "" {
		return nil, fmt.Errorf("%w: IpamPoolId is required", ErrInvalidParameter)
	}

	if cidr == "" {
		return nil, fmt.Errorf("%w: Cidr is required", ErrInvalidParameter)
	}

	b.mu.Lock("ProvisionIpamPoolCidr")
	defer b.mu.Unlock()

	if _, ok := b.ipamPools.Get(poolID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamPoolNotFound, poolID)
	}

	entry := &IpamPoolCidr{Cidr: cidr, State: ipamPoolCidrStateProvisioned}
	b.ipamPoolCidrs[poolID] = append(b.ipamPoolCidrs[poolID], entry)

	cp := *entry

	return &cp, nil
}

// DeprovisionIpamPoolCidr removes a previously provisioned CIDR range from an IPAM pool.
func (b *InMemoryBackend) DeprovisionIpamPoolCidr(poolID, cidr string) (*IpamPoolCidr, error) {
	if poolID == "" {
		return nil, fmt.Errorf("%w: IpamPoolId is required", ErrInvalidParameter)
	}

	if cidr == "" {
		return nil, fmt.Errorf("%w: Cidr is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeprovisionIpamPoolCidr")
	defer b.mu.Unlock()

	if _, ok := b.ipamPools.Get(poolID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamPoolNotFound, poolID)
	}

	cidrs := b.ipamPoolCidrs[poolID]

	for i, c := range cidrs {
		if c.Cidr == cidr {
			removed := *c
			b.ipamPoolCidrs[poolID] = append(cidrs[:i], cidrs[i+1:]...)

			return &removed, nil
		}
	}

	return nil, fmt.Errorf("%w: %s is not provisioned to pool %s", ErrIpamPoolCidrNotFound, cidr, poolID)
}

// GetIpamPoolCidrs returns the CIDR ranges provisioned to an IPAM pool.
func (b *InMemoryBackend) GetIpamPoolCidrs(poolID string) []*IpamPoolCidr {
	b.mu.RLock("GetIpamPoolCidrs")
	defer b.mu.RUnlock()

	cidrs := b.ipamPoolCidrs[poolID]
	out := make([]*IpamPoolCidr, 0, len(cidrs))

	for _, c := range cidrs {
		cp := *c
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].Cidr < out[j].Cidr
	})

	return out
}

// autoCIDRLocked generates a unique CIDR from the given poolCidr and netmask length.
// Must be called with b.mu held.
func (b *InMemoryBackend) autoCIDRLocked(poolCidr string, netmaskLength int) (string, error) {
	const defaultPoolCIDR = "10.0.0.0/8"
	const defaultNetmask = 24

	if poolCidr == "" {
		poolCidr = defaultPoolCIDR
	}

	if netmaskLength <= 0 {
		netmaskLength = defaultNetmask
	}

	_, network, err := net.ParseCIDR(poolCidr)
	if err != nil {
		return "", fmt.Errorf("%w: invalid pool CIDR %s", ErrInvalidParameter, poolCidr)
	}

	existingCount := b.ipamPoolAllocations.Len()

	ip := network.IP.To4()
	if ip == nil {
		ip = make(net.IP, ipv4Len)
	}

	ipInt := uint32(
		ip[0],
	)<<octet3Shift | uint32(
		ip[1],
	)<<octet2Shift | uint32(
		ip[2],
	)<<octetMask | uint32(
		ip[3],
	)
	shift := max(ipv4Shift-netmaskLength, 0)

	//nolint:gosec // existingCount is small; integer overflow is acceptable in mock context
	ipInt += uint32(existingCount) << uint(shift)
	//nolint:gosec // byte truncation is intentional: each octet is extracted from a 32-bit IP integer
	shiftedIP := net.IP{
		byte(ipInt >> octet3Shift),
		byte(ipInt >> octet2Shift),
		byte(ipInt >> octetMask),
		byte(ipInt),
	}

	return fmt.Sprintf("%s/%d", shiftedIP.String(), netmaskLength), nil
}

// ---- IPAM Pool Allocations ----

// ipamAllocOpts returns the first IpamAllocationOptions in opts, or a zero value if none given.
func ipamAllocOpts(opts []IpamAllocationOptions) IpamAllocationOptions {
	if len(opts) > 0 {
		return opts[0]
	}

	return IpamAllocationOptions{}
}

// AllocateIpamPoolCidr allocates a CIDR from an IPAM pool.
// If cidr is empty, one is auto-generated from the pool's network space.
func (b *InMemoryBackend) AllocateIpamPoolCidr(
	poolID, cidr string,
	netmaskLength int,
	opts ...IpamAllocationOptions,
) (*IpamPoolAllocation, error) {
	if poolID == "" {
		return nil, fmt.Errorf("%w: IpamPoolId is required", ErrInvalidParameter)
	}

	o := ipamAllocOpts(opts)

	b.mu.Lock("AllocateIpamPoolCidr")
	defer b.mu.Unlock()

	pool, ok := b.ipamPools.Get(poolID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamPoolNotFound, poolID)
	}

	allocCidr := cidr
	if allocCidr == "" {
		var err error

		allocCidr, err = b.autoCIDRLocked(pool.Cidr, netmaskLength)
		if err != nil {
			return nil, err
		}
	}

	alloc := &IpamPoolAllocation{
		IpamPoolAllocationID: "ipam-alloc-" + uuid.New().String()[:8],
		IpamPoolID:           poolID,
		Cidr:                 allocCidr,
		Description:          o.Description,
		ResourceType:         o.ResourceType,
		ResourceID:           o.ResourceID,
		ResourceOwner:        o.ResourceOwner,
		ResourceRegion:       b.Region,
	}
	if alloc.ResourceType == "" {
		alloc.ResourceType = "custom"
	}
	b.ipamPoolAllocations.Put(alloc)
	b.recordIpamResourceCidrLocked(pool, alloc)

	cp := *alloc

	return &cp, nil
}

// GetIpamPoolAllocations returns allocations for an IPAM pool, optionally filtered to a
// single allocation ID.
func (b *InMemoryBackend) GetIpamPoolAllocations(poolID, allocationID string) ([]*IpamPoolAllocation, error) {
	if poolID == "" {
		return nil, fmt.Errorf("%w: IpamPoolId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetIpamPoolAllocations")
	defer b.mu.RUnlock()

	if _, ok := b.ipamPools.Get(poolID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamPoolNotFound, poolID)
	}

	out := make([]*IpamPoolAllocation, 0, b.ipamPoolAllocations.Len())

	for _, alloc := range b.ipamPoolAllocations.All() {
		if alloc.IpamPoolID != poolID {
			continue
		}

		if allocationID != "" && alloc.IpamPoolAllocationID != allocationID {
			continue
		}

		cp := *alloc
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].IpamPoolAllocationID < out[j].IpamPoolAllocationID
	})

	return out, nil
}

// ReleaseIpamPoolAllocation releases an IPAM pool allocation.
func (b *InMemoryBackend) ReleaseIpamPoolAllocation(poolID, allocationID string) error {
	if allocationID == "" {
		return fmt.Errorf("%w: IpamPoolAllocationId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ReleaseIpamPoolAllocation")
	defer b.mu.Unlock()

	alloc, ok := b.ipamPoolAllocations.Get(allocationID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrIpamAllocationNotFound, allocationID)
	}

	if poolID != "" && alloc.IpamPoolID != poolID {
		return fmt.Errorf("%w: %s", ErrIpamAllocationNotFound, allocationID)
	}

	b.forgetIpamResourceCidrLocked(alloc)
	b.ipamPoolAllocations.Delete(allocationID)

	return nil
}

// DescribeIpamPoolAllocations returns IPAM pool allocations across ALL pools,
// optionally filtered to the given allocation IDs. Unlike GetIpamPoolAllocations
// (which requires a single IpamPoolId), this is a cross-pool describe — the
// real AWS DescribeIpamPoolAllocationsInput has no IpamPoolId member, only
// IpamPoolAllocationIds (field-diffed against aws-sdk-go-v2's
// DescribeIpamPoolAllocationsInput).
func (b *InMemoryBackend) DescribeIpamPoolAllocations(allocationIDs []string) []*IpamPoolAllocation {
	b.mu.RLock("DescribeIpamPoolAllocations")
	defer b.mu.RUnlock()

	idSet := toIDSet(allocationIDs)

	out := make([]*IpamPoolAllocation, 0, b.ipamPoolAllocations.Len())

	for _, alloc := range b.ipamPoolAllocations.All() {
		if len(idSet) > 0 && !idSet[alloc.IpamPoolAllocationID] {
			continue
		}

		cp := *alloc
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].IpamPoolAllocationID < out[j].IpamPoolAllocationID
	})

	return out
}

// ModifyIpamPoolAllocation updates the description of an IPAM pool allocation.
// Matches the real ModifyIpamPoolAllocationInput shape, which (like
// DescribeIpamPoolAllocations) has no IpamPoolId member — only
// IpamPoolAllocationId and Description.
func (b *InMemoryBackend) ModifyIpamPoolAllocation(allocationID, description string) (*IpamPoolAllocation, error) {
	if allocationID == "" {
		return nil, fmt.Errorf("%w: IpamPoolAllocationId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyIpamPoolAllocation")
	defer b.mu.Unlock()

	alloc, ok := b.ipamPoolAllocations.Get(allocationID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamAllocationNotFound, allocationID)
	}

	alloc.Description = description
	cp := *alloc

	return &cp, nil
}

// ---- IPAM Resource Discoveries ----

// DescribeIpamResourceDiscoveries returns IPAM resource discoveries, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeIpamResourceDiscoveries(ids []string) []*IpamResourceDiscovery {
	b.mu.RLock("DescribeIpamResourceDiscoveries")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*IpamResourceDiscovery, 0, b.ipamResourceDiscoveries.Len())

	for _, d := range b.ipamResourceDiscoveries.All() {
		if len(idSet) > 0 && !idSet[d.IpamResourceDiscoveryID] {
			continue
		}

		cp := *d
		cp.OperatingRegions = append([]string(nil), d.OperatingRegions...)
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].IpamResourceDiscoveryID < out[j].IpamResourceDiscoveryID
	})

	return out
}

// DescribeIpamResourceDiscoveryAssociations returns IPAM resource discovery associations,
// optionally filtered by IDs.
func (b *InMemoryBackend) DescribeIpamResourceDiscoveryAssociations(
	ids []string,
) []*IpamResourceDiscoveryAssociation {
	b.mu.RLock("DescribeIpamResourceDiscoveryAssociations")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*IpamResourceDiscoveryAssociation, 0, b.ipamResourceDiscoveryAssocs.Len())

	for _, a := range b.ipamResourceDiscoveryAssocs.All() {
		if len(idSet) > 0 && !idSet[a.IpamResourceDiscoveryAssociationID] {
			continue
		}

		cp := *a
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].IpamResourceDiscoveryAssociationID < out[j].IpamResourceDiscoveryAssociationID
	})

	return out
}
