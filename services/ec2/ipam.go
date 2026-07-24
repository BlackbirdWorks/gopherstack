package ec2

import (
	"fmt"
	"sort"

	"github.com/google/uuid"
)

// ---- IPAM ----

// ipamOpts returns the first IpamOptions in opts, or a zero value if none was given.
func ipamOpts(opts []IpamOptions) IpamOptions {
	if len(opts) > 0 {
		return opts[0]
	}

	return IpamOptions{}
}

// CreateIpam creates a new IPAM instance, along with its default public/private scopes and
// default resource discovery, mirroring real AWS behavior.
func (b *InMemoryBackend) CreateIpam(opts ...IpamOptions) (*Ipam, error) {
	o := ipamOpts(opts)

	b.mu.Lock("CreateIpam")
	defer b.mu.Unlock()

	ipamID := "ipam-" + uuid.New().String()[:8]

	privScope := &IpamScope{
		IpamScopeID:   "ipam-scope-" + uuid.New().String()[:8],
		IpamScopeARN:  "arn:aws:ec2:" + b.Region + ":" + b.AccountID + ":ipam-scope/",
		IpamID:        ipamID,
		IpamScopeType: ipamScopeTypePrivate,
		IsDefault:     true,
		State:         ipamStateCreateComplete,
	}
	privScope.IpamScopeARN += privScope.IpamScopeID
	b.ipamScopes.Put(privScope)

	pubScope := &IpamScope{
		IpamScopeID:   "ipam-scope-" + uuid.New().String()[:8],
		IpamScopeARN:  "arn:aws:ec2:" + b.Region + ":" + b.AccountID + ":ipam-scope/",
		IpamID:        ipamID,
		IpamScopeType: ipamScopeTypePublic,
		IsDefault:     true,
		State:         ipamStateCreateComplete,
	}
	pubScope.IpamScopeARN += pubScope.IpamScopeID
	b.ipamScopes.Put(pubScope)

	discovery := &IpamResourceDiscovery{
		IpamResourceDiscoveryID: "ipam-res-disco-" + uuid.New().String()[:8],
		OwnerID:                 b.AccountID,
		Region:                  b.Region,
		IsDefault:               true,
		State:                   ipamStateCreateComplete,
	}
	discovery.IpamResourceDiscoveryARN = "arn:aws:ec2:" + b.Region + ":" + b.AccountID +
		":ipam-resource-discovery/" + discovery.IpamResourceDiscoveryID
	b.ipamResourceDiscoveries.Put(discovery)

	assoc := &IpamResourceDiscoveryAssociation{
		IpamResourceDiscoveryAssociationID: "ipam-res-disco-assoc-" + uuid.New().String()[:8],
		IpamID:                             ipamID,
		IpamARN:                            "arn:aws:ec2:" + b.Region + ":" + b.AccountID + ":ipam/" + ipamID,
		IpamRegion:                         b.Region,
		IpamResourceDiscoveryID:            discovery.IpamResourceDiscoveryID,
		OwnerID:                            b.AccountID,
		IsDefault:                          true,
		ResourceDiscoveryStatus:            ipamResourceDiscoveryAssocStatus,
		State:                              ipamStateCreateComplete,
	}
	assoc.IpamResourceDiscoveryAssociationARN = "arn:aws:ec2:" + b.Region + ":" + b.AccountID +
		":ipam-resource-discovery-association/" + assoc.IpamResourceDiscoveryAssociationID
	b.ipamResourceDiscoveryAssocs.Put(assoc)

	ipam := &Ipam{
		IpamID:                                ipamID,
		IpamARN:                               "arn:aws:ec2:" + b.Region + ":" + b.AccountID + ":ipam/" + ipamID,
		State:                                 ipamStateCreateComplete,
		Region:                                b.Region,
		OwnerID:                               b.AccountID,
		Description:                           o.Description,
		OperatingRegions:                      append([]string(nil), o.OperatingRegions...),
		Tier:                                  o.Tier,
		PublicDefaultScopeID:                  pubScope.IpamScopeID,
		PrivateDefaultScopeID:                 privScope.IpamScopeID,
		ScopeCount:                            2, //nolint:mnd // AWS always creates exactly the 2 default scopes
		DefaultResourceDiscoveryID:            discovery.IpamResourceDiscoveryID,
		DefaultResourceDiscoveryAssociationID: assoc.IpamResourceDiscoveryAssociationID,
		ResourceDiscoveryAssociationCount:     1,
	}
	if ipam.Tier == "" {
		ipam.Tier = "advanced"
	}
	b.ipams.Put(ipam)

	cp := *ipam
	cp.OperatingRegions = append([]string(nil), ipam.OperatingRegions...)

	return &cp, nil
}

// DescribeIpams returns IPAM instances, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeIpams(ids []string) []*Ipam {
	b.mu.RLock("DescribeIpams")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*Ipam, 0, b.ipams.Len())

	for _, ipam := range b.ipams.All() {
		if len(idSet) > 0 && !idSet[ipam.IpamID] {
			continue
		}

		cp := *ipam
		cp.OperatingRegions = append([]string(nil), ipam.OperatingRegions...)
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].IpamID < out[j].IpamID
	})

	return out
}

// ModifyIpam updates an IPAM's description, operating regions, or tier.
func (b *InMemoryBackend) ModifyIpam(id string, opts IpamOptions) (*Ipam, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: IpamId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyIpam")
	defer b.mu.Unlock()

	ipam, ok := b.ipams.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamNotFound, id)
	}

	if opts.Description != "" {
		ipam.Description = opts.Description
	}

	if len(opts.OperatingRegions) > 0 {
		ipam.OperatingRegions = append([]string(nil), opts.OperatingRegions...)
	}

	if opts.Tier != "" {
		ipam.Tier = opts.Tier
	}

	ipam.State = ipamStateModifyComplete

	cp := *ipam
	cp.OperatingRegions = append([]string(nil), ipam.OperatingRegions...)

	return &cp, nil
}

// DeleteIpam removes an IPAM instance and its default scopes/resource discovery.
func (b *InMemoryBackend) DeleteIpam(id string) error {
	if id == "" {
		return fmt.Errorf("%w: IpamId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteIpam")
	defer b.mu.Unlock()

	ipam, ok := b.ipams.Get(id)
	if !ok {
		return fmt.Errorf("%w: %s", ErrIpamNotFound, id)
	}
	b.ipamScopes.Delete(ipam.PublicDefaultScopeID)
	delete(b.tags, ipam.PublicDefaultScopeID)
	b.ipamScopes.Delete(ipam.PrivateDefaultScopeID)
	delete(b.tags, ipam.PrivateDefaultScopeID)
	b.ipamResourceDiscoveries.Delete(ipam.DefaultResourceDiscoveryID)
	delete(b.tags, ipam.DefaultResourceDiscoveryID)
	b.ipamResourceDiscoveryAssocs.Delete(ipam.DefaultResourceDiscoveryAssociationID)
	delete(b.tags, ipam.DefaultResourceDiscoveryAssociationID)
	b.ipams.Delete(id)
	delete(b.tags, id)

	return nil
}

// ---- IPAM Scopes ----

// CreateIpamScope creates an additional (non-default) private IPAM scope.
func (b *InMemoryBackend) CreateIpamScope(ipamID, description string) (*IpamScope, error) {
	if ipamID == "" {
		return nil, fmt.Errorf("%w: IpamId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateIpamScope")
	defer b.mu.Unlock()

	ipam, ok := b.ipams.Get(ipamID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamNotFound, ipamID)
	}

	scopeID := "ipam-scope-" + uuid.New().String()[:8]
	scope := &IpamScope{
		IpamScopeID:   scopeID,
		IpamScopeARN:  "arn:aws:ec2:" + b.Region + ":" + b.AccountID + ":ipam-scope/" + scopeID,
		IpamID:        ipamID,
		IpamScopeType: ipamScopeTypePrivate,
		State:         ipamStateCreateComplete,
		Description:   description,
	}
	b.ipamScopes.Put(scope)
	ipam.ScopeCount++

	cp := *scope

	return &cp, nil
}

// DescribeIpamScopes returns IPAM scopes, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeIpamScopes(ids []string) []*IpamScope {
	b.mu.RLock("DescribeIpamScopes")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*IpamScope, 0, b.ipamScopes.Len())

	for _, scope := range b.ipamScopes.All() {
		if len(idSet) > 0 && !idSet[scope.IpamScopeID] {
			continue
		}

		cp := *scope
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].IpamScopeID < out[j].IpamScopeID
	})

	return out
}

// ModifyIpamScope updates an IPAM scope's description.
func (b *InMemoryBackend) ModifyIpamScope(id, description string) (*IpamScope, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: IpamScopeId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyIpamScope")
	defer b.mu.Unlock()

	scope, ok := b.ipamScopes.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamScopeNotFound, id)
	}

	scope.Description = description
	scope.State = ipamStateModifyComplete

	cp := *scope

	return &cp, nil
}

// DeleteIpamScope removes a non-default IPAM scope. Default scopes cannot be deleted.
func (b *InMemoryBackend) DeleteIpamScope(id string) error {
	if id == "" {
		return fmt.Errorf("%w: IpamScopeId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteIpamScope")
	defer b.mu.Unlock()

	scope, ok := b.ipamScopes.Get(id)
	if !ok {
		return fmt.Errorf("%w: %s", ErrIpamScopeNotFound, id)
	}

	if scope.IsDefault {
		return fmt.Errorf("%w: default IPAM scopes cannot be deleted", ErrIpamScopeDefault)
	}

	if ipam, ipamOK := b.ipams.Get(scope.IpamID); ipamOK {
		ipam.ScopeCount--
	}
	b.ipamScopes.Delete(id)
	delete(b.tags, id)

	return nil
}

// ---- IPAM Pools ----

// ipamPoolOpts returns the first IpamPoolOptions in opts, or a zero value if none was given.
func ipamPoolOpts(opts []IpamPoolOptions) IpamPoolOptions {
	if len(opts) > 0 {
		return opts[0]
	}

	return IpamPoolOptions{}
}

// CreateIpamPool creates a new IPAM pool under the given IPAM (resolved to its default scope
// for addressFamily). If cidr is non-empty it is immediately provisioned to the pool, matching
// the CreateIpamPool ProvisionedCidrs request parameter.
func (b *InMemoryBackend) CreateIpamPool(
	ipamID, addressFamily, locale, cidr string, opts ...IpamPoolOptions,
) (*IpamPool, error) {
	if ipamID == "" {
		return nil, fmt.Errorf("%w: IpamId is required", ErrInvalidParameter)
	}

	if addressFamily == "" {
		addressFamily = "ipv4"
	}

	o := ipamPoolOpts(opts)

	b.mu.Lock("CreateIpamPool")
	defer b.mu.Unlock()

	ipam, ok := b.ipams.Get(ipamID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamNotFound, ipamID)
	}

	scopeID := o.IpamScopeID
	if scopeID == "" {
		if addressFamily == "ipv6" {
			scopeID = ipam.PublicDefaultScopeID
		} else {
			scopeID = ipam.PrivateDefaultScopeID
		}
	}

	poolID := "ipam-pool-" + uuid.New().String()[:8]
	pool := &IpamPool{
		IpamPoolID:                     poolID,
		IpamPoolARN:                    "arn:aws:ec2:" + b.Region + ":" + b.AccountID + ":ipam-pool/" + poolID,
		IpamID:                         ipamID,
		IpamScopeID:                    scopeID,
		State:                          ipamStateCreateComplete,
		Locale:                         locale,
		AddressFamily:                  addressFamily,
		Cidr:                           cidr,
		Description:                    o.Description,
		AutoImport:                     o.AutoImport,
		PubliclyAdvertisable:           o.PubliclyAdvertisable,
		AllocationMinNetmaskLength:     o.AllocationMinNetmaskLength,
		AllocationMaxNetmaskLength:     o.AllocationMaxNetmaskLength,
		AllocationDefaultNetmaskLength: o.AllocationDefaultNetmaskLength,
	}
	b.ipamPools.Put(pool)

	if cidr != "" {
		b.ipamPoolCidrs[poolID] = []*IpamPoolCidr{{Cidr: cidr, State: ipamPoolCidrStateProvisioned}}
	}

	cp := *pool

	return &cp, nil
}

// DescribeIpamPools returns IPAM pools, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeIpamPools(ids []string) []*IpamPool {
	b.mu.RLock("DescribeIpamPools")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*IpamPool, 0, b.ipamPools.Len())

	for _, pool := range b.ipamPools.All() {
		if len(idSet) > 0 && !idSet[pool.IpamPoolID] {
			continue
		}

		cp := *pool
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].IpamPoolID < out[j].IpamPoolID
	})

	return out
}

// ModifyIpamPool updates mutable attributes of an IPAM pool.
func (b *InMemoryBackend) ModifyIpamPool(id string, opts IpamPoolOptions) (*IpamPool, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: IpamPoolId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyIpamPool")
	defer b.mu.Unlock()

	pool, ok := b.ipamPools.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamPoolNotFound, id)
	}

	if opts.Description != "" {
		pool.Description = opts.Description
	}

	pool.AutoImport = opts.AutoImport

	if opts.AllocationMinNetmaskLength > 0 {
		pool.AllocationMinNetmaskLength = opts.AllocationMinNetmaskLength
	}

	if opts.AllocationMaxNetmaskLength > 0 {
		pool.AllocationMaxNetmaskLength = opts.AllocationMaxNetmaskLength
	}

	if opts.AllocationDefaultNetmaskLength > 0 {
		pool.AllocationDefaultNetmaskLength = opts.AllocationDefaultNetmaskLength
	}

	pool.State = ipamStateModifyComplete

	cp := *pool

	return &cp, nil
}

// DeleteIpamPool removes an IPAM pool and its provisioned CIDRs.
func (b *InMemoryBackend) DeleteIpamPool(id string) error {
	if id == "" {
		return fmt.Errorf("%w: IpamPoolId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteIpamPool")
	defer b.mu.Unlock()

	if _, ok := b.ipamPools.Get(id); !ok {
		return fmt.Errorf("%w: %s", ErrIpamPoolNotFound, id)
	}
	b.ipamPools.Delete(id)
	delete(b.ipamPoolCidrs, id)
	delete(b.tags, id)

	return nil
}
