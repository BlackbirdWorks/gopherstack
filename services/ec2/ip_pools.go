package ec2

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"

	"github.com/google/uuid"
)

// ip_pools.go implements three related "address pool" families: Customer-Owned IP
// (COIP) pools used with Outposts/Local Zones local gateways, and the public IPv4/IPv6
// address pools used for BYOIP CIDR provisioning.

// ipv4BitLength is the number of bits in an IPv4 address, used to derive the size of a
// provisioned CIDR range from its netmask length.
const ipv4BitLength = 32

// ipv4PoolBaseOctet and ipv4OctetModulus generate a distinct synthetic TEST-NET-2
// (198.51.100.0/24, RFC 5737) third octet for each CIDR provisioned into a public IPv4 pool.
const (
	ipv4PoolBaseOctet = 100
	ipv4OctetModulus  = 256
)

// ---- Errors ----

// ErrCoipPoolNotFound is returned when a CoIP pool ID does not exist.
var ErrCoipPoolNotFound = errors.New("InvalidPoolID.NotFound")

// ErrCoipCidrNotFound is returned when a CoIP CIDR does not exist within a pool.
var ErrCoipCidrNotFound = errors.New("InvalidParameterValue")

// ErrIpv4PoolNotFound is returned when a public IPv4 pool ID does not exist.
var ErrIpv4PoolNotFound = errors.New("InvalidPublicIpv4Pool.NotFound")

// ErrIpv4PoolCidrNotFound is returned when a CIDR does not exist within a public IPv4 pool.
var ErrIpv4PoolCidrNotFound = errors.New("InvalidParameterValue")

// ErrIpv6PoolNotFound is returned when an IPv6 address pool ID does not exist.
var ErrIpv6PoolNotFound = errors.New("InvalidParameterValue")

// ---- COIP data types ----

// CoipPool represents a Customer-Owned IP address pool associated with a local gateway
// route table.
type CoipPool struct {
	LocalGatewayRouteTableID string   `json:"localGatewayRouteTableId,omitempty"`
	PoolID                   string   `json:"poolId,omitempty"`
	PoolArn                  string   `json:"poolArn,omitempty"`
	PoolCidrs                []string `json:"poolCidrs,omitempty"`
}

// CoipCidr represents a single customer-owned IP address range within a CoIP pool.
type CoipCidr struct {
	Cidr                     string `json:"cidr,omitempty"`
	CoipPoolID               string `json:"coipPoolId,omitempty"`
	LocalGatewayRouteTableID string `json:"localGatewayRouteTableId,omitempty"`
}

// ---- Public IPv4 pool data types ----

// Ipv4PoolRange represents a provisioned address range within a public IPv4 pool.
type Ipv4PoolRange struct {
	FirstAddress          string `json:"firstAddress,omitempty"`
	LastAddress           string `json:"lastAddress,omitempty"`
	AddressCount          int32  `json:"addressCount,omitempty"`
	AvailableAddressCount int32  `json:"availableAddressCount,omitempty"`
}

// Ipv4Pool represents a public IPv4 address pool used for BYOIP CIDR provisioning.
type Ipv4Pool struct {
	PoolID             string          `json:"poolId,omitempty"`
	Description        string          `json:"description,omitempty"`
	NetworkBorderGroup string          `json:"networkBorderGroup,omitempty"`
	PoolAddressRanges  []Ipv4PoolRange `json:"poolAddressRanges,omitempty"`
}

// ---- IPv6 pool data types ----

// Ipv6Pool represents an IPv6 address pool.
type Ipv6Pool struct {
	PoolID         string   `json:"poolId,omitempty"`
	Description    string   `json:"description,omitempty"`
	PoolCidrBlocks []string `json:"poolCidrBlocks,omitempty"`
}

// Ipv6CidrAssociation represents a resource association for a CIDR block within an IPv6
// address pool.
type Ipv6CidrAssociation struct {
	Ipv6Cidr           string `json:"ipv6Cidr,omitempty"`
	AssociatedResource string `json:"associatedResource,omitempty"`
}

// ---- Reset ----

// resetIPPoolMapsLocked re-initialises all maps owned by this file. Must be called with
// b.mu held.
func (b *InMemoryBackend) resetIPPoolMapsLocked() {
}

// ---- COIP pools ----

// CreateCoipPool creates a new Customer-Owned IP address pool associated with the given
// local gateway route table.
func (b *InMemoryBackend) CreateCoipPool(localGatewayRouteTableID string, tags map[string]string) (*CoipPool, error) {
	if localGatewayRouteTableID == "" {
		return nil, fmt.Errorf("%w: LocalGatewayRouteTableId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateCoipPool")
	defer b.mu.Unlock()

	id := "ipv4pool-coip-" + uuid.New().String()[:17]
	pool := &CoipPool{
		PoolID:                   id,
		PoolArn:                  "arn:aws:ec2:" + b.Region + ":" + b.AccountID + ":coip-pool/" + id,
		LocalGatewayRouteTableID: localGatewayRouteTableID,
	}
	b.coipPools.Put(pool)
	b.setTagsLocked(id, tags)

	return copyCoipPool(pool), nil
}

// DeleteCoipPool removes a CoIP pool and all of its address ranges.
func (b *InMemoryBackend) DeleteCoipPool(poolID string) (*CoipPool, error) {
	if poolID == "" {
		return nil, fmt.Errorf("%w: CoipPoolId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteCoipPool")
	defer b.mu.Unlock()

	pool, ok := b.coipPools.Get(poolID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrCoipPoolNotFound, poolID)
	}
	b.coipPools.Delete(poolID)
	delete(b.tags, poolID)

	for _, cidr := range b.coipCidrs.All() {
		key := coipCidrsKeyFn(cidr)
		if cidr.CoipPoolID == poolID {
			b.coipCidrs.Delete(key)
		}
	}

	return copyCoipPool(pool), nil
}

// DescribeCoipPools returns CoIP pools, optionally filtered by pool ID.
func (b *InMemoryBackend) DescribeCoipPools(ids []string) []*CoipPool {
	b.mu.RLock("DescribeCoipPools")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(ids))
	for _, id := range ids {
		filter[id] = true
	}

	out := make([]*CoipPool, 0, b.coipPools.Len())

	for _, pool := range b.coipPools.All() {
		if len(filter) > 0 && !filter[pool.PoolID] {
			continue
		}

		out = append(out, copyCoipPool(pool))
	}

	sort.Slice(out, func(i, j int) bool { return out[i].PoolID < out[j].PoolID })

	return out
}

// coipCidrKey builds the internal map key for a CoIP CIDR entry.
func coipCidrKey(poolID, cidr string) string {
	return poolID + "|" + cidr
}

// CreateCoipCidr adds a customer-owned IP address range to a CoIP pool.
func (b *InMemoryBackend) CreateCoipCidr(poolID, cidr string) (*CoipCidr, error) {
	if poolID == "" || cidr == "" {
		return nil, fmt.Errorf("%w: CoipPoolId and Cidr are required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateCoipCidr")
	defer b.mu.Unlock()

	pool, ok := b.coipPools.Get(poolID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrCoipPoolNotFound, poolID)
	}

	entry := &CoipCidr{Cidr: cidr, CoipPoolID: poolID, LocalGatewayRouteTableID: pool.LocalGatewayRouteTableID}
	b.coipCidrs.Put(entry)
	pool.PoolCidrs = appendUniqueString(pool.PoolCidrs, cidr)

	cp := *entry

	return &cp, nil
}

// DeleteCoipCidr removes a customer-owned IP address range from a CoIP pool.
func (b *InMemoryBackend) DeleteCoipCidr(poolID, cidr string) (*CoipCidr, error) {
	if poolID == "" || cidr == "" {
		return nil, fmt.Errorf("%w: CoipPoolId and Cidr are required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteCoipCidr")
	defer b.mu.Unlock()

	key := coipCidrKey(poolID, cidr)

	entry, ok := b.coipCidrs.Get(key)
	if !ok {
		return nil, fmt.Errorf("%w: %s not found in pool %s", ErrCoipCidrNotFound, cidr, poolID)
	}
	b.coipCidrs.Delete(key)

	if pool, poolOK := b.coipPools.Get(poolID); poolOK {
		pool.PoolCidrs = removeString(pool.PoolCidrs, cidr)
	}

	cp := *entry

	return &cp, nil
}

// GetCoipPoolUsage returns the pool's local gateway route table ID and address usage. This
// mock does not track live address allocations, so CoipAddressUsages is always empty.
func (b *InMemoryBackend) GetCoipPoolUsage(poolID string) (*CoipPool, error) {
	if poolID == "" {
		return nil, fmt.Errorf("%w: PoolId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetCoipPoolUsage")
	defer b.mu.RUnlock()

	pool, ok := b.coipPools.Get(poolID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrCoipPoolNotFound, poolID)
	}

	return copyCoipPool(pool), nil
}

func copyCoipPool(p *CoipPool) *CoipPool {
	cp := *p
	cp.PoolCidrs = append([]string(nil), p.PoolCidrs...)

	return &cp
}

// ---- Public IPv4 pools ----

// CreatePublicIpv4Pool creates a new empty public IPv4 address pool.
func (b *InMemoryBackend) CreatePublicIpv4Pool(networkBorderGroup string, tags map[string]string) *Ipv4Pool {
	b.mu.Lock("CreatePublicIpv4Pool")
	defer b.mu.Unlock()

	if networkBorderGroup == "" {
		networkBorderGroup = b.Region
	}

	id := "ipv4pool-ec2-" + uuid.New().String()[:17]
	pool := &Ipv4Pool{
		PoolID:             id,
		NetworkBorderGroup: networkBorderGroup,
	}
	b.ipv4Pools.Put(pool)
	b.setTagsLocked(id, tags)

	return copyIpv4Pool(pool)
}

// DeletePublicIpv4Pool deletes a public IPv4 address pool.
func (b *InMemoryBackend) DeletePublicIpv4Pool(poolID string) error {
	if poolID == "" {
		return fmt.Errorf("%w: PoolId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeletePublicIpv4Pool")
	defer b.mu.Unlock()

	if _, ok := b.ipv4Pools.Get(poolID); !ok {
		return fmt.Errorf("%w: %s", ErrIpv4PoolNotFound, poolID)
	}
	b.ipv4Pools.Delete(poolID)
	delete(b.tags, poolID)

	return nil
}

// DescribePublicIpv4Pools returns public IPv4 address pools, optionally filtered by ID.
func (b *InMemoryBackend) DescribePublicIpv4Pools(ids []string) []*Ipv4Pool {
	b.mu.RLock("DescribePublicIpv4Pools")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(ids))
	for _, id := range ids {
		filter[id] = true
	}

	out := make([]*Ipv4Pool, 0, b.ipv4Pools.Len())

	for _, pool := range b.ipv4Pools.All() {
		if len(filter) > 0 && !filter[pool.PoolID] {
			continue
		}

		out = append(out, copyIpv4Pool(pool))
	}

	sort.Slice(out, func(i, j int) bool { return out[i].PoolID < out[j].PoolID })

	return out
}

// ProvisionPublicIpv4PoolCidr provisions a CIDR of the given netmask length (carved out of a
// synthetic /16 derived from the pool ID) into a public IPv4 pool.
func (b *InMemoryBackend) ProvisionPublicIpv4PoolCidr(poolID string, netmaskLength int32) (*Ipv4PoolRange, error) {
	if poolID == "" {
		return nil, fmt.Errorf("%w: PoolId is required", ErrInvalidParameter)
	}

	if netmaskLength <= 0 || netmaskLength > ipv4BitLength {
		return nil, fmt.Errorf("%w: NetmaskLength must be between 1 and %d", ErrInvalidParameter, ipv4BitLength)
	}

	b.mu.Lock("ProvisionPublicIpv4PoolCidr")
	defer b.mu.Unlock()

	pool, ok := b.ipv4Pools.Get(poolID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpv4PoolNotFound, poolID)
	}

	addressCount := int32(1) << (ipv4BitLength - uint(netmaskLength))
	idx := len(pool.PoolAddressRanges)
	octet := (ipv4PoolBaseOctet + idx) % ipv4OctetModulus
	base := fmt.Sprintf("198.51.%d.0", octet)

	pr := Ipv4PoolRange{
		FirstAddress:          base,
		LastAddress:           fmt.Sprintf("198.51.%d.255", octet),
		AddressCount:          addressCount,
		AvailableAddressCount: addressCount,
	}
	pool.PoolAddressRanges = append(pool.PoolAddressRanges, pr)

	return &pr, nil
}

// DeprovisionPublicIpv4PoolCidr removes a previously-provisioned CIDR range (identified by its
// first address) from a public IPv4 pool.
func (b *InMemoryBackend) DeprovisionPublicIpv4PoolCidr(poolID, cidr string) error {
	if poolID == "" || cidr == "" {
		return fmt.Errorf("%w: PoolId and Cidr are required", ErrInvalidParameter)
	}

	b.mu.Lock("DeprovisionPublicIpv4PoolCidr")
	defer b.mu.Unlock()

	pool, ok := b.ipv4Pools.Get(poolID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrIpv4PoolNotFound, poolID)
	}

	kept := pool.PoolAddressRanges[:0:0]
	found := false

	for _, r := range pool.PoolAddressRanges {
		if r.FirstAddress == cidr {
			found = true

			continue
		}

		kept = append(kept, r)
	}

	if !found {
		return fmt.Errorf("%w: %s not found in pool %s", ErrIpv4PoolCidrNotFound, cidr, poolID)
	}

	pool.PoolAddressRanges = kept

	return nil
}

func copyIpv4Pool(p *Ipv4Pool) *Ipv4Pool {
	cp := *p
	cp.PoolAddressRanges = append([]Ipv4PoolRange(nil), p.PoolAddressRanges...)

	return &cp
}

// ---- IPv6 pools ----

// CreateIpv6Pool creates a new IPv6 address pool. Not an EC2 API by itself (IPv6 pools are
// created implicitly via ProvisionByoipCidr with an IPv6 CIDR), but exposed so tests and the
// handler layer can seed pools deterministically.
func (b *InMemoryBackend) CreateIpv6Pool(description string, poolCidrBlocks []string) *Ipv6Pool {
	b.mu.Lock("CreateIpv6Pool")
	defer b.mu.Unlock()

	id := "ipv6pool-ec2-" + uuid.New().String()[:17]
	pool := &Ipv6Pool{
		PoolID:         id,
		Description:    description,
		PoolCidrBlocks: append([]string(nil), poolCidrBlocks...),
	}
	b.ipv6Pools.Put(pool)

	return copyIpv6Pool(pool)
}

// DescribeIpv6Pools returns IPv6 address pools, optionally filtered by ID.
func (b *InMemoryBackend) DescribeIpv6Pools(ids []string) []*Ipv6Pool {
	b.mu.RLock("DescribeIpv6Pools")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(ids))
	for _, id := range ids {
		filter[id] = true
	}

	out := make([]*Ipv6Pool, 0, b.ipv6Pools.Len())

	for _, pool := range b.ipv6Pools.All() {
		if len(filter) > 0 && !filter[pool.PoolID] {
			continue
		}

		out = append(out, copyIpv6Pool(pool))
	}

	sort.Slice(out, func(i, j int) bool { return out[i].PoolID < out[j].PoolID })

	return out
}

// GetAssociatedIpv6PoolCidrs returns the CIDR block associations for an IPv6 address pool.
// This mock does not track live VPC/subnet associations, so it always returns the pool's
// declared CIDR blocks with no associated resource.
func (b *InMemoryBackend) GetAssociatedIpv6PoolCidrs(poolID string) ([]Ipv6CidrAssociation, error) {
	if poolID == "" {
		return nil, fmt.Errorf("%w: PoolId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetAssociatedIpv6PoolCidrs")
	defer b.mu.RUnlock()

	pool, ok := b.ipv6Pools.Get(poolID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpv6PoolNotFound, poolID)
	}

	out := make([]Ipv6CidrAssociation, 0, len(pool.PoolCidrBlocks))
	for _, cidr := range pool.PoolCidrBlocks {
		out = append(out, Ipv6CidrAssociation{Ipv6Cidr: cidr})
	}

	return out, nil
}

func copyIpv6Pool(p *Ipv6Pool) *Ipv6Pool {
	cp := *p
	cp.PoolCidrBlocks = append([]string(nil), p.PoolCidrBlocks...)

	return &cp
}

// ---- shared helpers ----

// copyStringMap returns a shallow copy of m, or nil if m is empty.
func copyStringMap(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}

	cp := make(map[string]string, len(m))
	maps.Copy(cp, m)

	return cp
}

// appendUniqueString appends v to s if not already present.
func appendUniqueString(s []string, v string) []string {
	if slices.Contains(s, v) {
		return s
	}

	return append(s, v)
}

// removeString returns s with all occurrences of v removed.
func removeString(s []string, v string) []string {
	out := s[:0:0]

	for _, existing := range s {
		if existing != v {
			out = append(out, existing)
		}
	}

	return out
}
