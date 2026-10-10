package ec2

import (
	"fmt"
	"math/big"
	"net/netip"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/google/uuid"
)

const (
	hostnameTypeIPName       = "ip-name"
	hostnameTypeResourceName = "resource-name"
	ipSourceAmazon           = "amazon"
	ipSourceBYOIP            = "byoip"
	subnetIPv6PrefixLen      = 64
	maxFreeBlockScan         = 1 << 16
	bitsPerByte              = 8
	maxScanShift             = 62
	resourceTypeSubnet       = "subnet"
)

// CreateSubnetParams are the CreateSubnet request members.
type CreateSubnetParams struct {
	VPCID              string
	CIDRBlock          string
	AvailabilityZone   string
	AvailabilityZoneID string
	OutpostArn         string
	IPv6CIDRBlock      string
	IPv4IpamPoolID     string
	IPv6IpamPoolID     string
	IPv4NetmaskLength  int
	IPv6NetmaskLength  int
	IPv6Native         bool
}

// SubnetAttributeUpdate carries the ModifySubnetAttribute members; nil or empty means not specified.
type SubnetAttributeUpdate struct {
	MapPublicIPOnLaunch             *bool
	EnableDNS64                     *bool
	AssignIPv6AddressOnCreation     *bool
	EnableResourceNameDNSARecord    *bool
	EnableResourceNameDNSAAAARecord *bool
	MapCustomerOwnedIPOnLaunch      *bool
	DisableLNIAtDeviceIndex         *bool
	EnableLNIAtDeviceIndex          *int
	CustomerOwnedIPv4Pool           string
	PrivateDNSHostnameType          string
}

// SubnetIpv6Associations returns the IPv6 CIDR associations of a subnet.
func (b *InMemoryBackend) SubnetIpv6Associations(subnetID string) []*SubnetCIDRAssociation {
	b.mu.RLock("SubnetIpv6Associations")
	defer b.mu.RUnlock()

	src := b.subnetCIDRAssociations[subnetID]
	out := make([]*SubnetCIDRAssociation, 0, len(src))

	for _, a := range src {
		cp := *a
		out = append(out, &cp)
	}

	return out
}

// CreateSubnetWithOptions creates a subnet from the full CreateSubnet request.
func (b *InMemoryBackend) CreateSubnetWithOptions(p CreateSubnetParams) (*Subnet, error) {
	if err := validateSubnetParams(p); err != nil {
		return nil, err
	}

	if err := b.validateOutpostArn(p.OutpostArn); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateSubnet")
	defer b.mu.Unlock()

	vpc, ok := b.vpcs.Get(p.VPCID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrVPCNotFound, p.VPCID)
	}

	az, err := b.resolveSubnetAZLocked(p.AvailabilityZone, p.AvailabilityZoneID)
	if err != nil {
		return nil, err
	}

	id := newSubnetID()

	cidr, v4Alloc, err := b.subnetIPv4CIDRLocked(vpc, p)
	if err != nil {
		return nil, err
	}

	assoc, v6Alloc, err := b.subnetIPv6AssociationLocked(p)
	if err != nil {
		return nil, err
	}

	s := &Subnet{
		ID:               id,
		VPCID:            p.VPCID,
		CIDRBlock:        cidr,
		AvailabilityZone: az,
		OutpostArn:       p.OutpostArn,
		Arn:              arn.Build("ec2", b.Region, b.AccountID, "subnet/"+id),
		Ipv6Native:       p.IPv6Native,
	}
	if p.IPv6Native {
		s.PrivateDNSHostnameType = hostnameTypeResourceName
		s.EnableResourceNameDNSAAAARecord = true
	}

	b.subnets.Put(s)
	b.indexSubnetLocked(id, p.VPCID)
	b.expandRegionalNatGatewaysLocked(p.VPCID)

	if assoc != nil {
		b.subnetCIDRAssociations[id] = append(b.subnetCIDRAssociations[id], assoc)
	}

	b.recordSubnetAllocationLocked(v4Alloc, id)
	b.recordSubnetAllocationLocked(v6Alloc, id)

	return s, nil
}

func validateSubnetParams(p CreateSubnetParams) error {
	if p.VPCID == "" {
		return fmt.Errorf("%w: VpcId is required", ErrInvalidParameter)
	}

	if p.AvailabilityZone != "" && p.AvailabilityZoneID != "" {
		return fmt.Errorf("%w: AvailabilityZone and AvailabilityZoneId cannot both be specified",
			ErrInvalidParameterCombination)
	}

	if p.IPv4IpamPoolID != "" && p.CIDRBlock != "" {
		return fmt.Errorf("%w: CidrBlock and Ipv4IpamPoolId cannot both be specified",
			ErrInvalidParameterCombination)
	}

	if p.IPv6IpamPoolID != "" && p.IPv6CIDRBlock != "" {
		return fmt.Errorf("%w: Ipv6CidrBlock and Ipv6IpamPoolId cannot both be specified",
			ErrInvalidParameterCombination)
	}

	if p.IPv6Native {
		return validateIPv6NativeParams(p)
	}

	if p.CIDRBlock == "" && p.IPv4IpamPoolID == "" {
		return fmt.Errorf("%w: CidrBlock is required", ErrInvalidParameter)
	}

	if p.IPv4IpamPoolID != "" && p.IPv4NetmaskLength == 0 {
		return fmt.Errorf("%w: Ipv4NetmaskLength is required with Ipv4IpamPoolId", ErrMissingParameter)
	}

	return validateIPv6Request(p)
}

func validateIPv6NativeParams(p CreateSubnetParams) error {
	if p.CIDRBlock != "" || p.IPv4IpamPoolID != "" {
		return fmt.Errorf("%w: an IPv6 only subnet cannot have an IPv4 CIDR block",
			ErrInvalidParameterCombination)
	}

	if p.IPv6CIDRBlock == "" && p.IPv6IpamPoolID == "" {
		return fmt.Errorf("%w: Ipv6CidrBlock is required for an IPv6 only subnet", ErrMissingParameter)
	}

	return validateIPv6Request(p)
}

func validateIPv6Request(p CreateSubnetParams) error {
	if p.IPv6IpamPoolID != "" && p.IPv6NetmaskLength == 0 {
		return fmt.Errorf("%w: Ipv6NetmaskLength is required with Ipv6IpamPoolId", ErrMissingParameter)
	}

	if p.IPv6CIDRBlock == "" {
		return nil
	}

	pfx, err := netip.ParsePrefix(p.IPv6CIDRBlock)
	if err != nil || !pfx.Addr().Is6() || pfx.Addr().Is4In6() {
		return fmt.Errorf("%w: invalid IPv6 CIDR block %q", ErrInvalidParameter, p.IPv6CIDRBlock)
	}

	if pfx.Bits() != subnetIPv6PrefixLen {
		return fmt.Errorf("%w: the IPv6 CIDR block of a subnet must be a /%d", ErrInvalidParameter,
			subnetIPv6PrefixLen)
	}

	return nil
}

func (b *InMemoryBackend) resolveSubnetAZLocked(az, azID string) (string, error) {
	if azID != "" {
		for _, name := range b.DescribeAvailabilityZones(b.Region) {
			if availabilityZoneID(name) == azID {
				return name, nil
			}
		}

		return "", fmt.Errorf("%w: invalid availability zone id %q", ErrInvalidParameter, azID)
	}

	if az == "" {
		return b.Region + "a", nil
	}

	if err := b.validateZoneName(az); err != nil {
		return "", err
	}

	return az, nil
}

func (b *InMemoryBackend) subnetIPv4CIDRLocked(vpc *VPC, p CreateSubnetParams) (string, *IpamPoolAllocation, error) {
	if p.IPv6Native {
		return "", nil, nil
	}

	cidr := p.CIDRBlock

	var alloc *IpamPoolAllocation

	if p.IPv4IpamPoolID != "" {
		var err error

		cidr, alloc, err = b.allocateSubnetFromPoolLocked(p.IPv4IpamPoolID, p.IPv4NetmaskLength,
			[]string{vpc.CIDRBlock}, b.subnetCIDRsLocked(vpc.ID, false))
		if err != nil {
			return "", nil, err
		}
	}

	if err := validateSubnetPrefixLen(cidr); err != nil {
		return "", nil, err
	}

	if !cidrContains(vpc.CIDRBlock, cidr) {
		return "", nil, fmt.Errorf("%w: subnet CIDR %s is not within VPC CIDR %s",
			ErrSubnetRange, cidr, vpc.CIDRBlock)
	}

	for _, existing := range b.subnets.All() {
		if existing.VPCID == p.VPCID && existing.CIDRBlock != "" && cidrsOverlap(cidr, existing.CIDRBlock) {
			return "", nil, fmt.Errorf("%w: CIDR %s overlaps with existing subnet %s (%s)",
				ErrSubnetCIDRConflict, cidr, existing.ID, existing.CIDRBlock)
		}
	}

	return cidr, alloc, nil
}

func (b *InMemoryBackend) subnetIPv6AssociationLocked(
	p CreateSubnetParams,
) (*SubnetCIDRAssociation, *IpamPoolAllocation, error) {
	if p.IPv6CIDRBlock == "" && p.IPv6IpamPoolID == "" {
		return nil, nil, nil
	}

	blocks := b.vpcIPv6BlocksLocked(p.VPCID)
	cidr := p.IPv6CIDRBlock

	var alloc *IpamPoolAllocation

	if p.IPv6IpamPoolID != "" {
		parents := make([]string, 0, len(blocks))
		for _, a := range blocks {
			parents = append(parents, a.Ipv6CidrBlock)
		}

		var err error

		cidr, alloc, err = b.allocateSubnetFromPoolLocked(p.IPv6IpamPoolID, p.IPv6NetmaskLength,
			parents, b.subnetCIDRsLocked(p.VPCID, true))
		if err != nil {
			return nil, nil, err
		}
	}

	parent := slices.IndexFunc(blocks, func(a *VpcIpv6CidrBlockAssociation) bool {
		return cidrContains(a.Ipv6CidrBlock, cidr)
	})
	if parent < 0 {
		return nil, nil, fmt.Errorf("%w: subnet IPv6 CIDR %s is not within an IPv6 CIDR block of VPC %s",
			ErrSubnetRange, cidr, p.VPCID)
	}

	for _, existing := range b.subnetCIDRsLocked(p.VPCID, true) {
		if cidrsOverlap(cidr, existing) {
			return nil, nil, fmt.Errorf("%w: IPv6 CIDR %s overlaps with an existing subnet (%s)",
				ErrSubnetCIDRConflict, cidr, existing)
		}
	}

	source := ipSourceAmazon
	if blocks[parent].Ipv6Pool != "" {
		source = ipSourceBYOIP
	}

	return &SubnetCIDRAssociation{
		AssociationID: "subnet-cidr-assoc-" + strings.ReplaceAll(uuid.New().String(), "-", "")[:17],
		IPv6CIDRBlock: cidr,
		State:         stateAssociated,
		IPSource:      source,
	}, alloc, nil
}

func (b *InMemoryBackend) vpcIPv6BlocksLocked(vpcID string) []*VpcIpv6CidrBlockAssociation {
	prefix := vpcID + ":"

	var out []*VpcIpv6CidrBlockAssociation

	for key, a := range b.vpcIpv6CidrAssociations {
		if strings.HasPrefix(key, prefix) && a.State == stateAssociated {
			out = append(out, a)
		}
	}

	slices.SortFunc(out, func(x, y *VpcIpv6CidrBlockAssociation) int {
		return strings.Compare(x.AssociationID, y.AssociationID)
	})

	return out
}

func (b *InMemoryBackend) subnetCIDRsLocked(vpcID string, ipv6 bool) []string {
	var out []string

	for _, s := range b.subnets.All() {
		if s.VPCID != vpcID {
			continue
		}

		if !ipv6 {
			if s.CIDRBlock != "" {
				out = append(out, s.CIDRBlock)
			}

			continue
		}

		for _, a := range b.subnetCIDRAssociations[s.ID] {
			out = append(out, a.IPv6CIDRBlock)
		}
	}

	return out
}

func (b *InMemoryBackend) allocateSubnetFromPoolLocked(
	poolID string, netmask int, parents, taken []string,
) (string, *IpamPoolAllocation, error) {
	pool, ok := b.ipamPools.Get(poolID)
	if !ok {
		return "", nil, fmt.Errorf("%w: %s", ErrIpamPoolNotFound, poolID)
	}

	if (pool.AllocationMinNetmaskLength > 0 && netmask < int(pool.AllocationMinNetmaskLength)) ||
		(pool.AllocationMaxNetmaskLength > 0 && netmask > int(pool.AllocationMaxNetmaskLength)) {
		return "", nil, fmt.Errorf("%w: netmask length %d is outside the allocation range of pool %s",
			ErrInvalidParameter, netmask, poolID)
	}

	cidr, found := firstFreeBlock(parents, netmask, taken)
	if !found {
		return "", nil, fmt.Errorf("%w: no free /%d block is available in the VPC for pool %s",
			ErrInvalidParameter, netmask, poolID)
	}

	alloc := &IpamPoolAllocation{
		IpamPoolAllocationID: "ipam-alloc-" + uuid.New().String()[:8],
		IpamPoolID:           poolID,
		Cidr:                 cidr,
		ResourceType:         resourceTypeSubnet,
		ResourceRegion:       b.Region,
		ResourceOwner:        b.AccountID,
	}

	return cidr, alloc, nil
}

func (b *InMemoryBackend) recordSubnetAllocationLocked(alloc *IpamPoolAllocation, subnetID string) {
	if alloc == nil {
		return
	}

	pool, ok := b.ipamPools.Get(alloc.IpamPoolID)
	if !ok {
		return
	}

	alloc.ResourceID = subnetID
	b.ipamPoolAllocations.Put(alloc)
	b.recordIpamResourceCidrLocked(pool, alloc)
}

func (b *InMemoryBackend) releaseSubnetAllocationsLocked(subnetID string) {
	for _, alloc := range b.ipamPoolAllocations.All() {
		if alloc.ResourceType == resourceTypeSubnet && alloc.ResourceID == subnetID {
			b.forgetIpamResourceCidrLocked(alloc)
			b.ipamPoolAllocations.Delete(alloc.IpamPoolAllocationID)
		}
	}
}

// firstFreeBlock returns the first aligned /bits block inside any parent CIDR that overlaps none of taken.
func firstFreeBlock(parents []string, bits int, taken []string) (string, bool) {
	for _, parent := range parents {
		pfx, err := netip.ParsePrefix(parent)
		if err != nil || bits < pfx.Bits() || bits > pfx.Addr().BitLen() {
			continue
		}

		pfx = pfx.Masked()
		base := new(big.Int).SetBytes(pfx.Addr().AsSlice())
		step := new(big.Int).Lsh(big.NewInt(1), uint(pfx.Addr().BitLen()-bits))

		for i := range maxFreeBlockScan {
			if i >= 1<<min(bits-pfx.Bits(), maxScanShift) {
				break
			}

			cand := new(big.Int).Add(base, new(big.Int).Mul(step, big.NewInt(int64(i))))
			raw := cand.FillBytes(make([]byte, pfx.Addr().BitLen()/bitsPerByte))
			addr, _ := netip.AddrFromSlice(raw)
			c := netip.PrefixFrom(addr, bits).String()

			if !slices.ContainsFunc(taken, func(t string) bool { return cidrsOverlap(c, t) }) {
				return c, true
			}
		}
	}

	return "", false
}

// ModifySubnetAttributes applies one ModifySubnetAttribute request; AWS allows a single attribute per call.
func (b *InMemoryBackend) ModifySubnetAttributes(subnetID string, u SubnetAttributeUpdate) error {
	if subnetID == "" {
		return fmt.Errorf("%w: SubnetId is required", ErrInvalidParameter)
	}

	switch u.attributeCount() {
	case 0:
		return fmt.Errorf("%w: a subnet attribute to modify is required", ErrMissingParameter)
	case 1:
	default:
		return fmt.Errorf("%w: only one subnet attribute can be modified at a time", ErrInvalidParameterCombination)
	}

	b.mu.Lock("ModifySubnetAttribute")
	defer b.mu.Unlock()

	subnet, ok := b.subnets.Get(subnetID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrSubnetNotFound, subnetID)
	}

	return u.apply(subnet)
}

func (u SubnetAttributeUpdate) attributeCount() int {
	n := 0

	for _, set := range []bool{
		u.MapPublicIPOnLaunch != nil, u.EnableDNS64 != nil, u.AssignIPv6AddressOnCreation != nil,
		u.EnableResourceNameDNSARecord != nil, u.EnableResourceNameDNSAAAARecord != nil,
		u.MapCustomerOwnedIPOnLaunch != nil || u.CustomerOwnedIPv4Pool != "",
		u.DisableLNIAtDeviceIndex != nil || u.EnableLNIAtDeviceIndex != nil,
		u.PrivateDNSHostnameType != "",
	} {
		if set {
			n++
		}
	}

	return n
}

func (u SubnetAttributeUpdate) apply(s *Subnet) error {
	switch {
	case u.MapPublicIPOnLaunch != nil:
		s.MapPublicIPOnLaunch = *u.MapPublicIPOnLaunch
	case u.EnableDNS64 != nil:
		s.EnableDNS64 = *u.EnableDNS64
	case u.AssignIPv6AddressOnCreation != nil:
		s.AssignIPv6AddressOnCreation = *u.AssignIPv6AddressOnCreation
	case u.EnableResourceNameDNSARecord != nil:
		s.EnableResourceNameDNSARecord = *u.EnableResourceNameDNSARecord
	case u.EnableResourceNameDNSAAAARecord != nil:
		s.EnableResourceNameDNSAAAARecord = *u.EnableResourceNameDNSAAAARecord
	case u.PrivateDNSHostnameType != "":
		return u.applyHostnameType(s)
	case u.MapCustomerOwnedIPOnLaunch != nil || u.CustomerOwnedIPv4Pool != "":
		return u.applyCustomerOwnedPool(s)
	default:
		return u.applyLNI(s)
	}

	return nil
}

func (u SubnetAttributeUpdate) applyHostnameType(s *Subnet) error {
	switch u.PrivateDNSHostnameType {
	case hostnameTypeIPName:
		if s.Ipv6Native {
			return fmt.Errorf("%w: an IPv6 only subnet requires hostname type %s", ErrInvalidParameter,
				hostnameTypeResourceName)
		}
	case hostnameTypeResourceName:
	default:
		return fmt.Errorf("%w: invalid PrivateDnsHostnameTypeOnLaunch %q", ErrInvalidParameter,
			u.PrivateDNSHostnameType)
	}

	s.PrivateDNSHostnameType = u.PrivateDNSHostnameType

	return nil
}

func (u SubnetAttributeUpdate) applyCustomerOwnedPool(s *Subnet) error {
	enable := u.MapCustomerOwnedIPOnLaunch != nil && *u.MapCustomerOwnedIPOnLaunch
	if enable && u.CustomerOwnedIPv4Pool == "" {
		return fmt.Errorf("%w: CustomerOwnedIpv4Pool is required with MapCustomerOwnedIpOnLaunch",
			ErrMissingParameter)
	}

	if u.MapCustomerOwnedIPOnLaunch != nil {
		s.MapCustomerOwnedIPOnLaunch = enable
	}

	if u.CustomerOwnedIPv4Pool != "" {
		s.CustomerOwnedIPv4Pool = u.CustomerOwnedIPv4Pool
	}

	return nil
}

func (u SubnetAttributeUpdate) applyLNI(s *Subnet) error {
	if u.EnableLNIAtDeviceIndex != nil {
		if *u.EnableLNIAtDeviceIndex < 1 {
			return fmt.Errorf("%w: EnableLniAtDeviceIndex must be at least 1; the primary interface cannot be a LNI",
				ErrInvalidParameter)
		}

		s.EnableLNIAtDeviceIndex = *u.EnableLNIAtDeviceIndex

		return nil
	}

	if *u.DisableLNIAtDeviceIndex {
		s.EnableLNIAtDeviceIndex = 0
	}

	return nil
}
