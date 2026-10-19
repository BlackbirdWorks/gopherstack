package s3files

import (
	"context"
	"net/netip"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	maxSecurityGroups = 5
	reservedLowHosts  = 4
)

// CreateMountTargetInput is the backend form of CreateMountTarget.
type CreateMountTargetInput struct {
	FileSystemID   string
	SubnetID       string
	IPAddressType  string
	IPv4Address    string
	IPv6Address    string
	SecurityGroups []string
}

func validateSecurityGroups(sgs []string) error {
	if len(sgs) > maxSecurityGroups {
		return validation("A mount target supports at most 5 security groups")
	}

	for _, sg := range sgs {
		if !strings.HasPrefix(sg, "sg-") {
			return validation("Invalid security group ID " + sg)
		}
	}

	return nil
}

func validateMountTargetInput(in *CreateMountTargetInput) error {
	if in.FileSystemID == "" || in.SubnetID == "" {
		return validation("FileSystemId and SubnetId are required")
	}

	if in.IPAddressType == "" {
		in.IPAddressType = ipv4Only
	}

	if err := validateIPFamilies(*in); err != nil {
		return err
	}

	return validateSecurityGroups(in.SecurityGroups)
}

func validateIPFamilies(in CreateMountTargetInput) error {
	switch in.IPAddressType {
	case ipv4Only:
		if in.IPv6Address != "" {
			return validation("Ipv6Address requires IpAddressType IPV6_ONLY or DUAL_STACK")
		}
	case ipv6Only:
		if in.IPv4Address != "" {
			return validation("Ipv4Address requires IpAddressType IPV4_ONLY or DUAL_STACK")
		}
	case dualStack:
	default:
		return validation("IpAddressType must be IPV4_ONLY, IPV6_ONLY or DUAL_STACK")
	}

	if a, err := netip.ParseAddr(in.IPv4Address); in.IPv4Address != "" && (err != nil || !a.Is4()) {
		return validation("Ipv4Address is not a valid IPv4 address")
	}

	if a, err := netip.ParseAddr(in.IPv6Address); in.IPv6Address != "" && (err != nil || !a.Is6()) {
		return validation("Ipv6Address is not a valid IPv6 address")
	}

	return nil
}

// ipv4Prefix returns the subnet's IPv4 CIDR when it is known and parseable.
func ipv4Prefix(info SubnetInfo, known bool) (netip.Prefix, bool) {
	if !known {
		return netip.Prefix{}, false
	}

	prefix, err := netip.ParsePrefix(info.CIDR)

	return prefix, err == nil && prefix.Addr().Is4()
}

// usedIPv4 lists the IPv4 addresses this backend's mount targets hold in a subnet. Callers hold the lock.
func (b *InMemoryBackend) usedIPv4(subnetID string) map[string]bool {
	used := map[string]bool{}

	for _, mt := range b.mountTargets.All() {
		if mt.SubnetID == subnetID {
			used[mt.IPv4Address] = true
		}
	}

	return used
}

// pickIPv4 validates or allocates the mount target's IPv4 address inside the subnet. Callers hold the lock.
func (b *InMemoryBackend) pickIPv4(in CreateMountTargetInput, info SubnetInfo, known bool) (string, error) {
	if in.IPAddressType == ipv6Only {
		return "", nil
	}

	used := b.usedIPv4(in.SubnetID)
	prefix, hasPrefix := ipv4Prefix(info, known)

	if in.IPv4Address != "" {
		if used[in.IPv4Address] {
			return "", conflict("MountTarget", "", "IP address "+in.IPv4Address+" is already in use")
		}

		if hasPrefix && !prefix.Contains(netip.MustParseAddr(in.IPv4Address)) {
			return "", validation("Ipv4Address is outside the subnet CIDR " + info.CIDR)
		}

		return in.IPv4Address, nil
	}

	if !hasPrefix {
		return "", nil
	}

	addr := prefix.Masked().Addr()
	for range reservedLowHosts {
		addr = addr.Next()
	}

	for ; prefix.Contains(addr) && prefix.Contains(addr.Next()); addr = addr.Next() {
		if !used[addr.String()] {
			return addr.String(), nil
		}
	}

	return "", conflict("MountTarget", "", "Subnet "+in.SubnetID+" has no free IP addresses")
}

// CreateMountTarget creates a mount target for an available file system.
func (b *InMemoryBackend) CreateMountTarget(ctx context.Context, in CreateMountTargetInput) (*MountTarget, error) {
	if err := validateMountTargetInput(&in); err != nil {
		return nil, err
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateMountTarget")
	defer b.mu.Unlock()

	b.sweepLocked()

	fs, err := b.liveFileSystem(region, in.FileSystemID)
	if err != nil {
		return nil, err
	}

	if state, _ := b.state(&fs.Lifecycle); state != statusAvailable {
		return nil, conflict("FileSystem", fs.ID, "File system "+fs.ID+" is "+state+", not available")
	}

	var info SubnetInfo

	known := false

	if b.subnets != nil {
		if info, known = b.subnets.Subnet(in.SubnetID); !known {
			return nil, notFound("Subnet " + in.SubnetID + " not found")
		}
	}

	for _, mt := range b.mountTargets.All() {
		if mt.FileSystemID == fs.ID && mt.SubnetID == in.SubnetID {
			return nil, conflict(
				"MountTarget",
				mt.ID,
				"File system "+fs.ID+" already has a mount target in "+in.SubnetID,
			)
		}
	}

	ipv4, err := b.pickIPv4(in, info, known)
	if err != nil {
		return nil, err
	}

	mt := &MountTarget{
		ID:                 awsid.New("fsmt-"),
		FileSystemID:       fs.ID,
		SubnetID:           in.SubnetID,
		VPCID:              info.VPCID,
		AvailabilityZoneID: info.AvailabilityZoneID,
		IPv4Address:        ipv4,
		IPv6Address:        in.IPv6Address,
		IPAddressType:      in.IPAddressType,
		NetworkInterfaceID: awsid.New("eni-"),
		OwnerID:            b.accountID,
		Region:             region,
		SecurityGroups:     slices.Clone(in.SecurityGroups),
		Lifecycle:          b.newLifecycle(),
	}
	b.mountTargets.Put(mt)

	return b.mtView(mt), nil
}

// GetMountTarget returns a mount target by ID.
func (b *InMemoryBackend) GetMountTarget(ctx context.Context, id string) (*MountTarget, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("GetMountTarget")
	defer b.mu.RUnlock()

	mt, err := b.liveMountTarget(region, id)
	if err != nil {
		return nil, err
	}

	return b.mtView(mt), nil
}

// ListMountTargets lists mount targets, optionally filtered by file system or access point.
func (b *InMemoryBackend) ListMountTargets(
	ctx context.Context,
	fileSystemRef, accessPointRef string,
	maxResults int,
	token string,
) (page.Page[*MountTarget], error) {
	if err := validatePaging(maxResults, token); err != nil {
		return page.Page[*MountTarget]{}, err
	}

	region := getRegion(ctx, b.region)

	b.mu.RLock("ListMountTargets")
	defer b.mu.RUnlock()

	fsID := ""
	if fileSystemRef != "" {
		fs, err := b.liveFileSystem(region, fileSystemRef)
		if err != nil {
			return page.Page[*MountTarget]{}, err
		}

		fsID = fs.ID
	}

	if accessPointRef != "" {
		ap, err := b.liveAccessPoint(region, accessPointRef)
		if err != nil {
			return page.Page[*MountTarget]{}, err
		}

		if fsID != "" && fsID != ap.FileSystemID {
			return page.Page[*MountTarget]{Data: []*MountTarget{}}, nil
		}

		fsID = ap.FileSystemID
	}

	var all []*MountTarget

	for _, mt := range b.mountTargets.All() {
		if _, gone := b.state(&mt.Lifecycle); gone || mt.Region != region || (fsID != "" && mt.FileSystemID != fsID) {
			continue
		}

		all = append(all, b.mtView(mt))
	}

	slices.SortFunc(all, func(x, y *MountTarget) int { return strings.Compare(x.ID, y.ID) })

	return page.New(all, token, maxResults, defaultPageSize), nil
}

// UpdateMountTarget replaces the mount target's security groups.
func (b *InMemoryBackend) UpdateMountTarget(ctx context.Context, id string, sgs []string) (*MountTarget, error) {
	if len(sgs) == 0 {
		return nil, validation("SecurityGroups must contain at least one security group")
	}

	if err := validateSecurityGroups(sgs); err != nil {
		return nil, err
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("UpdateMountTarget")
	defer b.mu.Unlock()

	b.sweepLocked()

	mt, err := b.liveMountTarget(region, id)
	if err != nil {
		return nil, err
	}

	if state, _ := b.state(&mt.Lifecycle); state != statusAvailable {
		return nil, conflict("MountTarget", mt.ID, "Mount target "+mt.ID+" is "+state+", not available")
	}

	mt.SecurityGroups = slices.Clone(sgs)

	return b.mtView(mt), nil
}

// DeleteMountTarget deletes a mount target.
func (b *InMemoryBackend) DeleteMountTarget(ctx context.Context, id string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteMountTarget")
	defer b.mu.Unlock()

	b.sweepLocked()

	mt, err := b.liveMountTarget(region, id)
	if err != nil {
		return err
	}

	if b.beginDelete(&mt.Lifecycle) {
		b.mountTargets.Delete(mt.ID)
	}

	return nil
}
