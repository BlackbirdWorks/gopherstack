package efs

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// subnetDerivedVpcID returns a stable synthetic VpcID derived from a subnet ID.
// AWS derives VpcId from the subnet; since the mock has no VPC backend, we synthesise
// it deterministically: "subnet-XXXXXXXX" → "vpc-XXXXXXXX", anything else → "vpc-00000000".
func subnetDerivedVpcID(subnetID string) string {
	const prefix = "subnet-"
	if strings.HasPrefix(subnetID, prefix) {
		return "vpc-" + subnetID[len(prefix):]
	}

	return "vpc-00000000"
}

// mountTargetAZName returns the availability zone name for a new mount target.
// If the file system was pinned to a zone (One Zone storage class), that zone is used;
// otherwise the first zone in the region is returned as a default.
func mountTargetAZName(fs *FileSystem, region string) string {
	if fs.AvailabilityZoneName != "" {
		return fs.AvailabilityZoneName
	}

	return region + "a"
}

// azNameToID converts an AZ name like "us-east-1a" to an AZ ID like "use1-az1".
// The mapping is approximate but sufficient for mock parity.
func azNameToID(azName string) string {
	if azName == "" {
		return ""
	}
	// Strip the trailing zone letter (a=1, b=2, …) to build the ID suffix.
	letter := azName[len(azName)-1]
	suffix := int(letter-'a') + 1
	// Compress the region prefix: drop hyphens and digits except the last digit.
	// e.g. "us-east-1" → "use1", "us-west-2" → "usw2", "eu-west-1" → "euw1"
	regionPart := azName[:len(azName)-1] // strip trailing letter
	if idx := strings.LastIndex(regionPart, "-"); idx >= 0 {
		regionPart = strings.ReplaceAll(regionPart[:idx], "-", "") + regionPart[idx+1:]
	}

	return fmt.Sprintf("%s-az%d", regionPart, suffix)
}

// CreateMountTarget creates a mount target for a file system.
// Returns ErrMountTargetConflict if a mount target already exists in the same subnet.
func (b *InMemoryBackend) CreateMountTarget(
	ctx context.Context,
	req CreateMountTargetRequest,
) (*MountTarget, error) {
	if req.IPAddressType == "" {
		req.IPAddressType = ipAddressTypeIPv4Only
	}
	switch req.IPAddressType {
	case ipAddressTypeIPv4Only, ipAddressTypeIPv6Only, ipAddressTypeDualStack:
		// valid
	default:
		return nil, fmt.Errorf(
			"%w: invalid IpAddressType %q, must be IPV4_ONLY, IPV6_ONLY, or DUAL_STACK",
			ErrValidation,
			req.IPAddressType,
		)
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateMountTarget")
	defer b.mu.Unlock()

	fs, ok := b.fileSystems.Get(regionKey(region, req.FileSystemID))
	if !ok {
		return nil, fmt.Errorf("%w: file system %s not found", ErrNotFound, req.FileSystemID)
	}

	// O(1) subnet conflict check via index: one mount target per subnet per file system.
	if req.SubnetID != "" {
		if b.mtSubnetIdx[region] != nil {
			if _, dup := b.mtSubnetIdx[region][req.FileSystemID][req.SubnetID]; dup {
				return nil, fmt.Errorf(
					"%w: mount target already exists for file system %s in subnet %s",
					ErrMountTargetConflict,
					req.FileSystemID,
					req.SubnetID,
				)
			}
		}
	}

	if len(req.SecurityGroups) > maxSecurityGroups {
		return nil, fmt.Errorf(
			"%w: too many security groups: %d (max %d)",
			ErrSecurityGroupLimitExceeded,
			len(req.SecurityGroups),
			maxSecurityGroups,
		)
	}

	id := "fsmt-" + uuid.NewString()[:8]
	mtARN := arn.Build("elasticfilesystem", region, b.accountID, "mount-target/"+id)
	eniID := "eni-" + uuid.NewString()[:8]

	sgs := make([]string, len(req.SecurityGroups))
	copy(sgs, req.SecurityGroups)

	// Synthesise VPC and AZ fields from the subnet ID and file system config.
	// AWS derives these from real VPC/subnet metadata; the mock approximates them
	// deterministically so callers receive non-empty, stable values.
	vpcID := subnetDerivedVpcID(req.SubnetID)
	azName := mountTargetAZName(fs, region)
	azID := azNameToID(azName)

	mt := &MountTarget{
		region:               region,
		MountTargetID:        id,
		MountTargetArn:       mtARN,
		FileSystemID:         req.FileSystemID,
		SubnetID:             req.SubnetID,
		VpcID:                vpcID,
		AvailabilityZoneName: azName,
		AvailabilityZoneID:   azID,
		IPAddress:            req.IPAddress,
		IPAddressType:        req.IPAddressType,
		IPv6Address:          req.IPv6Address,
		NetworkInterfaceID:   eniID,
		LifeCycleState:       statusAvailable,
		OwnerID:              b.accountID,
		SecurityGroups:       sgs,
	}
	b.mountTargets.Put(mt)
	b.mountTargetsByARN.Put(mt)
	// Update subnet index for O(1) conflict detection.
	b.mtSubnetStore(region, req.FileSystemID)[req.SubnetID] = id
	fs.NumberOfMountTargets++

	cp := *mt
	cp.SecurityGroups = make([]string, len(mt.SecurityGroups))
	copy(cp.SecurityGroups, mt.SecurityGroups)

	return &cp, nil
}

// DescribeMountTargets returns mount targets, optionally filtered by file system ID or mount target ID.
func (b *InMemoryBackend) DescribeMountTargets(
	ctx context.Context,
	fileSystemID, mountTargetID, marker string, maxItems int,
) ([]*MountTarget, string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeMountTargets")
	defer b.mu.RUnlock()

	return describeByIDOrFilter(
		func(id string) (*MountTarget, bool) { return b.mountTargets.Get(regionKey(region, id)) },
		b.mountTargetsByRegion.Get(region),
		mountTargetID, ErrMountTargetNotFound,
		fileSystemID,
		func(mt *MountTarget) string { return mt.FileSystemID },
		copyMountTarget,
		func(mt *MountTarget) string { return mt.MountTargetID },
		marker, maxItems,
	)
}

func copyMountTarget(mt *MountTarget) *MountTarget {
	cp := *mt
	cp.SecurityGroups = make([]string, len(mt.SecurityGroups))
	copy(cp.SecurityGroups, mt.SecurityGroups)

	return &cp
}

// DeleteMountTarget deletes a mount target by ID.
func (b *InMemoryBackend) DeleteMountTarget(ctx context.Context, mountTargetID string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteMountTarget")
	defer b.mu.Unlock()

	mt, ok := b.mountTargets.Get(regionKey(region, mountTargetID))
	if !ok {
		return fmt.Errorf("%w: mount target %s not found", ErrMountTargetNotFound, mountTargetID)
	}
	if fs, found := b.fileSystems.Get(regionKey(region, mt.FileSystemID)); found {
		fs.NumberOfMountTargets--
	}
	b.mountTargetsByARN.Delete(regionKey(region, mt.MountTargetArn))
	b.mountTargets.Delete(regionKey(region, mountTargetID))
	// Clean up subnet index.
	if b.mtSubnetIdx[region] != nil && b.mtSubnetIdx[region][mt.FileSystemID] != nil {
		delete(b.mtSubnetIdx[region][mt.FileSystemID], mt.SubnetID)
	}

	return nil
}

// AddMountTargetInternal inserts a pre-built MountTarget directly into the backend (test seed helper).
func (b *InMemoryBackend) AddMountTargetInternal(mt *MountTarget) {
	b.mu.Lock("AddMountTargetInternal")
	defer b.mu.Unlock()

	mt.region = regionFromARN(mt.MountTargetArn, b.region)
	b.mountTargets.Put(mt)
	b.mountTargetsByARN.Put(mt)
}
