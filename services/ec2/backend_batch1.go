package ec2

import (
	"encoding/base64"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

// Errors for batch1 operations.
var (
	ErrVolumeModificationNotFound         = errors.New("InvalidVolumeModification.NotFound")
	ErrNetworkInterfacePermissionNotFound = errors.New("InvalidPermission.NotFound")
	ErrSubnetCIDRNotFound                 = errors.New(
		"InvalidSubnetCidrBlockAssociationID.NotFound",
	)
)

const (
	stateCompleted          = "completed"
	stateAssociated         = "associated"
	snapshotProgress100     = "100%"
	stateBlockAllSharing    = "block-all-sharing"
	defaultVPCCIDR          = "172.31.0.0/16"
	defaultSubnetCIDR       = "172.31.16.0/20"
	stateMonitoringEnabled  = "enabled"
	stateMonitoringDisabled = "disabled"
	progressComplete        = int64(100)
)

// VolumeModification tracks a ModifyVolume operation.
type VolumeModification struct {
	StartTime         time.Time `json:"startTime"`
	EndTime           time.Time `json:"endTime"`
	VolumeID          string    `json:"volumeID"`
	ModificationState string    `json:"modificationState"`
	TargetVolumeType  string    `json:"targetVolumeType"`
	OrigVolumeType    string    `json:"origVolumeType"`
	TargetSize        int       `json:"targetSize"`
	OrigSize          int       `json:"origSize"`
	TargetIops        int       `json:"targetIops,omitempty"`
	OrigIops          int       `json:"origIops,omitempty"`
	Progress          int64     `json:"progress"`
}

// NetworkInterfacePermission is a permission granted on a network interface.
type NetworkInterfacePermission struct {
	PermissionID       string `json:"permissionID"`
	NetworkInterfaceID string `json:"networkInterfaceID"`
	AwsAccountID       string `json:"awsAccountID,omitempty"`
	AwsService         string `json:"awsService,omitempty"`
	Permission         string `json:"permission"`
	State              string `json:"state"`
}

// PeeringConnectionOptions holds DNS/routing options for a VPC peering connection.
type PeeringConnectionOptions struct {
	AllowDNSResolutionFromRemoteVPC            bool `json:"allowDnsResolutionFromRemoteVpc"`
	AllowEgressFromLocalClassicLinkToRemoteVPC bool `json:"allowEgressFromLocalClassicLinkToRemoteVpc"`
	AllowEgressFromLocalVPCToRemoteClassicLink bool `json:"allowEgressFromLocalVpcToRemoteClassicLink"`
}

// AddressAttribute holds domain-name attributes for an Elastic IP.
type AddressAttribute struct {
	AllocationID string `json:"allocationID"`
	PublicIP     string `json:"publicIP"`
	DomainName   string `json:"domainName,omitempty"`
}

// InstanceMetadataDefaults holds account-level IMDS defaults.
type InstanceMetadataDefaults struct {
	HTTPTokens              string `json:"httpTokens"`
	HTTPEndpoint            string `json:"httpEndpoint"`
	InstanceMetadataTags    string `json:"instanceMetadataTags"`
	HTTPPutResponseHopLimit int    `json:"httpPutResponseHopLimit"`
}

// SubnetCIDRAssociation tracks an IPv6 CIDR associated with a subnet.
type SubnetCIDRAssociation struct {
	AssociationID string `json:"associationID"`
	IPv6CIDRBlock string `json:"ipv6CidrBlock"`
	State         string `json:"state"`
}

// ---- ModifyVolume ----

// ModifyVolume records a volume modification request and immediately applies
// the target type/size so DescribeVolumes reflects the change.
func (b *InMemoryBackend) ModifyVolume(
	volumeID, volumeType string,
	size, iops int,
) (*VolumeModification, error) {
	if volumeID == "" {
		return nil, fmt.Errorf("%w: VolumeId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyVolume")
	defer b.mu.Unlock()

	vol, ok := b.volumes[volumeID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrVolumeNotFound, volumeID)
	}

	mod := &VolumeModification{
		VolumeID:          volumeID,
		ModificationState: stateCompleted,
		OrigVolumeType:    vol.VolumeType,
		OrigSize:          vol.Size,
		TargetVolumeType:  volumeType,
		TargetSize:        size,
		TargetIops:        iops,
		StartTime:         time.Now().UTC(),
		Progress:          progressComplete,
	}
	mod.EndTime = mod.StartTime

	if volumeType != "" {
		vol.VolumeType = volumeType
	} else {
		mod.TargetVolumeType = vol.VolumeType
	}

	if size > 0 {
		vol.Size = size
	} else {
		mod.TargetSize = vol.Size
	}

	b.volumeModifications[volumeID] = mod

	return mod, nil
}

// ---- DescribeVolumeStatus ----

// VolumeStatusItem represents the status of a single EBS volume.
type VolumeStatusItem struct {
	VolumeID         string
	AvailabilityZone string
	VolumeStatus     string
	StatusMessage    string
}

// DescribeVolumeStatus returns health status for the given volumes (all if ids is empty).
func (b *InMemoryBackend) DescribeVolumeStatus(ids []string) []VolumeStatusItem {
	b.mu.RLock("DescribeVolumeStatus")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(ids))
	for _, id := range ids {
		filter[id] = true
	}

	var out []VolumeStatusItem
	for _, vol := range b.volumes {
		if len(filter) > 0 && !filter[vol.ID] {
			continue
		}
		az := vol.AZ
		if az == "" {
			az = b.Region + "a"
		}
		out = append(out, VolumeStatusItem{
			VolumeID:         vol.ID,
			AvailabilityZone: az,
			VolumeStatus:     "ok",
			StatusMessage:    "",
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].VolumeID < out[j].VolumeID })

	return out
}

// ---- DescribeVolumesModifications ----

// DescribeVolumesModifications returns stored modification records.
func (b *InMemoryBackend) DescribeVolumesModifications(ids []string) []*VolumeModification {
	b.mu.RLock("DescribeVolumesModifications")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(ids))
	for _, id := range ids {
		filter[id] = true
	}

	var out []*VolumeModification
	for _, mod := range b.volumeModifications {
		if len(filter) > 0 && !filter[mod.VolumeID] {
			continue
		}
		cp := *mod
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].VolumeID < out[j].VolumeID })

	return out
}

// ---- CopySnapshot ----

// CopySnapshot creates a new snapshot as a copy of an existing one.
func (b *InMemoryBackend) CopySnapshot(sourceSnapshotID, description string) (*Snapshot, error) {
	if sourceSnapshotID == "" {
		return nil, fmt.Errorf("%w: SourceSnapshotId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CopySnapshot")
	defer b.mu.Unlock()

	src, ok := b.snapshots[sourceSnapshotID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrSnapshotNotFound, sourceSnapshotID)
	}

	desc := description
	if desc == "" {
		desc = "Copy of " + sourceSnapshotID
	}

	snap := &Snapshot{
		SnapshotID:  "snap-" + uuid.New().String()[:17],
		VolumeID:    src.VolumeID,
		Description: desc,
		State:       stateCompleted,
		Progress:    snapshotProgress100,
		StartTime:   time.Now().UTC(),
		VolumeSize:  src.VolumeSize,
		Encrypted:   src.Encrypted,
		KmsKeyID:    src.KmsKeyID,
	}
	b.snapshots[snap.SnapshotID] = snap

	return snap, nil
}

// ---- CreateSnapshots ----

// SnapshotEntry is used by CreateSnapshots to specify a volume.
type SnapshotEntry struct {
	VolumeID    string
	SnapshotID  string
	Description string
	State       string
}

// CreateSnapshots creates one snapshot per volumeID in the list.
func (b *InMemoryBackend) CreateSnapshots(
	volumeIDs []string,
	description string,
) ([]*Snapshot, error) {
	if len(volumeIDs) == 0 {
		return nil, fmt.Errorf("%w: at least one VolumeId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateSnapshots")
	defer b.mu.Unlock()

	for _, vid := range volumeIDs {
		if _, ok := b.volumes[vid]; !ok {
			return nil, fmt.Errorf("%w: %s", ErrVolumeNotFound, vid)
		}
	}

	snaps := make([]*Snapshot, 0, len(volumeIDs))
	for _, vid := range volumeIDs {
		vol := b.volumes[vid]
		snap := &Snapshot{
			SnapshotID:  "snap-" + uuid.New().String()[:17],
			VolumeID:    vid,
			Description: description,
			State:       stateCompleted,
			Progress:    snapshotProgress100,
			StartTime:   time.Now().UTC(),
			VolumeSize:  vol.Size,
			Encrypted:   vol.Encrypted,
			KmsKeyID:    vol.KmsKeyID,
		}
		b.snapshots[snap.SnapshotID] = snap
		snaps = append(snaps, snap)
	}

	return snaps, nil
}

// ---- Snapshot block public access ----

// GetSnapshotBlockPublicAccessState returns the account-level block state.
// Returns "block-all-sharing" (default) if never explicitly set.
func (b *InMemoryBackend) GetSnapshotBlockPublicAccessState() string {
	b.mu.RLock("GetSnapshotBlockPublicAccessState")
	defer b.mu.RUnlock()

	if b.snapshotBlockPublicAccess == "" {
		return stateBlockAllSharing
	}

	return b.snapshotBlockPublicAccess
}

// EnableSnapshotBlockPublicAccess sets the block state to block-all-sharing or block-new-sharing.
func (b *InMemoryBackend) EnableSnapshotBlockPublicAccess(state string) error {
	if state != stateBlockAllSharing && state != "block-new-sharing" {
		return fmt.Errorf(
			"%w: State must be "+stateBlockAllSharing+" or block-new-sharing",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("EnableSnapshotBlockPublicAccess")
	defer b.mu.Unlock()

	b.snapshotBlockPublicAccess = state

	return nil
}

// DisableSnapshotBlockPublicAccess sets the block state to unblocked.
func (b *InMemoryBackend) DisableSnapshotBlockPublicAccess() {
	b.mu.Lock("DisableSnapshotBlockPublicAccess")
	defer b.mu.Unlock()

	b.snapshotBlockPublicAccess = "unblocked"
}

// ---- Snapshot tier ----

// SnapshotTierItem contains tier info for a single snapshot.
type SnapshotTierItem struct {
	SnapshotID  string
	VolumeID    string
	StorageTier string
}

// DescribeSnapshotTierStatus returns tier info for given snapshot IDs (or all).
func (b *InMemoryBackend) DescribeSnapshotTierStatus(ids []string) []SnapshotTierItem {
	b.mu.RLock("DescribeSnapshotTierStatus")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(ids))
	for _, id := range ids {
		filter[id] = true
	}

	var out []SnapshotTierItem
	for _, snap := range b.snapshots {
		if len(filter) > 0 && !filter[snap.SnapshotID] {
			continue
		}
		tier := b.snapshotTiers[snap.SnapshotID]
		if tier == "" {
			tier = "standard"
		}
		out = append(out, SnapshotTierItem{
			SnapshotID:  snap.SnapshotID,
			VolumeID:    snap.VolumeID,
			StorageTier: tier,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SnapshotID < out[j].SnapshotID })

	return out
}

// ModifySnapshotTier updates the storage tier for a snapshot ("archive" or "standard").
func (b *InMemoryBackend) ModifySnapshotTier(snapshotID, storageTier string) error {
	if snapshotID == "" {
		return fmt.Errorf("%w: SnapshotId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifySnapshotTier")
	defer b.mu.Unlock()

	if _, ok := b.snapshots[snapshotID]; !ok {
		return fmt.Errorf("%w: %s", ErrSnapshotNotFound, snapshotID)
	}
	b.snapshotTiers[snapshotID] = storageTier

	return nil
}

// ResetSnapshotAttribute resets the createVolumePermission attribute of a snapshot.
func (b *InMemoryBackend) ResetSnapshotAttribute(snapshotID string) error {
	if snapshotID == "" {
		return fmt.Errorf("%w: SnapshotId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ResetSnapshotAttribute")
	defer b.mu.Unlock()

	if _, ok := b.snapshots[snapshotID]; !ok {
		return fmt.Errorf("%w: %s", ErrSnapshotNotFound, snapshotID)
	}
	delete(b.snapshotAttributes, snapshotID)

	return nil
}

// ---- CreateDefaultVpc ----

// CreateDefaultVpc creates a new default VPC in the account.
// Returns error if a default VPC already exists.
func (b *InMemoryBackend) CreateDefaultVpc() (*VPC, error) {
	b.mu.Lock("CreateDefaultVpc")
	defer b.mu.Unlock()

	for _, v := range b.vpcs {
		if v.IsDefault {
			return nil, fmt.Errorf("%w: a default VPC already exists", ErrInvalidParameter)
		}
	}

	vpc := &VPC{
		ID:        "vpc-" + uuid.New().String()[:17],
		CIDRBlock: defaultVPCCIDR,
		IsDefault: true,
	}
	b.vpcs[vpc.ID] = vpc

	return vpc, nil
}

// ---- CreateDefaultSubnet ----

// CreateDefaultSubnet creates a new default subnet in the given availability zone.
func (b *InMemoryBackend) CreateDefaultSubnet(az string) (*Subnet, error) {
	if az == "" {
		az = b.Region + "a"
	}

	b.mu.Lock("CreateDefaultSubnet")
	defer b.mu.Unlock()

	var defaultVPCID string
	for _, v := range b.vpcs {
		if v.IsDefault {
			defaultVPCID = v.ID

			break
		}
	}
	if defaultVPCID == "" {
		return nil, fmt.Errorf("%w: no default VPC found", ErrVPCNotFound)
	}

	subnet := &Subnet{
		ID:                  "subnet-" + uuid.New().String()[:17],
		VPCID:               defaultVPCID,
		CIDRBlock:           defaultSubnetCIDR,
		AvailabilityZone:    az,
		IsDefault:           true,
		MapPublicIPOnLaunch: true,
	}
	b.subnets[subnet.ID] = subnet

	return subnet, nil
}

// ---- AssociateSubnetCidrBlock ----

// AssociateSubnetCidrBlock adds an IPv6 CIDR block to a subnet.
func (b *InMemoryBackend) AssociateSubnetCidrBlock(
	subnetID, ipv6CIDRBlock string,
) (*SubnetCIDRAssociation, error) {
	if subnetID == "" {
		return nil, fmt.Errorf("%w: SubnetId is required", ErrInvalidParameter)
	}
	if ipv6CIDRBlock == "" {
		return nil, fmt.Errorf("%w: Ipv6CidrBlock is required", ErrInvalidParameter)
	}

	b.mu.Lock("AssociateSubnetCidrBlock")
	defer b.mu.Unlock()

	if _, ok := b.subnets[subnetID]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrSubnetNotFound, subnetID)
	}

	assoc := &SubnetCIDRAssociation{
		AssociationID: "subnet-cidr-assoc-" + uuid.New().String()[:8],
		IPv6CIDRBlock: ipv6CIDRBlock,
		State:         stateAssociated,
	}
	b.subnetCIDRAssociations[subnetID] = append(b.subnetCIDRAssociations[subnetID], assoc)

	return assoc, nil
}

// ---- DisassociateSubnetCidrBlock ----

// DisassociateSubnetCidrBlock removes an IPv6 CIDR block association from a subnet.
func (b *InMemoryBackend) DisassociateSubnetCidrBlock(associationID string) (string, error) {
	if associationID == "" {
		return "", fmt.Errorf("%w: AssociationId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DisassociateSubnetCidrBlock")
	defer b.mu.Unlock()

	for subnetID, assocs := range b.subnetCIDRAssociations {
		for i, assoc := range assocs {
			if assoc.AssociationID == associationID {
				b.subnetCIDRAssociations[subnetID] = append(assocs[:i], assocs[i+1:]...)

				return subnetID, nil
			}
		}
	}

	return "", fmt.Errorf("%w: %s", ErrSubnetCIDRNotFound, associationID)
}

// ---- AssociateSecurityGroupVpc ----

// SGVpcAssociationState holds the state of a security group VPC association.
type SGVpcAssociationState struct {
	SGID  string
	VPCID string
	State string
}

// AssociateSecurityGroupVpc extends a security group to an additional VPC.
func (b *InMemoryBackend) AssociateSecurityGroupVpc(
	sgID, vpcID string,
) (*SGVpcAssociationState, error) {
	if sgID == "" {
		return nil, fmt.Errorf("%w: GroupId is required", ErrInvalidParameter)
	}
	if vpcID == "" {
		return nil, fmt.Errorf("%w: VpcId is required", ErrInvalidParameter)
	}

	b.mu.Lock("AssociateSecurityGroupVpc")
	defer b.mu.Unlock()

	if _, ok := b.securityGroups[sgID]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrSecurityGroupNotFound, sgID)
	}
	if _, ok := b.vpcs[vpcID]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrVPCNotFound, vpcID)
	}

	if b.sgVpcAssociations[sgID] == nil {
		b.sgVpcAssociations[sgID] = make(map[string]string)
	}
	b.sgVpcAssociations[sgID][vpcID] = stateAssociated

	return &SGVpcAssociationState{SGID: sgID, VPCID: vpcID, State: stateAssociated}, nil
}

// ---- DisassociateSecurityGroupVpc ----

// DisassociateSecurityGroupVpc removes a security group from a VPC.
func (b *InMemoryBackend) DisassociateSecurityGroupVpc(sgID, vpcID string) error {
	if sgID == "" {
		return fmt.Errorf("%w: GroupId is required", ErrInvalidParameter)
	}
	if vpcID == "" {
		return fmt.Errorf("%w: VpcId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DisassociateSecurityGroupVpc")
	defer b.mu.Unlock()

	if m, ok := b.sgVpcAssociations[sgID]; ok {
		delete(m, vpcID)

		return nil
	}

	return fmt.Errorf("%w: %s is not associated with %s", ErrInvalidParameter, sgID, vpcID)
}

// ---- DescribeSecurityGroupReferences ----

// SGReference represents a reference to a security group from another VPC.
type SGReference struct {
	GroupID                string
	ReferencingVPCID       string
	VpcPeeringConnectionID string
}

// DescribeSecurityGroupReferences returns cross-VPC references for the given SG IDs.
// Returns empty for SGs not referenced externally (stub-level accuracy).
func (b *InMemoryBackend) DescribeSecurityGroupReferences(sgIDs []string) []SGReference {
	b.mu.RLock("DescribeSecurityGroupReferences")
	defer b.mu.RUnlock()

	// Return references based on sg-vpc associations as proxy for cross-VPC rules.
	var out []SGReference
	filter := make(map[string]bool, len(sgIDs))
	for _, id := range sgIDs {
		filter[id] = true
	}
	for sgID, vpcMap := range b.sgVpcAssociations {
		if len(filter) > 0 && !filter[sgID] {
			continue
		}
		for vpcID := range vpcMap {
			if sg, ok := b.securityGroups[sgID]; ok && sg.VPCID != vpcID {
				out = append(out, SGReference{
					GroupID:          sgID,
					ReferencingVPCID: vpcID,
				})
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].GroupID < out[j].GroupID })

	return out
}

// ---- DescribeStaleSecurityGroups ----

// StaleSGItem is a stale security group entry.
type StaleSGItem struct {
	GroupID     string
	GroupName   string
	Description string
	VPCID       string
}

// findDeletedPeerVPCsLocked returns VPC IDs with terminated peering connections to vpcID.
func (b *InMemoryBackend) findDeletedPeerVPCsLocked(vpcID string) map[string]bool {
	result := make(map[string]bool)
	for _, pc := range b.vpcPeeringConnections {
		if pc.State != tgwRouteStateDeleted && pc.State != "rejected" && pc.State != "failed" {
			continue
		}
		if pc.RequesterVpcID == vpcID {
			result[pc.AccepterVpcID] = true
		} else if pc.AccepterVpcID == vpcID {
			result[pc.RequesterVpcID] = true
		}
	}

	return result
}

// hasStaleRuleLocked returns true if sg has any rule referencing a group in a deleted-peer VPC.
func (b *InMemoryBackend) hasStaleRuleLocked(
	sg *SecurityGroup,
	deletedPeerVPCs map[string]bool,
) bool {
	for _, rule := range append(sg.IngressRules, sg.EgressRules...) {
		if rule.SourceGroupID == "" {
			continue
		}
		srcSG, ok := b.securityGroups[rule.SourceGroupID]
		if ok && deletedPeerVPCs[srcSG.VPCID] {
			return true
		}
	}

	return false
}

// DescribeStaleSecurityGroups returns security groups in stale peering state for the VPC.
// In this implementation, stale SGs are those with dangling VPC peering references.
func (b *InMemoryBackend) DescribeStaleSecurityGroups(vpcID string) []StaleSGItem {
	b.mu.RLock("DescribeStaleSecurityGroups")
	defer b.mu.RUnlock()

	deletedPeerVPCs := b.findDeletedPeerVPCsLocked(vpcID)

	var out []StaleSGItem
	for _, sg := range b.securityGroups {
		if sg.VPCID != vpcID || !b.hasStaleRuleLocked(sg, deletedPeerVPCs) {
			continue
		}
		out = append(out, StaleSGItem{
			GroupID:     sg.ID,
			GroupName:   sg.Name,
			Description: sg.Description,
			VPCID:       sg.VPCID,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].GroupID < out[j].GroupID })

	return out
}

// ---- DescribeSecurityGroupVpcAssociations ----

// SGVpcAssocItem is an entry returned by DescribeSecurityGroupVpcAssociations.
type SGVpcAssocItem struct {
	SGID  string
	VPCID string
	State string
}

// DescribeSecurityGroupVpcAssociations returns SG-VPC associations for the given SG IDs.
func (b *InMemoryBackend) DescribeSecurityGroupVpcAssociations(sgIDs []string) []SGVpcAssocItem {
	b.mu.RLock("DescribeSecurityGroupVpcAssociations")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(sgIDs))
	for _, id := range sgIDs {
		filter[id] = true
	}

	var out []SGVpcAssocItem
	for sgID, vpcMap := range b.sgVpcAssociations {
		if len(filter) > 0 && !filter[sgID] {
			continue
		}
		for vpcID, state := range vpcMap {
			out = append(out, SGVpcAssocItem{SGID: sgID, VPCID: vpcID, State: state})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].SGID != out[j].SGID {
			return out[i].SGID < out[j].SGID
		}

		return out[i].VPCID < out[j].VPCID
	})

	return out
}

// ---- ModifyVpcTenancy ----

// ModifyVpcTenancy sets the instance tenancy for a VPC ("default" or "dedicated").
func (b *InMemoryBackend) ModifyVpcTenancy(vpcID, tenancy string) error {
	if vpcID == "" {
		return fmt.Errorf("%w: VpcId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyVpcTenancy")
	defer b.mu.Unlock()

	if _, ok := b.vpcs[vpcID]; !ok {
		return fmt.Errorf("%w: %s", ErrVPCNotFound, vpcID)
	}
	b.vpcTenancy[vpcID] = tenancy

	return nil
}

// ---- ModifyVpcPeeringConnectionOptions ----

// ModifyVpcPeeringConnectionOptions updates DNS options for a VPC peering connection.
func (b *InMemoryBackend) ModifyVpcPeeringConnectionOptions(
	peeringID string,
	opts PeeringConnectionOptions,
) error {
	if peeringID == "" {
		return fmt.Errorf("%w: VpcPeeringConnectionId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyVpcPeeringConnectionOptions")
	defer b.mu.Unlock()

	if _, ok := b.vpcPeeringConnections[peeringID]; !ok {
		return fmt.Errorf("%w: %s", ErrVpcPeeringConnectionNotFound, peeringID)
	}
	o := opts
	b.vpcPeeringOptions[peeringID] = &o

	return nil
}

// GetVpcPeeringConnectionOptions returns stored options for a peering connection.
func (b *InMemoryBackend) GetVpcPeeringConnectionOptions(
	peeringID string,
) *PeeringConnectionOptions {
	b.mu.RLock("GetVpcPeeringConnectionOptions")
	defer b.mu.RUnlock()

	return b.vpcPeeringOptions[peeringID]
}

// ---- EIP attributes ----

// DescribeAddressesAttribute returns domain-name attributes for Elastic IPs.
func (b *InMemoryBackend) DescribeAddressesAttribute(allocationIDs []string) []AddressAttribute {
	b.mu.RLock("DescribeAddressesAttribute")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(allocationIDs))
	for _, id := range allocationIDs {
		filter[id] = true
	}

	var out []AddressAttribute
	for _, addr := range b.addresses {
		if len(filter) > 0 && !filter[addr.AllocationID] {
			continue
		}
		attr := AddressAttribute{
			AllocationID: addr.AllocationID,
			PublicIP:     addr.PublicIP,
		}
		if stored, ok := b.addressAttributes[addr.AllocationID]; ok {
			attr.DomainName = stored.DomainName
		}
		out = append(out, attr)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].AllocationID < out[j].AllocationID })

	return out
}

// ModifyAddressAttribute sets the domain name for an Elastic IP.
func (b *InMemoryBackend) ModifyAddressAttribute(allocationID, domainName string) error {
	if allocationID == "" {
		return fmt.Errorf("%w: AllocationId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyAddressAttribute")
	defer b.mu.Unlock()

	addr, ok := b.addresses[allocationID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrInvalidParameter, allocationID)
	}
	b.addressAttributes[allocationID] = &AddressAttribute{
		AllocationID: allocationID,
		PublicIP:     addr.PublicIP,
		DomainName:   domainName,
	}

	return nil
}

// ResetAddressAttribute clears the domain name for an Elastic IP.
func (b *InMemoryBackend) ResetAddressAttribute(allocationID string) error {
	if allocationID == "" {
		return fmt.Errorf("%w: AllocationId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ResetAddressAttribute")
	defer b.mu.Unlock()

	if _, ok := b.addresses[allocationID]; !ok {
		return fmt.Errorf("%w: %s", ErrInvalidParameter, allocationID)
	}
	delete(b.addressAttributes, allocationID)

	return nil
}

// ---- Instance console output ----

// GetConsoleOutput returns a base64-encoded console output for an instance.
// Instances in this mock don't generate real console output; returns empty string.
func (b *InMemoryBackend) GetConsoleOutput(instanceID string) (string, time.Time, error) {
	if instanceID == "" {
		return "", time.Time{}, fmt.Errorf("%w: InstanceId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetConsoleOutput")
	defer b.mu.RUnlock()

	if _, ok := b.instances[instanceID]; !ok {
		return "", time.Time{}, fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
	}

	output := base64.StdEncoding.EncodeToString([]byte("[ EC2 mock console ]\n"))

	return output, time.Now().UTC(), nil
}

// ---- ModifyInstanceMetadataOptions ----

// IMDSOptions holds per-instance IMDS configuration.
type IMDSOptions struct {
	HTTPTokens              string `json:"httpTokens"`
	HTTPEndpoint            string `json:"httpEndpoint"`
	InstanceMetadataTags    string `json:"instanceMetadataTags"`
	State                   string `json:"state"`
	HTTPPutResponseHopLimit int    `json:"httpPutResponseHopLimit"`
}

// ModifyInstanceMetadataOptions updates IMDS settings for an instance.
func (b *InMemoryBackend) ModifyInstanceMetadataOptions(
	instanceID, httpTokens, httpEndpoint, instanceMetadataTags string,
	hopLimit int,
) (*IMDSOptions, error) {
	if instanceID == "" {
		return nil, fmt.Errorf("%w: InstanceId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyInstanceMetadataOptions")
	defer b.mu.Unlock()

	inst, ok := b.instances[instanceID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
	}

	// Persist tokens setting on instance for DescribeInstances accuracy.
	if httpTokens != "" {
		inst.MetadataOptionsTokens = httpTokens
	}
	if httpEndpoint != "" {
		inst.MetadataOptionsState = httpEndpoint
	}

	opts := &IMDSOptions{
		HTTPTokens:              inst.MetadataOptionsTokens,
		HTTPEndpoint:            inst.MetadataOptionsState,
		HTTPPutResponseHopLimit: hopLimit,
		InstanceMetadataTags:    instanceMetadataTags,
		State:                   "applied",
	}
	if opts.HTTPTokens == "" {
		opts.HTTPTokens = "optional"
	}
	if opts.HTTPEndpoint == "" {
		opts.HTTPEndpoint = "enabled"
	}
	if opts.HTTPPutResponseHopLimit == 0 {
		opts.HTTPPutResponseHopLimit = 1
	}

	b.instanceIMDSOptions[instanceID] = opts

	return opts, nil
}

// ---- InstanceMetadata account defaults ----

// GetInstanceMetadataDefaults returns account-level IMDS defaults.
func (b *InMemoryBackend) GetInstanceMetadataDefaults() *InstanceMetadataDefaults {
	b.mu.RLock("GetInstanceMetadataDefaults")
	defer b.mu.RUnlock()

	if b.instanceMetadataDefaults == nil {
		return &InstanceMetadataDefaults{
			HTTPTokens:              "no-preference",
			HTTPEndpoint:            "enabled",
			HTTPPutResponseHopLimit: 0,
			InstanceMetadataTags:    "no-preference",
		}
	}
	cp := *b.instanceMetadataDefaults

	return &cp
}

// ModifyInstanceMetadataDefaults updates account-level IMDS defaults.
func (b *InMemoryBackend) ModifyInstanceMetadataDefaults(
	httpTokens, httpEndpoint, instanceMetadataTags string,
	hopLimit int,
) error {
	b.mu.Lock("ModifyInstanceMetadataDefaults")
	defer b.mu.Unlock()

	d := &InstanceMetadataDefaults{}
	if b.instanceMetadataDefaults != nil {
		*d = *b.instanceMetadataDefaults
	}
	if httpTokens != "" {
		d.HTTPTokens = httpTokens
	}
	if httpEndpoint != "" {
		d.HTTPEndpoint = httpEndpoint
	}
	if instanceMetadataTags != "" {
		d.InstanceMetadataTags = instanceMetadataTags
	}
	if hopLimit > 0 {
		d.HTTPPutResponseHopLimit = hopLimit
	}
	b.instanceMetadataDefaults = d

	return nil
}

// ---- Instance credit specifications ----

// InstanceCreditSpec holds the CPU credit configuration for burstable instances.
type InstanceCreditSpec struct {
	InstanceID string `json:"instanceID"`
	CPUCredits string `json:"cpuCredits"` // "standard" or "unlimited"
}

// DescribeInstanceCreditSpecifications returns CPU credit specs for the given instances.
func (b *InMemoryBackend) DescribeInstanceCreditSpecifications(ids []string) []InstanceCreditSpec {
	b.mu.RLock("DescribeInstanceCreditSpecifications")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(ids))
	for _, id := range ids {
		filter[id] = true
	}

	var out []InstanceCreditSpec
	for _, inst := range b.instances {
		if len(filter) > 0 && !filter[inst.ID] {
			continue
		}
		credits := b.instanceCreditSpecs[inst.ID]
		if credits == "" {
			// Default: burstable types use "standard", others return nothing in real AWS
			// but we return "standard" to keep responses non-empty.
			credits = "standard"
		}
		out = append(out, InstanceCreditSpec{InstanceID: inst.ID, CPUCredits: credits})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].InstanceID < out[j].InstanceID })

	return out
}

// ModifyInstanceCreditSpecification updates the CPU credit spec for an instance.
func (b *InMemoryBackend) ModifyInstanceCreditSpecification(
	instanceID, cpuCredits string,
) error {
	if instanceID == "" {
		return fmt.Errorf("%w: InstanceId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyInstanceCreditSpecification")
	defer b.mu.Unlock()

	if _, ok := b.instances[instanceID]; !ok {
		return fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
	}
	b.instanceCreditSpecs[instanceID] = cpuCredits

	return nil
}

// ---- DescribeInstanceTopology ----

// InstanceTopologyItem holds topology info for an instance.
type InstanceTopologyItem struct {
	InstanceID       string
	InstanceType     string
	GroupName        string
	AvailabilityZone string
	ZoneID           string
	NetworkNodes     []string
}

// DescribeInstanceTopology returns placement topology info for instances.
func (b *InMemoryBackend) DescribeInstanceTopology(ids []string) []InstanceTopologyItem {
	b.mu.RLock("DescribeInstanceTopology")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(ids))
	for _, id := range ids {
		filter[id] = true
	}

	var out []InstanceTopologyItem
	for _, inst := range b.instances {
		if len(filter) > 0 && !filter[inst.ID] {
			continue
		}
		az := b.Region + "a"
		if sub, ok := b.subnets[inst.SubnetID]; ok && sub.AvailabilityZone != "" {
			az = sub.AvailabilityZone
		}
		out = append(out, InstanceTopologyItem{
			InstanceID:       inst.ID,
			InstanceType:     inst.InstanceType,
			AvailabilityZone: az,
			ZoneID:           az + "1",
			NetworkNodes:     []string{"nn-" + inst.ID[:8]},
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].InstanceID < out[j].InstanceID })

	return out
}

// ---- MonitorInstances / UnmonitorInstances ----

// MonitoringState holds the monitoring state for an instance.
type MonitoringState struct {
	InstanceID string
	State      string // "pending" | "enabled" | "disabling" | "disabled"
}

// MonitorInstances enables detailed monitoring for the given instances.
func (b *InMemoryBackend) MonitorInstances(instanceIDs []string) ([]MonitoringState, error) {
	if len(instanceIDs) == 0 {
		return nil, fmt.Errorf("%w: at least one InstanceId is required", ErrInvalidParameter)
	}

	b.mu.Lock("MonitorInstances")
	defer b.mu.Unlock()

	var out []MonitoringState
	for _, id := range instanceIDs {
		if _, ok := b.instances[id]; !ok {
			return nil, fmt.Errorf("%w: %s", ErrInstanceNotFound, id)
		}
		b.instanceMonitoring[id] = stateMonitoringEnabled
		out = append(out, MonitoringState{InstanceID: id, State: stateMonitoringEnabled})
	}

	return out, nil
}

// UnmonitorInstances disables detailed monitoring for the given instances.
func (b *InMemoryBackend) UnmonitorInstances(instanceIDs []string) ([]MonitoringState, error) {
	if len(instanceIDs) == 0 {
		return nil, fmt.Errorf("%w: at least one InstanceId is required", ErrInvalidParameter)
	}

	b.mu.Lock("UnmonitorInstances")
	defer b.mu.Unlock()

	var out []MonitoringState
	for _, id := range instanceIDs {
		if _, ok := b.instances[id]; !ok {
			return nil, fmt.Errorf("%w: %s", ErrInstanceNotFound, id)
		}
		b.instanceMonitoring[id] = stateMonitoringDisabled
		out = append(out, MonitoringState{InstanceID: id, State: stateMonitoringDisabled})
	}

	return out, nil
}

// ---- Network Interface attribute ----

// NIAttributeResult holds the result of DescribeNetworkInterfaceAttribute.
type NIAttributeResult struct {
	NetworkInterfaceID string
	Description        string
	GroupIDs           []string
	SourceDestCheck    bool
}

// DescribeNetworkInterfaceAttribute returns a requested attribute for a network interface.
func (b *InMemoryBackend) DescribeNetworkInterfaceAttribute(
	niID string, _ string,
) (*NIAttributeResult, error) {
	if niID == "" {
		return nil, fmt.Errorf("%w: NetworkInterfaceId is required", ErrInvalidParameter)
	}

	b.mu.RLock("DescribeNetworkInterfaceAttribute")
	defer b.mu.RUnlock()

	ni, ok := b.networkInterfaces[niID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNetworkInterfaceNotFound, niID)
	}

	return &NIAttributeResult{
		NetworkInterfaceID: niID,
		Description:        ni.Description,
		SourceDestCheck:    ni.SourceDestCheck,
	}, nil
}

// ResetNetworkInterfaceAttribute resets sourceDestCheck to true for a network interface.
func (b *InMemoryBackend) ResetNetworkInterfaceAttribute(niID string) error {
	if niID == "" {
		return fmt.Errorf("%w: NetworkInterfaceId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ResetNetworkInterfaceAttribute")
	defer b.mu.Unlock()

	ni, ok := b.networkInterfaces[niID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrNetworkInterfaceNotFound, niID)
	}
	ni.SourceDestCheck = true

	return nil
}

// ---- Network Interface permissions ----

// DescribeNetworkInterfacePermissions returns permissions for the given NIDs (or all).
func (b *InMemoryBackend) DescribeNetworkInterfacePermissions(
	niIDs []string,
) []*NetworkInterfacePermission {
	b.mu.RLock("DescribeNetworkInterfacePermissions")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(niIDs))
	for _, id := range niIDs {
		filter[id] = true
	}

	var out []*NetworkInterfacePermission
	for _, perm := range b.niPermissions {
		if len(filter) > 0 && !filter[perm.NetworkInterfaceID] {
			continue
		}
		cp := *perm
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PermissionID < out[j].PermissionID })

	return out
}

// CreateNetworkInterfacePermission creates a new permission on a network interface.
func (b *InMemoryBackend) CreateNetworkInterfacePermission(
	niID, awsAccountID, awsService, permission string,
) (*NetworkInterfacePermission, error) {
	if niID == "" {
		return nil, fmt.Errorf("%w: NetworkInterfaceId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateNetworkInterfacePermission")
	defer b.mu.Unlock()

	if _, ok := b.networkInterfaces[niID]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrNetworkInterfaceNotFound, niID)
	}

	perm := &NetworkInterfacePermission{
		PermissionID:       "eni-perm-" + uuid.New().String()[:8],
		NetworkInterfaceID: niID,
		AwsAccountID:       awsAccountID,
		AwsService:         awsService,
		Permission:         permission,
		State:              "granted",
	}
	b.niPermissions[perm.PermissionID] = perm

	return perm, nil
}

// DeleteNetworkInterfacePermission removes a network interface permission.
func (b *InMemoryBackend) DeleteNetworkInterfacePermission(permissionID string) error {
	if permissionID == "" {
		return fmt.Errorf("%w: NetworkInterfacePermissionId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteNetworkInterfacePermission")
	defer b.mu.Unlock()

	if _, ok := b.niPermissions[permissionID]; !ok {
		return fmt.Errorf("%w: %s", ErrNetworkInterfacePermissionNotFound, permissionID)
	}
	delete(b.niPermissions, permissionID)

	return nil
}

// ---- IPv6 address assignment ----

// AssignIpv6Addresses assigns IPv6 addresses to a network interface.
func (b *InMemoryBackend) AssignIpv6Addresses(niID string, count int) ([]string, error) {
	if niID == "" {
		return nil, fmt.Errorf("%w: NetworkInterfaceId is required", ErrInvalidParameter)
	}
	if count < 1 {
		count = 1
	}

	b.mu.Lock("AssignIpv6Addresses")
	defer b.mu.Unlock()

	if _, ok := b.networkInterfaces[niID]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrNetworkInterfaceNotFound, niID)
	}

	assigned := make([]string, 0, count)
	for range count {
		addr := fmt.Sprintf("2001:db8::%s", uuid.New().String()[:4])
		b.niIPv6Addresses[niID] = append(b.niIPv6Addresses[niID], addr)
		assigned = append(assigned, addr)
	}

	return assigned, nil
}

// UnassignIpv6Addresses removes IPv6 addresses from a network interface.
func (b *InMemoryBackend) UnassignIpv6Addresses(niID string, addresses []string) error {
	if niID == "" {
		return fmt.Errorf("%w: NetworkInterfaceId is required", ErrInvalidParameter)
	}

	b.mu.Lock("UnassignIpv6Addresses")
	defer b.mu.Unlock()

	if _, ok := b.networkInterfaces[niID]; !ok {
		return fmt.Errorf("%w: %s", ErrNetworkInterfaceNotFound, niID)
	}

	toRemove := make(map[string]bool, len(addresses))
	for _, a := range addresses {
		toRemove[a] = true
	}

	filtered := b.niIPv6Addresses[niID][:0]
	for _, a := range b.niIPv6Addresses[niID] {
		if !toRemove[a] {
			filtered = append(filtered, a)
		}
	}
	b.niIPv6Addresses[niID] = filtered

	return nil
}

// ---- DescribeAccountAttributes ----

// AccountAttribute represents a single EC2 account attribute.
type AccountAttribute struct {
	Name   string
	Values []string
}

// DescribeAccountAttributes returns fixed account attributes.
func (b *InMemoryBackend) DescribeAccountAttributes(names []string) []AccountAttribute {
	b.mu.RLock("DescribeAccountAttributes")
	defer b.mu.RUnlock()

	all := []AccountAttribute{
		{Name: "supported-platforms", Values: []string{"VPC"}},
		{Name: "default-vpc", Values: []string{"vpc-default"}},
		{Name: "max-instances", Values: []string{"20"}},
		{Name: "vpc-max-security-groups-per-interface", Values: []string{"5"}},
		{Name: "max-elastic-ips", Values: []string{"5"}},
		{Name: "vpc-max-elastic-ips", Values: []string{"5"}},
	}

	if len(names) == 0 {
		return all
	}

	filter := make(map[string]bool, len(names))
	for _, n := range names {
		filter[n] = true
	}

	var out []AccountAttribute
	for _, attr := range all {
		if filter[attr.Name] {
			out = append(out, attr)
		}
	}

	return out
}

// ---- DescribePrefixLists ----

// PrefixList represents an AWS-managed prefix list.
type PrefixList struct {
	PrefixListID   string
	PrefixListName string
	CIDRs          []string
}

// DescribePrefixLists returns AWS-managed prefix lists (static).
func (b *InMemoryBackend) DescribePrefixLists(ids []string) []PrefixList {
	b.mu.RLock("DescribePrefixLists")
	defer b.mu.RUnlock()

	all := []PrefixList{
		{
			PrefixListID:   "pl-63a5400a",
			PrefixListName: "com.amazonaws." + b.Region + ".s3",
			CIDRs:          []string{"52.216.0.0/15", "54.231.0.0/17"},
		},
		{
			PrefixListID:   "pl-02cd2c6b",
			PrefixListName: "com.amazonaws." + b.Region + ".dynamodb",
			CIDRs:          []string{"52.119.224.0/20"},
		},
	}

	if len(ids) == 0 {
		return all
	}

	filter := make(map[string]bool, len(ids))
	for _, id := range ids {
		filter[id] = true
	}

	var out []PrefixList
	for _, pl := range all {
		if filter[pl.PrefixListID] {
			out = append(out, pl)
		}
	}

	return out
}

// ---- ID format ----

// IDFormatItem represents an ID format setting for a resource type.
type IDFormatItem struct {
	Resource   string
	Deadline   string
	UseLongIDs bool
}

// DescribeIDFormat returns ID format settings for the account.
func (b *InMemoryBackend) DescribeIDFormat(resources []string) []IDFormatItem {
	b.mu.RLock("DescribeIDFormat")
	defer b.mu.RUnlock()

	defaults := []string{"instance", "reservation", "snapshot", "volume"}
	filter := make(map[string]bool, len(resources))
	for _, r := range resources {
		filter[r] = true
	}

	var out []IDFormatItem
	for _, r := range defaults {
		if len(filter) > 0 && !filter[r] {
			continue
		}
		useLong := b.idFormatSettings[r]
		out = append(out, IDFormatItem{Resource: r, UseLongIDs: useLong})
	}

	return out
}

// ModifyIDFormat updates the long-ID setting for a resource type.
func (b *InMemoryBackend) ModifyIDFormat(resource string, useLongIDs bool) error {
	if resource == "" {
		return fmt.Errorf("%w: Resource is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyIDFormat")
	defer b.mu.Unlock()

	b.idFormatSettings[resource] = useLongIDs

	return nil
}

// DescribeIdentityIDFormat returns ID format settings for a given principal (same as account-level here).
func (b *InMemoryBackend) DescribeIdentityIDFormat(
	_ string,
	resources []string,
) []IDFormatItem {
	return b.DescribeIDFormat(resources)
}

// ModifyIdentityIDFormat updates the long-ID setting for a resource type (identity-scoped; same store as account).
func (b *InMemoryBackend) ModifyIdentityIDFormat(
	_ string, resource string,
	useLongIDs bool,
) error {
	return b.ModifyIDFormat(resource, useLongIDs)
}

// DescribeAggregateIDFormat returns ID format settings for all resource types.
func (b *InMemoryBackend) DescribeAggregateIDFormat() []IDFormatItem {
	return b.DescribeIDFormat(nil)
}

// DescribePrincipalIDFormat returns ID format for a principal (same as aggregate here).
func (b *InMemoryBackend) DescribePrincipalIDFormat(_ string) []IDFormatItem {
	return b.DescribeIDFormat(nil)
}

// ---- Instance event notification attributes ----

// InstanceEventNotificationAttributes holds account-level event notification settings.
type InstanceEventNotificationAttributes struct {
	IncludeAllTagsOfInstance bool
}

// DescribeInstanceEventNotificationAttributes returns account-level settings.
func (b *InMemoryBackend) DescribeInstanceEventNotificationAttributes() *InstanceEventNotificationAttributes {
	b.mu.RLock("DescribeInstanceEventNotificationAttributes")
	defer b.mu.RUnlock()

	if b.instanceEventNotifAttrs == nil {
		return &InstanceEventNotificationAttributes{}
	}
	cp := *b.instanceEventNotifAttrs

	return &cp
}

// DeregisterInstanceEventNotificationAttributes clears event notification attributes.
func (b *InMemoryBackend) DeregisterInstanceEventNotificationAttributes() {
	b.mu.Lock("DeregisterInstanceEventNotificationAttributes")
	defer b.mu.Unlock()

	b.instanceEventNotifAttrs = nil
}
