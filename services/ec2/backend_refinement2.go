package ec2

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/google/uuid"
)

// Snapshot represents an EBS snapshot.
type Snapshot struct {
	StartTime   time.Time `json:"startTime"`
	VolumeID    string    `json:"volumeID"`
	SnapshotID  string    `json:"snapshotID"`
	Description string    `json:"description"`
	State       string    `json:"state"`
	Progress    string    `json:"progress"`
	KmsKeyId    string    `json:"kmsKeyId,omitempty"`
	VolumeSize  int       `json:"volumeSize"`
	Encrypted   bool      `json:"encrypted"`
}

// NACLEntry represents a single NACL rule entry.
type NACLEntry struct {
	Protocol   string `json:"protocol"`
	CIDRBlock  string `json:"cidrBlock"`
	RuleAction string `json:"ruleAction"`
	RuleNumber int    `json:"ruleNumber"`
	FromPort   int    `json:"fromPort,omitempty"`
	ToPort     int    `json:"toPort,omitempty"`
	Egress     bool   `json:"egress"`
}

// StoredNetworkACL represents a persisted Network ACL (created explicitly via CreateNetworkAcl).
type StoredNetworkACL struct {
	ID             string      `json:"id"`
	VPCID          string      `json:"vpcID"`
	AssociationIDs []string    `json:"associationIDs"`
	Entries        []NACLEntry `json:"entries"`
	IsDefault      bool        `json:"isDefault"`
}

// SecurityGroupRuleDetail represents a full security group rule record as returned by DescribeSecurityGroupRules.
type SecurityGroupRuleDetail struct {
	SecurityGroupRuleID string `json:"securityGroupRuleID"`
	GroupID             string `json:"groupID"`
	Protocol            string `json:"protocol"`
	CIDRIPv4            string `json:"cidrIpv4,omitempty"`
	FromPort            int    `json:"fromPort"`
	ToPort              int    `json:"toPort"`
	IsEgress            bool   `json:"isEgress"`
}

// Additional errors for refinement 2 operations.
var (
	ErrSnapshotNotFound       = errors.New("InvalidSnapshotID.NotFound")
	ErrNetworkACLNotFound     = errors.New("InvalidNetworkAclID.NotFound")
	ErrLaunchTemplateNotFound = errors.New("InvalidLaunchTemplateID.NotFound")
)

// naclDefaultDenyRuleNumber is the AWS-defined default-deny rule number placed at the end of every NACL.
const naclDefaultDenyRuleNumber = 32767

// VPC attribute name constants.
const (
	attrEnableDNSSupport          = "enableDnsSupport"
	attrEnableDNSHostnames        = "enableDnsHostnames"
	attrMapPublicIPOnLaunch       = "mapPublicIpOnLaunch"
	attrEnableResourceNameDNSARec = "enableResourceNameDnsARecordOnLaunch"
)

// ---- Snapshots ----

// CreateSnapshot creates an EBS snapshot from a volume.
func (b *InMemoryBackend) CreateSnapshot(volumeID, description string) (*Snapshot, error) {
	if volumeID == "" {
		return nil, fmt.Errorf("%w: VolumeId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateSnapshot")
	defer b.mu.Unlock()

	vol, ok := b.volumes[volumeID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrVolumeNotFound, volumeID)
	}

	snap := &Snapshot{
		SnapshotID:  "snap-" + uuid.New().String()[:17],
		VolumeID:    volumeID,
		Description: description,
		State:       "completed",
		Progress:    "100%",
		StartTime:   time.Now().UTC(),
		VolumeSize:  vol.Size,
		Encrypted:   vol.Encrypted,
		KmsKeyId:    vol.KmsKeyId,
	}
	b.snapshots[snap.SnapshotID] = snap

	cp := *snap

	return &cp, nil
}

// DescribeSnapshots returns snapshots, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeSnapshots(ids []string) []*Snapshot {
	b.mu.RLock("DescribeSnapshots")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*Snapshot, 0, len(b.snapshots))
	for _, snap := range b.snapshots {
		if len(idSet) > 0 && !idSet[snap.SnapshotID] {
			continue
		}

		cp := *snap
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].SnapshotID < out[j].SnapshotID
	})

	return out
}

// DeleteSnapshot removes a snapshot.
func (b *InMemoryBackend) DeleteSnapshot(id string) error {
	if id == "" {
		return fmt.Errorf("%w: SnapshotId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteSnapshot")
	defer b.mu.Unlock()

	if _, ok := b.snapshots[id]; !ok {
		return fmt.Errorf("%w: %s", ErrSnapshotNotFound, id)
	}

	delete(b.snapshots, id)

	return nil
}

// ---- AMI lifecycle ----

// CopyImage copies an AMI stub, producing a new ID.
func (b *InMemoryBackend) CopyImage(sourceImageID, name, description string) (*AMIStub, error) {
	if sourceImageID == "" {
		return nil, fmt.Errorf("%w: SourceImageId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CopyImage")
	defer b.mu.Unlock()

	var src *AMIStub
	if existing, ok := b.images[sourceImageID]; ok {
		src = existing
	} else {
		for _, a := range stubAMIs {
			if a.ImageID == sourceImageID {
				src = &a

				break
			}
		}
	}

	if src == nil {
		return nil, fmt.Errorf("%w: source AMI %s not found", ErrInvalidParameter, sourceImageID)
	}

	if name == "" {
		name = "copy-of-" + src.Name
	}

	if description == "" {
		description = src.Description
	}

	newImage := &AMIStub{
		ImageID:        "ami-" + uuid.New().String()[:17],
		Name:           name,
		Description:    description,
		Architecture:   src.Architecture,
		Platform:       src.Platform,
		RootDeviceName: src.RootDeviceName,
	}
	b.images[newImage.ImageID] = newImage
	b.imageUsageReports[newImage.ImageID] = &ImageUsageReport{
		ImageID:        newImage.ImageID,
		State:          stateAvailable,
		GenerationDate: time.Now().UTC().Format(time.RFC3339),
	}

	cp := *newImage

	return &cp, nil
}

// DeregisterImage removes an AMI from the image store.
func (b *InMemoryBackend) DeregisterImage(imageID string) error {
	if imageID == "" {
		return fmt.Errorf("%w: ImageId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeregisterImage")
	defer b.mu.Unlock()

	if _, ok := b.images[imageID]; !ok {
		return fmt.Errorf("%w: image %s not found", ErrInvalidParameter, imageID)
	}

	delete(b.images, imageID)
	delete(b.imageUsageReports, imageID)

	return nil
}

// ---- VPC / Subnet attribute mutations ----

// ModifyVpcAttribute enables or disables DNS support or DNS hostnames for a VPC.
func (b *InMemoryBackend) ModifyVpcAttribute(vpcID, attribute string, _ bool) error {
	if vpcID == "" {
		return fmt.Errorf("%w: VpcId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyVpcAttribute")
	defer b.mu.Unlock()

	if _, ok := b.vpcs[vpcID]; !ok {
		return fmt.Errorf("%w: %s", ErrVPCNotFound, vpcID)
	}

	// accepted attributes; mock silently accepts both
	switch attribute {
	case attrEnableDNSSupport, attrEnableDNSHostnames:
		// stored in VPC attribute map – currently no field; AWS just accepts and
		// the setting takes effect; mock records acceptance as a no-op.
		return nil
	default:
		return fmt.Errorf("%w: unknown VPC attribute %q", ErrInvalidParameter, attribute)
	}
}

// ModifySubnetAttribute enables or disables auto-assign public IP for a subnet.
func (b *InMemoryBackend) ModifySubnetAttribute(subnetID, attribute string, value bool) error {
	if subnetID == "" {
		return fmt.Errorf("%w: SubnetId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifySubnetAttribute")
	defer b.mu.Unlock()

	subnet, ok := b.subnets[subnetID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrSubnetNotFound, subnetID)
	}

	switch attribute {
	case attrMapPublicIPOnLaunch:
		subnet.MapPublicIpOnLaunch = value
		return nil
	case attrEnableResourceNameDNSARec:
		return nil
	default:
		return fmt.Errorf("%w: unknown subnet attribute %q", ErrInvalidParameter, attribute)
	}
}

// ---- Network ACL CRUD ----

// CreateNetworkACL creates a non-default network ACL in a VPC.
func (b *InMemoryBackend) CreateNetworkACL(vpcID string) (*StoredNetworkACL, error) {
	if vpcID == "" {
		return nil, fmt.Errorf("%w: VpcId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateNetworkACL")
	defer b.mu.Unlock()

	if _, ok := b.vpcs[vpcID]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrVPCNotFound, vpcID)
	}

	acl := &StoredNetworkACL{
		ID:             "acl-" + uuid.New().String()[:17],
		VPCID:          vpcID,
		IsDefault:      false,
		AssociationIDs: nil,
		Entries: []NACLEntry{
			// AWS default: deny all inbound and outbound
			{
				RuleNumber: naclDefaultDenyRuleNumber, Protocol: "-1",
				CIDRBlock: "0.0.0.0/0", RuleAction: "deny", Egress: false,
			},
			{
				RuleNumber: naclDefaultDenyRuleNumber, Protocol: "-1",
				CIDRBlock: "0.0.0.0/0", RuleAction: "deny", Egress: true,
			},
		},
	}
	b.networkACLs[acl.ID] = acl

	cp := *acl
	cp.Entries = append([]NACLEntry(nil), acl.Entries...)

	return &cp, nil
}

// DeleteNetworkACL removes a non-default network ACL.
func (b *InMemoryBackend) DeleteNetworkACL(id string) error {
	if id == "" {
		return fmt.Errorf("%w: NetworkAclId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteNetworkACL")
	defer b.mu.Unlock()

	acl, ok := b.networkACLs[id]
	if !ok {
		return fmt.Errorf("%w: %s", ErrNetworkACLNotFound, id)
	}

	if acl.IsDefault {
		return fmt.Errorf("%w: cannot delete default network ACL", ErrInvalidParameter)
	}

	delete(b.networkACLs, id)

	return nil
}

// CreateNetworkACLEntry adds a rule to an existing network ACL.
func (b *InMemoryBackend) CreateNetworkACLEntry(
	aclID string, ruleNumber int, protocol, action, cidr string,
	egress bool, fromPort, toPort int,
) error {
	if aclID == "" {
		return fmt.Errorf("%w: NetworkAclId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateNetworkACLEntry")
	defer b.mu.Unlock()

	acl, ok := b.networkACLs[aclID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrNetworkACLNotFound, aclID)
	}

	// check duplicate rule number + direction
	for _, e := range acl.Entries {
		if e.RuleNumber == ruleNumber && e.Egress == egress {
			return fmt.Errorf(
				"%w: rule number %d already exists for egress=%v",
				ErrInvalidParameter, ruleNumber, egress,
			)
		}
	}

	acl.Entries = append(acl.Entries, NACLEntry{
		RuleNumber: ruleNumber,
		Protocol:   protocol,
		RuleAction: action,
		CIDRBlock:  cidr,
		Egress:     egress,
		FromPort:   fromPort,
		ToPort:     toPort,
	})

	return nil
}

// DeleteNetworkACLEntry removes a rule from a network ACL.
func (b *InMemoryBackend) DeleteNetworkACLEntry(aclID string, ruleNumber int, egress bool) error {
	if aclID == "" {
		return fmt.Errorf("%w: NetworkAclId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteNetworkACLEntry")
	defer b.mu.Unlock()

	acl, ok := b.networkACLs[aclID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrNetworkACLNotFound, aclID)
	}

	filtered := acl.Entries[:0]
	found := false

	for _, e := range acl.Entries {
		if e.RuleNumber == ruleNumber && e.Egress == egress {
			found = true

			continue
		}

		filtered = append(filtered, e)
	}

	if !found {
		return fmt.Errorf(
			"%w: rule %d (egress=%v) not found",
			ErrInvalidParameter,
			ruleNumber,
			egress,
		)
	}

	acl.Entries = filtered

	return nil
}

// DescribeStoredNetworkAcls returns explicitly created network ACLs, filtered by VPC IDs.
func (b *InMemoryBackend) DescribeStoredNetworkAcls(ids []string) []*StoredNetworkACL {
	b.mu.RLock("DescribeStoredNetworkAcls")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*StoredNetworkACL, 0, len(b.networkACLs))
	for _, acl := range b.networkACLs {
		if len(idSet) > 0 && !idSet[acl.ID] {
			continue
		}

		cp := *acl
		cp.Entries = append([]NACLEntry(nil), acl.Entries...)
		cp.AssociationIDs = append([]string(nil), acl.AssociationIDs...)
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].ID < out[j].ID
	})

	return out
}

// ---- Security group rules ----

// DescribeSecurityGroupRules returns all ingress and egress rules for the given group.
func (b *InMemoryBackend) DescribeSecurityGroupRules(
	groupID string,
) ([]*SecurityGroupRuleDetail, error) {
	if groupID == "" {
		return nil, fmt.Errorf("%w: GroupId is required", ErrInvalidParameter)
	}

	b.mu.RLock("DescribeSecurityGroupRules")
	defer b.mu.RUnlock()

	sg, ok := b.securityGroups[groupID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrSecurityGroupNotFound, groupID)
	}

	var out []*SecurityGroupRuleDetail

	for i, r := range sg.IngressRules {
		out = append(out, &SecurityGroupRuleDetail{
			SecurityGroupRuleID: fmt.Sprintf("sgr-%s-in-%d", groupID, i),
			GroupID:             groupID,
			Protocol:            r.Protocol,
			CIDRIPv4:            r.IPRange,
			FromPort:            r.FromPort,
			ToPort:              r.ToPort,
			IsEgress:            false,
		})
	}

	for i, r := range sg.EgressRules {
		out = append(out, &SecurityGroupRuleDetail{
			SecurityGroupRuleID: fmt.Sprintf("sgr-%s-out-%d", groupID, i),
			GroupID:             groupID,
			Protocol:            r.Protocol,
			CIDRIPv4:            r.IPRange,
			FromPort:            r.FromPort,
			ToPort:              r.ToPort,
			IsEgress:            true,
		})
	}

	return out, nil
}

// ModifySecurityGroupRules updates one or more rules (by position index) within a security group.
// Only protocol, IPRange, and port range can be mutated; egress/ingress direction is immutable.
func (b *InMemoryBackend) ModifySecurityGroupRules(
	groupID string,
	updates []SecurityGroupRule,
	egress bool,
) error {
	if groupID == "" {
		return fmt.Errorf("%w: GroupId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifySecurityGroupRules")
	defer b.mu.Unlock()

	sg, ok := b.securityGroups[groupID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrSecurityGroupNotFound, groupID)
	}

	if egress {
		sg.EgressRules = updates
	} else {
		sg.IngressRules = updates
	}

	return nil
}

// ---- Launch template delete + versions ----

// DeleteLaunchTemplate removes a launch template by ID.
func (b *InMemoryBackend) DeleteLaunchTemplate(id string) error {
	if id == "" {
		return fmt.Errorf("%w: LaunchTemplateId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteLaunchTemplate")
	defer b.mu.Unlock()

	if _, ok := b.launchTemplates[id]; !ok {
		return fmt.Errorf("%w: %s", ErrLaunchTemplateNotFound, id)
	}

	delete(b.launchTemplates, id)

	return nil
}

// DescribeLaunchTemplateVersions returns versions of a specific launch template.
// In this mock, every template has exactly one version.
func (b *InMemoryBackend) DescribeLaunchTemplateVersions(id string) ([]*LaunchTemplate, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: LaunchTemplateId is required", ErrInvalidParameter)
	}

	b.mu.RLock("DescribeLaunchTemplateVersions")
	defer b.mu.RUnlock()

	lt, ok := b.launchTemplates[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrLaunchTemplateNotFound, id)
	}

	cp := *lt

	return []*LaunchTemplate{&cp}, nil
}

// ---- VPC endpoint delete ----

// DeleteVpcEndpoints deletes one or more VPC endpoints.
func (b *InMemoryBackend) DeleteVpcEndpoints(ids []string) ([]string, error) {
	if len(ids) == 0 {
		return nil, fmt.Errorf("%w: at least one VpcEndpointId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteVpcEndpoints")
	defer b.mu.Unlock()

	var unsuccessful []string

	for _, id := range ids {
		if _, ok := b.vpcEndpoints[id]; !ok {
			unsuccessful = append(unsuccessful, id)

			continue
		}

		delete(b.vpcEndpoints, id)
	}

	if len(unsuccessful) > 0 {
		return unsuccessful, fmt.Errorf(
			"%w: endpoints not found: %v",
			ErrVpcEndpointNotFound,
			unsuccessful,
		)
	}

	return nil, nil
}

// ---- Sorted describe helpers ----

// DescribeLaunchTemplatesSorted returns launch templates sorted by ID.
func (b *InMemoryBackend) DescribeLaunchTemplatesSorted(names []string) []*LaunchTemplate {
	ts := b.DescribeLaunchTemplates(names)
	sort.Slice(ts, func(i, j int) bool {
		return ts[i].ID < ts[j].ID
	})

	return ts
}

// DescribeVpcEndpointsByVPC returns VPC endpoints for a specific VPC.
func (b *InMemoryBackend) DescribeVpcEndpointsByVPC(vpcID string) []*VpcEndpoint {
	b.mu.RLock("DescribeVpcEndpointsByVPC")
	defer b.mu.RUnlock()

	var out []*VpcEndpoint

	for _, ep := range b.vpcEndpoints {
		if ep.VPCID != vpcID {
			continue
		}

		cp := *ep
		cp.SubnetIDs = append([]string(nil), ep.SubnetIDs...)
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].ID < out[j].ID
	})

	return out
}

// DescribeNetworkAclsFiltered returns ACLs from both default (auto-generated) and
// stored (CreateNetworkAcl) collections, with optional VPC-ID filter.
func (b *InMemoryBackend) DescribeNetworkAclsFiltered(vpcIDs []string) []*NetworkACL {
	// default ACLs
	defaultACLs := b.DescribeNetworkAcls(vpcIDs)

	// stored ACLs
	b.mu.RLock("DescribeNetworkAclsFiltered")
	defer b.mu.RUnlock()

	allowed := make(map[string]bool, len(vpcIDs))
	for _, id := range vpcIDs {
		allowed[id] = true
	}

	for _, acl := range b.networkACLs {
		if len(allowed) > 0 && !allowed[acl.VPCID] {
			continue
		}

		assocIDs := append([]string(nil), acl.AssociationIDs...)
		defaultACLs = append(defaultACLs, &NetworkACL{
			ID:             acl.ID,
			VPCID:          acl.VPCID,
			IsDefault:      acl.IsDefault,
			AssociationIDs: assocIDs,
		})
	}

	sort.Slice(defaultACLs, func(i, j int) bool {
		return defaultACLs[i].ID < defaultACLs[j].ID
	})

	return defaultACLs
}

// DescribeSnapshotsSorted returns snapshots sorted by snapshot ID.
func (b *InMemoryBackend) DescribeSnapshotsSorted(ids []string) []*Snapshot {
	return b.DescribeSnapshots(ids)
}

// DescribeSubnetsByVPC returns subnets filtered by VPC ID using the secondary index.
func (b *InMemoryBackend) DescribeSubnetsByVPC(vpcID string) []*Subnet {
	b.mu.RLock("DescribeSubnetsByVPC")
	defer b.mu.RUnlock()

	var out []*Subnet

	for _, s := range b.subnets {
		if s.VPCID != vpcID {
			continue
		}

		cp := *s
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].ID < out[j].ID
	})

	return out
}

// DescribeInstancesByVPC returns running instances in a given VPC using the secondary index.
func (b *InMemoryBackend) DescribeInstancesByVPC(vpcID string) []*Instance {
	b.mu.RLock("DescribeInstancesByVPC")
	defer b.mu.RUnlock()

	ids, ok := b.instanceIDsByVPC[vpcID]
	if !ok {
		return nil
	}

	out := make([]*Instance, 0, len(ids))

	for id := range ids {
		if inst, exists := b.instances[id]; exists {
			cp := *inst
			out = append(out, &cp)
		}
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].ID < out[j].ID
	})

	return out
}

// DescribeLaunchTemplatesSortedByName returns launch templates sorted by name (no filter).
func (b *InMemoryBackend) DescribeLaunchTemplatesSortedByName() []*LaunchTemplate {
	ts := b.DescribeLaunchTemplates(nil)
	slices.SortFunc(ts, func(a, b *LaunchTemplate) int {
		if a.Name < b.Name {
			return -1
		}
		if a.Name > b.Name {
			return 1
		}

		return 0
	})

	return ts
}
