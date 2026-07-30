package ec2

import (
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

// ---- VPC Block Public Access (BPA) constants ----

const (
	vpcBPABlockModeOff            = "off"
	vpcBPAStateDefault            = "default-state"
	vpcBPAStateUpdateComplete     = "update-complete"
	vpcBPAExclusionsAllowed       = "allowed"
	vpcBPAManagedByAccount        = "account"
	vpcBPAExclusionStateComplete  = "create-complete"
	vpcBPAExclusionUpdateComplete = "update-complete"
	vpcBPAExclusionDeleteComplete = "delete-complete"
	vpcBPAExclusionIDPrefixLength = 17
)

// vpcBPAValidBlockModes are the AWS-defined InternetGatewayBlockMode values.
//
//nolint:gochecknoglobals // lookup set
var vpcBPAValidBlockModes = map[string]bool{
	"off":                 true,
	"block-bidirectional": true,
	"block-ingress":       true,
}

// vpcBPAValidExclusionModes are the AWS-defined InternetGatewayExclusionMode values.
//
//nolint:gochecknoglobals // lookup set
var vpcBPAValidExclusionModes = map[string]bool{
	"allow-bidirectional": true,
	"allow-egress":        true,
}

// ClassicLinkInstance tracks an EC2-Classic instance's ClassicLink attachment to a VPC.
type ClassicLinkInstance struct {
	InstanceID string   `json:"instanceId,omitempty"`
	VpcID      string   `json:"vpcId,omitempty"`
	Groups     []string `json:"groups,omitempty"`
}

// VpcBlockPublicAccessOptions is the per-account+region VPC Block Public
// Access (BPA) configuration singleton.
type VpcBlockPublicAccessOptions struct {
	LastUpdateTimestamp      time.Time `json:"lastUpdateTimestamp"`
	AwsAccountID             string    `json:"awsAccountId,omitempty"`
	AwsRegion                string    `json:"awsRegion,omitempty"`
	InternetGatewayBlockMode string    `json:"internetGatewayBlockMode,omitempty"`
	State                    string    `json:"state,omitempty"`
	Reason                   string    `json:"reason,omitempty"`
	ManagedBy                string    `json:"managedBy,omitempty"`
	ExclusionsAllowed        string    `json:"exclusionsAllowed,omitempty"`
}

// VpcBlockPublicAccessExclusion represents an exclusion from the account's VPC
// Block Public Access mode for a single VPC or subnet.
type VpcBlockPublicAccessExclusion struct {
	CreationTimestamp            time.Time `json:"creationTimestamp"`
	LastUpdateTimestamp          time.Time `json:"lastUpdateTimestamp"`
	ExclusionID                  string    `json:"exclusionId,omitempty"`
	ResourceArn                  string    `json:"resourceArn,omitempty"`
	VpcID                        string    `json:"vpcId,omitempty"`
	SubnetID                     string    `json:"subnetId,omitempty"`
	InternetGatewayExclusionMode string    `json:"internetGatewayExclusionMode,omitempty"`
	State                        string    `json:"state,omitempty"`
	Reason                       string    `json:"reason,omitempty"`
}

func cloneVpcBPAExclusion(excl *VpcBlockPublicAccessExclusion) *VpcBlockPublicAccessExclusion {
	cp := *excl

	return &cp
}

// ---- VPC ClassicLink ----

// AttachClassicLinkVpc links an EC2-Classic instance to a ClassicLink-enabled VPC.
func (b *InMemoryBackend) AttachClassicLinkVpc(instanceID, vpcID string, groups []string) error {
	if instanceID == "" {
		return fmt.Errorf("%w: InstanceId is required", ErrInvalidParameter)
	}

	if vpcID == "" {
		return fmt.Errorf("%w: VpcId is required", ErrInvalidParameter)
	}

	b.mu.Lock("AttachClassicLinkVpc")
	defer b.mu.Unlock()

	if _, ok := b.instances.Get(instanceID); !ok {
		return fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
	}

	vpc, ok := b.vpcs.Get(vpcID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrVPCNotFound, vpcID)
	}

	if !vpc.ClassicLinkEnabled {
		return fmt.Errorf("%w: %s is not enabled for ClassicLink", ErrVpcClassicLinkDisabled, vpcID)
	}

	for _, groupID := range groups {
		if _, sgOK := b.securityGroups.Get(groupID); !sgOK {
			return fmt.Errorf("%w: %s", ErrSecurityGroupNotFound, groupID)
		}
	}
	b.classicLinkInstances.Put(&ClassicLinkInstance{
		InstanceID: instanceID,
		VpcID:      vpcID,
		Groups:     append([]string(nil), groups...),
	})

	return nil
}

// DetachClassicLinkVpc unlinks a ClassicLink instance from a VPC.
func (b *InMemoryBackend) DetachClassicLinkVpc(instanceID, vpcID string) error {
	if instanceID == "" {
		return fmt.Errorf("%w: InstanceId is required", ErrInvalidParameter)
	}

	if vpcID == "" {
		return fmt.Errorf("%w: VpcId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DetachClassicLinkVpc")
	defer b.mu.Unlock()

	link, ok := b.classicLinkInstances.Get(instanceID)
	if !ok || link.VpcID != vpcID {
		return fmt.Errorf("%w: %s", ErrClassicLinkInstanceNotFound, instanceID)
	}
	b.classicLinkInstances.Delete(instanceID)

	return nil
}

// DescribeClassicLinkInstances returns linked ClassicLink instances, optionally filtered by instance ID.
func (b *InMemoryBackend) DescribeClassicLinkInstances(instanceIDs []string) []*ClassicLinkInstance {
	b.mu.RLock("DescribeClassicLinkInstances")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(instanceIDs))
	for _, id := range instanceIDs {
		idSet[id] = true
	}

	out := make([]*ClassicLinkInstance, 0, b.classicLinkInstances.Len())

	for _, link := range b.classicLinkInstances.All() {
		if len(idSet) > 0 && !idSet[link.InstanceID] {
			continue
		}

		cp := *link
		cp.Groups = append([]string(nil), link.Groups...)
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].InstanceID < out[j].InstanceID })

	return out
}

// EnableVpcClassicLink enables a VPC for ClassicLink.
func (b *InMemoryBackend) EnableVpcClassicLink(vpcID string) error {
	if vpcID == "" {
		return fmt.Errorf("%w: VpcId is required", ErrInvalidParameter)
	}

	b.mu.Lock("EnableVpcClassicLink")
	defer b.mu.Unlock()

	vpc, ok := b.vpcs.Get(vpcID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrVPCNotFound, vpcID)
	}

	vpc.ClassicLinkEnabled = true

	return nil
}

// DisableVpcClassicLink disables ClassicLink for a VPC. Fails if any instances
// remain linked to it, matching real AWS's dependency check.
func (b *InMemoryBackend) DisableVpcClassicLink(vpcID string) error {
	if vpcID == "" {
		return fmt.Errorf("%w: VpcId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DisableVpcClassicLink")
	defer b.mu.Unlock()

	vpc, ok := b.vpcs.Get(vpcID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrVPCNotFound, vpcID)
	}

	for _, link := range b.classicLinkInstances.All() {
		if link.VpcID == vpcID {
			return fmt.Errorf("%w: %s has linked EC2-Classic instances", ErrDependencyViolation, vpcID)
		}
	}

	vpc.ClassicLinkEnabled = false

	return nil
}

// describeVpcsByIDsLocked returns copies of the requested VPCs (or all VPCs if
// ids is empty). Must be called with b.mu held for reading.
func (b *InMemoryBackend) describeVpcsByIDsLocked(ids []string) ([]*VPC, error) {
	if len(ids) == 0 {
		out := make([]*VPC, 0, b.vpcs.Len())
		for _, vpc := range b.vpcs.All() {
			cp := *vpc
			out = append(out, &cp)
		}

		sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

		return out, nil
	}

	out := make([]*VPC, 0, len(ids))

	for _, id := range ids {
		vpc, ok := b.vpcs.Get(id)
		if !ok {
			return nil, fmt.Errorf("%w: %s", ErrVPCNotFound, id)
		}

		cp := *vpc
		out = append(out, &cp)
	}

	return out, nil
}

// DescribeVpcClassicLink returns the ClassicLink status of VPCs, optionally filtered by ID.
func (b *InMemoryBackend) DescribeVpcClassicLink(vpcIDs []string) ([]*VPC, error) {
	b.mu.RLock("DescribeVpcClassicLink")
	defer b.mu.RUnlock()

	return b.describeVpcsByIDsLocked(vpcIDs)
}

// EnableVpcClassicLinkDNSSupport enables ClassicLink DNS support for a VPC.
func (b *InMemoryBackend) EnableVpcClassicLinkDNSSupport(vpcID string) error {
	if vpcID == "" {
		return fmt.Errorf("%w: VpcId is required", ErrInvalidParameter)
	}

	b.mu.Lock("EnableVpcClassicLinkDnsSupport")
	defer b.mu.Unlock()

	vpc, ok := b.vpcs.Get(vpcID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrVPCNotFound, vpcID)
	}

	vpc.ClassicLinkDNSSupported = true

	return nil
}

// DisableVpcClassicLinkDNSSupport disables ClassicLink DNS support for a VPC.
func (b *InMemoryBackend) DisableVpcClassicLinkDNSSupport(vpcID string) error {
	if vpcID == "" {
		return fmt.Errorf("%w: VpcId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DisableVpcClassicLinkDnsSupport")
	defer b.mu.Unlock()

	vpc, ok := b.vpcs.Get(vpcID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrVPCNotFound, vpcID)
	}

	vpc.ClassicLinkDNSSupported = false

	return nil
}

// DescribeVpcClassicLinkDNSSupport returns ClassicLink DNS support status for VPCs.
func (b *InMemoryBackend) DescribeVpcClassicLinkDNSSupport(vpcIDs []string) ([]*VPC, error) {
	b.mu.RLock("DescribeVpcClassicLinkDnsSupport")
	defer b.mu.RUnlock()

	return b.describeVpcsByIDsLocked(vpcIDs)
}

// ---- VPC Block Public Access ----

// DescribeVpcBlockPublicAccessOptions returns the account+region VPC BPA options.
func (b *InMemoryBackend) DescribeVpcBlockPublicAccessOptions() *VpcBlockPublicAccessOptions {
	b.mu.RLock("DescribeVpcBlockPublicAccessOptions")
	defer b.mu.RUnlock()

	cp := *b.vpcBlockPublicAccessOptions
	cp.AwsAccountID = b.AccountID
	cp.AwsRegion = b.Region

	return &cp
}

// ModifyVpcBlockPublicAccessOptions updates the account+region VPC BPA mode.
func (b *InMemoryBackend) ModifyVpcBlockPublicAccessOptions(blockMode string) (*VpcBlockPublicAccessOptions, error) {
	if !vpcBPAValidBlockModes[blockMode] {
		return nil, fmt.Errorf("%w: InternetGatewayBlockMode %q is invalid", ErrInvalidParameter, blockMode)
	}

	b.mu.Lock("ModifyVpcBlockPublicAccessOptions")
	defer b.mu.Unlock()

	b.vpcBlockPublicAccessOptions.InternetGatewayBlockMode = blockMode
	b.vpcBlockPublicAccessOptions.State = vpcBPAStateUpdateComplete
	b.vpcBlockPublicAccessOptions.ManagedBy = vpcBPAManagedByAccount
	b.vpcBlockPublicAccessOptions.LastUpdateTimestamp = time.Now().UTC()

	cp := *b.vpcBlockPublicAccessOptions
	cp.AwsAccountID = b.AccountID
	cp.AwsRegion = b.Region

	return &cp, nil
}

// CreateVpcBlockPublicAccessExclusion creates an exclusion for a VPC or subnet
// from the account's VPC Block Public Access mode.
func (b *InMemoryBackend) CreateVpcBlockPublicAccessExclusion(
	vpcID, subnetID, mode string,
	tags map[string]string,
) (*VpcBlockPublicAccessExclusion, error) {
	if !vpcBPAValidExclusionModes[mode] {
		return nil, fmt.Errorf("%w: InternetGatewayExclusionMode %q is invalid", ErrInvalidParameter, mode)
	}

	if vpcID == "" && subnetID == "" {
		return nil, fmt.Errorf("%w: one of VpcId or SubnetId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateVpcBlockPublicAccessExclusion")
	defer b.mu.Unlock()

	if vpcID != "" {
		if _, ok := b.vpcs.Get(vpcID); !ok {
			return nil, fmt.Errorf("%w: %s", ErrVPCNotFound, vpcID)
		}
	}

	if subnetID != "" {
		if _, ok := b.subnets.Get(subnetID); !ok {
			return nil, fmt.Errorf("%w: %s", ErrSubnetNotFound, subnetID)
		}
	}

	id := "vpcbpa-exclusion-" + uuid.New().String()[:vpcBPAExclusionIDPrefixLength]
	now := time.Now().UTC()
	arn := "arn:aws:ec2:" + b.Region + ":" + b.AccountID + ":vpc-block-public-access-exclusion/" + id

	excl := &VpcBlockPublicAccessExclusion{
		ExclusionID:                  id,
		ResourceArn:                  arn,
		VpcID:                        vpcID,
		SubnetID:                     subnetID,
		InternetGatewayExclusionMode: mode,
		State:                        vpcBPAExclusionStateComplete,
		CreationTimestamp:            now,
		LastUpdateTimestamp:          now,
	}
	b.vpcBlockPublicAccessExclusions.Put(excl)
	b.setTagsLocked(id, tags)

	return cloneVpcBPAExclusion(excl), nil
}

// ModifyVpcBlockPublicAccessExclusion updates the exclusion mode of an existing exclusion.
func (b *InMemoryBackend) ModifyVpcBlockPublicAccessExclusion(
	exclusionID, mode string,
) (*VpcBlockPublicAccessExclusion, error) {
	if exclusionID == "" {
		return nil, fmt.Errorf("%w: ExclusionId is required", ErrInvalidParameter)
	}

	if !vpcBPAValidExclusionModes[mode] {
		return nil, fmt.Errorf("%w: InternetGatewayExclusionMode %q is invalid", ErrInvalidParameter, mode)
	}

	b.mu.Lock("ModifyVpcBlockPublicAccessExclusion")
	defer b.mu.Unlock()

	excl, ok := b.vpcBlockPublicAccessExclusions.Get(exclusionID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrVpcBlockPublicAccessExclusionNotFound, exclusionID)
	}

	excl.InternetGatewayExclusionMode = mode
	excl.State = vpcBPAExclusionUpdateComplete
	excl.LastUpdateTimestamp = time.Now().UTC()

	return cloneVpcBPAExclusion(excl), nil
}

// DeleteVpcBlockPublicAccessExclusion removes an exclusion.
func (b *InMemoryBackend) DeleteVpcBlockPublicAccessExclusion(
	exclusionID string,
) (*VpcBlockPublicAccessExclusion, error) {
	if exclusionID == "" {
		return nil, fmt.Errorf("%w: ExclusionId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteVpcBlockPublicAccessExclusion")
	defer b.mu.Unlock()

	excl, ok := b.vpcBlockPublicAccessExclusions.Get(exclusionID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrVpcBlockPublicAccessExclusionNotFound, exclusionID)
	}

	excl.State = vpcBPAExclusionDeleteComplete
	excl.LastUpdateTimestamp = time.Now().UTC()
	out := cloneVpcBPAExclusion(excl)
	b.vpcBlockPublicAccessExclusions.Delete(exclusionID)
	delete(b.tags, exclusionID)

	return out, nil
}

// DescribeVpcBlockPublicAccessExclusions returns exclusions, optionally filtered by ID.
func (b *InMemoryBackend) DescribeVpcBlockPublicAccessExclusions(ids []string) []*VpcBlockPublicAccessExclusion {
	b.mu.RLock("DescribeVpcBlockPublicAccessExclusions")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*VpcBlockPublicAccessExclusion, 0, b.vpcBlockPublicAccessExclusions.Len())

	for _, excl := range b.vpcBlockPublicAccessExclusions.All() {
		if len(idSet) > 0 && !idSet[excl.ExclusionID] {
			continue
		}

		out = append(out, cloneVpcBPAExclusion(excl))
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ExclusionID < out[j].ExclusionID })

	return out
}
