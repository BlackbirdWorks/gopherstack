package ec2

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"time"
)

// dhcpOptionsDefault is the sentinel value AWS uses for "reset to default DHCP options".
const dhcpOptionsDefault = "default"

// ---- Error sentinels ----

var (
	// ErrTransitGatewayNotFound is returned when a TGW ID does not exist.
	ErrTransitGatewayNotFound = errors.New("InvalidTransitGatewayID.NotFound")
	// ErrTGWAttachmentNotFound is returned when a TGW VPC attachment ID does not exist.
	ErrTGWAttachmentNotFound = errors.New("InvalidTransitGatewayAttachmentID.NotFound")
	// ErrFlowLogNotFound is returned when a flow log ID does not exist.
	ErrFlowLogNotFound = errors.New("InvalidFlowLogId.NotFound")
	// ErrDhcpOptionsNotFound is returned when a DHCP options set ID does not exist.
	ErrDhcpOptionsNotFound = errors.New("InvalidDhcpOptionsID.NotFound")
)

// ---- Data types ----

// FlowLog represents a VPC Flow Log record.
type FlowLog struct {
	CreationTime       time.Time `json:"creationTime"`
	FlowLogID          string    `json:"flowLogId,omitempty"`
	ResourceID         string    `json:"resourceId,omitempty"`
	TrafficType        string    `json:"trafficType,omitempty"`
	LogDestinationType string    `json:"logDestinationType,omitempty"`
	LogDestination     string    `json:"logDestination,omitempty"`
	FlowLogStatus      string    `json:"flowLogStatus,omitempty"`
}

// DhcpConfiguration is a single key-value configuration inside a DHCP options set.
type DhcpConfiguration struct {
	Key    string   `json:"key,omitempty"`
	Values []string `json:"values,omitempty"`
}

// DhcpOptions represents an EC2 DHCP options set.
type DhcpOptions struct {
	DhcpOptionsID    string              `json:"dhcpOptionsId,omitempty"`
	Configurations   []DhcpConfiguration `json:"configurations,omitempty"`
	AssociatedVPCIDs []string            `json:"associatedVpcIds,omitempty"`
}

// LaunchTemplateVersion holds the versioned launch template data.
type LaunchTemplateVersion struct {
	CreateTime         time.Time `json:"createTime"`
	LaunchTemplateID   string    `json:"launchTemplateId,omitempty"`
	LaunchTemplateName string    `json:"launchTemplateName,omitempty"`
	ImageID            string    `json:"imageId,omitempty"`
	InstanceType       string    `json:"instanceType,omitempty"`
	VersionNumber      int64     `json:"versionNumber"`
	DefaultVersion     bool      `json:"defaultVersion,omitempty"`
}

// ---- Transit Gateway VPC Attachments ----

// CreateTransitGatewayVpcAttachment creates a new TGW VPC attachment.
func (b *InMemoryBackend) CreateTransitGatewayVpcAttachment(
	tgwID, vpcID string, _ []string,
) (*TransitGatewayVpcAttachment, error) {
	if tgwID == "" {
		return nil, fmt.Errorf("%w: TransitGatewayId is required", ErrInvalidParameter)
	}

	if vpcID == "" {
		return nil, fmt.Errorf("%w: VpcId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateTransitGatewayVpcAttachment")
	defer b.mu.Unlock()

	if _, ok := b.transitGateways.Get(tgwID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrTransitGatewayNotFound, tgwID)
	}

	att := &TransitGatewayVpcAttachment{
		TransitGatewayAttachmentID: newTransitGatewayAttachmentID(),
		TransitGatewayID:           tgwID,
		VpcID:                      vpcID,
		State:                      stateAvailable,
		CreationTime:               time.Now().UTC(),
	}
	b.tgwVpcAttachments.Put(att)

	cp := *att

	return &cp, nil
}

// DescribeTransitGatewayVpcAttachments returns TGW VPC attachments, optionally filtered by attachment IDs.
func (b *InMemoryBackend) DescribeTransitGatewayVpcAttachments(
	ids []string,
) []*TransitGatewayVpcAttachment {
	b.mu.RLock("DescribeTransitGatewayVpcAttachments")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*TransitGatewayVpcAttachment, 0, b.tgwVpcAttachments.Len())

	for _, att := range b.tgwVpcAttachments.All() {
		if len(idSet) > 0 && !idSet[att.TransitGatewayAttachmentID] {
			continue
		}

		cp := *att
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].TransitGatewayAttachmentID < out[j].TransitGatewayAttachmentID
	})

	return out
}

// DeleteTransitGatewayVpcAttachment removes a TGW VPC attachment.
func (b *InMemoryBackend) DeleteTransitGatewayVpcAttachment(id string) error {
	if id == "" {
		return fmt.Errorf("%w: TransitGatewayAttachmentId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteTransitGatewayVpcAttachment")
	defer b.mu.Unlock()

	if _, ok := b.tgwVpcAttachments.Get(id); !ok {
		return fmt.Errorf("%w: %s", ErrTGWAttachmentNotFound, id)
	}
	b.tgwVpcAttachments.Delete(id)
	delete(b.tags, id)

	return nil
}

// ---- VPC Flow Logs ----

// CreateFlowLogs creates flow log records for the given resources.
func (b *InMemoryBackend) CreateFlowLogs(
	resourceIDs []string,
	trafficType, logDestinationType, logDestination string,
) ([]*FlowLog, error) {
	if len(resourceIDs) == 0 {
		return nil, fmt.Errorf("%w: at least one ResourceId is required", ErrInvalidParameter)
	}

	if trafficType == "" {
		trafficType = "ALL"
	}

	if logDestinationType == "" {
		logDestinationType = "cloud-watch-logs"
	}

	b.mu.Lock("CreateFlowLogs")
	defer b.mu.Unlock()

	out := make([]*FlowLog, 0, len(resourceIDs))

	for _, rid := range resourceIDs {
		fl := &FlowLog{
			FlowLogID:          newFlowLogID(),
			ResourceID:         rid,
			TrafficType:        trafficType,
			LogDestinationType: logDestinationType,
			LogDestination:     logDestination,
			FlowLogStatus:      "ACTIVE",
			CreationTime:       time.Now().UTC(),
		}
		b.flowLogs.Put(fl)

		cp := *fl
		out = append(out, &cp)
	}

	return out, nil
}

// DescribeFlowLogs returns flow logs, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeFlowLogs(ids []string) []*FlowLog {
	b.mu.RLock("DescribeFlowLogs")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*FlowLog, 0, b.flowLogs.Len())

	for _, fl := range b.flowLogs.All() {
		if len(idSet) > 0 && !idSet[fl.FlowLogID] {
			continue
		}

		cp := *fl
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].FlowLogID < out[j].FlowLogID
	})

	return out
}

// DeleteFlowLogs removes flow logs by ID.
func (b *InMemoryBackend) DeleteFlowLogs(ids []string) error {
	if len(ids) == 0 {
		return fmt.Errorf("%w: at least one FlowLogId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteFlowLogs")
	defer b.mu.Unlock()

	for _, id := range ids {
		if _, ok := b.flowLogs.Get(id); !ok {
			return fmt.Errorf("%w: %s", ErrFlowLogNotFound, id)
		}
	}

	for _, id := range ids {
		b.flowLogs.Delete(id)
		delete(b.tags, id)
	}

	return nil
}

// ---- DHCP Options ----

// CreateDhcpOptions creates a new DHCP options set.
func (b *InMemoryBackend) CreateDhcpOptions(configs []DhcpConfiguration) (*DhcpOptions, error) {
	b.mu.Lock("CreateDhcpOptions")
	defer b.mu.Unlock()

	opts := &DhcpOptions{
		DhcpOptionsID:    newDHCPOptionsID(),
		Configurations:   configs,
		AssociatedVPCIDs: []string{},
	}
	b.dhcpOptionSets.Put(opts)

	cp := *opts

	return &cp, nil
}

// DescribeDhcpOptions returns DHCP option sets, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeDhcpOptions(ids []string) []*DhcpOptions {
	b.mu.RLock("DescribeDhcpOptions")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*DhcpOptions, 0, b.dhcpOptionSets.Len())

	for _, opts := range b.dhcpOptionSets.All() {
		if len(idSet) > 0 && !idSet[opts.DhcpOptionsID] {
			continue
		}

		cp := *opts
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].DhcpOptionsID < out[j].DhcpOptionsID
	})

	return out
}

// AssociateDhcpOptions associates a DHCP options set with a VPC.
func (b *InMemoryBackend) AssociateDhcpOptions(dhcpOptionsID, vpcID string) error {
	if vpcID == "" {
		return fmt.Errorf("%w: VpcId is required", ErrInvalidParameter)
	}

	b.mu.Lock("AssociateDhcpOptions")
	defer b.mu.Unlock()

	if _, ok := b.vpcs.Get(vpcID); !ok {
		return fmt.Errorf("%w: %s", ErrVPCNotFound, vpcID)
	}

	// dhcpOptionsDefault is a special sentinel meaning "reset to AWS default DHCP options"
	if dhcpOptionsID == dhcpOptionsDefault {
		return nil
	}

	opts, ok := b.dhcpOptionSets.Get(dhcpOptionsID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrDhcpOptionsNotFound, dhcpOptionsID)
	}

	if !slices.Contains(opts.AssociatedVPCIDs, vpcID) {
		opts.AssociatedVPCIDs = append(opts.AssociatedVPCIDs, vpcID)
	}

	return nil
}

// DeleteDhcpOptions removes a DHCP options set.
func (b *InMemoryBackend) DeleteDhcpOptions(id string) error {
	if id == "" {
		return fmt.Errorf("%w: DhcpOptionsId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteDhcpOptions")
	defer b.mu.Unlock()

	if _, ok := b.dhcpOptionSets.Get(id); !ok {
		return fmt.Errorf("%w: %s", ErrDhcpOptionsNotFound, id)
	}
	b.dhcpOptionSets.Delete(id)
	delete(b.tags, id)

	return nil
}

// ---- Launch Template extras ----

// ModifyLaunchTemplate updates the default version of a launch template.
func (b *InMemoryBackend) ModifyLaunchTemplate(
	id string,
	defaultVersion int64,
) (*LaunchTemplate, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: LaunchTemplateId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyLaunchTemplate")
	defer b.mu.Unlock()

	lt, ok := b.launchTemplates.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrLaunchTemplateNotFound, id)
	}

	if defaultVersion > 0 && defaultVersion <= lt.LatestVersionNumber {
		lt.DefaultVersionNumber = defaultVersion
	}

	cp := *lt

	return &cp, nil
}

// CreateLaunchTemplateVersion adds a new version to an existing launch template.
func (b *InMemoryBackend) CreateLaunchTemplateVersion(
	id, imageID, instanceType string,
) (*LaunchTemplateVersion, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: LaunchTemplateId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateLaunchTemplateVersion")
	defer b.mu.Unlock()

	lt, ok := b.launchTemplates.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrLaunchTemplateNotFound, id)
	}

	lt.LatestVersionNumber++

	if imageID != "" {
		lt.ImageID = imageID
	}

	if instanceType != "" {
		lt.InstanceType = instanceType
	}

	ver := &LaunchTemplateVersion{
		LaunchTemplateID:   lt.ID,
		LaunchTemplateName: lt.Name,
		ImageID:            lt.ImageID,
		InstanceType:       lt.InstanceType,
		CreateTime:         time.Now().UTC(),
		VersionNumber:      lt.LatestVersionNumber,
		DefaultVersion:     lt.DefaultVersionNumber == lt.LatestVersionNumber,
	}

	return ver, nil
}

// DeleteLaunchTemplateVersions removes specific versions from a launch template.
// It returns the list of successfully deleted version numbers.
func (b *InMemoryBackend) DeleteLaunchTemplateVersions(
	id string,
	versions []int64,
) ([]int64, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: LaunchTemplateId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteLaunchTemplateVersions")
	defer b.mu.Unlock()

	if _, ok := b.launchTemplates.Get(id); !ok {
		return nil, fmt.Errorf("%w: %s", ErrLaunchTemplateNotFound, id)
	}

	// We don't store explicit versions, so just acknowledge the deletion of non-default versions.
	deleted := append([]int64{}, versions...)

	return deleted, nil
}

// GetLaunchTemplateData returns the launch template data for an instance.
func (b *InMemoryBackend) GetLaunchTemplateData(instanceID string) (*LaunchTemplate, error) {
	if instanceID == "" {
		return nil, fmt.Errorf("%w: InstanceId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetLaunchTemplateData")
	defer b.mu.RUnlock()

	inst, ok := b.instances.Get(instanceID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
	}

	lt := &LaunchTemplate{
		ID:           newLaunchTemplateID(),
		ImageID:      inst.ImageID,
		InstanceType: inst.InstanceType,
		CreatedBy:    b.AccountID,
		CreateTime:   inst.LaunchTime,
	}

	return lt, nil
}
