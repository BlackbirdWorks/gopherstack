package ec2

import (
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// Errors for the final EC2 parity sweep (gopherstack-5o9).
var (
	// ErrTGWPropagationNotFound is returned when disabling a transit gateway
	// route table propagation that was never enabled.
	ErrTGWPropagationNotFound = errors.New("InvalidTransitGatewayRouteTablePropagation.NotFound")

	// ErrInterruptibleAllocationNotFound is returned when updating an
	// interruptible Capacity Reservation allocation that was never created.
	ErrInterruptibleAllocationNotFound = errors.New("InvalidCapacityReservationId.NotFound")

	// ErrPublicIPNotFound is returned when an Elastic IP lookup by public IP
	// address (rather than allocation ID) fails.
	ErrPublicIPNotFound = errors.New("InvalidAddress.NotFound")
)

const (
	tgwPropagationStateEnabled  = "enabled"
	tgwPropagationStateDisabled = "disabled"

	interruptibleAllocStatusActive = "active"

	moveStatusMovingToVpc = "movingToVpc"

	vpcEndpointConnectionStateRejected = "rejected"

	imageRefTypeInstance       = "ec2:Instance"
	imageRefTypeLaunchTemplate = "ec2:LaunchTemplate"

	// maxImageAncestryDepth bounds the SourceImageID walk in GetImageAncestry
	// so a (never expected) cycle in copy history cannot loop forever.
	maxImageAncestryDepth = 32

	spotPlacementScoreFloor = 1
	spotPlacementScoreCeil  = 10
	spotPlacementScoreRange = spotPlacementScoreCeil - spotPlacementScoreFloor + 1
	spotPlacementScoreTopN  = 10
)

// ---- models ----

// InterruptibleCapacityReservationAllocation tracks an interruptible
// allocation carved out of an existing (source) Capacity Reservation's spare
// capacity. It overlays the source CapacityReservation rather than minting a
// new reservation resource: the real CreateInterruptibleCapacityReservationAllocation
// and UpdateInterruptibleCapacityReservationAllocation outputs identify the
// allocation by its source reservation, not a distinct ID.
type InterruptibleCapacityReservationAllocation struct {
	SourceCapacityReservationID string `json:"sourceCapacityReservationID,omitempty"`
	Status                      string `json:"status,omitempty"`
	TargetInstanceCount         int32  `json:"targetInstanceCount,omitempty"`
}

// CapacityReservationInstanceUsage is a per-account usage row within
// CapacityReservationUsage.
type CapacityReservationInstanceUsage struct {
	AccountID         string `json:"accountID,omitempty"`
	UsedInstanceCount int32  `json:"usedInstanceCount,omitempty"`
}

// CapacityReservationUsage is the real-usage view returned by
// GetCapacityReservationUsage, derived from the source CapacityReservation
// plus any instances currently targeting it and any interruptible allocation
// carved out of it.
type CapacityReservationUsage struct {
	InterruptibleAllocation *InterruptibleCapacityReservationAllocation
	CapacityReservationID   string
	InstanceType            string
	State                   string
	InstanceUsages          []CapacityReservationInstanceUsage
	AvailableInstanceCount  int
	TotalInstanceCount      int
	Interruptible           bool
}

// CapacityReservationTopologyEntry is a single row returned by
// DescribeCapacityReservationTopology, derived from an existing
// CapacityReservation.
type CapacityReservationTopologyEntry struct {
	CapacityReservationID string
	InstanceType          string
	AvailabilityZone      string
}

// MovingAddressStatus represents the EC2-Classic move state of an Elastic IP,
// as tracked by MoveAddressToVpc/DescribeMovingAddresses.
type MovingAddressStatus struct {
	PublicIP   string `json:"publicIP,omitempty"`
	MoveStatus string `json:"moveStatus,omitempty"`
}

// TransitGatewayAttachmentSummary is the unified view of a TGW attachment
// returned by DescribeTransitGatewayAttachments. It aggregates rows sourced
// from the existing per-type attachment maps (VPC, peering, Connect) rather
// than a dedicated map, since each attachment already has a single owning map
// elsewhere in the backend.
type TransitGatewayAttachmentSummary struct {
	TransitGatewayAttachmentID string
	TransitGatewayID           string
	ResourceID                 string
	ResourceType               string
	State                      string
}

// ImageReferenceEntry is a single resource-reference row returned by
// DescribeImageReferences, derived from existing instance and launch
// template state.
type ImageReferenceEntry struct {
	ImageID      string
	ResourceType string
	Arn          string
}

// ImageAncestryEntry is a single AMI in the ancestry chain returned by
// GetImageAncestry.
type ImageAncestryEntry struct {
	ImageID       string
	SourceImageID string
}

// SpotPlacementScoreEntry is a single deterministic Spot placement score
// returned by GetSpotPlacementScores.
type SpotPlacementScoreEntry struct {
	Region             string
	AvailabilityZoneID string
	Score              int32
}

// ElasticGpuStub is the wire shape for DescribeElasticGpus entries. No API in
// this backend attaches Elastic Graphics accelerators to instances (AWS
// retired the feature in 2023), so DescribeElasticGpus never populates one —
// this type exists purely so the response stays correctly shaped, mirroring
// the ClientVpnConnection precedent elsewhere in this package.
type ElasticGpuStub struct {
	ElasticGpuID   string
	InstanceID     string
	ElasticGpuType string
}

// initParityFinalMaps initialises the state added for the final EC2 parity
// sweep (gopherstack-5o9): TGW route table propagations, interruptible
// Capacity Reservation allocations, and EC2-Classic moving-address status
// (split out to keep newInMemoryBackendMaps under the funlen limit).
func initParityFinalMaps(b *InMemoryBackend) {
	b.tgwRTPropagations = make(map[string]map[string]*TransitGatewayRouteTablePropagation)
	b.reachabilityAnalyzerOrgSharing = false
}

// ---- VerifiedAccess modify ----

// ModifyVerifiedAccessGroup updates a Verified Access group's description
// and/or moves it to a different Verified Access instance, mutating the
// existing group created by CreateVerifiedAccessGroup.
func (b *InMemoryBackend) ModifyVerifiedAccessGroup(
	id, instanceID, description string,
) (*VerifiedAccessGroup, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: VerifiedAccessGroupId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyVerifiedAccessGroup")
	defer b.mu.Unlock()

	grp, ok := b.verifiedAccessGroups.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrVerifiedAccessGroupNotFound, id)
	}

	if instanceID != "" {
		if _, exists := b.verifiedAccessInstances.Get(instanceID); !exists {
			return nil, fmt.Errorf("%w: %s", ErrVerifiedAccessInstanceNotFound, instanceID)
		}

		grp.VerifiedAccessInstanceID = instanceID
	}

	if description != "" {
		grp.Description = description
	}

	cp := *grp

	return &cp, nil
}

// ModifyVerifiedAccessInstance updates a Verified Access instance's
// description, mutating the existing instance created by
// CreateVerifiedAccessInstance.
func (b *InMemoryBackend) ModifyVerifiedAccessInstance(id, description string) (*VerifiedAccessInstance, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: VerifiedAccessInstanceId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyVerifiedAccessInstance")
	defer b.mu.Unlock()

	inst, ok := b.verifiedAccessInstances.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrVerifiedAccessInstanceNotFound, id)
	}

	if description != "" {
		inst.Description = description
	}

	cp := *inst

	return &cp, nil
}

// ModifyVerifiedAccessTrustProvider updates a Verified Access trust
// provider's description, mutating the existing trust provider created by
// CreateVerifiedAccessTrustProvider.
func (b *InMemoryBackend) ModifyVerifiedAccessTrustProvider(
	id, description string,
) (*VerifiedAccessTrustProvider, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: VerifiedAccessTrustProviderId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyVerifiedAccessTrustProvider")
	defer b.mu.Unlock()

	tp, ok := b.verifiedAccessTrustProviders.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrVerifiedAccessTrustProviderNF, id)
	}

	if description != "" {
		tp.Description = description
	}

	cp := *tp

	return &cp, nil
}

// ---- Transit Gateway route propagation + unified attachment describe ----

// tgwAttachmentResourceLocked returns the resource ID/type backing a TGW
// attachment, scanning the existing per-type attachment maps. Must be called
// with b.mu held.
func (b *InMemoryBackend) tgwAttachmentResourceLocked(attachmentID string) (string, string) {
	if att, ok := b.tgwVpcAttachments.Get(attachmentID); ok {
		return att.VpcID, tgwResourceTypeVPC
	}

	if att, ok := b.tgwPeeringAttachments.Get(attachmentID); ok {
		return att.AccepterTransitGatewayID, "peering"
	}

	if att, ok := b.tgwConnects.Get(attachmentID); ok {
		return att.TransportTransitGatewayAttachmentID, "connect"
	}

	return "", ""
}

// EnableTransitGatewayRouteTablePropagation enables route propagation from an
// attachment into a TGW route table.
func (b *InMemoryBackend) EnableTransitGatewayRouteTablePropagation(
	routeTableID, attachmentID string,
) (*TransitGatewayRouteTablePropagation, error) {
	if routeTableID == "" {
		return nil, fmt.Errorf("%w: TransitGatewayRouteTableId is required", ErrInvalidParameter)
	}

	if attachmentID == "" {
		return nil, fmt.Errorf("%w: TransitGatewayAttachmentId is required", ErrInvalidParameter)
	}

	b.mu.Lock("EnableTransitGatewayRouteTablePropagation")
	defer b.mu.Unlock()

	if _, ok := b.tgwRouteTables.Get(routeTableID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrTGWRouteTableNotFound, routeTableID)
	}

	if !b.transitGatewayAttachmentExistsLocked(attachmentID) {
		return nil, fmt.Errorf("%w: %s", ErrTGWAttachmentNotFound, attachmentID)
	}

	resourceID, resourceType := b.tgwAttachmentResourceLocked(attachmentID)

	if b.tgwRTPropagations[routeTableID] == nil {
		b.tgwRTPropagations[routeTableID] = make(map[string]*TransitGatewayRouteTablePropagation)
	}

	prop := &TransitGatewayRouteTablePropagation{
		ResourceID:                 resourceID,
		ResourceType:               resourceType,
		State:                      tgwPropagationStateEnabled,
		TransitGatewayAttachmentID: attachmentID,
	}
	b.tgwRTPropagations[routeTableID][attachmentID] = prop

	cp := *prop

	return &cp, nil
}

// DisableTransitGatewayRouteTablePropagation disables a previously enabled
// route propagation.
func (b *InMemoryBackend) DisableTransitGatewayRouteTablePropagation(
	routeTableID, attachmentID string,
) (*TransitGatewayRouteTablePropagation, error) {
	if routeTableID == "" {
		return nil, fmt.Errorf("%w: TransitGatewayRouteTableId is required", ErrInvalidParameter)
	}

	if attachmentID == "" {
		return nil, fmt.Errorf("%w: TransitGatewayAttachmentId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DisableTransitGatewayRouteTablePropagation")
	defer b.mu.Unlock()

	inner, ok := b.tgwRTPropagations[routeTableID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTGWPropagationNotFound, attachmentID)
	}

	prop, ok := inner[attachmentID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTGWPropagationNotFound, attachmentID)
	}

	delete(inner, attachmentID)

	cp := *prop
	cp.State = tgwPropagationStateDisabled

	return &cp, nil
}

// DescribeTransitGatewayAttachments returns a unified view over the existing
// VPC, peering, and Connect attachment maps.
func (b *InMemoryBackend) DescribeTransitGatewayAttachments(ids []string) []*TransitGatewayAttachmentSummary {
	b.mu.RLock("DescribeTransitGatewayAttachments")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(ids))
	for _, id := range ids {
		filter[id] = true
	}

	out := make([]*TransitGatewayAttachmentSummary, 0)

	for _, att := range b.tgwVpcAttachments.All() {
		if len(filter) > 0 && !filter[att.TransitGatewayAttachmentID] {
			continue
		}

		out = append(out, &TransitGatewayAttachmentSummary{
			TransitGatewayAttachmentID: att.TransitGatewayAttachmentID,
			TransitGatewayID:           att.TransitGatewayID,
			ResourceID:                 att.VpcID,
			ResourceType:               tgwResourceTypeVPC,
			State:                      att.State,
		})
	}

	for _, att := range b.tgwPeeringAttachments.All() {
		if len(filter) > 0 && !filter[att.TransitGatewayAttachmentID] {
			continue
		}

		out = append(out, &TransitGatewayAttachmentSummary{
			TransitGatewayAttachmentID: att.TransitGatewayAttachmentID,
			TransitGatewayID:           att.RequesterTransitGatewayID,
			ResourceID:                 att.AccepterTransitGatewayID,
			ResourceType:               "peering",
			State:                      att.State,
		})
	}

	for _, att := range b.tgwConnects.All() {
		if len(filter) > 0 && !filter[att.TransitGatewayAttachmentID] {
			continue
		}

		out = append(out, &TransitGatewayAttachmentSummary{
			TransitGatewayAttachmentID: att.TransitGatewayAttachmentID,
			TransitGatewayID:           att.TransitGatewayID,
			ResourceID:                 att.TransportTransitGatewayAttachmentID,
			ResourceType:               "connect",
			State:                      att.State,
		})
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].TransitGatewayAttachmentID < out[j].TransitGatewayAttachmentID
	})

	return out
}

// ---- Capacity Reservation extras ----

// CreateInterruptibleCapacityReservationAllocation carves out an
// interruptible allocation from a source Capacity Reservation's spare
// capacity, reducing the source's AvailableInstanceCount.
func (b *InMemoryBackend) CreateInterruptibleCapacityReservationAllocation(
	sourceCapacityReservationID string, instanceCount int32,
) (*InterruptibleCapacityReservationAllocation, error) {
	if sourceCapacityReservationID == "" {
		return nil, fmt.Errorf("%w: CapacityReservationId is required", ErrInvalidParameter)
	}

	if instanceCount <= 0 {
		return nil, fmt.Errorf("%w: InstanceCount must be positive", ErrInvalidParameter)
	}

	b.mu.Lock("CreateInterruptibleCapacityReservationAllocation")
	defer b.mu.Unlock()

	cr, ok := b.capacityReservations.Get(sourceCapacityReservationID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrCapacityReservationNotFound, sourceCapacityReservationID)
	}

	if cr.AvailableInstanceCount < int(instanceCount) {
		return nil, fmt.Errorf(
			"%w: only %d instances available in %s", ErrCapacityReservationFull,
			cr.AvailableInstanceCount, sourceCapacityReservationID,
		)
	}

	cr.AvailableInstanceCount -= int(instanceCount)

	alloc := &InterruptibleCapacityReservationAllocation{
		SourceCapacityReservationID: sourceCapacityReservationID,
		Status:                      interruptibleAllocStatusActive,
		TargetInstanceCount:         instanceCount,
	}
	b.interruptibleCRAllocations.Put(alloc)

	cp := *alloc

	return &cp, nil
}

// UpdateInterruptibleCapacityReservationAllocation resizes a previously
// created interruptible allocation, adjusting the source reservation's
// AvailableInstanceCount by the delta.
func (b *InMemoryBackend) UpdateInterruptibleCapacityReservationAllocation(
	sourceCapacityReservationID string, targetInstanceCount int32,
) (*InterruptibleCapacityReservationAllocation, error) {
	if sourceCapacityReservationID == "" {
		return nil, fmt.Errorf("%w: CapacityReservationId is required", ErrInvalidParameter)
	}

	if targetInstanceCount < 0 {
		return nil, fmt.Errorf("%w: TargetInstanceCount must not be negative", ErrInvalidParameter)
	}

	b.mu.Lock("UpdateInterruptibleCapacityReservationAllocation")
	defer b.mu.Unlock()

	cr, ok := b.capacityReservations.Get(sourceCapacityReservationID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrCapacityReservationNotFound, sourceCapacityReservationID)
	}

	alloc, ok := b.interruptibleCRAllocations.Get(sourceCapacityReservationID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrInterruptibleAllocationNotFound, sourceCapacityReservationID)
	}

	delta := targetInstanceCount - alloc.TargetInstanceCount
	if delta > 0 && cr.AvailableInstanceCount < int(delta) {
		return nil, fmt.Errorf(
			"%w: only %d instances available in %s", ErrCapacityReservationFull,
			cr.AvailableInstanceCount, sourceCapacityReservationID,
		)
	}

	cr.AvailableInstanceCount -= int(delta)
	alloc.TargetInstanceCount = targetInstanceCount
	alloc.Status = interruptibleAllocStatusActive

	cp := *alloc

	return &cp, nil
}

// GetCapacityReservationUsage derives real usage for a Capacity Reservation
// from the reservation itself, any instances currently targeting it, and any
// interruptible allocation carved out of it.
func (b *InMemoryBackend) GetCapacityReservationUsage(id string) (*CapacityReservationUsage, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: CapacityReservationId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetCapacityReservationUsage")
	defer b.mu.RUnlock()

	cr, ok := b.capacityReservations.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrCapacityReservationNotFound, id)
	}

	var usedCount int32

	for _, inst := range b.instances.All() {
		if inst.CapacityReservationSpec.CapacityReservationID == id {
			usedCount++
		}
	}

	var usages []CapacityReservationInstanceUsage
	if usedCount > 0 {
		usages = []CapacityReservationInstanceUsage{{AccountID: b.AccountID, UsedInstanceCount: usedCount}}
	}

	usage := &CapacityReservationUsage{
		CapacityReservationID:  cr.CapacityReservationID,
		InstanceType:           cr.InstanceType,
		State:                  cr.State,
		AvailableInstanceCount: cr.AvailableInstanceCount,
		TotalInstanceCount:     cr.TotalInstanceCount,
		InstanceUsages:         usages,
	}

	if alloc, hasAlloc := b.interruptibleCRAllocations.Get(id); hasAlloc {
		usage.Interruptible = true
		allocCopy := *alloc
		usage.InterruptibleAllocation = &allocCopy
	}

	return usage, nil
}

// DescribeCapacityReservationTopology returns per-reservation topology rows
// derived from the existing Capacity Reservation state.
func (b *InMemoryBackend) DescribeCapacityReservationTopology(ids []string) []*CapacityReservationTopologyEntry {
	b.mu.RLock("DescribeCapacityReservationTopology")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(ids))
	for _, id := range ids {
		filter[id] = true
	}

	out := make([]*CapacityReservationTopologyEntry, 0, b.capacityReservations.Len())

	for _, cr := range b.capacityReservations.All() {
		if len(filter) > 0 && !filter[cr.CapacityReservationID] {
			continue
		}

		out = append(out, &CapacityReservationTopologyEntry{
			CapacityReservationID: cr.CapacityReservationID,
			InstanceType:          cr.InstanceType,
			AvailabilityZone:      cr.AvailabilityZone,
		})
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].CapacityReservationID < out[j].CapacityReservationID
	})

	return out
}

// ---- Address / VPC endpoint / NAT gateway misc ----

// findAddressByPublicIPLocked scans the addresses map for an EIP by public IP
// address. Must be called with b.mu held.
func (b *InMemoryBackend) findAddressByPublicIPLocked(publicIP string) *Address {
	for _, addr := range b.addresses.All() {
		if addr.PublicIP == publicIP {
			return addr
		}
	}

	return nil
}

// MoveAddressToVpc marks an existing Elastic IP as moving into the VPC
// platform, overlaying EC2-Classic move status on top of the existing
// address state.
func (b *InMemoryBackend) MoveAddressToVpc(publicIP string) (*Address, error) {
	if publicIP == "" {
		return nil, fmt.Errorf("%w: PublicIp is required", ErrInvalidParameter)
	}

	b.mu.Lock("MoveAddressToVpc")
	defer b.mu.Unlock()

	addr := b.findAddressByPublicIPLocked(publicIP)
	if addr == nil {
		return nil, fmt.Errorf("%w: %s", ErrPublicIPNotFound, publicIP)
	}
	b.movingAddresses.Put(&MovingAddressStatus{
		PublicIP:   publicIP,
		MoveStatus: moveStatusMovingToVpc,
	})

	cp := *addr

	return &cp, nil
}

// DescribeMovingAddresses returns the moving-address status entries created
// by MoveAddressToVpc, optionally filtered by public IP.
func (b *InMemoryBackend) DescribeMovingAddresses(publicIPs []string) []*MovingAddressStatus {
	b.mu.RLock("DescribeMovingAddresses")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(publicIPs))
	for _, ip := range publicIPs {
		filter[ip] = true
	}

	out := make([]*MovingAddressStatus, 0, b.movingAddresses.Len())

	for _, st := range b.movingAddresses.All() {
		if len(filter) > 0 && !filter[st.PublicIP] {
			continue
		}

		cp := *st
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].PublicIP < out[j].PublicIP })

	return out
}

// RejectVpcEndpointConnections rejects existing pending VPC endpoint
// connections for a service. Endpoint IDs with no matching connection are
// reported back as unsuccessful, matching AcceptVpcEndpointConnections'
// sibling behaviour of never fabricating a connection that was never
// requested.
func (b *InMemoryBackend) RejectVpcEndpointConnections(serviceID string, vpcEndpointIDs []string) ([]string, error) {
	if serviceID == "" {
		return nil, fmt.Errorf("%w: ServiceId is required", ErrInvalidParameter)
	}

	if len(vpcEndpointIDs) == 0 {
		return nil, fmt.Errorf("%w: at least one VpcEndpointId is required", ErrInvalidParameter)
	}

	b.mu.Lock("RejectVpcEndpointConnections")
	defer b.mu.Unlock()

	unsuccessful := make([]string, 0, len(vpcEndpointIDs))

	for _, epID := range vpcEndpointIDs {
		key := serviceID + ":" + epID

		conn, ok := b.vpcEndpointConnections.Get(key)
		if !ok {
			unsuccessful = append(unsuccessful, epID)

			continue
		}

		conn.State = vpcEndpointConnectionStateRejected
	}

	return unsuccessful, nil
}

// UnassignPrivateNatGatewayAddress removes previously assigned secondary
// private IPs from a NAT gateway, mutating the existing NAT gateway's address
// state.
func (b *InMemoryBackend) UnassignPrivateNatGatewayAddress(
	natGatewayID string, privateIPs []string,
) (*NatGateway, error) {
	if natGatewayID == "" {
		return nil, fmt.Errorf("%w: NatGatewayId is required", ErrInvalidParameter)
	}

	if len(privateIPs) == 0 {
		return nil, fmt.Errorf("%w: PrivateIpAddress is required", ErrInvalidParameter)
	}

	b.mu.Lock("UnassignPrivateNatGatewayAddress")
	defer b.mu.Unlock()

	ngw, ok := b.natGateways.Get(natGatewayID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNatGatewayNotFound, natGatewayID)
	}

	remove := make(map[string]bool, len(privateIPs))
	for _, ip := range privateIPs {
		remove[ip] = true
	}

	kept := ngw.SecondaryPrivateIPs[:0:0]

	for _, ip := range ngw.SecondaryPrivateIPs {
		if !remove[ip] {
			kept = append(kept, ip)
		}
	}

	ngw.SecondaryPrivateIPs = kept

	cp := *ngw

	return &cp, nil
}

// ---- Image extras ----

// lookupImageLocked returns the AMI with the given ID from either the
// dynamic images map or the seeded stubAMIs list, or nil if not found. Must
// be called with b.mu held (for reading or writing). Shared by CopyImage,
// GetImageAncestry, and DescribeImageReferences so all three agree on what
// counts as a known image.
func (b *InMemoryBackend) lookupImageLocked(imageID string) *AMIStub {
	if existing, ok := b.images.Get(imageID); ok {
		return existing
	}

	for _, a := range stubAMIs {
		if a.ImageID == imageID {
			cp := a

			return &cp
		}
	}

	return nil
}

// CancelImageLaunchPermission removes the launch permission entry for an AMI,
// reusing the same imageAttributes store ModifyImageAttribute/ResetImageAttribute
// maintain for the "launchPermission" attribute.
func (b *InMemoryBackend) CancelImageLaunchPermission(imageID string) error {
	if imageID == "" {
		return fmt.Errorf("%w: ImageId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CancelImageLaunchPermission")
	defer b.mu.Unlock()

	if m, ok := b.imageAttributes[imageID]; ok {
		delete(m, "launchPermission")
	}

	return nil
}

// DescribeImageReferences scans existing instance and launch template state
// for resources referencing the given AMIs.
func (b *InMemoryBackend) DescribeImageReferences(imageIDs []string) []*ImageReferenceEntry {
	b.mu.RLock("DescribeImageReferences")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(imageIDs))
	for _, id := range imageIDs {
		filter[id] = true
	}

	out := make([]*ImageReferenceEntry, 0)

	for _, inst := range b.instances.All() {
		if inst.ImageID == "" || (len(filter) > 0 && !filter[inst.ImageID]) {
			continue
		}

		out = append(out, &ImageReferenceEntry{
			ImageID:      inst.ImageID,
			ResourceType: imageRefTypeInstance,
			Arn:          arn.Build("ec2", b.Region, b.AccountID, "instance/"+inst.ID),
		})
	}

	for _, lt := range b.launchTemplates.All() {
		if lt.ImageID == "" || (len(filter) > 0 && !filter[lt.ImageID]) {
			continue
		}

		out = append(out, &ImageReferenceEntry{
			ImageID:      lt.ImageID,
			ResourceType: imageRefTypeLaunchTemplate,
			Arn:          arn.Build("ec2", b.Region, b.AccountID, "launch-template/"+lt.ID),
		})
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].ImageID != out[j].ImageID {
			return out[i].ImageID < out[j].ImageID
		}

		return out[i].Arn < out[j].Arn
	})

	return out
}

// GetImageAncestry walks the SourceImageID chain (populated by CopyImage)
// from the given AMI up to its root.
func (b *InMemoryBackend) GetImageAncestry(imageID string) ([]*ImageAncestryEntry, error) {
	if imageID == "" {
		return nil, fmt.Errorf("%w: ImageId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetImageAncestry")
	defer b.mu.RUnlock()

	img := b.lookupImageLocked(imageID)
	if img == nil {
		return nil, fmt.Errorf("%w: %s", ErrImageNotFound, imageID)
	}

	out := make([]*ImageAncestryEntry, 0, 1)
	seen := make(map[string]bool, 1)
	cur := img

	for depth := 0; cur != nil && depth < maxImageAncestryDepth; depth++ {
		if seen[cur.ImageID] {
			break
		}

		seen[cur.ImageID] = true

		out = append(out, &ImageAncestryEntry{
			ImageID:       cur.ImageID,
			SourceImageID: cur.SourceImageID,
		})

		if cur.SourceImageID == "" {
			break
		}

		cur = b.lookupImageLocked(cur.SourceImageID)
	}

	return out, nil
}

// GetFlowLogsIntegrationTemplate generates a CloudFormation template string
// wiring an existing flow log to Athena, referencing the flow log's real ID
// and the requested S3 destination ARN.
func (b *InMemoryBackend) GetFlowLogsIntegrationTemplate(flowLogID, s3DestinationArn string) (string, error) {
	if flowLogID == "" {
		return "", fmt.Errorf("%w: FlowLogId is required", ErrInvalidParameter)
	}

	if s3DestinationArn == "" {
		return "", fmt.Errorf("%w: ConfigDeliveryS3DestinationArn is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetFlowLogsIntegrationTemplate")
	defer b.mu.RUnlock()

	fl, ok := b.flowLogs.Get(flowLogID)
	if !ok {
		return "", fmt.Errorf("%w: %s", ErrFlowLogNotFound, flowLogID)
	}

	tmpl := fmt.Sprintf(`{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Description": "CloudFormation template to integrate VPC Flow Log %s with Athena",
  "Resources": {
    "FlowLogAthenaWorkGroup": {
      "Type": "AWS::Athena::WorkGroup",
      "Properties": {
        "Name": "flow-log-%s-athena",
        "WorkGroupConfiguration": {
          "ResultConfiguration": {
            "OutputLocation": %q
          }
        }
      }
    }
  }
}`, fl.FlowLogID, fl.FlowLogID, s3DestinationArn)

	return tmpl, nil
}

// ---- Misc singletons ----

// spotPlacementScoreFor derives a deterministic score (1-10) for a seed
// string so repeated queries against the same inputs are stable, since this
// mock has no real capacity telemetry to sample from.
func spotPlacementScoreFor(seed string) int32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(seed))

	return int32(h.Sum32()%spotPlacementScoreRange) + spotPlacementScoreFloor
}

// GetSpotPlacementScores returns deterministic Spot placement scores for the
// requested instance types across the requested (or default) regions/AZs.
func (b *InMemoryBackend) GetSpotPlacementScores(
	instanceTypes, regionNames []string, singleAZ bool,
) ([]*SpotPlacementScoreEntry, error) {
	if len(instanceTypes) == 0 {
		return nil, fmt.Errorf("%w: InstanceType is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetSpotPlacementScores")
	defer b.mu.RUnlock()

	regions := regionNames
	if len(regions) == 0 {
		regions = stubRegions
	}

	seedSuffix := strings.Join(instanceTypes, ",")
	out := make([]*SpotPlacementScoreEntry, 0, len(regions))

	for _, region := range regions {
		if !singleAZ {
			out = append(out, &SpotPlacementScoreEntry{
				Region: region,
				Score:  spotPlacementScoreFor(region + seedSuffix),
			})

			continue
		}

		for _, az := range b.DescribeAvailabilityZones(region) {
			out = append(out, &SpotPlacementScoreEntry{
				AvailabilityZoneID: az,
				Score:              spotPlacementScoreFor(az + seedSuffix),
			})
		}
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Score != out[j].Score {
			return out[i].Score > out[j].Score
		}

		return out[i].Region+out[i].AvailabilityZoneID < out[j].Region+out[j].AvailabilityZoneID
	})

	if len(out) > spotPlacementScoreTopN {
		out = out[:spotPlacementScoreTopN]
	}

	return out, nil
}

// SendDiagnosticInterrupt validates that the instance exists. Real AWS sends
// a diagnostic NMI/QMP interrupt to the underlying hypervisor guest, which
// has no observable effect this backend can emulate beyond the existence
// check every real call also performs.
func (b *InMemoryBackend) SendDiagnosticInterrupt(instanceID string) error {
	if instanceID == "" {
		return fmt.Errorf("%w: InstanceId is required", ErrInvalidParameter)
	}

	b.mu.RLock("SendDiagnosticInterrupt")
	defer b.mu.RUnlock()

	if _, ok := b.instances.Get(instanceID); !ok {
		return fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
	}

	return nil
}

// EnableReachabilityAnalyzerOrganizationSharing sets the per-account flag
// enabling Reachability Analyzer resource sharing across an organization.
func (b *InMemoryBackend) EnableReachabilityAnalyzerOrganizationSharing() bool {
	b.mu.Lock("EnableReachabilityAnalyzerOrganizationSharing")
	defer b.mu.Unlock()

	b.reachabilityAnalyzerOrgSharing = true

	return true
}

// DescribeElasticGpus returns Elastic Graphics accelerators. No API in this
// backend attaches Elastic Graphics accelerators to instances (AWS retired
// the feature in 2023), so this always returns an empty, correctly shaped
// list — the same "no API populates this" precedent already established for
// ClientVpnConnection elsewhere in this package.
func (b *InMemoryBackend) DescribeElasticGpus(_ []string) []*ElasticGpuStub {
	return []*ElasticGpuStub{}
}
