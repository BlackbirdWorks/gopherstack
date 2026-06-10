package ec2

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

// ---- errors ----

var (
	// ErrEgressOnlyIGWNotFound is returned when an egress-only internet gateway is not found.
	ErrEgressOnlyIGWNotFound = errors.New("InvalidEgressOnlyInternetGatewayID.NotFound")
	// ErrIAMAssociationNotFound is returned when an IAM instance profile association is not found.
	ErrIAMAssociationNotFound = errors.New("InvalidAssociationID.NotFound")
	// ErrTGWRouteTableNotFound is returned when a transit gateway route table is not found.
	ErrTGWRouteTableNotFound = errors.New("InvalidTransitGatewayRouteTableId.NotFound")
)

// ---- constants ----

const (
	tgwRouteTypeStatic   = "static"
	tgwRouteStateDeleted = "deleted"
)

// ---- models ----

// EgressOnlyInternetGateway represents an EC2 egress-only internet gateway.
type EgressOnlyInternetGateway struct {
	CreateTime time.Time `json:"createTime"`
	ID         string    `json:"id,omitempty"`
	VPCID      string    `json:"vpcID,omitempty"`
	State      string    `json:"state,omitempty"`
}

// IamInstanceProfileAssociation represents an IAM instance profile association.
type IamInstanceProfileAssociation struct {
	Timestamp          time.Time `json:"timestamp"`
	AssociationID      string    `json:"associationID,omitempty"`
	InstanceID         string    `json:"instanceID,omitempty"`
	IamInstanceProfile string    `json:"iamInstanceProfile,omitempty"` // ARN or name
	State              string    `json:"state,omitempty"`
}

// TransitGatewayRouteTable represents a TGW route table.
type TransitGatewayRouteTable struct {
	CreateTime         time.Time `json:"createTime"`
	TransitGatewayID   string    `json:"transitGatewayID,omitempty"`
	RouteTableID       string    `json:"routeTableID,omitempty"`
	State              string    `json:"state,omitempty"`
	DefaultAssociation bool      `json:"defaultAssociation,omitempty"`
	DefaultPropagation bool      `json:"defaultPropagation,omitempty"`
}

// TransitGatewayRoute represents a route in a TGW route table.
type TransitGatewayRoute struct {
	DestinationCidrBlock       string `json:"destinationCidrBlock,omitempty"`
	TransitGatewayAttachmentID string `json:"transitGatewayAttachmentID,omitempty"`
	TransitGatewayRouteTableID string `json:"transitGatewayRouteTableID,omitempty"`
	State                      string `json:"state,omitempty"`
	Type                       string `json:"type,omitempty"`
}

// TransitGatewayRouteTableAssociation represents an association between a TGW route table and attachment.
type TransitGatewayRouteTableAssociation struct {
	TransitGatewayRouteTableID string `json:"transitGatewayRouteTableID,omitempty"`
	TransitGatewayAttachmentID string `json:"transitGatewayAttachmentID,omitempty"`
	ResourceID                 string `json:"resourceID,omitempty"`
	ResourceType               string `json:"resourceType,omitempty"`
	State                      string `json:"state,omitempty"`
}

// VpcCidrBlockAssociation represents a secondary CIDR block associated with a VPC.
type VpcCidrBlockAssociation struct {
	AssociationID string `json:"associationID,omitempty"`
	CidrBlock     string `json:"cidrBlock,omitempty"`
	State         string `json:"state,omitempty"`
}

// ---- EgressOnly Internet Gateway ----

// CreateEgressOnlyInternetGateway creates a new egress-only internet gateway.
func (b *InMemoryBackend) CreateEgressOnlyInternetGateway(
	vpcID string,
) (*EgressOnlyInternetGateway, error) {
	if vpcID == "" {
		return nil, fmt.Errorf("%w: VpcId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateEgressOnlyInternetGateway")
	defer b.mu.Unlock()

	if _, ok := b.vpcs[vpcID]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrVPCNotFound, vpcID)
	}

	igw := &EgressOnlyInternetGateway{
		ID:         "eigw-" + uuid.New().String()[:17],
		VPCID:      vpcID,
		State:      stateAvailable,
		CreateTime: time.Now(),
	}
	b.egressOnlyIGWs[igw.ID] = igw

	cp := *igw

	return &cp, nil
}

// DescribeEgressOnlyInternetGateways returns egress-only internet gateways, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeEgressOnlyInternetGateways(
	ids []string,
) []*EgressOnlyInternetGateway {
	b.mu.RLock("DescribeEgressOnlyInternetGateways")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*EgressOnlyInternetGateway, 0, len(b.egressOnlyIGWs))

	for _, igw := range b.egressOnlyIGWs {
		if len(idSet) > 0 && !idSet[igw.ID] {
			continue
		}

		cp := *igw
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].ID < out[j].ID
	})

	return out
}

// DeleteEgressOnlyInternetGateway removes an egress-only internet gateway.
func (b *InMemoryBackend) DeleteEgressOnlyInternetGateway(id string) error {
	if id == "" {
		return fmt.Errorf("%w: EgressOnlyInternetGatewayId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteEgressOnlyInternetGateway")
	defer b.mu.Unlock()

	if _, ok := b.egressOnlyIGWs[id]; !ok {
		return fmt.Errorf("%w: %s", ErrEgressOnlyIGWNotFound, id)
	}

	delete(b.egressOnlyIGWs, id)

	return nil
}

// ---- IAM Instance Profile Associations ----

// AssociateIamInstanceProfile associates an IAM instance profile with an instance.
func (b *InMemoryBackend) AssociateIamInstanceProfile(
	instanceID, profileARN string,
) (*IamInstanceProfileAssociation, error) {
	if instanceID == "" {
		return nil, fmt.Errorf("%w: InstanceId is required", ErrInvalidParameter)
	}

	b.mu.Lock("AssociateIamInstanceProfile")
	defer b.mu.Unlock()

	if _, ok := b.instances[instanceID]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
	}

	assoc := &IamInstanceProfileAssociation{
		AssociationID:      "iip-assoc-" + uuid.New().String()[:17],
		InstanceID:         instanceID,
		IamInstanceProfile: profileARN,
		State:              stateAvailable,
		Timestamp:          time.Now(),
	}
	b.iamAssociations[assoc.AssociationID] = assoc

	cp := *assoc

	return &cp, nil
}

// DisassociateIamInstanceProfile removes an IAM instance profile association.
func (b *InMemoryBackend) DisassociateIamInstanceProfile(
	associationID string,
) (*IamInstanceProfileAssociation, error) {
	if associationID == "" {
		return nil, fmt.Errorf("%w: AssociationId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DisassociateIamInstanceProfile")
	defer b.mu.Unlock()

	assoc, ok := b.iamAssociations[associationID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrIAMAssociationNotFound, associationID)
	}

	delete(b.iamAssociations, associationID)

	cp := *assoc

	return &cp, nil
}

// DescribeIamInstanceProfileAssociations returns IAM instance profile associations,
// optionally filtered by IDs or instance ID.
func (b *InMemoryBackend) DescribeIamInstanceProfileAssociations(
	associationIDs []string,
	instanceID string,
) []*IamInstanceProfileAssociation {
	b.mu.RLock("DescribeIamInstanceProfileAssociations")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(associationIDs))
	for _, id := range associationIDs {
		idSet[id] = true
	}

	out := make([]*IamInstanceProfileAssociation, 0, len(b.iamAssociations))

	for _, assoc := range b.iamAssociations {
		if len(idSet) > 0 && !idSet[assoc.AssociationID] {
			continue
		}

		if instanceID != "" && assoc.InstanceID != instanceID {
			continue
		}

		cp := *assoc
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].AssociationID < out[j].AssociationID
	})

	return out
}

// ReplaceIamInstanceProfileAssociation replaces an IAM instance profile on an existing association.
func (b *InMemoryBackend) ReplaceIamInstanceProfileAssociation(
	associationID, profileARN string,
) (*IamInstanceProfileAssociation, error) {
	if associationID == "" {
		return nil, fmt.Errorf("%w: AssociationId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ReplaceIamInstanceProfileAssociation")
	defer b.mu.Unlock()

	assoc, ok := b.iamAssociations[associationID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrIAMAssociationNotFound, associationID)
	}

	assoc.IamInstanceProfile = profileARN
	assoc.Timestamp = time.Now()

	cp := *assoc

	return &cp, nil
}

// ---- ReplaceRouteTableAssociation ----

// ReplaceRouteTableAssociation replaces an existing route table association with a new route table.
// Returns the new association ID.
func (b *InMemoryBackend) ReplaceRouteTableAssociation(
	associationID, newRouteTableID string,
) (string, error) {
	if associationID == "" {
		return "", fmt.Errorf("%w: AssociationId is required", ErrInvalidParameter)
	}

	if newRouteTableID == "" {
		return "", fmt.Errorf("%w: RouteTableId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ReplaceRouteTableAssociation")
	defer b.mu.Unlock()

	newRT, ok := b.routeTables[newRouteTableID]
	if !ok {
		return "", fmt.Errorf("%w: %s", ErrRouteTableNotFound, newRouteTableID)
	}

	// Find the old association.
	var subnetID string

	for _, rt := range b.routeTables {
		for i, assoc := range rt.Associations {
			if assoc.ID == associationID {
				subnetID = assoc.SubnetID
				rt.Associations = append(rt.Associations[:i], rt.Associations[i+1:]...)

				break
			}
		}

		if subnetID != "" {
			break
		}
	}

	if subnetID == "" {
		return "", fmt.Errorf("%w: %s", ErrAssociationNotFound, associationID)
	}

	newAssocID := "rtbassoc-" + uuid.New().String()[:17]
	newRT.Associations = append(newRT.Associations, RouteAssociation{
		ID:           newAssocID,
		RouteTableID: newRouteTableID,
		SubnetID:     subnetID,
	})

	return newAssocID, nil
}

// ---- AssociateVpcCidrBlock ----

// AssociateVpcCidrBlock associates a secondary CIDR block with a VPC.
func (b *InMemoryBackend) AssociateVpcCidrBlock(
	vpcID, cidrBlock string,
) (*VpcCidrBlockAssociation, error) {
	if vpcID == "" {
		return nil, fmt.Errorf("%w: VpcId is required", ErrInvalidParameter)
	}

	b.mu.Lock("AssociateVpcCidrBlock")
	defer b.mu.Unlock()

	if _, ok := b.vpcs[vpcID]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrVPCNotFound, vpcID)
	}

	assoc := &VpcCidrBlockAssociation{
		AssociationID: "vpc-cidr-assoc-" + uuid.New().String()[:17],
		CidrBlock:     cidrBlock,
		State:         stateAvailable,
	}
	b.vpcCidrAssociations[vpcID+":"+assoc.AssociationID] = assoc

	cp := *assoc

	return &cp, nil
}

// ---- Transit Gateway Route Tables ----

// CreateTransitGatewayRouteTable creates a new TGW route table.
func (b *InMemoryBackend) CreateTransitGatewayRouteTable(
	tgwID string,
) (*TransitGatewayRouteTable, error) {
	if tgwID == "" {
		return nil, fmt.Errorf("%w: TransitGatewayId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateTransitGatewayRouteTable")
	defer b.mu.Unlock()

	if _, ok := b.transitGateways[tgwID]; !ok {
		return nil, fmt.Errorf("%w: transit gateway %s not found", ErrInvalidParameter, tgwID)
	}

	rt := &TransitGatewayRouteTable{
		RouteTableID:     "tgw-rtb-" + uuid.New().String()[:17],
		TransitGatewayID: tgwID,
		State:            stateAvailable,
		CreateTime:       time.Now(),
	}
	b.tgwRouteTables[rt.RouteTableID] = rt

	cp := *rt

	return &cp, nil
}

// DescribeTransitGatewayRouteTables returns TGW route tables, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeTransitGatewayRouteTables(
	ids []string,
) []*TransitGatewayRouteTable {
	b.mu.RLock("DescribeTransitGatewayRouteTables")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*TransitGatewayRouteTable, 0, len(b.tgwRouteTables))

	for _, rt := range b.tgwRouteTables {
		if len(idSet) > 0 && !idSet[rt.RouteTableID] {
			continue
		}

		cp := *rt
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].RouteTableID < out[j].RouteTableID
	})

	return out
}

// DeleteTransitGatewayRouteTable removes a TGW route table.
func (b *InMemoryBackend) DeleteTransitGatewayRouteTable(id string) error {
	if id == "" {
		return fmt.Errorf("%w: TransitGatewayRouteTableId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteTransitGatewayRouteTable")
	defer b.mu.Unlock()

	if _, ok := b.tgwRouteTables[id]; !ok {
		return fmt.Errorf("%w: %s", ErrTGWRouteTableNotFound, id)
	}

	delete(b.tgwRouteTables, id)

	return nil
}

// ---- Transit Gateway Routes ----

// CreateTransitGatewayRoute adds a static route to a TGW route table.
func (b *InMemoryBackend) CreateTransitGatewayRoute(
	routeTableID, destinationCIDR, attachmentID string,
) (*TransitGatewayRoute, error) {
	if routeTableID == "" {
		return nil, fmt.Errorf("%w: TransitGatewayRouteTableId is required", ErrInvalidParameter)
	}

	if destinationCIDR == "" {
		return nil, fmt.Errorf("%w: DestinationCidrBlock is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateTransitGatewayRoute")
	defer b.mu.Unlock()

	if _, ok := b.tgwRouteTables[routeTableID]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrTGWRouteTableNotFound, routeTableID)
	}

	route := &TransitGatewayRoute{
		DestinationCidrBlock:       destinationCIDR,
		TransitGatewayAttachmentID: attachmentID,
		TransitGatewayRouteTableID: routeTableID,
		State:                      stateActive,
		Type:                       tgwRouteTypeStatic,
	}
	key := routeTableID + ":" + destinationCIDR
	b.tgwRoutes[key] = route

	cp := *route

	return &cp, nil
}

// DeleteTransitGatewayRoute removes a static route from a TGW route table.
func (b *InMemoryBackend) DeleteTransitGatewayRoute(routeTableID, destinationCIDR string) error {
	if routeTableID == "" {
		return fmt.Errorf("%w: TransitGatewayRouteTableId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteTransitGatewayRoute")
	defer b.mu.Unlock()

	key := routeTableID + ":" + destinationCIDR
	if _, ok := b.tgwRoutes[key]; !ok {
		return fmt.Errorf(
			"%w: route %s in %s not found",
			ErrInvalidParameter,
			destinationCIDR,
			routeTableID,
		)
	}

	delete(b.tgwRoutes, key)

	return nil
}

// ReplaceTransitGatewayRoute replaces (or upserts) a route in a TGW route table.
func (b *InMemoryBackend) ReplaceTransitGatewayRoute(
	routeTableID, destinationCIDR, attachmentID string,
) (*TransitGatewayRoute, error) {
	if routeTableID == "" {
		return nil, fmt.Errorf("%w: TransitGatewayRouteTableId is required", ErrInvalidParameter)
	}

	if destinationCIDR == "" {
		return nil, fmt.Errorf("%w: DestinationCidrBlock is required", ErrInvalidParameter)
	}

	b.mu.Lock("ReplaceTransitGatewayRoute")
	defer b.mu.Unlock()

	if _, ok := b.tgwRouteTables[routeTableID]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrTGWRouteTableNotFound, routeTableID)
	}

	key := routeTableID + ":" + destinationCIDR
	route := &TransitGatewayRoute{
		DestinationCidrBlock:       destinationCIDR,
		TransitGatewayAttachmentID: attachmentID,
		TransitGatewayRouteTableID: routeTableID,
		State:                      stateActive,
		Type:                       tgwRouteTypeStatic,
	}
	b.tgwRoutes[key] = route

	cp := *route

	return &cp, nil
}

// ---- Transit Gateway Route Table Associations ----

// AssociateTransitGatewayRouteTable associates a TGW attachment with a route table.
func (b *InMemoryBackend) AssociateTransitGatewayRouteTable(
	routeTableID, attachmentID string,
) (*TransitGatewayRouteTableAssociation, error) {
	if routeTableID == "" {
		return nil, fmt.Errorf("%w: TransitGatewayRouteTableId is required", ErrInvalidParameter)
	}

	if attachmentID == "" {
		return nil, fmt.Errorf("%w: TransitGatewayAttachmentId is required", ErrInvalidParameter)
	}

	b.mu.Lock("AssociateTransitGatewayRouteTable")
	defer b.mu.Unlock()

	if _, ok := b.tgwRouteTables[routeTableID]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrTGWRouteTableNotFound, routeTableID)
	}

	assoc := &TransitGatewayRouteTableAssociation{
		TransitGatewayRouteTableID: routeTableID,
		TransitGatewayAttachmentID: attachmentID,
		ResourceType:               "vpc",
		State:                      stateAvailable,
	}
	key := routeTableID + ":" + attachmentID
	b.tgwRTAssociations[key] = assoc

	cp := *assoc

	return &cp, nil
}

// DisassociateTransitGatewayRouteTable removes an association between a TGW attachment and route table.
func (b *InMemoryBackend) DisassociateTransitGatewayRouteTable(
	routeTableID, attachmentID string,
) error {
	if routeTableID == "" {
		return fmt.Errorf("%w: TransitGatewayRouteTableId is required", ErrInvalidParameter)
	}

	if attachmentID == "" {
		return fmt.Errorf("%w: TransitGatewayAttachmentId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DisassociateTransitGatewayRouteTable")
	defer b.mu.Unlock()

	key := routeTableID + ":" + attachmentID
	if _, ok := b.tgwRTAssociations[key]; !ok {
		return fmt.Errorf(
			"%w: association between %s and %s not found",
			ErrInvalidParameter,
			routeTableID,
			attachmentID,
		)
	}

	delete(b.tgwRTAssociations, key)

	return nil
}
