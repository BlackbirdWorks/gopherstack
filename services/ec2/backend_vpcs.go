package ec2

import (
	"fmt"

	"github.com/google/uuid"
)

// CreateDefaultVpc creates a new default VPC in the account.
// Returns error if a default VPC already exists.
func (b *InMemoryBackend) CreateDefaultVpc() (*VPC, error) {
	b.mu.Lock("CreateDefaultVpc")
	defer b.mu.Unlock()

	for _, v := range b.vpcs.All() {
		if v.IsDefault {
			return nil, fmt.Errorf("%w: a default VPC already exists", ErrInvalidParameter)
		}
	}

	vpc := &VPC{
		ID:        "vpc-" + uuid.New().String()[:17],
		CIDRBlock: defaultVPCCIDR,
		IsDefault: true,
	}
	b.vpcs.Put(vpc)

	return vpc, nil
}

// ---- CreateDefaultSubnet ----

// ModifyVpcTenancy sets the instance tenancy for a VPC ("default" or "dedicated").
func (b *InMemoryBackend) ModifyVpcTenancy(vpcID, tenancy string) error {
	if vpcID == "" {
		return fmt.Errorf("%w: VpcId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyVpcTenancy")
	defer b.mu.Unlock()

	if _, ok := b.vpcs.Get(vpcID); !ok {
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

	if _, ok := b.vpcPeeringConnections.Get(peeringID); !ok {
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

// DisassociateVpcCidrBlock removes a secondary CIDR block association from a VPC.
func (b *InMemoryBackend) DisassociateVpcCidrBlock(associationID string) error {
	if associationID == "" {
		return fmt.Errorf("%w: AssociationId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DisassociateVpcCidrBlock")
	defer b.mu.Unlock()

	// Keys are stored as "vpcID:assocID"
	for key, assoc := range b.vpcCidrAssociations {
		if assoc.AssociationID == associationID {
			delete(b.vpcCidrAssociations, key)

			return nil
		}
	}

	return fmt.Errorf("%w: %s", ErrInvalidParameter, associationID)
}

// ---- NAT Gateway address ops ----

// ModifyVpcAttribute enables or disables DNS support or DNS hostnames for a VPC.
func (b *InMemoryBackend) ModifyVpcAttribute(vpcID, attribute string, value bool) error {
	if vpcID == "" {
		return fmt.Errorf("%w: VpcId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyVpcAttribute")
	defer b.mu.Unlock()

	vpc, ok := b.vpcs.Get(vpcID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrVPCNotFound, vpcID)
	}

	switch attribute {
	case attrEnableDNSSupport, attrEnableDNSHostnames:
		if vpc.Attributes == nil {
			vpc.Attributes = make(map[string]bool)
		}
		vpc.Attributes[attribute] = value

		return nil
	default:
		return fmt.Errorf("%w: unknown VPC attribute %q", ErrInvalidParameter, attribute)
	}
}

// CreateVpcPeeringConnection creates a new pending VPC peering connection.
func (b *InMemoryBackend) CreateVpcPeeringConnection(
	requesterVPCID, accepterVPCID string,
) (*VpcPeeringConnection, error) {
	if requesterVPCID == "" || accepterVPCID == "" {
		return nil, fmt.Errorf("%w: VpcId and PeerVpcId are required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateVpcPeeringConnection")
	defer b.mu.Unlock()

	if _, ok := b.vpcs.Get(requesterVPCID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrVPCNotFound, requesterVPCID)
	}

	pc := &VpcPeeringConnection{
		VpcPeeringConnectionID: "pcx-" + uuid.New().String()[:8],
		RequesterVpcID:         requesterVPCID,
		AccepterVpcID:          accepterVPCID,
		State:                  "pending-acceptance",
	}
	b.vpcPeeringConnections.Put(pc)

	cp := *pc

	return &cp, nil
}

// DeleteVpcPeeringConnection removes a VPC peering connection.
func (b *InMemoryBackend) DeleteVpcPeeringConnection(id string) error {
	if id == "" {
		return fmt.Errorf("%w: VpcPeeringConnectionId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteVpcPeeringConnection")
	defer b.mu.Unlock()

	if _, ok := b.vpcPeeringConnections.Get(id); !ok {
		return fmt.Errorf("%w: peering connection %s not found", ErrInvalidParameter, id)
	}
	b.vpcPeeringConnections.Delete(id)

	return nil
}

// ---- Transit gateways ----

// TransitGateway is a stub for an AWS Transit Gateway resource.
type TransitGateway struct {
	ID          string `json:"transitGatewayId,omitempty"`
	Description string `json:"description,omitempty"`
	State       string `json:"state,omitempty"`
	OwnerID     string `json:"ownerId,omitempty"`
}
