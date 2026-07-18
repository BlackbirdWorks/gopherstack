package ec2

import (
	"fmt"
	"time"

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

// DescribeVpcs returns VPCs, optionally filtered by IDs.
// When ids are provided, lookups are O(len(ids)) via the VPC map rather than
// scanning every VPC in the backend.
func (b *InMemoryBackend) DescribeVpcs(ids []string) []*VPC {
	b.mu.RLock("DescribeVpcs")
	defer b.mu.RUnlock()

	if len(ids) > 0 {
		out := make([]*VPC, 0, len(ids))

		for _, id := range ids {
			v, ok := b.vpcs.Get(id)
			if !ok {
				continue
			}

			cp := *v
			out = append(out, &cp)
		}

		return out
	}

	out := make([]*VPC, 0, b.vpcs.Len())

	for _, v := range b.vpcs.All() {
		cp := *v
		out = append(out, &cp)
	}

	return out
}

// CreateVpc creates a new VPC with the given CIDR block.
func (b *InMemoryBackend) CreateVpc(cidr string) (*VPC, error) {
	if cidr == "" {
		return nil, fmt.Errorf("%w: CidrBlock is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateVpc")
	defer b.mu.Unlock()

	for _, existing := range b.vpcs.All() {
		if cidrsOverlap(cidr, existing.CIDRBlock) {
			return nil, fmt.Errorf("%w: CIDR %s overlaps with existing VPC %s (%s)",
				ErrCIDRConflict, cidr, existing.ID, existing.CIDRBlock)
		}
	}

	id := "vpc-" + uuid.New().String()[:17]
	v := &VPC{
		ID:        id,
		CIDRBlock: cidr,
	}
	b.vpcs.Put(v)

	return v, nil
}

// cascadeDeleteVpcIGWsLocked removes all internet gateways that have an
// attachment to the given VPC. Must be called with b.mu held.
func (b *InMemoryBackend) cascadeDeleteVpcIGWsLocked(vpcID string) {
	for _, igw := range b.internetGateways.All() {
		igwID := internetGatewaysKeyFn(igw)
		for _, att := range igw.Attachments {
			if att.VPCID == vpcID {
				b.internetGateways.Delete(igwID)
				delete(b.tags, igwID)

				break
			}
		}
	}
}

// DeleteVpc removes a VPC by ID, cascade-deleting all dependent resources
// (instances, internet gateways, NAT gateways, route tables, security groups,
// network interfaces, and subnets) along with their tags.
// Uses secondary indexes for instances, subnets, route tables, and security groups
// to avoid O(n_all) scans for each resource type.
func (b *InMemoryBackend) DeleteVpc(id string) error {
	b.mu.Lock("DeleteVpc")
	defer b.mu.Unlock()

	if _, ok := b.vpcs.Get(id); !ok {
		return fmt.Errorf("%w: %s", ErrVPCNotFound, id)
	}

	// Cascade: terminate instances belonging to this VPC via secondary index.
	for instID := range b.instanceIDsByVPC[id] {
		if inst, ok := b.instances.Get(instID); ok {
			inst.State = StateTerminated
			inst.TerminatedAt = time.Now()
			delete(b.tags, instID)
			b.detachVolumesAndEIPsLocked(instID)
		}
	}
	delete(b.instanceIDsByVPC, id)

	// Cascade: detach and delete internet gateways attached to this VPC.
	b.cascadeDeleteVpcIGWsLocked(id)

	// Cascade: delete NAT gateways belonging to this VPC via secondary index,
	// avoiding a full-map scan under the write lock.
	for ngwID := range b.natGatewayIDsByVPC[id] {
		if ngw, ok := b.natGateways.Get(ngwID); ok {
			b.recycleIPLocked(ngw.PrivateIP)
			b.natGateways.Delete(ngwID)
			delete(b.tags, ngwID)
		}
	}
	delete(b.natGatewayIDsByVPC, id)

	// Cascade: remove route tables belonging to this VPC via secondary index.
	for rtID := range b.routeTableIDsByVPC[id] {
		b.routeTables.Delete(rtID)
		delete(b.tags, rtID)
	}
	delete(b.routeTableIDsByVPC, id)

	// Cascade: remove security groups belonging to this VPC via secondary index.
	for sgID := range b.sgIDsByVPC[id] {
		b.securityGroups.Delete(sgID)
		delete(b.tags, sgID)
	}
	delete(b.sgIDsByVPC, id)

	// Cascade: remove network interfaces belonging to this VPC via secondary
	// index, avoiding a full-map scan under the write lock.
	for eniID := range b.eniIDsByVPC[id] {
		if eni, ok := b.networkInterfaces.Get(eniID); ok {
			b.recycleENIIPsLocked(eni)
			b.deindexENILocked(eniID, eni)
			b.networkInterfaces.Delete(eniID)
			delete(b.tags, eniID)
		}
	}
	delete(b.eniIDsByVPC, id)

	// Cascade: remove subnets belonging to this VPC via secondary index.
	for subnetID := range b.subnetIDsByVPC[id] {
		b.subnets.Delete(subnetID)
		delete(b.tags, subnetID)
	}
	delete(b.subnetIDsByVPC, id)
	b.vpcs.Delete(id)
	delete(b.tags, id)

	return nil
}
