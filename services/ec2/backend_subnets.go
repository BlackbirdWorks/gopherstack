package ec2

import (
	"fmt"
	"sort"

	"github.com/google/uuid"
)

// CreateDefaultSubnet creates a new default subnet in the given availability zone.
func (b *InMemoryBackend) CreateDefaultSubnet(az string) (*Subnet, error) {
	if az == "" {
		az = b.Region + "a"
	}

	b.mu.Lock("CreateDefaultSubnet")
	defer b.mu.Unlock()

	var defaultVPCID string
	for _, v := range b.vpcs.All() {
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
	b.subnets.Put(subnet)

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

	if _, ok := b.subnets.Get(subnetID); !ok {
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
	SGID  string `json:"sgid,omitempty"`
	VPCID string `json:"vpcid,omitempty"`
	State string `json:"state,omitempty"`
}

// CreateSubnetCidrReservation creates a CIDR reservation within a subnet.
func (b *InMemoryBackend) CreateSubnetCidrReservation(
	subnetID, cidr, reservationType, description string,
) (*SubnetCIDRReservation, error) {
	if subnetID == "" || cidr == "" {
		return nil, fmt.Errorf("%w: SubnetId and Cidr are required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateSubnetCidrReservation")
	defer b.mu.Unlock()

	if _, ok := b.subnets.Get(subnetID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrSubnetNotFound, subnetID)
	}

	reservation := &SubnetCIDRReservation{
		SubnetCIDRReservationID: "scr-" + uuid.New().String()[:8],
		SubnetID:                subnetID,
		CIDR:                    cidr,
		ReservationType:         reservationType,
		Description:             description,
		OwnerID:                 b.AccountID,
		State:                   "assigned",
	}
	b.subnetCIDRReservations[subnetID] = append(b.subnetCIDRReservations[subnetID], reservation)

	return reservation, nil
}

// DeleteSubnetCidrReservation removes a subnet CIDR reservation.
func (b *InMemoryBackend) DeleteSubnetCidrReservation(reservationID string) error {
	if reservationID == "" {
		return fmt.Errorf("%w: SubnetCidrReservationId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteSubnetCidrReservation")
	defer b.mu.Unlock()

	for subnetID, reservations := range b.subnetCIDRReservations {
		for i, r := range reservations {
			if r.SubnetCIDRReservationID == reservationID {
				b.subnetCIDRReservations[subnetID] = append(reservations[:i], reservations[i+1:]...)

				return nil
			}
		}
	}

	return fmt.Errorf("%w: %s", ErrInvalidParameter, reservationID)
}

// GetSubnetCidrReservations returns CIDR reservations for a subnet.
func (b *InMemoryBackend) GetSubnetCidrReservations(
	subnetID string,
) ([]*SubnetCIDRReservation, error) {
	if subnetID == "" {
		return nil, fmt.Errorf("%w: SubnetId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetSubnetCidrReservations")
	defer b.mu.RUnlock()

	reservations := b.subnetCIDRReservations[subnetID]
	out := make([]*SubnetCIDRReservation, len(reservations))
	for i, r := range reservations {
		cp := *r
		out[i] = &cp
	}

	return out, nil
}

// ---- GetSecurityGroupsForVpc ----

// SecurityGroupForVpcItem is a SG returned by GetSecurityGroupsForVpc.
type SecurityGroupForVpcItem struct {
	GroupID     string `json:"groupID,omitempty"`
	GroupName   string `json:"groupName,omitempty"`
	Description string `json:"description,omitempty"`
	VPCID       string `json:"vpcid,omitempty"`
}

// ModifySubnetAttribute enables or disables auto-assign public IP for a subnet.
func (b *InMemoryBackend) ModifySubnetAttribute(subnetID, attribute string, value bool) error {
	if subnetID == "" {
		return fmt.Errorf("%w: SubnetId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifySubnetAttribute")
	defer b.mu.Unlock()

	subnet, ok := b.subnets.Get(subnetID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrSubnetNotFound, subnetID)
	}

	switch attribute {
	case attrMapPublicIPOnLaunch:
		subnet.MapPublicIPOnLaunch = value

		return nil
	case attrEnableResourceNameDNSARec:
		return nil
	default:
		return fmt.Errorf("%w: unknown subnet attribute %q", ErrInvalidParameter, attribute)
	}
}

// ---- Network ACL CRUD ----

// DescribeSubnetsByVPC returns subnets filtered by VPC ID using the secondary index.
func (b *InMemoryBackend) DescribeSubnetsByVPC(vpcID string) []*Subnet {
	b.mu.RLock("DescribeSubnetsByVPC")
	defer b.mu.RUnlock()

	var out []*Subnet

	for _, s := range b.subnets.All() {
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
