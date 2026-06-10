package ec2

import (
	"errors"
	"fmt"
	"sort"

	"github.com/google/uuid"
)

// Errors introduced in refinement 3.
var (
	errR3KeyPairNotFound = errors.New("InvalidKeyPair.NotFound")
)

// ---- Network ACL: Replace entry ----

// ReplaceNetworkACLEntry replaces an existing NACL rule identified by
// (aclID, ruleNumber, egress) with new parameters.
func (b *InMemoryBackend) ReplaceNetworkACLEntry(
	aclID string, ruleNumber int, protocol, action, cidr string,
	egress bool, fromPort, toPort int,
) error {
	if aclID == "" {
		return fmt.Errorf("%w: NetworkAclId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ReplaceNetworkACLEntry")
	defer b.mu.Unlock()

	acl, ok := b.networkACLs[aclID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrNetworkACLNotFound, aclID)
	}

	for i, e := range acl.Entries {
		if e.RuleNumber == ruleNumber && e.Egress == egress {
			acl.Entries[i] = NACLEntry{
				RuleNumber: ruleNumber,
				Protocol:   protocol,
				RuleAction: action,
				CIDRBlock:  cidr,
				Egress:     egress,
				FromPort:   fromPort,
				ToPort:     toPort,
			}

			return nil
		}
	}

	return fmt.Errorf(
		"%w: rule %d (egress=%v) not found in ACL %s",
		ErrInvalidParameter, ruleNumber, egress, aclID,
	)
}

// ---- Network ACL: Association management ----

// ReplaceNetworkACLAssociation moves a subnet from its current NACL to
// the specified one and returns a new association ID.
func (b *InMemoryBackend) ReplaceNetworkACLAssociation(aclID, subnetID string) (string, error) {
	if aclID == "" {
		return "", fmt.Errorf("%w: NetworkAclId is required", ErrInvalidParameter)
	}

	if subnetID == "" {
		return "", fmt.Errorf("%w: SubnetId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ReplaceNetworkACLAssociation")
	defer b.mu.Unlock()

	if _, ok := b.networkACLs[aclID]; !ok {
		return "", fmt.Errorf("%w: %s", ErrNetworkACLNotFound, aclID)
	}

	if _, ok := b.subnets[subnetID]; !ok {
		return "", fmt.Errorf("%w: %s", ErrSubnetNotFound, subnetID)
	}

	// Remove subnetID from any existing ACL.
	for _, existing := range b.networkACLs {
		for i, assoc := range existing.AssociationIDs {
			if assoc == subnetID {
				existing.AssociationIDs = append(
					existing.AssociationIDs[:i],
					existing.AssociationIDs[i+1:]...,
				)

				break
			}
		}
	}

	target := b.networkACLs[aclID]
	target.AssociationIDs = append(target.AssociationIDs, subnetID)
	newAssocID := "aclassoc-" + uuid.New().String()[:8]

	return newAssocID, nil
}

// ---- VPC Endpoint Services ----

// DescribeVpcEndpointServices returns the set of commonly available AWS
// endpoint service names for the backend region.
func (b *InMemoryBackend) DescribeVpcEndpointServices() []string {
	return []string{
		"com.amazonaws." + b.Region + ".s3",
		"com.amazonaws." + b.Region + ".dynamodb",
		"com.amazonaws." + b.Region + ".ec2",
		"com.amazonaws." + b.Region + ".ec2messages",
		"com.amazonaws." + b.Region + ".ssm",
		"com.amazonaws." + b.Region + ".ssmmessages",
		"com.amazonaws." + b.Region + ".kms",
		"com.amazonaws." + b.Region + ".secretsmanager",
		"com.amazonaws." + b.Region + ".sts",
		"com.amazonaws." + b.Region + ".logs",
		"com.amazonaws." + b.Region + ".monitoring",
		"com.amazonaws." + b.Region + ".elasticloadbalancing",
		"com.amazonaws." + b.Region + ".lambda",
		"com.amazonaws." + b.Region + ".sqs",
		"com.amazonaws." + b.Region + ".sns",
	}
}

// ---- Key pair: export ----

// ExportKeyPair returns the public-key material for a stored key pair.
// For keys created via CreateKeyPair a deterministic SSH-authorized-keys line
// is generated from the fingerprint. AWS similarly stores only the public half.
func (b *InMemoryBackend) ExportKeyPair(name string) (string, error) {
	if name == "" {
		return "", fmt.Errorf("%w: KeyName is required", ErrInvalidParameter)
	}

	b.mu.RLock("ExportKeyPair")
	defer b.mu.RUnlock()

	kp, ok := b.keyPairs[name]
	if !ok {
		return "", fmt.Errorf("%w: %s", errR3KeyPairNotFound, name)
	}

	return "ssh-rsa AAAAB3NzaC1yc2EAAAADAQ... " + kp.Fingerprint + " exported@gopherstack", nil
}

// ---- Instance type offerings ----

// InstanceTypeOffering pairs an instance type with an AZ offering.
type InstanceTypeOffering struct {
	InstanceType     string `json:"instanceType,omitempty"`
	AvailabilityZone string `json:"availabilityZone,omitempty"`
	Location         string `json:"location,omitempty"`
	LocationType     string `json:"locationType,omitempty"`
}

// DescribeInstanceTypeOfferings returns a static list of instance type / AZ pairs.
func (b *InMemoryBackend) DescribeInstanceTypeOfferings() []InstanceTypeOffering {
	azs := b.DescribeAvailabilityZones(b.Region)
	types := []string{
		"t3.nano", instanceTypeT3Micro, "t3.small", instanceTypeT3Medium, "t3.large",
		"t3.xlarge", "t3.2xlarge",
		"m5.large", instanceTypeM5Xlarge, "m5.2xlarge", "m5.4xlarge",
		"c5.large", "c5.xlarge", "c5.2xlarge",
		"r5.large", "r5.xlarge",
		"p3.2xlarge",
		"inf1.xlarge",
	}

	out := make([]InstanceTypeOffering, 0, len(types)*len(azs))

	for _, t := range types {
		for _, az := range azs {
			out = append(out, InstanceTypeOffering{
				InstanceType:     t,
				AvailabilityZone: az,
				Location:         az,
				LocationType:     "availability-zone",
			})
		}
	}

	return out
}

// ---- VPC peering: create / delete (accept is in backend_accept_ops.go) ----

// CreateVpcPeeringConnection creates a new pending VPC peering connection.
func (b *InMemoryBackend) CreateVpcPeeringConnection(
	requesterVPCID, accepterVPCID string,
) (*VpcPeeringConnection, error) {
	if requesterVPCID == "" || accepterVPCID == "" {
		return nil, fmt.Errorf("%w: VpcId and PeerVpcId are required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateVpcPeeringConnection")
	defer b.mu.Unlock()

	if _, ok := b.vpcs[requesterVPCID]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrVPCNotFound, requesterVPCID)
	}

	pc := &VpcPeeringConnection{
		VpcPeeringConnectionID: "pcx-" + uuid.New().String()[:8],
		RequesterVpcID:         requesterVPCID,
		AccepterVpcID:          accepterVPCID,
		State:                  "pending-acceptance",
	}
	b.vpcPeeringConnections[pc.VpcPeeringConnectionID] = pc

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

	if _, ok := b.vpcPeeringConnections[id]; !ok {
		return fmt.Errorf("%w: peering connection %s not found", ErrInvalidParameter, id)
	}

	delete(b.vpcPeeringConnections, id)

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

// DescribeTransitGateways returns transit gateways, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeTransitGateways(ids []string) []*TransitGateway {
	b.mu.RLock("DescribeTransitGateways")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*TransitGateway, 0, len(b.transitGateways))

	for _, tgw := range b.transitGateways {
		if len(idSet) > 0 && !idSet[tgw.ID] {
			continue
		}

		cp := *tgw
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].ID < out[j].ID
	})

	return out
}

// CreateTransitGateway creates a new transit gateway stub.
func (b *InMemoryBackend) CreateTransitGateway(description string) (*TransitGateway, error) {
	b.mu.Lock("CreateTransitGateway")
	defer b.mu.Unlock()

	tgw := &TransitGateway{
		ID:          "tgw-" + b.AccountID[:8],
		Description: description,
		State:       stateAvailable,
		OwnerID:     b.AccountID,
	}
	b.transitGateways[tgw.ID] = tgw

	cp := *tgw

	return &cp, nil
}

// DeleteTransitGateway removes a transit gateway stub.
func (b *InMemoryBackend) DeleteTransitGateway(id string) error {
	if id == "" {
		return fmt.Errorf("%w: TransitGatewayId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteTransitGateway")
	defer b.mu.Unlock()

	if _, ok := b.transitGateways[id]; !ok {
		return fmt.Errorf("%w: transit gateway %s not found", ErrInvalidParameter, id)
	}

	delete(b.transitGateways, id)

	return nil
}
