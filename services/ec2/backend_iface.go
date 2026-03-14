package ec2

// Backend defines the interface for EC2 backend operations.
// InMemoryBackend implements this interface; alternative providers (e.g. a
// real-AWS pass-through or a test double) can do so too, making the Handler
// backend-agnostic.
type Backend interface {
	// ---- instances ----

	// RunInstances creates one or more EC2 instance stubs.
	RunInstances(imageID, instanceType, subnetID string, count int) ([]*Instance, error)

	// DescribeInstances returns instances, optionally filtered by IDs or state name.
	DescribeInstances(ids []string, state string) []*Instance

	// TerminateInstances transitions instances to shutting-down / terminated.
	// Returns previous and current state for each instance.
	TerminateInstances(ids []string) ([]*InstanceStateChange, error)

	// StartInstances transitions stopped instances to pending / running.
	// Returns ErrInvalidInstanceState if an instance is not stopped.
	StartInstances(ids []string) ([]*InstanceStateChange, error)

	// StopInstances transitions running instances to stopping / stopped.
	// Returns ErrInvalidInstanceState if an instance is not running.
	StopInstances(ids []string) ([]*InstanceStateChange, error)

	// RebootInstances keeps instances running (mock no-op transition).
	RebootInstances(ids []string) error

	// DescribeInstanceStatus returns per-instance status entries.
	DescribeInstanceStatus(ids []string) []*Instance

	// ---- images ----

	// DescribeImages returns the built-in stub AMI catalogue.
	DescribeImages() []AMIStub

	// ---- regions / AZs ----

	// DescribeRegions returns the list of supported AWS region names.
	DescribeRegions() []string

	// DescribeAvailabilityZones returns AZ names for the given region.
	DescribeAvailabilityZones(region string) []string

	// ---- security groups ----

	// DescribeSecurityGroups returns security groups, optionally filtered by IDs.
	DescribeSecurityGroups(ids []string) []*SecurityGroup

	// CreateSecurityGroup creates a security group and returns it.
	CreateSecurityGroup(name, description, vpcID string) (*SecurityGroup, error)

	// DeleteSecurityGroup removes a security group by ID.
	DeleteSecurityGroup(id string) error

	// AuthorizeSecurityGroupIngress appends ingress rules to a security group.
	AuthorizeSecurityGroupIngress(groupID string, rules []SecurityGroupRule) error

	// AuthorizeSecurityGroupEgress appends egress rules to a security group.
	AuthorizeSecurityGroupEgress(groupID string, rules []SecurityGroupRule) error

	// RevokeSecurityGroupIngress removes matching ingress rules from a security group.
	RevokeSecurityGroupIngress(groupID string, rules []SecurityGroupRule) error

	// ---- VPCs ----

	// DescribeVpcs returns VPCs, optionally filtered by IDs.
	DescribeVpcs(ids []string) []*VPC

	// CreateVpc creates a new VPC with the given CIDR block.
	CreateVpc(cidr string) (*VPC, error)

	// DeleteVpc removes a VPC by ID.
	DeleteVpc(id string) error

	// ---- subnets ----

	// DescribeSubnets returns subnets, optionally filtered by IDs.
	DescribeSubnets(ids []string) []*Subnet

	// CreateSubnet creates a new subnet in the given VPC.
	CreateSubnet(vpcID, cidr, az string) (*Subnet, error)

	// DeleteSubnet removes a subnet by ID.
	DeleteSubnet(id string) error

	// ---- key pairs ----

	// CreateKeyPair generates an RSA key pair and stores it.
	CreateKeyPair(name string) (*KeyPair, error)

	// ImportKeyPair stores a pre-existing key pair by name without key material.
	ImportKeyPair(name string) (*KeyPair, error)

	// DescribeKeyPairs returns key pairs, optionally filtered by names.
	DescribeKeyPairs(names []string) []*KeyPair

	// DeleteKeyPair removes a key pair by name.
	DeleteKeyPair(name string) error

	// ---- EBS volumes ----

	// CreateVolume creates a new EBS volume stub.
	CreateVolume(az, volType string, size int) (*Volume, error)

	// DescribeVolumes returns volumes, optionally filtered by IDs.
	DescribeVolumes(ids []string) []*Volume

	// DeleteVolume removes a volume by ID.
	DeleteVolume(id string) error

	// AttachVolume attaches a volume to an instance.
	AttachVolume(volumeID, instanceID, device string) (*VolumeAttachment, error)

	// DetachVolume detaches a volume; force flag is accepted but ignored in mock.
	DetachVolume(volumeID string, force bool) (*VolumeAttachment, error)

	// ---- elastic IPs ----

	// AllocateAddress allocates a new Elastic IP address.
	AllocateAddress() (*Address, error)

	// AssociateAddress associates an allocation with an instance; returns association ID.
	AssociateAddress(allocationID, instanceID string) (string, error)

	// DisassociateAddress removes an address association.
	DisassociateAddress(associationID string) error

	// ReleaseAddress releases an allocated Elastic IP.
	ReleaseAddress(allocationID string) error

	// DescribeAddresses returns Elastic IP allocations, optionally filtered by IDs.
	DescribeAddresses(allocationIDs []string) []*Address

	// ---- internet gateways ----

	// CreateInternetGateway creates a new internet gateway.
	CreateInternetGateway() (*InternetGateway, error)

	// DeleteInternetGateway removes an internet gateway.
	DeleteInternetGateway(id string) error

	// DescribeInternetGateways returns internet gateways, optionally filtered by IDs.
	DescribeInternetGateways(ids []string) []*InternetGateway

	// AttachInternetGateway attaches an internet gateway to a VPC.
	AttachInternetGateway(igwID, vpcID string) error

	// DetachInternetGateway detaches an internet gateway from a VPC.
	DetachInternetGateway(igwID, vpcID string) error

	// ---- route tables ----

	// CreateRouteTable creates a new route table in the given VPC.
	CreateRouteTable(vpcID string) (*RouteTable, error)

	// DeleteRouteTable removes a route table.
	DeleteRouteTable(id string) error

	// DescribeRouteTables returns route tables, optionally filtered by IDs.
	DescribeRouteTables(ids []string) []*RouteTable

	// CreateRoute adds a route to a route table.
	CreateRoute(rtID, destCIDR, gatewayID, natGatewayID string) error

	// DeleteRoute removes a route from a route table by destination CIDR.
	DeleteRoute(rtID, destCIDR string) error

	// AssociateRouteTable associates a route table with a subnet; returns association ID.
	AssociateRouteTable(rtID, subnetID string) (string, error)

	// DisassociateRouteTable removes a subnet association from a route table.
	DisassociateRouteTable(assocID string) error

	// ---- NAT gateways ----

	// CreateNatGateway creates a NAT gateway in the given subnet.
	CreateNatGateway(subnetID, allocationID string) (*NatGateway, error)

	// DeleteNatGateway removes a NAT gateway.
	DeleteNatGateway(id string) error

	// DescribeNatGateways returns NAT gateways, optionally filtered by IDs.
	DescribeNatGateways(ids []string) []*NatGateway

	// ---- network interfaces ----

	// DescribeNetworkInterfaces returns ENIs, optionally filtered by IDs.
	DescribeNetworkInterfaces(ids []string) []*NetworkInterface

	// CreateNetworkInterface creates a new ENI in the given subnet.
	CreateNetworkInterface(subnetID, description string) (*NetworkInterface, error)

	// DeleteNetworkInterface removes a network interface by ID.
	DeleteNetworkInterface(id string) error

	// AttachNetworkInterface attaches an ENI to an instance; returns the attachment ID.
	AttachNetworkInterface(eniID, instanceID string, deviceIndex int) (string, error)

	// DetachNetworkInterface detaches a network interface by attachment ID.
	DetachNetworkInterface(attachmentID string, force bool) error

	// AssignPrivateIPAddresses adds secondary private IPs to an ENI.
	// If ips is non-empty those addresses are assigned; otherwise count new IPs are allocated.
	AssignPrivateIPAddresses(eniID string, count int, ips []string) error

	// UnassignPrivateIPAddresses removes secondary private IPs from an ENI.
	UnassignPrivateIPAddresses(eniID string, ips []string) error

	// ModifyNetworkInterfaceAttribute updates a single attribute of an ENI.
	ModifyNetworkInterfaceAttribute(eniID, attr, value string) error

	// ---- spot instances ----

	// RequestSpotInstances creates a spot instance request (mock: immediately fulfilled).
	RequestSpotInstances(imageID, instanceType, subnetID, spotPrice string) (*SpotInstanceRequest, error)

	// DescribeSpotInstanceRequests returns spot requests, optionally filtered by IDs.
	DescribeSpotInstanceRequests(ids []string) []*SpotInstanceRequest

	// CancelSpotInstanceRequests cancels the given spot requests.
	CancelSpotInstanceRequests(ids []string) error

	// ---- placement groups ----

	// CreatePlacementGroup creates a new placement group.
	CreatePlacementGroup(name, strategy string) (*PlacementGroup, error)

	// DescribePlacementGroups returns placement groups, optionally filtered by names.
	DescribePlacementGroups(names []string) []*PlacementGroup

	// DeletePlacementGroup removes a placement group by name.
	DeletePlacementGroup(name string) error

	// ---- tags ----

	// CreateTags adds or updates tags on one or more resources.
	CreateTags(resourceIDs []string, tags map[string]string) error

	// DeleteTags removes the specified tag keys from one or more resources.
	DeleteTags(resourceIDs []string, keys []string) error

	// DescribeTags returns tag entries, optionally filtered by resource IDs.
	DescribeTags(resourceIDs []string) []TagEntry
}
