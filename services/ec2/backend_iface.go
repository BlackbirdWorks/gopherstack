package ec2

import "time"

// Backend defines the interface for EC2 backend operations.
// InMemoryBackend implements this interface; alternative providers (e.g. a
// real-AWS pass-through or a test double) can do so too, making the Handler
// backend-agnostic.
type Backend interface {
	// ---- instances ----

	// RunInstances creates one or more EC2 instance stubs.
	RunInstances(imageID, instanceType, subnetID string, count int) ([]*Instance, error)

	// SetInstanceAttribute persists a modifiable attribute on an instance.
	// Attribute names match EC2 ModifyInstanceAttribute keys (e.g. "userData", "instanceType").
	// Returns ErrInvalidInstanceState if the instance must be stopped for the given attribute.
	SetInstanceAttribute(instanceID, attribute, value string) error

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

	// CreateImage creates an AMI from an instance.
	CreateImage(instanceID, name, description string) (*AMIStub, error)

	// DescribeImageUsageReports returns synthetic image usage reports.
	DescribeImageUsageReports() []*ImageUsageReport

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
	ImportKeyPair(name, publicKeyMaterial string) (*KeyPair, error)

	// DescribeKeyPairs returns key pairs, optionally filtered by names.
	DescribeKeyPairs(names []string) []*KeyPair

	// DeleteKeyPair removes a key pair by name.
	DeleteKeyPair(name string) error

	// ---- EBS volumes ----

	// CreateVolume creates a new EBS volume stub.
	CreateVolume(az, volType string, size int) (*Volume, error)

	// SetVolumeEncryption marks a volume as encrypted and optionally sets its KMS key ID.
	SetVolumeEncryption(volumeID string, encrypted bool, kmsKeyID string) error

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

	// ---- EBS snapshots ----

	// CreateSnapshot creates an EBS snapshot from a volume.
	CreateSnapshot(volumeID, description string) (*Snapshot, error)

	// DescribeSnapshots returns snapshots, optionally filtered by IDs.
	DescribeSnapshots(ids []string) []*Snapshot

	// DeleteSnapshot removes a snapshot.
	DeleteSnapshot(id string) error

	// ---- AMI lifecycle ----

	// CopyImage copies an AMI to produce a new one.
	CopyImage(sourceImageID, name, description string) (*AMIStub, error)

	// DeregisterImage removes an AMI from the store.
	DeregisterImage(imageID string) error

	// ---- VPC / Subnet attributes ----

	// ModifyVpcAttribute enables or disables a VPC DNS attribute.
	ModifyVpcAttribute(vpcID, attribute string, value bool) error

	// ModifySubnetAttribute enables or disables an attribute on a subnet.
	ModifySubnetAttribute(subnetID, attribute string, value bool) error

	// ---- Network ACL CRUD ----

	// DescribeNetworkAcls returns default (auto-generated) network ACLs, optionally filtered by VPC IDs.
	DescribeNetworkAcls(vpcIDs []string) []*NetworkACL

	// CreateNetworkACL creates a new non-default network ACL in a VPC.
	CreateNetworkACL(vpcID string) (*StoredNetworkACL, error)

	// DeleteNetworkACL removes a non-default network ACL.
	DeleteNetworkACL(id string) error

	// CreateNetworkACLEntry adds a rule to an existing network ACL.
	CreateNetworkACLEntry(
		aclID string, ruleNumber int, protocol, action, cidr string,
		egress bool, fromPort, toPort int,
	) error

	// DeleteNetworkACLEntry removes a rule from a network ACL.
	DeleteNetworkACLEntry(aclID string, ruleNumber int, egress bool) error

	// DescribeStoredNetworkAcls returns explicitly created network ACLs.
	DescribeStoredNetworkAcls(ids []string) []*StoredNetworkACL

	// ---- Security group rules ----

	// DescribeSecurityGroupRules returns all rules for a security group.
	DescribeSecurityGroupRules(groupID string) ([]*SecurityGroupRuleDetail, error)

	// ModifySecurityGroupRules replaces all rules in the specified direction.
	ModifySecurityGroupRules(groupID string, rules []SecurityGroupRule, egress bool) error

	// ---- Launch template lifecycle ----

	// DeleteLaunchTemplate removes a launch template by ID.
	DeleteLaunchTemplate(id string) error

	// DescribeLaunchTemplateVersions returns versions of a launch template.
	DescribeLaunchTemplateVersions(id string) ([]*LaunchTemplate, error)

	// ---- VPC endpoint lifecycle ----

	// DeleteVpcEndpoints deletes one or more VPC endpoints, returning unsuccessful IDs.
	DeleteVpcEndpoints(ids []string) ([]string, error)

	// ---- VPC endpoints ----

	// CreateVpcEndpoint creates a VPC endpoint.
	CreateVpcEndpoint(
		vpcID, serviceName, endpointType string,
		subnetIDs []string,
	) (*VpcEndpoint, error)

	// DescribeVpcEndpoints returns VPC endpoints, optionally filtered by IDs.
	DescribeVpcEndpoints(ids []string) []*VpcEndpoint

	// ---- launch templates ----

	// CreateLaunchTemplate creates a launch template.
	CreateLaunchTemplate(name, imageID, instanceType string) (*LaunchTemplate, error)

	// DescribeLaunchTemplates returns launch templates, optionally filtered by names.
	DescribeLaunchTemplates(names []string) []*LaunchTemplate

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
	RequestSpotInstances(
		imageID, instanceType, subnetID, spotPrice string,
	) (*SpotInstanceRequest, error)

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

	// TagsForResource returns a copy of all tags currently set on a single
	// resource. Returns an empty (non-nil) map when nothing is tagged.
	TagsForResource(resourceID string) map[string]string

	// ---- accept / advertise / allocate operations ----

	// AcceptAddressTransfer accepts a pending Elastic IP address transfer.
	AcceptAddressTransfer(address string) (*AddressTransfer, error)

	// AcceptCapacityReservationBillingOwnership accepts billing ownership of a capacity reservation.
	AcceptCapacityReservationBillingOwnership(
		capacityReservationID string,
	) (*CapacityReservation, error)

	// AcceptReservedInstancesExchangeQuote accepts a reserved instances exchange quote.
	AcceptReservedInstancesExchangeQuote(
		reservedInstanceIDs []string,
	) (*ReservedInstancesExchange, error)

	// AcceptTransitGatewayMulticastDomainAssociations accepts multicast domain subnet associations.
	AcceptTransitGatewayMulticastDomainAssociations(
		transitGatewayMulticastDomainID, transitGatewayAttachmentID string,
		subnetIDs []string,
	) ([]*TransitGatewayMulticastDomainAssociation, error)

	// AcceptTransitGatewayPeeringAttachment accepts a transit gateway peering attachment.
	AcceptTransitGatewayPeeringAttachment(
		transitGatewayAttachmentID string,
	) (*TransitGatewayPeeringAttachment, error)

	// AcceptTransitGatewayVpcAttachment accepts a transit gateway VPC attachment.
	AcceptTransitGatewayVpcAttachment(
		transitGatewayAttachmentID string,
	) (*TransitGatewayVpcAttachment, error)

	// AcceptVpcEndpointConnections accepts VPC endpoint connections to a service.
	AcceptVpcEndpointConnections(
		serviceID string,
		vpcEndpointIDs []string,
	) ([]*VpcEndpointConnection, error)

	// AcceptVpcPeeringConnection accepts a VPC peering connection.
	AcceptVpcPeeringConnection(vpcPeeringConnectionID string) (*VpcPeeringConnection, error)

	// AdvertiseByoipCidr marks a BYOIP CIDR range as advertised.
	AdvertiseByoipCidr(cidr string) (*ByoipCidr, error)

	// AllocateHosts allocates one or more Dedicated Hosts.
	AllocateHosts(availabilityZone, instanceType string, hostCount int) ([]*Host, error)

	// ---- describe operations for new resource types ----

	// DescribeCapacityReservations returns capacity reservations, optionally filtered by IDs.
	DescribeCapacityReservations(ids []string) []*CapacityReservation

	// DescribeByoipCidrs returns BYOIP CIDRs, optionally filtered by state.
	DescribeByoipCidrs(state string) []*ByoipCidr

	// DescribeHosts returns dedicated hosts, optionally filtered by IDs.
	DescribeHosts(ids []string) []*Host

	// DescribeVpcPeeringConnections returns VPC peering connections, optionally filtered by IDs.
	DescribeVpcPeeringConnections(ids []string) []*VpcPeeringConnection

	// CreateVpcPeeringConnection creates a new pending VPC peering connection.
	CreateVpcPeeringConnection(requesterVPCID, accepterVPCID string) (*VpcPeeringConnection, error)

	// DeleteVpcPeeringConnection removes a VPC peering connection.
	DeleteVpcPeeringConnection(id string) error

	// ---- NACL: replace/reassociate ----

	// ReplaceNetworkACLEntry replaces a NACL rule by (ruleNumber, egress).
	ReplaceNetworkACLEntry(
		aclID string, ruleNumber int, protocol, action, cidr string,
		egress bool, fromPort, toPort int,
	) error

	// ReplaceNetworkACLAssociation moves a subnet to a different NACL.
	ReplaceNetworkACLAssociation(aclID, subnetID string) (string, error)

	// ---- VPC endpoint services ----

	// DescribeVpcEndpointServices returns available AWS endpoint service names.
	DescribeVpcEndpointServices() []string

	// ---- Key pair export ----

	// ExportKeyPair returns the public-key material for a key pair.
	ExportKeyPair(name string) (string, error)

	// ---- Instance type offerings ----

	// DescribeInstanceTypeOfferings returns available instance type / AZ pairs.
	DescribeInstanceTypeOfferings() []InstanceTypeOffering

	// ---- Transit gateways ----

	// DescribeTransitGateways returns transit gateways, optionally filtered by IDs.
	DescribeTransitGateways(ids []string) []*TransitGateway

	// CreateTransitGateway creates a new transit gateway stub.
	CreateTransitGateway(description string) (*TransitGateway, error)

	// DeleteTransitGateway removes a transit gateway by ID.
	DeleteTransitGateway(id string) error

	// CreateTransitGatewayVpcAttachment creates a TGW VPC attachment.
	CreateTransitGatewayVpcAttachment(
		tgwID, vpcID string,
		subnetIDs []string,
	) (*TransitGatewayVpcAttachment, error)

	// DescribeTransitGatewayVpcAttachments returns TGW VPC attachments, optionally filtered by IDs.
	DescribeTransitGatewayVpcAttachments(ids []string) []*TransitGatewayVpcAttachment

	// DeleteTransitGatewayVpcAttachment removes a TGW VPC attachment by ID.
	DeleteTransitGatewayVpcAttachment(id string) error

	// ModifyTransitGatewayAttribute updates attributes of a transit gateway.
	ModifyTransitGatewayAttribute(id, description string) error

	// ---- Flow Logs ----

	// CreateFlowLogs creates flow log records for the given resources.
	CreateFlowLogs(
		resourceIDs []string,
		trafficType, logDestinationType, logDestination string,
	) ([]*FlowLog, error)

	// DescribeFlowLogs returns flow logs, optionally filtered by IDs.
	DescribeFlowLogs(ids []string) []*FlowLog

	// DeleteFlowLogs removes flow logs by ID.
	DeleteFlowLogs(ids []string) error

	// ---- DHCP Options ----

	// CreateDhcpOptions creates a new DHCP options set.
	CreateDhcpOptions(configs []DhcpConfiguration) (*DhcpOptions, error)

	// DescribeDhcpOptions returns DHCP option sets, optionally filtered by IDs.
	DescribeDhcpOptions(ids []string) []*DhcpOptions

	// AssociateDhcpOptions associates a DHCP options set with a VPC.
	AssociateDhcpOptions(dhcpOptionsID, vpcID string) error

	// DeleteDhcpOptions removes a DHCP options set by ID.
	DeleteDhcpOptions(id string) error

	// ---- Launch Template extras ----

	// ModifyLaunchTemplate updates the default version of a launch template.
	ModifyLaunchTemplate(id string, defaultVersion int64) (*LaunchTemplate, error)

	// CreateLaunchTemplateVersion adds a new version to a launch template.
	CreateLaunchTemplateVersion(id, imageID, instanceType string) (*LaunchTemplateVersion, error)

	// DeleteLaunchTemplateVersions removes specific versions from a launch template.
	DeleteLaunchTemplateVersions(id string, versions []int64) ([]int64, error)

	// GetLaunchTemplateData returns synthesized launch template data from an instance.
	GetLaunchTemplateData(instanceID string) (*LaunchTemplate, error)

	// ---- Egress-only Internet Gateway ----

	// CreateEgressOnlyInternetGateway creates a new egress-only internet gateway.
	CreateEgressOnlyInternetGateway(vpcID string) (*EgressOnlyInternetGateway, error)

	// DescribeEgressOnlyInternetGateways returns egress-only internet gateways, optionally filtered by IDs.
	DescribeEgressOnlyInternetGateways(ids []string) []*EgressOnlyInternetGateway

	// DeleteEgressOnlyInternetGateway removes an egress-only internet gateway.
	DeleteEgressOnlyInternetGateway(id string) error

	// ---- IAM Instance Profile Associations ----

	// AssociateIamInstanceProfile associates an IAM instance profile with an instance.
	AssociateIamInstanceProfile(
		instanceID, profileARN string,
	) (*IamInstanceProfileAssociation, error)

	// DisassociateIamInstanceProfile removes an IAM instance profile association.
	DisassociateIamInstanceProfile(associationID string) (*IamInstanceProfileAssociation, error)

	// DescribeIamInstanceProfileAssociations returns IAM instance profile associations.
	DescribeIamInstanceProfileAssociations(
		associationIDs []string,
		instanceID string,
	) []*IamInstanceProfileAssociation

	// ReplaceIamInstanceProfileAssociation replaces the IAM instance profile on an existing association.
	ReplaceIamInstanceProfileAssociation(
		associationID, profileARN string,
	) (*IamInstanceProfileAssociation, error)

	// ---- Route Table extras ----

	// ReplaceRouteTableAssociation replaces an existing route table association with a new route table.
	ReplaceRouteTableAssociation(associationID, newRouteTableID string) (string, error)

	// ---- VPC CIDR ----

	// AssociateVpcCidrBlock associates a secondary CIDR block with a VPC.
	AssociateVpcCidrBlock(vpcID, cidrBlock string) (*VpcCidrBlockAssociation, error)

	// ---- Transit Gateway Route Tables ----

	// CreateTransitGatewayRouteTable creates a new TGW route table.
	CreateTransitGatewayRouteTable(tgwID string) (*TransitGatewayRouteTable, error)

	// DescribeTransitGatewayRouteTables returns TGW route tables, optionally filtered by IDs.
	DescribeTransitGatewayRouteTables(ids []string) []*TransitGatewayRouteTable

	// DeleteTransitGatewayRouteTable removes a TGW route table.
	DeleteTransitGatewayRouteTable(id string) error

	// ---- Transit Gateway Routes ----

	// CreateTransitGatewayRoute adds a static route to a TGW route table.
	CreateTransitGatewayRoute(
		routeTableID, destinationCIDR, attachmentID string,
	) (*TransitGatewayRoute, error)

	// DeleteTransitGatewayRoute removes a static route from a TGW route table.
	DeleteTransitGatewayRoute(routeTableID, destinationCIDR string) error

	// ReplaceTransitGatewayRoute replaces or upserts a route in a TGW route table.
	ReplaceTransitGatewayRoute(
		routeTableID, destinationCIDR, attachmentID string,
	) (*TransitGatewayRoute, error)

	// ---- Transit Gateway Route Table Associations ----

	// AssociateTransitGatewayRouteTable associates a TGW attachment with a route table.
	AssociateTransitGatewayRouteTable(
		routeTableID, attachmentID string,
	) (*TransitGatewayRouteTableAssociation, error)

	// DisassociateTransitGatewayRouteTable removes an association between a TGW attachment and route table.
	DisassociateTransitGatewayRouteTable(routeTableID, attachmentID string) error

	// ---- VPN Gateways ----

	// CreateVpnGateway creates a new virtual private gateway.
	CreateVpnGateway(gatewayType string) (*VpnGateway, error)

	// DescribeVpnGateways returns virtual private gateways, optionally filtered by IDs.
	DescribeVpnGateways(ids []string) []*VpnGateway

	// DeleteVpnGateway removes a VPN gateway.
	DeleteVpnGateway(id string) error

	// AttachVpnGateway attaches a VPN gateway to a VPC.
	AttachVpnGateway(vgwID, vpcID string) error

	// DetachVpnGateway detaches a VPN gateway from a VPC.
	DetachVpnGateway(vgwID, vpcID string) error

	// ---- Customer Gateways ----

	// CreateCustomerGateway creates a new customer gateway.
	CreateCustomerGateway(gatewayType, ipAddress, bgpAsn string) (*CustomerGateway, error)

	// DescribeCustomerGateways returns customer gateways, optionally filtered by IDs.
	DescribeCustomerGateways(ids []string) []*CustomerGateway

	// DeleteCustomerGateway removes a customer gateway.
	DeleteCustomerGateway(id string) error

	// ---- VPN Connections ----

	// CreateVpnConnection creates a VPN connection between a customer gateway and VPN gateway.
	CreateVpnConnection(connType, customerGatewayID, vpnGatewayID string) (*VpnConnection, error)

	// DescribeVpnConnections returns VPN connections, optionally filtered by IDs.
	DescribeVpnConnections(ids []string) []*VpnConnection

	// DeleteVpnConnection removes a VPN connection.
	DeleteVpnConnection(id string) error

	// ---- VPC Peering extras ----

	// RejectVpcPeeringConnection rejects a pending VPC peering connection.
	RejectVpcPeeringConnection(id string) error

	// ---- VPC Endpoint Service Configurations ----

	// CreateVpcEndpointServiceConfiguration creates a new endpoint service config.
	CreateVpcEndpointServiceConfiguration(
		acceptanceRequired bool,
		nlbARNs []string,
	) (*VpcEndpointServiceConfig, error)

	// DescribeVpcEndpointServiceConfigurations returns endpoint service configs, optionally filtered by IDs.
	DescribeVpcEndpointServiceConfigurations(ids []string) []*VpcEndpointServiceConfig

	// DeleteVpcEndpointServiceConfigurations removes endpoint service configs by IDs.
	DeleteVpcEndpointServiceConfigurations(ids []string) error

	// ModifyVpcEndpointServiceConfiguration updates acceptance required for a service config.
	ModifyVpcEndpointServiceConfiguration(id string, acceptanceRequired bool) error

	// ---- IPAM ----

	// CreateIpam creates a new IPAM instance.
	CreateIpam() (*Ipam, error)

	// DescribeIpams returns IPAM instances, optionally filtered by IDs.
	DescribeIpams(ids []string) []*Ipam

	// DeleteIpam removes an IPAM instance.
	DeleteIpam(id string) error

	// CreateIpamPool creates a new IPAM pool.
	CreateIpamPool(ipamID, addressFamily, locale, cidr string) (*IpamPool, error)

	// DescribeIpamPools returns IPAM pools, optionally filtered by IDs.
	DescribeIpamPools(ids []string) []*IpamPool

	// DeleteIpamPool removes an IPAM pool.
	DeleteIpamPool(id string) error

	// AllocateIpamPoolCidr allocates a CIDR from an IPAM pool.
	AllocateIpamPoolCidr(poolID, cidr string, netmaskLength int) (*IpamPoolAllocation, error)

	// GetIpamPoolCidrs returns allocations for an IPAM pool.
	GetIpamPoolCidrs(poolID string) []*IpamPoolAllocation

	// ReleaseIpamPoolAllocation releases an IPAM pool allocation.
	ReleaseIpamPoolAllocation(poolID, allocationID string) error

	// ---- spot fleet ----

	// RequestSpotFleet creates a new Spot Fleet request and fulfills it.
	RequestSpotFleet(config SpotFleetRequestConfig) (*SpotFleetRequest, error)

	// DescribeSpotFleetRequests returns spot fleet requests, optionally filtered by IDs.
	DescribeSpotFleetRequests(fleetIDs []string) ([]*SpotFleetRequest, error)

	// CancelSpotFleetRequests cancels spot fleet requests.
	CancelSpotFleetRequests(
		fleetIDs []string,
		terminateInstances bool,
	) ([]SpotFleetCancelResult, error)

	// ModifySpotFleetRequest updates the target capacity of a spot fleet request.
	ModifySpotFleetRequest(
		fleetID string,
		targetCapacity int,
		excessTermination string,
	) (*SpotFleetRequest, error)

	// DescribeSpotFleetInstances returns the instances in a spot fleet.
	DescribeSpotFleetInstances(fleetID string) ([]SpotFleetInstance, error)

	// DescribeSpotFleetRequestHistory returns history records for a spot fleet.
	DescribeSpotFleetRequestHistory(
		fleetID string,
		startTime time.Time,
	) ([]SpotFleetHistoryRecord, error)

	// ---- reset ----

	// Reset clears all resource state, returning the backend to its initial state.
	Reset()

	// ---- batch1: EBS volume lifecycle ----

	ModifyVolume(volumeID, volumeType string, size, iops int) (*VolumeModification, error)
	DescribeVolumeStatus(ids []string) []VolumeStatusItem
	DescribeVolumesModifications(ids []string) []*VolumeModification
	CopySnapshot(sourceSnapshotID, description string) (*Snapshot, error)
	CreateSnapshots(volumeIDs []string, description string) ([]*Snapshot, error)

	// ---- batch1: snapshot block public access ----

	GetSnapshotBlockPublicAccessState() string
	EnableSnapshotBlockPublicAccess(state string) error
	DisableSnapshotBlockPublicAccess()
	DescribeSnapshotTierStatus(ids []string) []SnapshotTierItem
	ModifySnapshotTier(snapshotID, storageTier string) error
	ResetSnapshotAttribute(snapshotID string) error

	// ---- batch1: VPC/Subnet/SG ----

	CreateDefaultVpc() (*VPC, error)
	CreateDefaultSubnet(az string) (*Subnet, error)
	AssociateSubnetCidrBlock(subnetID, ipv6CIDRBlock string) (*SubnetCIDRAssociation, error)
	DisassociateSubnetCidrBlock(associationID string) (string, error)
	AssociateSecurityGroupVpc(sgID, vpcID string) (*SGVpcAssociationState, error)
	DisassociateSecurityGroupVpc(sgID, vpcID string) error
	DescribeSecurityGroupReferences(sgIDs []string) []SGReference
	DescribeStaleSecurityGroups(vpcID string) []StaleSGItem
	DescribeSecurityGroupVpcAssociations(sgIDs []string) []SGVpcAssocItem
	ModifyVpcTenancy(vpcID, tenancy string) error
	ModifyVpcPeeringConnectionOptions(peeringID string, opts PeeringConnectionOptions) error
	GetVpcPeeringConnectionOptions(peeringID string) *PeeringConnectionOptions

	// ---- batch1: EIP attributes ----

	DescribeAddressesAttribute(allocationIDs []string) []AddressAttribute
	ModifyAddressAttribute(allocationID, domainName string) error
	ResetAddressAttribute(allocationID string) error

	// ---- batch1: instance ----

	GetConsoleOutput(instanceID string) (string, time.Time, error)
	ModifyInstanceMetadataOptions(
		instanceID, httpTokens, httpEndpoint, instanceMetadataTags string,
		hopLimit int,
	) (*IMDSOptions, error)
	GetInstanceMetadataDefaults() *InstanceMetadataDefaults
	ModifyInstanceMetadataDefaults(
		httpTokens, httpEndpoint, instanceMetadataTags string,
		hopLimit int,
	) error
	DescribeInstanceCreditSpecifications(ids []string) []InstanceCreditSpec
	ModifyInstanceCreditSpecification(instanceID, cpuCredits string) error
	DescribeInstanceTopology(ids []string) []InstanceTopologyItem
	MonitorInstances(instanceIDs []string) ([]MonitoringState, error)
	UnmonitorInstances(instanceIDs []string) ([]MonitoringState, error)

	// ---- batch1: network interface ----

	DescribeNetworkInterfaceAttribute(niID, attribute string) (*NIAttributeResult, error)
	ResetNetworkInterfaceAttribute(niID string) error
	DescribeNetworkInterfacePermissions(niIDs []string) []*NetworkInterfacePermission
	CreateNetworkInterfacePermission(
		niID, awsAccountID, awsService, permission string,
	) (*NetworkInterfacePermission, error)
	DeleteNetworkInterfacePermission(permissionID string) error
	AssignIpv6Addresses(niID string, count int) ([]string, error)
	UnassignIpv6Addresses(niID string, addresses []string) error

	// ---- batch1: account/misc ----

	DescribeAccountAttributes(names []string) []AccountAttribute
	DescribePrefixLists(ids []string) []PrefixList
	DescribeIDFormat(resources []string) []IDFormatItem
	ModifyIDFormat(resource string, useLongIDs bool) error
	DescribeIdentityIDFormat(_ string, resources []string) []IDFormatItem
	ModifyIdentityIDFormat(_ string, resource string, useLongIDs bool) error
	DescribeAggregateIDFormat() []IDFormatItem
	DescribePrincipalIDFormat(_ string) []IDFormatItem
	DescribeInstanceEventNotificationAttributes() *InstanceEventNotificationAttributes
	DeregisterInstanceEventNotificationAttributes()

	// ---- batch2 ----

	CreateVpcEndpointConnectionNotification(
		serviceID, endpointID, notifARN string,
		events []string,
	) (*VpcEndpointConnectionNotification, error)
	DescribeVpcEndpointConnectionNotifications(ids []string) []*VpcEndpointConnectionNotification
	DeleteVpcEndpointConnectionNotifications(ids []string) error
	ModifyVpcEndpointConnectionNotification(
		id, notifARN string,
		events []string,
	) (*VpcEndpointConnectionNotification, error)
	DescribeVpcEndpointConnections(serviceIDs []string) []*VpcEndpointConnection
	DescribeVpcEndpointAssociations(endpointIDs []string) []*VpcEndpoint
	ModifyVpcEndpointServicePayerResponsibility(serviceID, payerResponsibility string) error
	DescribeVpcEndpointServicePermissions(serviceID string) []string
	ModifyVpcEndpointServicePermissions(serviceID string, add, remove []string) error
	ModifyVpcEndpoint(endpointID string, addSubnetIDs, removeSubnetIDs []string) error
	EnableEbsEncryptionByDefault()
	DisableEbsEncryptionByDefault()
	GetEbsEncryptionByDefault() bool
	GetEbsDefaultKmsKeyID() string
	ModifyEbsDefaultKmsKeyID(kmsKeyID string) error
	EnableVolumeIO(volumeID string) error
	LockSnapshot(snapshotID, lockMode string, durationDays int) (*SnapshotLock, error)
	UnlockSnapshot(snapshotID string) error
	DescribeLockedSnapshots(ids []string) []*SnapshotLock
	CopyVolumes(volumeIDs []string, destinationRegion string) ([]CopyVolumesResult, error)
	DisassociateVpcCidrBlock(associationID string) error
	DisassociateNatGatewayAddress(natGatewayID string) error
	AssociateNatGatewayAddress(natGatewayID, allocationID string) error
	AssignPrivateNatGatewayAddress(natGatewayID string) error
	DisableImage(imageID string) error
	EnableImage(imageID string) error
	EnableImageBlockPublicAccess(state string) error
	DisableImageBlockPublicAccess()
	GetImageBlockPublicAccessState() string
	EnableImageDeprecation(imageID, deprecateAt string) error
	DisableImageDeprecation(imageID string) error
	EnableImageDeregistrationProtection(imageID string) error
	DisableImageDeregistrationProtection(imageID string) error
	ModifyImageAttribute(imageID, attribute, value string) error
	ResetImageAttribute(imageID, attribute string) error
	DescribeInstanceImageMetadata(instanceIDs []string) []InstanceImageMetadataItem
	EnableSerialConsoleAccess()
	DisableSerialConsoleAccess()
	GetSerialConsoleAccessStatus() bool
	EnableVgwRoutePropagation(routeTableID, gatewayID string) error
	DisableVgwRoutePropagation(routeTableID, gatewayID string) error
	GetDefaultCreditSpecification() string
	ModifyDefaultCreditSpecification(cpuCredits string) error
	CreateReplaceRootVolumeTask(instanceID, snapshotID string) (*ReplaceRootVolumeTask, error)
	DescribeReplaceRootVolumeTasks(ids []string) []*ReplaceRootVolumeTask
	EnableAddressTransfer(allocationID, transferAccountID string) (*AddressTransfer, error)
	DisableAddressTransfer(allocationID string) error
	DescribeAddressTransfers(allocationIDs []string) []*AddressTransfer
	CreateSubnetCidrReservation(
		subnetID, cidr, reservationType, description string,
	) (*SubnetCIDRReservation, error)
	DeleteSubnetCidrReservation(reservationID string) error

	// ---- batch3 ----

	CreateCapacityReservation(
		instanceType, availabilityZone string,
		instanceCount int,
	) (*CapacityReservation, error)
	CancelCapacityReservation(reservationID string) error
	ModifyCapacityReservation(reservationID string, instanceCount int) error
	GetGroupsForCapacityReservation(reservationID string) ([]string, error)
	CreateInstanceConnectEndpoint(
		subnetID string,
		securityGroupIDs []string,
		preserveClientIP bool,
	) (*InstanceConnectEndpoint, error)
	DeleteInstanceConnectEndpoint(id string) error
	DescribeInstanceConnectEndpoints(ids []string) []*InstanceConnectEndpoint
	ModifyInstanceConnectEndpoint(id string, preserveClientIP bool) error
	CreateInstanceEventWindow(name, cronExpression string) (*InstanceEventWindow, error)
	DeleteInstanceEventWindow(id string) error
	DescribeInstanceEventWindows(ids []string) []*InstanceEventWindow
	ModifyInstanceEventWindow(id, name, cronExpression string) error
	CreateSpotDatafeedSubscription(bucket, prefix string) (*SpotDatafeed, error)
	DeleteSpotDatafeedSubscription()
	DescribeSpotDatafeedSubscription() *SpotDatafeed
	RegisterImage(name, description, architecture string) (*AMIStub, error)
	ImportImage(description, architecture, platform string) (*ImageImportTask, error)
	DescribeImportImageTasks(taskIDs []string) []*ImageImportTask
	ExportImage(imageID, description string) (string, error)
	ListImagesInRecycleBin(imageIDs []string) []*RecycleBinImage
	RestoreImageFromRecycleBin(imageID string) error
	ListSnapshotsInRecycleBin(snapshotIDs []string) []*Snapshot
	RestoreSnapshotFromRecycleBin(snapshotID string) error
	RestoreSnapshotTier(snapshotID string) error
	ImportSnapshot(description string) (*SnapshotImportTask, error)
	DescribeImportSnapshotTasks(taskIDs []string) []*SnapshotImportTask
	EnableFastLaunch(imageID string) error
	DisableFastLaunch(imageID string) error
	DescribeFastLaunchImages(imageIDs []string) []FastLaunchImageItem
	EnableFastSnapshotRestores(snapshotIDs, availabilityZones []string) error
	DisableFastSnapshotRestores(snapshotIDs, availabilityZones []string) error
	DescribeFastSnapshotRestores() []FastSnapshotRestoreItem
	GetPasswordData(instanceID string) (string, time.Time, error)
	GetConsoleScreenshot(instanceID string) (string, error)
	GetInstanceTypesFromInstanceRequirements() []string
	GetSubnetCidrReservations(subnetID string) ([]*SubnetCIDRReservation, error)
	GetSecurityGroupsForVpc(vpcID string) ([]SecurityGroupForVpcItem, error)
	ReplaceRoute(rtID, destCIDR, gatewayID, natGatewayID string) error
	RegisterInstanceEventNotificationAttributes(includeAllTags bool)
	ResetEbsDefaultKmsKeyID()
	UpdateSecurityGroupRuleDescriptionsIngress(groupID string, rules []SecurityGroupRule) error
	UpdateSecurityGroupRuleDescriptionsEgress(groupID string, rules []SecurityGroupRule) error
	ListVolumesInRecycleBin(volumeIDs []string) []*RecycleBinVolume
	RestoreVolumeFromRecycleBin(volumeID string) error
	RestoreAddressToClassic(publicIP string) error
	ReportInstanceStatus(instanceID, status, description string) error
	ModifyVpnConnection(vpnConnectionID, vpnGatewayID string) error
	CreateVpnConnectionRoute(vpnConnectionID, destinationCIDR string) (*VpnConnectionRoute, error)
	DeleteVpnConnectionRoute(vpnConnectionID, destinationCIDR string) error
	ModifyTransitGateway(tgwID, description string) error

	// ---- batch4: ManagedPrefixList ----
	CreateManagedPrefixList(name, addressFamily string, maxEntries int) (*ManagedPrefixList, error)
	DeleteManagedPrefixList(id string) error
	DescribeManagedPrefixLists(ids []string) []*ManagedPrefixList
	GetManagedPrefixListEntries(id string) ([]PrefixListEntry, error)
	ModifyManagedPrefixList(id string, addEntries, removeEntries []PrefixListEntry) error
	RestoreManagedPrefixListVersion(id string, version int64) error

	// ---- batch4: ClientVpnEndpoint ----
	CreateClientVpnEndpoint(clientCidrBlock, description string, dnsServers []string) (*ClientVpnEndpoint, error)
	DeleteClientVpnEndpoint(id string) error
	DescribeClientVpnEndpoints(ids []string) []*ClientVpnEndpoint
	AssociateClientVpnTargetNetwork(endpointID, subnetID string) error
	DisassociateClientVpnTargetNetwork(endpointID, subnetID string) error
	DescribeClientVpnTargetNetworks(endpointID string) ([]string, error)
	CreateClientVpnRoute(endpointID, destinationCidr, description string) error
	DeleteClientVpnRoute(endpointID, destinationCidr string) error
	DescribeClientVpnRoutes(endpointID string) ([]ClientVpnRoute, error)
	AuthorizeClientVpnIngress(endpointID, cidr, description string) error
	RevokeClientVpnIngress(endpointID, cidr string) error
	DescribeClientVpnAuthorizationRules(endpointID string) ([]ClientVpnAuthRule, error)
	ModifyClientVpnEndpoint(endpointID, description string, dnsServers []string) error
	ApplySecurityGroupsToClientVpnTargetNetwork(endpointID string, sgIDs []string) error
	DescribeClientVpnConnections(endpointID string) ([]string, error)
	TerminateClientVpnConnections(endpointID string) error

	// ---- batch4: TGW Peering ----
	CreateTransitGatewayPeeringAttachment(
		transitGatewayID, peerTransitGatewayID string, _ string,
	) (*TransitGatewayPeeringAttachment, error)
	DeleteTransitGatewayPeeringAttachment(id string) error
	DescribeTransitGatewayPeeringAttachments(ids []string) []*TransitGatewayPeeringAttachment

	// ---- batch4: TGW Connect ----
	CreateTransitGatewayConnect(transportAttachmentID, transitGatewayID string) (*TransitGatewayConnect, error)
	DeleteTransitGatewayConnect(id string) error
	DescribeTransitGatewayConnects(ids []string) []*TransitGatewayConnect
	CreateTransitGatewayConnectPeer(
		connectAttachmentID, peerAddress string,
		insideCidrBlocks []string,
	) (*TransitGatewayConnectPeer, error)
	DeleteTransitGatewayConnectPeer(id string) error
	DescribeTransitGatewayConnectPeers(ids []string) []*TransitGatewayConnectPeer

	// ---- batch4: TGW PrefixListRef ----
	CreateTransitGatewayPrefixListReference(
		routeTableID, prefixListID string,
		blackhole bool,
	) (*TransitGatewayPrefixListReference, error)
	DeleteTransitGatewayPrefixListReference(routeTableID, prefixListID string) error
	GetTransitGatewayPrefixListReferences(routeTableID string) ([]*TransitGatewayPrefixListReference, error)

	// ---- batch4: VerifiedAccess ----
	CreateVerifiedAccessEndpoint(groupID, endpointType, description string) (*VerifiedAccessEndpoint, error)
	DeleteVerifiedAccessEndpoint(id string) error
	DescribeVerifiedAccessEndpoints(ids []string) []*VerifiedAccessEndpoint
	ModifyVerifiedAccessEndpoint(id, description string) error
	CreateVerifiedAccessGroup(instanceID, description string) (*VerifiedAccessGroup, error)
	DeleteVerifiedAccessGroup(id string) error
	DescribeVerifiedAccessGroups(ids []string) []*VerifiedAccessGroup
	CreateVerifiedAccessInstance(description string) (*VerifiedAccessInstance, error)
	DeleteVerifiedAccessInstance(id string) error
	DescribeVerifiedAccessInstances(ids []string) []*VerifiedAccessInstance
	CreateVerifiedAccessTrustProvider(trustProviderType, description string) (*VerifiedAccessTrustProvider, error)
	DeleteVerifiedAccessTrustProvider(id string) error
	DescribeVerifiedAccessTrustProviders(ids []string) []*VerifiedAccessTrustProvider
	AttachVerifiedAccessTrustProvider(instanceID, trustProviderID string) error
	DetachVerifiedAccessTrustProvider(instanceID, trustProviderID string) error

	// ---- batch5: TrafficMirror ----
	CreateTrafficMirrorFilter(description string) (*TrafficMirrorFilter, error)
	DeleteTrafficMirrorFilter(id string) error
	DescribeTrafficMirrorFilters(ids []string) []*TrafficMirrorFilter
	ModifyTrafficMirrorFilterNetworkServices(id string, add, remove []string) error
	CreateTrafficMirrorFilterRule(
		filterID, direction, action, srcCIDR, dstCIDR, description string,
		ruleNumber, protocol int,
	) (*TrafficMirrorFilterRule, error)
	DeleteTrafficMirrorFilterRule(id string) error
	DescribeTrafficMirrorFilterRules(filterID string) ([]*TrafficMirrorFilterRule, error)
	ModifyTrafficMirrorFilterRule(id, action, description string) error
	CreateTrafficMirrorSession(
		networkInterfaceID, targetID, filterID, description string,
		sessionNumber int,
	) (*TrafficMirrorSession, error)
	DeleteTrafficMirrorSession(id string) error
	DescribeTrafficMirrorSessions(ids []string) []*TrafficMirrorSession
	ModifyTrafficMirrorSession(id, targetID, filterID, description string) error
	CreateTrafficMirrorTarget(
		networkInterfaceID, networkLoadBalancerArn, description string,
	) (*TrafficMirrorTarget, error)
	DeleteTrafficMirrorTarget(id string) error
	DescribeTrafficMirrorTargets(ids []string) []*TrafficMirrorTarget

	// ---- batch5: EC2 Fleet ----
	CreateFleet(fleetType string, totalTargetCapacity int) (*Fleet, error)
	DeleteFleets(ids []string) []string
	DescribeFleets(ids []string) []*Fleet
	ModifyFleet(id string, totalTargetCapacity int, excessPolicy string) error

	// ---- batch5: NetworkInsights ----
	CreateNetworkInsightsPath(
		sourceID, destinationID, protocol string,
		destinationPort int,
	) (*NetworkInsightsPath, error)
	DeleteNetworkInsightsPath(id string) error
	DescribeNetworkInsightsPaths(ids []string) []*NetworkInsightsPath
	StartNetworkInsightsAnalysis(pathID string) (*NetworkInsightsAnalysis, error)
	DeleteNetworkInsightsAnalysis(id string) error
	DescribeNetworkInsightsAnalyses(ids []string) []*NetworkInsightsAnalysis
	CreateNetworkInsightsAccessScope() (*NetworkInsightsAccessScope, error)
	DeleteNetworkInsightsAccessScope(id string) error
	DescribeNetworkInsightsAccessScopes(ids []string) []*NetworkInsightsAccessScope
	StartNetworkInsightsAccessScopeAnalysis(scopeID string) (*NetworkInsightsAccessScopeAnalysis, error)
	DeleteNetworkInsightsAccessScopeAnalysis(id string) error
	DescribeNetworkInsightsAccessScopeAnalyses(ids []string) []*NetworkInsightsAccessScopeAnalysis

	// ---- batch5: BYOIP ----
	ProvisionByoipCidr(cidr, description string) (*ByoipCidr, error)
	DeprovisionByoipCidr(cidr string) (*ByoipCidr, error)
	WithdrawByoipCidr(cidr string) (*ByoipCidr, error)

	// ---- batch5: CarrierGateway ----
	CreateCarrierGateway(vpcID string) (*CarrierGateway, error)
	DeleteCarrierGateway(id string) error
	DescribeCarrierGateways(ids []string) []*CarrierGateway

	// ---- batch5: ReservedInstances ----
	DescribeReservedInstances(ids []string) []*ReservedInstance
	DescribeReservedInstancesOfferings(instanceType, az, productDesc string) []*ReservedInstancesOffering
	PurchaseReservedInstancesOffering(offeringID string, instanceCount int) (*ReservedInstance, error)
	CreateReservedInstancesListing(reservedInstancesID string, instanceCount int) (*ReservedInstancesListing, error)
	CancelReservedInstancesListing(id string) error
	DescribeReservedInstancesListings(ids []string) []*ReservedInstancesListing
	DescribeReservedInstancesModifications(ids []string) []*ReservedInstancesModification
	ModifyReservedInstances(
		reservedInstancesIDs []string,
		targetInstanceType string,
		targetCount int,
	) (*ReservedInstancesModification, error)
	DeleteQueuedReservedInstances(ids []string)
}
