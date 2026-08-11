package networkmanager

// EC2Resolver lets this backend validate ARNs that reference EC2 resources
// (VPC/Subnet/CustomerGateway/TransitGateway/VpnConnection/
// TransitGatewayConnectPeer/TransitGatewayRouteTable) against the real
// services/ec2 backend, mirroring services/directconnect's
// EC2GatewayResolver (services/directconnect/store.go). Wired in by
// cli.go's wireNetworkManagerEC2. A nil resolver (the default) accepts
// every EC2 ARN unvalidated -- e.g. isolated unit tests with no EC2 backend
// wired, matching this package's prior documented scope decision.
type EC2Resolver interface {
	ResolveVpc(vpcArn string) bool
	ResolveSubnet(subnetArn string) bool
	ResolveCustomerGateway(customerGatewayArn string) bool
	ResolveTransitGateway(transitGatewayArn string) bool
	ResolveVpnConnection(vpnConnectionArn string) bool
	ResolveTransitGatewayConnectPeer(transitGatewayConnectPeerArn string) bool
	ResolveTransitGatewayRouteTable(transitGatewayRouteTableArn string) bool

	// TransitGatewayRouteTableForAttachment resolves a real EC2
	// TransitGatewayAttachmentArn to the TransitGatewayRouteTable ID
	// associated with it, for StartRouteAnalysis's real route-table walk
	// (routeanalysis.go). ok is false when the attachment does not exist,
	// is not available, or has no associated route table.
	TransitGatewayRouteTableForAttachment(transitGatewayAttachmentArn string) (routeTableID string, ok bool)
	// TransitGatewayRoutes returns every route in the named TGW route
	// table, for the same real walk.
	TransitGatewayRoutes(routeTableID string) []EC2TransitGatewayRoute
}

// EC2TransitGatewayRoute is the subset of services/ec2's
// TransitGatewayRoute this package's route-analysis walk needs, decoupled
// from ec2's own type so this package does not import services/ec2
// directly -- cli.go's adapter bridges the two, matching
// services/directconnect's EC2GatewayResolver pattern.
type EC2TransitGatewayRoute struct {
	DestinationCIDRBlock string
	State                string
	AttachmentID         string
}

// DirectConnectResolver lets this backend validate DirectConnectGatewayArn
// against the real services/directconnect backend. Wired in by cli.go's
// wireNetworkManagerDirectConnect. A nil resolver (the default) accepts
// every DirectConnectGatewayArn unvalidated.
type DirectConnectResolver interface {
	ResolveDirectConnectGateway(directConnectGatewayArn string) bool
}
