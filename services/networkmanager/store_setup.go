package networkmanager

import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func globalNetworkKeyFn(v *GlobalNetwork) string { return v.GlobalNetworkID }
func siteKeyFn(v *Site) string                   { return v.SiteID }
func deviceKeyFn(v *Device) string               { return v.DeviceID }
func linkKeyFn(v *Link) string                   { return v.LinkID }
func connectionKeyFn(v *Connection) string       { return v.ConnectionID }
func connectPeerKeyFn(v *ConnectPeer) string     { return v.ConnectPeerID }
func coreNetworkKeyFn(v *CoreNetwork) string     { return v.CoreNetworkID }
func attachmentKeyFn(v *Attachment) string       { return v.AttachmentID }
func peeringKeyFn(v *Peering) string             { return v.PeeringID }
func routeAnalysisKeyFn(v *RouteAnalysis) string { return v.RouteAnalysisID }

// linkAssociationKey builds the composite primary key a LinkAssociation is
// stored under: real AWS scopes an association by the (GlobalNetworkId,
// DeviceId, LinkId) triple, not a single generated ID (confirmed:
// AssociateLink/DisassociateLink both require all three, PARITY.md family
// E).
func linkAssociationKey(globalNetworkID, deviceID, linkID string) string {
	return globalNetworkID + "#" + deviceID + "#" + linkID
}

func linkAssociationKeyFn(v *LinkAssociation) string {
	return linkAssociationKey(v.GlobalNetworkID, v.DeviceID, v.LinkID)
}

func customerGatewayAssociationKey(globalNetworkID, cgwArn string) string {
	return globalNetworkID + "#" + cgwArn
}

func customerGatewayAssociationKeyFn(v *CustomerGatewayAssociation) string {
	return customerGatewayAssociationKey(v.GlobalNetworkID, v.CustomerGatewayArn)
}

func transitGatewayRegistrationKey(globalNetworkID, tgwArn string) string {
	return globalNetworkID + "#" + tgwArn
}

func transitGatewayRegistrationKeyFn(v *TransitGatewayRegistration) string {
	return transitGatewayRegistrationKey(v.GlobalNetworkID, v.TransitGatewayArn)
}

func transitGatewayConnectPeerAssociationKey(globalNetworkID, tgwCpArn string) string {
	return globalNetworkID + "#" + tgwCpArn
}

func transitGatewayConnectPeerAssociationKeyFn(v *TransitGatewayConnectPeerAssociation) string {
	return transitGatewayConnectPeerAssociationKey(v.GlobalNetworkID, v.TransitGatewayConnectPeerArn)
}

func connectPeerAssociationKey(globalNetworkID, connectPeerID string) string {
	return globalNetworkID + "#" + connectPeerID
}

func connectPeerAssociationKeyFn(v *ConnectPeerAssociation) string {
	return connectPeerAssociationKey(v.GlobalNetworkID, v.ConnectPeerID)
}

func policyHistoryKeyFn(v *CoreNetworkPolicyHistory) string {
	// keyed by an out-of-band CoreNetworkId the caller supplies via Put --
	// see corenetworks.go, which always constructs/looks these up by the
	// owning CoreNetwork's own ID rather than a field on the history
	// itself (CoreNetworkPolicyHistory carries no such field of its own).
	return v.ownerCoreNetworkID
}

func prefixListAssocKey(prefixListArn, coreNetworkID string) string {
	return prefixListArn + "#" + coreNetworkID
}

func prefixListAssocKeyFn(v *CoreNetworkPrefixListAssociation) string {
	return prefixListAssocKey(v.PrefixListArn, v.CoreNetworkID)
}

func routingPolicyLabelKey(coreNetworkID, attachmentID string) string {
	return coreNetworkID + "#" + attachmentID
}

func routingPolicyLabelKeyFn(v *AttachmentRoutingPolicyLabel) string {
	return routingPolicyLabelKey(v.CoreNetworkID, v.AttachmentID)
}

func networkResourceMetadataKeyFn(v *networkResourceMetadata) string { return v.ResourceArn }
func resourcePolicyKeyFn(v *resourcePolicy) string                   { return v.ResourceArn }

// registerAllTables registers every resource collection exactly once. Must
// be called during construction only -- see services/outposts/store_setup.go's
// doc comment for why (store.Register panics on a duplicate name).
func registerAllTables(b *InMemoryBackend) {
	b.globalNetworks = store.Register(b.registry, "globalNetworks", store.New(globalNetworkKeyFn))
	b.sites = store.Register(b.registry, "sites", store.New(siteKeyFn))
	b.devices = store.Register(b.registry, "devices", store.New(deviceKeyFn))
	b.links = store.Register(b.registry, "links", store.New(linkKeyFn))
	b.linkAssociations = store.Register(b.registry, "linkAssociations", store.New(linkAssociationKeyFn))
	b.connections = store.Register(b.registry, "connections", store.New(connectionKeyFn))

	b.customerGatewayAssociations = store.Register(
		b.registry, "customerGatewayAssociations", store.New(customerGatewayAssociationKeyFn),
	)
	b.transitGatewayRegistrations = store.Register(
		b.registry, "transitGatewayRegistrations", store.New(transitGatewayRegistrationKeyFn),
	)
	b.transitGatewayConnectPeerAssociations = store.Register(
		b.registry, "transitGatewayConnectPeerAssociations",
		store.New(transitGatewayConnectPeerAssociationKeyFn),
	)
	b.connectPeerAssociations = store.Register(
		b.registry, "connectPeerAssociations", store.New(connectPeerAssociationKeyFn),
	)

	b.connectPeers = store.Register(b.registry, "connectPeers", store.New(connectPeerKeyFn))

	b.coreNetworks = store.Register(b.registry, "coreNetworks", store.New(coreNetworkKeyFn))
	b.policyHistories = store.Register(b.registry, "policyHistories", store.New(policyHistoryKeyFn))
	b.prefixListAssocs = store.Register(b.registry, "prefixListAssocs", store.New(prefixListAssocKeyFn))
	b.routingPolicyLabels = store.Register(b.registry, "routingPolicyLabels", store.New(routingPolicyLabelKeyFn))

	b.attachments = store.Register(b.registry, "attachments", store.New(attachmentKeyFn))
	b.peerings = store.Register(b.registry, "peerings", store.New(peeringKeyFn))

	b.routeAnalyses = store.Register(b.registry, "routeAnalyses", store.New(routeAnalysisKeyFn))

	b.resourceMetadata = store.Register(b.registry, "resourceMetadata", store.New(networkResourceMetadataKeyFn))
	b.resourcePolicies = store.Register(b.registry, "resourcePolicies", store.New(resourcePolicyKeyFn))
}
