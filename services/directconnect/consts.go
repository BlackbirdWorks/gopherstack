package directconnect

import "time"

// asyncTransitionDelay is the simulated delay between each hop of a
// resource's state-transition chain, matching services/outposts'
// orderTransitionDelay / services/resiliencehub's asyncTransitionDelay /
// services/grafana's workspaceTransitionDelay.
const asyncTransitionDelay = 100 * time.Millisecond

// defaultPageLimit is the page size used by every Describe/List operation
// when the caller omits MaxResults, matching services/outposts/
// services/resiliencehub's convention.
const defaultPageLimit = 100

// Bandwidth wire-value strings shared across LAG-capacity checks
// (lags.go's lagMaxConnections) and the static Location port-speed seed
// data (static_data.go) -- named so they are not duplicated as bare string
// literals.
const (
	bandwidth1Gbps   = "1Gbps"
	bandwidth10Gbps  = "10Gbps"
	bandwidth100Gbps = "100Gbps"
	bandwidth400Gbps = "400Gbps"
)

// ConnectionState wire values (types.ConnectionState). ordering is specific
// to a hosted connection (see AllocateHostedConnection/
// AllocateConnectionOnInterconnect); requested is the initial state of a
// standard connection created via CreateConnection.
const (
	ConnectionStateOrdering  = "ordering"
	ConnectionStateRequested = "requested"
	ConnectionStatePending   = "pending"
	ConnectionStateAvailable = "available"
	ConnectionStateDown      = "down"
	ConnectionStateDeleting  = "deleting"
	ConnectionStateDeleted   = "deleted"
	ConnectionStateRejected  = "rejected"
	ConnectionStateUnknown   = "unknown"
)

// LagState / InterconnectState wire values -- both 7-value enums, identical
// shape, no "ordering" (neither is ever a hosted sub-resource).
const (
	LagStateRequested = "requested"
	LagStatePending   = "pending"
	LagStateAvailable = "available"
	LagStateDown      = "down"
	LagStateDeleting  = "deleting"
	LagStateDeleted   = "deleted"
	LagStateUnknown   = "unknown"
)

const (
	InterconnectStateRequested = "requested"
	InterconnectStatePending   = "pending"
	InterconnectStateAvailable = "available"
	InterconnectStateDown      = "down"
	InterconnectStateDeleting  = "deleting"
	InterconnectStateDeleted   = "deleted"
	InterconnectStateUnknown   = "unknown"
)

// VirtualInterfaceState wire values (types.VirtualInterfaceState) -- the
// richest, 10-value enum in this service.
const (
	VifStateConfirming = "confirming"
	VifStateVerifying  = "verifying"
	VifStatePending    = "pending"
	VifStateAvailable  = "available"
	VifStateDown       = "down"
	VifStateTesting    = "testing"
	VifStateDeleting   = "deleting"
	VifStateDeleted    = "deleted"
	VifStateRejected   = "rejected"
	VifStateUnknown    = "unknown"
)

// DirectConnectGatewayState wire values.
const (
	GatewayStatePending   = "pending"
	GatewayStateAvailable = "available"
	GatewayStateDeleting  = "deleting"
	GatewayStateDeleted   = "deleted"
)

// DirectConnectGatewayAssociationState wire values -- the only state enum in
// this service with a mid-lifecycle "updating" value.
const (
	AssociationStateAssociating    = "associating"
	AssociationStateAssociated     = "associated"
	AssociationStateDisassociating = "disassociating"
	AssociationStateDisassociated  = "disassociated"
	AssociationStateUpdating       = "updating"
)

// DirectConnectGatewayAssociationProposalState wire values -- only 3 values,
// no "rejected" (declining is: never accept, then the proposer deletes it).
const (
	ProposalStateRequested = "requested"
	ProposalStateAccepted  = "accepted"
	ProposalStateDeleted   = "deleted"
)

// DirectConnectGatewayAttachmentState wire values (derived, computed from a
// VIF's own state -- see gateways.go's attachmentFromVIF).
const (
	AttachmentStateAttaching = "attaching"
	AttachmentStateAttached  = "attached"
	AttachmentStateDetaching = "detaching"
	AttachmentStateDetached  = "detached"
)

// DirectConnectGatewayAttachmentType wire values -- only
// TransitVirtualInterface/PrivateVirtualInterface exist; public VIFs
// structurally cannot attach to a gateway.
const (
	AttachmentTypeTransit = "TransitVirtualInterface"
	AttachmentTypePrivate = "PrivateVirtualInterface"
)

// GatewayType wire values (types.GatewayType).
const (
	GatewayTypeVirtualPrivateGateway = "virtualPrivateGateway"
	GatewayTypeTransitGateway        = "transitGateway"
)

// VirtualInterfaceType wire values -- a bare *string on the wire (types.go
// confirms no typed enum exists), not one of this service's generated enum
// types.
const (
	VifTypePrivate = "private"
	VifTypePublic  = "public"
	VifTypeTransit = "transit"
)

// MacSecKey.State wire values -- bare string, not a typed enum (PARITY.md
// wire-trap #6).
const (
	MacSecStateAssociating    = "associating"
	MacSecStateAssociated     = "associated"
	MacSecStateDisassociating = "disassociating"
	MacSecStateDisassociated  = "disassociated"
)

// HasLogicalRedundancy wire values.
const (
	HasLogicalRedundancyUnknown = "unknown"
	HasLogicalRedundancyYes     = "yes"
	HasLogicalRedundancyNo      = "no"
)

// Connection/Lag/Interconnect EncryptionMode wire values -- bare *string, not
// a typed enum.
const (
	EncryptionModeNoEncrypt     = "no_encrypt"
	EncryptionModeShouldEncrypt = "should_encrypt"
	EncryptionModeMustEncrypt   = "must_encrypt"
)

// Connection/Lag/Interconnect PortEncryptionStatus wire values -- bare
// *string, not a typed enum.
const (
	PortEncryptionUp   = "Encryption Up"
	PortEncryptionDown = "Encryption Down"
)

// BGPPeerState wire values (the peer's lifecycle, distinct from BgpStatus's
// operational up/down axis).
const (
	BGPPeerStateVerifying = "verifying"
	BGPPeerStatePending   = "pending"
	BGPPeerStateAvailable = "available"
	BGPPeerStateDeleting  = "deleting"
	BGPPeerStateDeleted   = "deleted"
)

// BGPStatus wire values (the peer's operational up/down state).
const (
	BGPStatusUp      = "up"
	BGPStatusDown    = "down"
	BGPStatusUnknown = "unknown"
)

// AddressFamily wire values.
const (
	AddressFamilyIPv4 = "ipv4"
	AddressFamilyIPv6 = "ipv6"
)

// NniPartnerType wire values -- DescribeCustomerMetadata always returns
// nonPartner (see PARITY.md's honest-gap section: no real agreement
// workflow modeled).
const (
	NniPartnerTypeV1         = "v1"
	NniPartnerTypeV2         = "v2"
	NniPartnerTypeNonPartner = "nonPartner"
)

// LoaContentTypePdf is the only LoaContentType value this SDK models.
const LoaContentTypePdf = "application/pdf"

// defaultMtu is the default VIF MTU when the caller omits Mtu (1500 or 8500
// per NewPrivateVirtualInterface's doc comment).
const defaultMtu = 1500

// jumboMtu is the "jumbo frame" MTU value -- the only other supported value.
const jumboMtu = 8500

// resource-kind labels used in client-exception messages. Message text
// only -- this service has no ResourceId/ResourceType wire fields at all
// (see errors.go), unlike outposts/resiliencehub's ConflictException.
const (
	resourceConnection   = "connection"
	resourceLag          = "LAG"
	resourceInterconnect = "interconnect"
	resourceVif          = "virtual interface"
	resourceGateway      = "Direct Connect gateway"
	resourceAssociation  = "Direct Connect gateway association"
	resourceProposal     = "Direct Connect gateway association proposal"
	resourceBGPPeer      = "BGP peer"
	resourceMacSecKey    = "MACsec key"
	resourceTaggable     = "resource"
	resourceVifTest      = "virtual interface test"
)
