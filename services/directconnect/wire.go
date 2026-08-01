package directconnect

// Wire request/response shapes for the awsjson1.1 protocol. Field names use
// EXACT lowerCamelCase JSON keys read directly from
// aws-sdk-go-v2/service/directconnect@v1.43.3's serializers.go/
// deserializers.go (every AWS JSON-protocol member name is the Go SDK's own
// exported field name with only its first rune lowercased -- confirmed
// individually for Connection, Tag, AssociatedGateway, MacSecKey,
// VirtualGateway, Location, and every timestamp field; see PARITY.md). This
// is NOT the lowerCamel-by-service-convention scheme resiliencehub/outposts
// use for restjson1 -- it happens to look similar but is independently
// derived from this service's own wire evidence, not copied from theirs.
//
// time.Time fields are wire-encoded as epoch-seconds JSON numbers (pkgs/
// awstime.Epoch), never RFC3339 strings -- see wire_convert.go's epochPtr.

// ---- shared nested shapes ----

// tagWire mirrors types.Tag ("key"/"value" -- confirmed via
// deserializeDocumentTag, NOT pkgs/tags.KV's "Key"/"Value" capitalization).
type tagWire struct {
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

// routeFilterPrefixWire mirrors types.RouteFilterPrefix.
type routeFilterPrefixWire struct {
	Cidr string `json:"cidr,omitempty"`
}

// macSecKeyWire mirrors types.MacSecKey.
type macSecKeyWire struct {
	Ckn       string `json:"ckn,omitempty"`
	SecretARN string `json:"secretARN,omitempty"`
	StartOn   string `json:"startOn,omitempty"`
	State     string `json:"state,omitempty"`
}

// rateLimiterStatusWire mirrors types.RateLimiterStatus.
type rateLimiterStatusWire struct {
	TotalBandwidth string `json:"totalBandwidth,omitempty"`
	InUse          int32  `json:"inUse"`
	MaxAllowed     int32  `json:"maxAllowed"`
	Remaining      int32  `json:"remaining"`
}

// bgpPeerWire mirrors types.BGPPeer.
type bgpPeerWire struct {
	AddressFamily      string `json:"addressFamily,omitempty"`
	AmazonAddress      string `json:"amazonAddress,omitempty"`
	AsnLong            *int64 `json:"asnLong,omitempty"`
	AuthKey            string `json:"authKey,omitempty"`
	AwsDeviceV2        string `json:"awsDeviceV2,omitempty"`
	AwsLogicalDeviceID string `json:"awsLogicalDeviceId,omitempty"`
	BgpPeerID          string `json:"bgpPeerId,omitempty"`
	BgpPeerState       string `json:"bgpPeerState,omitempty"`
	BgpStatus          string `json:"bgpStatus,omitempty"`
	CustomerAddress    string `json:"customerAddress,omitempty"`
	Asn                int32  `json:"asn"`
}

// routerTypeWire mirrors types.RouterType.
type routerTypeWire struct {
	Platform                  string `json:"platform,omitempty"`
	RouterTypeIdentifier      string `json:"routerTypeIdentifier,omitempty"`
	Software                  string `json:"software,omitempty"`
	Vendor                    string `json:"vendor,omitempty"`
	XsltTemplateName          string `json:"xsltTemplateName,omitempty"`
	XsltTemplateNameForMacSec string `json:"xsltTemplateNameForMacSec,omitempty"`
}

// locationWire mirrors types.Location.
type locationWire struct {
	LocationCode              string   `json:"locationCode,omitempty"`
	LocationName              string   `json:"locationName,omitempty"`
	Region                    string   `json:"region,omitempty"`
	AvailableMacSecPortSpeeds []string `json:"availableMacSecPortSpeeds,omitempty"`
	AvailablePortSpeeds       []string `json:"availablePortSpeeds,omitempty"`
	AvailableProviders        []string `json:"availableProviders,omitempty"`
}

// customerAgreementWire mirrors types.CustomerAgreement.
type customerAgreementWire struct {
	AgreementName string `json:"agreementName,omitempty"`
	Status        string `json:"status,omitempty"`
}

// resourceTagWire mirrors types.ResourceTag -- DescribeTags' batch-shaped
// output element.
type resourceTagWire struct {
	ResourceArn string    `json:"resourceArn,omitempty"`
	Tags        []tagWire `json:"tags,omitempty"`
}

// virtualGatewayWire mirrors types.VirtualGateway. VirtualGatewayState is a
// bare string on the wire (PARITY.md wire-trap #6).
type virtualGatewayWire struct {
	VirtualGatewayID    string `json:"virtualGatewayId,omitempty"`
	VirtualGatewayState string `json:"virtualGatewayState,omitempty"`
}

// loaWire mirrors types.Loa (the current, nested shape -- see
// DescribeLoaOutput's own flattened variant below for the deprecated
// sibling, PARITY.md wire-trap #7).
type loaWire struct {
	LoaContentType string `json:"loaContentType,omitempty"`
	LoaContent     []byte `json:"loaContent,omitempty"`
}

// associatedGatewayWire mirrors types.AssociatedGateway.
type associatedGatewayWire struct {
	ID           string `json:"id,omitempty"`
	OwnerAccount string `json:"ownerAccount,omitempty"`
	Region       string `json:"region,omitempty"`
	Type         string `json:"type,omitempty"`
}

// connectionWire mirrors types.Connection -- the standalone-Connection wire
// shape used by every op whose Output IS a Connection (CreateConnection,
// AllocateConnectionOnInterconnect, AllocateHostedConnection,
// AssociateConnectionWithLag, AssociateHostedConnection,
// DisassociateConnectionFromLag, ConfirmConnection returns only
// ConnectionState so does NOT use this, DeleteConnection, UpdateConnection)
// and the element type of every Connections[] list (DescribeConnections,
// DescribeConnectionsOnInterconnect, DescribeHostedConnections, and
// Lag.Connections).
type connectionWire struct {
	LoaIssueTime                     *float64               `json:"loaIssueTime,omitempty"`
	RateLimiterStatus                *rateLimiterStatusWire `json:"rateLimiterStatus,omitempty"`
	AwsDeviceV2                      string                 `json:"awsDeviceV2,omitempty"`
	AwsLogicalDeviceID               string                 `json:"awsLogicalDeviceId,omitempty"`
	Bandwidth                        string                 `json:"bandwidth,omitempty"`
	ConnectionID                     string                 `json:"connectionId,omitempty"`
	ConnectionName                   string                 `json:"connectionName,omitempty"`
	ConnectionState                  string                 `json:"connectionState,omitempty"`
	EncryptionMode                   string                 `json:"encryptionMode,omitempty"`
	HasLogicalRedundancy             string                 `json:"hasLogicalRedundancy,omitempty"`
	LagID                            string                 `json:"lagId,omitempty"`
	Location                         string                 `json:"location,omitempty"`
	MacSecKeys                       []macSecKeyWire        `json:"macSecKeys,omitempty"`
	OwnerAccount                     string                 `json:"ownerAccount,omitempty"`
	PartnerName                      string                 `json:"partnerName,omitempty"`
	PortEncryptionStatus             string                 `json:"portEncryptionStatus,omitempty"`
	ProviderName                     string                 `json:"providerName,omitempty"`
	Region                           string                 `json:"region,omitempty"`
	Tags                             []tagWire              `json:"tags,omitempty"`
	Vlan                             int32                  `json:"vlan"`
	JumboFrameCapable                bool                   `json:"jumboFrameCapable,omitempty"`
	MacSecCapable                    bool                   `json:"macSecCapable,omitempty"`
	PartnerInterconnectMacSecCapable bool                   `json:"partnerInterconnectMacSecCapable,omitempty"`
}

// lagWire mirrors types.Lag.
type lagWire struct {
	RateLimiterStatus       *rateLimiterStatusWire `json:"rateLimiterStatus,omitempty"`
	AwsDeviceV2             string                 `json:"awsDeviceV2,omitempty"`
	AwsLogicalDeviceID      string                 `json:"awsLogicalDeviceId,omitempty"`
	Connections             []connectionWire       `json:"connections,omitempty"`
	ConnectionsBandwidth    string                 `json:"connectionsBandwidth,omitempty"`
	EncryptionMode          string                 `json:"encryptionMode,omitempty"`
	HasLogicalRedundancy    string                 `json:"hasLogicalRedundancy,omitempty"`
	LagID                   string                 `json:"lagId,omitempty"`
	LagName                 string                 `json:"lagName,omitempty"`
	LagState                string                 `json:"lagState,omitempty"`
	Location                string                 `json:"location,omitempty"`
	MacSecKeys              []macSecKeyWire        `json:"macSecKeys,omitempty"`
	OwnerAccount            string                 `json:"ownerAccount,omitempty"`
	ProviderName            string                 `json:"providerName,omitempty"`
	Region                  string                 `json:"region,omitempty"`
	Tags                    []tagWire              `json:"tags,omitempty"`
	MinimumLinks            int32                  `json:"minimumLinks"`
	NumberOfConnections     int32                  `json:"numberOfConnections"`
	AllowsHostedConnections bool                   `json:"allowsHostedConnections,omitempty"`
	JumboFrameCapable       bool                   `json:"jumboFrameCapable,omitempty"`
	MacSecCapable           bool                   `json:"macSecCapable,omitempty"`
}

// interconnectWire mirrors types.Interconnect.
type interconnectWire struct {
	LoaIssueTime         *float64        `json:"loaIssueTime,omitempty"`
	AwsDeviceV2          string          `json:"awsDeviceV2,omitempty"`
	AwsLogicalDeviceID   string          `json:"awsLogicalDeviceId,omitempty"`
	Bandwidth            string          `json:"bandwidth,omitempty"`
	EncryptionMode       string          `json:"encryptionMode,omitempty"`
	HasLogicalRedundancy string          `json:"hasLogicalRedundancy,omitempty"`
	InterconnectID       string          `json:"interconnectId,omitempty"`
	InterconnectName     string          `json:"interconnectName,omitempty"`
	InterconnectState    string          `json:"interconnectState,omitempty"`
	LagID                string          `json:"lagId,omitempty"`
	Location             string          `json:"location,omitempty"`
	MacSecKeys           []macSecKeyWire `json:"macSecKeys,omitempty"`
	PortEncryptionStatus string          `json:"portEncryptionStatus,omitempty"`
	ProviderName         string          `json:"providerName,omitempty"`
	Region               string          `json:"region,omitempty"`
	Tags                 []tagWire       `json:"tags,omitempty"`
	JumboFrameCapable    bool            `json:"jumboFrameCapable,omitempty"`
	MacSecCapable        bool            `json:"macSecCapable,omitempty"`
}

// virtualInterfaceWire mirrors types.VirtualInterface -- used both as the
// FLATTENED shape (CreatePrivateVirtualInterfaceOutput,
// CreatePublicVirtualInterfaceOutput, AllocatePrivateVirtualInterfaceOutput,
// AllocatePublicVirtualInterfaceOutput, AssociateVirtualInterfaceOutput,
// UpdateVirtualInterfaceAttributesOutput -- these embed this struct's fields
// directly, PARITY.md wire-trap #1) and, wrapped in vifEnvelope, as the
// NESTED shape (CreateTransitVirtualInterfaceOutput,
// AllocateTransitVirtualInterfaceOutput, CreateBGPPeerOutput,
// DeleteBGPPeerOutput) and the element type of VirtualInterfaces[]
// (DescribeVirtualInterfacesOutput).
type virtualInterfaceWire struct {
	AmazonSideAsn          *int64                  `json:"amazonSideAsn,omitempty"`
	AsnLong                *int64                  `json:"asnLong,omitempty"`
	Mtu                    *int32                  `json:"mtu,omitempty"`
	OwnerAccount           string                  `json:"ownerAccount,omitempty"`
	Region                 string                  `json:"region,omitempty"`
	AuthKey                string                  `json:"authKey,omitempty"`
	AwsDeviceV2            string                  `json:"awsDeviceV2,omitempty"`
	AwsLogicalDeviceID     string                  `json:"awsLogicalDeviceId,omitempty"`
	VirtualInterfaceType   string                  `json:"virtualInterfaceType,omitempty"`
	ConnectionID           string                  `json:"connectionId,omitempty"`
	CustomerAddress        string                  `json:"customerAddress,omitempty"`
	CustomerRouterConfig   string                  `json:"customerRouterConfig,omitempty"`
	DirectConnectGatewayID string                  `json:"directConnectGatewayId,omitempty"`
	Location               string                  `json:"location,omitempty"`
	AddressFamily          string                  `json:"addressFamily,omitempty"`
	RateLimit              string                  `json:"rateLimit,omitempty"`
	AmazonAddress          string                  `json:"amazonAddress,omitempty"`
	VirtualInterfaceState  string                  `json:"virtualInterfaceState,omitempty"`
	VirtualInterfaceName   string                  `json:"virtualInterfaceName,omitempty"`
	VirtualGatewayID       string                  `json:"virtualGatewayId,omitempty"`
	VirtualInterfaceID     string                  `json:"virtualInterfaceId,omitempty"`
	Tags                   []tagWire               `json:"tags,omitempty"`
	RouteFilterPrefixes    []routeFilterPrefixWire `json:"routeFilterPrefixes,omitempty"`
	BgpPeers               []bgpPeerWire           `json:"bgpPeers,omitempty"`
	Vlan                   int32                   `json:"vlan"`
	Asn                    int32                   `json:"asn"`
	JumboFrameCapable      bool                    `json:"jumboFrameCapable,omitempty"`
	SiteLinkEnabled        bool                    `json:"siteLinkEnabled,omitempty"`
}

// vifEnvelope wraps a nested VirtualInterface -- see virtualInterfaceWire's
// doc comment.
type vifEnvelope struct {
	VirtualInterface *virtualInterfaceWire `json:"virtualInterface,omitempty"`
}

// directConnectGatewayWire mirrors types.DirectConnectGateway.
type directConnectGatewayWire struct {
	AmazonSideAsn             *int64    `json:"amazonSideAsn,omitempty"`
	DirectConnectGatewayID    string    `json:"directConnectGatewayId,omitempty"`
	DirectConnectGatewayName  string    `json:"directConnectGatewayName,omitempty"`
	DirectConnectGatewayState string    `json:"directConnectGatewayState,omitempty"`
	OwnerAccount              string    `json:"ownerAccount,omitempty"`
	StateChangeError          string    `json:"stateChangeError,omitempty"`
	Tags                      []tagWire `json:"tags,omitempty"`
}

// gatewayEnvelope wraps a single DirectConnectGateway.
type gatewayEnvelope struct {
	DirectConnectGateway *directConnectGatewayWire `json:"directConnectGateway,omitempty"`
}

// gatewayAssociationWire mirrors types.DirectConnectGatewayAssociation.
// AssociatedCoreNetwork is intentionally never populated (always omitted --
// no Cloud WAN backing service exists in this tree, see PARITY.md).
type gatewayAssociationWire struct {
	AssociatedGateway                     *associatedGatewayWire  `json:"associatedGateway,omitempty"`
	AssociationID                         string                  `json:"associationId,omitempty"`
	AssociationState                      string                  `json:"associationState,omitempty"`
	DirectConnectGatewayID                string                  `json:"directConnectGatewayId,omitempty"`
	DirectConnectGatewayOwnerAccount      string                  `json:"directConnectGatewayOwnerAccount,omitempty"`
	StateChangeError                      string                  `json:"stateChangeError,omitempty"`
	VirtualGatewayID                      string                  `json:"virtualGatewayId,omitempty"`
	VirtualGatewayOwnerAccount            string                  `json:"virtualGatewayOwnerAccount,omitempty"`
	AllowedPrefixesToDirectConnectGateway []routeFilterPrefixWire `json:"allowedPrefixesToDirectConnectGateway,omitempty"`
}

// associationEnvelope wraps a single DirectConnectGatewayAssociation.
type associationEnvelope struct {
	DirectConnectGatewayAssociation *gatewayAssociationWire `json:"directConnectGatewayAssociation,omitempty"`
}

// gatewayAssociationProposalWire mirrors
// types.DirectConnectGatewayAssociationProposal.
type gatewayAssociationProposalWire struct {
	AssociatedGateway                *associatedGatewayWire `json:"associatedGateway,omitempty"`
	DirectConnectGatewayID           string                 `json:"directConnectGatewayId,omitempty"`
	DirectConnectGatewayOwnerAccount string                 `json:"directConnectGatewayOwnerAccount,omitempty"`
	//nolint:lll // mirrors the SDK's own field/JSON-key name verbatim, cannot be shortened
	ExistingAllowedPrefixesToDirectConnectGateway []routeFilterPrefixWire `json:"existingAllowedPrefixesToDirectConnectGateway,omitempty"`
	ProposalID                                    string                  `json:"proposalId,omitempty"`
	ProposalState                                 string                  `json:"proposalState,omitempty"`
	//nolint:lll // mirrors the SDK's own field/JSON-key name verbatim, cannot be shortened
	RequestedAllowedPrefixesToDirectConnectGateway []routeFilterPrefixWire `json:"requestedAllowedPrefixesToDirectConnectGateway,omitempty"`
}

// proposalEnvelope wraps a single DirectConnectGatewayAssociationProposal.
type proposalEnvelope struct {
	//nolint:lll // mirrors the SDK's own field/JSON-key name verbatim, cannot be shortened
	DirectConnectGatewayAssociationProposal *gatewayAssociationProposalWire `json:"directConnectGatewayAssociationProposal,omitempty"`
}

// gatewayAttachmentWire mirrors types.DirectConnectGatewayAttachment --
// a DERIVED, computed-at-read-time record (see gateways.go's
// attachmentFromVIF); there is no dedicated Create op for this type.
type gatewayAttachmentWire struct {
	AttachmentState              string `json:"attachmentState,omitempty"`
	AttachmentType               string `json:"attachmentType,omitempty"`
	DirectConnectGatewayID       string `json:"directConnectGatewayId,omitempty"`
	StateChangeError             string `json:"stateChangeError,omitempty"`
	VirtualInterfaceID           string `json:"virtualInterfaceId,omitempty"`
	VirtualInterfaceOwnerAccount string `json:"virtualInterfaceOwnerAccount,omitempty"`
	VirtualInterfaceRegion       string `json:"virtualInterfaceRegion,omitempty"`
}

// vifTestHistoryWire mirrors types.VirtualInterfaceTestHistory.
type vifTestHistoryWire struct {
	EndTime               *float64 `json:"endTime,omitempty"`
	StartTime             *float64 `json:"startTime,omitempty"`
	TestDurationInMinutes *int32   `json:"testDurationInMinutes,omitempty"`
	TestID                string   `json:"testId,omitempty"`
	VirtualInterfaceID    string   `json:"virtualInterfaceId,omitempty"`
	Status                string   `json:"status,omitempty"`
	OwnerAccount          string   `json:"ownerAccount,omitempty"`
	BgpPeers              []string `json:"bgpPeers,omitempty"`
}
