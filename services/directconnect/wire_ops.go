package directconnect

// Per-operation request (Input) wire shapes not already covered by the
// shared shapes in wire.go, plus the handful of bespoke, single-purpose
// Output shapes (bare-enum returns, the DescribeLoa flattened deprecated
// shape, MacSec associate/disassociate). Every op whose Output IS one of
// wire.go's shared shapes (connectionWire, lagWire, interconnectWire,
// virtualInterfaceWire, *Envelope) is not repeated here -- see handler_*.go
// for which shared type each op returns.

// newBGPPeerWire mirrors types.NewBGPPeer.
type newBGPPeerWire struct {
	AsnLong         *int64 `json:"asnLong,omitempty"`
	AddressFamily   string `json:"addressFamily,omitempty"`
	AmazonAddress   string `json:"amazonAddress,omitempty"`
	AuthKey         string `json:"authKey,omitempty"`
	CustomerAddress string `json:"customerAddress,omitempty"`
	Asn             int32  `json:"asn"`
}

// newPrivateVifWire mirrors types.NewPrivateVirtualInterface.
type newPrivateVifWire struct {
	AsnLong                *int64    `json:"asnLong,omitempty"`
	Mtu                    *int32    `json:"mtu,omitempty"`
	EnableSiteLink         *bool     `json:"enableSiteLink,omitempty"`
	VirtualInterfaceName   string    `json:"virtualInterfaceName"`
	AddressFamily          string    `json:"addressFamily,omitempty"`
	AmazonAddress          string    `json:"amazonAddress,omitempty"`
	AuthKey                string    `json:"authKey,omitempty"`
	CustomerAddress        string    `json:"customerAddress,omitempty"`
	DirectConnectGatewayID string    `json:"directConnectGatewayId,omitempty"`
	RateLimit              string    `json:"rateLimit,omitempty"`
	VirtualGatewayID       string    `json:"virtualGatewayId,omitempty"`
	Tags                   []tagWire `json:"tags,omitempty"`
	Vlan                   int32     `json:"vlan"`
	Asn                    int32     `json:"asn"`
}

// newPrivateVifAllocationWire mirrors types.NewPrivateVirtualInterfaceAllocation.
type newPrivateVifAllocationWire struct {
	AsnLong              *int64    `json:"asnLong,omitempty"`
	Mtu                  *int32    `json:"mtu,omitempty"`
	VirtualInterfaceName string    `json:"virtualInterfaceName"`
	AddressFamily        string    `json:"addressFamily,omitempty"`
	AmazonAddress        string    `json:"amazonAddress,omitempty"`
	AuthKey              string    `json:"authKey,omitempty"`
	CustomerAddress      string    `json:"customerAddress,omitempty"`
	RateLimit            string    `json:"rateLimit,omitempty"`
	Tags                 []tagWire `json:"tags,omitempty"`
	Vlan                 int32     `json:"vlan"`
	Asn                  int32     `json:"asn"`
}

// newPublicVifWire mirrors types.NewPublicVirtualInterface.
type newPublicVifWire struct {
	AsnLong              *int64                  `json:"asnLong,omitempty"`
	VirtualInterfaceName string                  `json:"virtualInterfaceName"`
	AddressFamily        string                  `json:"addressFamily,omitempty"`
	AmazonAddress        string                  `json:"amazonAddress,omitempty"`
	AuthKey              string                  `json:"authKey,omitempty"`
	CustomerAddress      string                  `json:"customerAddress,omitempty"`
	RateLimit            string                  `json:"rateLimit,omitempty"`
	RouteFilterPrefixes  []routeFilterPrefixWire `json:"routeFilterPrefixes,omitempty"`
	Tags                 []tagWire               `json:"tags,omitempty"`
	Vlan                 int32                   `json:"vlan"`
	Asn                  int32                   `json:"asn"`
}

// newPublicVifAllocationWire mirrors types.NewPublicVirtualInterfaceAllocation.
type newPublicVifAllocationWire struct {
	AsnLong              *int64                  `json:"asnLong,omitempty"`
	VirtualInterfaceName string                  `json:"virtualInterfaceName"`
	AddressFamily        string                  `json:"addressFamily,omitempty"`
	AmazonAddress        string                  `json:"amazonAddress,omitempty"`
	AuthKey              string                  `json:"authKey,omitempty"`
	CustomerAddress      string                  `json:"customerAddress,omitempty"`
	RateLimit            string                  `json:"rateLimit,omitempty"`
	RouteFilterPrefixes  []routeFilterPrefixWire `json:"routeFilterPrefixes,omitempty"`
	Tags                 []tagWire               `json:"tags,omitempty"`
	Vlan                 int32                   `json:"vlan"`
	Asn                  int32                   `json:"asn"`
}

// newTransitVifWire mirrors types.NewTransitVirtualInterface. VirtualInterfaceName/
// Vlan are NOT required at the struct level here (unlike newPrivateVifWire) --
// see PARITY.md.
type newTransitVifWire struct {
	AsnLong                *int64    `json:"asnLong,omitempty"`
	Mtu                    *int32    `json:"mtu,omitempty"`
	EnableSiteLink         *bool     `json:"enableSiteLink,omitempty"`
	CustomerAddress        string    `json:"customerAddress,omitempty"`
	AmazonAddress          string    `json:"amazonAddress,omitempty"`
	AuthKey                string    `json:"authKey,omitempty"`
	AddressFamily          string    `json:"addressFamily,omitempty"`
	DirectConnectGatewayID string    `json:"directConnectGatewayId,omitempty"`
	RateLimit              string    `json:"rateLimit,omitempty"`
	VirtualInterfaceName   string    `json:"virtualInterfaceName,omitempty"`
	Tags                   []tagWire `json:"tags,omitempty"`
	Vlan                   int32     `json:"vlan,omitempty"`
	Asn                    int32     `json:"asn"`
}

// newTransitVifAllocationWire mirrors types.NewTransitVirtualInterfaceAllocation.
type newTransitVifAllocationWire struct {
	AsnLong              *int64    `json:"asnLong,omitempty"`
	Mtu                  *int32    `json:"mtu,omitempty"`
	AddressFamily        string    `json:"addressFamily,omitempty"`
	AmazonAddress        string    `json:"amazonAddress,omitempty"`
	AuthKey              string    `json:"authKey,omitempty"`
	CustomerAddress      string    `json:"customerAddress,omitempty"`
	RateLimit            string    `json:"rateLimit,omitempty"`
	VirtualInterfaceName string    `json:"virtualInterfaceName,omitempty"`
	Tags                 []tagWire `json:"tags,omitempty"`
	Vlan                 int32     `json:"vlan,omitempty"`
	Asn                  int32     `json:"asn"`
}

// ---- request Input shapes, one per op (alphabetical) ----

type acceptGatewayAssociationProposalRequest struct {
	AssociatedGatewayOwnerAccount string `json:"associatedGatewayOwnerAccount"`
	DirectConnectGatewayID        string `json:"directConnectGatewayId"`
	ProposalID                    string `json:"proposalId"`
	//nolint:lll // mirrors the SDK's own field/JSON-key name verbatim, cannot be shortened
	OverrideAllowedPrefixesToDirectConnectGateway []routeFilterPrefixWire `json:"overrideAllowedPrefixesToDirectConnectGateway,omitempty"`
}

type allocateConnectionOnInterconnectRequest struct {
	Bandwidth      string `json:"bandwidth"`
	ConnectionName string `json:"connectionName"`
	InterconnectID string `json:"interconnectId"`
	OwnerAccount   string `json:"ownerAccount"`
	Vlan           int32  `json:"vlan"`
}

type allocateHostedConnectionRequest struct {
	Bandwidth      string    `json:"bandwidth"`
	ConnectionID   string    `json:"connectionId"`
	ConnectionName string    `json:"connectionName"`
	OwnerAccount   string    `json:"ownerAccount"`
	Tags           []tagWire `json:"tags,omitempty"`
	Vlan           int32     `json:"vlan"`
}

type allocatePrivateVifRequest struct {
	NewPrivateVirtualInterfaceAllocation *newPrivateVifAllocationWire `json:"newPrivateVirtualInterfaceAllocation"`
	ConnectionID                         string                       `json:"connectionId"`
	OwnerAccount                         string                       `json:"ownerAccount"`
}

type allocatePublicVifRequest struct {
	NewPublicVirtualInterfaceAllocation *newPublicVifAllocationWire `json:"newPublicVirtualInterfaceAllocation"`
	ConnectionID                        string                      `json:"connectionId"`
	OwnerAccount                        string                      `json:"ownerAccount"`
}

type allocateTransitVifRequest struct {
	NewTransitVirtualInterfaceAllocation *newTransitVifAllocationWire `json:"newTransitVirtualInterfaceAllocation"`
	ConnectionID                         string                       `json:"connectionId"`
	OwnerAccount                         string                       `json:"ownerAccount"`
}

type associateConnectionWithLagRequest struct {
	ConnectionID string `json:"connectionId"`
	LagID        string `json:"lagId"`
}

type associateHostedConnectionRequest struct {
	ConnectionID       string `json:"connectionId"`
	ParentConnectionID string `json:"parentConnectionId"`
}

type associateMacSecKeyRequest struct {
	ConnectionID string `json:"connectionId"`
	Cak          string `json:"cak,omitempty"`
	Ckn          string `json:"ckn,omitempty"`
	SecretARN    string `json:"secretARN,omitempty"`
}

type associateMacSecKeyResponse struct {
	ConnectionID string          `json:"connectionId,omitempty"`
	MacSecKeys   []macSecKeyWire `json:"macSecKeys,omitempty"`
}

type associateVifRequest struct {
	ConnectionID       string `json:"connectionId"`
	VirtualInterfaceID string `json:"virtualInterfaceId"`
}

type connectionIDRequest struct {
	ConnectionID string `json:"connectionId"`
}

type connectionStateResponse struct {
	ConnectionState string `json:"connectionState,omitempty"`
}

type confirmCustomerAgreementRequest struct {
	AgreementName string `json:"agreementName,omitempty"`
}

type statusResponse struct {
	Status string `json:"status,omitempty"`
}

type confirmPrivateVifRequest struct {
	VirtualInterfaceID     string `json:"virtualInterfaceId"`
	DirectConnectGatewayID string `json:"directConnectGatewayId,omitempty"`
	VirtualGatewayID       string `json:"virtualGatewayId,omitempty"`
}

type confirmPublicVifRequest struct {
	VirtualInterfaceID string `json:"virtualInterfaceId"`
}

type confirmTransitVifRequest struct {
	DirectConnectGatewayID string `json:"directConnectGatewayId"`
	VirtualInterfaceID     string `json:"virtualInterfaceId"`
}

type vifStateResponse struct {
	VirtualInterfaceState string `json:"virtualInterfaceState,omitempty"`
}

type createBGPPeerRequest struct {
	NewBGPPeer         *newBGPPeerWire `json:"newBGPPeer"`
	VirtualInterfaceID string          `json:"virtualInterfaceId,omitempty"`
}

type createConnectionRequest struct {
	Bandwidth      string    `json:"bandwidth"`
	ConnectionName string    `json:"connectionName"`
	Location       string    `json:"location"`
	LagID          string    `json:"lagId,omitempty"`
	ProviderName   string    `json:"providerName,omitempty"`
	RequestMACSec  *bool     `json:"requestMACSec,omitempty"`
	Tags           []tagWire `json:"tags,omitempty"`
}

type createDirectConnectGatewayRequest struct {
	DirectConnectGatewayName string    `json:"directConnectGatewayName"`
	AmazonSideAsn            *int64    `json:"amazonSideAsn,omitempty"`
	Tags                     []tagWire `json:"tags,omitempty"`
}

type createGatewayAssociationRequest struct {
	DirectConnectGatewayID string `json:"directConnectGatewayId"`
	GatewayID              string `json:"gatewayId,omitempty"`
	VirtualGatewayID       string `json:"virtualGatewayId,omitempty"`
	//nolint:lll // mirrors the SDK's own field/JSON-key name verbatim, cannot be shortened
	AddAllowedPrefixesToDirectConnectGateway []routeFilterPrefixWire `json:"addAllowedPrefixesToDirectConnectGateway,omitempty"`
}

type createGatewayAssociationProposalRequest struct {
	DirectConnectGatewayID           string `json:"directConnectGatewayId"`
	DirectConnectGatewayOwnerAccount string `json:"directConnectGatewayOwnerAccount"`
	GatewayID                        string `json:"gatewayId"`
	//nolint:lll // mirrors the SDK's own field/JSON-key name verbatim, cannot be shortened
	AddAllowedPrefixesToDirectConnectGateway []routeFilterPrefixWire `json:"addAllowedPrefixesToDirectConnectGateway,omitempty"`
	//nolint:lll // mirrors the SDK's own field/JSON-key name verbatim, cannot be shortened
	RemoveAllowedPrefixesToDirectConnectGateway []routeFilterPrefixWire `json:"removeAllowedPrefixesToDirectConnectGateway,omitempty"`
}

type createInterconnectRequest struct {
	Bandwidth        string    `json:"bandwidth"`
	InterconnectName string    `json:"interconnectName"`
	Location         string    `json:"location"`
	LagID            string    `json:"lagId,omitempty"`
	ProviderName     string    `json:"providerName,omitempty"`
	RequestMACSec    *bool     `json:"requestMACSec,omitempty"`
	Tags             []tagWire `json:"tags,omitempty"`
}

type createLagRequest struct {
	RequestMACSec        *bool     `json:"requestMACSec,omitempty"`
	ConnectionsBandwidth string    `json:"connectionsBandwidth"`
	LagName              string    `json:"lagName"`
	Location             string    `json:"location"`
	ConnectionID         string    `json:"connectionId,omitempty"`
	ProviderName         string    `json:"providerName,omitempty"`
	ChildConnectionTags  []tagWire `json:"childConnectionTags,omitempty"`
	Tags                 []tagWire `json:"tags,omitempty"`
	NumberOfConnections  int32     `json:"numberOfConnections"`
}

type createPrivateVifRequest struct {
	NewPrivateVirtualInterface *newPrivateVifWire `json:"newPrivateVirtualInterface"`
	ConnectionID               string             `json:"connectionId"`
}

type createPublicVifRequest struct {
	NewPublicVirtualInterface *newPublicVifWire `json:"newPublicVirtualInterface"`
	ConnectionID              string            `json:"connectionId"`
}

type createTransitVifRequest struct {
	NewTransitVirtualInterface *newTransitVifWire `json:"newTransitVirtualInterface"`
	ConnectionID               string             `json:"connectionId"`
}

type deleteBGPPeerRequest struct {
	AsnLong            *int64 `json:"asnLong,omitempty"`
	BgpPeerID          string `json:"bgpPeerId,omitempty"`
	CustomerAddress    string `json:"customerAddress,omitempty"`
	VirtualInterfaceID string `json:"virtualInterfaceId,omitempty"`
	Asn                int32  `json:"asn,omitempty"`
}

type directConnectGatewayIDRequest struct {
	DirectConnectGatewayID string `json:"directConnectGatewayId"`
}

type deleteGatewayAssociationRequest struct {
	AssociationID          string `json:"associationId,omitempty"`
	DirectConnectGatewayID string `json:"directConnectGatewayId,omitempty"`
	VirtualGatewayID       string `json:"virtualGatewayId,omitempty"`
}

type proposalIDRequest struct {
	ProposalID string `json:"proposalId"`
}

type interconnectIDRequest struct {
	InterconnectID string `json:"interconnectId"`
}

type interconnectStateResponse struct {
	InterconnectState string `json:"interconnectState,omitempty"`
}

type lagIDRequest struct {
	LagID string `json:"lagId"`
}

type virtualInterfaceIDRequest struct {
	VirtualInterfaceID string `json:"virtualInterfaceId"`
}

type describeLoaRequest struct {
	ConnectionID   string `json:"connectionId"`
	LoaContentType string `json:"loaContentType,omitempty"`
	ProviderName   string `json:"providerName,omitempty"`
}

type loaEnvelope struct {
	Loa *loaWire `json:"loa,omitempty"`
}

type describeInterconnectLoaRequest struct {
	InterconnectID string `json:"interconnectId"`
	LoaContentType string `json:"loaContentType,omitempty"`
	ProviderName   string `json:"providerName,omitempty"`
}

type describeConnectionsRequest struct {
	ConnectionID string `json:"connectionId,omitempty"`
	MaxResults   *int32 `json:"maxResults,omitempty"`
	NextToken    string `json:"nextToken,omitempty"`
}

type connectionsListResponse struct {
	NextToken   string           `json:"nextToken,omitempty"`
	Connections []connectionWire `json:"connections,omitempty"`
}

type describeCustomerMetadataResponse struct {
	NniPartnerType string                  `json:"nniPartnerType,omitempty"`
	Agreements     []customerAgreementWire `json:"agreements,omitempty"`
}

type describeGatewayAssociationProposalsRequest struct {
	AssociatedGatewayID    string `json:"associatedGatewayId,omitempty"`
	DirectConnectGatewayID string `json:"directConnectGatewayId,omitempty"`
	MaxResults             *int32 `json:"maxResults,omitempty"`
	NextToken              string `json:"nextToken,omitempty"`
	ProposalID             string `json:"proposalId,omitempty"`
}

type gatewayAssociationProposalsListResponse struct {
	NextToken string `json:"nextToken,omitempty"`
	//nolint:lll // mirrors the SDK's own field/JSON-key name verbatim, cannot be shortened
	DirectConnectGatewayAssociationProposals []gatewayAssociationProposalWire `json:"directConnectGatewayAssociationProposals,omitempty"`
}

type describeGatewayAssociationsRequest struct {
	AssociatedGatewayID    string `json:"associatedGatewayId,omitempty"`
	AssociationID          string `json:"associationId,omitempty"`
	DirectConnectGatewayID string `json:"directConnectGatewayId,omitempty"`
	MaxResults             *int32 `json:"maxResults,omitempty"`
	NextToken              string `json:"nextToken,omitempty"`
	VirtualGatewayID       string `json:"virtualGatewayId,omitempty"`
}

type gatewayAssociationsListResponse struct {
	NextToken                        string                   `json:"nextToken,omitempty"`
	DirectConnectGatewayAssociations []gatewayAssociationWire `json:"directConnectGatewayAssociations,omitempty"`
}

type describeGatewayAttachmentsRequest struct {
	DirectConnectGatewayID string `json:"directConnectGatewayId,omitempty"`
	MaxResults             *int32 `json:"maxResults,omitempty"`
	NextToken              string `json:"nextToken,omitempty"`
	VirtualInterfaceID     string `json:"virtualInterfaceId,omitempty"`
}

type gatewayAttachmentsListResponse struct {
	NextToken                       string                  `json:"nextToken,omitempty"`
	DirectConnectGatewayAttachments []gatewayAttachmentWire `json:"directConnectGatewayAttachments,omitempty"`
}

type describeGatewaysRequest struct {
	DirectConnectGatewayID string `json:"directConnectGatewayId,omitempty"`
	MaxResults             *int32 `json:"maxResults,omitempty"`
	NextToken              string `json:"nextToken,omitempty"`
}

type gatewaysListResponse struct {
	NextToken             string                     `json:"nextToken,omitempty"`
	DirectConnectGateways []directConnectGatewayWire `json:"directConnectGateways,omitempty"`
}

type describeHostedConnectionsRequest struct {
	ConnectionID string `json:"connectionId"`
	MaxResults   *int32 `json:"maxResults,omitempty"`
	NextToken    string `json:"nextToken,omitempty"`
}

type describeInterconnectsRequest struct {
	InterconnectID string `json:"interconnectId,omitempty"`
	MaxResults     *int32 `json:"maxResults,omitempty"`
	NextToken      string `json:"nextToken,omitempty"`
}

type interconnectsListResponse struct {
	NextToken     string             `json:"nextToken,omitempty"`
	Interconnects []interconnectWire `json:"interconnects,omitempty"`
}

type describeLagsRequest struct {
	LagID      string `json:"lagId,omitempty"`
	MaxResults *int32 `json:"maxResults,omitempty"`
	NextToken  string `json:"nextToken,omitempty"`
}

type lagsListResponse struct {
	NextToken string    `json:"nextToken,omitempty"`
	Lags      []lagWire `json:"lags,omitempty"`
}

type describeLoaFlatResponse struct {
	LoaContentType string `json:"loaContentType,omitempty"`
	LoaContent     []byte `json:"loaContent,omitempty"`
}

type locationsListResponse struct {
	Locations []locationWire `json:"locations,omitempty"`
}

type describeRouterConfigurationRequest struct {
	VirtualInterfaceID   string `json:"virtualInterfaceId"`
	RouterTypeIdentifier string `json:"routerTypeIdentifier,omitempty"`
}

type routerConfigurationResponse struct {
	CustomerRouterConfig string          `json:"customerRouterConfig,omitempty"`
	Router               *routerTypeWire `json:"router,omitempty"`
	VirtualInterfaceID   string          `json:"virtualInterfaceId,omitempty"`
	VirtualInterfaceName string          `json:"virtualInterfaceName,omitempty"`
}

type describeTagsRequest struct {
	ResourceArns []string `json:"resourceArns"`
}

type describeTagsResponse struct {
	ResourceTags []resourceTagWire `json:"resourceTags,omitempty"`
}

type virtualGatewaysListResponse struct {
	VirtualGateways []virtualGatewayWire `json:"virtualGateways,omitempty"`
}

type describeVifsRequest struct {
	ConnectionID       string `json:"connectionId,omitempty"`
	MaxResults         *int32 `json:"maxResults,omitempty"`
	NextToken          string `json:"nextToken,omitempty"`
	VirtualInterfaceID string `json:"virtualInterfaceId,omitempty"`
}

type vifsListResponse struct {
	NextToken         string                 `json:"nextToken,omitempty"`
	VirtualInterfaces []virtualInterfaceWire `json:"virtualInterfaces,omitempty"`
}

type disassociateConnectionFromLagRequest struct {
	ConnectionID string `json:"connectionId"`
	LagID        string `json:"lagId"`
}

type disassociateMacSecKeyRequest struct {
	ConnectionID string `json:"connectionId"`
	SecretARN    string `json:"secretARN"`
}

type listVirtualInterfaceRoutesRequest struct {
	Filters            *routeFiltersWire `json:"filters,omitempty"`
	MaxResults         *int32            `json:"maxResults,omitempty"`
	NextToken          string            `json:"nextToken,omitempty"`
	VirtualInterfaceID string            `json:"virtualInterfaceId,omitempty"`
}

type listVirtualInterfaceRoutesResponse struct {
	NextToken          string      `json:"nextToken,omitempty"`
	VirtualInterfaceID string      `json:"virtualInterfaceId,omitempty"`
	Routes             []routeWire `json:"routes,omitempty"`
}

type listVifTestHistoryRequest struct {
	MaxResults         *int32   `json:"maxResults,omitempty"`
	NextToken          string   `json:"nextToken,omitempty"`
	Status             string   `json:"status,omitempty"`
	TestID             string   `json:"testId,omitempty"`
	VirtualInterfaceID string   `json:"virtualInterfaceId,omitempty"`
	BgpPeers           []string `json:"bgpPeers,omitempty"`
}

type vifTestHistoryListResponse struct {
	NextToken                   string               `json:"nextToken,omitempty"`
	VirtualInterfaceTestHistory []vifTestHistoryWire `json:"virtualInterfaceTestHistory,omitempty"`
}

type startBgpFailoverTestRequest struct {
	TestDurationInMinutes *int32   `json:"testDurationInMinutes,omitempty"`
	VirtualInterfaceID    string   `json:"virtualInterfaceId"`
	BgpPeers              []string `json:"bgpPeers,omitempty"`
}

type vifTestEnvelope struct {
	VirtualInterfaceTest *vifTestHistoryWire `json:"virtualInterfaceTest,omitempty"`
}

type tagResourceRequest struct {
	ResourceArn string    `json:"resourceArn"`
	Tags        []tagWire `json:"tags"`
}

type untagResourceRequest struct {
	ResourceArn string   `json:"resourceArn"`
	TagKeys     []string `json:"tagKeys"`
}

type updateConnectionRequest struct {
	ConnectionID   string `json:"connectionId"`
	ConnectionName string `json:"connectionName,omitempty"`
	EncryptionMode string `json:"encryptionMode,omitempty"`
}

type updateGatewayRequest struct {
	DirectConnectGatewayID      string `json:"directConnectGatewayId"`
	NewDirectConnectGatewayName string `json:"newDirectConnectGatewayName"`
}

type updateGatewayAssociationRequest struct {
	AssociationID string `json:"associationId,omitempty"`
	//nolint:lll // mirrors the SDK's own field/JSON-key name verbatim, cannot be shortened
	AddAllowedPrefixesToDirectConnectGateway []routeFilterPrefixWire `json:"addAllowedPrefixesToDirectConnectGateway,omitempty"`
	//nolint:lll // mirrors the SDK's own field/JSON-key name verbatim, cannot be shortened
	RemoveAllowedPrefixesToDirectConnectGateway []routeFilterPrefixWire `json:"removeAllowedPrefixesToDirectConnectGateway,omitempty"`
}

type updateLagRequest struct {
	LagID          string `json:"lagId"`
	EncryptionMode string `json:"encryptionMode,omitempty"`
	LagName        string `json:"lagName,omitempty"`
	MinimumLinks   int32  `json:"minimumLinks,omitempty"`
}

type updateVifAttributesRequest struct {
	VirtualInterfaceID   string `json:"virtualInterfaceId"`
	EnableSiteLink       *bool  `json:"enableSiteLink,omitempty"`
	Mtu                  *int32 `json:"mtu,omitempty"`
	RateLimit            string `json:"rateLimit,omitempty"`
	VirtualInterfaceName string `json:"virtualInterfaceName,omitempty"`
}
