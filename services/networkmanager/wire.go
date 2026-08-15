package networkmanager

import (
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// Wire request/response shapes for the restjson1 protocol. Unlike mgn's
// lowerCamelCase convention, this service's JSON member names are the SDK's
// own exported PascalCase field name verbatim (confirmed by direct read of
// aws-sdk-go-v2/service/networkmanager@v1.44.3's serializers.go/
// deserializers.go -- e.g. `object.Key("GlobalNetworkId")`,
// case "GlobalNetworkArn": -- see PARITY.md). Every *time.Time member
// deserializes via smithytime.ParseEpochSeconds, so every timestamp here is
// wire-coded as epoch-seconds via pkgs/awstime.Epoch (see wire_convert.go's
// epochPtr). Query-string filters (MaxResults/NextToken/*Ids[] on every
// GET) are lowerCamelCase and are parsed directly from url.Values in
// handler.go/family handler files, never JSON-decoded -- only POST/PATCH/
// (body-carrying-DELETE) request bodies get a wire struct below.

// ---- shared ----

type locationWire struct {
	Address   string `json:"Address,omitempty"`
	Latitude  string `json:"Latitude,omitempty"`
	Longitude string `json:"Longitude,omitempty"`
}

type awsLocationWire struct {
	SubnetArn string `json:"SubnetArn,omitempty"`
	Zone      string `json:"Zone,omitempty"`
}

type bandwidthWire struct {
	DownloadSpeed int32 `json:"DownloadSpeed,omitempty"`
	UploadSpeed   int32 `json:"UploadSpeed,omitempty"`
}

// ---- Global Network ----

type globalNetworkWire struct {
	CreatedAt        *float64  `json:"CreatedAt,omitempty"`
	GlobalNetworkArn string    `json:"GlobalNetworkArn,omitempty"`
	GlobalNetworkID  string    `json:"GlobalNetworkId,omitempty"`
	Description      string    `json:"Description,omitempty"`
	State            string    `json:"State,omitempty"`
	Tags             []tags.KV `json:"Tags,omitempty"`
}

type globalNetworkEnvelope struct {
	GlobalNetwork *globalNetworkWire `json:"GlobalNetwork,omitempty"`
}

type createGlobalNetworkReq struct {
	Description string    `json:"Description,omitempty"`
	Tags        []tags.KV `json:"Tags,omitempty"`
}

type updateGlobalNetworkReq struct {
	Description string `json:"Description,omitempty"`
}

type describeGlobalNetworksResponse struct {
	NextToken      string              `json:"NextToken,omitempty"`
	GlobalNetworks []globalNetworkWire `json:"GlobalNetworks"`
}

// ---- Site ----

type siteWire struct {
	CreatedAt       *float64      `json:"CreatedAt,omitempty"`
	Location        *locationWire `json:"Location,omitempty"`
	SiteArn         string        `json:"SiteArn,omitempty"`
	SiteID          string        `json:"SiteId,omitempty"`
	GlobalNetworkID string        `json:"GlobalNetworkId,omitempty"`
	Description     string        `json:"Description,omitempty"`
	State           string        `json:"State,omitempty"`
	Tags            []tags.KV     `json:"Tags,omitempty"`
}

type siteEnvelope struct {
	Site *siteWire `json:"Site,omitempty"`
}

type createSiteReq struct {
	Description string        `json:"Description,omitempty"`
	Location    *locationWire `json:"Location,omitempty"`
	Tags        []tags.KV     `json:"Tags,omitempty"`
}

type updateSiteReq struct {
	Location    *locationWire `json:"Location,omitempty"`
	Description string        `json:"Description,omitempty"`
}

type getSitesResponse struct {
	NextToken string     `json:"NextToken,omitempty"`
	Sites     []siteWire `json:"Sites"`
}

// ---- Device ----

type deviceWire struct {
	Location        *locationWire    `json:"Location,omitempty"`
	CreatedAt       *float64         `json:"CreatedAt,omitempty"`
	AWSLocation     *awsLocationWire `json:"AWSLocation,omitempty"`
	GlobalNetworkID string           `json:"GlobalNetworkId,omitempty"`
	DeviceArn       string           `json:"DeviceArn,omitempty"`
	DeviceID        string           `json:"DeviceId,omitempty"`
	Description     string           `json:"Description,omitempty"`
	Model           string           `json:"Model,omitempty"`
	SerialNumber    string           `json:"SerialNumber,omitempty"`
	SiteID          string           `json:"SiteId,omitempty"`
	Type            string           `json:"Type,omitempty"`
	Vendor          string           `json:"Vendor,omitempty"`
	State           string           `json:"State,omitempty"`
	Tags            []tags.KV        `json:"Tags,omitempty"`
}

type deviceEnvelope struct {
	Device *deviceWire `json:"Device,omitempty"`
}

type createDeviceReq struct {
	AWSLocation  *awsLocationWire `json:"AWSLocation,omitempty"`
	Location     *locationWire    `json:"Location,omitempty"`
	Description  string           `json:"Description,omitempty"`
	Model        string           `json:"Model,omitempty"`
	SerialNumber string           `json:"SerialNumber,omitempty"`
	SiteID       string           `json:"SiteId,omitempty"`
	Type         string           `json:"Type,omitempty"`
	Vendor       string           `json:"Vendor,omitempty"`
	Tags         []tags.KV        `json:"Tags,omitempty"`
}

type updateDeviceReq struct {
	AWSLocation  *awsLocationWire `json:"AWSLocation,omitempty"`
	Location     *locationWire    `json:"Location,omitempty"`
	Description  string           `json:"Description,omitempty"`
	Model        string           `json:"Model,omitempty"`
	SerialNumber string           `json:"SerialNumber,omitempty"`
	SiteID       string           `json:"SiteId,omitempty"`
	Type         string           `json:"Type,omitempty"`
	Vendor       string           `json:"Vendor,omitempty"`
}

type getDevicesResponse struct {
	NextToken string       `json:"NextToken,omitempty"`
	Devices   []deviceWire `json:"Devices"`
}

// ---- Link ----

type linkWire struct {
	CreatedAt       *float64       `json:"CreatedAt,omitempty"`
	Bandwidth       *bandwidthWire `json:"Bandwidth,omitempty"`
	LinkArn         string         `json:"LinkArn,omitempty"`
	LinkID          string         `json:"LinkId,omitempty"`
	GlobalNetworkID string         `json:"GlobalNetworkId,omitempty"`
	SiteID          string         `json:"SiteId,omitempty"`
	Description     string         `json:"Description,omitempty"`
	Provider        string         `json:"Provider,omitempty"`
	Type            string         `json:"Type,omitempty"`
	State           string         `json:"State,omitempty"`
	Tags            []tags.KV      `json:"Tags,omitempty"`
}

type linkEnvelope struct {
	Link *linkWire `json:"Link,omitempty"`
}

type createLinkReq struct {
	Bandwidth   *bandwidthWire `json:"Bandwidth,omitempty"`
	SiteID      string         `json:"SiteId"`
	Description string         `json:"Description,omitempty"`
	Provider    string         `json:"Provider,omitempty"`
	Type        string         `json:"Type,omitempty"`
	Tags        []tags.KV      `json:"Tags,omitempty"`
}

type updateLinkReq struct {
	Bandwidth   *bandwidthWire `json:"Bandwidth,omitempty"`
	Description string         `json:"Description,omitempty"`
	Provider    string         `json:"Provider,omitempty"`
	Type        string         `json:"Type,omitempty"`
}

type getLinksResponse struct {
	NextToken string     `json:"NextToken,omitempty"`
	Links     []linkWire `json:"Links"`
}

// ---- Link Association ----

type linkAssociationWire struct {
	DeviceID             string `json:"DeviceId,omitempty"`
	GlobalNetworkID      string `json:"GlobalNetworkId,omitempty"`
	LinkAssociationState string `json:"LinkAssociationState,omitempty"`
	LinkID               string `json:"LinkId,omitempty"`
}

type linkAssociationEnvelope struct {
	LinkAssociation *linkAssociationWire `json:"LinkAssociation,omitempty"`
}

type linkAssociationReq struct {
	DeviceID string `json:"DeviceId"`
	LinkID   string `json:"LinkId"`
}

type getLinkAssociationsResponse struct {
	NextToken        string                `json:"NextToken,omitempty"`
	LinkAssociations []linkAssociationWire `json:"LinkAssociations"`
}

// ---- Connection ----

type connectionWire struct {
	CreatedAt         *float64  `json:"CreatedAt,omitempty"`
	ConnectedDeviceID string    `json:"ConnectedDeviceId,omitempty"`
	ConnectedLinkID   string    `json:"ConnectedLinkId,omitempty"`
	ConnectionArn     string    `json:"ConnectionArn,omitempty"`
	ConnectionID      string    `json:"ConnectionId,omitempty"`
	DeviceID          string    `json:"DeviceId,omitempty"`
	GlobalNetworkID   string    `json:"GlobalNetworkId,omitempty"`
	LinkID            string    `json:"LinkId,omitempty"`
	Description       string    `json:"Description,omitempty"`
	State             string    `json:"State,omitempty"`
	Tags              []tags.KV `json:"Tags,omitempty"`
}

type connectionEnvelope struct {
	Connection *connectionWire `json:"Connection,omitempty"`
}

type createConnectionReq struct {
	ConnectedDeviceID string    `json:"ConnectedDeviceId"`
	DeviceID          string    `json:"DeviceId"`
	ConnectedLinkID   string    `json:"ConnectedLinkId,omitempty"`
	Description       string    `json:"Description,omitempty"`
	LinkID            string    `json:"LinkId,omitempty"`
	Tags              []tags.KV `json:"Tags,omitempty"`
}

type updateConnectionReq struct {
	ConnectedLinkID string `json:"ConnectedLinkId,omitempty"`
	Description     string `json:"Description,omitempty"`
	LinkID          string `json:"LinkId,omitempty"`
}

type getConnectionsResponse struct {
	NextToken   string           `json:"NextToken,omitempty"`
	Connections []connectionWire `json:"Connections"`
}

// ---- Customer Gateway Association ----

type customerGatewayAssociationWire struct {
	CustomerGatewayArn string `json:"CustomerGatewayArn,omitempty"`
	DeviceID           string `json:"DeviceId,omitempty"`
	GlobalNetworkID    string `json:"GlobalNetworkId,omitempty"`
	LinkID             string `json:"LinkId,omitempty"`
	State              string `json:"State,omitempty"`
}

type customerGatewayAssociationEnvelope struct {
	CustomerGatewayAssociation *customerGatewayAssociationWire `json:"CustomerGatewayAssociation,omitempty"`
}

type associateCustomerGatewayReq struct {
	CustomerGatewayArn string `json:"CustomerGatewayArn"`
	DeviceID           string `json:"DeviceId"`
	LinkID             string `json:"LinkId,omitempty"`
}

type getCustomerGatewayAssociationsResponse struct {
	NextToken                   string                           `json:"NextToken,omitempty"`
	CustomerGatewayAssociations []customerGatewayAssociationWire `json:"CustomerGatewayAssociations"`
}

// ---- Transit Gateway Registration ----

type transitGatewayRegistrationStateWire struct {
	Code    string `json:"Code,omitempty"`
	Message string `json:"Message,omitempty"`
}

type transitGatewayRegistrationWire struct {
	GlobalNetworkID   string                               `json:"GlobalNetworkId,omitempty"`
	State             *transitGatewayRegistrationStateWire `json:"State,omitempty"`
	TransitGatewayArn string                               `json:"TransitGatewayArn,omitempty"`
}

type transitGatewayRegistrationEnvelope struct {
	TransitGatewayRegistration *transitGatewayRegistrationWire `json:"TransitGatewayRegistration,omitempty"`
}

type registerTransitGatewayReq struct {
	TransitGatewayArn string `json:"TransitGatewayArn"`
}

type getTransitGatewayRegistrationsResponse struct {
	NextToken                   string                           `json:"NextToken,omitempty"`
	TransitGatewayRegistrations []transitGatewayRegistrationWire `json:"TransitGatewayRegistrations"`
}

// ---- Transit Gateway Connect Peer Association ----

type transitGatewayConnectPeerAssociationWire struct {
	DeviceID                     string `json:"DeviceId,omitempty"`
	GlobalNetworkID              string `json:"GlobalNetworkId,omitempty"`
	LinkID                       string `json:"LinkId,omitempty"`
	State                        string `json:"State,omitempty"`
	TransitGatewayConnectPeerArn string `json:"TransitGatewayConnectPeerArn,omitempty"`
}

type transitGatewayConnectPeerAssociationEnvelope struct {
	TransitGatewayConnectPeerAssociation *transitGatewayConnectPeerAssociationWire `json:"TransitGatewayConnectPeerAssociation,omitempty"` //nolint:lll // AWS SDK field name exceeds line limit
}

type associateTransitGatewayConnectPeerReq struct {
	DeviceID                     string `json:"DeviceId"`
	TransitGatewayConnectPeerArn string `json:"TransitGatewayConnectPeerArn"`
	LinkID                       string `json:"LinkId,omitempty"`
}

type getTransitGatewayConnectPeerAssociationsResponse struct {
	NextToken                             string                                     `json:"NextToken,omitempty"`
	TransitGatewayConnectPeerAssociations []transitGatewayConnectPeerAssociationWire `json:"TransitGatewayConnectPeerAssociations"` //nolint:lll // AWS SDK field name exceeds line limit
}

// ---- Connect Peer <-> Global Network association ----

type connectPeerAssociationWire struct {
	ConnectPeerID   string `json:"ConnectPeerId,omitempty"`
	DeviceID        string `json:"DeviceId,omitempty"`
	GlobalNetworkID string `json:"GlobalNetworkId,omitempty"`
	LinkID          string `json:"LinkId,omitempty"`
	State           string `json:"State,omitempty"`
}

type connectPeerAssociationEnvelope struct {
	ConnectPeerAssociation *connectPeerAssociationWire `json:"ConnectPeerAssociation,omitempty"`
}

type associateConnectPeerReq struct {
	ConnectPeerID string `json:"ConnectPeerId"`
	DeviceID      string `json:"DeviceId"`
	LinkID        string `json:"LinkId,omitempty"`
}

type getConnectPeerAssociationsResponse struct {
	NextToken               string                       `json:"NextToken,omitempty"`
	ConnectPeerAssociations []connectPeerAssociationWire `json:"ConnectPeerAssociations"`
}

// ---- Connect Peer (Cloud WAN) ----

type bgpOptionsWire struct {
	PeerAsn int64 `json:"PeerAsn,omitempty"`
}

type connectPeerBgpConfigurationWire struct {
	CoreNetworkAddress string `json:"CoreNetworkAddress,omitempty"`
	PeerAddress        string `json:"PeerAddress,omitempty"`
	CoreNetworkAsn     int64  `json:"CoreNetworkAsn,omitempty"`
	PeerAsn            int64  `json:"PeerAsn,omitempty"`
}

type connectPeerConfigurationWire struct {
	CoreNetworkAddress string                            `json:"CoreNetworkAddress,omitempty"`
	PeerAddress        string                            `json:"PeerAddress,omitempty"`
	Protocol           string                            `json:"Protocol,omitempty"`
	BgpConfigurations  []connectPeerBgpConfigurationWire `json:"BgpConfigurations,omitempty"`
	InsideCidrBlocks   []string                          `json:"InsideCidrBlocks,omitempty"`
}

type connectPeerErrorWire struct {
	Code        string `json:"Code,omitempty"`
	Message     string `json:"Message,omitempty"`
	RequestID   string `json:"RequestId,omitempty"`
	ResourceArn string `json:"ResourceArn,omitempty"`
}

type connectPeerWire struct {
	CreatedAt              *float64                      `json:"CreatedAt,omitempty"`
	Configuration          *connectPeerConfigurationWire `json:"Configuration,omitempty"`
	ConnectAttachmentID    string                        `json:"ConnectAttachmentId,omitempty"`
	ConnectPeerID          string                        `json:"ConnectPeerId,omitempty"`
	CoreNetworkID          string                        `json:"CoreNetworkId,omitempty"`
	EdgeLocation           string                        `json:"EdgeLocation,omitempty"`
	State                  string                        `json:"State,omitempty"`
	SubnetArn              string                        `json:"SubnetArn,omitempty"`
	Tags                   []tags.KV                     `json:"Tags,omitempty"`
	LastModificationErrors []connectPeerErrorWire        `json:"LastModificationErrors,omitempty"`
}

type connectPeerEnvelope struct {
	ConnectPeer *connectPeerWire `json:"ConnectPeer,omitempty"`
}

type createConnectPeerReq struct {
	ConnectAttachmentID string          `json:"ConnectAttachmentId"`
	PeerAddress         string          `json:"PeerAddress"`
	BgpOptions          *bgpOptionsWire `json:"BgpOptions,omitempty"`
	CoreNetworkAddress  string          `json:"CoreNetworkAddress,omitempty"`
	SubnetArn           string          `json:"SubnetArn,omitempty"`
	InsideCidrBlocks    []string        `json:"InsideCidrBlocks,omitempty"`
	Tags                []tags.KV       `json:"Tags,omitempty"`
}

type connectPeerSummaryWire struct {
	CreatedAt           *float64  `json:"CreatedAt,omitempty"`
	ConnectAttachmentID string    `json:"ConnectAttachmentId,omitempty"`
	ConnectPeerID       string    `json:"ConnectPeerId,omitempty"`
	ConnectPeerState    string    `json:"ConnectPeerState,omitempty"`
	CoreNetworkID       string    `json:"CoreNetworkId,omitempty"`
	EdgeLocation        string    `json:"EdgeLocation,omitempty"`
	SubnetArn           string    `json:"SubnetArn,omitempty"`
	Tags                []tags.KV `json:"Tags,omitempty"`
}

type listConnectPeersResponse struct {
	NextToken    string                   `json:"NextToken,omitempty"`
	ConnectPeers []connectPeerSummaryWire `json:"ConnectPeers"`
}

// ---- Core Network ----

type coreNetworkEdgeWire struct {
	EdgeLocation     string   `json:"EdgeLocation,omitempty"`
	InsideCidrBlocks []string `json:"InsideCidrBlocks,omitempty"`
	Asn              int64    `json:"Asn,omitempty"`
}

type coreNetworkSegmentWire struct {
	EdgeLocations  []string `json:"EdgeLocations,omitempty"`
	Name           string   `json:"Name,omitempty"`
	SharedSegments []string `json:"SharedSegments,omitempty"`
}

type coreNetworkWire struct {
	CreatedAt             *float64                 `json:"CreatedAt,omitempty"`
	CoreNetworkArn        string                   `json:"CoreNetworkArn,omitempty"`
	CoreNetworkID         string                   `json:"CoreNetworkId,omitempty"`
	GlobalNetworkID       string                   `json:"GlobalNetworkId,omitempty"`
	Description           string                   `json:"Description,omitempty"`
	State                 string                   `json:"State,omitempty"`
	Tags                  []tags.KV                `json:"Tags,omitempty"`
	Edges                 []coreNetworkEdgeWire    `json:"Edges,omitempty"`
	Segments              []coreNetworkSegmentWire `json:"Segments,omitempty"`
	NetworkFunctionGroups []struct{}               `json:"NetworkFunctionGroups,omitempty"`
}

type coreNetworkEnvelope struct {
	CoreNetwork *coreNetworkWire `json:"CoreNetwork,omitempty"`
}

type createCoreNetworkReq struct {
	GlobalNetworkID string    `json:"GlobalNetworkId"`
	Description     string    `json:"Description,omitempty"`
	PolicyDocument  string    `json:"PolicyDocument,omitempty"`
	Tags            []tags.KV `json:"Tags,omitempty"`
}

type updateCoreNetworkReq struct {
	Description string `json:"Description,omitempty"`
}

type coreNetworkSummaryWire struct {
	CoreNetworkArn  string    `json:"CoreNetworkArn,omitempty"`
	CoreNetworkID   string    `json:"CoreNetworkId,omitempty"`
	Description     string    `json:"Description,omitempty"`
	GlobalNetworkID string    `json:"GlobalNetworkId,omitempty"`
	OwnerAccountID  string    `json:"OwnerAccountId,omitempty"`
	State           string    `json:"State,omitempty"`
	Tags            []tags.KV `json:"Tags,omitempty"`
}

type listCoreNetworksResponse struct {
	NextToken    string                   `json:"NextToken,omitempty"`
	CoreNetworks []coreNetworkSummaryWire `json:"CoreNetworks"`
}

// ---- Core Network Policy lifecycle ----

type coreNetworkPolicyErrorWire struct {
	ErrorCode string `json:"ErrorCode"`
	Message   string `json:"Message"`
	Path      string `json:"Path,omitempty"`
}

type coreNetworkPolicyWire struct {
	CreatedAt       *float64                     `json:"CreatedAt,omitempty"`
	Alias           string                       `json:"Alias,omitempty"`
	ChangeSetState  string                       `json:"ChangeSetState,omitempty"`
	CoreNetworkID   string                       `json:"CoreNetworkId,omitempty"`
	Description     string                       `json:"Description,omitempty"`
	PolicyDocument  string                       `json:"PolicyDocument,omitempty"`
	PolicyErrors    []coreNetworkPolicyErrorWire `json:"PolicyErrors,omitempty"`
	PolicyVersionID int32                        `json:"PolicyVersionId,omitempty"`
}

type coreNetworkPolicyEnvelope struct {
	CoreNetworkPolicy *coreNetworkPolicyWire `json:"CoreNetworkPolicy,omitempty"`
}

type putCoreNetworkPolicyReq struct {
	PolicyDocument  string `json:"PolicyDocument"`
	Description     string `json:"Description,omitempty"`
	LatestVersionID int32  `json:"LatestVersionId,omitempty"`
}

type coreNetworkPolicyVersionWire struct {
	CreatedAt       *float64 `json:"CreatedAt,omitempty"`
	Alias           string   `json:"Alias,omitempty"`
	ChangeSetState  string   `json:"ChangeSetState,omitempty"`
	CoreNetworkID   string   `json:"CoreNetworkId,omitempty"`
	Description     string   `json:"Description,omitempty"`
	PolicyVersionID int32    `json:"PolicyVersionId,omitempty"`
}

type listCoreNetworkPolicyVersionsResponse struct {
	NextToken                 string                         `json:"NextToken,omitempty"`
	CoreNetworkPolicyVersions []coreNetworkPolicyVersionWire `json:"CoreNetworkPolicyVersions"`
}

type coreNetworkChangeValuesWire struct {
	SegmentName              string `json:"SegmentName,omitempty"`
	NetworkFunctionGroupName string `json:"NetworkFunctionGroupName,omitempty"`
}

type coreNetworkChangeWire struct {
	NewValues      *coreNetworkChangeValuesWire `json:"NewValues,omitempty"`
	PreviousValues *coreNetworkChangeValuesWire `json:"PreviousValues,omitempty"`
	Action         string                       `json:"Action,omitempty"`
	Identifier     string                       `json:"Identifier,omitempty"`
	IdentifierPath string                       `json:"IdentifierPath,omitempty"`
	Type           string                       `json:"Type,omitempty"`
}

type getCoreNetworkChangeSetResponse struct {
	NextToken          string                  `json:"NextToken,omitempty"`
	CoreNetworkChanges []coreNetworkChangeWire `json:"CoreNetworkChanges"`
}

type coreNetworkChangeEventWire struct {
	Values         *coreNetworkChangeValuesWire `json:"Values,omitempty"`
	EventTime      *float64                     `json:"EventTime,omitempty"`
	Action         string                       `json:"Action,omitempty"`
	IdentifierPath string                       `json:"IdentifierPath,omitempty"`
	Status         string                       `json:"Status,omitempty"`
	Type           string                       `json:"Type,omitempty"`
}

type getCoreNetworkChangeEventsResponse struct {
	NextToken               string                       `json:"NextToken,omitempty"`
	CoreNetworkChangeEvents []coreNetworkChangeEventWire `json:"CoreNetworkChangeEvents"`
}

// ---- Core Network Prefix List Association ----

type prefixListAssociationWire struct {
	CoreNetworkID   string `json:"CoreNetworkId,omitempty"`
	PrefixListAlias string `json:"PrefixListAlias,omitempty"`
	PrefixListArn   string `json:"PrefixListArn,omitempty"`
}

type createCoreNetworkPrefixListAssociationReq struct {
	CoreNetworkID   string `json:"CoreNetworkId"`
	PrefixListAlias string `json:"PrefixListAlias"`
	PrefixListArn   string `json:"PrefixListArn"`
}

type listCoreNetworkPrefixListAssociationsResponse struct {
	NextToken              string                      `json:"NextToken,omitempty"`
	PrefixListAssociations []prefixListAssociationWire `json:"PrefixListAssociations"`
}

// ---- Core Network Routing Information ----

type listCoreNetworkRoutingInformationReq struct {
	NextHopFilters         map[string][]string `json:"NextHopFilters,omitempty"`
	EdgeLocation           string              `json:"EdgeLocation"`
	SegmentName            string              `json:"SegmentName"`
	NextToken              string              `json:"NextToken,omitempty"`
	CommunityMatches       []string            `json:"CommunityMatches,omitempty"`
	ExactAsPathMatches     []string            `json:"ExactAsPathMatches,omitempty"`
	LocalPreferenceMatches []string            `json:"LocalPreferenceMatches,omitempty"`
	MedMatches             []string            `json:"MedMatches,omitempty"`
	MaxResults             int32               `json:"MaxResults,omitempty"`
}

type listCoreNetworkRoutingInformationResponse struct {
	NextToken                     string     `json:"NextToken,omitempty"`
	CoreNetworkRoutingInformation []struct{} `json:"CoreNetworkRoutingInformation"`
}

// ---- Attachment Routing Policy ----

type putAttachmentRoutingPolicyLabelReq struct {
	AttachmentID       string `json:"AttachmentId"`
	CoreNetworkID      string `json:"CoreNetworkId"`
	RoutingPolicyLabel string `json:"RoutingPolicyLabel"`
	ClientToken        string `json:"ClientToken,omitempty"`
}

type routingPolicyLabelResponse struct {
	AttachmentID       string `json:"AttachmentId,omitempty"`
	CoreNetworkID      string `json:"CoreNetworkId,omitempty"`
	RoutingPolicyLabel string `json:"RoutingPolicyLabel,omitempty"`
}

type attachmentRoutingPolicyAssociationSummaryWire struct {
	AttachmentID              string   `json:"AttachmentId,omitempty"`
	RoutingPolicyLabel        string   `json:"RoutingPolicyLabel,omitempty"`
	AssociatedRoutingPolicies []string `json:"AssociatedRoutingPolicies,omitempty"`
	PendingRoutingPolicies    []string `json:"PendingRoutingPolicies,omitempty"`
}

type listAttachmentRoutingPolicyAssociationsResponse struct {
	NextToken                           string                                          `json:"NextToken,omitempty"`
	AttachmentRoutingPolicyAssociations []attachmentRoutingPolicyAssociationSummaryWire `json:"AttachmentRoutingPolicyAssociations"` //nolint:lll // AWS SDK field name exceeds line limit
}

// ---- Attachment (generic + 5 subtypes) ----

type attachmentErrorWire struct {
	Code        string `json:"Code,omitempty"`
	Message     string `json:"Message,omitempty"`
	RequestID   string `json:"RequestId,omitempty"`
	ResourceArn string `json:"ResourceArn,omitempty"`
}

type attachmentWire struct {
	CreatedAt                  *float64              `json:"CreatedAt,omitempty"`
	UpdatedAt                  *float64              `json:"UpdatedAt,omitempty"`
	ResourceArn                string                `json:"ResourceArn,omitempty"`
	EdgeLocation               string                `json:"EdgeLocation,omitempty"`
	State                      string                `json:"State,omitempty"`
	AttachmentID               string                `json:"AttachmentId,omitempty"`
	AttachmentType             string                `json:"AttachmentType,omitempty"`
	CoreNetworkArn             string                `json:"CoreNetworkArn,omitempty"`
	CoreNetworkID              string                `json:"CoreNetworkId,omitempty"`
	SegmentName                string                `json:"SegmentName,omitempty"`
	NetworkFunctionGroupName   string                `json:"NetworkFunctionGroupName,omitempty"`
	OwnerAccountID             string                `json:"OwnerAccountId,omitempty"`
	Tags                       []tags.KV             `json:"Tags,omitempty"`
	LastModificationErrors     []attachmentErrorWire `json:"LastModificationErrors,omitempty"`
	EdgeLocations              []string              `json:"EdgeLocations,omitempty"`
	AttachmentPolicyRuleNumber int32                 `json:"AttachmentPolicyRuleNumber,omitempty"`
}

type attachmentEnvelope struct {
	Attachment *attachmentWire `json:"Attachment,omitempty"`
}

type listAttachmentsResponse struct {
	NextToken   string           `json:"NextToken,omitempty"`
	Attachments []attachmentWire `json:"Attachments"`
}

// ---- VPC Attachment ----

type vpcOptionsWire struct {
	ApplianceModeSupport            bool `json:"ApplianceModeSupport,omitempty"`
	DNSSupport                      bool `json:"DnsSupport,omitempty"`
	Ipv6Support                     bool `json:"Ipv6Support,omitempty"`
	SecurityGroupReferencingSupport bool `json:"SecurityGroupReferencingSupport,omitempty"`
}

type vpcAttachmentWire struct {
	Attachment *attachmentWire `json:"Attachment,omitempty"`
	Options    *vpcOptionsWire `json:"Options,omitempty"`
	SubnetArns []string        `json:"SubnetArns,omitempty"`
}

type vpcAttachmentEnvelope struct {
	VpcAttachment *vpcAttachmentWire `json:"VpcAttachment,omitempty"`
}

type createVpcAttachmentReq struct {
	CoreNetworkID      string          `json:"CoreNetworkId"`
	VpcArn             string          `json:"VpcArn"`
	SubnetArns         []string        `json:"SubnetArns"`
	Options            *vpcOptionsWire `json:"Options,omitempty"`
	RoutingPolicyLabel string          `json:"RoutingPolicyLabel,omitempty"`
	Tags               []tags.KV       `json:"Tags,omitempty"`
}

type updateVpcAttachmentReq struct {
	Options          *vpcOptionsWire `json:"Options,omitempty"`
	AddSubnetArns    []string        `json:"AddSubnetArns,omitempty"`
	RemoveSubnetArns []string        `json:"RemoveSubnetArns,omitempty"`
}

// ---- Connect Attachment ----

type connectAttachmentOptionsWire struct {
	Protocol string `json:"Protocol,omitempty"`
}

type connectAttachmentWire struct {
	Attachment            *attachmentWire               `json:"Attachment,omitempty"`
	Options               *connectAttachmentOptionsWire `json:"Options,omitempty"`
	TransportAttachmentID string                        `json:"TransportAttachmentId,omitempty"`
}

type connectAttachmentEnvelope struct {
	ConnectAttachment *connectAttachmentWire `json:"ConnectAttachment,omitempty"`
}

type createConnectAttachmentReq struct {
	CoreNetworkID         string                        `json:"CoreNetworkId"`
	EdgeLocation          string                        `json:"EdgeLocation"`
	Options               *connectAttachmentOptionsWire `json:"Options"`
	TransportAttachmentID string                        `json:"TransportAttachmentId"`
	RoutingPolicyLabel    string                        `json:"RoutingPolicyLabel,omitempty"`
	Tags                  []tags.KV                     `json:"Tags,omitempty"`
}

// ---- Site-to-Site VPN Attachment ----

type siteToSiteVpnAttachmentWire struct {
	Attachment       *attachmentWire `json:"Attachment,omitempty"`
	VpnConnectionArn string          `json:"VpnConnectionArn,omitempty"`
}

type siteToSiteVpnAttachmentEnvelope struct {
	SiteToSiteVpnAttachment *siteToSiteVpnAttachmentWire `json:"SiteToSiteVpnAttachment,omitempty"`
}

type createSiteToSiteVpnAttachmentReq struct {
	CoreNetworkID      string    `json:"CoreNetworkId"`
	VpnConnectionArn   string    `json:"VpnConnectionArn"`
	RoutingPolicyLabel string    `json:"RoutingPolicyLabel,omitempty"`
	Tags               []tags.KV `json:"Tags,omitempty"`
}

// ---- Direct Connect Gateway Attachment ----

type directConnectGatewayAttachmentWire struct {
	Attachment              *attachmentWire `json:"Attachment,omitempty"`
	DirectConnectGatewayArn string          `json:"DirectConnectGatewayArn,omitempty"`
}

type directConnectGatewayAttachmentEnvelope struct {
	DirectConnectGatewayAttachment *directConnectGatewayAttachmentWire `json:"DirectConnectGatewayAttachment,omitempty"`
}

type createDirectConnectGatewayAttachmentReq struct {
	CoreNetworkID           string    `json:"CoreNetworkId"`
	DirectConnectGatewayArn string    `json:"DirectConnectGatewayArn"`
	EdgeLocations           []string  `json:"EdgeLocations"`
	RoutingPolicyLabel      string    `json:"RoutingPolicyLabel,omitempty"`
	Tags                    []tags.KV `json:"Tags,omitempty"`
}

type updateDirectConnectGatewayAttachmentReq struct {
	EdgeLocations []string `json:"EdgeLocations,omitempty"`
}

// ---- Transit Gateway Route Table Attachment ----

type transitGatewayRouteTableAttachmentWire struct {
	Attachment                  *attachmentWire `json:"Attachment,omitempty"`
	PeeringID                   string          `json:"PeeringId,omitempty"`
	TransitGatewayRouteTableArn string          `json:"TransitGatewayRouteTableArn,omitempty"`
}

type transitGatewayRouteTableAttachmentEnvelope struct {
	TransitGatewayRouteTableAttachment *transitGatewayRouteTableAttachmentWire `json:"TransitGatewayRouteTableAttachment,omitempty"` //nolint:lll // AWS SDK field name exceeds line limit
}

type createTransitGatewayRouteTableAttachmentReq struct {
	PeeringID                   string    `json:"PeeringId"`
	TransitGatewayRouteTableArn string    `json:"TransitGatewayRouteTableArn"`
	RoutingPolicyLabel          string    `json:"RoutingPolicyLabel,omitempty"`
	Tags                        []tags.KV `json:"Tags,omitempty"`
}

// ---- Peerings ----

type peeringErrorWire struct {
	Code        string `json:"Code,omitempty"`
	Message     string `json:"Message,omitempty"`
	RequestID   string `json:"RequestId,omitempty"`
	ResourceArn string `json:"ResourceArn,omitempty"`
}

type peeringWire struct {
	CreatedAt              *float64           `json:"CreatedAt,omitempty"`
	CoreNetworkArn         string             `json:"CoreNetworkArn,omitempty"`
	CoreNetworkID          string             `json:"CoreNetworkId,omitempty"`
	EdgeLocation           string             `json:"EdgeLocation,omitempty"`
	OwnerAccountID         string             `json:"OwnerAccountId,omitempty"`
	PeeringID              string             `json:"PeeringId,omitempty"`
	PeeringType            string             `json:"PeeringType,omitempty"`
	ResourceArn            string             `json:"ResourceArn,omitempty"`
	State                  string             `json:"State,omitempty"`
	Tags                   []tags.KV          `json:"Tags,omitempty"`
	LastModificationErrors []peeringErrorWire `json:"LastModificationErrors,omitempty"`
}

type peeringEnvelope struct {
	Peering *peeringWire `json:"Peering,omitempty"`
}

type transitGatewayPeeringWire struct {
	Peering                           *peeringWire `json:"Peering,omitempty"`
	TransitGatewayArn                 string       `json:"TransitGatewayArn,omitempty"`
	TransitGatewayPeeringAttachmentID string       `json:"TransitGatewayPeeringAttachmentId,omitempty"`
}

type transitGatewayPeeringEnvelope struct {
	TransitGatewayPeering *transitGatewayPeeringWire `json:"TransitGatewayPeering,omitempty"`
}

type createTransitGatewayPeeringReq struct {
	CoreNetworkID     string    `json:"CoreNetworkId"`
	TransitGatewayArn string    `json:"TransitGatewayArn"`
	Tags              []tags.KV `json:"Tags,omitempty"`
}

type listPeeringsResponse struct {
	NextToken string        `json:"NextToken,omitempty"`
	Peerings  []peeringWire `json:"Peerings"`
}

// ---- Route Analysis ----

type routeAnalysisEndpointSpecWire struct {
	IPAddress                   string `json:"IpAddress,omitempty"`
	TransitGatewayAttachmentArn string `json:"TransitGatewayAttachmentArn,omitempty"`
}

type routeAnalysisEndpointWire struct {
	IPAddress                   string `json:"IpAddress,omitempty"`
	TransitGatewayArn           string `json:"TransitGatewayArn,omitempty"`
	TransitGatewayAttachmentArn string `json:"TransitGatewayAttachmentArn,omitempty"`
}

type routeAnalysisCompletionWire struct {
	ReasonCode string `json:"ReasonCode,omitempty"`
	ResultCode string `json:"ResultCode,omitempty"`
}

type routeAnalysisPathWire struct {
	CompletionStatus *routeAnalysisCompletionWire `json:"CompletionStatus,omitempty"`
	Path             []pathComponentWire          `json:"Path,omitempty"`
}

type networkResourceSummaryWire struct {
	Definition           string `json:"Definition,omitempty"`
	NameTag              string `json:"NameTag,omitempty"`
	RegisteredGatewayArn string `json:"RegisteredGatewayArn,omitempty"`
	ResourceArn          string `json:"ResourceArn,omitempty"`
	ResourceType         string `json:"ResourceType,omitempty"`
	IsMiddlebox          bool   `json:"IsMiddlebox,omitempty"`
}

type pathComponentWire struct {
	Resource             *networkResourceSummaryWire `json:"Resource,omitempty"`
	DestinationCidrBlock string                      `json:"DestinationCidrBlock,omitempty"`
	Sequence             int32                       `json:"Sequence,omitempty"`
}

type routeAnalysisWire struct {
	StartTimestamp    *float64                   `json:"StartTimestamp,omitempty"`
	Destination       *routeAnalysisEndpointWire `json:"Destination,omitempty"`
	Source            *routeAnalysisEndpointWire `json:"Source,omitempty"`
	ForwardPath       *routeAnalysisPathWire     `json:"ForwardPath,omitempty"`
	ReturnPath        *routeAnalysisPathWire     `json:"ReturnPath,omitempty"`
	GlobalNetworkID   string                     `json:"GlobalNetworkId,omitempty"`
	OwnerAccountID    string                     `json:"OwnerAccountId,omitempty"`
	RouteAnalysisID   string                     `json:"RouteAnalysisId,omitempty"`
	Status            string                     `json:"Status,omitempty"`
	IncludeReturnPath bool                       `json:"IncludeReturnPath,omitempty"`
	UseMiddleboxes    bool                       `json:"UseMiddleboxes,omitempty"`
}

type routeAnalysisEnvelope struct {
	RouteAnalysis *routeAnalysisWire `json:"RouteAnalysis,omitempty"`
}

type startRouteAnalysisReq struct {
	Destination       *routeAnalysisEndpointSpecWire `json:"Destination"`
	Source            *routeAnalysisEndpointSpecWire `json:"Source"`
	IncludeReturnPath bool                           `json:"IncludeReturnPath,omitempty"`
	UseMiddleboxes    bool                           `json:"UseMiddleboxes,omitempty"`
}

// ---- Network introspection ----

type networkResourceWire struct {
	DefinitionTimestamp  *float64          `json:"DefinitionTimestamp,omitempty"`
	Metadata             map[string]string `json:"Metadata,omitempty"`
	AccountID            string            `json:"AccountId,omitempty"`
	AwsRegion            string            `json:"AwsRegion,omitempty"`
	CoreNetworkID        string            `json:"CoreNetworkId,omitempty"`
	Definition           string            `json:"Definition,omitempty"`
	RegisteredGatewayArn string            `json:"RegisteredGatewayArn,omitempty"`
	ResourceArn          string            `json:"ResourceArn,omitempty"`
	ResourceID           string            `json:"ResourceId,omitempty"`
	ResourceType         string            `json:"ResourceType,omitempty"`
	Tags                 []tags.KV         `json:"Tags,omitempty"`
}

type getNetworkResourcesResponse struct {
	NextToken        string                `json:"NextToken,omitempty"`
	NetworkResources []networkResourceWire `json:"NetworkResources"`
}

type networkResourceCountWire struct {
	ResourceType string `json:"ResourceType,omitempty"`
	Count        int32  `json:"Count,omitempty"`
}

type getNetworkResourceCountsResponse struct {
	NextToken             string                     `json:"NextToken,omitempty"`
	NetworkResourceCounts []networkResourceCountWire `json:"NetworkResourceCounts"`
}

type relationshipWire struct {
	From string `json:"From,omitempty"`
	To   string `json:"To,omitempty"`
}

type getNetworkResourceRelationshipsResponse struct {
	NextToken     string             `json:"NextToken,omitempty"`
	Relationships []relationshipWire `json:"Relationships"`
}

type coreNetworkNetworkFunctionGroupIdentifierWire struct {
	CoreNetworkID            string `json:"CoreNetworkId,omitempty"`
	EdgeLocation             string `json:"EdgeLocation,omitempty"`
	NetworkFunctionGroupName string `json:"NetworkFunctionGroupName,omitempty"`
}

type coreNetworkSegmentEdgeIdentifierWire struct {
	CoreNetworkID string `json:"CoreNetworkId,omitempty"`
	EdgeLocation  string `json:"EdgeLocation,omitempty"`
	SegmentName   string `json:"SegmentName,omitempty"`
}

type routeTableIdentifierWire struct {
	CoreNetworkNetworkFunctionGroup *coreNetworkNetworkFunctionGroupIdentifierWire `json:"CoreNetworkNetworkFunctionGroup,omitempty"` //nolint:lll // AWS SDK field name exceeds line limit
	CoreNetworkSegmentEdge          *coreNetworkSegmentEdgeIdentifierWire          `json:"CoreNetworkSegmentEdge,omitempty"`          //nolint:lll // AWS SDK field name exceeds line limit
	TransitGatewayRouteTableArn     string                                         `json:"TransitGatewayRouteTableArn,omitempty"`     //nolint:lll // AWS SDK field name exceeds line limit
}

type getNetworkRoutesReq struct {
	RouteTableIdentifier *routeTableIdentifierWire `json:"RouteTableIdentifier"`
	DestinationFilters   map[string][]string       `json:"DestinationFilters,omitempty"`
	ExactCidrMatches     []string                  `json:"ExactCidrMatches,omitempty"`
	LongestPrefixMatches []string                  `json:"LongestPrefixMatches,omitempty"`
	PrefixListIDs        []string                  `json:"PrefixListIds,omitempty"`
	States               []string                  `json:"States,omitempty"`
	SubnetOfMatches      []string                  `json:"SubnetOfMatches,omitempty"`
	SupernetOfMatches    []string                  `json:"SupernetOfMatches,omitempty"`
	Types                []string                  `json:"Types,omitempty"`
}

type getNetworkRoutesResponse struct {
	CoreNetworkSegmentEdge *coreNetworkSegmentEdgeIdentifierWire `json:"CoreNetworkSegmentEdge,omitempty"`
	RouteTableTimestamp    *float64                              `json:"RouteTableTimestamp,omitempty"`
	RouteTableArn          string                                `json:"RouteTableArn,omitempty"`
	RouteTableType         string                                `json:"RouteTableType,omitempty"`
	NetworkRoutes          []struct{}                            `json:"NetworkRoutes"`
}

type connectionHealthWire struct {
	Status    string   `json:"Status,omitempty"`
	Timestamp *float64 `json:"Timestamp,omitempty"`
	Type      string   `json:"Type,omitempty"`
}

type networkTelemetryWire struct {
	Health               *connectionHealthWire `json:"Health,omitempty"`
	AccountID            string                `json:"AccountId,omitempty"`
	Address              string                `json:"Address,omitempty"`
	AwsRegion            string                `json:"AwsRegion,omitempty"`
	CoreNetworkID        string                `json:"CoreNetworkId,omitempty"`
	RegisteredGatewayArn string                `json:"RegisteredGatewayArn,omitempty"`
	ResourceArn          string                `json:"ResourceArn,omitempty"`
	ResourceID           string                `json:"ResourceId,omitempty"`
	ResourceType         string                `json:"ResourceType,omitempty"`
}

type getNetworkTelemetryResponse struct {
	NextToken        string                 `json:"NextToken,omitempty"`
	NetworkTelemetry []networkTelemetryWire `json:"NetworkTelemetry"`
}

// ---- Update network resource metadata ----

type updateNetworkResourceMetadataReq struct {
	Metadata map[string]string `json:"Metadata"`
}

type updateNetworkResourceMetadataResponse struct {
	Metadata    map[string]string `json:"Metadata,omitempty"`
	ResourceArn string            `json:"ResourceArn,omitempty"`
}

// ---- Organizations integration ----

type accountStatusWire struct {
	AccountID           string `json:"AccountId,omitempty"`
	SLRDeploymentStatus string `json:"SLRDeploymentStatus,omitempty"`
}

type organizationStatusWire struct {
	OrganizationAwsServiceAccessStatus string              `json:"OrganizationAwsServiceAccessStatus,omitempty"`
	OrganizationID                     string              `json:"OrganizationId,omitempty"`
	SLRDeploymentStatus                string              `json:"SLRDeploymentStatus,omitempty"`
	AccountStatusList                  []accountStatusWire `json:"AccountStatusList,omitempty"`
}

type organizationStatusEnvelope struct {
	OrganizationStatus *organizationStatusWire `json:"OrganizationStatus,omitempty"`
}

type startOrganizationServiceAccessUpdateReq struct {
	Action string `json:"Action"`
}

type listOrganizationServiceAccessStatusResponse struct {
	OrganizationStatus *organizationStatusWire `json:"OrganizationStatus,omitempty"`
	NextToken          string                  `json:"NextToken,omitempty"`
}

// ---- Resource policy ----

type putResourcePolicyReq struct {
	PolicyDocument string `json:"PolicyDocument"`
}

type getResourcePolicyResponse struct {
	PolicyDocument string `json:"PolicyDocument,omitempty"`
}

// ---- Tagging ----

type tagResourceReq struct {
	Tags []tags.KV `json:"Tags"`
}

type listTagsForResourceResponse struct {
	TagList []tags.KV `json:"TagList"`
}
