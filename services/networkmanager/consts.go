package networkmanager

import "time"

// asyncTransitionDelay is the simulated delay between a resource's initial
// PENDING/CREATING state and its terminal AVAILABLE state (or, for deletes,
// between DELETING and actual removal from the backend's tables) -- matches
// services/mgn/services/directconnect/services/resiliencehub/services/
// outposts/services/grafana's identically-named constant and its
// worker.Group.After-driven state-advance idiom (see store.go's
// scheduleAdvance/scheduleRemoval helpers).
const asyncTransitionDelay = 100 * time.Millisecond

// defaultPageLimit is the page size used by every List/Get/Describe
// operation when the caller omits MaxResults, matching this campaign's
// convention.
const defaultPageLimit = 100

// ---- GlobalNetworkState / SiteState / DeviceState / LinkState / ConnectionState ----
// All five are byte-identical 4-value shapes (types.GlobalNetworkState etc,
// PARITY.md family A-F).
const (
	statePending   = "PENDING"
	stateAvailable = "AVAILABLE"
	stateDeleting  = "DELETING"
	stateUpdating  = "UPDATING"
)

// ---- LinkAssociationState / CustomerGatewayAssociationState /
// TransitGatewayConnectPeerAssociationState / ConnectPeerAssociationState ----
// All four are byte-identical 4-value shapes ending in DELETED rather than
// looping back to absent (types.LinkAssociationState etc, PARITY.md
// families E/G/I/J).
const (
	assocStatePending   = "PENDING"
	assocStateAvailable = "AVAILABLE"
	assocStateDeleting  = "DELETING"
	assocStateDeleted   = "DELETED"
)

// TransitGatewayRegistrationState wire values -- the only registration/
// association state enum in this service with a 5th, terminal FAILED value
// (types.TransitGatewayRegistrationState, PARITY.md family H).
const (
	tgwRegStatePending   = "PENDING"
	tgwRegStateAvailable = "AVAILABLE"
	tgwRegStateDeleting  = "DELETING"
	tgwRegStateDeleted   = "DELETED"
	tgwRegStateFailed    = "FAILED"
)

// ConnectPeerState wire values -- no PENDING, only 4 values
// (types.ConnectPeerState, PARITY.md family K).
const (
	connectPeerStateCreating  = "CREATING"
	connectPeerStateFailed    = "FAILED"
	connectPeerStateAvailable = "AVAILABLE"
	connectPeerStateDeleting  = "DELETING"
)

// CoreNetworkState wire values (types.CoreNetworkState, PARITY.md family L).
const (
	coreNetworkStateCreating  = "CREATING"
	coreNetworkStateUpdating  = "UPDATING"
	coreNetworkStateAvailable = "AVAILABLE"
	coreNetworkStateDeleting  = "DELETING"
)

// CoreNetworkPolicyAlias wire values -- exactly 2 (types.CoreNetworkPolicyAlias,
// PARITY.md family M).
const (
	policyAliasLive   = "LIVE"
	policyAliasLatest = "LATEST"
)

// ChangeSetState wire values -- 6 values, the versioned-policy state machine
// (types.ChangeSetState, PARITY.md family M).
const (
	changeSetStatePendingGeneration  = "PENDING_GENERATION"
	changeSetStateReadyToExecute     = "READY_TO_EXECUTE"
	changeSetStateExecuting          = "EXECUTING"
	changeSetStateExecutionSucceeded = "EXECUTION_SUCCEEDED"
)

// PeeringState wire values -- no PENDING, matching ConnectPeerState's shape
// (types.PeeringState, PARITY.md family R).
const (
	peeringStateCreating  = "CREATING"
	peeringStateAvailable = "AVAILABLE"
	peeringStateDeleting  = "DELETING"
)

// PeeringType wire values -- exactly 1 value today (types.PeeringType).
const peeringTypeTransitGateway = "TRANSIT_GATEWAY"

// AttachmentState wire values -- 9 values, the richest state enum in this
// service (types.AttachmentState, PARITY.md family Q).
const (
	attachmentStateRejected                    = "REJECTED"
	attachmentStatePendingAttachmentAcceptance = "PENDING_ATTACHMENT_ACCEPTANCE"
	attachmentStateCreating                    = "CREATING"
	attachmentStateAvailable                   = "AVAILABLE"
	attachmentStateDeleting                    = "DELETING"
)

// AttachmentType wire values (types.AttachmentType, PARITY.md family Q1-Q5).
const (
	attachmentTypeConnect                  = "CONNECT"
	attachmentTypeSiteToSiteVpn            = "SITE_TO_SITE_VPN"
	attachmentTypeVpc                      = "VPC"
	attachmentTypeDirectConnectGateway     = "DIRECT_CONNECT_GATEWAY"
	attachmentTypeTransitGatewayRouteTable = "TRANSIT_GATEWAY_ROUTE_TABLE"
)

// TunnelProtocol wire values (types.TunnelProtocol).
const (
	tunnelProtocolGre     = "GRE"
	tunnelProtocolNoEncap = "NO_ENCAP"
)

// RouteAnalysisStatus wire values (types.RouteAnalysisStatus, PARITY.md family S).
const (
	routeAnalysisStatusRunning   = "RUNNING"
	routeAnalysisStatusCompleted = "COMPLETED"
)

// RouteAnalysisCompletionResultCode wire values -- exactly 2
// (types.RouteAnalysisCompletionResultCode).
const (
	routeAnalysisResultConnected    = "CONNECTED"
	routeAnalysisResultNotConnected = "NOT_CONNECTED"
)

// routeAnalysisReasonAttachmentNotFound is the honest, deterministic reason
// code this backend always returns for a completed route analysis -- see
// routeanalysis.go's doc comment for why: this emulator does not model real
// EC2 Transit Gateway route-table state cross-service, so it cannot walk a
// real path. Returning NOT_CONNECTED with this reason is an honest "cannot
// resolve" result, never a fabricated CONNECTED verdict.
const routeAnalysisReasonAttachmentNotFound = "TRANSIT_GATEWAY_ATTACHMENT_NOT_FOUND"

// routeAnalysisReasonNoDestination is returned when the caller's Destination
// carries neither an IpAddress nor a TransitGatewayAttachmentArn.
const routeAnalysisReasonNoDestination = "NO_DESTINATION_ARN_PROVIDED"

// RouteState / RouteType / RouteTableType wire values (family T).
const (
	routeStateActive                  = "ACTIVE"
	routeTypeStatic                   = "STATIC"
	routeTableTypeTransitGatewayRoute = "TRANSIT_GATEWAY_ROUTE_TABLE"
	routeTableTypeCoreNetworkSegment  = "CORE_NETWORK_SEGMENT"
)

// ConnectionStatus / ConnectionType wire values -- GetNetworkTelemetry's
// ConnectionHealth (family T). This backend's only defensible, non-fabricated
// default is Status UP once the underlying resource reaches AVAILABLE --
// never invented flapping/degraded values (see introspection.go).
const (
	connectionStatusUp = "UP"
)

// ValidationExceptionReason wire values -- UpperCamelCase, a THIRD casing
// convention distinct from mgn's lowerCamelCase and this service's own
// SCREAMING_SNAKE_CASE majority (PARITY.md wire-shape trap).
const (
	validationReasonFieldValidationFailed = "FieldValidationFailed"
)

// Path-parameter placeholder names shared across many routes in the family
// route tables (handler_*.go) -- named constants here so golangci-lint's
// goconst does not flag the same literal repeated dozens of times across
// files.
const (
	paramGlobalNetworkID  = ":GlobalNetworkId"
	paramCoreNetworkID    = ":CoreNetworkId"
	paramAttachmentID     = ":AttachmentId"
	paramResourceArn      = ":ResourceArn"
	paramPolicyVersionID  = ":PolicyVersionId"
	paramConnectPeerID    = ":ConnectPeerId"
	paramPeeringID        = ":PeeringId"
	segGlobalNetworks     = "global-networks"
	segCoreNetworks       = "core-networks"
	segConnectPeers       = "connect-peers"
	segVpcAttachments     = "vpc-attachments"
	segDirectConnectGWs   = "direct-connect-gateway-attachments"
	segTgwRegistrations   = "transit-gateway-registrations"
	segTgwConnectPeerAssn = "transit-gateway-connect-peer-associations"
	segConnectPeerAssn    = "connect-peer-associations"
	segPolicyVersions     = "core-network-policy-versions"
	segPrefixList         = "prefix-list"
	segCoreNetworkSeg     = "core-network"
	segRoutingPolicyLabel = "routing-policy-label"
	segSites              = "sites"
	segDevices            = "devices"
	segLinks              = "links"
	segLinkAssociations   = "link-associations"
	segConnections        = "connections"
	segCustomerGWAssn     = "customer-gateway-associations"
	segAttachments        = "attachments"
	segResourcePolicy     = "resource-policy"
	segTags               = "tags"
)

// resourceTypeConnection/resourceTypeConnectPeer/resourceTypeCoreNetworkKind/
// resourceTypeAttachmentKind are the resource-kind labels this file's
// tagging.go/introspection.go use both as networkResourceItem.ResourceType
// values and as ARN resource-kind segments -- named once here so goconst
// stops flagging the repeated literal.
const (
	resourceTypeConnection  = "connection"
	resourceTypeConnectPeer = "connect-peer"
	resourceTypeCoreNetwork = "core-network"
	resourceTypeAttachment  = "attachment"
	orgAccessStatusDisabled = "DISABLED"
)

// resource-kind labels used in ConflictException/ResourceNotFoundException/
// ServiceQuotaExceededException's wire ResourceType field.
const (
	resourceGlobalNetwork  = "GLOBAL_NETWORK"
	resourceSite           = "SITE"
	resourceDevice         = "DEVICE"
	resourceLink           = "LINK"
	resourceLinkAssoc      = "LINK_ASSOCIATION"
	resourceConnection     = "CONNECTION"
	resourceCustomerGwAssn = "CUSTOMER_GATEWAY_ASSOCIATION"
	resourceTgwReg         = "TRANSIT_GATEWAY_REGISTRATION"
	resourceTgwCpAssn      = "TRANSIT_GATEWAY_CONNECT_PEER_ASSOCIATION"
	resourceConnectPeerAsn = "CONNECT_PEER_ASSOCIATION"
	resourceConnectPeer    = "CONNECT_PEER"
	resourceCoreNetwork    = "CORE_NETWORK"
	resourcePolicyVersion  = "CORE_NETWORK_POLICY_VERSION"
	resourcePrefixListAssn = "CORE_NETWORK_PREFIX_LIST_ASSOCIATION"
	resourceAttachment     = "ATTACHMENT"
	resourcePeering        = "PEERING"
	resourceRouteAnalysis  = "ROUTE_ANALYSIS"
	resourceRoutingLabel   = "ROUTING_POLICY_LABEL"
	resourceTaggable       = "RESOURCE"
)
