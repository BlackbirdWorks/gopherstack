package networkmanager

import (
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// This file holds every internal resource representation this backend
// stores, plus a clone() for each -- see .claude/memories/parity-principles.md
// and services/outposts/resiliencehub's clone()-per-type idiom: every
// store.Table[V].Get/Snapshot/All caller in this package must deep-copy
// before returning to a handler, since several of these types carry nested
// slices/pointers this backend mutates in place during async state
// transitions (see store.go's scheduleAdvance).
//
// Timestamps mirror a real smithy *time.Time member (confirmed via
// smithytime.ParseEpochSeconds in the SDK's own deserializer -- see
// PARITY.md), so every CreatedAt/UpdatedAt/etc. field here is a time.Time,
// wire-encoded as epoch-seconds via pkgs/awstime.Epoch (see wire_convert.go).

// ---- Global Networks / Sites / Devices / Links ----

// GlobalNetwork mirrors types.GlobalNetwork.
type GlobalNetwork struct {
	CreatedAt        time.Time
	Tags             *tags.Tags
	GlobalNetworkID  string
	GlobalNetworkArn string
	Description      string
	State            string
}

func (g *GlobalNetwork) clone() *GlobalNetwork {
	if g == nil {
		return nil
	}

	cp := *g

	return &cp
}

// Location mirrors types.Location.
type Location struct {
	Address   string
	Latitude  string
	Longitude string
}

// Site mirrors types.Site.
type Site struct {
	CreatedAt       time.Time
	Tags            *tags.Tags
	Location        *Location
	SiteID          string
	SiteArn         string
	GlobalNetworkID string
	Description     string
	State           string
}

func (s *Site) clone() *Site {
	if s == nil {
		return nil
	}

	cp := *s
	if s.Location != nil {
		loc := *s.Location
		cp.Location = &loc
	}

	return &cp
}

// AWSLocation mirrors types.AWSLocation.
type AWSLocation struct {
	SubnetArn string
	Zone      string
}

// Device mirrors types.Device.
type Device struct {
	CreatedAt       time.Time
	Tags            *tags.Tags
	AWSLocation     *AWSLocation
	Location        *Location
	DeviceID        string
	DeviceArn       string
	GlobalNetworkID string
	Description     string
	Model           string
	SerialNumber    string
	SiteID          string
	Type            string
	Vendor          string
	State           string
}

func (d *Device) clone() *Device {
	if d == nil {
		return nil
	}

	cp := *d
	if d.AWSLocation != nil {
		l := *d.AWSLocation
		cp.AWSLocation = &l
	}

	if d.Location != nil {
		l := *d.Location
		cp.Location = &l
	}

	return &cp
}

// Bandwidth mirrors types.Bandwidth.
type Bandwidth struct {
	DownloadSpeed int32
	UploadSpeed   int32
}

// Link mirrors types.Link.
type Link struct {
	CreatedAt       time.Time
	Tags            *tags.Tags
	Bandwidth       *Bandwidth
	LinkID          string
	LinkArn         string
	GlobalNetworkID string
	SiteID          string
	Description     string
	Provider        string
	Type            string
	State           string
}

func (l *Link) clone() *Link {
	if l == nil {
		return nil
	}

	cp := *l
	if l.Bandwidth != nil {
		b := *l.Bandwidth
		cp.Bandwidth = &b
	}

	return &cp
}

// LinkAssociation mirrors types.LinkAssociation.
type LinkAssociation struct {
	DeviceID             string
	GlobalNetworkID      string
	LinkID               string
	LinkAssociationState string
}

func (a *LinkAssociation) clone() *LinkAssociation {
	if a == nil {
		return nil
	}

	cp := *a

	return &cp
}

// Connection mirrors types.Connection.
type Connection struct {
	CreatedAt         time.Time
	Tags              *tags.Tags
	ConnectionID      string
	ConnectionArn     string
	ConnectedDeviceID string
	ConnectedLinkID   string
	DeviceID          string
	GlobalNetworkID   string
	LinkID            string
	Description       string
	State             string
}

func (c *Connection) clone() *Connection {
	if c == nil {
		return nil
	}

	cp := *c

	return &cp
}

// ---- Association families (G/H/I/J) ----

// CustomerGatewayAssociation mirrors types.CustomerGatewayAssociation.
type CustomerGatewayAssociation struct {
	CustomerGatewayArn string
	DeviceID           string
	GlobalNetworkID    string
	LinkID             string
	State              string
}

func (a *CustomerGatewayAssociation) clone() *CustomerGatewayAssociation {
	if a == nil {
		return nil
	}

	cp := *a

	return &cp
}

// TransitGatewayRegistrationStateReason mirrors
// types.TransitGatewayRegistrationStateReason.
type TransitGatewayRegistrationStateReason struct {
	Code    string
	Message string
}

// TransitGatewayRegistration mirrors types.TransitGatewayRegistration.
type TransitGatewayRegistration struct {
	State             *TransitGatewayRegistrationStateReason
	GlobalNetworkID   string
	TransitGatewayArn string
}

func (r *TransitGatewayRegistration) clone() *TransitGatewayRegistration {
	if r == nil {
		return nil
	}

	cp := *r
	if r.State != nil {
		s := *r.State
		cp.State = &s
	}

	return &cp
}

// TransitGatewayConnectPeerAssociation mirrors
// types.TransitGatewayConnectPeerAssociation.
type TransitGatewayConnectPeerAssociation struct {
	DeviceID                     string
	GlobalNetworkID              string
	LinkID                       string
	State                        string
	TransitGatewayConnectPeerArn string
}

func (a *TransitGatewayConnectPeerAssociation) clone() *TransitGatewayConnectPeerAssociation {
	if a == nil {
		return nil
	}

	cp := *a

	return &cp
}

// ConnectPeerAssociation mirrors types.ConnectPeerAssociation -- the
// concrete Global-Networks/Cloud-WAN bridge (PARITY.md family J).
type ConnectPeerAssociation struct {
	ConnectPeerID   string
	DeviceID        string
	GlobalNetworkID string
	LinkID          string
	State           string
}

func (a *ConnectPeerAssociation) clone() *ConnectPeerAssociation {
	if a == nil {
		return nil
	}

	cp := *a

	return &cp
}

// ---- Cloud WAN: Connect Peers (family K) ----

// BgpOptions mirrors types.BgpOptions.
type BgpOptions struct {
	PeerAsn int64
}

// ConnectPeerBgpConfiguration mirrors types.ConnectPeerBgpConfiguration.
type ConnectPeerBgpConfiguration struct {
	CoreNetworkAddress string
	PeerAddress        string
	CoreNetworkAsn     int64
	PeerAsn            int64
}

// ConnectPeerConfiguration mirrors types.ConnectPeerConfiguration.
type ConnectPeerConfiguration struct {
	CoreNetworkAddress string
	PeerAddress        string
	Protocol           string
	BgpConfigurations  []ConnectPeerBgpConfiguration
	InsideCidrBlocks   []string
}

func (c *ConnectPeerConfiguration) clone() *ConnectPeerConfiguration {
	if c == nil {
		return nil
	}

	cp := *c
	cp.BgpConfigurations = append([]ConnectPeerBgpConfiguration(nil), c.BgpConfigurations...)
	cp.InsideCidrBlocks = append([]string(nil), c.InsideCidrBlocks...)

	return &cp
}

// ConnectPeer mirrors types.ConnectPeer.
type ConnectPeer struct {
	CreatedAt           time.Time
	Tags                *tags.Tags
	Configuration       *ConnectPeerConfiguration
	ConnectAttachmentID string
	ConnectPeerID       string
	CoreNetworkID       string
	EdgeLocation        string
	State               string
	SubnetArn           string
}

func (c *ConnectPeer) clone() *ConnectPeer {
	if c == nil {
		return nil
	}

	cp := *c
	cp.Configuration = c.Configuration.clone()

	return &cp
}

// ---- Cloud WAN: Core Networks (family L) ----

// CoreNetwork mirrors types.CoreNetwork. Edges/Segments/NetworkFunctionGroups
// stay empty in this pass -- no policy-execution engine computes them (see
// corenetworks.go's doc comment on the scoped-down change-set diff).
type CoreNetwork struct {
	CreatedAt       time.Time
	Tags            *tags.Tags
	CoreNetworkArn  string
	CoreNetworkID   string
	GlobalNetworkID string
	Description     string
	State           string
}

func (c *CoreNetwork) clone() *CoreNetwork {
	if c == nil {
		return nil
	}

	cp := *c

	return &cp
}

// CoreNetworkPolicyError mirrors types.CoreNetworkPolicyError.
type CoreNetworkPolicyError struct {
	ErrorCode string
	Message   string
	Path      string
}

// CoreNetworkPolicyVersion is this backend's stored history entry for one
// PutCoreNetworkPolicy/RestoreCoreNetworkPolicyVersion call -- the versioned
// half of types.CoreNetworkPolicy plus types.CoreNetworkPolicyVersion.
type CoreNetworkPolicyVersion struct {
	CreatedAt       time.Time
	CoreNetworkID   string
	Description     string
	PolicyDocument  string
	ChangeSetState  string
	PolicyVersionID int32
}

func (v *CoreNetworkPolicyVersion) clone() *CoreNetworkPolicyVersion {
	if v == nil {
		return nil
	}

	cp := *v

	return &cp
}

// CoreNetworkPolicyHistory is the per-core-network policy state machine:
// every submitted version plus which version is LIVE (deployed by
// ExecuteCoreNetworkChangeSet) versus LATEST (most recently
// Put/Restored) -- see PARITY.md family M's doc comment on why these two
// pointers are NOT the same until execution happens.
type CoreNetworkPolicyHistory struct {
	Versions           map[int32]*CoreNetworkPolicyVersion
	ownerCoreNetworkID string
	LiveID             int32 // 0 means "no LIVE version yet"
	LatestID           int32
	NextID             int32
}

func (h *CoreNetworkPolicyHistory) clone() *CoreNetworkPolicyHistory {
	if h == nil {
		return nil
	}

	cp := *h
	cp.Versions = make(map[int32]*CoreNetworkPolicyVersion, len(h.Versions))

	for k, v := range h.Versions {
		cp.Versions[k] = v.clone()
	}

	return &cp
}

// CoreNetworkPrefixListAssociation mirrors types.PrefixListAssociation.
type CoreNetworkPrefixListAssociation struct {
	CoreNetworkID   string
	PrefixListAlias string
	PrefixListArn   string
}

func (a *CoreNetworkPrefixListAssociation) clone() *CoreNetworkPrefixListAssociation {
	if a == nil {
		return nil
	}

	cp := *a

	return &cp
}

// AttachmentRoutingPolicyLabel mirrors
// types.AttachmentRoutingPolicyAssociationSummary's stored shape (one label
// per attachment, family P).
type AttachmentRoutingPolicyLabel struct {
	AttachmentID       string
	CoreNetworkID      string
	RoutingPolicyLabel string
}

func (l *AttachmentRoutingPolicyLabel) clone() *AttachmentRoutingPolicyLabel {
	if l == nil {
		return nil
	}

	cp := *l

	return &cp
}

// ---- Attachments (family Q, Q1-Q5) ----

// AttachmentKind discriminates the 5 subtypes sharing the base Attachment
// shape (types.AttachmentType).
type AttachmentKind string

// AttachmentError mirrors types.AttachmentError -- this backend never
// populates a live Attachment's LastModificationErrors (no failure-injection
// engine models a real per-attachment error), but the type is declared so
// the wire shape stays complete.
type AttachmentError struct {
	Code        string
	Message     string
	RequestID   string
	ResourceArn string
}

// Attachment is this backend's single flat representation of the shared
// base Attachment shape (types.Attachment) plus every subtype-specific
// field (VpcAttachment/ConnectAttachment/SiteToSiteVpnAttachment/
// DirectConnectGatewayAttachment/TransitGatewayRouteTableAttachment), keyed
// by AttachmentId. A discriminator (Kind) selects which subtype-specific
// fields are populated -- simpler than 5 separate tables given every op in
// family Q operates generically across all 5 kinds by AttachmentId alone.
type Attachment struct {
	CreatedAt                   time.Time
	UpdatedAt                   time.Time
	Tags                        *tags.Tags
	VpcOptions                  *VpcOptions
	ConnectOptions              *ConnectAttachmentOptions
	OwnerAccountID              string
	RoutingPolicyLabel          string
	AttachmentType              string
	CoreNetworkArn              string
	CoreNetworkID               string
	EdgeLocation                string
	NetworkFunctionGroupName    string
	TransitGatewayRouteTableArn string
	ResourceArn                 string
	SegmentName                 string
	State                       string
	AttachmentID                string
	PeeringID                   string
	DirectConnectGatewayArn     string
	VpcArn                      string
	VpnConnectionArn            string
	TransportAttachmentID       string
	SubnetArns                  []string
	EdgeLocations               []string
	LastModificationErrors      []AttachmentError
	AttachmentPolicyRuleNumber  int32
}

func (a *Attachment) clone() *Attachment {
	if a == nil {
		return nil
	}

	cp := *a
	cp.EdgeLocations = append([]string(nil), a.EdgeLocations...)
	cp.SubnetArns = append([]string(nil), a.SubnetArns...)
	cp.LastModificationErrors = append([]AttachmentError(nil), a.LastModificationErrors...)

	if a.VpcOptions != nil {
		o := *a.VpcOptions
		cp.VpcOptions = &o
	}

	if a.ConnectOptions != nil {
		o := *a.ConnectOptions
		cp.ConnectOptions = &o
	}

	return &cp
}

// VpcOptions mirrors types.VpcOptions.
type VpcOptions struct {
	ApplianceModeSupport            bool
	DNSSupport                      bool
	Ipv6Support                     bool
	SecurityGroupReferencingSupport bool
}

// ConnectAttachmentOptions mirrors types.ConnectAttachmentOptions.
type ConnectAttachmentOptions struct {
	Protocol string
}

// ---- Peerings (family R) ----

// PeeringError mirrors types.PeeringError.
type PeeringError struct {
	Code      string
	Message   string
	RequestID string
}

// Peering is this backend's flat representation of the base Peering shape
// (types.Peering) plus TransitGatewayPeering's subtype fields -- the only
// peering subtype today (PeeringType has exactly 1 value).
type Peering struct {
	CreatedAt                         time.Time
	Tags                              *tags.Tags
	OwnerAccountID                    string
	CoreNetworkArn                    string
	CoreNetworkID                     string
	EdgeLocation                      string
	PeeringID                         string
	PeeringType                       string
	ResourceArn                       string
	State                             string
	TransitGatewayArn                 string
	TransitGatewayPeeringAttachmentID string
	LastModificationErrors            []PeeringError
}

func (p *Peering) clone() *Peering {
	if p == nil {
		return nil
	}

	cp := *p
	cp.LastModificationErrors = append([]PeeringError(nil), p.LastModificationErrors...)

	return &cp
}

// ---- Route analysis (family S) ----

// RouteAnalysisEndpoint mirrors types.RouteAnalysisEndpointOptions (the
// resolved echo) built from the caller's
// RouteAnalysisEndpointOptionsSpecification input.
type RouteAnalysisEndpoint struct {
	IPAddress                   string
	TransitGatewayArn           string
	TransitGatewayAttachmentArn string
}

// RouteAnalysisCompletion mirrors types.RouteAnalysisCompletion.
type RouteAnalysisCompletion struct {
	ReasonCode string
	ResultCode string
}

// RouteAnalysisPath mirrors types.RouteAnalysisPath. Path stays empty in
// this pass -- see routeanalysis.go's doc comment: this backend does not
// walk real cross-service EC2 Transit Gateway route-table state, so it
// never fabricates a PathComponent sequence.
type RouteAnalysisPath struct {
	CompletionStatus *RouteAnalysisCompletion
}

func (p *RouteAnalysisPath) clone() *RouteAnalysisPath {
	if p == nil {
		return nil
	}

	cp := *p
	if p.CompletionStatus != nil {
		c := *p.CompletionStatus
		cp.CompletionStatus = &c
	}

	return &cp
}

// RouteAnalysis mirrors types.RouteAnalysis.
type RouteAnalysis struct {
	Destination       *RouteAnalysisEndpoint
	Source            *RouteAnalysisEndpoint
	ForwardPath       *RouteAnalysisPath
	ReturnPath        *RouteAnalysisPath
	GlobalNetworkID   string
	OwnerAccountID    string
	RouteAnalysisID   string
	Status            string
	IncludeReturnPath bool
}

func (r *RouteAnalysis) clone() *RouteAnalysis {
	if r == nil {
		return nil
	}

	cp := *r
	if r.Destination != nil {
		d := *r.Destination
		cp.Destination = &d
	}

	if r.Source != nil {
		s := *r.Source
		cp.Source = &s
	}

	cp.ForwardPath = r.ForwardPath.clone()
	cp.ReturnPath = r.ReturnPath.clone()

	return &cp
}

// ---- Network resource metadata (family U) ----

// networkResourceMetadata is this backend's side-store of caller-supplied
// key-value annotations keyed by ResourceArn, independent of AWS Tags
// (family U's doc comment).
type networkResourceMetadata struct {
	Metadata    map[string]string
	ResourceArn string
}

func (m *networkResourceMetadata) clone() *networkResourceMetadata {
	if m == nil {
		return nil
	}

	cp := *m
	cp.Metadata = make(map[string]string, len(m.Metadata))
	maps.Copy(cp.Metadata, m.Metadata)

	return &cp
}

// ---- Resource-based policy (family W) ----

// resourcePolicy is this backend's store of the resource-based IAM-style
// JSON policy document attached to a NetworkManager resource ARN, keyed by
// ResourceArn -- structurally unrelated to CoreNetworkPolicyHistory despite
// the shared English word "policy" (family W's doc comment).
type resourcePolicy struct {
	ResourceArn    string
	PolicyDocument string
}

// ---- Organizations integration (family V) ----

// organizationStatus mirrors types.OrganizationStatus -- one singleton per
// backend (there is exactly one AWS Organization per account in this
// model), not a table.
type organizationStatus struct {
	OrganizationAwsServiceAccessStatus string
	OrganizationID                     string
	SLRDeploymentStatus                string
	AccountStatusList                  []accountStatus
}

func (o *organizationStatus) clone() *organizationStatus {
	if o == nil {
		return nil
	}

	cp := *o
	cp.AccountStatusList = append([]accountStatus(nil), o.AccountStatusList...)

	return &cp
}

// accountStatus mirrors types.AccountStatus.
type accountStatus struct {
	AccountID           string
	SLRDeploymentStatus string
}

func cloneStrMap(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}

	cp := make(map[string]string, len(m))
	maps.Copy(cp, m)

	return cp
}
