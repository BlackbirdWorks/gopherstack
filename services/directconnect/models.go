// Package directconnect emulates the AWS Direct Connect control-plane API
// (aws-sdk-go-v2/service/directconnect, protocol awsjson1.1 -- a single
// POST / dispatched by X-Amz-Target: OvertureService.<Op>, not REST). See
// PARITY.md for the full pre-implementation audit this package was built
// from, including the honest-gap list for the partner/reseller flow, LOA-CFA
// PDF content, the static Locations/RouterType reference catalogs, and
// DescribeCustomerMetadata's empty-by-default agreement list.
package directconnect

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// RouteFilterPrefix mirrors types.RouteFilterPrefix.
type RouteFilterPrefix struct {
	Cidr string
}

// cloneRouteFilterPrefixes returns a deep copy of ps (nil-safe; value-typed
// elements so a slice copy is already a full deep copy).
func cloneRouteFilterPrefixes(ps []RouteFilterPrefix) []RouteFilterPrefix {
	if ps == nil {
		return nil
	}

	cp := make([]RouteFilterPrefix, len(ps))
	copy(cp, ps)

	return cp
}

// MacSecKey mirrors types.MacSecKey. State is a bare string on the wire (not
// a typed enum -- see PARITY.md wire-trap #6): one of
// associating/associated/disassociating/disassociated.
//
// Stored as []*MacSecKey (not []MacSecKey) on Connection/Lag/Interconnect
// specifically so store.go's scheduleTransition can hold a stable pointer
// to one key's State field across the key's associating->associated timer:
// a []MacSecKey value slice's backing array can be reallocated by a later
// append (e.g. a second AssociateMacSecKey call on the same connection),
// which would silently stand up an in-flight timer to mutate the old,
// now-orphaned backing array instead of the live one -- the exact aliasing
// bug class this task's instructions call out. A []*MacSecKey's pointers
// stay valid across any later append to the outer slice.
type MacSecKey struct {
	Ckn       string
	SecretARN string
	StartOn   string
	State     string
}

// cloneMacSecKeys returns a deep copy of ks (nil-safe): a fresh slice of
// fresh *MacSecKey values sharing no memory with ks.
func cloneMacSecKeys(ks []*MacSecKey) []*MacSecKey {
	if ks == nil {
		return nil
	}

	cp := make([]*MacSecKey, len(ks))

	for i, k := range ks {
		if k == nil {
			continue
		}

		v := *k
		cp[i] = &v
	}

	return cp
}

// RateLimiterStatus mirrors types.RateLimiterStatus.
type RateLimiterStatus struct {
	TotalBandwidth string
	InUse          int32
	MaxAllowed     int32
	Remaining      int32
}

// clone returns a deep copy of r, or nil if r is nil.
func (r *RateLimiterStatus) clone() *RateLimiterStatus {
	if r == nil {
		return nil
	}

	cp := *r

	return &cp
}

// BGPPeer mirrors types.BGPPeer. AsnProvided/AsnValue back the dual
// Asn/AsnLong zero-out rule from PARITY.md wire-trap #5 -- see
// wire_convert.go's asnWireFields.
//
// Stored as []*BGPPeer (not []BGPPeer) on VirtualInterface for the same
// stable-pointer reason MacSecKey is -- StartBgpFailoverTest schedules a
// timer against one peer's BgpStatus field, and CreateBGPPeer/DeleteBGPPeer
// append/remove elements of the same slice.
type BGPPeer struct {
	BgpPeerID          string
	BgpPeerState       string
	BgpStatus          string
	AddressFamily      string
	AmazonAddress      string
	CustomerAddress    string
	AuthKey            string
	AwsDeviceV2        string
	AwsLogicalDeviceID string
	AsnValue           int64
	AsnProvided        bool
}

// cloneBGPPeers returns a deep copy of ps (nil-safe): a fresh slice of
// fresh *BGPPeer values sharing no memory with ps.
func cloneBGPPeers(ps []*BGPPeer) []*BGPPeer {
	if ps == nil {
		return nil
	}

	cp := make([]*BGPPeer, len(ps))

	for i, p := range ps {
		if p == nil {
			continue
		}

		v := *p
		cp[i] = &v
	}

	return cp
}

// Connection is the internal state of one Direct Connect connection (a
// standard, hosted, or partner-allocated connection -- see PARITY.md's
// "Hosted connections, interconnects, and the partner/reseller flow"
// section for the honest bookkeeping-only scope of the latter two).
type Connection struct {
	Tags              *tags.Tags
	LoaIssueTime      *time.Time
	RateLimiterStatus *RateLimiterStatus
	ConnectionID      string
	ConnectionName    string
	ConnectionState   string
	Bandwidth         string
	Location          string
	Region            string
	// LagID is set when this connection is a member of a LAG (via CreateLag
	// or AssociateConnectionWithLag).
	LagID                string
	OwnerAccount         string
	ProviderName         string
	PartnerName          string
	AwsDeviceV2          string
	AwsLogicalDeviceID   string
	HasLogicalRedundancy string
	EncryptionMode       string
	PortEncryptionStatus string
	// InterconnectID is set for a connection allocated via
	// AllocateConnectionOnInterconnect -- the parent Interconnect it rides
	// on. Empty for a standard connection.
	InterconnectID string
	// ParentConnectionID is set for a connection allocated via
	// AllocateHostedConnection onto an existing standard Connection (as
	// opposed to a LAG, tracked via LagID, or an Interconnect, tracked via
	// InterconnectID) and reassignable via AssociateHostedConnection.
	ParentConnectionID               string
	MacSecKeys                       []*MacSecKey
	Vlan                             int32
	JumboFrameCapable                bool
	MacSecCapable                    bool
	PartnerInterconnectMacSecCapable bool
}

// clone returns a deep copy of c, or nil if c is nil. Tags is deliberately
// NOT cloned -- it is its own concurrency-safe type, matching
// services/outposts/services/resiliencehub's identical clone() convention.
func (c *Connection) clone() *Connection {
	if c == nil {
		return nil
	}

	cp := *c
	cp.MacSecKeys = cloneMacSecKeys(c.MacSecKeys)
	cp.RateLimiterStatus = c.RateLimiterStatus.clone()

	if c.LoaIssueTime != nil {
		t := *c.LoaIssueTime
		cp.LoaIssueTime = &t
	}

	return &cp
}

// Lag is the internal state of one link aggregation group. Its member
// Connections are NOT stored here as a nested slice -- they are their own
// Connection records with LagID set, looked up dynamically by
// connectionsByLagLocked at wire-build time, avoiding a second, aliasable
// copy of the same data (the exact class of bug outposts' de-stub pass
// found: a nested slice that silently goes stale).
type Lag struct {
	Tags                    *tags.Tags
	RateLimiterStatus       *RateLimiterStatus
	LagID                   string
	LagName                 string
	LagState                string
	ConnectionsBandwidth    string
	Location                string
	Region                  string
	OwnerAccount            string
	ProviderName            string
	AwsDeviceV2             string
	AwsLogicalDeviceID      string
	HasLogicalRedundancy    string
	EncryptionMode          string
	MacSecKeys              []*MacSecKey
	MinimumLinks            int32
	NumberOfConnections     int32
	AllowsHostedConnections bool
	JumboFrameCapable       bool
	MacSecCapable           bool
}

// clone returns a deep copy of l, or nil if l is nil.
func (l *Lag) clone() *Lag {
	if l == nil {
		return nil
	}

	cp := *l
	cp.MacSecKeys = cloneMacSecKeys(l.MacSecKeys)
	cp.RateLimiterStatus = l.RateLimiterStatus.clone()

	return &cp
}

// Interconnect is the internal state of one interconnect -- a Direct
// Connect Partner's physical cross-connect. See PARITY.md's honest-gap
// section: this is real bookkeeping (state machine, parent/child
// relationships), never a simulation of the physical link itself.
type Interconnect struct {
	Tags                 *tags.Tags
	InterconnectID       string
	InterconnectName     string
	InterconnectState    string
	Bandwidth            string
	Location             string
	Region               string
	LagID                string
	ProviderName         string
	AwsDeviceV2          string
	AwsLogicalDeviceID   string
	HasLogicalRedundancy string
	EncryptionMode       string
	PortEncryptionStatus string
	MacSecKeys           []*MacSecKey
	JumboFrameCapable    bool
	MacSecCapable        bool
}

// clone returns a deep copy of i, or nil if i is nil.
func (i *Interconnect) clone() *Interconnect {
	if i == nil {
		return nil
	}

	cp := *i
	cp.MacSecKeys = cloneMacSecKeys(i.MacSecKeys)

	return &cp
}

// VirtualInterface is the internal state of one private/public/transit
// virtual interface -- the richest resource in this service (10-value state
// enum, dual ASN fields, BGP peer list). AmazonSideAsn is NOT stored here:
// it is derived at wire-build time from the bound DirectConnectGateway (see
// wire_convert.go's toVirtualInterfaceWire), since it is really a property
// of the gateway the VIF is attached to, not the VIF itself -- storing it
// twice would risk the two copies drifting.
type VirtualInterface struct {
	Tags                   *tags.Tags
	VirtualInterfaceID     string
	VirtualInterfaceName   string
	VirtualInterfaceType   string
	VirtualInterfaceState  string
	ConnectionID           string
	OwnerAccount           string
	Region                 string
	Location               string
	AddressFamily          string
	AmazonAddress          string
	CustomerAddress        string
	AuthKey                string
	DirectConnectGatewayID string
	VirtualGatewayID       string
	CustomerRouterConfig   string
	RateLimit              string
	AwsDeviceV2            string
	AwsLogicalDeviceID     string
	BgpPeers               []*BGPPeer
	RouteFilterPrefixes    []RouteFilterPrefix
	Vlan                   int32
	Mtu                    int32
	AsnValue               int64
	AsnProvided            bool
	JumboFrameCapable      bool
	SiteLinkEnabled        bool
}

// clone returns a deep copy of v, or nil if v is nil.
func (v *VirtualInterface) clone() *VirtualInterface {
	if v == nil {
		return nil
	}

	cp := *v
	cp.BgpPeers = cloneBGPPeers(v.BgpPeers)
	cp.RouteFilterPrefixes = cloneRouteFilterPrefixes(v.RouteFilterPrefixes)

	return &cp
}

// DirectConnectGateway is the internal state of one Direct Connect gateway
// -- a GLOBAL (non-regional) resource, see PARITY.md's ARN section and
// store.go's GatewayARN. The name deliberately mirrors the real SDK type
// types.DirectConnectGateway rather than shortening to "Gateway": a
// shorter name would collide in meaning with this package's existing
// GatewayAssociation/GatewayAssociationProposal.GatewayID, which names the
// ASSOCIATED VGW/TGW -- a different concept entirely.
//
//nolint:revive // see doc comment above
type DirectConnectGateway struct {
	Tags                      *tags.Tags
	DirectConnectGatewayID    string
	DirectConnectGatewayName  string
	DirectConnectGatewayState string
	OwnerAccount              string
	StateChangeError          string
	AmazonSideAsn             int64
}

// clone returns a deep copy of g, or nil if g is nil.
func (g *DirectConnectGateway) clone() *DirectConnectGateway {
	if g == nil {
		return nil
	}

	cp := *g

	return &cp
}

// GatewayAssociation is the internal state of one association between a
// DirectConnectGateway and a virtual private gateway or transit gateway.
// GatewayType/GatewayID/GatewayOwnerAccount/GatewayRegion back the modern
// AssociatedGateway wire shape; VirtualGatewayID/VirtualGatewayOwnerAccount
// (the legacy fields) are kept in sync whenever GatewayType is
// virtualPrivateGateway -- see PARITY.md's note that both must stay
// populated in parallel, not just one or the other.
type GatewayAssociation struct {
	AssociationID                    string
	DirectConnectGatewayID           string
	DirectConnectGatewayOwnerAccount string
	AssociationState                 string
	StateChangeError                 string
	GatewayID                        string
	GatewayOwnerAccount              string
	GatewayRegion                    string
	GatewayType                      string
	AllowedPrefixes                  []RouteFilterPrefix
}

// clone returns a deep copy of a, or nil if a is nil.
func (a *GatewayAssociation) clone() *GatewayAssociation {
	if a == nil {
		return nil
	}

	cp := *a
	cp.AllowedPrefixes = cloneRouteFilterPrefixes(a.AllowedPrefixes)

	return &cp
}

// GatewayAssociationProposal is the internal state of one cross-account
// association proposal -- see PARITY.md's "cross-account proposal flow"
// section for the proposer/accepter directionality this backend assumes.
type GatewayAssociationProposal struct {
	ProposalID                       string
	DirectConnectGatewayID           string
	DirectConnectGatewayOwnerAccount string
	ProposalState                    string
	GatewayID                        string
	GatewayOwnerAccount              string
	GatewayRegion                    string
	GatewayType                      string
	ExistingAllowedPrefixes          []RouteFilterPrefix
	RequestedAllowedPrefixes         []RouteFilterPrefix
}

// clone returns a deep copy of p, or nil if p is nil.
func (p *GatewayAssociationProposal) clone() *GatewayAssociationProposal {
	if p == nil {
		return nil
	}

	cp := *p
	cp.ExistingAllowedPrefixes = cloneRouteFilterPrefixes(p.ExistingAllowedPrefixes)
	cp.RequestedAllowedPrefixes = cloneRouteFilterPrefixes(p.RequestedAllowedPrefixes)

	return &cp
}

// VifTestHistory is the internal state of one StartBgpFailoverTest run --
// see PARITY.md's "BGP failover testing" section.
type VifTestHistory struct {
	StartTime             *time.Time
	EndTime               *time.Time
	TestID                string
	VirtualInterfaceID    string
	Status                string
	OwnerAccount          string
	BgpPeers              []string
	TestDurationInMinutes int32
}

// clone returns a deep copy of t, or nil if t is nil.
func (t *VifTestHistory) clone() *VifTestHistory {
	if t == nil {
		return nil
	}

	cp := *t
	cp.BgpPeers = cloneStrs(t.BgpPeers)

	if t.StartTime != nil {
		st := *t.StartTime
		cp.StartTime = &st
	}

	if t.EndTime != nil {
		et := *t.EndTime
		cp.EndTime = &et
	}

	return &cp
}

// cloneStrs returns a deep copy of a string slice (nil-safe).
func cloneStrs(ss []string) []string {
	if ss == nil {
		return nil
	}

	cp := make([]string, len(ss))
	copy(cp, ss)

	return cp
}
