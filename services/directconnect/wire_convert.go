package directconnect

import (
	"math"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// epochPtr converts t to an epoch-seconds *float64 for the wire, or nil if
// t is nil. AWS JSON-protocol timestamps are epoch-seconds numbers, never
// RFC3339 strings -- see pkgs/awstime.Epoch's doc comment and
// parity-principles.md's known bug class.
func epochPtr(t *time.Time) *float64 {
	if t == nil {
		return nil
	}

	v := awstime.Epoch(*t)

	return &v
}

// asnWireFields implements the dual Asn/AsnLong zero-out rule from
// PARITY.md wire-trap #5: when no ASN was ever supplied, both wire fields
// are zero-value/omitted; when one was supplied and fits a 4-byte (int32)
// value, both wire fields report it identically; when the supplied value
// exceeds int32's range, the legacy Asn field reports 0 while AsnLong
// carries the real value.
func asnWireFields(value int64, provided bool) (int32, *int64) {
	if !provided {
		return 0, nil
	}

	v := value
	if value > math.MaxInt32 || value < math.MinInt32 {
		return 0, &v
	}

	return int32(value), &v
}

// resolveAsnInput implements the input side of PARITY.md wire-trap #5: if
// asnLong was supplied, the API only accepts it (the doc comment says
// legacy asn is ignored when both are present); otherwise legacy asn is
// used if non-zero.
func resolveAsnInput(asn int32, asnLong *int64) (int64, bool) {
	if asnLong != nil {
		return *asnLong, true
	}

	if asn != 0 {
		return int64(asn), true
	}

	return 0, false
}

// toTagWire converts a *tags.Tags to a sorted wire tag list (nil-safe,
// returns nil for an empty/nil Tags so the JSON field is omitted).
func toTagWire(t *tags.Tags) []tagWire {
	if t == nil || t.Len() == 0 {
		return nil
	}

	kvs := tags.MapToKV(t.Clone())
	out := make([]tagWire, 0, len(kvs))

	for _, kv := range kvs {
		out = append(out, tagWire{Key: kv.Key, Value: kv.Value})
	}

	return out
}

// tagWireToMap converts a wire tag list to a plain map for storage.
func tagWireToMap(ws []tagWire) map[string]string {
	if len(ws) == 0 {
		return nil
	}

	m := make(map[string]string, len(ws))
	for _, w := range ws {
		m[w.Key] = w.Value
	}

	return m
}

// tagWireKeys returns the Key field of every element of ws, for
// validateNewTags' duplicate-key check.
func tagWireKeys(ws []tagWire) []string {
	keys := make([]string, len(ws))
	for i, w := range ws {
		keys[i] = w.Key
	}

	return keys
}

func toRouteFilterPrefixWire(ps []RouteFilterPrefix) []routeFilterPrefixWire {
	if len(ps) == 0 {
		return nil
	}

	out := make([]routeFilterPrefixWire, len(ps))
	for i, p := range ps {
		out[i] = routeFilterPrefixWire(p)
	}

	return out
}

func fromRouteFilterPrefixWire(ws []routeFilterPrefixWire) []RouteFilterPrefix {
	if len(ws) == 0 {
		return nil
	}

	out := make([]RouteFilterPrefix, len(ws))
	for i, w := range ws {
		out[i] = RouteFilterPrefix(w)
	}

	return out
}

func toMacSecKeyWire(ks []*MacSecKey) []macSecKeyWire {
	if len(ks) == 0 {
		return nil
	}

	out := make([]macSecKeyWire, 0, len(ks))

	for _, k := range ks {
		if k == nil {
			continue
		}

		out = append(out, macSecKeyWire{Ckn: k.Ckn, SecretARN: k.SecretARN, StartOn: k.StartOn, State: k.State})
	}

	return out
}

func toBGPPeerWire(ps []*BGPPeer) []bgpPeerWire {
	if len(ps) == 0 {
		return nil
	}

	out := make([]bgpPeerWire, 0, len(ps))

	for _, p := range ps {
		if p == nil {
			continue
		}

		asn, asnLong := asnWireFields(p.AsnValue, p.AsnProvided)
		out = append(out, bgpPeerWire{
			AddressFamily:      p.AddressFamily,
			AmazonAddress:      p.AmazonAddress,
			Asn:                asn,
			AsnLong:            asnLong,
			AuthKey:            p.AuthKey,
			AwsDeviceV2:        p.AwsDeviceV2,
			AwsLogicalDeviceID: p.AwsLogicalDeviceID,
			BgpPeerID:          p.BgpPeerID,
			BgpPeerState:       p.BgpPeerState,
			BgpStatus:          p.BgpStatus,
			CustomerAddress:    p.CustomerAddress,
		})
	}

	return out
}

func toRateLimiterStatusWire(r *RateLimiterStatus) *rateLimiterStatusWire {
	if r == nil {
		return nil
	}

	return &rateLimiterStatusWire{
		TotalBandwidth: r.TotalBandwidth,
		InUse:          r.InUse,
		MaxAllowed:     r.MaxAllowed,
		Remaining:      r.Remaining,
	}
}

// toConnectionWire converts a Connection to its wire shape. Callers must
// hold at least a read lock.
func toConnectionWire(c *Connection) connectionWire {
	return connectionWire{
		LoaIssueTime:                     epochPtr(c.LoaIssueTime),
		RateLimiterStatus:                toRateLimiterStatusWire(c.RateLimiterStatus),
		AwsDeviceV2:                      c.AwsDeviceV2,
		AwsLogicalDeviceID:               c.AwsLogicalDeviceID,
		Bandwidth:                        c.Bandwidth,
		ConnectionID:                     c.ConnectionID,
		ConnectionName:                   c.ConnectionName,
		ConnectionState:                  c.ConnectionState,
		EncryptionMode:                   c.EncryptionMode,
		HasLogicalRedundancy:             c.HasLogicalRedundancy,
		LagID:                            c.LagID,
		Location:                         c.Location,
		MacSecKeys:                       toMacSecKeyWire(c.MacSecKeys),
		OwnerAccount:                     c.OwnerAccount,
		PartnerName:                      c.PartnerName,
		PortEncryptionStatus:             c.PortEncryptionStatus,
		ProviderName:                     c.ProviderName,
		Region:                           c.Region,
		Tags:                             toTagWire(c.Tags),
		Vlan:                             c.Vlan,
		JumboFrameCapable:                c.JumboFrameCapable,
		MacSecCapable:                    c.MacSecCapable,
		PartnerInterconnectMacSecCapable: c.PartnerInterconnectMacSecCapable,
	}
}

// toLagWire converts a Lag to its wire shape, assembling Connections[] from
// the given member-connection snapshot (see connections.go's
// connectionsByLag) rather than a nested, independently-stored slice.
func toLagWire(l *Lag, members []*Connection) lagWire {
	conns := make([]connectionWire, 0, len(members))
	for _, c := range members {
		conns = append(conns, toConnectionWire(c))
	}

	return lagWire{
		RateLimiterStatus:       toRateLimiterStatusWire(l.RateLimiterStatus),
		AwsDeviceV2:             l.AwsDeviceV2,
		AwsLogicalDeviceID:      l.AwsLogicalDeviceID,
		Connections:             conns,
		ConnectionsBandwidth:    l.ConnectionsBandwidth,
		EncryptionMode:          l.EncryptionMode,
		HasLogicalRedundancy:    l.HasLogicalRedundancy,
		LagID:                   l.LagID,
		LagName:                 l.LagName,
		LagState:                l.LagState,
		Location:                l.Location,
		MacSecKeys:              toMacSecKeyWire(l.MacSecKeys),
		OwnerAccount:            l.OwnerAccount,
		ProviderName:            l.ProviderName,
		Region:                  l.Region,
		Tags:                    toTagWire(l.Tags),
		MinimumLinks:            l.MinimumLinks,
		NumberOfConnections:     l.NumberOfConnections,
		AllowsHostedConnections: l.AllowsHostedConnections,
		JumboFrameCapable:       l.JumboFrameCapable,
		MacSecCapable:           l.MacSecCapable,
	}
}

func toInterconnectWire(i *Interconnect) interconnectWire {
	return interconnectWire{
		AwsDeviceV2:          i.AwsDeviceV2,
		AwsLogicalDeviceID:   i.AwsLogicalDeviceID,
		Bandwidth:            i.Bandwidth,
		EncryptionMode:       i.EncryptionMode,
		HasLogicalRedundancy: i.HasLogicalRedundancy,
		InterconnectID:       i.InterconnectID,
		InterconnectName:     i.InterconnectName,
		InterconnectState:    i.InterconnectState,
		LagID:                i.LagID,
		Location:             i.Location,
		MacSecKeys:           toMacSecKeyWire(i.MacSecKeys),
		PortEncryptionStatus: i.PortEncryptionStatus,
		ProviderName:         i.ProviderName,
		Region:               i.Region,
		Tags:                 toTagWire(i.Tags),
		JumboFrameCapable:    i.JumboFrameCapable,
		MacSecCapable:        i.MacSecCapable,
	}
}

// toVirtualInterfaceWire converts a VirtualInterface to its wire shape.
// amazonSideAsn is the ASN of the DirectConnectGateway this VIF is attached
// to (looked up by the caller, see models.go's VirtualInterface doc
// comment for why this is not stored on the VIF itself) -- nil if the VIF
// has no DirectConnectGatewayId or the gateway is unknown.
func toVirtualInterfaceWire(v *VirtualInterface, amazonSideAsn *int64) virtualInterfaceWire {
	asn, asnLong := asnWireFields(v.AsnValue, v.AsnProvided)

	var mtu *int32
	if v.Mtu != 0 {
		m := v.Mtu
		mtu = &m
	}

	return virtualInterfaceWire{
		AmazonSideAsn:          amazonSideAsn,
		AsnLong:                asnLong,
		Mtu:                    mtu,
		AddressFamily:          v.AddressFamily,
		AmazonAddress:          v.AmazonAddress,
		AuthKey:                v.AuthKey,
		AwsDeviceV2:            v.AwsDeviceV2,
		AwsLogicalDeviceID:     v.AwsLogicalDeviceID,
		BgpPeers:               toBGPPeerWire(v.BgpPeers),
		ConnectionID:           v.ConnectionID,
		CustomerAddress:        v.CustomerAddress,
		CustomerRouterConfig:   v.CustomerRouterConfig,
		DirectConnectGatewayID: v.DirectConnectGatewayID,
		Location:               v.Location,
		OwnerAccount:           v.OwnerAccount,
		RateLimit:              v.RateLimit,
		Region:                 v.Region,
		RouteFilterPrefixes:    toRouteFilterPrefixWire(v.RouteFilterPrefixes),
		Tags:                   toTagWire(v.Tags),
		VirtualGatewayID:       v.VirtualGatewayID,
		VirtualInterfaceID:     v.VirtualInterfaceID,
		VirtualInterfaceName:   v.VirtualInterfaceName,
		VirtualInterfaceState:  v.VirtualInterfaceState,
		VirtualInterfaceType:   v.VirtualInterfaceType,
		Vlan:                   v.Vlan,
		Asn:                    asn,
		JumboFrameCapable:      v.JumboFrameCapable,
		SiteLinkEnabled:        v.SiteLinkEnabled,
	}
}

func toGatewayWire(g *DirectConnectGateway) directConnectGatewayWire {
	var asn *int64
	if g.AmazonSideAsn != 0 {
		v := g.AmazonSideAsn
		asn = &v
	}

	return directConnectGatewayWire{
		AmazonSideAsn:             asn,
		DirectConnectGatewayID:    g.DirectConnectGatewayID,
		DirectConnectGatewayName:  g.DirectConnectGatewayName,
		DirectConnectGatewayState: g.DirectConnectGatewayState,
		OwnerAccount:              g.OwnerAccount,
		StateChangeError:          g.StateChangeError,
		Tags:                      toTagWire(g.Tags),
	}
}

func toAssociationWire(a *GatewayAssociation) gatewayAssociationWire {
	w := gatewayAssociationWire{
		AllowedPrefixesToDirectConnectGateway: toRouteFilterPrefixWire(a.AllowedPrefixes),
		AssociationID:                         a.AssociationID,
		AssociationState:                      a.AssociationState,
		DirectConnectGatewayID:                a.DirectConnectGatewayID,
		DirectConnectGatewayOwnerAccount:      a.DirectConnectGatewayOwnerAccount,
		StateChangeError:                      a.StateChangeError,
	}

	if a.GatewayID != "" {
		w.AssociatedGateway = &associatedGatewayWire{
			ID:           a.GatewayID,
			OwnerAccount: a.GatewayOwnerAccount,
			Region:       a.GatewayRegion,
			Type:         a.GatewayType,
		}
	}

	// The legacy VirtualGatewayId/VirtualGatewayOwnerAccount fields are kept
	// in sync whenever the association names a virtual private gateway --
	// see models.go's GatewayAssociation doc comment.
	if a.GatewayType == GatewayTypeVirtualPrivateGateway {
		w.VirtualGatewayID = a.GatewayID
		w.VirtualGatewayOwnerAccount = a.GatewayOwnerAccount
	}

	return w
}

func toProposalWire(p *GatewayAssociationProposal) gatewayAssociationProposalWire {
	w := gatewayAssociationProposalWire{
		DirectConnectGatewayID:                        p.DirectConnectGatewayID,
		DirectConnectGatewayOwnerAccount:              p.DirectConnectGatewayOwnerAccount,
		ExistingAllowedPrefixesToDirectConnectGateway: toRouteFilterPrefixWire(p.ExistingAllowedPrefixes),
		ProposalID:    p.ProposalID,
		ProposalState: p.ProposalState,
		RequestedAllowedPrefixesToDirectConnectGateway: toRouteFilterPrefixWire(p.RequestedAllowedPrefixes),
	}

	if p.GatewayID != "" {
		w.AssociatedGateway = &associatedGatewayWire{
			ID:           p.GatewayID,
			OwnerAccount: p.GatewayOwnerAccount,
			Region:       p.GatewayRegion,
			Type:         p.GatewayType,
		}
	}

	return w
}

func toVifTestHistoryWire(t *VifTestHistory) vifTestHistoryWire {
	var duration *int32
	if t.TestDurationInMinutes != 0 {
		d := t.TestDurationInMinutes
		duration = &d
	}

	return vifTestHistoryWire{
		EndTime:               epochPtr(t.EndTime),
		StartTime:             epochPtr(t.StartTime),
		TestDurationInMinutes: duration,
		TestID:                t.TestID,
		VirtualInterfaceID:    t.VirtualInterfaceID,
		Status:                t.Status,
		OwnerAccount:          t.OwnerAccount,
		BgpPeers:              cloneStrs(t.BgpPeers),
	}
}
