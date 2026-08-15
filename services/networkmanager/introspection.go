package networkmanager

import (
	"encoding/json"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// This file implements PARITY.md family T (network introspection, 5 ops)
// and family U (UpdateNetworkResourceMetadata, 1 op).
//
// Honesty bar per PARITY.md: GetNetworkResources/GetNetworkResourceCounts/
// GetNetworkResourceRelationships are real, mechanical rollups over this
// backend's own already-modeled state (devices/sites/links/connections/
// core-networks/attachments/connect-peers/peerings) -- never fabricated.
// GetNetworkResources.Definition is a JSON serialization of the resource's
// OWN already-known NetworkManager-side attributes, NOT a real
// cross-service Describe call into services/ec2 (real AWS's own documented
// behavior) -- this package has no live cross-service backend reference
// (see attachments.go's identical scope note), so this is itself a scoped-
// down simplification of the ideal, loudly flagged here rather than
// pretended away. GetNetworkRoutes/ListCoreNetworkRoutingInformation (the
// latter in corenetworks.go) return an honest empty route list -- no route-
// propagation engine exists to derive one from. GetNetworkTelemetry's
// ConnectionHealth.Status is UP for every Connection/ConnectPeer this
// backend has advanced to AVAILABLE, and nothing else -- a deterministic,
// non-fabricated default, never invented flapping/degraded values (see
// PARITY.md's gaps list).

// networkResourceItem is the concrete internal shape gathered per resource
// before conversion to the wire NetworkResource. ResourceID/Tags mirror the
// real NetworkResource.ResourceId/Tags members -- both are one field access
// away on every source struct (SiteID/DeviceID/.../Tags) but were dropped
// entirely by every gatherer below until this fix (gopherstack-6flj).
type networkResourceItem struct {
	Tags          *tags.Tags
	Arn           string
	ResourceID    string
	ResourceType  string
	CoreNetworkID string
	Definition    string
}

// gatherNetworkResources collects every NetworkManager-side resource
// belonging to globalNetworkID -- devices/sites/links/connections directly,
// plus core-networks/attachments/connect-peers/peerings reachable through
// this global network's core networks. Split into one small per-kind
// helper below rather than one long function, both for readability and to
// keep cognitive complexity low. Callers must hold b.mu (read lock
// suffices).
func (b *InMemoryBackend) gatherNetworkResources(globalNetworkID string) []networkResourceItem {
	coreItems, coreNetworkIDs := b.coreNetworkResourceItems(globalNetworkID)
	groups := [][]networkResourceItem{
		b.siteResourceItems(globalNetworkID),
		b.deviceResourceItems(globalNetworkID),
		b.linkResourceItems(globalNetworkID),
		b.connectionResourceItems(globalNetworkID),
		coreItems,
		b.coreNetworkScopedResourceItems(coreNetworkIDs),
	}

	total := 0
	for _, g := range groups {
		total += len(g)
	}

	out := make([]networkResourceItem, 0, total)
	for _, g := range groups {
		out = append(out, g...)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Arn < out[j].Arn })

	return out
}

func (b *InMemoryBackend) siteResourceItems(globalNetworkID string) []networkResourceItem {
	var out []networkResourceItem

	for _, s := range b.sites.Snapshot() {
		if s.GlobalNetworkID == globalNetworkID {
			out = append(out, networkResourceItem{
				Arn: s.SiteArn, ResourceID: s.SiteID, ResourceType: "site", Definition: mustJSON(s), Tags: s.Tags,
			})
		}
	}

	return out
}

func (b *InMemoryBackend) deviceResourceItems(globalNetworkID string) []networkResourceItem {
	var out []networkResourceItem

	for _, d := range b.devices.Snapshot() {
		if d.GlobalNetworkID == globalNetworkID {
			out = append(out, networkResourceItem{
				Arn: d.DeviceArn, ResourceID: d.DeviceID, ResourceType: "device", Definition: mustJSON(d), Tags: d.Tags,
			})
		}
	}

	return out
}

func (b *InMemoryBackend) linkResourceItems(globalNetworkID string) []networkResourceItem {
	var out []networkResourceItem

	for _, l := range b.links.Snapshot() {
		if l.GlobalNetworkID == globalNetworkID {
			out = append(out, networkResourceItem{
				Arn: l.LinkArn, ResourceID: l.LinkID, ResourceType: "link", Definition: mustJSON(l), Tags: l.Tags,
			})
		}
	}

	return out
}

func (b *InMemoryBackend) connectionResourceItems(globalNetworkID string) []networkResourceItem {
	var out []networkResourceItem

	for _, c := range b.connections.Snapshot() {
		if c.GlobalNetworkID == globalNetworkID {
			out = append(out, networkResourceItem{
				Arn: c.ConnectionArn, ResourceID: c.ConnectionID, ResourceType: resourceTypeConnection,
				Definition: mustJSON(c), Tags: c.Tags,
			})
		}
	}

	return out
}

// coreNetworkResourceItems returns the CoreNetwork resource items belonging
// to globalNetworkID plus the set of their CoreNetworkIds, so callers can
// scope attachment/connect-peer/peering lookups (which key off CoreNetworkID,
// not GlobalNetworkID directly) without a second full-table scan.
func (b *InMemoryBackend) coreNetworkResourceItems(
	globalNetworkID string,
) ([]networkResourceItem, map[string]bool) {
	var out []networkResourceItem

	coreNetworkIDs := map[string]bool{}

	for _, cn := range b.coreNetworks.Snapshot() {
		if cn.GlobalNetworkID != globalNetworkID {
			continue
		}

		coreNetworkIDs[cn.CoreNetworkID] = true
		out = append(out, networkResourceItem{
			Arn: cn.CoreNetworkArn, ResourceID: cn.CoreNetworkID, ResourceType: resourceTypeCoreNetwork,
			CoreNetworkID: cn.CoreNetworkID, Definition: mustJSON(cn), Tags: cn.Tags,
		})
	}

	return out, coreNetworkIDs
}

// coreNetworkScopedResourceItems returns every attachment/connect-peer/
// peering whose CoreNetworkID is in coreNetworkIDs.
func (b *InMemoryBackend) coreNetworkScopedResourceItems(
	coreNetworkIDs map[string]bool,
) []networkResourceItem {
	var out []networkResourceItem

	for _, a := range b.attachments.Snapshot() {
		if coreNetworkIDs[a.CoreNetworkID] {
			out = append(out, networkResourceItem{
				Arn: a.ResourceArn, ResourceID: a.AttachmentID, ResourceType: resourceTypeAttachment,
				CoreNetworkID: a.CoreNetworkID, Definition: mustJSON(a), Tags: a.Tags,
			})
		}
	}

	for _, c := range b.connectPeers.Snapshot() {
		if coreNetworkIDs[c.CoreNetworkID] {
			out = append(out, networkResourceItem{
				Arn: b.connectPeerARN(c.ConnectPeerID), ResourceID: c.ConnectPeerID,
				ResourceType:  resourceTypeConnectPeer,
				CoreNetworkID: c.CoreNetworkID, Definition: mustJSON(c), Tags: c.Tags,
			})
		}
	}

	for _, p := range b.peerings.Snapshot() {
		if coreNetworkIDs[p.CoreNetworkID] {
			out = append(out, networkResourceItem{
				Arn: p.ResourceArn, ResourceID: p.PeeringID, ResourceType: "peering", CoreNetworkID: p.CoreNetworkID,
				Definition: mustJSON(p), Tags: p.Tags,
			})
		}
	}

	return out
}

func mustJSON(v any) string {
	data, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}

	return string(data)
}

// networkResourceFilter is the common optional-filter set every family T
// op takes.
type networkResourceFilter struct {
	CoreNetworkID string
	ResourceArn   string
	ResourceType  string
}

func (f networkResourceFilter) matches(item networkResourceItem) bool {
	if f.CoreNetworkID != "" && item.CoreNetworkID != f.CoreNetworkID {
		return false
	}

	if f.ResourceArn != "" && item.Arn != f.ResourceArn {
		return false
	}

	if f.ResourceType != "" && item.ResourceType != f.ResourceType {
		return false
	}

	return true
}

func (b *InMemoryBackend) GetNetworkResources(
	globalNetworkID string, filter networkResourceFilter, token string, limit int,
) (page.Page[networkResourceItem], error) {
	b.mu.RLock("GetNetworkResources")
	defer b.mu.RUnlock()

	if !b.globalNetworkExists(globalNetworkID) {
		return page.Page[networkResourceItem]{}, notFoundError(
			resourceGlobalNetwork,
			globalNetworkID,
		)
	}

	all := b.gatherNetworkResources(globalNetworkID)

	filtered := all[:0:0]

	for _, item := range all {
		if filter.matches(item) {
			filtered = append(filtered, item)
		}
	}

	return page.New(filtered, token, limit, defaultPageLimit), nil
}

// GetNetworkResourceCounts deliberately does NOT validate globalNetworkID
// existence -- PARITY.md confirms this is the one Get* op in this family
// with no ResourceNotFoundException in its real error set, unlike its three
// siblings; an unknown GlobalNetworkID here honestly returns zero counts
// rather than an error the real SDK client has no typed case for.
func (b *InMemoryBackend) GetNetworkResourceCounts(
	globalNetworkID, resourceType string,
) map[string]int32 {
	b.mu.RLock("GetNetworkResourceCounts")
	defer b.mu.RUnlock()

	counts := map[string]int32{}

	for _, item := range b.gatherNetworkResources(globalNetworkID) {
		if resourceType != "" && item.ResourceType != resourceType {
			continue
		}

		counts[item.ResourceType]++
	}

	return counts
}

// networkRelationship is a directed From/To ARN edge.
type networkRelationship struct {
	From string
	To   string
}

// deviceSiteRelationships returns one Device->Site edge per device that
// names a SiteID belonging to globalNetworkID.
func (b *InMemoryBackend) deviceSiteRelationships(globalNetworkID string) []networkRelationship {
	var rels []networkRelationship

	for _, d := range b.devices.Snapshot() {
		if d.GlobalNetworkID != globalNetworkID || d.SiteID == "" {
			continue
		}

		if s, ok := b.sites.Get(d.SiteID); ok {
			rels = append(rels, networkRelationship{From: d.DeviceArn, To: s.SiteArn})
		}
	}

	return rels
}

// linkSiteRelationships returns one Link->Site edge per link that names a
// SiteID belonging to globalNetworkID.
func (b *InMemoryBackend) linkSiteRelationships(globalNetworkID string) []networkRelationship {
	var rels []networkRelationship

	for _, l := range b.links.Snapshot() {
		if l.GlobalNetworkID != globalNetworkID || l.SiteID == "" {
			continue
		}

		if s, ok := b.sites.Get(l.SiteID); ok {
			rels = append(rels, networkRelationship{From: l.LinkArn, To: s.SiteArn})
		}
	}

	return rels
}

// deviceLinkRelationships returns one Device->Link edge per LinkAssociation
// in globalNetworkID.
func (b *InMemoryBackend) deviceLinkRelationships(globalNetworkID string) []networkRelationship {
	var rels []networkRelationship

	for _, la := range b.linkAssociations.Snapshot() {
		if la.GlobalNetworkID != globalNetworkID {
			continue
		}

		d, dok := b.devices.Get(la.DeviceID)
		l, lok := b.links.Get(la.LinkID)

		if dok && lok {
			rels = append(rels, networkRelationship{From: d.DeviceArn, To: l.LinkArn})
		}
	}

	return rels
}

// attachmentCoreNetworkRelationships returns one <underlying-resource>->
// CoreNetwork edge per attachment whose CoreNetworkID is in coreNetworkIDs.
func (b *InMemoryBackend) attachmentCoreNetworkRelationships(
	coreNetworkIDs map[string]bool,
) []networkRelationship {
	var rels []networkRelationship

	for _, a := range b.attachments.Snapshot() {
		if coreNetworkIDs[a.CoreNetworkID] && a.ResourceArn != "" {
			rels = append(rels, networkRelationship{From: a.ResourceArn, To: a.CoreNetworkArn})
		}
	}

	return rels
}

func (b *InMemoryBackend) coreNetworkIDsFor(globalNetworkID string) map[string]bool {
	coreNetworkIDs := map[string]bool{}

	for _, cn := range b.coreNetworks.Snapshot() {
		if cn.GlobalNetworkID == globalNetworkID {
			coreNetworkIDs[cn.CoreNetworkID] = true
		}
	}

	return coreNetworkIDs
}

func (b *InMemoryBackend) GetNetworkResourceRelationships(
	globalNetworkID string, filter networkResourceFilter, token string, limit int,
) (page.Page[networkRelationship], error) {
	b.mu.RLock("GetNetworkResourceRelationships")
	defer b.mu.RUnlock()

	if !b.globalNetworkExists(globalNetworkID) {
		return page.Page[networkRelationship]{}, notFoundError(
			resourceGlobalNetwork,
			globalNetworkID,
		)
	}

	relGroups := [][]networkRelationship{
		b.deviceSiteRelationships(globalNetworkID),
		b.linkSiteRelationships(globalNetworkID),
		b.deviceLinkRelationships(globalNetworkID),
		b.attachmentCoreNetworkRelationships(b.coreNetworkIDsFor(globalNetworkID)),
	}

	total := 0
	for _, g := range relGroups {
		total += len(g)
	}

	rels := make([]networkRelationship, 0, total)
	for _, g := range relGroups {
		rels = append(rels, g...)
	}

	out := rels[:0:0]

	for _, r := range rels {
		if filter.ResourceArn == "" || r.From == filter.ResourceArn || r.To == filter.ResourceArn {
			out = append(out, r)
		}
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].From != out[j].From {
			return out[i].From < out[j].From
		}

		return out[i].To < out[j].To
	})

	return page.New(out, token, limit, defaultPageLimit), nil
}

// GetNetworkRoutes returns an honest empty route list -- see this file's
// doc comment. It still echoes back the resolved RouteTableType/
// RouteTableArn derived from whichever RouteTableIdentifier field the
// caller supplied, and validates GlobalNetworkID.
func (b *InMemoryBackend) GetNetworkRoutes(
	globalNetworkID, transitGatewayRouteTableArn string, hasCoreNetworkSegmentEdge bool,
) (string, string, error) {
	b.mu.RLock("GetNetworkRoutes")
	defer b.mu.RUnlock()

	if !b.globalNetworkExists(globalNetworkID) {
		return "", "", notFoundError(resourceGlobalNetwork, globalNetworkID)
	}

	if transitGatewayRouteTableArn != "" {
		return transitGatewayRouteTableArn, routeTableTypeTransitGatewayRoute, nil
	}

	if hasCoreNetworkSegmentEdge {
		return "", routeTableTypeCoreNetworkSegment, nil
	}

	return "", "", nil
}

// GetNetworkTelemetry emits a deterministic Health.Status: UP entry for
// every Connection/ConnectPeer already AVAILABLE and in scope --
// see this file's doc comment for why nothing else is ever reported.
func (b *InMemoryBackend) GetNetworkTelemetry(
	globalNetworkID string, filter networkResourceFilter, token string, limit int,
) (page.Page[networkTelemetryWire], error) {
	b.mu.RLock("GetNetworkTelemetry")
	defer b.mu.RUnlock()

	if !b.globalNetworkExists(globalNetworkID) {
		return page.Page[networkTelemetryWire]{}, notFoundError(
			resourceGlobalNetwork,
			globalNetworkID,
		)
	}

	var out []networkTelemetryWire

	for _, c := range b.connections.Snapshot() {
		if c.GlobalNetworkID != globalNetworkID || c.State != stateAvailable {
			continue
		}

		item := networkResourceItem{Arn: c.ConnectionArn, ResourceType: resourceTypeConnection}
		if !filter.matches(item) {
			continue
		}

		out = append(out, networkTelemetryWire{
			AccountID: b.accountID, AwsRegion: b.region, ResourceArn: c.ConnectionArn, ResourceID: c.ConnectionID,
			ResourceType: resourceTypeConnection,
			Health: &connectionHealthWire{
				Status:    connectionStatusUp,
				Timestamp: epochPtr(nowUTC()),
			},
		})
	}

	coreNetworkIDs := map[string]bool{}
	for _, cn := range b.coreNetworks.Snapshot() {
		if cn.GlobalNetworkID == globalNetworkID {
			coreNetworkIDs[cn.CoreNetworkID] = true
		}
	}

	for _, c := range b.connectPeers.Snapshot() {
		if !coreNetworkIDs[c.CoreNetworkID] || c.State != connectPeerStateAvailable {
			continue
		}

		arn := b.connectPeerARN(c.ConnectPeerID)

		item := networkResourceItem{
			Arn:           arn,
			ResourceType:  resourceTypeConnectPeer,
			CoreNetworkID: c.CoreNetworkID,
		}
		if !filter.matches(item) {
			continue
		}

		out = append(out, networkTelemetryWire{
			AccountID: b.accountID, AwsRegion: b.region, CoreNetworkID: c.CoreNetworkID, ResourceArn: arn,
			ResourceID: c.ConnectPeerID, ResourceType: resourceTypeConnectPeer,
			Health: &connectionHealthWire{
				Status:    connectionStatusUp,
				Timestamp: epochPtr(nowUTC()),
			},
		})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ResourceArn < out[j].ResourceArn })

	return page.New(out, token, limit, defaultPageLimit), nil
}

// ---- Update network resource metadata (family U) ----

func (b *InMemoryBackend) UpdateNetworkResourceMetadata(
	globalNetworkID, resourceArn string, metadata map[string]string,
) (map[string]string, error) {
	b.mu.Lock("UpdateNetworkResourceMetadata")
	defer b.mu.Unlock()

	if !b.globalNetworkExists(globalNetworkID) {
		return nil, notFoundError(resourceGlobalNetwork, globalNetworkID)
	}

	m := &networkResourceMetadata{ResourceArn: resourceArn, Metadata: cloneStrMap(metadata)}
	b.resourceMetadata.Put(m)

	return m.clone().Metadata, nil
}
