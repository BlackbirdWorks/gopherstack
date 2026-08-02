package networkmanager

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
	"github.com/blackbirdworks/gopherstack/pkgs/worker"
)

// InMemoryBackend is the in-memory store for AWS Network Manager.
//
// A single coarse lock guards every collection below: this service spans two
// related product halves (Global Networks and Cloud WAN) with one real
// structural bridge between them (ConnectPeerAssociation binds a Cloud WAN
// ConnectPeer to a Global-Networks Device/Link), plus a versioned core
// network policy state machine and async attachment/peering/route-analysis
// transitions -- all cross-map transactions, so the invariant boundary is
// the whole backend (see .claude/memories/pkgs-catalog.md's locking rule).
type InMemoryBackend struct {
	globalNetworks   *store.Table[GlobalNetwork]
	sites            *store.Table[Site]
	devices          *store.Table[Device]
	links            *store.Table[Link]
	linkAssociations *store.Table[LinkAssociation]
	connections      *store.Table[Connection]

	customerGatewayAssociations           *store.Table[CustomerGatewayAssociation]
	transitGatewayRegistrations           *store.Table[TransitGatewayRegistration]
	transitGatewayConnectPeerAssociations *store.Table[TransitGatewayConnectPeerAssociation]
	connectPeerAssociations               *store.Table[ConnectPeerAssociation]

	connectPeers *store.Table[ConnectPeer]

	coreNetworks        *store.Table[CoreNetwork]
	policyHistories     *store.Table[CoreNetworkPolicyHistory]
	prefixListAssocs    *store.Table[CoreNetworkPrefixListAssociation]
	routingPolicyLabels *store.Table[AttachmentRoutingPolicyLabel]

	attachments *store.Table[Attachment]
	peerings    *store.Table[Peering]

	routeAnalyses *store.Table[RouteAnalysis]

	resourceMetadata *store.Table[networkResourceMetadata]
	resourcePolicies *store.Table[resourcePolicy]

	orgStatus *organizationStatus

	registry *store.Registry

	mu        *lockmetrics.RWMutex
	work      *worker.Group
	accountID string
	region    string
}

// NewInMemoryBackend creates a new in-memory AWS Network Manager backend.
func NewInMemoryBackend(ctx context.Context, accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:  store.NewRegistry(),
		accountID: accountID,
		region:    region,
		mu:        lockmetrics.New("networkmanager"),
		work:      worker.NewGroup(ctx, "networkmanager"),
		orgStatus: &organizationStatus{OrganizationAwsServiceAccessStatus: orgAccessStatusDisabled},
	}
	registerAllTables(b)

	return b
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the account ID for this backend.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Close stops all scheduled state-transition timers so none outlives the
// backend. Safe to call multiple times.
func (b *InMemoryBackend) Close() { b.work.Stop() }

// Reset clears all stored state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	closeAllTags(b)
	b.registry.ResetAll()
	b.orgStatus = &organizationStatus{OrganizationAwsServiceAccessStatus: orgAccessStatusDisabled}
}

// closeAllTags releases every tags.Tags instance's Prometheus registration
// before the underlying tables are cleared. Callers must hold b.mu.
func closeAllTags(b *InMemoryBackend) {
	for _, v := range b.globalNetworks.All() {
		closeTags(v.Tags)
	}

	for _, v := range b.sites.All() {
		closeTags(v.Tags)
	}

	for _, v := range b.devices.All() {
		closeTags(v.Tags)
	}

	for _, v := range b.links.All() {
		closeTags(v.Tags)
	}

	for _, v := range b.connections.All() {
		closeTags(v.Tags)
	}

	for _, v := range b.connectPeers.All() {
		closeTags(v.Tags)
	}

	for _, v := range b.coreNetworks.All() {
		closeTags(v.Tags)
	}

	for _, v := range b.attachments.All() {
		closeTags(v.Tags)
	}

	for _, v := range b.peerings.All() {
		closeTags(v.Tags)
	}
}

func closeTags(t interface{ Close() }) {
	if t != nil {
		t.Close()
	}
}

// ---- ARN builders ----
//
// All 9 taggable NetworkManager resource kinds are GLOBAL ARNs with no
// region segment (arn:${Partition}:networkmanager::${Account}:<kind>/${id},
// confirmed directly from AWS's own IAM Service Authorization Reference
// page -- see PARITY.md). pkgs/arn.BuildGlobal already exists for exactly
// this shape (added for DirectConnect's dx-gateway); reused here rather than
// adding a new "networkmanager" special case to arn.Build, since every
// resource kind in this service is global, not just one.

func (b *InMemoryBackend) globalNetworkARN(id string) string {
	return arn.BuildGlobal("networkmanager", b.region, b.accountID, "global-network/"+id)
}

func (b *InMemoryBackend) siteARN(globalNetworkID, id string) string {
	return arn.BuildGlobal("networkmanager", b.region, b.accountID, "site/"+globalNetworkID+"/"+id)
}

func (b *InMemoryBackend) deviceARN(globalNetworkID, id string) string {
	return arn.BuildGlobal("networkmanager", b.region, b.accountID, "device/"+globalNetworkID+"/"+id)
}

func (b *InMemoryBackend) linkARN(globalNetworkID, id string) string {
	return arn.BuildGlobal("networkmanager", b.region, b.accountID, "link/"+globalNetworkID+"/"+id)
}

func (b *InMemoryBackend) connectionARN(globalNetworkID, id string) string {
	return arn.BuildGlobal("networkmanager", b.region, b.accountID, "connection/"+globalNetworkID+"/"+id)
}

func (b *InMemoryBackend) connectPeerARN(id string) string {
	return arn.BuildGlobal("networkmanager", b.region, b.accountID, "connect-peer/"+id)
}

func (b *InMemoryBackend) coreNetworkARN(id string) string {
	return arn.BuildGlobal("networkmanager", b.region, b.accountID, "core-network/"+id)
}

func (b *InMemoryBackend) attachmentARN(id string) string {
	return arn.BuildGlobal("networkmanager", b.region, b.accountID, "attachment/"+id)
}

func (b *InMemoryBackend) peeringARN(id string) string {
	return arn.BuildGlobal("networkmanager", b.region, b.accountID, "peering/"+id)
}

// ---- ID generation ----
//
// No ID format is confirmed anywhere in this SDK module's doc comments --
// every generator below is a defensible, UNCONFIRMED hex-suffix convention
// matching services/mgn/resiliencehub's identical randomHexID() treatment.

const randomIDBytes = 8

func randomHexID() string {
	buf := make([]byte, randomIDBytes)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("%016x", time.Now().UnixNano())
	}

	return hex.EncodeToString(buf)
}

func newGlobalNetworkID() string { return "global-network-" + randomHexID() }
func newSiteID() string          { return "site-" + randomHexID() }
func newDeviceID() string        { return "device-" + randomHexID() }
func newLinkID() string          { return "link-" + randomHexID() }
func newConnectionID() string    { return "connection-" + randomHexID() }
func newConnectPeerID() string   { return "connect-peer-" + randomHexID() }
func newCoreNetworkID() string   { return "core-network-" + randomHexID() }
func newAttachmentID() string    { return "attachment-" + randomHexID() }
func newPeeringID() string       { return "peering-" + randomHexID() }
func newRouteAnalysisID() string { return "route-analysis-" + randomHexID() }

// nowUTC returns the current time in UTC -- a tiny named helper so call
// sites read as intent, matching services/mgn/directconnect's identical
// helper.
func nowUTC() time.Time { return time.Now().UTC() }

// scheduleAdvance schedules table[id]'s State field to move from `from` to
// `to` after asyncTransitionDelay, but only if it is still `from` when the
// timer fires (a concurrent Delete/Update may have already moved it
// elsewhere). statePtr must return a pointer into the same value stored in
// table so the mutation is visible to subsequent Get calls.
func scheduleAdvance[V any](
	b *InMemoryBackend, label string, table *store.Table[V], id string,
	statePtr func(*V) *string, from, to string,
) {
	b.work.After(label, asyncTransitionDelay, func() {
		b.mu.Lock(label)
		defer b.mu.Unlock()

		if v, ok := table.Get(id); ok {
			sp := statePtr(v)
			if *sp == from {
				*sp = to
			}
		}
	})
}

// scheduleRemoval schedules table[id] for deletion after
// asyncTransitionDelay, simulating the DELETING -> gone tail of every
// delete-lifecycle op in this service.
func scheduleRemoval[V any](b *InMemoryBackend, label string, table *store.Table[V], id string) {
	b.work.After(label, asyncTransitionDelay, func() {
		b.mu.Lock(label)
		defer b.mu.Unlock()

		table.Delete(id)
	})
}
