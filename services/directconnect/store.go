package directconnect

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
	"github.com/blackbirdworks/gopherstack/pkgs/worker"
)

// EC2GatewayResolver lets this backend validate/resolve a virtual private
// gateway or transit gateway id against the real services/ec2 backend,
// rather than accepting any string unchecked -- see PARITY.md's "EC2
// VPN/Transit Gateway binding" section. Both methods return ok=false when
// the id does not resolve to a real EC2 resource. Optional: a nil resolver
// (the default) means every GatewayId/VirtualGatewayId is accepted
// unvalidated, which is still honest (this service's own SDK does not
// require a specific VGW/TGW to exist either) but forgoes the cross-service
// check when EC2 isn't wired in (e.g. isolated unit tests).
type EC2GatewayResolver interface {
	// ResolveVpnGateway reports whether id names a real EC2 VpnGateway.
	ResolveVpnGateway(id string) bool
	// ResolveTransitGateway reports whether id names a real EC2
	// TransitGateway.
	ResolveTransitGateway(id string) bool
	// VirtualGateways returns every VGW id known to EC2, for
	// DescribeVirtualGateways to proxy from rather than maintaining a
	// second, duplicate VGW store (see PARITY.md).
	VirtualGateways() []string
}

// InMemoryBackend is the in-memory store for AWS Direct Connect.
//
// A single coarse lock guards every collection below: operations routinely
// cross resource boundaries (AssociateConnectionWithLag reads+writes both a
// Connection and its new Lag; DeleteResiliencyPolicy-style FK checks scan
// one table while mutating another), so the invariant boundary is the whole
// backend -- see .claude/memories/pkgs-catalog.md's locking rule.
type InMemoryBackend struct {
	connections       *store.Table[Connection]
	lags              *store.Table[Lag]
	interconnects     *store.Table[Interconnect]
	virtualInterfaces *store.Table[VirtualInterface]
	vifsByConnection  *store.Index[VirtualInterface]
	gateways          *store.Table[DirectConnectGateway]
	associations      *store.Table[GatewayAssociation]
	proposals         *store.Table[GatewayAssociationProposal]
	vifTests          *store.Table[VifTestHistory]
	vifTestsByVif     *store.Index[VifTestHistory]
	registry          *store.Registry

	ec2Resolver EC2GatewayResolver

	mu             *lockmetrics.RWMutex
	work           *worker.Group
	accountID      string
	region         string
	nextPrivateAsn int64
}

// privateAsnRangeStart is the first ASN this backend auto-assigns to a
// DirectConnectGateway when the caller omits AmazonSideAsn. 64512 is the
// first ASN in the 16-bit private-use range (64512-65534) real BGP
// deployments commonly use for this purpose; this SDK gives no specific
// default value anywhere, so this is a documented, defensible placeholder,
// NOT a verified AWS default.
const privateAsnRangeStart = 64512

// NewInMemoryBackend creates a new in-memory AWS Direct Connect backend.
func NewInMemoryBackend(ctx context.Context, accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:       store.NewRegistry(),
		accountID:      accountID,
		region:         region,
		mu:             lockmetrics.New("directconnect"),
		work:           worker.NewGroup(ctx, "directconnect"),
		nextPrivateAsn: privateAsnRangeStart,
	}
	registerAllTables(b)

	return b
}

// SetEC2GatewayResolver wires the backend to validate/resolve VGW/TGW ids
// against the real services/ec2 backend -- see EC2GatewayResolver's doc
// comment. Called from cli.go's wireDirectConnectEC2, mirroring
// wireCloudWatchInfraActions's adapter-setter pattern.
func (b *InMemoryBackend) SetEC2GatewayResolver(r EC2GatewayResolver) {
	b.mu.Lock("SetEC2GatewayResolver")
	defer b.mu.Unlock()

	b.ec2Resolver = r
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

	for _, c := range b.connections.All() {
		if c.Tags != nil {
			c.Tags.Close()
		}
	}

	for _, l := range b.lags.All() {
		if l.Tags != nil {
			l.Tags.Close()
		}
	}

	for _, i := range b.interconnects.All() {
		if i.Tags != nil {
			i.Tags.Close()
		}
	}

	for _, v := range b.virtualInterfaces.All() {
		if v.Tags != nil {
			v.Tags.Close()
		}
	}

	for _, g := range b.gateways.All() {
		if g.Tags != nil {
			g.Tags.Close()
		}
	}

	b.registry.ResetAll()
	b.nextPrivateAsn = privateAsnRangeStart
}

// ConnectionARN builds the ARN for a Connection ID. Confirmed shape (real
// Terraform AWS provider source, internal/service/directconnect/connection.go):
// "arn:{partition}:directconnect:{region}:{account}:dxcon/{id}".
func (b *InMemoryBackend) ConnectionARN(id string) string {
	return arn.Build("directconnect", b.region, b.accountID, "dxcon/"+id)
}

// LagARN builds the ARN for a Lag ID. Confirmed shape (Terraform's lag.go):
// "arn:{partition}:directconnect:{region}:{account}:dxlag/{id}".
func (b *InMemoryBackend) LagARN(id string) string {
	return arn.Build("directconnect", b.region, b.accountID, "dxlag/"+id)
}

// VifARN builds the ARN for a VirtualInterface ID. Confirmed shape
// (Terraform's private_virtual_interface.go, shared across
// private/public/transit): "arn:{partition}:directconnect:{region}:{account}:dxvif/{id}".
func (b *InMemoryBackend) VifARN(id string) string {
	return arn.Build("directconnect", b.region, b.accountID, "dxvif/"+id)
}

// InterconnectARN builds the ARN for an Interconnect ID. UNCONFIRMED shape
// -- PARITY.md: no Terraform-managed resource type exists for Interconnect
// at all (partner-only), so no primary source confirms its ARN
// resource-path segment. This reuses the "dxcon/" segment Connection uses
// (Direct Connect's real console/API historically shares the same
// "dxcon-xxxxxxxx" ID-format prefix between connections and interconnects),
// a defensible analogy, NOT a verified fact.
func (b *InMemoryBackend) InterconnectARN(id string) string {
	return arn.Build("directconnect", b.region, b.accountID, "dxcon/"+id)
}

// GatewayARN builds the ARN for a DirectConnectGateway ID. Confirmed GLOBAL
// (no region segment) shape (Terraform's gateway.go, gatewayARN func:
// `c.GlobalARN(ctx, "directconnect", "dx-gateway/"+id)`) -- see PARITY.md's
// ARN section and pkgs/arn.BuildGlobal's doc comment.
func (b *InMemoryBackend) GatewayARN(id string) string {
	return arn.BuildGlobal("directconnect", b.region, b.accountID, "dx-gateway/"+id)
}

const randomIDBytes = 4

// randomHex returns a random 8-character lowercase hex string, matching
// real Direct Connect's own "dxcon-ffabc123"-shaped IDs (8 hex chars after
// the resource-kind prefix, per AWS's published API examples).
func randomHex() string {
	buf := make([]byte, randomIDBytes)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("%08x", time.Now().UnixNano())
	}

	return hex.EncodeToString(buf)
}

// newConnectionID generates a Connection ID ("dxcon-" + 8 hex chars, per
// AWS's own published API examples).
func newConnectionID() string { return "dxcon-" + randomHex() }

// newLagID generates a Lag ID ("dxlag-" + 8 hex chars).
func newLagID() string { return "dxlag-" + randomHex() }

// newInterconnectID generates an Interconnect ID. Real AWS API examples
// show interconnect IDs sharing the same "dxcon-" prefix as connections
// (e.g. "dxcon-fghkkgc7" in AWS's own documentation) -- reused here rather
// than inventing a distinct "dxic-"-style prefix that has no evidence
// behind it.
func newInterconnectID() string { return "dxcon-" + randomHex() }

// newVifID generates a VirtualInterface ID ("dxvif-" + 8 hex chars).
func newVifID() string { return "dxvif-" + randomHex() }

// newBgpPeerID generates a BGP peer ID. UNCONFIRMED format -- no doc
// comment in this SDK module describes BgpPeerId's shape; "bgp-" + 8 hex
// chars is a defensible analogy to this service's other short IDs.
func newBgpPeerID() string { return "bgp-" + randomHex() }

// newTestID generates a StartBgpFailoverTest TestId. UNCONFIRMED format;
// a UUID is a defensible, collision-free choice matching services/outposts'
// treatment of every unconfirmed ID format it generates.
func newTestID() string { return uuid.NewString() }

// newGatewayID generates a DirectConnectGatewayId. Real AWS API examples
// show these as bare UUIDs (e.g.
// "11460e6f-c1bc-40a7-b8f9-06f4e6a9d1e1" in AWS's own documentation).
func newGatewayID() string { return uuid.NewString() }

// newAssociationID generates a DirectConnectGatewayAssociation
// AssociationId. Real AWS API examples show these as bare UUIDs.
func newAssociationID() string { return uuid.NewString() }

// newProposalID generates a DirectConnectGatewayAssociationProposal
// ProposalId. Real AWS API examples show these as bare UUIDs.
func newProposalID() string { return uuid.NewString() }

// resourceIDFromARN extracts the resource ID from a Direct Connect ARN of
// the form ".../{marker}{id}" (e.g. marker "dxcon/" against
// "arn:aws:directconnect:us-east-1:000000000000:dxcon/dxcon-abc12345"),
// returning ok=false if arnStr does not contain marker.
func resourceIDFromARN(arnStr, marker string) (string, bool) {
	idx := strings.LastIndex(arnStr, marker)
	if idx < 0 {
		return "", false
	}

	id := arnStr[idx+len(marker):]
	if id == "" {
		return "", false
	}

	return id, true
}

// nowUTC returns the current time in UTC -- a tiny named helper so call
// sites read as intent ("record this timestamp now") rather than a bare
// time.Now().UTC() repeated at every call site.
func nowUTC() time.Time { return time.Now().UTC() }

// nextAutoAmazonSideAsn returns the next auto-assigned private ASN for a
// DirectConnectGateway created without an explicit AmazonSideAsn. Callers
// must hold b.mu.
func (b *InMemoryBackend) nextAutoAmazonSideAsn() int64 {
	v := b.nextPrivateAsn
	b.nextPrivateAsn++

	return v
}

// scheduleTransition walks target through steps in order, waiting
// asyncTransitionDelay between each hop, mutating *target directly under
// b.mu -- the shared timer-driven state-machine helper used by every
// resource kind in this service (Connection.ConnectionState,
// Lag.LagState, Interconnect.InterconnectState,
// VirtualInterface.VirtualInterfaceState, DirectConnectGateway.
// DirectConnectGatewayState, GatewayAssociation.AssociationState,
// MacSecKey.State, ...). target must point at a field on a value that stays
// at a fixed address for the backend's lifetime (i.e. a field on a *V
// stored in one of this backend's store.Table[V] collections, never
// replaced by Table.Put after creation) -- see models.go's clone()
// functions, which is why every mutation here is safe to alias.
func (b *InMemoryBackend) scheduleTransition(component string, steps []string, target *string) {
	if len(steps) == 0 {
		return
	}

	var schedule func(i int)

	schedule = func(i int) {
		if i >= len(steps) {
			return
		}

		b.work.After(component, asyncTransitionDelay, func() {
			b.mu.Lock("asyncTransition:" + component)
			*target = steps[i]
			b.mu.Unlock()

			schedule(i + 1)
		})
	}

	schedule(0)
}
