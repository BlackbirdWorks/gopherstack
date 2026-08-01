package outposts

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
	"github.com/blackbirdworks/gopherstack/pkgs/worker"
)

// orderTransitionDelay/capacityTaskTransitionDelay are the simulated delays
// before an async Order/CapacityTask reaches its terminal state, mirroring
// services/grafana's workspaceTransitionDelay.
const (
	orderTransitionDelay        = 100 * time.Millisecond
	capacityTaskTransitionDelay = 100 * time.Millisecond
)

// InMemoryBackend is the in-memory store for AWS Outposts.
//
// A single coarse lock guards every collection below: operations routinely
// cross resource boundaries (CreateOutpost seeds an Asset; StartCapacityTask
// reads an Asset and writes a CapacityTask; CreateOrder reads a Quote and
// writes an Order; TagResource resolves an ARN into either the Outposts or
// Sites table), so the invariant boundary is the whole backend -- see
// .claude/memories/pkgs-catalog.md's locking rule.
type InMemoryBackend struct {
	outposts               *store.Table[Outpost]
	outpostsBySite         *store.Index[Outpost]
	sites                  *store.Table[Site]
	orders                 *store.Table[Order]
	ordersByOutpost        *store.Index[Order]
	quotes                 *store.Table[Quote]
	capacityTasks          *store.Table[CapacityTask]
	capacityTasksByOutpost *store.Index[CapacityTask]
	assets                 *store.Table[Asset]
	assetsByOutpost        *store.Index[Asset]
	connections            *store.Table[Connection]
	registry               *store.Registry

	// renewalIdempotency caches CreateRenewal's response keyed by
	// outpostID+"::"+clientToken so a retried request with the same
	// ClientToken replays the original response instead of recomputing (and
	// re-recording) a new subscription. It is deliberately NOT persisted --
	// losing this cache across a restore only means a replayed ClientToken
	// computes a fresh (but numerically identical, since pricing is
	// deterministic) response rather than reusing the cached one, which is
	// harmless. See renewals.go.
	renewalIdempotency map[string]createRenewalResult

	mu        *lockmetrics.RWMutex
	work      *worker.Group
	accountID string
	region    string
}

// NewInMemoryBackend creates a new in-memory AWS Outposts backend.
func NewInMemoryBackend(ctx context.Context, accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:           store.NewRegistry(),
		accountID:          accountID,
		region:             region,
		mu:                 lockmetrics.New("outposts"),
		work:               worker.NewGroup(ctx, "outposts"),
		renewalIdempotency: make(map[string]createRenewalResult),
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

	for _, o := range b.outposts.All() {
		if o.Tags != nil {
			o.Tags.Close()
		}
	}

	for _, s := range b.sites.All() {
		if s.Tags != nil {
			s.Tags.Close()
		}
	}

	b.registry.ResetAll()
	clear(b.renewalIdempotency)
}

// OutpostARN builds the ARN for an Outpost ID. Confirmed shape (in-repo test
// fixture precedent from services/ec2 and services/route53resolver -- see
// PARITY.md's ARN section): "arn:{partition}:outposts:{region}:{account}:outpost/{id}".
func (b *InMemoryBackend) OutpostARN(id string) string {
	return arn.Build("outposts", b.region, b.accountID, "outpost/"+id)
}

// SiteARN builds the ARN for a Site ID. UNCONFIRMED shape -- no primary
// source (SDK trait, real Terraform provider, or AWS docs) could confirm the
// Site ARN resource-path segment; "site/{id}" is a defensible analogy to the
// confirmed "outpost/{id}" shape, not a verified fact. See PARITY.md.
func (b *InMemoryBackend) SiteARN(id string) string {
	return arn.Build("outposts", b.region, b.accountID, "site/"+id)
}

// resourceIDFromARN extracts the resource ID from an Outposts ARN of the
// form "arn:{partition}:outposts:{region}:{account}:{kind}/{id}", returning
// ok=false if arnStr does not contain marker.
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

const randomIDBytes = 6

// randomHexID returns a random 12-character lowercase hex string.
func randomHexID() string {
	buf := make([]byte, randomIDBytes)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("%012x", time.Now().UnixNano())
	}

	return hex.EncodeToString(buf)
}

// newOutpostID generates an Outpost ID in the "op-xxxxxxxxxxxx" shape real
// Outpost IDs use (visible in this repo's own test fixtures, e.g.
// services/ec2/local_gateway_test.go's "op-1"). This exact-length format is a
// reasonable emulation, not confirmed byte-for-byte (OutpostId is a bare
// *string with no pattern trait reproduced in the Go source).
func newOutpostID() string { return "op-" + randomHexID() }

// newSiteID generates a Site ID. UNCONFIRMED format -- see PARITY.md.
func newSiteID() string { return "os-" + randomHexID() }

// newOrderID generates an Order ID. UNCONFIRMED format -- see PARITY.md.
func newOrderID() string { return "oo-" + randomHexID() }

// newQuoteID generates a Quote ID. UNCONFIRMED format -- see PARITY.md.
func newQuoteID() string { return "oq-" + randomHexID() }

// newCapacityTaskID generates a CapacityTask ID. UNCONFIRMED format -- see
// PARITY.md.
func newCapacityTaskID() string { return "ct-" + randomHexID() }

// newAssetID generates an Asset ID. UNCONFIRMED format -- see PARITY.md.
func newAssetID() string { return "asset-" + randomHexID() }

// newConnectionID generates a Connection ID. UNCONFIRMED format -- see
// PARITY.md.
func newConnectionID() string { return "conn-" + randomHexID() }

// newLineItemID generates a LineItem ID. UNCONFIRMED format -- see
// PARITY.md.
func newLineItemID() string { return "li-" + randomHexID() }

// newQuoteOptionID generates a QuoteOption ID. UNCONFIRMED format -- see
// PARITY.md.
func newQuoteOptionID() string { return "qo-" + randomHexID() }

// newSubscriptionID generates a Subscription ID. UNCONFIRMED format -- see
// PARITY.md.
func newSubscriptionID() string { return "sub-" + randomHexID() }

// cloneStrs returns a deep copy of a string slice (nil-safe).
func cloneStrs(ss []string) []string {
	if ss == nil {
		return nil
	}

	cp := make([]string, len(ss))
	copy(cp, ss)

	return cp
}
