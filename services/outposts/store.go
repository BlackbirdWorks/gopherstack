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
	renewalIdempotency map[string]CreateRenewalResult

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
		renewalIdempotency: make(map[string]CreateRenewalResult),
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

// idHexLen17 is the hex-digit length AWS uses for Outpost/Site/Order/Quote/
// QuoteOption/CapacityTask/LineItem IDs, confirmed via the real ID Pattern
// regexes published on docs.aws.amazon.com/outposts/latest/APIReference/
// (e.g. Outpost.OutpostArn: "^arn:aws([a-z-]+)?:outposts:[a-z\d-]+:\d{12}:outpost/op-[a-f0-9]{17}$").
const idHexLen17 = 17

// randomHexID17 returns a random 17-character lowercase hex string, the
// fixed length every AWS-Outposts-issued ID's hex suffix uses.
func randomHexID17() string {
	buf := make([]byte, idHexLen17/2+1) // ceil(17/2) bytes, trimmed to 17 hex chars below
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("%017x", time.Now().UnixNano())[:idHexLen17]
	}

	return hex.EncodeToString(buf)[:idHexLen17]
}

// newOutpostID generates an Outpost ID matching the confirmed real pattern
// "op-[a-f0-9]{17}" (Outpost.OutpostArn, docs.aws.amazon.com/outposts/latest/
// APIReference/API_Outpost.html).
func newOutpostID() string { return "op-" + randomHexID17() }

// newSiteID generates a Site ID matching the confirmed real pattern
// "os-[a-f0-9]{17}" (Site.SiteArn, docs.aws.amazon.com/outposts/latest/
// APIReference/API_Site.html).
func newSiteID() string { return "os-" + randomHexID17() }

// newOrderID generates an Order ID matching the confirmed real pattern
// "oo-[a-f0-9]{17}" (Order.OrderId, docs.aws.amazon.com/outposts/latest/
// APIReference/API_Order.html).
func newOrderID() string { return "oo-" + randomHexID17() }

// newQuoteID generates a Quote ID matching the confirmed real pattern
// "oq-[a-f0-9]{17}" (Quote.QuoteId, docs.aws.amazon.com/outposts/latest/
// APIReference/API_Quote.html). Quotes also accept an ARN-shaped identifier
// on input ("arn:...:quote/oq-...", confirmed via GetQuote/Order.QuoteIdentifier's
// own Pattern) even though Quote itself has no QuoteArn output field -- see
// resolveQuoteLocked.
func newQuoteID() string { return "oq-" + randomHexID17() }

// newCapacityTaskID generates a CapacityTask ID matching the confirmed real
// pattern "cap-[a-f0-9]{17}" (CapacityTaskId, docs.aws.amazon.com/outposts/
// latest/APIReference/API_GetCapacityTask.html) -- NOT the "ct-" prefix a
// prior pass guessed.
func newCapacityTaskID() string { return "cap-" + randomHexID17() }

// newAssetID generates an Asset ID. AssetId's confirmed pattern is
// "^(\w+)$" (docs.aws.amazon.com/outposts/latest/APIReference/API_StartConnection.html) --
// \w excludes '-', so this deliberately omits the hyphen a prior pass used.
func newAssetID() string { return "asset" + randomHexID() }

// newConnectionID generates a Connection ID. ConnectionId's confirmed
// pattern is "^[a-zA-Z0-9+/=]{1,1024}$" (docs.aws.amazon.com/outposts/latest/
// APIReference/API_StartConnection.html) -- no '-' allowed, so this
// deliberately omits the hyphen a prior pass used.
func newConnectionID() string { return "conn" + randomHexID() }

// newLineItemID generates a LineItem ID matching the confirmed real pattern
// "ooi-[a-f0-9]{17}" (LineItem.LineItemId, docs.aws.amazon.com/outposts/
// latest/APIReference/API_LineItem.html) -- NOT the "li-" prefix a prior
// pass guessed.
func newLineItemID() string { return "ooi-" + randomHexID17() }

// newQuoteOptionID generates a QuoteOption ID matching the confirmed real
// pattern "oqo-[a-f0-9]{17}" (Order.QuoteOptionIdentifier, docs.aws.amazon.com/
// outposts/latest/APIReference/API_Order.html) -- NOT the "qo-" prefix a
// prior pass guessed.
func newQuoteOptionID() string { return "oqo-" + randomHexID17() }

// newSubscriptionID generates a Subscription ID. Subscription.SubscriptionId's
// confirmed pattern is the unconstrained "^[\S \n]+$" (docs.aws.amazon.com/
// outposts/latest/APIReference/API_Subscription.html), so any non-whitespace
// token is real-shaped; "sub-" + hex remains a reasonable, uncontradicted
// choice.
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
