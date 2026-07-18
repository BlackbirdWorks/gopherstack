package verifiedpermissions

import (
	"encoding/base64"
	"fmt"
	"sort"
	"time"

	cedar "github.com/cedar-policy/cedar-go"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// timeFormat is the ISO 8601 timestamp format used by Verified Permissions API responses.
const timeFormat = "2006-01-02T15:04:05.000Z"

// arnNoRegion builds an ARN with empty region (verifiedpermissions uses global ARNs).
func arnNoRegion(accountID, resourceType, resourceID string) string {
	return arn.Build("verifiedpermissions", "", accountID, fmt.Sprintf("%s/%s", resourceType, resourceID))
}

// InMemoryBackend is the in-memory store for Verified Permissions resources.
//
// policyStores registers directly on b.registry, keyed by its real
// PolicyStoreID field. policies, policyTemplates, and identitySources were
// previously nested by policy store (map[string]map[string]*T); each is now a
// flat *store.Table keyed by the composite "policyStoreID/id" string (see
// policyKey/policyTemplateKey/identitySourceKey), with a companion
// *store.Index grouping entries by policy store for the per-store scans the
// nested maps used to answer directly -- the same pattern services/codeartifact
// uses for its region-nested maps. All three carry real, wire-visible
// PolicyStoreID fields, so each is a "clean" table registered directly on
// b.registry, no DTO wrapper needed (see persistence.go).
//
// schemas has no wire-visible identity field at all (one schema per policy
// store, keyed only by the outer map's policyStoreID), so it gained a hidden
// policyStoreID field purely for this key; it is a "dirty" table (store.New
// only, deliberately NOT store.Register-ed onto b.registry) round-tripped
// through a DTO wrapper in persistence.go.
//
// arnIndex is a derived cache rebuilt from the tables above on Restore, so it
// is never itself persisted. resourceTags, policySetCache, and policySetDirty
// remain plain maps: resourceTags is a non-*T value map (map[string]string)
// still persisted directly; policySetCache/policySetDirty are ephemeral
// caches that are never persisted.
type InMemoryBackend struct {
	registry               *store.Registry
	policyStores           *store.Table[PolicyStore]
	policies               *store.Table[Policy]
	policiesByStore        *store.Index[Policy]
	policyTemplates        *store.Table[PolicyTemplate]
	policyTemplatesByStore *store.Index[PolicyTemplate]
	identitySources        *store.Table[IdentitySource]
	identitySourcesByStore *store.Index[IdentitySource]
	schemas                *store.Table[PolicyStoreSchema]
	// arnIndex maps ARN -> (resourceType, policyStoreID, resourceID) for O(1) tag ops
	// values are encoded as "policyStore:<id>", "policy:<storeID>:<policyID>", etc.
	arnIndex     map[string]string
	resourceTags map[string]map[string]string // ARN -> tags (all resource types)
	// policySetCache caches the compiled Cedar PolicySet per policy store.
	// Invalidated by CreatePolicy/UpdatePolicy/DeletePolicy.
	policySetCache map[string]*cedar.PolicySet
	policySetDirty map[string]bool
	mu             *lockmetrics.RWMutex
	accountID      string
	region         string
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:       store.NewRegistry(),
		arnIndex:       make(map[string]string),
		resourceTags:   make(map[string]map[string]string),
		policySetCache: make(map[string]*cedar.PolicySet),
		policySetDirty: make(map[string]bool),
		accountID:      accountID,
		region:         region,
		mu:             lockmetrics.New("verifiedpermissions"),
	}

	registerAllTables(b)

	return b
}

// AccountID returns the AWS account ID configured for this backend.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Reset clears all policy store state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	// schemas is a "dirty" table (hidden policyStoreID field) deliberately
	// NOT on b.registry -- see store_setup.go's registerAllTables doc -- so
	// it needs its own Reset() call here.
	b.schemas.Reset()
	b.arnIndex = make(map[string]string)
	b.resourceTags = make(map[string]map[string]string)
	b.policySetCache = make(map[string]*cedar.PolicySet)
	b.policySetDirty = make(map[string]bool)
}

// listByPolicyStore clones every entry in a policy-store-scoped store.Index
// group via cloneFn, sorts the clones by createdAtFn ascending, and paginates
// the result via idFn. It factors out the identical clone/sort/paginate shape
// shared by ListPolicyTemplates and ListIdentitySources (both: look up the
// index group for a policy store, clone into a value slice, sort by creation
// date, paginate by the resource's own ID) so the two call sites differ only
// in the type-specific clone/field accessors passed in.
func listByPolicyStore[V any](
	entries []*V,
	nextToken string,
	maxResults int,
	cloneFn func(*V) V,
	createdAtFn func(V) time.Time,
	idFn func(V) string,
) ([]V, string) {
	out := make([]V, 0, len(entries))
	for _, v := range entries {
		out = append(out, cloneFn(v))
	}

	sort.Slice(out, func(i, j int) bool {
		return createdAtFn(out[i]).Before(createdAtFn(out[j]))
	})

	return paginate(out, nextToken, maxResults, idFn)
}

// paginate slices items starting after nextToken and up to maxResults.
// Returns the page and the next continuation token (empty string if last page).
// Tokens are base64-encoded resource IDs to prevent clients from relying on their format.
func paginate[T any](items []T, nextToken string, maxResults int, idFn func(T) string) ([]T, string) {
	start := 0

	if nextToken != "" {
		rawID := decodePageToken(nextToken)

		for i, item := range items {
			if idFn(item) == rawID {
				start = i + 1

				break
			}
		}
	}

	items = items[start:]

	if maxResults > 0 && len(items) > maxResults {
		return items[:maxResults], encodePageToken(idFn(items[maxResults-1]))
	}

	return items, ""
}

// encodePageToken base64-encodes an ID to produce an opaque pagination token.
func encodePageToken(id string) string {
	return base64.StdEncoding.EncodeToString([]byte(id))
}

// decodePageToken reverses encodePageToken; returns the raw ID or the token itself on failure.
func decodePageToken(token string) string {
	b, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return token
	}

	return string(b)
}

// AddPolicyStoreInternal inserts a pre-built PolicyStore directly into the backend (for test seeding).
func (b *InMemoryBackend) AddPolicyStoreInternal(ps *PolicyStore) {
	b.mu.Lock("AddPolicyStoreInternal")
	defer b.mu.Unlock()

	b.policyStores.Put(ps)
	b.arnIndex[ps.Arn] = arnKindPolicyStore + ":" + ps.PolicyStoreID
}

// AddPolicyInternal inserts a pre-built Policy directly into the backend (for test seeding).
func (b *InMemoryBackend) AddPolicyInternal(p *Policy) {
	b.mu.Lock("AddPolicyInternal")
	defer b.mu.Unlock()

	b.policies.Put(p)
	b.arnIndex[policyARN(b.accountID, p.PolicyStoreID, p.PolicyID)] =
		arnKindPolicy + ":" + p.PolicyStoreID + ":" + p.PolicyID
}

// AddPolicyTemplateInternal inserts a pre-built PolicyTemplate directly into the backend (for test seeding).
func (b *InMemoryBackend) AddPolicyTemplateInternal(pt *PolicyTemplate) {
	b.mu.Lock("AddPolicyTemplateInternal")
	defer b.mu.Unlock()

	b.policyTemplates.Put(pt)
	b.arnIndex[policyTemplateARN(b.accountID, pt.PolicyStoreID, pt.PolicyTemplateID)] =
		arnKindPolicyTemplate + ":" + pt.PolicyStoreID + ":" + pt.PolicyTemplateID
}

// AddIdentitySourceInternal inserts a pre-built IdentitySource directly into the backend (for test seeding).
func (b *InMemoryBackend) AddIdentitySourceInternal(is *IdentitySource) {
	b.mu.Lock("AddIdentitySourceInternal")
	defer b.mu.Unlock()

	b.identitySources.Put(is)
	b.arnIndex[identitySourceARN(b.accountID, is.PolicyStoreID, is.IdentitySourceID)] =
		arnKindIdentitySource + ":" + is.PolicyStoreID + ":" + is.IdentitySourceID
}
