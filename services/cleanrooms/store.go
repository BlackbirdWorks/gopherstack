package cleanrooms

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// InMemoryBackend is the in-memory implementation of StorageBackend.
//
// registry holds every converted resource table directly, including every
// composite-keyed one (see store_setup.go): each derives its store.Table key
// entirely from real, already-wire-visible identity field(s) already present
// on the value type (e.g. ConfiguredTableAnalysisRule carries both
// ConfiguredTableIdentifier and Type; AnalysisTemplate carries both
// MembershipIdentifier and its own ID), so plain registry.SnapshotAll/
// RestoreAll round-trips every table losslessly with no DTO wrapper needed
// anywhere -- unlike services/workmail or services/codeartifact, which each
// hide at least one unexported field behind a DTO. See persistence.go.
//
// tagsByArn is the one field left as a plain (non-store.Table) map: its
// values are map[string]string, not *T, so there is nothing for store.Table
// to key on. It is persisted directly (see persistence.go).
type InMemoryBackend struct {
	registry *store.Registry

	collaborations *store.Table[Collaboration]
	memberships    *store.Table[Membership]

	configuredTables *store.Table[ConfiguredTable]

	ctAnalysisRules        *store.Table[ConfiguredTableAnalysisRule]
	ctAnalysisRulesByTable *store.Index[ConfiguredTableAnalysisRule]

	ctAssociations             *store.Table[ConfiguredTableAssociation]
	ctAssociationsByMembership *store.Index[ConfiguredTableAssociation]

	ctaAnalysisRules              *store.Table[ConfiguredTableAssociationAnalysisRule]
	ctaAnalysisRulesByAssociation *store.Index[ConfiguredTableAssociationAnalysisRule]

	analysisTemplates             *store.Table[AnalysisTemplate]
	analysisTemplatesByMembership *store.Index[AnalysisTemplate]

	privacyBudgetTemplates             *store.Table[PrivacyBudgetTemplate]
	privacyBudgetTemplatesByMembership *store.Index[PrivacyBudgetTemplate]

	idMappingTables             *store.Table[IDMappingTable]
	idMappingTablesByMembership *store.Index[IDMappingTable]

	idNamespaceAssociations             *store.Table[IDNamespaceAssociation]
	idNamespaceAssociationsByMembership *store.Index[IDNamespaceAssociation]

	camaAssociations             *store.Table[ConfiguredAudienceModelAssociation]
	camaAssociationsByMembership *store.Index[ConfiguredAudienceModelAssociation]

	changeRequests                *store.Table[CollaborationChangeRequest]
	changeRequestsByCollaboration *store.Index[CollaborationChangeRequest]

	schemas                *store.Table[Schema]
	schemasByCollaboration *store.Index[Schema]

	schemaAnalysisRules *store.Table[SchemaAnalysisRule]

	protectedQueries             *store.Table[ProtectedQuery]
	protectedQueriesByMembership *store.Index[ProtectedQuery]

	protectedJobs             *store.Table[ProtectedJob]
	protectedJobsByMembership *store.Index[ProtectedJob]

	tagsByArn map[string]map[string]string

	nowFn func() float64
	mu    *lockmetrics.RWMutex

	accountID string
	region    string
	muNow     sync.Mutex
}

// NewInMemoryBackendWithContext creates a backend tied to svcCtx (ignored; no lifecycle goroutines).
func NewInMemoryBackendWithContext(_ context.Context, accountID, region string) *InMemoryBackend {
	return NewInMemoryBackend(accountID, region)
}

// NewInMemoryBackend creates a new in-memory Clean Rooms backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		mu:        lockmetrics.New("cleanrooms"),
		accountID: accountID,
		region:    region,
		registry:  store.NewRegistry(),
		tagsByArn: make(map[string]map[string]string),
		nowFn:     func() float64 { return float64(time.Now().Unix()) },
	}
	registerAllTables(b)

	return b
}

func (b *InMemoryBackend) Region() string { return b.region }

func (b *InMemoryBackend) AccountID() string { return b.accountID }

func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.tagsByArn = make(map[string]map[string]string)
}

// ---- composite keys ----
//
// Every nested resource collection in this backend was previously a
// two-or-three-level map (outer key(s) = a parent identifier or two;
// innermost key = the resource's own identifier or type). Phase 3.3
// flattens each into a single *store.Table keyed by a composite "a|b[|c]"
// string, with a companion *store.Index (where a per-parent scan or cascade
// delete needs one) grouping entries by the parent -- see store_setup.go.
// Every value type already carries the real, wire-visible field(s) each key
// component below reads, so no hidden field or DTO wrapper is needed
// anywhere in this backend.
func membershipKey(membershipID, id string) string { return membershipID + "|" + id }

func collaborationKey(collaborationID, id string) string { return collaborationID + "|" + id }

func ctAnalysisRuleKey(configuredTableID, ruleType string) string {
	return configuredTableID + "|" + ruleType
}

func ctaAnalysisRuleKey(assocID, ruleType string) string { return assocID + "|" + ruleType }

func schemaAnalysisRuleKey(collaborationID, name, ruleType string) string {
	return collaborationKey(collaborationID, name) + "|" + ruleType
}

// listItems ranges over a slice of items (typically the result of a
// store.Index.Get lookup), optionally skipping items where include returns
// false, converts each item to a summary, sorts by the less predicate, then
// paginates.
func listItems[T, S any](
	items []*T,
	include func(*T) bool,
	convert func(*T) *S,
	less func(a, b *S) bool,
	maxResults, nextToken string,
) ([]*S, string) {
	result := make([]*S, 0, len(items))
	for _, t := range items {
		if include != nil && !include(t) {
			continue
		}
		result = append(result, convert(t))
	}
	sort.Slice(result, func(i, j int) bool { return less(result[i], result[j]) })

	return paginate(result, maxResults, nextToken)
}

// listNestedItems ranges over a flat slice of items (typically the result of
// a store.Table.All() account-wide scan), collecting items that satisfy
// match, converts them to summaries, sorts, and paginates.
func listNestedItems[T, S any](
	allItems []*T,
	match func(*T) bool,
	convert func(*T) *S,
	less func(a, b *S) bool,
	maxResults, nextToken string,
) ([]*S, string) {
	var result []*S
	for _, t := range allItems {
		if match(t) {
			result = append(result, convert(t))
		}
	}
	sort.Slice(result, func(i, j int) bool { return less(result[i], result[j]) })

	return paginate(result, maxResults, nextToken)
}

func paginate[T any](items []T, maxResultsStr, nextToken string) ([]T, string) {
	if len(items) == 0 {
		return items, ""
	}
	pageSize := 100
	if maxResultsStr != "" {
		_, _ = fmt.Sscanf(maxResultsStr, "%d", &pageSize)
	}
	if pageSize <= 0 || pageSize > 1000 {
		pageSize = 100
	}
	start := 0
	if nextToken != "" {
		_, _ = fmt.Sscanf(nextToken, "%d", &start)
	}
	if start >= len(items) {
		return []T{}, ""
	}
	end := start + pageSize
	if end >= len(items) {
		return items[start:], ""
	}

	return items[start:end], strconv.Itoa(end)
}

func (b *InMemoryBackend) now() float64 {
	b.muNow.Lock()
	defer b.muNow.Unlock()

	return b.nowFn()
}

func contains(ss []string, s string) bool {
	return slices.Contains(ss, s)
}

func removeFrom(ss []string, s string) []string {
	var out []string
	for _, v := range ss {
		if v != s {
			out = append(out, v)
		}
	}

	return out
}
