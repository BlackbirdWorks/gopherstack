package accessanalyzer

import (
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// InMemoryBackend implements StorageBackend using in-memory maps.
type InMemoryBackend struct {
	mu       *lockmetrics.RWMutex
	registry *store.Registry

	analyzers *store.Table[Analyzer] // key: name

	// archiveRules was previously nested (analyzerName -> ruleName ->
	// *ArchiveRule); it is now one flat table keyed by the composite
	// "analyzerName|ruleName" string (see archiveRuleKey in store_setup.go),
	// with archiveRulesByAnalyzer grouping entries by analyzer for the "all
	// archive rules of analyzer X" lookups the nested map used to answer
	// directly. "Dirty" table (ArchiveRule.AnalyzerName is json:"-"): NOT on
	// b.registry, persisted via a DTO in persistence.go.
	archiveRules           *store.Table[ArchiveRule]
	archiveRulesByAnalyzer *store.Index[ArchiveRule]

	// findings was previously nested (analyzerName -> findingID -> *Finding).
	// findingID (a UUID minted by AddFinding) is globally unique, so this is
	// a flat table keyed directly by Finding.ID, with findingsByAnalyzer
	// grouping entries by analyzer name -- derived from the already
	// wire-visible AnalyzerArn field, see analyzerNameFromArn -- for the "all
	// findings of analyzer X" lookups the nested map used to answer directly.
	findings           *store.Table[Finding]
	findingsByAnalyzer *store.Index[Finding]

	// tags maps resourceARN -> tags; its value (map[string]string) has no
	// identity of its own, so it is not a store.Table candidate and remains
	// a plain persisted map.
	tags map[string]map[string]string

	policyGenerations *store.Table[PolicyGeneration] // key: jobID

	accessPreviews *store.Table[AccessPreview] // key: id

	// analyzedResources is keyed by the composite "analyzerArn|resourceArn"
	// string (see analyzedResourceKey in store_setup.go) -- both halves are
	// real, already wire-visible fields on AnalyzedResource, so no hidden
	// field was needed; a resource can be observed by more than one
	// analyzer, so ResourceArn alone is not a valid primary key.
	analyzedResources *store.Table[AnalyzedResource]

	findingRecommendations *store.Table[FindingRecommendation] // key: id (== findingID)

	accountID string
	region    string
}

// NewInMemoryBackend constructs a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		mu:        lockmetrics.New("accessanalyzer"),
		registry:  store.NewRegistry(),
		accountID: accountID,
		region:    region,
		tags:      make(map[string]map[string]string),
	}

	registerAllTables(b)

	return b
}

// analyzerARN returns the ARN for an analyzer by name.
func (b *InMemoryBackend) analyzerARN(name string) string {
	return arn.Build("access-analyzer", b.region, b.accountID, fmt.Sprintf("analyzer/%s", name))
}

// Reset clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	// archiveRules is a "dirty" table (see store_setup.go), so it is not on
	// b.registry and needs its own Reset() call here.
	b.archiveRules.Reset()
	b.tags = make(map[string]map[string]string)
}

// Region returns the backend's region.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the backend's account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Snapshot and Restore (persistence.Persistable) live in persistence.go,
// alongside the archiveRules DTO their registry-table snapshot needs.
