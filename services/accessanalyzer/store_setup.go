package accessanalyzer

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend is registered exactly once,
// here, as a *store.Table[T] on b.registry.
//
// analyzers, policyGenerations, accessPreviews, and findingRecommendations
// were already flat (not nested) and each carries a real, wire-visible
// identity field (Name / JobID / ID / ID), so each is a direct
// *store.Table[T] keyed by that field. No policyGenerations/accessPreviews/
// findingRecommendations data was persisted before Phase 3.3 (the pre-refactor
// Snapshot only covered analyzers/archiveRules/findings/tags); registering
// them here means they now round-trip through Snapshot/Restore for free via
// b.registry.SnapshotAll/RestoreAll, which is a persistence-coverage
// expansion, not a behavior change to any operation.
//
// analyzedResources was already flat but has no single identity field: a
// resource can be observed by more than one analyzer, so ResourceArn alone
// cannot be the key. It keeps exactly the composite "analyzerArn|resourceArn"
// key the original code computed inline at every call site (see
// analyzedResourceKey) -- both halves are real, already wire-visible fields
// on AnalyzedResource, so no hidden field was needed.
//
// findings was previously nested (analyzerName -> findingID -> *Finding).
// Finding.ID (a UUID minted by AddFinding) is globally unique, and
// Finding.AnalyzerArn is already a real, wire-visible field, so findings
// becomes a flat *store.Table[Finding] keyed directly by ID, with
// findingsByAnalyzer grouping by analyzer name -- derived from AnalyzerArn
// via analyzerNameFromArn, the same "analyzer/{name}" suffix
// InMemoryBackend.analyzerARN builds -- for the "all findings of analyzer X"
// lookups the nested map answered directly. No hidden field needed.
//
// archiveRules was previously nested (analyzerName -> ruleName -> *ArchiveRule).
// Unlike Finding, ArchiveRule carries nothing that identifies its parent
// analyzer on the wire (RuleName alone is only unique within one analyzer),
// so it gained a new field purely for this composite key: AnalyzerName,
// tagged json:"-" since it is never part of the CreateArchiveRule/
// GetArchiveRule API response (archiveRuleToJSON builds the response by hand
// and never touches it). This makes archiveRules a "dirty" table (per the
// services/codecommit convention): built with store.New only, deliberately
// NOT store.Register-ed onto b.registry, so b.registry.SnapshotAll/
// RestoreAll/ResetAll never touch it directly; persistence.go instead builds
// an ephemeral DTO registry that gives AnalyzerName a real JSON tag, and
// Reset (backend.go) resets it explicitly.
//
// tags remains a plain persisted map: its value (map[string]string) has no
// identity of its own, so there is nothing for store.Table to key on.
import (
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func analyzerKeyFn(v *Analyzer) string { return v.Name }

// archiveRuleKey builds the composite store.Table primary key
// ("analyzerName|ruleName") shared by the flattened archiveRules table.
// Access sites use it directly so the exact same key is used for
// Put/Get/Delete/Has.
func archiveRuleKey(analyzerName, ruleName string) string { return analyzerName + "|" + ruleName }
func archiveRuleKeyFn(v *ArchiveRule) string              { return archiveRuleKey(v.AnalyzerName, v.RuleName) }
func archiveRuleAnalyzerIndexKeyFn(v *ArchiveRule) string { return v.AnalyzerName }

func findingKeyFn(v *Finding) string { return v.ID }

// analyzerNameFromArn extracts the analyzer name from an Access Analyzer ARN
// of the form "arn:{partition}:access-analyzer:{region}:{accountID}:analyzer/{name}"
// -- the exact shape InMemoryBackend.analyzerARN builds. It is the pure
// function findings' byAnalyzer index uses to derive its group key straight
// from the already-persisted, wire-visible AnalyzerArn field, which is why
// Finding (unlike ArchiveRule) needed no hidden field for this conversion.
func analyzerNameFromArn(analyzerArn string) string {
	_, name, ok := strings.Cut(analyzerArn, "analyzer/")
	if !ok {
		return ""
	}

	return name
}

func findingAnalyzerIndexKeyFn(v *Finding) string { return analyzerNameFromArn(v.AnalyzerArn) }

// analyzedResourceKey builds the composite store.Table primary key
// ("analyzerArn|resourceArn") the original nested-map-free code already
// computed inline at every call site.
func analyzedResourceKey(analyzerArn, resourceArn string) string {
	return analyzerArn + "|" + resourceArn
}

func analyzedResourceKeyFn(v *AnalyzedResource) string {
	return analyzedResourceKey(v.AnalyzerArn, v.ResourceArn)
}

func policyGenerationKeyFn(v *PolicyGeneration) string { return v.JobID }

func accessPreviewKeyFn(v *AccessPreview) string { return v.ID }

func findingRecommendationKeyFn(v *FindingRecommendation) string { return v.ID }

// registerAllTables registers every converted resource collection on
// b.registry exactly once. It must be called during construction only
// (immediately after b.registry is created), never on every Reset() --
// store.Register panics on a duplicate name, so runtime resets go through
// b.registry.ResetAll() instead (see InMemoryBackend.Reset in backend.go).
func registerAllTables(b *InMemoryBackend) {
	for _, register := range tableRegistrations {
		register(b)
	}
}

// tableRegistrations is the data-driven list registerAllTables walks: one
// closure per resource table, each binding its own store.New/store.Register
// call (and, for the composite-keyed collections, its companion
// store.Table.AddIndex call) to the concrete field and value type.
//
//nolint:gochecknoglobals // registration table, analogous to errCodeLookup-style lookup tables elsewhere
var tableRegistrations = []func(*InMemoryBackend){
	func(b *InMemoryBackend) {
		b.analyzers = store.Register(b.registry, "analyzers", store.New(analyzerKeyFn))
	},
	func(b *InMemoryBackend) {
		b.findings = store.Register(b.registry, "findings", store.New(findingKeyFn))
		b.findingsByAnalyzer = b.findings.AddIndex("byAnalyzer", findingAnalyzerIndexKeyFn)
	},
	func(b *InMemoryBackend) {
		b.analyzedResources = store.Register(b.registry, "analyzedResources", store.New(analyzedResourceKeyFn))
	},
	func(b *InMemoryBackend) {
		b.policyGenerations = store.Register(b.registry, "policyGenerations", store.New(policyGenerationKeyFn))
	},
	func(b *InMemoryBackend) {
		b.accessPreviews = store.Register(b.registry, "accessPreviews", store.New(accessPreviewKeyFn))
	},
	func(b *InMemoryBackend) {
		b.findingRecommendations = store.Register(
			b.registry, "findingRecommendations", store.New(findingRecommendationKeyFn),
		)
	},
	// "Dirty" table: store.New only, NOT store.Register -- see the file doc
	// comment above. Reset() (backend.go) resets it explicitly;
	// persistence.go builds a separate ephemeral DTO registry for it.
	func(b *InMemoryBackend) {
		b.archiveRules = store.New(archiveRuleKeyFn)
		b.archiveRulesByAnalyzer = b.archiveRules.AddIndex("byAnalyzer", archiveRuleAnalyzerIndexKeyFn)
	},
}
