package personalize

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend is registered exactly
// once, here, as a *store.Table[T] on b.registry.
//
// Every value type already carries its own real (non-json:"-") identity
// field: DatasetGroup/Dataset/Schema/Solution/Campaign/EventTracker/Filter/
// Recommender/MetricAttribution key by Name (the same field the original
// map[string]*T used as its key); SolutionVersion/DatasetImportJob/
// DatasetExportJob/BatchInferenceJob/BatchSegmentJob/DataDeletionJob key by
// their own ARN field (also matching the original map key exactly);
// storedFeatureTransformation keys by ARN. None of these resources are
// parent-nested (no map[string]map[string]*T anywhere in this backend), so
// none need a composite "parent|child" key or a secondary store.Index --
// every List op already answered "children of parent X" via a linear
// predicate scan over the whole collection (see e.g. ListDatasets), which a
// filter over store.Table.Snapshot()/All() reproduces exactly. All 16 tables
// are therefore "clean" (store.Register-ed directly on b.registry, per the
// services/sesv2 convention), unlike services/ses or services/codecommit's
// "dirty" DTO-registry tables.
//
// tags (map[string]map[string]string) is left as a plain map: its value type
// is map[string]string, not *T, so it does not fit store.Table's
// keyed-by-identity-value shape. See persistence.go for how it is persisted
// directly instead.
import "github.com/blackbirdworks/gopherstack/pkgs/store"

func datasetGroupKeyFn(v *DatasetGroup) string { return v.Name }
func datasetKeyFn(v *Dataset) string           { return v.Name }
func schemaKeyFn(v *Schema) string             { return v.Name }
func solutionKeyFn(v *Solution) string         { return v.Name }

func solutionVersionKeyFn(v *SolutionVersion) string { return v.SolutionVersionArn }

func campaignKeyFn(v *Campaign) string                   { return v.Name }
func eventTrackerKeyFn(v *EventTracker) string           { return v.Name }
func filterKeyFn(v *Filter) string                       { return v.Name }
func recommenderKeyFn(v *Recommender) string             { return v.Name }
func metricAttributionKeyFn(v *MetricAttribution) string { return v.Name }

func datasetImportJobKeyFn(v *DatasetImportJob) string   { return v.DatasetImportJobArn }
func datasetExportJobKeyFn(v *DatasetExportJob) string   { return v.DatasetExportJobArn }
func batchInferenceJobKeyFn(v *BatchInferenceJob) string { return v.BatchInferenceJobArn }
func batchSegmentJobKeyFn(v *BatchSegmentJob) string     { return v.BatchSegmentJobArn }
func dataDeletionJobKeyFn(v *DataDeletionJob) string     { return v.DataDeletionJobArn }

func featureTransformationKeyFn(v *storedFeatureTransformation) string { return v.ARN }

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
// call to the concrete field and value type.
//
//nolint:gochecknoglobals // registration table, analogous to errCodeLookup-style lookup tables elsewhere
var tableRegistrations = []func(*InMemoryBackend){
	func(b *InMemoryBackend) {
		b.datasetGroups = store.Register(b.registry, "datasetGroups", store.New(datasetGroupKeyFn))
	},
	func(b *InMemoryBackend) {
		b.datasets = store.Register(b.registry, "datasets", store.New(datasetKeyFn))
	},
	func(b *InMemoryBackend) {
		b.schemas = store.Register(b.registry, "schemas", store.New(schemaKeyFn))
	},
	func(b *InMemoryBackend) {
		b.solutions = store.Register(b.registry, "solutions", store.New(solutionKeyFn))
	},
	func(b *InMemoryBackend) {
		b.solutionVersions = store.Register(b.registry, "solutionVersions", store.New(solutionVersionKeyFn))
	},
	func(b *InMemoryBackend) {
		b.campaigns = store.Register(b.registry, "campaigns", store.New(campaignKeyFn))
	},
	func(b *InMemoryBackend) {
		b.datasetImportJobs = store.Register(b.registry, "datasetImportJobs", store.New(datasetImportJobKeyFn))
	},
	func(b *InMemoryBackend) {
		b.datasetExportJobs = store.Register(b.registry, "datasetExportJobs", store.New(datasetExportJobKeyFn))
	},
	func(b *InMemoryBackend) {
		b.batchInferenceJobs = store.Register(b.registry, "batchInferenceJobs", store.New(batchInferenceJobKeyFn))
	},
	func(b *InMemoryBackend) {
		b.batchSegmentJobs = store.Register(b.registry, "batchSegmentJobs", store.New(batchSegmentJobKeyFn))
	},
	func(b *InMemoryBackend) {
		b.eventTrackers = store.Register(b.registry, "eventTrackers", store.New(eventTrackerKeyFn))
	},
	func(b *InMemoryBackend) {
		b.filters = store.Register(b.registry, "filters", store.New(filterKeyFn))
	},
	func(b *InMemoryBackend) {
		b.recommenders = store.Register(b.registry, "recommenders", store.New(recommenderKeyFn))
	},
	func(b *InMemoryBackend) {
		b.metricAttributions = store.Register(b.registry, "metricAttributions", store.New(metricAttributionKeyFn))
	},
	func(b *InMemoryBackend) {
		b.dataDeletionJobs = store.Register(b.registry, "dataDeletionJobs", store.New(dataDeletionJobKeyFn))
	},
	func(b *InMemoryBackend) {
		b.featureTransformations = store.Register(
			b.registry, "featureTransformations", store.New(featureTransformationKeyFn),
		)
	},
}
