package ce

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend is replaced with a
// *store.Table[T], following the pattern established by services/ec2 (commit
// 12e611a4, data-driven registration slice) and services/sesv2 (the sibling
// "every value already carries a real identity field" case). See pkgs/store's
// package doc for the underlying primitive.
//
// Every convertible value type here already carries its own identity as a
// real (non-json:"-") field -- CostCategory.ARN, AnomalyMonitor.MonitorARN,
// AnomalySubscription.SubscriptionARN, Anomaly.AnomalyID,
// CostAllocationTag.TagKey, and CommitmentAnalysis.AnalysisID were already
// set consistently at every write site before this refactor. So all 6 tables
// below are "clean" tables registered directly on b.registry, with no
// ephemeral DTO-registry needed, and persistence.go drives every one of them
// through b.registry.SnapshotAll() / RestoreAll().
//
// Left as plain fields (not store.Table-backed):
//   - costLedger ([]CostEntry): a synthetic, regenerated-on-Reset ledger with
//     no per-entry identity; it is not persisted today (absent from the
//     pre-refactor backendSnapshot) and remains a plain slice.
//   - backfillJobs ([]*BackfillJob): append-only history; it was persisted as
//     a raw slice before this refactor and remains one. BackfillJob now
//     carries an internal-only BackfillID (added for deterministic pagination
//     cursoring -- see ListBackfillHistory) but a plain slice, not a
//     store.Table, is still the simplest fit since nothing ever looks a job
//     up by that ID.
import "github.com/blackbirdworks/gopherstack/pkgs/store"

func costCategoryKeyFn(v *CostCategory) string { return v.ARN }

func anomalyMonitorKeyFn(v *AnomalyMonitor) string { return v.MonitorARN }

func anomalySubscriptionKeyFn(v *AnomalySubscription) string { return v.SubscriptionARN }

func anomalyKeyFn(v *Anomaly) string { return v.AnomalyID }

func costAllocationTagKeyFn(v *CostAllocationTag) string { return v.TagKey }

func commitmentAnalysisKeyFn(v *CommitmentAnalysis) string { return v.AnalysisID }

func savingsPlansGenerationKeyFn(v *SavingsPlansGeneration) string { return v.RecommendationID }

// registerAllTables constructs and registers every store.Table-backed
// resource field exactly once, at construction time. It must be called
// during construction only, never on every Reset(): store.Register panics on
// a duplicate name, so runtime resets go through b.registry.ResetAll()
// (backend.go) instead.
func registerAllTables(b *InMemoryBackend) {
	b.costCategories = store.Register(b.registry, "costCategories", store.New(costCategoryKeyFn))
	b.anomalyMonitors = store.Register(b.registry, "anomalyMonitors", store.New(anomalyMonitorKeyFn))
	b.anomalySubscriptions = store.Register(
		b.registry, "anomalySubscriptions", store.New(anomalySubscriptionKeyFn),
	)
	b.anomalies = store.Register(b.registry, "anomalies", store.New(anomalyKeyFn))
	b.costAllocationTags = store.Register(b.registry, "costAllocationTags", store.New(costAllocationTagKeyFn))
	b.commitmentAnalyses = store.Register(b.registry, "commitmentAnalyses", store.New(commitmentAnalysisKeyFn))
	b.savingsPlansGenerations = store.Register(
		b.registry, "savingsPlansGenerations", store.New(savingsPlansGenerationKeyFn),
	)
}
