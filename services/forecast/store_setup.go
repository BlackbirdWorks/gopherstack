package forecast

// Code in this file supports Phase 3.3 of the datalayer refactor: the
// map[resourceKind]map[string]*Resource resource field on InMemoryBackend is
// replaced by map[resourceKind]*store.Table[Resource] -- one store.Table per
// Forecast resource kind (dataset groups, datasets, dataset import jobs,
// predictors, forecasts, ...). Every Forecast resource kind already shares
// the single Resource type and is keyed by its own Name field, which AWS
// guarantees unique only within a kind (not globally) -- exactly matching
// the per-kind map the original code partitioned by resourceKind. ARN
// uniqueness is global (arn.Build embeds the kind in the resource part), but
// since each kind gets its own Table, keying each Table by Name alone is
// sufficient and mirrors the pre-refactor map[string]*Resource shape.
//
// arnIndex (ARN -> {kind, name}) is left as a raw map: it is a *cross-table*
// reverse index spanning all of the per-kind Tables below, which does not
// fit store.Index (an Index is always scoped to a single Table). It is
// rebuilt from the resources tables on Restore (see persistence.go) rather
// than persisted directly, since it is fully derivable from each Resource's
// own ARN/Kind fields.
//
// evaluations (map[string][]MonitorEvaluation) and tags
// (map[string]map[string]string) are also left as raw maps: their values
// are not *T, so neither fits store.Table's keyed-by-identity-value shape.
import "github.com/blackbirdworks/gopherstack/pkgs/store"

// allResourceKinds enumerates every Forecast resource kind that gets its own
// store.Table, registered once at construction time.
//
//nolint:gochecknoglobals // registration table, analogous to errCodeLookup-style lookup tables elsewhere
var allResourceKinds = []resourceKind{
	kindDatasetGroup,
	kindDataset,
	kindDatasetImportJob,
	kindPredictor,
	kindPredictorBacktestExport,
	kindForecast,
	kindForecastExport,
	kindExplainabilityExport,
	kindWhatIfAnalysis,
	kindWhatIfForecast,
	kindWhatIfForecastExport,
	kindMonitor,
	kindExplainability,
}

func resourceKeyFn(v *Resource) string { return v.Name }

// registerAllTables constructs and registers one store.Table[Resource] per
// Forecast resource kind. It must be called during construction only,
// immediately after b.registry is created -- store.Register panics on a
// duplicate name, so runtime resets go through b.registry.ResetAll() instead
// (see InMemoryBackend.Reset in backend.go).
func registerAllTables(b *InMemoryBackend) {
	b.resources = make(map[resourceKind]*store.Table[Resource], len(allResourceKinds))

	for _, kind := range allResourceKinds {
		b.resources[kind] = store.Register(b.registry, "resources:"+string(kind), store.New(resourceKeyFn))
	}
}
