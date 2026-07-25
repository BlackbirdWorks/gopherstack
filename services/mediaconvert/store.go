package mediaconvert

import (
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	presetCustom = "CUSTOM"
)

const (
	// statusActive is the active state for queues and presets.
	statusActive = "ACTIVE"
	// statusPaused is the paused state for queues.
	statusPaused = "PAUSED"
	// jobStatusSubmitted is the initial job state.
	jobStatusSubmitted = "SUBMITTED"
	// jobStatusProgressing is the in-progress job state.
	jobStatusProgressing = "PROGRESSING"
	// jobStatusComplete is the successfully finished job state.
	jobStatusComplete = "COMPLETE"
	// jobStatusError is the failed job state.
	jobStatusError = "ERROR"
	// jobStatusCanceled is the canceled job state.
	jobStatusCanceled = "CANCELED"
	// pricingPlanOnDemand is the default pricing plan.
	pricingPlanOnDemand = "ON_DEMAND"
	// jobPhaseProbing is the initial processing phase of a submitted job.
	jobPhaseProbing = "PROBING"
	// jobPhaseTranscoding is the second processing phase after probing.
	jobPhaseTranscoding = "TRANSCODING"
	// jobPhaseUploading is the third processing phase after transcoding.
	jobPhaseUploading = "UPLOADING"
	// priorityMin is the minimum allowed job/template priority.
	priorityMin = -50
	// priorityMax is the maximum allowed job/template priority.
	priorityMax = 50
	// orderAscending is the ascending list order value used by AWS MediaConvert.
	orderAscending = "ASCENDING"
	// deepCloneMaxDepth caps recursion in deepCloneValue to prevent stack overflows.
	// deepCloneMaxDepth bounds recursion in deepCloneValueAt to guard against
	// pathological/cyclic input. It is set well above the depth of any real
	// MediaConvert job-settings document so legitimate settings are never affected.
	deepCloneMaxDepth = 100
	// tokenTTL is how long a ClientRequestToken deduplication window lasts.
	tokenTTL = time.Minute
	// maxTokens caps the tokenIndex size to prevent unbounded growth.
	maxTokens = 10_000
	// jobEngineVersionUsed is the fixed engine version reported on all jobs.
	jobEngineVersionUsed = "2017-08-29"
	// defaultStatusUpdateInterval is the StatusUpdateInterval applied to jobs
	// and job templates when the caller doesn't specify one, matching the
	// real MediaConvert API's documented default.
	defaultStatusUpdateInterval = "SECONDS_60"
)

// epochSeconds converts a [time.Time] to a float64 Unix epoch seconds value,
// which is the format expected by the MediaConvert SDK for timestamp fields.
func epochSeconds(t time.Time) float64 {
	return float64(t.Unix())
}

// deepCloneMap returns a deep copy of a settings map, or nil if input is nil.
func deepCloneMap(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}

	cp := make(map[string]any, len(m))
	for k, v := range m {
		cp[k] = deepCloneValueAt(v, 0)
	}

	return cp
}

// deepCloneValueAt clones v with a depth counter. When depth >= deepCloneMaxDepth,
// the value is returned as-is (shared reference) rather than recursing further. This
// preserves the data even for unexpectedly deep documents instead of silently dropping
// it to nil; the bound only exists to guard against pathological/cyclic input.
func deepCloneValueAt(v any, depth int) any {
	if depth >= deepCloneMaxDepth {
		return v
	}

	switch vt := v.(type) {
	case map[string]any:
		cp := make(map[string]any, len(vt))
		for k, val := range vt {
			cp[k] = deepCloneValueAt(val, depth+1)
		}

		return cp
	case []any:
		cp := make([]any, len(vt))
		for i, item := range vt {
			cp[i] = deepCloneValueAt(item, depth+1)
		}

		return cp
	default:
		return v
	}
}

// nonNilTagsCopy returns a copy of the given tags map; never returns nil.
func nonNilTagsCopy(tags map[string]string) map[string]string {
	cp := make(map[string]string, len(tags))
	maps.Copy(cp, tags)

	return cp
}

// InMemoryBackend is the in-memory store for MediaConvert resources.
//
// registry holds every "clean" store.Table (queues, jobTemplates, jobs,
// presets) so their Reset/Snapshot/Restore collapse to one call each via
// resetTablesLocked / persistence.go. queueCounters and tokenIndex are
// "dirty" tables -- their value types carry no natural identity field of
// their own (see queueJobCounter.queueArn / tokenEntry.token) -- so they are
// NOT on this registry; persistence.go round-trips them through a throwaway
// DTO registry instead. queries is a store.Table too (for keyed Get/Put/
// Delete) but was never persisted pre-Phase-3.3 and stays that way: it is
// reset alongside the others but never appears in backendSnapshot. See
// store_setup.go for the full rationale.
type InMemoryBackend struct {
	registry      *store.Registry
	queries       *store.Table[jobsQuery]
	queues        *store.Table[Queue]
	queuesByArn   *store.Index[Queue] // ARN → queue; enables O(1) ARN lookup
	jobTemplates  *store.Table[JobTemplate]
	jobs          *store.Table[Job]
	presets       *store.Table[Preset]
	tags          map[string]map[string]string
	certificates  map[string]struct{}
	queueCounters *store.Table[queueJobCounter]
	tokenIndex    *store.Table[tokenEntry]
	policy        *Policy
	mu            *lockmetrics.RWMutex
	accountID     string
	region        string
}

// NewInMemoryBackend creates a new in-memory MediaConvert backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:     store.NewRegistry(),
		tags:         make(map[string]map[string]string),
		certificates: make(map[string]struct{}),
		accountID:    accountID,
		region:       region,
		mu:           lockmetrics.New("mediaconvert"),
	}
	registerAllTables(b)

	return b
}

// Region returns the region configured for this backend.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the account ID configured for this backend.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Reset clears all stored resources, resetting the backend to its initial state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.resetTablesLocked()
	b.tags = make(map[string]map[string]string)
	b.certificates = make(map[string]struct{})
	b.policy = nil
}

// resetTablesLocked resets every store.Table-backed resource field to empty:
// the "clean" tables via one b.registry.ResetAll() call, plus the "dirty"
// tables and the ephemeral queries table individually since they are not
// registered on b.registry (see store_setup.go). The caller MUST hold b.mu
// for writing.
func (b *InMemoryBackend) resetTablesLocked() {
	b.registry.ResetAll()
	b.queueCounters.Reset()
	b.tokenIndex.Reset()
	b.queries.Reset()
}
