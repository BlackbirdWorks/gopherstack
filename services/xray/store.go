package xray

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	statusActive   = "ACTIVE"
	statusUpdating = "UPDATING"

	// defaultSamplingRuleName is the name of the built-in default sampling rule.
	// AWS X-Ray always maintains this rule and it cannot be deleted.
	defaultSamplingRuleName = "Default"
)

// InMemoryBackend is the in-memory store for X-Ray resources.
type InMemoryBackend struct {
	lastRuleModification time.Time
	// registry is the lifecycle registry for every store.Table below. It
	// collapses Reset() to one registry.ResetAll() call instead of one
	// hand-written make() per map. Snapshot/Restore deliberately do NOT use
	// registry.SnapshotAll()/RestoreAll() wholesale -- parsedSegments and
	// serviceWindows are excluded from persistence (see persistence.go) so
	// their JSON round-trip is driven by hand, table by table.
	registry *store.Registry
	// groupsByARN is a secondary index on groups, keyed by GroupARN.
	groupsByARN *store.Index[Group]
	// retrievedTraces is keyed by retrieval token; its values are slices,
	// not *T, so it is left as a plain map (see store_setup.go).
	retrievedTraces map[string][]*Trace
	parsedSegments  *store.Table[Segment]
	// traceSegments is a secondary index on parsedSegments, keyed by TraceID
	// ("segments of a trace").
	traceSegments *store.Index[Segment]
	insights      *store.Table[Insight]
	// insightEvents is keyed by insight ID; its values are slices, not *T,
	// so it is left as a plain map (see store_setup.go).
	insightEvents    map[string][]*InsightEvent
	resourcePolicies *store.Table[ResourcePolicy]
	// retrievalTimes is map[string]time.Time -- not a *T map -- so it is
	// left as a plain map (see store_setup.go).
	retrievalTimes map[string]time.Time
	groups         *store.Table[Group]
	// resourceTags is map[string]map[string]string -- not a *T map -- so it
	// is left as a plain map (see store_setup.go).
	resourceTags     map[string]map[string]string
	traces           *store.Table[Trace]
	samplingStats    *store.Table[SamplingStatisticSummary]
	traceRetrievals  *store.Table[TraceRetrieval]
	serviceWindows   *store.Table[serviceInsightWindow]
	encryptionConfig *EncryptionConfig
	mu               *lockmetrics.RWMutex
	samplingRules    *store.Table[SamplingRule]
	traceSegmentDest string
	region           string
	accountID        string
	telemetry        []*TelemetryRecord
	indexingRules    []*IndexingRule
	telemetryIdx     int
}

// NewInMemoryBackend creates a new InMemoryBackend with the given accountID and region.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:        store.NewRegistry(),
		insightEvents:   make(map[string][]*InsightEvent),
		retrievedTraces: make(map[string][]*Trace),
		resourceTags:    make(map[string]map[string]string),
		indexingRules:   defaultIndexingRules(),
		telemetry:       make([]*TelemetryRecord, telemetryRingSize),
		mu:              lockmetrics.New("xray"),
		encryptionConfig: &EncryptionConfig{
			Type:   "NONE",
			Status: statusActive,
		},
		region:         region,
		accountID:      accountID,
		retrievalTimes: make(map[string]time.Time),
	}
	registerAllTables(b)
	b.samplingRules.Put(b.defaultSamplingRule())

	return b
}

func (b *InMemoryBackend) groupARN(name string) string {
	return "arn:aws:xray:" + b.region + ":" + b.accountID + ":group/default/" + name
}

func (b *InMemoryBackend) samplingRuleARN(name string) string {
	return "arn:aws:xray:" + b.region + ":" + b.accountID + ":sampling-rule/" + name
}

// Reset clears all backend state, resetting to an empty store.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	// Resets every store.Table-backed resource map in one call instead of
	// the per-map make() calls this used to be (Phase 3.3 pkgs/store
	// conversion). See registerAllTables in store_setup.go for the full
	// list of tables this covers.
	b.registry.ResetAll()
	b.samplingRules.Put(b.defaultSamplingRule())

	b.insightEvents = make(map[string][]*InsightEvent)
	b.retrievedTraces = make(map[string][]*Trace)
	b.retrievalTimes = make(map[string]time.Time)
	b.telemetry = make([]*TelemetryRecord, telemetryRingSize)
	b.telemetryIdx = 0
	b.indexingRules = defaultIndexingRules()
	b.encryptionConfig = &EncryptionConfig{Type: "NONE", Status: statusActive}
	b.lastRuleModification = time.Time{}
	b.traceSegmentDest = ""
}
