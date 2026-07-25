package cloudwatchlogs

import (
	"context"
	"encoding/base64"
	"strconv"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	statusEnabled    = "ENABLED"
	keyMessageField  = "@message"
	keyTimestamp     = "@timestamp"
	keyIngestionTime = "@ingestionTime"
	keyLogStream     = "@logStream"
)

// regionContextKey is the context key under which the per-request AWS region is stored.
type regionContextKey struct{}

// getRegion extracts the region from ctx, falling back to defaultRegion when unset.
func getRegion(ctx context.Context, defaultRegion string) string {
	if r, ok := ctx.Value(regionContextKey{}).(string); ok && r != "" {
		return r
	}

	return defaultRegion
}

const (
	// anomalyVisibilityTimeMinDays is the minimum allowed anomaly visibility time in days.
	anomalyVisibilityTimeMinDays = 7
	// anomalyVisibilityTimeMaxDays is the maximum allowed anomaly visibility time in days.
	anomalyVisibilityTimeMaxDays = 90
	// msPerDay is the number of milliseconds in a day.
	msPerDay = 24 * 60 * 60 * 1000
	// putLogEventsMaxEventAgeMs is the maximum age of a log event (14 days) in milliseconds.
	putLogEventsMaxEventAgeMs = 14 * msPerDay
	// putLogEventsFutureWindowMs is the maximum future offset (2 hours) for log events in milliseconds.
	putLogEventsFutureWindowMs = 2 * 60 * 60 * 1000
	// putLogEventsMaxMessageBytes is the maximum size of a single log event message (256 KB).
	putLogEventsMaxMessageBytes = 256 * 1024
	// minRealisticTimestampMs is the minimum timestamp treated as a real wall-clock time
	// (Sep 9 2001 00:00:00 UTC). Events below this are assumed to be synthetic test data
	// and bypass timestamp-window validation.
	minRealisticTimestampMs = 1_000_000_000_000
	// detectorStatusInitializing is the status of a newly created anomaly detector.
	detectorStatusInitializing = "INITIALIZING"
	// detectorStatusPaused is the status set by UpdateLogAnomalyDetector when
	// called with enabled=false (aws-sdk-go-v2
	// types.AnomalyDetectorStatusPaused).
	detectorStatusPaused = "PAUSED"
	// detectorStatusAnalyzing is the status UpdateLogAnomalyDetector resumes a
	// paused detector to when called with enabled=true (aws-sdk-go-v2
	// types.AnomalyDetectorStatusAnalyzing): a detector that has already
	// completed its initial training re-enters the steady-state "actively
	// analyzing" status on resume, rather than restarting from INITIALIZING.
	detectorStatusAnalyzing = "ANALYZING"
)

const (
	defaultDescribeLimit = 50
	defaultEventLimit    = 10000
	// maxEventsPerStream is the maximum number of events retained per log stream.
	// Oldest events are dropped when this cap is reached.
	maxEventsPerStream = 10_000
	// maxSubscriptionFilters is the AWS-imposed limit per log group.
	maxSubscriptionFilters = 2
	// defaultQueryTTL is how long a query is retained before eviction.
	defaultQueryTTL = time.Hour
	// defaultMaxQueries is the maximum number of queries retained at any time.
	defaultMaxQueries = 10_000
	// defaultDeliveryWorkers is the maximum number of concurrent subscription delivery goroutines.
	defaultDeliveryWorkers = 8
	// defaultDeliveryTimeout is the per-delivery timeout applied to each subscription filter call.
	defaultDeliveryTimeout = 10 * time.Second
	// defaultParsedQueryCacheSize caps the number of parsed Insights queries cached in memory.
	defaultParsedQueryCacheSize = 256
	// maxExportTasks is the upper bound on stored export tasks.
	maxExportTasks = 1000
	// maxImportTasks is the upper bound on stored import tasks.
	maxImportTasks = 1000
	// maxAnomalyDetectors is the upper bound on log anomaly detectors.
	maxAnomalyDetectors = 500
	// maxScheduledQueries is the upper bound on scheduled queries.
	maxScheduledQueries = 500
	// maxQueryDefinitions is the upper bound on query definitions.
	maxQueryDefinitions = 1000
	// maxCompiledPatternCache is the upper bound on cached compiled filter patterns.
	maxCompiledPatternCache = 1024
	// maxExportTaskAgeMs is the maximum age (ms) for state advancement in DescribeExportTasks.
	// Tasks older than this (e.g., test fixtures with synthetic creation times) are not advanced.
	maxExportTaskAgeMs = 5 * 60 * 1000
	// exportTaskAgeRunningMs is how old a PENDING task must be before being advanced to RUNNING.
	exportTaskAgeRunningMs = 2000
	// exportTaskAgeCompletedMs is how old a RUNNING task must be before being advanced to COMPLETED.
	exportTaskAgeCompletedMs = 5000
	// defaultMaxRetentionDays is the default global maximum log retention period.
	defaultMaxRetentionDays = 14
)

// InMemoryBackend implements StorageBackend using pkgs/store tables in place of
// the hand-rolled maps this backend used before Phase 3.3 (see store_setup.go).
type InMemoryBackend struct {
	deliverer     SubscriptionDeliverer
	metricEmitter MetricEmitter
	ctx           context.Context
	workerSem     chan struct{}

	// registry holds every persisted resource table; Snapshot/Restore drive it
	// via registry.SnapshotAll()/RestoreAll() (see persistence.go).
	registry *store.Registry
	// ephemeralRegistry holds resource tables that are never persisted (query
	// results/anomalies/scheduled-query run history -- matching this backend's
	// pre-Phase-3.3 behavior of leaving them out of backendSnapshot) but still
	// benefit from one-call Reset semantics.
	ephemeralRegistry *store.Registry

	accountPolicies *store.Table[AccountPolicy]

	// groups, streams, subscriptionFilters, and metricFilters are region-qualified
	// ("dirty") tables: their value types carry unexported identity fields (see
	// models.go) so they are registered on neither registry above -- Snapshot/
	// Restore drive them through DTOs and Reset clears them directly.
	groups         *store.Table[LogGroup]
	groupsByRegion *store.Index[LogGroup]

	streams        *store.Table[LogStream]
	streamsByGroup *store.Index[LogStream]

	subscriptionFilters        *store.Table[SubscriptionFilter]
	subscriptionFiltersByGroup *store.Index[SubscriptionFilter]

	metricFilters        *store.Table[MetricFilter]
	metricFiltersByGroup *store.Index[MetricFilter]

	// queries and anomalies are ephemeral (registered on ephemeralRegistry, not
	// registry -- see above).
	queries           *store.Table[storedQuery]
	anomalies         *store.Table[Anomaly]
	anomalyByDetector *store.Index[Anomaly]

	// parsedQueries and compiledPatterns remain plain maps -- see the doc
	// comment above registerAllTables in store_setup.go for why.
	parsedQueries    map[string]*insightsQuery
	compiledPatterns map[string]*compiledFilterPattern

	exportTasks            *store.Table[ExportTask]
	importTasks            *store.Table[ImportTask]
	deliveries             *store.Table[Delivery]
	logAnomalyDetectors    *store.Table[LogAnomalyDetector]
	scheduledQueries       *store.Table[ScheduledQuery]
	scheduledQueryRuns     *store.Table[scheduledQueryRunHistory]
	s3TableIntegrations    *store.Table[s3TableIntegrationEntry]
	mu                     *lockmetrics.RWMutex
	kmsKeys                *store.Table[kmsKeyEntry]
	queryDefinitions       *store.Table[QueryDefinition]
	dataProtectionPolicies *store.Table[dataProtectionPolicyEntry]
	resourcePolicies       *store.Table[ResourcePolicy]
	deliveryDestinations   *store.Table[DeliveryDestination]
	deliverySources        *store.Table[DeliverySource]
	destinations           *store.Table[CWLDestination]
	indexPolicies          *store.Table[IndexPolicy]
	transformers           *store.Table[Transformer]
	integrations           *store.Table[CWLIntegration]
	deletionProtected      *store.Table[deletionProtectionEntry]
	exportSink             ExportSink
	cancel                 context.CancelFunc
	region                 string
	accountID              string
	queriesOrder           []string
	parsedQueriesOrder     []string
	wg                     sync.WaitGroup
	queryTTL               time.Duration
	maxQueries             int
	maxParsedQueries       int
	deliveryTimeout        time.Duration
	compiledPatternsMu     sync.RWMutex
	settings               Settings
}

// NewInMemoryBackend creates a new InMemoryBackend with default configuration.
func NewInMemoryBackend() *InMemoryBackend {
	return NewInMemoryBackendWithContext(
		context.Background(),
		config.DefaultAccountID,
		config.DefaultRegion,
	)
}

// NewInMemoryBackendWithConfig creates a new InMemoryBackend with given account and region.
func NewInMemoryBackendWithConfig(accountID, region string) *InMemoryBackend {
	return NewInMemoryBackendWithContext(context.Background(), accountID, region)
}

// NewInMemoryBackendWithContext creates a new InMemoryBackend with the given parent context,
// account ID, and region. Subscription delivery goroutines are bounded by svcCtx so that
// they are cancelled on server shutdown.
// If svcCtx is nil, [context.Background] is used.
func NewInMemoryBackendWithContext(
	svcCtx context.Context,
	accountID, region string,
) *InMemoryBackend {
	if svcCtx == nil {
		svcCtx = context.Background()
	}

	ctx, cancel := context.WithCancel(svcCtx)

	b := &InMemoryBackend{
		accountID:         accountID,
		region:            region,
		registry:          store.NewRegistry(),
		ephemeralRegistry: store.NewRegistry(),
		parsedQueries:     make(map[string]*insightsQuery),
		compiledPatterns:  make(map[string]*compiledFilterPattern),
		mu:                lockmetrics.New("cloudwatchlogs"),
		queryTTL:          defaultQueryTTL,
		maxQueries:        defaultMaxQueries,
		maxParsedQueries:  defaultParsedQueryCacheSize,
		ctx:               ctx,
		cancel:            cancel,
		workerSem:         make(chan struct{}, defaultDeliveryWorkers),
		deliveryTimeout:   defaultDeliveryTimeout,
		settings: Settings{
			MaxRetentionDays: defaultMaxRetentionDays,
			JanitorInterval:  time.Minute,
		},
	}

	registerAllTables(b)
	registerEphemeralTables(b)
	registerRegionTables(b)

	return b
}

// SetSettings updates the backend settings.
func (b *InMemoryBackend) SetSettings(s Settings) {
	b.mu.Lock("SetSettings")
	defer b.mu.Unlock()
	b.settings = s
}

// SetSubscriptionDeliverer sets the deliverer used to forward log events to subscription filter destinations.
func (b *InMemoryBackend) SetSubscriptionDeliverer(d SubscriptionDeliverer) {
	b.mu.Lock("SetSubscriptionDeliverer")
	defer b.mu.Unlock()
	b.deliverer = d
}

// SetMetricEmitter sets the emitter used to forward metric filter matches to CloudWatch.
func (b *InMemoryBackend) SetMetricEmitter(e MetricEmitter) {
	b.mu.Lock("SetMetricEmitter")
	defer b.mu.Unlock()
	b.metricEmitter = e
}

// SetQueryTTL overrides the TTL used to evict queries by age.
// A value of zero disables TTL-based eviction. Primarily intended for tests.
func (b *InMemoryBackend) SetQueryTTL(d time.Duration) {
	b.mu.Lock("SetQueryTTL")
	defer b.mu.Unlock()
	b.queryTTL = d
}

// SetMaxQueries overrides the maximum number of queries retained in memory.
// A value of zero disables the cap. Primarily intended for tests.
func (b *InMemoryBackend) SetMaxQueries(n int) {
	b.mu.Lock("SetMaxQueries")
	defer b.mu.Unlock()
	b.maxQueries = n
}

// SetDeliveryTimeout overrides the per-delivery timeout applied to each subscription filter call.
// A zero value disables the timeout. Primarily intended for tests.
func (b *InMemoryBackend) SetDeliveryTimeout(d time.Duration) {
	b.mu.Lock("SetDeliveryTimeout")
	defer b.mu.Unlock()
	b.deliveryTimeout = d
}

// SetDeliveryWorkers overrides the maximum number of concurrent subscription delivery goroutines.
// Must be called before the first PutLogEvents. Primarily intended for tests.
func (b *InMemoryBackend) SetDeliveryWorkers(n int) {
	b.mu.Lock("SetDeliveryWorkers")
	defer b.mu.Unlock()
	b.workerSem = make(chan struct{}, n)
}

// Close cancels the lifecycle context, stops acceptance of new deliveries, and waits for all
// in-flight delivery goroutines to finish. After Close, PutLogEvents will no longer spawn
// delivery goroutines.
func (b *InMemoryBackend) Close() {
	b.cancel()
	b.wg.Wait()
}

// Drain waits for all in-flight subscription delivery goroutines to complete without cancelling
// the lifecycle context. Primarily intended for tests.
func (b *InMemoryBackend) Drain() {
	b.wg.Wait()
}

// encodeNextToken returns an opaque base64-encoded pagination cursor for the given slice offset.
func encodeNextToken(idx int) string {
	return base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(idx)))
}

// parseNextToken decodes a pagination cursor back to a slice offset.
// Accepts base64-encoded cursors (new format) with graceful fallback to plain decimal
// strings for backward compatibility.
func parseNextToken(token string) int {
	if token == "" {
		return 0
	}
	// Attempt base64 decode first (new format).
	if decoded, err := base64.StdEncoding.DecodeString(token); err == nil {
		token = string(decoded)
	}
	idx, err := strconv.Atoi(token)
	if err != nil || idx < 0 {
		return 0
	}

	return idx
}

// Reset clears all in-memory state from the backend. It is used by the
// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.ephemeralRegistry.ResetAll()
	b.groups.Reset()
	b.streams.Reset()
	b.subscriptionFilters.Reset()
	b.metricFilters.Reset()

	b.queriesOrder = nil
	b.parsedQueriesOrder = nil
	b.parsedQueries = make(map[string]*insightsQuery)

	b.compiledPatternsMu.Lock()
	b.compiledPatterns = make(map[string]*compiledFilterPattern)
	b.compiledPatternsMu.Unlock()
}
