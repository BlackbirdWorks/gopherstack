package cloudwatchlogs

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/collections"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	statusEnabled    = "ENABLED"
	keyMessageField  = "@message"
	keyTimestamp     = "@timestamp"
	keyIngestionTime = "@ingestionTime"
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

var (
	ErrLogGroupNotFound              = errors.New("ResourceNotFoundException")
	ErrLogGroupAlreadyExists         = errors.New("ResourceAlreadyExistsException")
	ErrLogStreamNotFound             = errors.New("ResourceNotFoundException")
	ErrLogStreamAlreadyExist         = errors.New("ResourceAlreadyExistsException")
	ErrSubscriptionFilterNotFound    = errors.New("ResourceNotFoundException")
	ErrSubscriptionFilterLimitExceed = errors.New("LimitExceededException")
	ErrQueryNotFound                 = errors.New("ResourceNotFoundException")
	ErrExportTaskNotFound            = errors.New("ResourceNotFoundException")
	ErrImportTaskNotFound            = errors.New("ResourceNotFoundException")
	ErrValidation                    = errors.New("InvalidParameterException")
	ErrDeliveryNotFound              = errors.New("ResourceNotFoundException")
	ErrLogAnomalyDetectorNotFound    = errors.New("ResourceNotFoundException")
	ErrScheduledQueryNotFound        = errors.New("ResourceNotFoundException")
	ErrMetricFilterNotFound          = errors.New("ResourceNotFoundException")
	ErrQueryDefinitionNotFound       = errors.New("ResourceNotFoundException")
	ErrInvalidSequenceToken          = errors.New("InvalidSequenceTokenException")
	ErrOperationAborted              = errors.New("OperationAbortedException")
	ErrInvalidOperation              = errors.New("InvalidOperationException")
)

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
)

// validEvaluationFrequencies returns the allowed values for the anomaly detector
// evaluation frequency field, matching the AWS CloudWatch Logs API enum.
func validEvaluationFrequencies() map[string]struct{} {
	return map[string]struct{}{
		"ONE_MIN":     {},
		"FIVE_MIN":    {},
		"TEN_MIN":     {},
		"FIFTEEN_MIN": {},
		"THIRTY_MIN":  {},
		"ONE_HOUR":    {},
	}
}

// validScheduledQueryStates returns the allowed values for the scheduled query state field.
func validScheduledQueryStates() map[string]struct{} {
	return map[string]struct{}{
		statusEnabled: {},
		"DISABLED":    {},
	}
}

// validAccountPolicyTypes returns the allowed values for the account policy type field.
func validAccountPolicyTypes() map[string]struct{} {
	return map[string]struct{}{
		"DATA_PROTECTION_POLICY":     {},
		"SUBSCRIPTION_FILTER_POLICY": {},
		"FIELD_INDEX_POLICY":         {},
		"TRANSFORMER_POLICY":         {},
	}
}

// validAccountPolicyScopes returns the allowed values for the account policy scope field.
func validAccountPolicyScopes() map[string]struct{} {
	return map[string]struct{}{
		"ALL":                {},
		"SELECTION_CRITERIA": {},
	}
}

// validDistributions returns the allowed values for subscription filter distribution.
func validDistributions() map[string]struct{} {
	return map[string]struct{}{
		DistributionRandom:      {},
		DistributionByLogStream: {},
	}
}

// validLogGroupClasses returns the allowed values for the log group class field.
func validLogGroupClasses() map[string]struct{} {
	return map[string]struct{}{
		LogGroupClassStandard:         {},
		LogGroupClassInfrequentAccess: {},
	}
}

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

const (
	exportStatusPending   = "PENDING"
	exportStatusRunning   = "RUNNING"
	exportStatusCompleted = "COMPLETED"
	exportStatusCancelled = "CANCELLED"
)

// SubscriptionDeliverer delivers encoded log event payloads to a subscription filter destination.
type SubscriptionDeliverer interface {
	// DeliverLogEvents delivers a gzipped, base64-encoded CloudWatch Logs payload to destinationArn.
	DeliverLogEvents(ctx context.Context, destinationArn string, payload []byte) error
}

// SubscriptionDelivererFunc is a function adapter for SubscriptionDeliverer.
type SubscriptionDelivererFunc func(ctx context.Context, destinationArn string, payload []byte) error

// DeliverLogEvents implements SubscriptionDeliverer.
func (f SubscriptionDelivererFunc) DeliverLogEvents(
	ctx context.Context,
	destinationArn string,
	payload []byte,
) error {
	return f(ctx, destinationArn, payload)
}

// MetricEmitter emits a CloudWatch metric data point.
// It is implemented by the CloudWatch backend and injected into InMemoryBackend
// so that metric filter matches on PutLogEvents can be forwarded to CloudWatch.
type MetricEmitter interface {
	// EmitMetric records a single metric data point with the given namespace, name, value, and unit.
	EmitMetric(namespace, name string, value float64, unit string) error
}

// MetricEmitterFunc is a function adapter for MetricEmitter.
type MetricEmitterFunc func(namespace, name string, value float64, unit string) error

// EmitMetric implements MetricEmitter.
func (f MetricEmitterFunc) EmitMetric(namespace, name string, value float64, unit string) error {
	return f(namespace, name, value, unit)
}

// StorageBackend is the interface for a CloudWatch Logs in-memory store.
type StorageBackend interface {
	CreateLogGroup(ctx context.Context, name, logGroupClass, kmsKeyID string) (*LogGroup, error)
	DeleteLogGroup(ctx context.Context, name string) error
	DescribeLogGroups(
		ctx context.Context,
		prefix, nextToken string,
		limit int,
	) ([]LogGroup, string, error)
	CreateLogStream(ctx context.Context, groupName, streamName string) (*LogStream, error)
	DeleteLogStream(ctx context.Context, groupName, streamName string) error
	DescribeLogStreams(
		ctx context.Context,
		groupName, prefix, nextToken, orderBy string,
		descending bool,
		limit int,
	) ([]LogStream, string, error)
	PutLogEvents(
		ctx context.Context, groupName, streamName, sequenceToken string, events []InputLogEvent,
	) (*PutLogEventsResult, error)
	GetLogEvents(
		ctx context.Context,
		groupName, streamName string,
		startTime, endTime *int64,
		limit int,
		nextToken string,
		startFromHead bool,
	) (
		[]OutputLogEvent, string, string, error)
	FilterLogEvents(ctx context.Context, p FilterLogEventsParams) (
		[]FilteredLogEvent, string, []SearchedLogStream, error)
	PutSubscriptionFilter(
		ctx context.Context, groupName, filterName, filterPattern, destinationArn, roleArn, distribution string,
	) error
	DescribeSubscriptionFilters(
		ctx context.Context,
		groupName, filterNamePrefix, nextToken string,
		limit int,
	) (
		[]SubscriptionFilter, string, error)
	DeleteSubscriptionFilter(ctx context.Context, groupName, filterName string) error
	SetRetentionPolicy(ctx context.Context, groupName string, days *int32) error
	StartQuery(
		ctx context.Context, queryID, queryString string, logGroupNames []string, startTime, endTime int64,
	) (*QueryInfo, error)
	GetQueryResults(queryID string) ([][]ResultField, QueryStatistics, QueryStatus, error)
	StopQuery(queryID string) error
	DescribeQueries(
		logGroupName, statusFilter, nextToken string,
		maxResults int,
	) ([]QueryInfo, string, error)

	// AssociateKmsKey associates a KMS key with a log group or query results resource.
	AssociateKmsKey(logGroupName, resourceIdentifier, kmsKeyID string) error
	// AssociateSourceToS3TableIntegration associates a data source with an S3 table integration.
	AssociateSourceToS3TableIntegration(
		integrationArn, dataSourceName, dataSourceType string,
	) (string, error)
	// CancelExportTask cancels a pending or running export task.
	CancelExportTask(taskID string) error
	// CancelImportTask cancels a running import task.
	CancelImportTask(importID string) (*ImportTask, error)
	// CreateDelivery creates a delivery between a delivery source and destination.
	CreateDelivery(
		deliverySourceName, deliveryDestinationArn string,
		tags map[string]string,
	) (*Delivery, error)
	// CreateExportTask creates an asynchronous export task to S3.
	CreateExportTask(
		taskName, logGroupName, logStreamNamePrefix, destination, destinationPrefix string,
		from, to int64,
	) (string, error)
	// CreateImportTask creates an import task from a CloudTrail Lake event data store.
	CreateImportTask(importRoleArn, importSourceArn string) (*ImportTask, error)
	// CreateLogAnomalyDetector creates an anomaly detector for one or more log groups.
	CreateLogAnomalyDetector(
		logGroupArnList []string,
		detectorName, evaluationFrequency, filterPattern, kmsKeyID string,
		anomalyVisibilityTime int64,
	) (string, error)
	// CreateScheduledQuery creates a scheduled CloudWatch Logs Insights query.
	CreateScheduledQuery(
		name, queryString, scheduleExpression, executionRoleArn, state string,
	) (string, error)
	// DeleteAccountPolicy deletes a CloudWatch Logs account-level policy.
	DeleteAccountPolicy(policyName, policyType string) error
	// DescribeExportTasks lists export tasks optionally filtered by task ID or status.
	DescribeExportTasks(
		taskID, statusCode string,
		limit int,
		nextToken string,
	) ([]ExportTask, string, error)
	// DescribeImportTasks lists import tasks optionally filtered by task ID.
	DescribeImportTasks(taskID string, limit int, nextToken string) ([]ImportTask, string, error)
	// DescribeDeliveries lists deliveries with pagination.
	DescribeDeliveries(limit int, nextToken string) ([]Delivery, string, error)
	// GetDelivery returns a single delivery by ID.
	GetDelivery(id string) (*Delivery, error)
	// DeleteDelivery deletes a delivery by ID.
	DeleteDelivery(id string) error
	// DeleteLogAnomalyDetector deletes a log anomaly detector.
	DeleteLogAnomalyDetector(detectorArn string) error
	// ListLogAnomalyDetectors lists anomaly detectors, optionally filtered by log group ARN.
	ListLogAnomalyDetectors(
		filterLogGroupArnList []string,
		limit int,
		nextToken string,
	) ([]LogAnomalyDetector, string, error)
	// UpdateLogAnomalyDetector updates evaluation frequency and/or anomaly visibility time.
	UpdateLogAnomalyDetector(
		detectorArn, evaluationFrequency string,
		anomalyVisibilityTime int64,
	) error
	// DeleteScheduledQuery deletes a scheduled query by ARN.
	DeleteScheduledQuery(scheduledQueryArn string) error
	// ListScheduledQueries lists all scheduled queries with pagination.
	ListScheduledQueries(limit int, nextToken string) ([]ScheduledQuery, string, error)
	// UpdateScheduledQuery updates the state of a scheduled query.
	UpdateScheduledQuery(scheduledQueryArn, state string) error
	// PutAccountPolicy creates or updates an account-level policy.
	PutAccountPolicy(
		policyName, policyType, policyDocument, scope, selectionCriteria string,
	) (*AccountPolicy, error)
	// DescribeAccountPolicies returns account-level policies, optionally filtered.
	DescribeAccountPolicies(
		policyType, policyName string,
		accountIdentifiers []string,
		limit int,
		nextToken string,
	) ([]AccountPolicy, string, error)
	// DisassociateKmsKey removes the KMS key association from a log group or resource.
	DisassociateKmsKey(logGroupName, resourceIdentifier string) error
	// PutMetricFilter creates or updates a metric filter for a log group.
	PutMetricFilter(
		ctx context.Context, logGroupName, filterName, filterPattern string, transformations []MetricTransformation,
	) error
	// DescribeMetricFilters lists metric filters with optional filters.
	DescribeMetricFilters(
		ctx context.Context,
		logGroupName, filterNamePrefix, metricName, metricNamespace, nextToken string,
		limit int,
	) ([]MetricFilter, string, error)
	// DeleteMetricFilter deletes a metric filter from a log group.
	DeleteMetricFilter(ctx context.Context, logGroupName, filterName string) error
	// TestMetricFilter tests a metric filter pattern against provided log event messages.
	TestMetricFilter(
		filterPattern string,
		logEventMessages []string,
	) ([]MetricFilterMatchRecord, error)
	// PutQueryDefinition creates or updates a query definition.
	PutQueryDefinition(
		name, queryString, queryDefinitionID string,
		logGroupNames []string,
	) (string, error)
	// DescribeQueryDefinitions lists query definitions optionally filtered by name prefix.
	DescribeQueryDefinitions(
		queryDefinitionNamePrefix string,
		limit int,
		nextToken string,
	) ([]QueryDefinition, string, error)
	// DeleteQueryDefinition deletes a query definition by ID.
	DeleteQueryDefinition(queryDefinitionID string) error
	// GetLogAnomalyDetector returns the anomaly detector with the given ARN.
	GetLogAnomalyDetector(detectorArn string) (*LogAnomalyDetector, error)
	// GetScheduledQuery returns the scheduled query with the given ARN.
	GetScheduledQuery(scheduledQueryArn string) (*ScheduledQuery, error)
	// GetLogGroupFields returns the most common log fields for a log group.
	GetLogGroupFields(ctx context.Context, logGroupName string) ([]LogGroupField, error)
	// GetLogRecord returns a single log event by its log record pointer.
	GetLogRecord(ctx context.Context, logRecordPointer string) (map[string]string, error)
	// ListAnomalies lists anomalies for the given anomaly detector ARN with pagination.
	ListAnomalies(anomalyDetectorArn string, limit int, nextToken string) ([]Anomaly, string, error)
	// ListLogGroupsForQuery returns the log group names used in a specific query.
	ListLogGroupsForQuery(queryID string) ([]string, error)
	// GetScheduledQueryHistory returns the execution history for a scheduled query.
	GetScheduledQueryHistory(
		scheduledQueryArn string,
		nextToken string,
		maxResults int,
	) ([]ScheduledQueryRunSummary, string, error)
	// UpdateAnomaly updates anomaly suppression settings. No actual anomaly data is stored.
	UpdateAnomaly(anomalyID, anomalyDetectorArn string, suppressionType string) error
	// ListLogGroups is the newer paginated list operation, equivalent to DescribeLogGroups.
	ListLogGroups(
		ctx context.Context,
		namePrefix, nextToken string,
		limit int,
	) ([]LogGroup, string, error)
}

// storedQuery holds the execution state of a single Logs Insights query.
type storedQuery struct {
	createdAt time.Time
	info      QueryInfo
	results   [][]ResultField
	logGroups []string
	stats     QueryStatistics
}

// InMemoryBackend implements StorageBackend using in-memory maps.
type InMemoryBackend struct {
	deliverer              SubscriptionDeliverer
	metricEmitter          MetricEmitter
	ctx                    context.Context
	accountPolicies        map[string]*AccountPolicy
	groups                 map[string]map[string]*LogGroup
	workerSem              chan struct{}
	streams                map[string]map[string]map[string]*LogStream
	events                 map[string]map[string]map[string][]*OutputLogEvent
	subscriptionFilters    map[string]map[string][]*SubscriptionFilter
	queries                map[string]*storedQuery
	parsedQueries          map[string]*insightsQuery
	compiledPatterns       map[string]*compiledFilterPattern
	exportTasks            map[string]*ExportTask
	importTasks            map[string]*ImportTask
	deliveries             map[string]*Delivery
	logAnomalyDetectors    map[string]*LogAnomalyDetector
	anomalies              map[string]map[string]*Anomaly
	scheduledQueries       map[string]*ScheduledQuery
	scheduledQueryRuns     map[string][]*ScheduledQueryRunSummary
	s3TableIntegrations    map[string]string
	mu                     *lockmetrics.RWMutex
	kmsKeys                map[string]string
	metricFilters          map[string]map[string]map[string]*MetricFilter
	queryDefinitions       map[string]*QueryDefinition
	dataProtectionPolicies map[string]string // logGroupName -> policyDocument JSON
	resourcePolicies       map[string]ResourcePolicy
	deliveryDestinations   map[string]DeliveryDestination
	deliverySources        map[string]DeliverySource
	destinations           map[string]CWLDestination
	indexPolicies          map[string]IndexPolicy
	transformers           map[string]Transformer
	integrations           map[string]CWLIntegration
	deletionProtected      map[string]bool
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

	return &InMemoryBackend{
		accountID:              accountID,
		region:                 region,
		groups:                 make(map[string]map[string]*LogGroup),
		streams:                make(map[string]map[string]map[string]*LogStream),
		events:                 make(map[string]map[string]map[string][]*OutputLogEvent),
		subscriptionFilters:    make(map[string]map[string][]*SubscriptionFilter),
		queries:                make(map[string]*storedQuery),
		parsedQueries:          make(map[string]*insightsQuery),
		compiledPatterns:       make(map[string]*compiledFilterPattern),
		exportTasks:            make(map[string]*ExportTask),
		importTasks:            make(map[string]*ImportTask),
		deliveries:             make(map[string]*Delivery),
		logAnomalyDetectors:    make(map[string]*LogAnomalyDetector),
		anomalies:              make(map[string]map[string]*Anomaly),
		scheduledQueries:       make(map[string]*ScheduledQuery),
		scheduledQueryRuns:     make(map[string][]*ScheduledQueryRunSummary),
		accountPolicies:        make(map[string]*AccountPolicy),
		kmsKeys:                make(map[string]string),
		s3TableIntegrations:    make(map[string]string),
		metricFilters:          make(map[string]map[string]map[string]*MetricFilter),
		queryDefinitions:       make(map[string]*QueryDefinition),
		dataProtectionPolicies: make(map[string]string),
		resourcePolicies:       make(map[string]ResourcePolicy),
		deliveryDestinations:   make(map[string]DeliveryDestination),
		deliverySources:        make(map[string]DeliverySource),
		destinations:           make(map[string]CWLDestination),
		indexPolicies:          make(map[string]IndexPolicy),
		transformers:           make(map[string]Transformer),
		integrations:           make(map[string]CWLIntegration),
		deletionProtected:      make(map[string]bool),
		mu:                     lockmetrics.New("cloudwatchlogs"),
		queryTTL:               defaultQueryTTL,
		maxQueries:             defaultMaxQueries,
		maxParsedQueries:       defaultParsedQueryCacheSize,
		ctx:                    ctx,
		cancel:                 cancel,
		workerSem:              make(chan struct{}, defaultDeliveryWorkers),
		deliveryTimeout:        defaultDeliveryTimeout,
		settings: Settings{
			MaxRetentionDays: defaultMaxRetentionDays,
			JanitorInterval:  time.Minute,
		},
	}
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

func (b *InMemoryBackend) groupARN(region, name string) string {
	return arn.Build("logs", region, b.accountID, "log-group:"+name)
}

func (b *InMemoryBackend) streamARN(region, groupName, streamName string) string {
	return arn.Build("logs", region, b.accountID, "log-group:"+groupName+":log-stream:"+streamName)
}

// groupsStore returns the log-group map for the given region, lazily creating it.
// Callers must hold b.mu.
func (b *InMemoryBackend) groupsStore(region string) map[string]*LogGroup {
	if b.groups[region] == nil {
		b.groups[region] = make(map[string]*LogGroup)
	}

	return b.groups[region]
}

// streamsStore returns the streams map (group -> stream -> *LogStream) for the given
// region, lazily creating it. Callers must hold b.mu.
func (b *InMemoryBackend) streamsStore(region string) map[string]map[string]*LogStream {
	if b.streams[region] == nil {
		b.streams[region] = make(map[string]map[string]*LogStream)
	}

	return b.streams[region]
}

// eventsStore returns the events map (group -> stream -> []*OutputLogEvent) for the
// given region, lazily creating it. Callers must hold b.mu.
func (b *InMemoryBackend) eventsStore(region string) map[string]map[string][]*OutputLogEvent {
	if b.events[region] == nil {
		b.events[region] = make(map[string]map[string][]*OutputLogEvent)
	}

	return b.events[region]
}

// subscriptionFiltersStore returns the subscription-filter map (group -> filters) for
// the given region, lazily creating it. Callers must hold b.mu.
func (b *InMemoryBackend) subscriptionFiltersStore(region string) map[string][]*SubscriptionFilter {
	if b.subscriptionFilters[region] == nil {
		b.subscriptionFilters[region] = make(map[string][]*SubscriptionFilter)
	}

	return b.subscriptionFilters[region]
}

// metricFiltersStore returns the metric-filter map (group -> name -> *MetricFilter) for
// the given region, lazily creating it. Callers must hold b.mu.
func (b *InMemoryBackend) metricFiltersStore(region string) map[string]map[string]*MetricFilter {
	if b.metricFilters[region] == nil {
		b.metricFilters[region] = make(map[string]map[string]*MetricFilter)
	}

	return b.metricFilters[region]
}

// CreateLogGroup creates a new log group with the given class and optional KMS key.
// logGroupClass must be STANDARD or INFREQUENT_ACCESS (defaults to STANDARD if empty).
func (b *InMemoryBackend) CreateLogGroup(
	ctx context.Context,
	name, logGroupClass, kmsKeyID string,
) (*LogGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: logGroupName is required", ErrValidation)
	}

	if !validLogGroupName(name) {
		return nil, fmt.Errorf(
			"%w: logGroupName contains invalid characters (allowed: [a-zA-Z0-9._-/#], length 1-512)",
			ErrValidation,
		)
	}

	if logGroupClass == "" {
		logGroupClass = LogGroupClassStandard
	}

	if _, ok := validLogGroupClasses()[logGroupClass]; !ok {
		return nil, fmt.Errorf(
			"%w: invalid logGroupClass %q, must be STANDARD or INFREQUENT_ACCESS",
			ErrValidation,
			logGroupClass,
		)
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateLogGroup")
	defer b.mu.Unlock()

	groups := b.groupsStore(region)
	if _, exists := groups[name]; exists {
		return nil, fmt.Errorf("%w: Log group %s already exists", ErrLogGroupAlreadyExists, name)
	}

	g := &LogGroup{
		CreationTime:  time.Now().UnixMilli(),
		LogGroupName:  name,
		Arn:           b.groupARN(region, name),
		LogGroupClass: logGroupClass,
		KmsKeyID:      kmsKeyID,
	}
	groups[name] = g
	b.streamsStore(region)[name] = make(map[string]*LogStream)
	b.eventsStore(region)[name] = make(map[string][]*OutputLogEvent)

	if kmsKeyID != "" {
		b.kmsKeys[name] = kmsKeyID
	}

	cp := *g

	return &cp, nil
}

// DeleteLogGroup deletes a log group and all its streams/events.
func (b *InMemoryBackend) DeleteLogGroup(ctx context.Context, name string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteLogGroup")
	defer b.mu.Unlock()

	groups := b.groupsStore(region)
	if _, exists := groups[name]; !exists {
		return fmt.Errorf("%w: Log group %s not found", ErrLogGroupNotFound, name)
	}

	delete(groups, name)
	delete(b.streamsStore(region), name)
	delete(b.eventsStore(region), name)
	delete(b.subscriptionFiltersStore(region), name)
	delete(b.metricFiltersStore(region), name)

	return nil
}

// SetRetentionPolicy sets or clears the retention policy for a log group.
// A nil days value removes any existing retention policy.
func (b *InMemoryBackend) SetRetentionPolicy(
	ctx context.Context,
	groupName string,
	days *int32,
) error {
	if days != nil {
		if _, ok := validRetentionDays()[*days]; !ok {
			return fmt.Errorf(
				"%w: invalid retentionInDays %d, must be one of the allowed values",
				ErrValidation,
				*days,
			)
		}
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("SetRetentionPolicy")
	defer b.mu.Unlock()

	g, exists := b.groupsStore(region)[groupName]
	if !exists {
		return fmt.Errorf("%w: Log group %s not found", ErrLogGroupNotFound, groupName)
	}

	g.RetentionInDays = days

	return nil
}

// DescribeLogGroups returns log groups optionally filtered by prefix, with pagination.
func (b *InMemoryBackend) DescribeLogGroups(
	ctx context.Context, prefix, nextToken string, limit int,
) ([]LogGroup, string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeLogGroups")
	defer b.mu.RUnlock()

	if limit > defaultDescribeLimit {
		limit = defaultDescribeLimit
	}

	regionGroups := b.groupsStore(region)
	all := make([]LogGroup, 0, len(regionGroups))
	for _, g := range regionGroups {
		if prefix == "" || strings.HasPrefix(g.LogGroupName, prefix) {
			all = append(all, *g)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].LogGroupName < all[j].LogGroupName })

	groups, token := paginateGroups(all, nextToken, limit)

	return groups, token, nil
}

// CreateLogStream creates a new log stream within a log group.
func (b *InMemoryBackend) CreateLogStream(
	ctx context.Context,
	groupName, streamName string,
) (*LogStream, error) {
	if groupName == "" {
		return nil, fmt.Errorf("%w: logGroupName is required", ErrValidation)
	}

	if streamName == "" {
		return nil, fmt.Errorf("%w: logStreamName is required", ErrValidation)
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateLogStream")
	defer b.mu.Unlock()

	if _, exists := b.groupsStore(region)[groupName]; !exists {
		return nil, fmt.Errorf("%w: Log group %s not found", ErrLogGroupNotFound, groupName)
	}

	streams := b.streamsStore(region)
	if _, exists := streams[groupName][streamName]; exists {
		return nil, fmt.Errorf(
			"%w: Log stream %s already exists",
			ErrLogStreamAlreadyExist,
			streamName,
		)
	}

	s := &LogStream{
		CreationTime:  time.Now().UnixMilli(),
		LogStreamName: streamName,
		Arn:           b.streamARN(region, groupName, streamName),
	}
	streams[groupName][streamName] = s
	b.eventsStore(region)[groupName][streamName] = nil

	return s, nil
}

// DeleteLogStream deletes a log stream and all its events from a log group.
func (b *InMemoryBackend) DeleteLogStream(ctx context.Context, groupName, streamName string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteLogStream")
	defer b.mu.Unlock()

	groups := b.groupsStore(region)
	if _, exists := groups[groupName]; !exists {
		return fmt.Errorf("%w: Log group %s not found", ErrLogGroupNotFound, groupName)
	}

	streams := b.streamsStore(region)
	if _, exists := streams[groupName][streamName]; !exists {
		return fmt.Errorf("%w: Log stream %s not found", ErrLogStreamNotFound, streamName)
	}

	stream := streams[groupName][streamName]
	if stream != nil && groups[groupName] != nil {
		groups[groupName].StoredBytes -= stream.StoredBytes
	}

	delete(streams[groupName], streamName)
	delete(b.eventsStore(region)[groupName], streamName)

	return nil
}

// sortLogStreams sorts log streams by the given orderBy field and direction.
func sortLogStreams(all []LogStream, orderBy string, descending bool) {
	if orderBy == "LastEventTime" {
		sort.Slice(all, func(i, j int) bool {
			return compareLastEventTime(all[i], all[j], descending)
		})

		return
	}

	sort.Slice(all, func(i, j int) bool {
		if descending {
			return all[i].LogStreamName > all[j].LogStreamName
		}

		return all[i].LogStreamName < all[j].LogStreamName
	})
}

func compareLastEventTime(a, b LogStream, descending bool) bool {
	var ta, tb int64
	if a.LastEventTimestamp != nil {
		ta = *a.LastEventTimestamp
	}
	if b.LastEventTimestamp != nil {
		tb = *b.LastEventTimestamp
	}
	if descending {
		return ta > tb
	}

	return ta < tb
}

// DescribeLogStreams returns log streams for a group, optionally filtered by prefix, with pagination.
// orderBy controls sort field: "LastEventTime" sorts by last event timestamp; anything else sorts by name.
// descending controls sort direction.
// AWS rules: descending=true with orderBy=LogStreamName is invalid;
// logStreamNamePrefix with orderBy=LastEventTime is invalid.
func (b *InMemoryBackend) DescribeLogStreams(
	ctx context.Context, groupName, prefix, nextToken, orderBy string, descending bool, limit int,
) (
	[]LogStream, string, error,
) {
	// Validate (orderBy, descending, prefix) tuple per AWS rules.
	effectiveOrderBy := orderBy
	if effectiveOrderBy == "" {
		effectiveOrderBy = "LogStreamName"
	}

	if effectiveOrderBy == "LogStreamName" && descending {
		return nil, "", fmt.Errorf(
			"%w: descending is only valid when orderBy is LastEventTime",
			ErrValidation,
		)
	}

	if effectiveOrderBy == "LastEventTime" && prefix != "" {
		return nil, "", fmt.Errorf(
			"%w: logStreamNamePrefix cannot be used with orderBy=LastEventTime",
			ErrValidation,
		)
	}

	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeLogStreams")
	defer b.mu.RUnlock()

	if _, exists := b.groupsStore(region)[groupName]; !exists {
		return nil, "", fmt.Errorf("%w: Log group %s not found", ErrLogGroupNotFound, groupName)
	}

	if limit > defaultDescribeLimit {
		limit = defaultDescribeLimit
	}

	groupStreams := b.streamsStore(region)[groupName]
	all := make([]LogStream, 0, len(groupStreams))
	for _, s := range groupStreams {
		if prefix == "" || strings.HasPrefix(s.LogStreamName, prefix) {
			all = append(all, *s)
		}
	}

	sortLogStreams(all, orderBy, descending)

	streams, token := paginateStreams(all, nextToken, limit)

	return streams, token, nil
}

// validatePutLogEventsBatch checks the batch size constraints for PutLogEvents.
func validatePutLogEventsBatch(events []InputLogEvent) error {
	// AWS PutLogEvents limits per request:
	//   * up to 10,000 events
	//   * up to 1 MiB total batch size; each event is counted as
	//     len(message) + 26 bytes of overhead.
	const (
		maxEventsPerBatch  = 10000
		maxBatchBytes      = 1024 * 1024
		eventOverheadBytes = 26
	)

	if len(events) > maxEventsPerBatch {
		return fmt.Errorf("%w: PutLogEvents accepts at most %d events per request",
			ErrValidation, maxEventsPerBatch)
	}

	totalBytes := 0
	for i, e := range events {
		msgLen := len(e.Message)
		if msgLen > putLogEventsMaxMessageBytes {
			return fmt.Errorf(
				"%w: event at index %d exceeds maximum message size of %d bytes",
				ErrValidation, i, putLogEventsMaxMessageBytes,
			)
		}

		totalBytes += msgLen + eventOverheadBytes
	}

	if totalBytes > maxBatchBytes {
		return fmt.Errorf("%w: PutLogEvents batch size %d exceeds %d-byte limit",
			ErrValidation, totalBytes, maxBatchBytes)
	}

	return nil
}

type rejectedTracker struct {
	tooOldStart *int32
	tooNewStart *int32
	expiredEnd  *int32
}

func (t *rejectedTracker) track(
	ts int64,
	idx int32,
	retentionCutoffMs, hardCutoff, futureLimit int64,
) bool {
	if ts > futureLimit {
		if t.tooNewStart == nil {
			t.tooNewStart = &idx
		}

		return false
	}

	// Reject events older than the 14-day hard cap.
	if ts < hardCutoff {
		if t.tooOldStart == nil {
			t.tooOldStart = &idx
		}

		return false
	}

	// Events beyond the group's retention window are marked as expired
	// in the response but are still stored; the janitor evicts them later.
	if retentionCutoffMs > hardCutoff && ts < retentionCutoffMs {
		if t.expiredEnd == nil || idx > *t.expiredEnd {
			t.expiredEnd = &idx
		}
	}

	return true
}

// classifyLogEvents splits events into accepted and rejected based on timestamp windows.
// It returns the accepted events and rejected-event index pointers for the response.
func classifyLogEvents(
	events []InputLogEvent,
	retentionCutoffMs, hardCutoff, futureLimit int64,
) ([]InputLogEvent, *RejectedLogEventsInfo) {
	var acceptedEvents []InputLogEvent
	var tracker rejectedTracker

	for i, e := range events {
		idx := int32(i)

		// Synthetic test timestamps (before Sep 2001) bypass window validation.
		if e.Timestamp < minRealisticTimestampMs {
			acceptedEvents = append(acceptedEvents, e)

			continue
		}

		if tracker.track(e.Timestamp, idx, retentionCutoffMs, hardCutoff, futureLimit) {
			acceptedEvents = append(acceptedEvents, e)
		}
	}

	var rejectedInfo *RejectedLogEventsInfo
	if tracker.tooNewStart != nil || tracker.tooOldStart != nil || tracker.expiredEnd != nil {
		rejectedInfo = &RejectedLogEventsInfo{
			TooNewLogEventStartIndex: tracker.tooNewStart,
			TooOldLogEventStartIndex: tracker.tooOldStart,
			ExpiredLogEventEndIndex:  tracker.expiredEnd,
		}
	}

	return acceptedEvents, rejectedInfo
}

// PutLogEvents appends log events to a stream and returns a PutLogEventsResult.
// sequenceToken is optional; if provided and mismatched, returns ErrInvalidSequenceToken.
// Events with timestamps outside the allowed window are tracked in RejectedLogEventsInfo.
func (b *InMemoryBackend) PutLogEvents(
	ctx context.Context,
	groupName, streamName, sequenceToken string,
	events []InputLogEvent,
) (*PutLogEventsResult, error) {
	if err := validatePutLogEventsBatch(events); err != nil {
		return nil, err
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("PutLogEvents")

	groups := b.groupsStore(region)
	if _, exists := groups[groupName]; !exists {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: Log group %s not found", ErrLogGroupNotFound, groupName)
	}

	streams := b.streamsStore(region)
	groupEvents := b.eventsStore(region)
	if _, exists := streams[groupName][streamName]; !exists {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: Log stream %s not found", ErrLogStreamNotFound, streamName)
	}

	stream := streams[groupName][streamName]

	// Validate sequence token if provided. AWS still returns InvalidSequenceTokenException
	// for wrong tokens even though tokens are now optional.
	if sequenceToken != "" {
		expectedToken := strconv.FormatInt(int64(len(groupEvents[groupName][streamName])), 10)
		if sequenceToken != expectedToken {
			b.mu.Unlock()

			return nil, fmt.Errorf(
				"%w: expected sequenceToken %s",
				ErrInvalidSequenceToken,
				expectedToken,
			)
		}
	}

	now := time.Now().UnixMilli()

	// Determine the retention cutoff for timestamp validation.
	group := groups[groupName]
	var retentionCutoffMs int64
	if group.RetentionInDays != nil && *group.RetentionInDays > 0 {
		retentionCutoffMs = now - int64(*group.RetentionInDays)*msPerDay
	} else {
		retentionCutoffMs = now - putLogEventsMaxEventAgeMs
	}

	hardCutoff := now - putLogEventsMaxEventAgeMs
	futureLimit := now + putLogEventsFutureWindowMs
	acceptedEvents, rejectedInfo := classifyLogEvents(
		events,
		retentionCutoffMs,
		hardCutoff,
		futureLimit,
	)

	b.appendEvents(region, groupName, streamName, stream, now, acceptedEvents)

	stream.LastIngestionTime = &now
	nextToken := strconv.FormatInt(int64(len(groupEvents[groupName][streamName])), 10)

	// Collect matching subscription filters and metric filter matches while holding the lock.
	filters := b.matchingFilters(region, groupName, acceptedEvents)
	eventsForDelivery := append([]InputLogEvent(nil), acceptedEvents...)
	filtersForDelivery := cloneSubscriptionFilters(filters)
	metricMatches := b.matchingMetricFilters(region, groupName, acceptedEvents)
	emitter := b.metricEmitter

	b.mu.Unlock()

	// Emit CloudWatch metrics for matched metric filters (no lock held).
	if len(metricMatches) > 0 && emitter != nil {
		b.emitMetricFilterMatches(emitter, metricMatches)
	}

	b.scheduleFilterDelivery(groupName, streamName, eventsForDelivery, filtersForDelivery)

	return &PutLogEventsResult{
		NextSequenceToken:     nextToken,
		RejectedLogEventsInfo: rejectedInfo,
	}, nil
}

// scheduleFilterDelivery asynchronously delivers accepted log events to matching
// subscription filters. No-ops when there are no filters or no deliverer configured.
func (b *InMemoryBackend) scheduleFilterDelivery(
	groupName, streamName string,
	events []InputLogEvent,
	filters []*SubscriptionFilter,
) {
	if len(filters) == 0 || b.deliverer == nil {
		return
	}
	b.wg.Go(func() {
		// Acquire a worker slot or abort if the backend is shutting down.
		select {
		case b.workerSem <- struct{}{}:
			defer func() { <-b.workerSem }()
		case <-b.ctx.Done():
			return
		}
		b.deliverToFilters(
			b.ctx,
			groupName,
			streamName,
			b.accountID,
			events,
			filters,
			b.deliverer,
			b.deliveryTimeout,
		)
	})
}

// appendEvents writes events into the stream, updates stream timestamp metadata,
// and enforces the per-stream event cap.
// Must be called while holding the backend write lock.
// Note: log events may arrive with out-of-order timestamps (AWS allows this),
// so min/max timestamp tracking must inspect all events.
func (b *InMemoryBackend) appendEvents(
	region, groupName, streamName string, stream *LogStream, now int64, events []InputLogEvent,
) {
	groupEvents := b.eventsStore(region)
	groups := b.groupsStore(region)
	for _, ev := range events {
		idx := len(groupEvents[groupName][streamName])
		ptr := base64.StdEncoding.EncodeToString(
			fmt.Appendf(nil, "%s/%s/%d", groupName, streamName, idx),
		)
		out := &OutputLogEvent{
			IngestionTime: now,
			Message:       ev.Message,
			Timestamp:     ev.Timestamp,
			Ptr:           ptr,
		}
		groupEvents[groupName][streamName] = append(groupEvents[groupName][streamName], out)

		msgLen := int64(len(ev.Message))
		stream.StoredBytes += msgLen
		groups[groupName].StoredBytes += msgLen

		if stream.FirstEventTimestamp == nil || ev.Timestamp < *stream.FirstEventTimestamp {
			ts := ev.Timestamp
			stream.FirstEventTimestamp = &ts
		}
		if stream.LastEventTimestamp == nil || ev.Timestamp > *stream.LastEventTimestamp {
			ts := ev.Timestamp
			stream.LastEventTimestamp = &ts
		}
	}

	// Enforce per-stream event cap: keep only the most recent maxEventsPerStream events.
	if cur := groupEvents[groupName][streamName]; len(cur) > maxEventsPerStream {
		groupEvents[groupName][streamName] = cur[len(cur)-maxEventsPerStream:]
		// Recalculate metadata from the remaining events: since events may have
		// out-of-order timestamps, the dropped events might include the global
		// min/max, so we must re-scan rather than assume positional ordering.
		updateStreamTimestamps(stream, groupEvents[groupName][streamName])
	}
}

// GetLogEvents returns events for a stream with optional time bounds, limit, and pagination.
// startFromHead controls the iteration direction:
//   - true  (start from oldest): begin at the oldest matching event.
//   - false (AWS default when no nextToken is provided): begin at the newest events.
//
// In practice the AWS SDK always passes a nextToken once pagination begins, at which point the
// token encodes the offset directly and startFromHead is ignored.
func (b *InMemoryBackend) GetLogEvents(
	ctx context.Context,
	groupName, streamName string,
	startTime, endTime *int64,
	limit int,
	nextToken string,
	startFromHead bool,
) ([]OutputLogEvent, string, string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("GetLogEvents")
	defer b.mu.RUnlock()

	if _, exists := b.groupsStore(region)[groupName]; !exists {
		return nil, "", "", fmt.Errorf("%w: Log group %s not found", ErrLogGroupNotFound, groupName)
	}

	if _, exists := b.streamsStore(region)[groupName][streamName]; !exists {
		return nil, "", "", fmt.Errorf(
			"%w: Log stream %s not found",
			ErrLogStreamNotFound,
			streamName,
		)
	}

	all := b.eventsStore(region)[groupName][streamName]
	filtered := filterByTime(all, startTime, endTime)

	if limit <= 0 {
		limit = defaultEventLimit
	}

	var startIdx int
	if nextToken != "" {
		// An explicit token always takes precedence over startFromHead.
		startIdx = parseNextToken(nextToken)
	} else if !startFromHead {
		// No token + startFromHead=false (the AWS default): begin at the last page.
		if len(filtered) > limit {
			startIdx = len(filtered) - limit
		}
	}
	// nextToken=="" && startFromHead=true: startIdx stays 0 (oldest first).

	end := min(startIdx+limit, len(filtered))

	page := filtered[startIdx:end]

	fwdToken := encodeNextToken(end)
	bwdToken := encodeNextToken(startIdx)

	result := make([]OutputLogEvent, len(page))
	for i, e := range page {
		result[i] = *e
	}

	return result, fwdToken, bwdToken, nil
}

// FilterLogEventsParams holds the inputs for InMemoryBackend.FilterLogEvents.
type FilterLogEventsParams struct {
	StartTime           *int64
	EndTime             *int64
	GroupName           string
	FilterPattern       string
	NextToken           string
	LogStreamNamePrefix string
	StreamNames         []string
	Limit               int
}

// taggedEvent pairs a stored event with the name of the stream it came from so
// FilterLogEvents can populate the logStreamName field on each FilteredLogEvent.
type taggedEvent struct {
	ev     *OutputLogEvent
	stream string
}

// FilterLogEvents searches events across streams in a group with an optional
// filter pattern. Results are interleaved across streams and sorted by event
// timestamp (ascending), matching AWS behaviour. The returned events carry the
// originating logStreamName and a deterministic eventId.
func (b *InMemoryBackend) FilterLogEvents(
	ctx context.Context,
	p FilterLogEventsParams,
) ([]FilteredLogEvent, string, []SearchedLogStream, error) {
	// AWS rejects requests that set both logStreamNames and logStreamNamePrefix.
	if len(p.StreamNames) > 0 && p.LogStreamNamePrefix != "" {
		return nil, "", nil, fmt.Errorf(
			"%w: logStreamNames and logStreamNamePrefix are mutually exclusive", ErrValidation)
	}

	region := getRegion(ctx, b.region)

	b.mu.RLock("FilterLogEvents")
	defer b.mu.RUnlock()

	if _, exists := b.groupsStore(region)[p.GroupName]; !exists {
		return nil, "", nil, fmt.Errorf(
			"%w: Log group %s not found",
			ErrLogGroupNotFound,
			p.GroupName,
		)
	}

	// Compile the filter pattern once before iterating over events so that
	// wildcard regexes are not recompiled for every event.
	var compiled *compiledFilterPattern
	if p.FilterPattern != "" {
		compiled = compileFilterPattern(p.FilterPattern)
	}

	streamOrder := b.filterStreamOrderLocked(region, p.GroupName, p.StreamNames)
	if p.LogStreamNamePrefix != "" {
		streamOrder = filterStreamsByPrefix(streamOrder, p.LogStreamNamePrefix)
	}
	groupEvents := b.eventsStore(region)

	var all []taggedEvent

	for _, sName := range streamOrder {
		for _, ev := range groupEvents[p.GroupName][sName] {
			if compiled != nil && !compiled.matches(ev.Message) {
				continue
			}
			all = append(all, taggedEvent{ev: ev, stream: sName})
		}
	}

	all = filterTaggedByTime(all, p.StartTime, p.EndTime)
	// Interleave across streams: AWS returns matched events sorted by timestamp.
	// A stable sort preserves per-stream ingestion order for equal timestamps.
	sort.SliceStable(all, func(i, j int) bool {
		return all[i].ev.Timestamp < all[j].ev.Timestamp
	})

	startIdx := parseNextToken(p.NextToken)
	limit := p.Limit
	if limit <= 0 {
		limit = defaultEventLimit
	}

	end := startIdx + limit
	var outToken string
	if end < len(all) {
		outToken = encodeNextToken(end)
	} else {
		end = len(all)
	}
	if startIdx > len(all) {
		startIdx = len(all)
	}

	page := all[startIdx:end]
	result := make([]FilteredLogEvent, len(page))
	for i, te := range page {
		result[i] = FilteredLogEvent{
			EventID:       filteredEventID(p.GroupName, te.stream, te.ev),
			LogStreamName: te.stream,
			Message:       te.ev.Message,
			IngestionTime: te.ev.IngestionTime,
			Timestamp:     te.ev.Timestamp,
		}
	}

	// AWS deprecated searchedLogStreams (it returns an empty list). We mirror
	// that contract rather than fabricating data clients should not rely on.
	return result, outToken, []SearchedLogStream{}, nil
}

// filterStreamsByPrefix returns only the stream names that start with prefix,
// preserving order.
func filterStreamsByPrefix(streams []string, prefix string) []string {
	out := make([]string, 0, len(streams))
	for _, s := range streams {
		if strings.HasPrefix(s, prefix) {
			out = append(out, s)
		}
	}

	return out
}

// filterTaggedByTime applies the start/end time window to tagged events.
func filterTaggedByTime(events []taggedEvent, startTime, endTime *int64) []taggedEvent {
	if startTime == nil && endTime == nil {
		return events
	}

	out := make([]taggedEvent, 0, len(events))
	for _, te := range events {
		if startTime != nil && te.ev.Timestamp < *startTime {
			continue
		}
		if endTime != nil && te.ev.Timestamp > *endTime {
			continue
		}
		out = append(out, te)
	}

	return out
}

// filteredEventID derives a deterministic, opaque event ID for a filtered event.
// AWS returns a 56-character numeric eventId; we reuse the event's stable byte
// pointer so the same event always yields the same ID without storing extra state.
func filteredEventID(groupName, streamName string, ev *OutputLogEvent) string {
	if ev.Ptr != "" {
		return ev.Ptr
	}

	return base64.StdEncoding.EncodeToString(
		fmt.Appendf(nil, "%s/%s/%d/%d", groupName, streamName, ev.Timestamp, ev.IngestionTime))
}

// PutSubscriptionFilter creates or updates a subscription filter for a log group.
// roleArn is required by AWS when delivering to Kinesis streams; distribution defaults to Random.
func (b *InMemoryBackend) PutSubscriptionFilter(
	ctx context.Context,
	groupName, filterName, filterPattern, destinationArn, roleArn, distribution string,
) error {
	if groupName == "" {
		return fmt.Errorf("%w: logGroupName is required", ErrValidation)
	}

	if filterName == "" {
		return fmt.Errorf("%w: filterName is required", ErrValidation)
	}

	if destinationArn == "" {
		return fmt.Errorf("%w: destinationArn is required", ErrValidation)
	}

	if distribution == "" {
		distribution = DistributionRandom
	}

	if _, ok := validDistributions()[distribution]; !ok {
		return fmt.Errorf(
			"%w: invalid distribution %q, must be Random or ByLogStream",
			ErrValidation,
			distribution,
		)
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("PutSubscriptionFilter")
	defer b.mu.Unlock()

	if _, exists := b.groupsStore(region)[groupName]; !exists {
		return fmt.Errorf("%w: Log group %s not found", ErrLogGroupNotFound, groupName)
	}

	subFilters := b.subscriptionFiltersStore(region)
	existing := subFilters[groupName]

	// Check for a filter with the same name (update).
	for i, f := range existing {
		if f.FilterName == filterName {
			existing[i].FilterPattern = filterPattern
			existing[i].DestinationArn = destinationArn
			existing[i].RoleArn = roleArn
			existing[i].Distribution = distribution

			return nil
		}
	}

	// Enforce AWS limit of 2 subscription filters per log group.
	if len(existing) >= maxSubscriptionFilters {
		return fmt.Errorf("%w: log group %s already has the maximum number of subscription filters",
			ErrSubscriptionFilterLimitExceed, groupName)
	}

	subFilters[groupName] = append(existing, &SubscriptionFilter{
		FilterName:     filterName,
		FilterPattern:  filterPattern,
		LogGroupName:   groupName,
		DestinationArn: destinationArn,
		RoleArn:        roleArn,
		Distribution:   distribution,
		CreationTime:   time.Now().UnixMilli(),
	})

	return nil
}

// DescribeSubscriptionFilters returns subscription filters for a log group with optional prefix and pagination.
func (b *InMemoryBackend) DescribeSubscriptionFilters(
	ctx context.Context, groupName, filterNamePrefix, nextToken string, limit int,
) (
	[]SubscriptionFilter, string, error,
) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeSubscriptionFilters")
	defer b.mu.RUnlock()

	if _, exists := b.groupsStore(region)[groupName]; !exists {
		return nil, "", fmt.Errorf("%w: Log group %s not found", ErrLogGroupNotFound, groupName)
	}

	groupFilters := b.subscriptionFiltersStore(region)[groupName]
	all := make([]SubscriptionFilter, 0, len(groupFilters))
	for _, f := range groupFilters {
		if filterNamePrefix == "" || strings.HasPrefix(f.FilterName, filterNamePrefix) {
			all = append(all, *f)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].FilterName < all[j].FilterName })

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(all) {
		return []SubscriptionFilter{}, "", nil
	}

	if limit <= 0 {
		limit = defaultDescribeLimit
	}

	end := startIdx + limit
	var outToken string
	if end < len(all) {
		outToken = encodeNextToken(end)
	} else {
		end = len(all)
	}

	return all[startIdx:end], outToken, nil
}

// DeleteSubscriptionFilter removes a subscription filter from a log group.
func (b *InMemoryBackend) DeleteSubscriptionFilter(
	ctx context.Context,
	groupName, filterName string,
) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteSubscriptionFilter")
	defer b.mu.Unlock()

	if _, exists := b.groupsStore(region)[groupName]; !exists {
		return fmt.Errorf("%w: Log group %s not found", ErrLogGroupNotFound, groupName)
	}

	subFilters := b.subscriptionFiltersStore(region)
	filters := subFilters[groupName]
	for i, f := range filters {
		if f.FilterName == filterName {
			subFilters[groupName] = append(filters[:i], filters[i+1:]...)

			return nil
		}
	}

	return fmt.Errorf("%w: subscription filter %s not found in log group %s",
		ErrSubscriptionFilterNotFound, filterName, groupName)
}

// matchingFilters returns subscription filters whose pattern matches any of the given events.
// Must be called with the write lock held (called from PutLogEvents before Unlock).
func (b *InMemoryBackend) matchingFilters(
	region, groupName string,
	events []InputLogEvent,
) []*SubscriptionFilter {
	filters := b.subscriptionFiltersStore(region)[groupName]
	if len(filters) == 0 {
		return nil
	}

	var matched []*SubscriptionFilter
	for _, f := range filters {
		compiled := b.getCompiledPattern(f.FilterPattern)
		if filterMatchesCompiled(compiled, events) {
			matched = append(matched, f)
		}
	}

	return matched
}

// metricFilterMatch holds a metric filter and the count of events that matched it.
type metricFilterMatch struct {
	filter     *MetricFilter
	matchCount int
}

// matchingMetricFilters returns metric filters for groupName whose pattern matches at least one
// of the given events, along with the per-filter match count.
// Events outer, filters inner: each event is visited once regardless of filter count,
// cutting allocations from O(filters×events) repeated scans to a single pass.
// Must be called while holding the write lock.
func (b *InMemoryBackend) matchingMetricFilters(
	region, groupName string,
	events []InputLogEvent,
) []metricFilterMatch {
	mfMap := b.metricFiltersStore(region)[groupName]
	if len(mfMap) == 0 {
		return nil
	}

	// Pre-compile all patterns once, keyed by filter name.
	type filterEntry struct {
		filter   *MetricFilter
		compiled *compiledFilterPattern
	}
	entries := make([]filterEntry, 0, len(mfMap))
	for _, f := range mfMap {
		entries = append(
			entries,
			filterEntry{filter: f, compiled: b.getCompiledPattern(f.FilterPattern)},
		)
	}

	counts := make([]int, len(entries))
	for _, ev := range events {
		for i, e := range entries {
			if e.compiled == nil || e.compiled.matches(ev.Message) {
				counts[i]++
			}
		}
	}

	var matched []metricFilterMatch
	for i, e := range entries {
		if counts[i] > 0 {
			cp := *e.filter
			matched = append(matched, metricFilterMatch{filter: &cp, matchCount: counts[i]})
		}
	}

	return matched
}

// emitMetricFilterMatches calls the MetricEmitter for each matched metric filter transformation.
// One data point is emitted per matched event per transformation.
func (b *InMemoryBackend) emitMetricFilterMatches(
	emitter MetricEmitter,
	matches []metricFilterMatch,
) {
	for _, m := range matches {
		for _, t := range m.filter.MetricTransformations {
			val, parseErr := strconv.ParseFloat(t.MetricValue, 64)
			if parseErr != nil {
				// Non-numeric MetricValue (e.g. "$field") falls back to defaultValue or 1.0.
				val = 1.0
				if t.DefaultValue != nil {
					val = *t.DefaultValue
				}
			}
			for range m.matchCount {
				if emitErr := emitter.EmitMetric(t.MetricNamespace, t.MetricName, val, ""); emitErr != nil {
					logger.Load(b.ctx).Warn(
						"cloudwatchlogs: metric filter emit failed",
						"namespace", t.MetricNamespace,
						"metric", t.MetricName,
						"err", emitErr,
					)
				}
			}
		}
	}
}

// getCompiledPattern returns a cached compiled filter pattern, compiling and caching it on first use.
func (b *InMemoryBackend) getCompiledPattern(pattern string) *compiledFilterPattern {
	b.compiledPatternsMu.RLock()
	if c, ok := b.compiledPatterns[pattern]; ok {
		b.compiledPatternsMu.RUnlock()

		return c
	}
	b.compiledPatternsMu.RUnlock()

	c := compileFilterPattern(pattern)

	b.compiledPatternsMu.Lock()
	if len(b.compiledPatterns) < maxCompiledPatternCache {
		b.compiledPatterns[pattern] = c
	}
	b.compiledPatternsMu.Unlock()

	return c
}

// filterMatchesCompiled returns true when the compiled filter pattern matches at least one event.
// A nil compiled pattern matches all events.
func filterMatchesCompiled(compiled *compiledFilterPattern, events []InputLogEvent) bool {
	if compiled == nil {
		return len(events) > 0
	}

	for _, ev := range events {
		if compiled.matches(ev.Message) {
			return true
		}
	}

	return false
}

// deliverToFilters builds the subscription payload and delivers it to each matched filter destination.
func (b *InMemoryBackend) deliverToFilters(
	ctx context.Context,
	groupName, streamName, accountID string,
	events []InputLogEvent,
	filters []*SubscriptionFilter,
	deliverer SubscriptionDeliverer,
	timeout time.Duration,
) {
	filterNames := make([]string, len(filters))
	for i, f := range filters {
		filterNames[i] = f.FilterName
	}

	logEvts := make([]subscriptionLogEvent, len(events))
	for i, ev := range events {
		logEvts[i] = subscriptionLogEvent{
			ID:        uuid.New().String(),
			Timestamp: ev.Timestamp,
			Message:   ev.Message,
		}
	}

	payload := subscriptionPayload{
		MessageType:         "DATA_MESSAGE",
		Owner:               accountID,
		LogGroup:            groupName,
		LogStream:           streamName,
		SubscriptionFilters: filterNames,
		LogEvents:           logEvts,
	}

	encoded, err := encodeSubscriptionPayload(payload)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "cloudwatchlogs: failed to encode subscription payload",
			"logGroup", groupName, "error", err)

		return
	}

	for _, f := range filters {
		deliverCtx := ctx
		var cancel context.CancelFunc
		if timeout > 0 {
			deliverCtx, cancel = context.WithTimeout(ctx, timeout)
		}

		deliverErr := deliverer.DeliverLogEvents(deliverCtx, f.DestinationArn, encoded)
		if cancel != nil {
			cancel()
		}

		if deliverErr != nil {
			logger.Load(ctx).
				WarnContext(ctx, "cloudwatchlogs: failed to deliver log events to subscription filter",
					"logGroup", groupName, "filterName", f.FilterName, "destination", f.DestinationArn, "error", deliverErr)
		}
	}
}

// encodeSubscriptionPayload gzips the JSON payload and base64-encodes it.
func encodeSubscriptionPayload(payload subscriptionPayload) ([]byte, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)

	if _, err = gz.Write(raw); err != nil {
		return nil, err
	}

	if err = gz.Close(); err != nil {
		return nil, err
	}

	encoded := base64.StdEncoding.EncodeToString(buf.Bytes())

	return []byte(encoded), nil
}

// compiledFilterPattern holds a parsed and pre-compiled filter pattern for efficient
// repeated matching across many log events (used by FilterLogEvents, subscription
// filters and metric filters).
//
// AWS unstructured (plain-text) filter-pattern semantics (see
// https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/FilterAndPatternSyntax.html):
//
//   - Plain / quoted terms ("required") are AND-ed: every one must be present.
//   - "-term" (exclude) terms must NOT be present.
//   - "?term" (optional) terms are OR-ed: a message matches if it contains ANY of
//     them. AWS documents that when "?" terms are combined with required or exclude
//     terms, the "?" terms are ignored entirely; we honour that rule, so optional
//     terms only take effect when there are no required and no exclude terms.
type compiledFilterPattern struct {
	required []compiledTerm // AND: all must match
	optional []compiledTerm // OR: any matches (only used when required+exclude empty)
	exclude  []compiledTerm // NONE may match
}

// compiledTerm holds a single pre-compiled term from a filter pattern.
type compiledTerm struct {
	// exact is the literal substring for quoted/plain terms (non-wildcard).
	// re is used for wildcard terms.
	re      *regexp.Regexp
	exact   string
	isExact bool // true => use exact (strings.Contains); false => use re
}

// match reports whether the message satisfies this single term.
func (ct compiledTerm) match(message string) bool {
	if ct.isExact {
		return strings.Contains(message, ct.exact)
	}

	return ct.re.MatchString(message)
}

// compileTerm compiles a single (prefix-stripped) raw term into a compiledTerm.
// Quoted terms become exact substrings, terms containing "*" become wildcard
// regexes, and everything else is a plain substring.
func compileTerm(t string) compiledTerm {
	var ct compiledTerm

	switch {
	case len(t) >= 2 && t[0] == '"' && t[len(t)-1] == '"':
		ct.isExact = true
		ct.exact = t[1 : len(t)-1]
	case strings.ContainsRune(t, '*'):
		parts := strings.Split(t, "*")
		escaped := make([]string, len(parts))
		for i, p := range parts {
			escaped[i] = regexp.QuoteMeta(p)
		}
		re, err := regexp.Compile(strings.Join(escaped, ".*"))
		if err != nil {
			// The wildcard expansion produced an invalid regex (this should not
			// happen in practice because QuoteMeta escapes all special chars).
			// Fall back to treating the raw term as a plain substring so the
			// caller still receives a deterministic (if approximate) result.
			ct.isExact = true
			ct.exact = t
		} else {
			ct.re = re
		}
	default:
		ct.isExact = true
		ct.exact = t
	}

	return ct
}

// compileFilterPattern parses pattern into a compiledFilterPattern for efficient reuse.
// An empty pattern always matches all messages.
func compileFilterPattern(pattern string) *compiledFilterPattern {
	rawTerms := parseFilterPatternTerms(pattern)
	cp := &compiledFilterPattern{}

	for _, raw := range rawTerms {
		switch {
		case strings.HasPrefix(raw, "?") && len(raw) > 1:
			cp.optional = append(cp.optional, compileTerm(raw[1:]))
		case strings.HasPrefix(raw, "-") && len(raw) > 1:
			cp.exclude = append(cp.exclude, compileTerm(raw[1:]))
		default:
			cp.required = append(cp.required, compileTerm(raw))
		}
	}

	return cp
}

// matches reports whether the message satisfies the pattern, following AWS
// unstructured filter-pattern semantics.
func (p *compiledFilterPattern) matches(message string) bool {
	// Exclude terms: the message must not contain any of them.
	for _, ct := range p.exclude {
		if ct.match(message) {
			return false
		}
	}

	// Required terms: all must be present (AND).
	for _, ct := range p.required {
		if !ct.match(message) {
			return false
		}
	}

	// Optional ("?") terms only take effect when there are no required and no
	// exclude terms; AWS ignores "?" terms when combined with other terms.
	if len(p.optional) > 0 && len(p.required) == 0 && len(p.exclude) == 0 {
		for _, ct := range p.optional {
			if ct.match(message) {
				return true
			}
		}

		return false
	}

	return true
}

// filterPatternMatches returns true when the CloudWatch Logs filter pattern matches the message.
//
// Pattern syntax (AWS unstructured / plain-text):
//   - Empty pattern matches all messages.
//   - Space-separated plain or quoted terms are AND-ed: all must be present.
//   - A term prefixed with "?" is optional (OR): the message matches if it
//     contains any "?" term. "?" terms are ignored when combined with plain or
//     "-" terms (matching AWS behaviour).
//   - A term prefixed with "-" must NOT appear in the message.
//   - Quoted terms ("...") require an exact substring match.
//   - "*" inside a term is a wildcard.
func filterPatternMatches(pattern, message string) bool {
	return compileFilterPattern(pattern).matches(message)
}

// parseFilterPatternTerms splits a filter pattern into individual terms,
// respecting double-quoted phrases.
func parseFilterPatternTerms(pattern string) []string {
	var terms []string
	var cur strings.Builder
	inQuote := false

	for i := range len(pattern) {
		ch := pattern[i]

		switch {
		case ch == '"':
			inQuote = !inQuote
			cur.WriteByte(ch)
		case ch == ' ' && !inQuote:
			if cur.Len() > 0 {
				terms = append(terms, cur.String())
				cur.Reset()
			}
		default:
			cur.WriteByte(ch)
		}
	}

	if cur.Len() > 0 {
		terms = append(terms, cur.String())
	}

	return terms
}

func filterByTime(events []*OutputLogEvent, startTime, endTime *int64) []*OutputLogEvent {
	if startTime == nil && endTime == nil {
		return events
	}

	out := make([]*OutputLogEvent, 0, len(events))
	for _, ev := range events {
		if startTime != nil && ev.Timestamp < *startTime {
			continue
		}
		if endTime != nil && ev.Timestamp > *endTime {
			continue
		}
		out = append(out, ev)
	}

	return out
}

func sortedKeys(m map[string]*LogStream) []string {
	keys := collections.SortedKeys(m)

	return keys
}

// filterStreamOrderLocked returns the ordered list of stream names to iterate
// for FilterLogEvents. When streamNames is empty, all streams in the group are
// returned in sorted order. When streamNames is non-empty, only the requested
// names that exist in the group are returned, in sorted order, deduplicated.
// Caller must hold b.mu (read or write).
func (b *InMemoryBackend) filterStreamOrderLocked(
	region, groupName string,
	streamNames []string,
) []string {
	groupStreams := b.streamsStore(region)[groupName]
	if len(streamNames) == 0 {
		return sortedKeys(groupStreams)
	}

	seen := make(map[string]bool, len(streamNames))
	out := make([]string, 0, len(streamNames))

	for _, s := range streamNames {
		if seen[s] {
			continue
		}

		seen[s] = true

		if _, ok := groupStreams[s]; ok {
			out = append(out, s)
		}
	}

	sort.Strings(out)

	return out
}

func paginateGroups(all []LogGroup, nextToken string, limit int) ([]LogGroup, string) {
	startIdx := parseNextToken(nextToken)
	if startIdx >= len(all) {
		return []LogGroup{}, ""
	}

	if limit <= 0 {
		limit = defaultDescribeLimit
	}

	end := startIdx + limit

	var outToken string
	if end < len(all) {
		outToken = encodeNextToken(end)
	} else {
		end = len(all)
	}

	return all[startIdx:end], outToken
}

func paginateStreams(all []LogStream, nextToken string, limit int) ([]LogStream, string) {
	startIdx := parseNextToken(nextToken)
	if startIdx >= len(all) {
		return []LogStream{}, ""
	}

	if limit <= 0 {
		limit = defaultDescribeLimit
	}

	end := startIdx + limit

	var outToken string
	if end < len(all) {
		outToken = encodeNextToken(end)
	} else {
		end = len(all)
	}

	return all[startIdx:end], outToken
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

func cloneSubscriptionFilters(filters []*SubscriptionFilter) []*SubscriptionFilter {
	if len(filters) == 0 {
		return nil
	}

	out := make([]*SubscriptionFilter, len(filters))
	for i, f := range filters {
		cp := *f
		out[i] = &cp
	}

	return out
}

// removeFromOrder removes the first occurrence of queryID from queriesOrder.
// It must be called while holding the write lock.
func (b *InMemoryBackend) removeFromOrder(queryID string) {
	for i, qid := range b.queriesOrder {
		if qid == queryID {
			b.queriesOrder = append(b.queriesOrder[:i], b.queriesOrder[i+1:]...)

			return
		}
	}
}

// evictByTTL removes queries whose age has exceeded the configured TTL.
// It must be called while holding the write lock.
func (b *InMemoryBackend) evictByTTL() {
	if b.queryTTL <= 0 {
		return
	}

	cutoff := time.Now().Add(-b.queryTTL)
	newOrder := make([]string, 0, len(b.queriesOrder))
	for _, qid := range b.queriesOrder {
		sq, ok := b.queries[qid]
		if !ok {
			// Entry already removed from the map; drop the stale order reference.
			continue
		}
		if sq.createdAt.Before(cutoff) {
			delete(b.queries, qid)

			continue
		}
		newOrder = append(newOrder, qid)
	}
	b.queriesOrder = newOrder
}

// enforceCap drops the oldest queries when the stored count exceeds the configured cap.
// It must be called while holding the write lock.
func (b *InMemoryBackend) enforceCap() {
	if b.maxQueries <= 0 || len(b.queriesOrder) <= b.maxQueries {
		return
	}

	excess := len(b.queriesOrder) - b.maxQueries
	for _, qid := range b.queriesOrder[:excess] {
		delete(b.queries, qid)
	}
	b.queriesOrder = b.queriesOrder[excess:]
}

func (b *InMemoryBackend) getParsedInsightsQuery(queryString string) (*insightsQuery, error) {
	b.mu.RLock("getParsedInsightsQueryRead")
	cached, ok := b.parsedQueries[queryString]
	b.mu.RUnlock()
	if ok {
		return cloneInsightsQuery(cached), nil
	}

	parsed, parseErr := parseInsightsQuery(queryString)
	if parseErr != nil {
		return nil, parseErr
	}

	b.mu.Lock("getParsedInsightsQueryWrite")
	defer b.mu.Unlock()

	if cached, ok = b.parsedQueries[queryString]; ok {
		return cloneInsightsQuery(cached), nil
	}

	if b.maxParsedQueries > 0 && len(b.parsedQueriesOrder) >= b.maxParsedQueries {
		evictKey := b.parsedQueriesOrder[0]
		b.parsedQueriesOrder = b.parsedQueriesOrder[1:]
		delete(b.parsedQueries, evictKey)
	}

	b.parsedQueries[queryString] = parsed
	b.parsedQueriesOrder = append(b.parsedQueriesOrder, queryString)

	return cloneInsightsQuery(parsed), nil
}

// StartQuery stores a new insights query and executes it immediately against in-memory events.
// collectQueryEvents scans events in the given log groups within [startTime, endTime].
// Returns matching events, the total records scanned, and the total bytes scanned.
// It must be called while holding at least a read lock.
func (b *InMemoryBackend) collectQueryEvents(
	region string, logGroupNames []string, startTime, endTime int64,
) ([]*OutputLogEvent, float64, float64) {
	var eventsOut []*OutputLogEvent
	var recordsScanned, bytesScanned float64

	groupEvents := b.eventsStore(region)
	for _, groupName := range logGroupNames {
		streamMap, exists := groupEvents[groupName]
		if !exists {
			continue
		}
		for _, evts := range streamMap {
			for _, ev := range evts {
				recordsScanned++
				bytesScanned += float64(len(ev.Message))
				if startTime > 0 && ev.Timestamp < startTime {
					continue
				}
				if endTime > 0 && ev.Timestamp > endTime {
					continue
				}
				eventsOut = append(eventsOut, ev)
			}
		}
	}

	return eventsOut, recordsScanned, bytesScanned
}

// StartQuery stores a new insights query and executes it immediately against in-memory events.
func (b *InMemoryBackend) StartQuery(
	ctx context.Context,
	queryID, queryString string,
	logGroupNames []string,
	startTime, endTime int64,
) (*QueryInfo, error) {
	q, parseErr := b.getParsedInsightsQuery(queryString)
	if parseErr != nil {
		return nil, fmt.Errorf("invalid query: %w", parseErr)
	}

	region := getRegion(ctx, b.region)

	// Collect events under a read lock, then release the lock before running the
	// query. This prevents regex matching and sorting from holding the lock while
	// still delivering a consistent snapshot. collectQueryEvents already returns a
	// freshly allocated slice of pointers, so no additional copy is needed.
	b.mu.RLock("StartQuery")
	allEvents, recordsScanned, bytesScanned := b.collectQueryEvents(
		region,
		logGroupNames,
		startTime,
		endTime,
	)
	b.mu.RUnlock()

	// Execute the query outside the lock — regex matching and sorting can be non-trivial.
	results := executeQuery(q, allEvents)

	stats := QueryStatistics{
		RecordsScanned: recordsScanned,
		RecordsMatched: float64(len(results)),
		BytesScanned:   bytesScanned,
	}

	logGroupName := ""
	if len(logGroupNames) > 0 {
		logGroupName = logGroupNames[0]
	}

	info := QueryInfo{
		QueryID:      queryID,
		QueryString:  queryString,
		Status:       QueryStatusComplete,
		CreateTime:   time.Now().UnixMilli(),
		LogGroupName: logGroupName,
	}

	sq := &storedQuery{
		info:      info,
		results:   results,
		stats:     stats,
		logGroups: logGroupNames,
		createdAt: time.Now(),
	}

	// Store results under a write lock.
	b.mu.Lock("StartQuery")
	defer b.mu.Unlock()

	// Evict expired queries before inserting so that the new entry is always retained.
	b.evictByTTL()

	// If this queryID already exists, remove its stale position in queriesOrder to
	// prevent duplicates that could cause map-miss panics or over-counting.
	if _, exists := b.queries[queryID]; exists {
		b.removeFromOrder(queryID)
	}

	b.queries[queryID] = sq
	b.queriesOrder = append(b.queriesOrder, queryID)

	// Enforce the cap after inserting so the new entry counts against the limit.
	b.enforceCap()

	cp := info

	return &cp, nil
}

// GetQueryResults returns the results of a previously started query.
func (b *InMemoryBackend) GetQueryResults(
	queryID string,
) ([][]ResultField, QueryStatistics, QueryStatus, error) {
	b.mu.RLock("GetQueryResults")
	defer b.mu.RUnlock()

	sq, ok := b.queries[queryID]
	if !ok {
		return nil, QueryStatistics{}, "", fmt.Errorf(
			"%w: query %s not found",
			ErrQueryNotFound,
			queryID,
		)
	}

	return sq.results, sq.stats, sq.info.Status, nil
}

// StopQuery cancels a query that is currently running or scheduled.
// AWS returns InvalidOperationException when stopping a query that is not in a running state.
func (b *InMemoryBackend) StopQuery(queryID string) error {
	b.mu.Lock("StopQuery")
	defer b.mu.Unlock()

	sq, ok := b.queries[queryID]
	if !ok {
		return fmt.Errorf("%w: query %s not found", ErrQueryNotFound, queryID)
	}

	if sq.info.Status != QueryStatusRunning && sq.info.Status != QueryStatusScheduled {
		return fmt.Errorf(
			"%w: query %s is not in a running state (status: %s)",
			ErrInvalidOperation, queryID, sq.info.Status,
		)
	}

	sq.info.Status = QueryStatusCancelled

	return nil
}

// DescribeQueries returns metadata about stored queries with optional filtering and pagination.
func (b *InMemoryBackend) DescribeQueries(
	logGroupName, statusFilter, nextToken string, maxResults int,
) ([]QueryInfo, string, error) {
	b.mu.RLock("DescribeQueries")
	defer b.mu.RUnlock()

	all := make([]QueryInfo, 0, len(b.queriesOrder))
	for _, qid := range b.queriesOrder {
		sq, ok := b.queries[qid]
		if !ok {
			continue
		}
		if logGroupName != "" {
			found := slices.Contains(sq.logGroups, logGroupName)
			if !found {
				continue
			}
		}
		if statusFilter != "" && string(sq.info.Status) != statusFilter {
			continue
		}
		all = append(all, sq.info)
	}

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(all) {
		return []QueryInfo{}, "", nil
	}

	if maxResults <= 0 {
		maxResults = defaultDescribeLimit
	}

	end := startIdx + maxResults
	var outToken string
	if end < len(all) {
		outToken = encodeNextToken(end)
	} else {
		end = len(all)
	}

	return all[startIdx:end], outToken, nil
}

// Reset clears all in-memory state from the backend. It is used by the
// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.groups = make(map[string]map[string]*LogGroup)
	b.streams = make(map[string]map[string]map[string]*LogStream)
	b.events = make(map[string]map[string]map[string][]*OutputLogEvent)
	b.subscriptionFilters = make(map[string]map[string][]*SubscriptionFilter)
	b.queries = make(map[string]*storedQuery)
	b.queriesOrder = nil
	b.parsedQueries = make(map[string]*insightsQuery)
	b.parsedQueriesOrder = nil
	b.exportTasks = make(map[string]*ExportTask)
	b.importTasks = make(map[string]*ImportTask)
	b.deliveries = make(map[string]*Delivery)
	b.logAnomalyDetectors = make(map[string]*LogAnomalyDetector)
	b.anomalies = make(map[string]map[string]*Anomaly)
	b.scheduledQueries = make(map[string]*ScheduledQuery)
	b.scheduledQueryRuns = make(map[string][]*ScheduledQueryRunSummary)
	b.accountPolicies = make(map[string]*AccountPolicy)
	b.kmsKeys = make(map[string]string)
	b.s3TableIntegrations = make(map[string]string)
	b.metricFilters = make(map[string]map[string]map[string]*MetricFilter)
	b.queryDefinitions = make(map[string]*QueryDefinition)
	b.dataProtectionPolicies = make(map[string]string)
	b.resourcePolicies = make(map[string]ResourcePolicy)
	b.deliveryDestinations = make(map[string]DeliveryDestination)
	b.deliverySources = make(map[string]DeliverySource)
	b.destinations = make(map[string]CWLDestination)
	b.indexPolicies = make(map[string]IndexPolicy)
	b.transformers = make(map[string]Transformer)
	b.integrations = make(map[string]CWLIntegration)
	b.deletionProtected = make(map[string]bool)

	b.compiledPatternsMu.Lock()
	b.compiledPatterns = make(map[string]*compiledFilterPattern)
	b.compiledPatternsMu.Unlock()
}

// PutDataProtectionPolicy stores a data protection policy for a log group.
// policyDocument is stored as-is and returned verbatim by GetDataProtectionPolicy.
func (b *InMemoryBackend) PutDataProtectionPolicy(logGroupIdentifier, policyDocument string) error {
	if logGroupIdentifier == "" {
		return fmt.Errorf("%w: logGroupIdentifier is required", ErrValidation)
	}

	b.mu.Lock("PutDataProtectionPolicy")
	defer b.mu.Unlock()

	b.dataProtectionPolicies[logGroupIdentifier] = policyDocument

	return nil
}

// GetDataProtectionPolicy returns the data protection policy for a log group.
// Returns an empty policy document if none has been set.
func (b *InMemoryBackend) GetDataProtectionPolicy(logGroupIdentifier string) (string, error) {
	b.mu.RLock("GetDataProtectionPolicy")
	defer b.mu.RUnlock()

	policy, ok := b.dataProtectionPolicies[logGroupIdentifier]
	if !ok {
		return "{}", nil
	}

	return policy, nil
}

// DeleteDataProtectionPolicy removes the data protection policy for a log group.
func (b *InMemoryBackend) DeleteDataProtectionPolicy(logGroupIdentifier string) error {
	b.mu.Lock("DeleteDataProtectionPolicy")
	defer b.mu.Unlock()

	delete(b.dataProtectionPolicies, logGroupIdentifier)

	return nil
}

// AssociateKmsKey associates a KMS key with a log group or query results resource.
// Exactly one of logGroupName or resourceIdentifier must be non-empty.
func (b *InMemoryBackend) AssociateKmsKey(logGroupName, resourceIdentifier, kmsKeyID string) error {
	if kmsKeyID == "" {
		return fmt.Errorf("%w: kmsKeyId is required", ErrValidation)
	}

	if logGroupName == "" && resourceIdentifier == "" {
		return fmt.Errorf(
			"%w: one of logGroupName or resourceIdentifier is required",
			ErrValidation,
		)
	}

	b.mu.Lock("AssociateKmsKey")
	defer b.mu.Unlock()

	key := logGroupName
	if key == "" {
		key = resourceIdentifier
	}

	b.kmsKeys[key] = kmsKeyID

	return nil
}

// AssociateSourceToS3TableIntegration associates a data source with an S3 table integration.
// Returns a unique identifier for the association.
func (b *InMemoryBackend) AssociateSourceToS3TableIntegration(
	integrationArn, _, _ string,
) (string, error) {
	if integrationArn == "" {
		return "", fmt.Errorf("%w: integrationArn is required", ErrValidation)
	}

	id := uuid.New().String()

	b.mu.Lock("AssociateSourceToS3TableIntegration")
	defer b.mu.Unlock()

	b.s3TableIntegrations[id] = integrationArn

	return id, nil
}

// CancelExportTask cancels a pending or running export task.
// Returns an error if the task is already in a terminal state.
func (b *InMemoryBackend) CancelExportTask(taskID string) error {
	if taskID == "" {
		return fmt.Errorf("%w: taskId is required", ErrValidation)
	}

	b.mu.Lock("CancelExportTask")
	defer b.mu.Unlock()

	task, ok := b.exportTasks[taskID]
	if !ok {
		return fmt.Errorf("%w: export task %s not found", ErrExportTaskNotFound, taskID)
	}

	// AWS only allows cancellation of tasks in PENDING or RUNNING state.
	if task.Status != exportStatusPending && task.Status != exportStatusRunning {
		return fmt.Errorf("%w: export task %s is in terminal state %s and cannot be cancelled",
			ErrValidation, taskID, task.Status)
	}

	task.Status = exportStatusCancelled

	return nil
}

// CancelImportTask cancels a running import task.
// Returns an error if the task is not in the ACTIVE state.
func (b *InMemoryBackend) CancelImportTask(importID string) (*ImportTask, error) {
	if importID == "" {
		return nil, fmt.Errorf("%w: importId is required", ErrValidation)
	}

	b.mu.Lock("CancelImportTask")
	defer b.mu.Unlock()

	task, ok := b.importTasks[importID]
	if !ok {
		return nil, fmt.Errorf("%w: import task %s not found", ErrImportTaskNotFound, importID)
	}

	// AWS only allows cancellation of ACTIVE tasks.
	if task.Status != completenessStatusActive {
		return nil, fmt.Errorf("%w: import task %s is in state %s and cannot be cancelled",
			ErrValidation, importID, task.Status)
	}

	task.Status = "CANCELLED"
	task.LastUpdatedTime = time.Now().UnixMilli()

	cp := *task

	return &cp, nil
}

// CreateDelivery creates a delivery between a delivery source and destination.
func (b *InMemoryBackend) CreateDelivery(
	deliverySourceName, deliveryDestinationArn string,
	tags map[string]string,
) (*Delivery, error) {
	if deliverySourceName == "" {
		return nil, fmt.Errorf("%w: deliverySourceName is required", ErrValidation)
	}

	if deliveryDestinationArn == "" {
		return nil, fmt.Errorf("%w: deliveryDestinationArn is required", ErrValidation)
	}

	id := uuid.New().String()
	deliveryArn := arn.Build("logs", b.region, b.accountID, "delivery:"+id)
	now := time.Now().UnixMilli()

	d := &Delivery{
		ID:                     id,
		Arn:                    deliveryArn,
		DeliverySourceName:     deliverySourceName,
		DeliveryDestinationArn: deliveryDestinationArn,
		Tags:                   maps.Clone(tags),
		CreationTime:           now,
	}

	b.mu.Lock("CreateDelivery")
	defer b.mu.Unlock()

	b.deliveries[id] = d

	cp := *d
	cp.Tags = maps.Clone(d.Tags)

	return &cp, nil
}

// CreateExportTask creates an export task to export log data to S3.
// Returns the task ID.
func (b *InMemoryBackend) CreateExportTask(
	taskName, logGroupName, _, destination, destinationPrefix string,
	from, to int64,
) (string, error) {
	if logGroupName == "" {
		return "", fmt.Errorf("%w: logGroupName is required", ErrValidation)
	}

	if destination == "" {
		return "", fmt.Errorf("%w: destination is required", ErrValidation)
	}

	if from >= to {
		return "", fmt.Errorf("%w: from (%d) must be less than to (%d)", ErrValidation, from, to)
	}

	taskID := uuid.New().String()

	task := &ExportTask{
		TaskID:            taskID,
		TaskName:          taskName,
		LogGroupName:      logGroupName,
		Destination:       destination,
		DestinationPrefix: destinationPrefix,
		From:              from,
		To:                to,
		Status:            exportStatusPending,
		CreationTime:      time.Now().UnixMilli(),
	}

	b.mu.Lock("CreateExportTask")
	defer b.mu.Unlock()

	if len(b.exportTasks) >= maxExportTasks {
		return "", fmt.Errorf("%w: export task limit exceeded", ErrValidation)
	}

	b.exportTasks[taskID] = task

	return taskID, nil
}

// CreateImportTask creates an import task from a CloudTrail Lake event data store.
func (b *InMemoryBackend) CreateImportTask(
	importRoleArn, importSourceArn string,
) (*ImportTask, error) {
	if importRoleArn == "" {
		return nil, fmt.Errorf("%w: importRoleArn is required", ErrValidation)
	}

	if importSourceArn == "" {
		return nil, fmt.Errorf("%w: importSourceArn is required", ErrValidation)
	}

	importID := uuid.New().String()
	now := time.Now().UnixMilli()
	destARN := arn.Build("logs", b.region, b.accountID, "log-group:/aws/cloudtrail/"+importID)

	task := &ImportTask{
		ImportID:             importID,
		ImportSourceArn:      importSourceArn,
		ImportRoleArn:        importRoleArn,
		ImportDestinationArn: destARN,
		Status:               completenessStatusActive,
		CreationTime:         now,
		LastUpdatedTime:      now,
	}

	b.mu.Lock("CreateImportTask")
	defer b.mu.Unlock()

	if len(b.importTasks) >= maxImportTasks {
		return nil, fmt.Errorf("%w: import task limit exceeded", ErrValidation)
	}

	b.importTasks[importID] = task

	cp := *task

	return &cp, nil
}

// CreateLogAnomalyDetector creates an anomaly detector for one or more log groups.
// Returns the ARN of the created detector.
func (b *InMemoryBackend) CreateLogAnomalyDetector(
	logGroupArnList []string,
	detectorName, evaluationFrequency, filterPattern, kmsKeyID string,
	anomalyVisibilityTime int64,
) (string, error) {
	if len(logGroupArnList) == 0 {
		return "", fmt.Errorf("%w: logGroupArnList must not be empty", ErrValidation)
	}

	if evaluationFrequency != "" {
		if _, ok := validEvaluationFrequencies()[evaluationFrequency]; !ok {
			return "", fmt.Errorf(
				"%w: invalid evaluationFrequency %q",
				ErrValidation,
				evaluationFrequency,
			)
		}
	}

	if anomalyVisibilityTime != 0 {
		const msPerDay = 24 * 60 * 60 * 1000
		visibilityDays := anomalyVisibilityTime / msPerDay
		if visibilityDays < anomalyVisibilityTimeMinDays ||
			visibilityDays > anomalyVisibilityTimeMaxDays {
			return "", fmt.Errorf(
				"%w: anomalyVisibilityTime must be between %d and %d days",
				ErrValidation, anomalyVisibilityTimeMinDays, anomalyVisibilityTimeMaxDays,
			)
		}
	}

	id := uuid.New().String()
	detectorARN := arn.Build("logs", b.region, b.accountID, "log-anomaly-detector:"+id)
	now := time.Now().UnixMilli()

	detector := &LogAnomalyDetector{
		AnomalyDetectorArn:    detectorARN,
		DetectorName:          detectorName,
		DetectorStatus:        detectorStatusInitializing,
		LogGroupArnList:       slices.Clone(logGroupArnList),
		EvaluationFrequency:   evaluationFrequency,
		FilterPattern:         filterPattern,
		KmsKeyID:              kmsKeyID,
		AnomalyVisibilityTime: anomalyVisibilityTime,
		CreationTimeStamp:     now,
		LastModifiedTimeStamp: now,
	}

	b.mu.Lock("CreateLogAnomalyDetector")
	defer b.mu.Unlock()

	if len(b.logAnomalyDetectors) >= maxAnomalyDetectors {
		return "", fmt.Errorf("%w: anomaly detector limit exceeded", ErrValidation)
	}

	b.logAnomalyDetectors[detectorARN] = detector

	return detectorARN, nil
}

// CreateScheduledQuery creates a scheduled CloudWatch Logs Insights query.
// Returns the ARN of the created scheduled query.
func (b *InMemoryBackend) CreateScheduledQuery(
	name, queryString, scheduleExpression, _, state string,
) (string, error) {
	if name == "" {
		return "", fmt.Errorf("%w: name is required", ErrValidation)
	}

	if queryString == "" {
		return "", fmt.Errorf("%w: queryString is required", ErrValidation)
	}

	if state != "" {
		if _, ok := validScheduledQueryStates()[state]; !ok {
			return "", fmt.Errorf(
				"%w: invalid state %q, must be ENABLED or DISABLED",
				ErrValidation,
				state,
			)
		}
	} else {
		state = statusEnabled
	}

	id := uuid.New().String()
	queryARN := arn.Build("logs", b.region, b.accountID, "scheduled-query:"+id)

	sq := &ScheduledQuery{
		Arn:                queryARN,
		Name:               name,
		QueryString:        queryString,
		ScheduleExpression: scheduleExpression,
		State:              state,
		CreationTime:       time.Now().UnixMilli(),
	}

	b.mu.Lock("CreateScheduledQuery")
	defer b.mu.Unlock()

	if len(b.scheduledQueries) >= maxScheduledQueries {
		return "", fmt.Errorf("%w: scheduled query limit exceeded", ErrValidation)
	}

	b.scheduledQueries[queryARN] = sq

	// Seed an initial SUCCEEDED run so history is non-empty from creation.
	now := time.Now().UnixMilli()
	b.scheduledQueryRuns[queryARN] = []*ScheduledQueryRunSummary{
		{
			Arn:            queryARN,
			RunStatus:      "SUCCEEDED",
			ExecutionTime:  now,
			InvocationTime: now,
		},
	}

	return queryARN, nil
}

// DeleteAccountPolicy deletes a CloudWatch Logs account-level policy.
func (b *InMemoryBackend) DeleteAccountPolicy(policyName, policyType string) error {
	if policyName == "" {
		return fmt.Errorf("%w: policyName is required", ErrValidation)
	}

	if policyType == "" {
		return fmt.Errorf("%w: policyType is required", ErrValidation)
	}

	if _, ok := validAccountPolicyTypes()[policyType]; !ok {
		return fmt.Errorf("%w: invalid policyType %q", ErrValidation, policyType)
	}

	b.mu.Lock("DeleteAccountPolicy")
	defer b.mu.Unlock()

	key := policyName + ":" + policyType
	delete(b.accountPolicies, key)

	return nil
}

// DescribeExportTasks lists export tasks optionally filtered by task ID or status.
// It also lazily advances task state from PENDING→RUNNING→COMPLETED based on elapsed time.
func (b *InMemoryBackend) DescribeExportTasks(
	taskID, statusCode string,
	limit int,
	nextToken string,
) ([]ExportTask, string, error) {
	b.mu.Lock("DescribeExportTasks")
	defer b.mu.Unlock()

	now := time.Now().UnixMilli()
	for _, t := range b.exportTasks {
		age := now - t.CreationTime
		if age > maxExportTaskAgeMs {
			continue
		}
		if t.Status == exportStatusPending && age > exportTaskAgeRunningMs {
			t.Status = exportStatusRunning
		}
		if t.Status == exportStatusRunning && age > exportTaskAgeCompletedMs {
			t.Status = exportStatusCompleted
		}
	}

	all := make([]ExportTask, 0, len(b.exportTasks))
	for _, t := range b.exportTasks {
		if taskID != "" && t.TaskID != taskID {
			continue
		}
		if statusCode != "" && t.Status != statusCode {
			continue
		}
		all = append(all, *t)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].CreationTime < all[j].CreationTime })

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(all) {
		return []ExportTask{}, "", nil
	}
	if limit <= 0 {
		limit = defaultDescribeLimit
	}
	end := startIdx + limit
	var outToken string
	if end < len(all) {
		outToken = encodeNextToken(end)
	} else {
		end = len(all)
	}

	return all[startIdx:end], outToken, nil
}

// DescribeImportTasks lists import tasks optionally filtered by task ID.
func (b *InMemoryBackend) DescribeImportTasks(
	taskID string,
	limit int,
	nextToken string,
) ([]ImportTask, string, error) {
	b.mu.RLock("DescribeImportTasks")
	defer b.mu.RUnlock()

	all := make([]ImportTask, 0, len(b.importTasks))
	for _, t := range b.importTasks {
		if taskID != "" && t.ImportID != taskID {
			continue
		}
		all = append(all, *t)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].CreationTime < all[j].CreationTime })

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(all) {
		return []ImportTask{}, "", nil
	}
	if limit <= 0 {
		limit = defaultDescribeLimit
	}
	end := startIdx + limit
	var outToken string
	if end < len(all) {
		outToken = encodeNextToken(end)
	} else {
		end = len(all)
	}

	return all[startIdx:end], outToken, nil
}

// DescribeDeliveries lists deliveries with pagination.
func (b *InMemoryBackend) DescribeDeliveries(
	limit int,
	nextToken string,
) ([]Delivery, string, error) {
	b.mu.RLock("DescribeDeliveries")
	defer b.mu.RUnlock()

	all := make([]Delivery, 0, len(b.deliveries))
	for _, d := range b.deliveries {
		cp := *d
		cp.Tags = maps.Clone(d.Tags)
		all = append(all, cp)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].CreationTime < all[j].CreationTime })

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(all) {
		return []Delivery{}, "", nil
	}
	if limit <= 0 {
		limit = defaultDescribeLimit
	}
	end := startIdx + limit
	var outToken string
	if end < len(all) {
		outToken = encodeNextToken(end)
	} else {
		end = len(all)
	}

	return all[startIdx:end], outToken, nil
}

// GetDelivery returns a single delivery by ID.
func (b *InMemoryBackend) GetDelivery(id string) (*Delivery, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: id is required", ErrValidation)
	}

	b.mu.RLock("GetDelivery")
	defer b.mu.RUnlock()

	d, ok := b.deliveries[id]
	if !ok {
		return nil, fmt.Errorf("%w: delivery %s not found", ErrDeliveryNotFound, id)
	}
	cp := *d
	cp.Tags = maps.Clone(d.Tags)

	return &cp, nil
}

// DeleteDelivery deletes a delivery by ID.
func (b *InMemoryBackend) DeleteDelivery(id string) error {
	if id == "" {
		return fmt.Errorf("%w: id is required", ErrValidation)
	}

	b.mu.Lock("DeleteDelivery")
	defer b.mu.Unlock()

	if _, ok := b.deliveries[id]; !ok {
		return fmt.Errorf("%w: delivery %s not found", ErrDeliveryNotFound, id)
	}
	delete(b.deliveries, id)

	return nil
}

// DeleteLogAnomalyDetector deletes a log anomaly detector.
func (b *InMemoryBackend) DeleteLogAnomalyDetector(detectorArn string) error {
	if detectorArn == "" {
		return fmt.Errorf("%w: anomalyDetectorArn is required", ErrValidation)
	}

	b.mu.Lock("DeleteLogAnomalyDetector")
	defer b.mu.Unlock()

	if _, ok := b.logAnomalyDetectors[detectorArn]; !ok {
		return fmt.Errorf(
			"%w: anomaly detector %s not found",
			ErrLogAnomalyDetectorNotFound,
			detectorArn,
		)
	}
	delete(b.logAnomalyDetectors, detectorArn)

	return nil
}

// ListLogAnomalyDetectors lists anomaly detectors, optionally filtered by log group ARN.
func (b *InMemoryBackend) ListLogAnomalyDetectors(
	filterLogGroupArnList []string,
	limit int,
	nextToken string,
) ([]LogAnomalyDetector, string, error) {
	b.mu.RLock("ListLogAnomalyDetectors")
	defer b.mu.RUnlock()

	filterSet := make(map[string]bool, len(filterLogGroupArnList))
	for _, a := range filterLogGroupArnList {
		filterSet[a] = true
	}

	all := make([]LogAnomalyDetector, 0, len(b.logAnomalyDetectors))
	for _, d := range b.logAnomalyDetectors {
		if len(filterSet) > 0 {
			match := false
			for _, a := range d.LogGroupArnList {
				if filterSet[a] {
					match = true

					break
				}
			}
			if !match {
				continue
			}
		}
		cp := *d
		cp.LogGroupArnList = slices.Clone(d.LogGroupArnList)
		all = append(all, cp)
	}
	sort.Slice(
		all,
		func(i, j int) bool { return all[i].CreationTimeStamp < all[j].CreationTimeStamp },
	)

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(all) {
		return []LogAnomalyDetector{}, "", nil
	}
	if limit <= 0 {
		limit = defaultDescribeLimit
	}
	end := startIdx + limit
	var outToken string
	if end < len(all) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(all)
	}

	return all[startIdx:end], outToken, nil
}

// UpdateLogAnomalyDetector updates evaluation frequency and/or anomaly visibility time.
func (b *InMemoryBackend) UpdateLogAnomalyDetector(
	detectorArn, evaluationFrequency string,
	anomalyVisibilityTime int64,
) error {
	if detectorArn == "" {
		return fmt.Errorf("%w: anomalyDetectorArn is required", ErrValidation)
	}
	if evaluationFrequency != "" {
		if _, ok := validEvaluationFrequencies()[evaluationFrequency]; !ok {
			return fmt.Errorf(
				"%w: invalid evaluationFrequency %q",
				ErrValidation,
				evaluationFrequency,
			)
		}
	}

	b.mu.Lock("UpdateLogAnomalyDetector")
	defer b.mu.Unlock()

	d, ok := b.logAnomalyDetectors[detectorArn]
	if !ok {
		return fmt.Errorf(
			"%w: anomaly detector %s not found",
			ErrLogAnomalyDetectorNotFound,
			detectorArn,
		)
	}
	if evaluationFrequency != "" {
		d.EvaluationFrequency = evaluationFrequency
	}
	if anomalyVisibilityTime > 0 {
		if anomalyVisibilityTime != 0 {
			const msPerDay = 24 * 60 * 60 * 1000
			visibilityDays := anomalyVisibilityTime / msPerDay
			if visibilityDays < anomalyVisibilityTimeMinDays ||
				visibilityDays > anomalyVisibilityTimeMaxDays {
				return fmt.Errorf(
					"%w: anomalyVisibilityTime must be between %d and %d days",
					ErrValidation, anomalyVisibilityTimeMinDays, anomalyVisibilityTimeMaxDays,
				)
			}
		}
		d.AnomalyVisibilityTime = anomalyVisibilityTime
	}
	d.LastModifiedTimeStamp = time.Now().UnixMilli()

	return nil
}

// DeleteScheduledQuery deletes a scheduled query by ARN.
func (b *InMemoryBackend) DeleteScheduledQuery(scheduledQueryArn string) error {
	if scheduledQueryArn == "" {
		return fmt.Errorf("%w: scheduledQueryArn is required", ErrValidation)
	}

	b.mu.Lock("DeleteScheduledQuery")
	defer b.mu.Unlock()

	if _, ok := b.scheduledQueries[scheduledQueryArn]; !ok {
		return fmt.Errorf(
			"%w: scheduled query %s not found",
			ErrScheduledQueryNotFound,
			scheduledQueryArn,
		)
	}
	delete(b.scheduledQueries, scheduledQueryArn)

	return nil
}

// ListScheduledQueries lists all scheduled queries with pagination.
func (b *InMemoryBackend) ListScheduledQueries(
	limit int,
	nextToken string,
) ([]ScheduledQuery, string, error) {
	b.mu.RLock("ListScheduledQueries")
	defer b.mu.RUnlock()

	all := make([]ScheduledQuery, 0, len(b.scheduledQueries))
	for _, sq := range b.scheduledQueries {
		all = append(all, *sq)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].CreationTime < all[j].CreationTime })

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(all) {
		return []ScheduledQuery{}, "", nil
	}
	if limit <= 0 {
		limit = defaultDescribeLimit
	}
	end := startIdx + limit
	var outToken string
	if end < len(all) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(all)
	}

	return all[startIdx:end], outToken, nil
}

// UpdateScheduledQuery updates the state of a scheduled query.
func (b *InMemoryBackend) UpdateScheduledQuery(scheduledQueryArn, state string) error {
	if scheduledQueryArn == "" {
		return fmt.Errorf("%w: scheduledQueryArn is required", ErrValidation)
	}
	if state == "" {
		return fmt.Errorf("%w: state is required", ErrValidation)
	}
	if _, ok := validScheduledQueryStates()[state]; !ok {
		return fmt.Errorf("%w: invalid state %q, must be ENABLED or DISABLED", ErrValidation, state)
	}

	b.mu.Lock("UpdateScheduledQuery")
	defer b.mu.Unlock()

	sq, ok := b.scheduledQueries[scheduledQueryArn]
	if !ok {
		return fmt.Errorf(
			"%w: scheduled query %s not found",
			ErrScheduledQueryNotFound,
			scheduledQueryArn,
		)
	}
	sq.State = state

	return nil
}

// PutAccountPolicy creates or updates an account-level policy.
// scope must be ALL or SELECTION_CRITERIA (defaults to ALL if empty).
func (b *InMemoryBackend) PutAccountPolicy(
	policyName, policyType, policyDocument, scope, selectionCriteria string,
) (*AccountPolicy, error) {
	if policyName == "" {
		return nil, fmt.Errorf("%w: policyName is required", ErrValidation)
	}
	if policyType == "" {
		return nil, fmt.Errorf("%w: policyType is required", ErrValidation)
	}
	if _, ok := validAccountPolicyTypes()[policyType]; !ok {
		return nil, fmt.Errorf("%w: invalid policyType %q", ErrValidation, policyType)
	}
	if scope == "" {
		scope = "ALL"
	}
	if _, ok := validAccountPolicyScopes()[scope]; !ok {
		return nil, fmt.Errorf(
			"%w: invalid scope %q, must be ALL or SELECTION_CRITERIA",
			ErrValidation,
			scope,
		)
	}
	if scope == "SELECTION_CRITERIA" && selectionCriteria == "" {
		return nil, fmt.Errorf(
			"%w: selectionCriteria is required when scope is SELECTION_CRITERIA",
			ErrValidation,
		)
	}

	b.mu.Lock("PutAccountPolicy")
	defer b.mu.Unlock()

	key := policyName + ":" + policyType
	p := &AccountPolicy{
		PolicyName:        policyName,
		PolicyType:        policyType,
		PolicyDocument:    policyDocument,
		Scope:             scope,
		SelectionCriteria: selectionCriteria,
	}
	b.accountPolicies[key] = p
	cp := *p

	return &cp, nil
}

// DescribeAccountPolicies returns account-level policies, optionally filtered, with pagination.
// accountIdentifiers filters by account IDs embedded in the policy name (prefix match).
func (b *InMemoryBackend) DescribeAccountPolicies(
	policyType, policyName string,
	_ []string,
	limit int,
	nextToken string,
) ([]AccountPolicy, string, error) {
	if policyType != "" {
		if _, ok := validAccountPolicyTypes()[policyType]; !ok {
			return nil, "", fmt.Errorf("%w: invalid policyType %q", ErrValidation, policyType)
		}
	}

	b.mu.RLock("DescribeAccountPolicies")
	defer b.mu.RUnlock()

	all := make([]AccountPolicy, 0, len(b.accountPolicies))
	for _, p := range b.accountPolicies {
		if policyType != "" && p.PolicyType != policyType {
			continue
		}
		if policyName != "" && p.PolicyName != policyName {
			continue
		}
		all = append(all, *p)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].PolicyName < all[j].PolicyName })

	// Apply pagination.
	startIdx := parseNextToken(nextToken)
	if startIdx >= len(all) {
		return []AccountPolicy{}, "", nil
	}
	if limit <= 0 {
		limit = defaultDescribeLimit
	}
	end := startIdx + limit
	var outToken string
	if end < len(all) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(all)
	}

	return all[startIdx:end], outToken, nil
}

// DisassociateKmsKey removes the KMS key association from a log group or resource.
func (b *InMemoryBackend) DisassociateKmsKey(logGroupName, resourceIdentifier string) error {
	if logGroupName == "" && resourceIdentifier == "" {
		return fmt.Errorf(
			"%w: one of logGroupName or resourceIdentifier is required",
			ErrValidation,
		)
	}

	b.mu.Lock("DisassociateKmsKey")
	defer b.mu.Unlock()

	key := logGroupName
	if key == "" {
		key = resourceIdentifier
	}
	delete(b.kmsKeys, key)

	return nil
}

// PutMetricFilter creates or updates a metric filter for a log group.
func (b *InMemoryBackend) PutMetricFilter(
	ctx context.Context,
	logGroupName, filterName, filterPattern string,
	transformations []MetricTransformation,
) error {
	if logGroupName == "" {
		return fmt.Errorf("%w: logGroupName is required", ErrValidation)
	}
	if filterName == "" {
		return fmt.Errorf("%w: filterName is required", ErrValidation)
	}
	if len(transformations) == 0 {
		return fmt.Errorf("%w: at least one metricTransformation is required", ErrValidation)
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("PutMetricFilter")
	defer b.mu.Unlock()

	groups := b.groupsStore(region)
	if _, exists := groups[logGroupName]; !exists {
		return fmt.Errorf("%w: Log group %s not found", ErrLogGroupNotFound, logGroupName)
	}

	metricFilters := b.metricFiltersStore(region)
	if metricFilters[logGroupName] == nil {
		metricFilters[logGroupName] = make(map[string]*MetricFilter)
	}

	creationTime := time.Now().UnixMilli()
	if existing, ok := metricFilters[logGroupName][filterName]; ok {
		creationTime = existing.CreationTime
	}

	mf := &MetricFilter{
		FilterName:            filterName,
		LogGroupName:          logGroupName,
		FilterPattern:         filterPattern,
		MetricTransformations: append([]MetricTransformation(nil), transformations...),
		CreationTime:          creationTime,
	}
	metricFilters[logGroupName][filterName] = mf
	count := len(metricFilters[logGroupName])
	groups[logGroupName].MetricFilterCount = int32(
		count,
	) // #nosec G115 -- count bounded by AWS API limit

	return nil
}

// DescribeMetricFilters lists metric filters with optional filters.
func (b *InMemoryBackend) DescribeMetricFilters(
	ctx context.Context,
	logGroupName, filterNamePrefix, metricName, metricNamespace, nextToken string,
	limit int,
) ([]MetricFilter, string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeMetricFilters")
	defer b.mu.RUnlock()

	var all []MetricFilter
	for grp, filters := range b.metricFiltersStore(region) {
		if logGroupName != "" && grp != logGroupName {
			continue
		}
		for _, mf := range filters {
			if !metricFilterMatches(mf, filterNamePrefix, metricName, metricNamespace) {
				continue
			}
			cp := *mf
			cp.MetricTransformations = append(
				[]MetricTransformation(nil),
				mf.MetricTransformations...)
			all = append(all, cp)
		}
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].LogGroupName != all[j].LogGroupName {
			return all[i].LogGroupName < all[j].LogGroupName
		}

		return all[i].FilterName < all[j].FilterName
	})

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(all) {
		return []MetricFilter{}, "", nil
	}
	if limit <= 0 {
		limit = defaultDescribeLimit
	}
	end := startIdx + limit
	var outToken string
	if end < len(all) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(all)
	}

	return all[startIdx:end], outToken, nil
}

// metricFilterMatches returns true if mf passes the given filter criteria.
func metricFilterMatches(
	mf *MetricFilter,
	filterNamePrefix, metricName, metricNamespace string,
) bool {
	if filterNamePrefix != "" && !strings.HasPrefix(mf.FilterName, filterNamePrefix) {
		return false
	}
	if metricName == "" && metricNamespace == "" {
		return true
	}
	for _, t := range mf.MetricTransformations {
		if (metricName == "" || t.MetricName == metricName) &&
			(metricNamespace == "" || t.MetricNamespace == metricNamespace) {
			return true
		}
	}

	return false
}

// DeleteMetricFilter deletes a metric filter from a log group.
func (b *InMemoryBackend) DeleteMetricFilter(
	ctx context.Context,
	logGroupName, filterName string,
) error {
	if logGroupName == "" {
		return fmt.Errorf("%w: logGroupName is required", ErrValidation)
	}
	if filterName == "" {
		return fmt.Errorf("%w: filterName is required", ErrValidation)
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteMetricFilter")
	defer b.mu.Unlock()

	groups := b.groupsStore(region)
	if _, exists := groups[logGroupName]; !exists {
		return fmt.Errorf("%w: Log group %s not found", ErrLogGroupNotFound, logGroupName)
	}

	metricFilters := b.metricFiltersStore(region)
	filters := metricFilters[logGroupName]
	if _, ok := filters[filterName]; !ok {
		return fmt.Errorf(
			"%w: metric filter %s not found in log group %s",
			ErrMetricFilterNotFound,
			filterName,
			logGroupName,
		)
	}
	delete(filters, filterName)
	if len(filters) == 0 {
		delete(metricFilters, logGroupName)
	}
	count := len(metricFilters[logGroupName])
	groups[logGroupName].MetricFilterCount = int32(
		count,
	) // #nosec G115 -- count bounded by AWS API limit

	return nil
}

// TestMetricFilter tests a metric filter pattern against provided log event messages.
func (b *InMemoryBackend) TestMetricFilter(
	filterPattern string,
	logEventMessages []string,
) ([]MetricFilterMatchRecord, error) {
	if filterPattern == "" {
		return nil, fmt.Errorf("%w: filterPattern is required", ErrValidation)
	}

	compiled := compileFilterPattern(filterPattern)
	matches := make([]MetricFilterMatchRecord, 0, len(logEventMessages))
	for i, msg := range logEventMessages {
		if compiled.matches(msg) {
			matches = append(matches, MetricFilterMatchRecord{
				EventMessage:    msg,
				EventNumber:     int64(i + 1),
				ExtractedValues: map[string]string{},
			})
		}
	}

	return matches, nil
}

// PutQueryDefinition creates or updates a query definition.
func (b *InMemoryBackend) PutQueryDefinition(
	name, queryString, queryDefinitionID string,
	logGroupNames []string,
) (string, error) {
	if name == "" {
		return "", fmt.Errorf("%w: name is required", ErrValidation)
	}
	if queryString == "" {
		return "", fmt.Errorf("%w: queryString is required", ErrValidation)
	}

	b.mu.Lock("PutQueryDefinition")
	defer b.mu.Unlock()

	id := queryDefinitionID
	if id == "" {
		// New entry: enforce the cap.
		if len(b.queryDefinitions) >= maxQueryDefinitions {
			return "", fmt.Errorf("%w: query definition limit exceeded", ErrValidation)
		}
		id = uuid.New().String()
	} else {
		// Update path: the supplied ID must reference an existing definition.
		if _, exists := b.queryDefinitions[id]; !exists {
			return "", fmt.Errorf(
				"%w: query definition %s not found",
				ErrQueryDefinitionNotFound, id,
			)
		}
	}
	qd := &QueryDefinition{
		QueryDefinitionID: id,
		Name:              name,
		QueryString:       queryString,
		LogGroupNames:     slices.Clone(logGroupNames),
		LastModified:      time.Now().UnixMilli(),
	}
	b.queryDefinitions[id] = qd

	return id, nil
}

// DescribeQueryDefinitions lists query definitions optionally filtered by name prefix.
func (b *InMemoryBackend) DescribeQueryDefinitions(
	queryDefinitionNamePrefix string,
	limit int,
	nextToken string,
) ([]QueryDefinition, string, error) {
	b.mu.RLock("DescribeQueryDefinitions")
	defer b.mu.RUnlock()

	all := make([]QueryDefinition, 0, len(b.queryDefinitions))
	for _, qd := range b.queryDefinitions {
		if queryDefinitionNamePrefix != "" &&
			!strings.HasPrefix(qd.Name, queryDefinitionNamePrefix) {
			continue
		}
		cp := *qd
		cp.LogGroupNames = slices.Clone(qd.LogGroupNames)
		all = append(all, cp)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(all) {
		return []QueryDefinition{}, "", nil
	}
	if limit <= 0 {
		limit = defaultDescribeLimit
	}
	end := startIdx + limit
	var outToken string
	if end < len(all) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(all)
	}

	return all[startIdx:end], outToken, nil
}

// DeleteQueryDefinition deletes a query definition by ID.
func (b *InMemoryBackend) DeleteQueryDefinition(queryDefinitionID string) error {
	if queryDefinitionID == "" {
		return fmt.Errorf("%w: queryDefinitionId is required", ErrValidation)
	}

	b.mu.Lock("DeleteQueryDefinition")
	defer b.mu.Unlock()

	if _, ok := b.queryDefinitions[queryDefinitionID]; !ok {
		return fmt.Errorf(
			"%w: query definition %s not found",
			ErrQueryDefinitionNotFound,
			queryDefinitionID,
		)
	}
	delete(b.queryDefinitions, queryDefinitionID)

	return nil
}

// AddExportTaskInternal seeds an ExportTask directly into the store for testing.
// It overwrites any existing task with the same ID.
func (b *InMemoryBackend) AddExportTaskInternal(task ExportTask) {
	b.mu.Lock("AddExportTaskInternal")
	defer b.mu.Unlock()

	t := task
	b.exportTasks[task.TaskID] = &t
}

// AddImportTaskInternal seeds an ImportTask directly into the store for testing.
// It overwrites any existing task with the same ID.
func (b *InMemoryBackend) AddImportTaskInternal(task ImportTask) {
	b.mu.Lock("AddImportTaskInternal")
	defer b.mu.Unlock()

	t := task
	b.importTasks[task.ImportID] = &t
}

// AddDeliveryInternal seeds a Delivery directly into the store for testing.
// It overwrites any existing delivery with the same ID.
func (b *InMemoryBackend) AddDeliveryInternal(delivery Delivery) {
	b.mu.Lock("AddDeliveryInternal")
	defer b.mu.Unlock()

	d := delivery
	d.Tags = maps.Clone(delivery.Tags)
	b.deliveries[delivery.ID] = &d
}

// AddLogAnomalyDetectorInternal seeds a LogAnomalyDetector directly into the store for testing.
// It overwrites any existing detector with the same ARN.
func (b *InMemoryBackend) AddLogAnomalyDetectorInternal(detector LogAnomalyDetector) {
	b.mu.Lock("AddLogAnomalyDetectorInternal")
	defer b.mu.Unlock()

	d := detector
	d.LogGroupArnList = slices.Clone(detector.LogGroupArnList)
	b.logAnomalyDetectors[detector.AnomalyDetectorArn] = &d
}

// AddAnomalyInternal seeds an Anomaly directly into the store for testing.
// The anomaly is stored under its AnomalyDetectorArn.
func (b *InMemoryBackend) AddAnomalyInternal(anomaly Anomaly) {
	b.mu.Lock("AddAnomalyInternal")
	defer b.mu.Unlock()

	if b.anomalies[anomaly.AnomalyDetectorArn] == nil {
		b.anomalies[anomaly.AnomalyDetectorArn] = make(map[string]*Anomaly)
	}

	a := anomaly
	b.anomalies[anomaly.AnomalyDetectorArn][anomaly.AnomalyID] = &a
}

// AddScheduledQueryRunInternal seeds a ScheduledQueryRunSummary for testing.
func (b *InMemoryBackend) AddScheduledQueryRunInternal(
	scheduledQueryArn string,
	run ScheduledQueryRunSummary,
) {
	b.mu.Lock("AddScheduledQueryRunInternal")
	defer b.mu.Unlock()

	r := run
	b.scheduledQueryRuns[scheduledQueryArn] = append(b.scheduledQueryRuns[scheduledQueryArn], &r)
}

// SetQueryStatusInternal sets the status of an existing query for testing.
// Used to place a query into Running or Scheduled state before calling StopQuery.
func (b *InMemoryBackend) SetQueryStatusInternal(queryID string, status QueryStatus) {
	b.mu.Lock("SetQueryStatusInternal")
	defer b.mu.Unlock()

	if sq, ok := b.queries[queryID]; ok {
		sq.info.Status = status
	}
}

// validLogGroupName returns true if name matches the AWS CloudWatch Logs allowed character set.
// Pattern: [.\\-_/#A-Za-z0-9]+, length 1-512.
func validLogGroupName(name string) bool {
	if len(name) == 0 || len(name) > 512 {
		return false
	}
	for _, r := range name {
		if !isValidLogGroupChar(r) {
			return false
		}
	}

	return true
}

func isValidLogGroupChar(r rune) bool {
	return (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') ||
		r == '.' || r == '_' || r == '-' || r == '/' || r == '#'
}

// validRetentionDays returns the full set of retention values accepted by AWS CloudWatch Logs.
func validRetentionDays() map[int32]struct{} {
	return map[int32]struct{}{
		1: {}, 3: {}, 5: {}, 7: {}, 14: {}, 30: {}, 60: {}, 90: {}, 120: {}, 150: {}, 180: {},
		365: {}, 400: {}, 545: {}, 731: {}, 1096: {}, 1827: {}, 2192: {}, 2557: {}, 2922: {}, 3288: {}, 3653: {},
	}
}

// GetLogAnomalyDetector returns the anomaly detector with the given ARN.
func (b *InMemoryBackend) GetLogAnomalyDetector(detectorArn string) (*LogAnomalyDetector, error) {
	if detectorArn == "" {
		return nil, fmt.Errorf("%w: anomalyDetectorArn is required", ErrValidation)
	}

	b.mu.RLock("GetLogAnomalyDetector")
	defer b.mu.RUnlock()

	d, ok := b.logAnomalyDetectors[detectorArn]
	if !ok {
		return nil, fmt.Errorf(
			"%w: anomaly detector %s not found",
			ErrLogAnomalyDetectorNotFound,
			detectorArn,
		)
	}
	cp := *d
	cp.LogGroupArnList = slices.Clone(d.LogGroupArnList)

	return &cp, nil
}

// GetScheduledQuery returns the scheduled query with the given ARN.
func (b *InMemoryBackend) GetScheduledQuery(scheduledQueryArn string) (*ScheduledQuery, error) {
	if scheduledQueryArn == "" {
		return nil, fmt.Errorf("%w: scheduledQueryArn is required", ErrValidation)
	}

	b.mu.RLock("GetScheduledQuery")
	defer b.mu.RUnlock()

	sq, ok := b.scheduledQueries[scheduledQueryArn]
	if !ok {
		return nil, fmt.Errorf(
			"%w: scheduled query %s not found",
			ErrScheduledQueryNotFound,
			scheduledQueryArn,
		)
	}
	cp := *sq

	return &cp, nil
}

// standardLogGroupFields returns the standard AWS CloudWatch Logs fields present in all log events.
// All standard fields are present in 100% of events.
func standardLogGroupFields() []LogGroupField {
	const pct int32 = 100

	return []LogGroupField{
		{Name: keyMessageField, Percent: pct},
		{Name: keyTimestamp, Percent: pct},
		{Name: keyIngestionTime, Percent: pct},
		{Name: "@logStream", Percent: pct},
	}
}

func (b *InMemoryBackend) GetLogGroupFields(
	ctx context.Context,
	logGroupName string,
) ([]LogGroupField, error) {
	if logGroupName == "" {
		return nil, fmt.Errorf("%w: logGroupName is required", ErrValidation)
	}

	region := getRegion(ctx, b.region)

	b.mu.RLock("GetLogGroupFields")
	defer b.mu.RUnlock()

	if _, exists := b.groupsStore(region)[logGroupName]; !exists {
		return nil, fmt.Errorf("%w: Log group %s not found", ErrLogGroupNotFound, logGroupName)
	}

	return standardLogGroupFields(), nil
}

// GetLogRecord returns a single log event by its log record pointer.
// The pointer is the base64-encoded "<groupName>/<streamName>/<index>" string.
func (b *InMemoryBackend) GetLogRecord(
	ctx context.Context,
	logRecordPointer string,
) (map[string]string, error) {
	if logRecordPointer == "" {
		return nil, fmt.Errorf("%w: logRecordPointer is required", ErrValidation)
	}

	raw, err := base64.StdEncoding.DecodeString(logRecordPointer)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid logRecordPointer: %w", ErrValidation, err)
	}

	const pointerParts = 3
	parts := strings.SplitN(string(raw), "/", pointerParts)
	if len(parts) < pointerParts {
		return nil, fmt.Errorf("%w: invalid logRecordPointer format", ErrValidation)
	}

	groupName := parts[0]
	streamName := parts[1]
	idx, parseErr := strconv.Atoi(parts[2])
	if parseErr != nil || idx < 0 {
		return nil, fmt.Errorf("%w: invalid logRecordPointer index", ErrValidation)
	}

	region := getRegion(ctx, b.region)

	b.mu.RLock("GetLogRecord")
	defer b.mu.RUnlock()

	if _, exists := b.groupsStore(region)[groupName]; !exists {
		return nil, fmt.Errorf("%w: Log group %s not found", ErrLogGroupNotFound, groupName)
	}

	if _, exists := b.streamsStore(region)[groupName][streamName]; !exists {
		return nil, fmt.Errorf("%w: Log stream %s not found", ErrLogStreamNotFound, streamName)
	}

	evts := b.eventsStore(region)[groupName][streamName]
	if idx >= len(evts) {
		return nil, fmt.Errorf("%w: log record index %d out of range", ErrValidation, idx)
	}

	ev := evts[idx]
	result := map[string]string{
		keyMessageField:  ev.Message,
		keyTimestamp:     strconv.FormatInt(ev.Timestamp, 10),
		keyIngestionTime: strconv.FormatInt(ev.IngestionTime, 10),
		"@logStream":     streamName,
		"@logGroup":      groupName,
	}

	return result, nil
}

// ListAnomalies lists anomalies for the given anomaly detector ARN with pagination.
func (b *InMemoryBackend) ListAnomalies(
	anomalyDetectorArn string,
	limit int,
	nextToken string,
) ([]Anomaly, string, error) {
	b.mu.RLock("ListAnomalies")
	defer b.mu.RUnlock()

	if anomalyDetectorArn != "" {
		if _, ok := b.logAnomalyDetectors[anomalyDetectorArn]; !ok {
			return nil, "", fmt.Errorf(
				"%w: anomaly detector %s not found",
				ErrLogAnomalyDetectorNotFound,
				anomalyDetectorArn,
			)
		}
	}

	var all []Anomaly
	if anomalyDetectorArn != "" {
		for _, a := range b.anomalies[anomalyDetectorArn] {
			all = append(all, *a)
		}
	} else {
		for _, detectorAnomalies := range b.anomalies {
			for _, a := range detectorAnomalies {
				all = append(all, *a)
			}
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].FirstSeen < all[j].FirstSeen })

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(all) {
		return []Anomaly{}, "", nil
	}

	if limit <= 0 {
		limit = defaultDescribeLimit
	}

	end := startIdx + limit
	var outToken string
	if end < len(all) {
		outToken = encodeNextToken(end)
	} else {
		end = len(all)
	}

	return all[startIdx:end], outToken, nil
}

// ListLogGroupsForQuery returns the log group names that were used in a specific query.
func (b *InMemoryBackend) ListLogGroupsForQuery(queryID string) ([]string, error) {
	if queryID == "" {
		return nil, fmt.Errorf("%w: queryId is required", ErrValidation)
	}

	b.mu.RLock("ListLogGroupsForQuery")
	defer b.mu.RUnlock()

	sq, ok := b.queries[queryID]
	if !ok {
		return nil, fmt.Errorf("%w: query %s not found", ErrQueryNotFound, queryID)
	}

	result := make([]string, len(sq.logGroups))
	copy(result, sq.logGroups)

	return result, nil
}

// GetScheduledQueryHistory returns the execution history for a scheduled query.
func (b *InMemoryBackend) GetScheduledQueryHistory(
	scheduledQueryArn string,
	nextToken string,
	maxResults int,
) ([]ScheduledQueryRunSummary, string, error) {
	if scheduledQueryArn == "" {
		return nil, "", fmt.Errorf("%w: scheduledQueryArn is required", ErrValidation)
	}

	b.mu.RLock("GetScheduledQueryHistory")
	defer b.mu.RUnlock()

	if _, ok := b.scheduledQueries[scheduledQueryArn]; !ok {
		return nil, "", fmt.Errorf(
			"%w: scheduled query %s not found",
			ErrScheduledQueryNotFound,
			scheduledQueryArn,
		)
	}

	runs := b.scheduledQueryRuns[scheduledQueryArn]
	all := make([]ScheduledQueryRunSummary, 0, len(runs))
	for _, r := range runs {
		all = append(all, *r)
	}
	// Most recent invocations first.
	sort.Slice(all, func(i, j int) bool { return all[i].InvocationTime > all[j].InvocationTime })

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(all) {
		return []ScheduledQueryRunSummary{}, "", nil
	}

	if maxResults <= 0 {
		maxResults = defaultDescribeLimit
	}

	end := startIdx + maxResults
	var outToken string
	if end < len(all) {
		outToken = encodeNextToken(end)
	} else {
		end = len(all)
	}

	return all[startIdx:end], outToken, nil
}

// UpdateAnomaly updates the suppression state of a stored anomaly.
func (b *InMemoryBackend) UpdateAnomaly(
	anomalyID, anomalyDetectorArn, suppressionType string,
) error {
	if anomalyDetectorArn == "" {
		return fmt.Errorf("%w: anomalyDetectorArn is required", ErrValidation)
	}

	if anomalyID == "" {
		return fmt.Errorf("%w: anomalyId is required", ErrValidation)
	}

	b.mu.Lock("UpdateAnomaly")
	defer b.mu.Unlock()

	if _, ok := b.logAnomalyDetectors[anomalyDetectorArn]; !ok {
		return fmt.Errorf(
			"%w: anomaly detector %s not found",
			ErrLogAnomalyDetectorNotFound,
			anomalyDetectorArn,
		)
	}

	detectorAnomalies, ok := b.anomalies[anomalyDetectorArn]
	if !ok {
		return fmt.Errorf(
			"%w: anomaly %s not found in detector %s",
			ErrLogAnomalyDetectorNotFound,
			anomalyID,
			anomalyDetectorArn,
		)
	}

	anomaly, ok := detectorAnomalies[anomalyID]
	if !ok {
		return fmt.Errorf(
			"%w: anomaly %s not found in detector %s",
			ErrLogAnomalyDetectorNotFound,
			anomalyID,
			anomalyDetectorArn,
		)
	}

	anomaly.SuppressedState = suppressionType
	if suppressionType == "NO_SUPPRESSION" {
		anomaly.SuppressedDate = 0
	} else {
		anomaly.SuppressedDate = time.Now().UnixMilli()
	}

	return nil
}

// ListLogGroups is the newer paginated list operation, equivalent to DescribeLogGroups.
func (b *InMemoryBackend) ListLogGroups(
	ctx context.Context, namePrefix, nextToken string, limit int,
) ([]LogGroup, string, error) {
	return b.DescribeLogGroups(ctx, namePrefix, nextToken, limit)
}
