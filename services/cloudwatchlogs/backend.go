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

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/google/uuid"
)

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
		"ENABLED":  {},
		"DISABLED": {},
	}
}

// validAccountPolicyTypes returns the allowed values for the account policy type field.
func validAccountPolicyTypes() map[string]struct{} {
	return map[string]struct{}{
		"DATA_PROTECTION_POLICY":     {},
		"SUBSCRIPTION_FILTER_POLICY": {},
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
)

// SubscriptionDeliverer delivers encoded log event payloads to a subscription filter destination.
type SubscriptionDeliverer interface {
	// DeliverLogEvents delivers a gzipped, base64-encoded CloudWatch Logs payload to destinationArn.
	DeliverLogEvents(ctx context.Context, destinationArn string, payload []byte) error
}

// SubscriptionDelivererFunc is a function adapter for SubscriptionDeliverer.
type SubscriptionDelivererFunc func(ctx context.Context, destinationArn string, payload []byte) error

// DeliverLogEvents implements SubscriptionDeliverer.
func (f SubscriptionDelivererFunc) DeliverLogEvents(ctx context.Context, destinationArn string, payload []byte) error {
	return f(ctx, destinationArn, payload)
}

// StorageBackend is the interface for a CloudWatch Logs in-memory store.
type StorageBackend interface {
	CreateLogGroup(name string) (*LogGroup, error)
	DeleteLogGroup(name string) error
	DescribeLogGroups(prefix, nextToken string, limit int) ([]LogGroup, string, error)
	CreateLogStream(groupName, streamName string) (*LogStream, error)
	DeleteLogStream(groupName, streamName string) error
	DescribeLogStreams(groupName, prefix, nextToken string, limit int) ([]LogStream, string, error)
	PutLogEvents(groupName, streamName string, events []InputLogEvent) (string, error)
	GetLogEvents(
		groupName, streamName string,
		startTime, endTime *int64,
		limit int,
		nextToken string,
		startFromHead bool,
	) (
		[]OutputLogEvent, string, string, error)
	FilterLogEvents(groupName string, streamNames []string, filterPattern string,
		startTime, endTime *int64, limit int, nextToken string) ([]OutputLogEvent, string, error)
	PutSubscriptionFilter(groupName, filterName, filterPattern, destinationArn string) error
	DescribeSubscriptionFilters(groupName, filterNamePrefix, nextToken string, limit int) (
		[]SubscriptionFilter, string, error)
	DeleteSubscriptionFilter(groupName, filterName string) error
	SetRetentionPolicy(groupName string, days *int32) error
	StartQuery(queryID, queryString string, logGroupNames []string, startTime, endTime int64) (*QueryInfo, error)
	GetQueryResults(queryID string) ([][]ResultField, QueryStatistics, QueryStatus, error)
	StopQuery(queryID string) error
	DescribeQueries(logGroupName, statusFilter, nextToken string, maxResults int) ([]QueryInfo, string, error)

	// AssociateKmsKey associates a KMS key with a log group or query results resource.
	AssociateKmsKey(logGroupName, resourceIdentifier, kmsKeyID string) error
	// AssociateSourceToS3TableIntegration associates a data source with an S3 table integration.
	AssociateSourceToS3TableIntegration(integrationArn, dataSourceName, dataSourceType string) (string, error)
	// CancelExportTask cancels a pending or running export task.
	CancelExportTask(taskID string) error
	// CancelImportTask cancels a running import task.
	CancelImportTask(importID string) (*ImportTask, error)
	// CreateDelivery creates a delivery between a delivery source and destination.
	CreateDelivery(deliverySourceName, deliveryDestinationArn string, tags map[string]string) (*Delivery, error)
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
	CreateScheduledQuery(name, queryString, scheduleExpression, executionRoleArn, state string) (string, error)
	// DeleteAccountPolicy deletes a CloudWatch Logs account-level policy.
	DeleteAccountPolicy(policyName, policyType string) error
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
	deliverer           SubscriptionDeliverer
	ctx                 context.Context
	mu                  *lockmetrics.RWMutex
	workerSem           chan struct{}
	streams             map[string]map[string]*LogStream
	events              map[string]map[string][]*OutputLogEvent
	subscriptionFilters map[string][]*SubscriptionFilter
	queries             map[string]*storedQuery
	parsedQueries       map[string]*insightsQuery
	exportTasks         map[string]*ExportTask
	importTasks         map[string]*ImportTask
	deliveries          map[string]*Delivery
	logAnomalyDetectors map[string]*LogAnomalyDetector
	scheduledQueries    map[string]*ScheduledQuery
	accountPolicies     map[string]*AccountPolicy
	kmsKeys             map[string]string
	s3TableIntegrations map[string]string
	cancel              context.CancelFunc
	groups              map[string]*LogGroup
	accountID           string
	region              string
	queriesOrder        []string
	parsedQueriesOrder  []string
	wg                  sync.WaitGroup
	queryTTL            time.Duration
	maxQueries          int
	maxParsedQueries    int
	deliveryTimeout     time.Duration
}

// NewInMemoryBackend creates a new InMemoryBackend with default configuration.
func NewInMemoryBackend() *InMemoryBackend {
	return NewInMemoryBackendWithContext(context.Background(), config.DefaultAccountID, config.DefaultRegion)
}

// NewInMemoryBackendWithConfig creates a new InMemoryBackend with given account and region.
func NewInMemoryBackendWithConfig(accountID, region string) *InMemoryBackend {
	return NewInMemoryBackendWithContext(context.Background(), accountID, region)
}

// NewInMemoryBackendWithContext creates a new InMemoryBackend with the given parent context,
// account ID, and region. Subscription delivery goroutines are bounded by svcCtx so that
// they are cancelled on server shutdown.
// If svcCtx is nil, [context.Background] is used.
func NewInMemoryBackendWithContext(svcCtx context.Context, accountID, region string) *InMemoryBackend {
	if svcCtx == nil {
		svcCtx = context.Background()
	}

	ctx, cancel := context.WithCancel(svcCtx)

	return &InMemoryBackend{
		accountID:           accountID,
		region:              region,
		groups:              make(map[string]*LogGroup),
		streams:             make(map[string]map[string]*LogStream),
		events:              make(map[string]map[string][]*OutputLogEvent),
		subscriptionFilters: make(map[string][]*SubscriptionFilter),
		queries:             make(map[string]*storedQuery),
		parsedQueries:       make(map[string]*insightsQuery),
		exportTasks:         make(map[string]*ExportTask),
		importTasks:         make(map[string]*ImportTask),
		deliveries:          make(map[string]*Delivery),
		logAnomalyDetectors: make(map[string]*LogAnomalyDetector),
		scheduledQueries:    make(map[string]*ScheduledQuery),
		accountPolicies:     make(map[string]*AccountPolicy),
		kmsKeys:             make(map[string]string),
		s3TableIntegrations: make(map[string]string),
		mu:                  lockmetrics.New("cloudwatchlogs"),
		queryTTL:            defaultQueryTTL,
		maxQueries:          defaultMaxQueries,
		maxParsedQueries:    defaultParsedQueryCacheSize,
		ctx:                 ctx,
		cancel:              cancel,
		workerSem:           make(chan struct{}, defaultDeliveryWorkers),
		deliveryTimeout:     defaultDeliveryTimeout,
	}
}

// SetSubscriptionDeliverer sets the deliverer used to forward log events to subscription filter destinations.
func (b *InMemoryBackend) SetSubscriptionDeliverer(d SubscriptionDeliverer) {
	b.mu.Lock("SetSubscriptionDeliverer")
	defer b.mu.Unlock()
	b.deliverer = d
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

func (b *InMemoryBackend) groupARN(name string) string {
	return arn.Build("logs", b.region, b.accountID, "log-group:"+name)
}

func (b *InMemoryBackend) streamARN(groupName, streamName string) string {
	return arn.Build("logs", b.region, b.accountID, "log-group:"+groupName+":log-stream:"+streamName)
}

// CreateLogGroup creates a new log group.
func (b *InMemoryBackend) CreateLogGroup(name string) (*LogGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: logGroupName is required", ErrValidation)
	}

	b.mu.Lock("CreateLogGroup")
	defer b.mu.Unlock()

	if _, exists := b.groups[name]; exists {
		return nil, fmt.Errorf("%w: Log group %s already exists", ErrLogGroupAlreadyExists, name)
	}

	g := &LogGroup{
		CreationTime: time.Now().UnixMilli(),
		LogGroupName: name,
		Arn:          b.groupARN(name),
	}
	b.groups[name] = g
	b.streams[name] = make(map[string]*LogStream)
	b.events[name] = make(map[string][]*OutputLogEvent)

	return g, nil
}

// DeleteLogGroup deletes a log group and all its streams/events.
func (b *InMemoryBackend) DeleteLogGroup(name string) error {
	b.mu.Lock("DeleteLogGroup")
	defer b.mu.Unlock()

	if _, exists := b.groups[name]; !exists {
		return fmt.Errorf("%w: Log group %s not found", ErrLogGroupNotFound, name)
	}

	delete(b.groups, name)
	delete(b.streams, name)
	delete(b.events, name)
	delete(b.subscriptionFilters, name)

	return nil
}

// SetRetentionPolicy sets or clears the retention policy for a log group.
// A nil days value removes any existing retention policy.
func (b *InMemoryBackend) SetRetentionPolicy(groupName string, days *int32) error {
	b.mu.Lock("SetRetentionPolicy")
	defer b.mu.Unlock()

	g, exists := b.groups[groupName]
	if !exists {
		return fmt.Errorf("%w: Log group %s not found", ErrLogGroupNotFound, groupName)
	}

	g.RetentionInDays = days

	return nil
}

// DescribeLogGroups returns log groups optionally filtered by prefix, with pagination.
func (b *InMemoryBackend) DescribeLogGroups(prefix, nextToken string, limit int) ([]LogGroup, string, error) {
	b.mu.RLock("DescribeLogGroups")
	defer b.mu.RUnlock()

	all := make([]LogGroup, 0, len(b.groups))
	for _, g := range b.groups {
		if prefix == "" || strings.HasPrefix(g.LogGroupName, prefix) {
			all = append(all, *g)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].LogGroupName < all[j].LogGroupName })

	groups, token := paginateGroups(all, nextToken, limit)

	return groups, token, nil
}

// CreateLogStream creates a new log stream within a log group.
func (b *InMemoryBackend) CreateLogStream(groupName, streamName string) (*LogStream, error) {
	if groupName == "" {
		return nil, fmt.Errorf("%w: logGroupName is required", ErrValidation)
	}

	if streamName == "" {
		return nil, fmt.Errorf("%w: logStreamName is required", ErrValidation)
	}

	b.mu.Lock("CreateLogStream")
	defer b.mu.Unlock()

	if _, exists := b.groups[groupName]; !exists {
		return nil, fmt.Errorf("%w: Log group %s not found", ErrLogGroupNotFound, groupName)
	}

	if _, exists := b.streams[groupName][streamName]; exists {
		return nil, fmt.Errorf("%w: Log stream %s already exists", ErrLogStreamAlreadyExist, streamName)
	}

	s := &LogStream{
		CreationTime:  time.Now().UnixMilli(),
		LogStreamName: streamName,
		Arn:           b.streamARN(groupName, streamName),
	}
	b.streams[groupName][streamName] = s
	b.events[groupName][streamName] = nil

	return s, nil
}

// DeleteLogStream deletes a log stream and all its events from a log group.
func (b *InMemoryBackend) DeleteLogStream(groupName, streamName string) error {
	b.mu.Lock("DeleteLogStream")
	defer b.mu.Unlock()

	if _, exists := b.groups[groupName]; !exists {
		return fmt.Errorf("%w: Log group %s not found", ErrLogGroupNotFound, groupName)
	}

	if _, exists := b.streams[groupName][streamName]; !exists {
		return fmt.Errorf("%w: Log stream %s not found", ErrLogStreamNotFound, streamName)
	}

	delete(b.streams[groupName], streamName)
	delete(b.events[groupName], streamName)

	return nil
}

// DescribeLogStreams returns log streams for a group, optionally filtered by prefix, with pagination.
func (b *InMemoryBackend) DescribeLogStreams(groupName, prefix, nextToken string, limit int) (
	[]LogStream, string, error,
) {
	b.mu.RLock("DescribeLogStreams")
	defer b.mu.RUnlock()

	if _, exists := b.groups[groupName]; !exists {
		return nil, "", fmt.Errorf("%w: Log group %s not found", ErrLogGroupNotFound, groupName)
	}

	all := make([]LogStream, 0, len(b.streams[groupName]))
	for _, s := range b.streams[groupName] {
		if prefix == "" || strings.HasPrefix(s.LogStreamName, prefix) {
			all = append(all, *s)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].LogStreamName < all[j].LogStreamName })

	streams, token := paginateStreams(all, nextToken, limit)

	return streams, token, nil
}

// PutLogEvents appends log events to a stream and returns the next sequence token.
func (b *InMemoryBackend) PutLogEvents(groupName, streamName string, events []InputLogEvent) (string, error) {
	b.mu.Lock("PutLogEvents")

	if _, exists := b.groups[groupName]; !exists {
		b.mu.Unlock()

		return "", fmt.Errorf("%w: Log group %s not found", ErrLogGroupNotFound, groupName)
	}

	if _, exists := b.streams[groupName][streamName]; !exists {
		b.mu.Unlock()

		return "", fmt.Errorf("%w: Log stream %s not found", ErrLogStreamNotFound, streamName)
	}

	now := time.Now().UnixMilli()
	stream := b.streams[groupName][streamName]
	b.appendEvents(groupName, streamName, stream, now, events)

	stream.LastIngestionTime = &now
	nextToken := strconv.FormatInt(int64(len(b.events[groupName][streamName])), 10)

	// Collect matching subscription filters for async delivery (while holding the lock).
	filters := b.matchingFilters(groupName, events)
	deliverer := b.deliverer
	accountID := b.accountID
	timeout := b.deliveryTimeout
	workerSem := b.workerSem
	ctx := b.ctx
	eventsForDelivery := append([]InputLogEvent(nil), events...)
	filtersForDelivery := cloneSubscriptionFilters(filters)

	b.mu.Unlock()

	if len(filters) > 0 && deliverer != nil {
		b.wg.Go(func() {
			// Acquire a worker slot or abort if the backend is shutting down.
			select {
			case workerSem <- struct{}{}:
				defer func() { <-workerSem }()
			case <-ctx.Done():
				return
			}

			b.deliverToFilters(
				ctx,
				groupName,
				streamName,
				accountID,
				eventsForDelivery,
				filtersForDelivery,
				deliverer,
				timeout,
			)
		})
	}

	return nextToken, nil
}

// appendEvents writes events into the stream, updates stream timestamp metadata,
// and enforces the per-stream event cap.
// Must be called while holding the backend write lock.
// Note: log events may arrive with out-of-order timestamps (AWS allows this),
// so min/max timestamp tracking must inspect all events.
func (b *InMemoryBackend) appendEvents(
	groupName, streamName string, stream *LogStream, now int64, events []InputLogEvent,
) {
	for _, ev := range events {
		out := &OutputLogEvent{
			IngestionTime: now,
			Message:       ev.Message,
			Timestamp:     ev.Timestamp,
		}
		b.events[groupName][streamName] = append(b.events[groupName][streamName], out)

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
	if cur := b.events[groupName][streamName]; len(cur) > maxEventsPerStream {
		b.events[groupName][streamName] = cur[len(cur)-maxEventsPerStream:]
		// Recalculate metadata from the remaining events: since events may have
		// out-of-order timestamps, the dropped events might include the global
		// min/max, so we must re-scan rather than assume positional ordering.
		updateStreamTimestamps(stream, b.events[groupName][streamName])
	}
}

// GetLogEvents returns events for a stream with optional time bounds, limit, and pagination.
// startFromHead controls the iteration direction:
//   - true  (start from oldest): begin at the oldest matching event.
//   - false (AWS default when no nextToken is provided): begin at the newest events.
//
// In practice the AWS SDK always passes a nextToken once pagination begins, at which point the
// token encodes the offset directly and startFromHead is ignored.
func (b *InMemoryBackend) GetLogEvents(groupName, streamName string, startTime, endTime *int64,
	limit int, nextToken string, startFromHead bool,
) ([]OutputLogEvent, string, string, error) {
	b.mu.RLock("GetLogEvents")
	defer b.mu.RUnlock()

	if _, exists := b.groups[groupName]; !exists {
		return nil, "", "", fmt.Errorf("%w: Log group %s not found", ErrLogGroupNotFound, groupName)
	}

	if _, exists := b.streams[groupName][streamName]; !exists {
		return nil, "", "", fmt.Errorf("%w: Log stream %s not found", ErrLogStreamNotFound, streamName)
	}

	all := b.events[groupName][streamName]
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

	fwdToken := strconv.Itoa(end)
	bwdToken := strconv.Itoa(startIdx)

	result := make([]OutputLogEvent, len(page))
	for i, e := range page {
		result[i] = *e
	}

	return result, fwdToken, bwdToken, nil
}

// FilterLogEvents searches events across streams in a group with optional filter pattern.
func (b *InMemoryBackend) FilterLogEvents(groupName string, streamNames []string, filterPattern string,
	startTime, endTime *int64, limit int, nextToken string,
) ([]OutputLogEvent, string, error) {
	b.mu.RLock("FilterLogEvents")
	defer b.mu.RUnlock()

	if _, exists := b.groups[groupName]; !exists {
		return nil, "", fmt.Errorf("%w: Log group %s not found", ErrLogGroupNotFound, groupName)
	}

	streamSet := make(map[string]bool)
	for _, s := range streamNames {
		streamSet[s] = true
	}

	// Compile the filter pattern once before iterating over events so that
	// wildcard regexes are not recompiled for every event.
	var compiled *compiledFilterPattern
	if filterPattern != "" {
		compiled = compileFilterPattern(filterPattern)
	}

	var all []*OutputLogEvent
	streamOrder := sortedKeys(b.streams[groupName])
	for _, sName := range streamOrder {
		if len(streamSet) > 0 && !streamSet[sName] {
			continue
		}
		for _, ev := range b.events[groupName][sName] {
			if compiled != nil && !compiled.matches(ev.Message) {
				continue
			}
			all = append(all, ev)
		}
	}

	filtered := filterByTime(all, startTime, endTime)

	startIdx := parseNextToken(nextToken)
	if limit <= 0 {
		limit = defaultEventLimit
	}

	end := startIdx + limit
	var outToken string
	if end < len(filtered) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(filtered)
	}

	page := filtered[startIdx:end]
	result := make([]OutputLogEvent, len(page))
	for i, e := range page {
		result[i] = *e
	}

	return result, outToken, nil
}

// PutSubscriptionFilter creates or updates a subscription filter for a log group.
func (b *InMemoryBackend) PutSubscriptionFilter(groupName, filterName, filterPattern, destinationArn string) error {
	if groupName == "" {
		return fmt.Errorf("%w: logGroupName is required", ErrValidation)
	}

	if filterName == "" {
		return fmt.Errorf("%w: filterName is required", ErrValidation)
	}

	if destinationArn == "" {
		return fmt.Errorf("%w: destinationArn is required", ErrValidation)
	}

	b.mu.Lock("PutSubscriptionFilter")
	defer b.mu.Unlock()

	if _, exists := b.groups[groupName]; !exists {
		return fmt.Errorf("%w: Log group %s not found", ErrLogGroupNotFound, groupName)
	}

	existing := b.subscriptionFilters[groupName]

	// Check for a filter with the same name (update).
	for i, f := range existing {
		if f.FilterName == filterName {
			existing[i].FilterPattern = filterPattern
			existing[i].DestinationArn = destinationArn

			return nil
		}
	}

	// Enforce AWS limit of 2 subscription filters per log group.
	if len(existing) >= maxSubscriptionFilters {
		return fmt.Errorf("%w: log group %s already has the maximum number of subscription filters",
			ErrSubscriptionFilterLimitExceed, groupName)
	}

	b.subscriptionFilters[groupName] = append(existing, &SubscriptionFilter{
		FilterName:     filterName,
		FilterPattern:  filterPattern,
		LogGroupName:   groupName,
		DestinationArn: destinationArn,
		CreationTime:   time.Now().UnixMilli(),
	})

	return nil
}

// DescribeSubscriptionFilters returns subscription filters for a log group with optional prefix and pagination.
func (b *InMemoryBackend) DescribeSubscriptionFilters(groupName, filterNamePrefix, nextToken string, limit int) (
	[]SubscriptionFilter, string, error,
) {
	b.mu.RLock("DescribeSubscriptionFilters")
	defer b.mu.RUnlock()

	if _, exists := b.groups[groupName]; !exists {
		return nil, "", fmt.Errorf("%w: Log group %s not found", ErrLogGroupNotFound, groupName)
	}

	all := make([]SubscriptionFilter, 0)
	for _, f := range b.subscriptionFilters[groupName] {
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
		outToken = strconv.Itoa(end)
	} else {
		end = len(all)
	}

	return all[startIdx:end], outToken, nil
}

// DeleteSubscriptionFilter removes a subscription filter from a log group.
func (b *InMemoryBackend) DeleteSubscriptionFilter(groupName, filterName string) error {
	b.mu.Lock("DeleteSubscriptionFilter")
	defer b.mu.Unlock()

	if _, exists := b.groups[groupName]; !exists {
		return fmt.Errorf("%w: Log group %s not found", ErrLogGroupNotFound, groupName)
	}

	filters := b.subscriptionFilters[groupName]
	for i, f := range filters {
		if f.FilterName == filterName {
			b.subscriptionFilters[groupName] = append(filters[:i], filters[i+1:]...)

			return nil
		}
	}

	return fmt.Errorf("%w: subscription filter %s not found in log group %s",
		ErrSubscriptionFilterNotFound, filterName, groupName)
}

// matchingFilters returns subscription filters whose pattern matches any of the given events.
// Must be called with the write lock held (called from PutLogEvents before Unlock).
func (b *InMemoryBackend) matchingFilters(groupName string, events []InputLogEvent) []*SubscriptionFilter {
	filters := b.subscriptionFilters[groupName]
	if len(filters) == 0 {
		return nil
	}

	var matched []*SubscriptionFilter
	for _, f := range filters {
		if filterMatches(f.FilterPattern, events) {
			matched = append(matched, f)
		}
	}

	return matched
}

// filterMatches returns true when the filter pattern matches at least one event.
// An empty pattern matches all events.
// The pattern is compiled once and reused across events.
func filterMatches(pattern string, events []InputLogEvent) bool {
	if pattern == "" {
		return len(events) > 0
	}

	compiled := compileFilterPattern(pattern)

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
		cancel := func() {}
		if timeout > 0 {
			deliverCtx, cancel = context.WithTimeout(ctx, timeout)
		}

		deliverErr := deliverer.DeliverLogEvents(deliverCtx, f.DestinationArn, encoded)
		cancel()

		if deliverErr != nil {
			logger.Load(ctx).WarnContext(ctx, "cloudwatchlogs: failed to deliver log events to subscription filter",
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
// repeated matching across many log events (used by FilterLogEvents).
type compiledFilterPattern struct {
	terms []compiledTerm
}

// compiledTerm holds a single pre-compiled term from a filter pattern.
type compiledTerm struct {
	// exact is the literal substring for quoted/plain terms (non-wildcard).
	// re is used for wildcard terms.
	re      *regexp.Regexp
	exact   string
	negate  bool
	isExact bool // true => use exact (strings.Contains); false => use re
}

// compileFilterPattern parses pattern into a compiledFilterPattern for efficient reuse.
// An empty pattern always matches all messages.
func compileFilterPattern(pattern string) *compiledFilterPattern {
	rawTerms := parseFilterPatternTerms(pattern)
	terms := make([]compiledTerm, 0, len(rawTerms))

	for _, raw := range rawTerms {
		negate := strings.HasPrefix(raw, "?")
		t := raw
		if negate {
			t = raw[1:]
		}

		var ct compiledTerm
		ct.negate = negate

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

		terms = append(terms, ct)
	}

	return &compiledFilterPattern{terms: terms}
}

// matches reports whether the message satisfies all terms in the pattern.
func (p *compiledFilterPattern) matches(message string) bool {
	for _, ct := range p.terms {
		var hit bool
		if ct.isExact {
			hit = strings.Contains(message, ct.exact)
		} else {
			hit = ct.re.MatchString(message)
		}

		if ct.negate == hit {
			return false
		}
	}

	return true
}

// filterPatternMatches returns true when the CloudWatch Logs filter pattern matches the message.
//
// Pattern syntax:
//   - Empty pattern matches all messages.
//   - Space-separated terms (AND logic): all terms must match.
//   - Term prefixed with "?" means NOT (the term must NOT appear).
//   - Quoted terms ("...") require an exact substring match.
//   - Terms without quotes use substring matching; "*" inside a term is a wildcard.
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
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	return keys
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
		outToken = strconv.Itoa(end)
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
		outToken = strconv.Itoa(end)
	} else {
		end = len(all)
	}

	return all[startIdx:end], outToken
}

func parseNextToken(token string) int {
	if token == "" {
		return 0
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

	b.parsedQueries[queryString] = cloneInsightsQuery(parsed)
	b.parsedQueriesOrder = append(b.parsedQueriesOrder, queryString)

	return cloneInsightsQuery(parsed), nil
}

// StartQuery stores a new insights query and executes it immediately against in-memory events.
// collectQueryEvents scans events in the given log groups within [startTime, endTime].
// It must be called while holding at least a read lock.
func (b *InMemoryBackend) collectQueryEvents(
	logGroupNames []string, startTime, endTime int64,
) ([]*OutputLogEvent, float64) {
	var eventsOut []*OutputLogEvent
	var recordsScanned float64

	for _, groupName := range logGroupNames {
		streamMap, exists := b.events[groupName]
		if !exists {
			continue
		}
		for _, evts := range streamMap {
			for _, ev := range evts {
				recordsScanned++
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

	return eventsOut, recordsScanned
}

// StartQuery stores a new insights query and executes it immediately against in-memory events.
func (b *InMemoryBackend) StartQuery(
	queryID, queryString string, logGroupNames []string, startTime, endTime int64,
) (*QueryInfo, error) {
	q, parseErr := b.getParsedInsightsQuery(queryString)
	if parseErr != nil {
		return nil, fmt.Errorf("invalid query: %w", parseErr)
	}

	// Collect events under a read lock, then release the lock before running the
	// query. This prevents regex matching and sorting from holding the lock while
	// still delivering a consistent snapshot (no writes can interleave the collect
	// and execute phases — a copy of the slice is taken under the lock).
	b.mu.RLock("StartQuery")
	allEventsRaw, recordsScanned := b.collectQueryEvents(logGroupNames, startTime, endTime)
	// Take a snapshot copy of the event pointers so we can safely release the lock.
	allEvents := make([]*OutputLogEvent, len(allEventsRaw))
	copy(allEvents, allEventsRaw)
	b.mu.RUnlock()

	// Execute the query outside the lock — regex matching and sorting can be non-trivial.
	results := executeQuery(q, allEvents)

	stats := QueryStatistics{
		RecordsScanned: recordsScanned,
		RecordsMatched: float64(len(results)),
		BytesScanned:   0,
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
func (b *InMemoryBackend) GetQueryResults(queryID string) ([][]ResultField, QueryStatistics, QueryStatus, error) {
	b.mu.RLock("GetQueryResults")
	defer b.mu.RUnlock()

	sq, ok := b.queries[queryID]
	if !ok {
		return nil, QueryStatistics{}, "", fmt.Errorf("%w: query %s not found", ErrQueryNotFound, queryID)
	}

	return sq.results, sq.stats, sq.info.Status, nil
}

// StopQuery marks a query as cancelled. Since execution is synchronous, this is a no-op on results.
func (b *InMemoryBackend) StopQuery(queryID string) error {
	b.mu.Lock("StopQuery")
	defer b.mu.Unlock()

	sq, ok := b.queries[queryID]
	if !ok {
		return fmt.Errorf("%w: query %s not found", ErrQueryNotFound, queryID)
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
		outToken = strconv.Itoa(end)
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

	b.groups = make(map[string]*LogGroup)
	b.streams = make(map[string]map[string]*LogStream)
	b.events = make(map[string]map[string][]*OutputLogEvent)
	b.subscriptionFilters = make(map[string][]*SubscriptionFilter)
	b.queries = make(map[string]*storedQuery)
	b.queriesOrder = nil
	b.parsedQueries = make(map[string]*insightsQuery)
	b.parsedQueriesOrder = nil
	b.exportTasks = make(map[string]*ExportTask)
	b.importTasks = make(map[string]*ImportTask)
	b.deliveries = make(map[string]*Delivery)
	b.logAnomalyDetectors = make(map[string]*LogAnomalyDetector)
	b.scheduledQueries = make(map[string]*ScheduledQuery)
	b.accountPolicies = make(map[string]*AccountPolicy)
	b.kmsKeys = make(map[string]string)
	b.s3TableIntegrations = make(map[string]string)
}

// AssociateKmsKey associates a KMS key with a log group or query results resource.
// Exactly one of logGroupName or resourceIdentifier must be non-empty.
func (b *InMemoryBackend) AssociateKmsKey(logGroupName, resourceIdentifier, kmsKeyID string) error {
	if kmsKeyID == "" {
		return fmt.Errorf("%w: kmsKeyId is required", ErrValidation)
	}

	if logGroupName == "" && resourceIdentifier == "" {
		return fmt.Errorf("%w: one of logGroupName or resourceIdentifier is required", ErrValidation)
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
	if task.Status != "PENDING" && task.Status != "RUNNING" {
		return fmt.Errorf("%w: export task %s is in terminal state %s and cannot be cancelled",
			ErrValidation, taskID, task.Status)
	}

	task.Status = "CANCELLED"

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
	if task.Status != "ACTIVE" {
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
		Status:            "PENDING",
		CreationTime:      time.Now().UnixMilli(),
	}

	b.mu.Lock("CreateExportTask")
	defer b.mu.Unlock()

	b.exportTasks[taskID] = task

	return taskID, nil
}

// CreateImportTask creates an import task from a CloudTrail Lake event data store.
func (b *InMemoryBackend) CreateImportTask(importRoleArn, importSourceArn string) (*ImportTask, error) {
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
		Status:               "ACTIVE",
		CreationTime:         now,
		LastUpdatedTime:      now,
	}

	b.mu.Lock("CreateImportTask")
	defer b.mu.Unlock()

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
			return "", fmt.Errorf("%w: invalid evaluationFrequency %q", ErrValidation, evaluationFrequency)
		}
	}

	id := uuid.New().String()
	detectorARN := arn.Build("logs", b.region, b.accountID, "log-anomaly-detector:"+id)

	detector := &LogAnomalyDetector{
		AnomalyDetectorArn:    detectorARN,
		DetectorName:          detectorName,
		LogGroupArnList:       slices.Clone(logGroupArnList),
		EvaluationFrequency:   evaluationFrequency,
		FilterPattern:         filterPattern,
		KmsKeyID:              kmsKeyID,
		AnomalyVisibilityTime: anomalyVisibilityTime,
		CreationTimeStamp:     time.Now().UnixMilli(),
	}

	b.mu.Lock("CreateLogAnomalyDetector")
	defer b.mu.Unlock()

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
			return "", fmt.Errorf("%w: invalid state %q, must be ENABLED or DISABLED", ErrValidation, state)
		}
	} else {
		state = "ENABLED"
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

	b.scheduledQueries[queryARN] = sq

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
