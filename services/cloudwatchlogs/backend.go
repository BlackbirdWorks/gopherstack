package cloudwatchlogs

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
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
	DescribeLogStreams(groupName, prefix, nextToken string, limit int) ([]LogStream, string, error)
	PutLogEvents(groupName, streamName string, events []InputLogEvent) (string, error)
	GetLogEvents(groupName, streamName string, startTime, endTime *int64, limit int, nextToken string) (
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
	cancel              context.CancelFunc
	groups              map[string]*LogGroup
	accountID           string
	region              string
	queriesOrder        []string
	wg                  sync.WaitGroup
	queryTTL            time.Duration
	maxQueries          int
	deliveryTimeout     time.Duration
}

// NewInMemoryBackend creates a new InMemoryBackend with default configuration.
func NewInMemoryBackend() *InMemoryBackend {
	return NewInMemoryBackendWithConfig(config.DefaultAccountID, config.DefaultRegion)
}

// NewInMemoryBackendWithConfig creates a new InMemoryBackend with given account and region.
func NewInMemoryBackendWithConfig(accountID, region string) *InMemoryBackend {
	ctx, cancel := context.WithCancel(context.Background())

	return &InMemoryBackend{
		accountID:           accountID,
		region:              region,
		groups:              make(map[string]*LogGroup),
		streams:             make(map[string]map[string]*LogStream),
		events:              make(map[string]map[string][]*OutputLogEvent),
		subscriptionFilters: make(map[string][]*SubscriptionFilter),
		queries:             make(map[string]*storedQuery),
		mu:                  lockmetrics.New("cloudwatchlogs"),
		queryTTL:            defaultQueryTTL,
		maxQueries:          defaultMaxQueries,
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
	defer b.mu.Unlock()

	if _, exists := b.groups[groupName]; !exists {
		return "", fmt.Errorf("%w: Log group %s not found", ErrLogGroupNotFound, groupName)
	}

	if _, exists := b.streams[groupName][streamName]; !exists {
		return "", fmt.Errorf("%w: Log stream %s not found", ErrLogStreamNotFound, streamName)
	}

	now := time.Now().UnixMilli()
	stream := b.streams[groupName][streamName]

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
	}

	stream.LastIngestionTime = &now
	nextToken := strconv.FormatInt(int64(len(b.events[groupName][streamName])), 10)

	// Collect matching subscription filters for async delivery (while holding the lock).
	filters := b.matchingFilters(groupName, events)
	deliverer := b.deliverer
	accountID := b.accountID

	if len(filters) > 0 && deliverer != nil {
		// Capture all state needed by the goroutine while holding the lock.
		timeout := b.deliveryTimeout
		workerSem := b.workerSem
		ctx := b.ctx

		b.wg.Go(func() {
			// Acquire a worker slot or abort if the backend is shutting down.
			select {
			case workerSem <- struct{}{}:
				defer func() { <-workerSem }()
			case <-ctx.Done():
				return
			}

			delivCtx := ctx
			if timeout > 0 {
				var cancel context.CancelFunc
				delivCtx, cancel = context.WithTimeout(ctx, timeout)
				defer cancel()
			}

			b.deliverToFilters(delivCtx, groupName, streamName, accountID, events, filters, deliverer)
		})
	}

	return nextToken, nil
}

// GetLogEvents returns events for a stream with optional time bounds, limit, and pagination.
func (b *InMemoryBackend) GetLogEvents(groupName, streamName string, startTime, endTime *int64,
	limit int, nextToken string,
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

	startIdx := parseNextToken(nextToken)
	if limit <= 0 {
		limit = defaultEventLimit
	}

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

	var all []*OutputLogEvent
	streamOrder := sortedKeys(b.streams[groupName])
	for _, sName := range streamOrder {
		if len(streamSet) > 0 && !streamSet[sName] {
			continue
		}
		for _, ev := range b.events[groupName][sName] {
			if filterPattern != "" && !filterPatternMatches(filterPattern, ev.Message) {
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
func filterMatches(pattern string, events []InputLogEvent) bool {
	if pattern == "" {
		return len(events) > 0
	}

	for _, ev := range events {
		if filterPatternMatches(pattern, ev.Message) {
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
		deliverErr := deliverer.DeliverLogEvents(ctx, f.DestinationArn, encoded)
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

// filterPatternMatches returns true when the CloudWatch Logs filter pattern matches the message.
//
// Pattern syntax:
//   - Empty pattern matches all messages.
//   - Space-separated terms (AND logic): all terms must match.
//   - Term prefixed with "?" means NOT (the term must NOT appear).
//   - Quoted terms ("...") require an exact substring match.
//   - Terms without quotes use substring matching; "*" inside a term is a wildcard.
func filterPatternMatches(pattern, message string) bool {
	terms := parseFilterPatternTerms(pattern)

	for _, term := range terms {
		negate := strings.HasPrefix(term, "?")
		t := term
		if negate {
			t = term[1:]
		}

		hit := filterTermMatches(t, message)
		if negate == hit {
			// negate && hit => excluded term found; !negate && !hit => required term missing.
			return false
		}
	}

	return true
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

// filterTermMatches returns true when the term matches the message.
// Quoted terms require an exact substring match; unquoted terms with "*"
// use wildcard matching; otherwise a simple substring match is used.
func filterTermMatches(term, message string) bool {
	if len(term) >= 2 && term[0] == '"' && term[len(term)-1] == '"' {
		return strings.Contains(message, term[1:len(term)-1])
	}

	if !strings.ContainsRune(term, '*') {
		return strings.Contains(message, term)
	}

	// Build a regexp from the wildcard term.
	parts := strings.Split(term, "*")
	escaped := make([]string, len(parts))
	for i, p := range parts {
		escaped[i] = regexp.QuoteMeta(p)
	}

	re, err := regexp.Compile(strings.Join(escaped, ".*"))
	if err != nil {
		return strings.Contains(message, term)
	}

	return re.MatchString(message)
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
	q, parseErr := parseInsightsQuery(queryString)
	if parseErr != nil {
		return nil, fmt.Errorf("invalid query: %w", parseErr)
	}

	// Collect events and execute the query under a single read lock to avoid
	// observing an inconsistent snapshot (TOCTOU: collect then release then store).
	b.mu.RLock("StartQuery")
	allEvents, recordsScanned := b.collectQueryEvents(logGroupNames, startTime, endTime)
	// Execute the query while still holding the read lock so the result is
	// consistent with the collected events.
	results := executeQuery(q, allEvents)
	b.mu.RUnlock()

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
}
