package cloudwatchlogs

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/google/uuid"
)

var (
	ErrLogGroupNotFound              = errors.New("ResourceNotFoundException")
	ErrLogGroupAlreadyExists         = errors.New("ResourceAlreadyExistsException")
	ErrLogStreamNotFound             = errors.New("ResourceNotFoundException")
	ErrLogStreamAlreadyExist         = errors.New("ResourceAlreadyExistsException")
	ErrSubscriptionFilterNotFound    = errors.New("ResourceNotFoundException")
	ErrSubscriptionFilterLimitExceed = errors.New("LimitExceededException")
)

const (
	defaultDescribeLimit = 50
	defaultEventLimit    = 10000
	// maxSubscriptionFilters is the AWS-imposed limit per log group.
	maxSubscriptionFilters = 2
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
}

// InMemoryBackend implements StorageBackend using in-memory maps.
type InMemoryBackend struct {
	groups              map[string]*LogGroup
	streams             map[string]map[string]*LogStream
	events              map[string]map[string][]*OutputLogEvent
	subscriptionFilters map[string][]*SubscriptionFilter
	deliverer           SubscriptionDeliverer
	mu                  *lockmetrics.RWMutex
	accountID           string
	region              string
}

// NewInMemoryBackend creates a new InMemoryBackend with default configuration.
func NewInMemoryBackend() *InMemoryBackend {
	return NewInMemoryBackendWithConfig(config.DefaultAccountID, config.DefaultRegion)
}

// NewInMemoryBackendWithConfig creates a new InMemoryBackend with given account and region.
func NewInMemoryBackendWithConfig(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		accountID:           accountID,
		region:              region,
		groups:              make(map[string]*LogGroup),
		streams:             make(map[string]map[string]*LogStream),
		events:              make(map[string]map[string][]*OutputLogEvent),
		subscriptionFilters: make(map[string][]*SubscriptionFilter),
		mu:                  lockmetrics.New("cloudwatchlogs"),
	}
}

// SetSubscriptionDeliverer sets the deliverer used to forward log events to subscription filter destinations.
func (b *InMemoryBackend) SetSubscriptionDeliverer(d SubscriptionDeliverer) {
	b.mu.Lock("SetSubscriptionDeliverer")
	defer b.mu.Unlock()
	b.deliverer = d
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

	stream.LastIngestionTime = &now
	nextToken := strconv.FormatInt(int64(len(b.events[groupName][streamName])), 10)

	// Collect matching subscription filters for async delivery (while holding the lock).
	filters := b.matchingFilters(groupName, events)
	deliverer := b.deliverer
	accountID := b.accountID

	if len(filters) > 0 && deliverer != nil {
		go b.deliverToFilters(context.Background(), groupName, streamName, accountID, events, filters, deliverer)
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
			if filterPattern != "" && !strings.Contains(ev.Message, filterPattern) {
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
		if strings.Contains(ev.Message, pattern) {
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
		slog.Default().WarnContext(ctx, "cloudwatchlogs: failed to encode subscription payload",
			"logGroup", groupName, "error", err)

		return
	}

	for _, f := range filters {
		deliverErr := deliverer.DeliverLogEvents(ctx, f.DestinationArn, encoded)
		if deliverErr != nil {
			slog.Default().WarnContext(ctx, "cloudwatchlogs: failed to deliver log events to subscription filter",
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
