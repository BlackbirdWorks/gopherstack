package cloudwatchlogs

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	ErrLogGroupNotFound      = errors.New("ResourceNotFoundException")
	ErrLogGroupAlreadyExists = errors.New("ResourceAlreadyExistsException")
	ErrLogStreamNotFound     = errors.New("ResourceNotFoundException")
	ErrLogStreamAlreadyExist = errors.New("ResourceAlreadyExistsException")
)

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
}

// InMemoryBackend implements StorageBackend using in-memory maps.
type InMemoryBackend struct {
	groups    map[string]*LogGroup
	streams   map[string]map[string]*LogStream
	events    map[string]map[string][]*OutputLogEvent
	accountID string
	region    string
	mu        sync.RWMutex
}

// NewInMemoryBackend creates a new InMemoryBackend with default configuration.
func NewInMemoryBackend() *InMemoryBackend {
	return NewInMemoryBackendWithConfig("000000000000", "us-east-1")
}

// NewInMemoryBackendWithConfig creates a new InMemoryBackend with given account and region.
func NewInMemoryBackendWithConfig(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		accountID: accountID,
		region:    region,
		groups:    make(map[string]*LogGroup),
		streams:   make(map[string]map[string]*LogStream),
		events:    make(map[string]map[string][]*OutputLogEvent),
	}
}

func (b *InMemoryBackend) groupARN(name string) string {
	return fmt.Sprintf("arn:aws:logs:%s:%s:log-group:%s", b.region, b.accountID, name)
}

func (b *InMemoryBackend) streamARN(groupName, streamName string) string {
	return fmt.Sprintf("arn:aws:logs:%s:%s:log-group:%s:log-stream:%s",
		b.region, b.accountID, groupName, streamName)
}

// CreateLogGroup creates a new log group.
func (b *InMemoryBackend) CreateLogGroup(name string) (*LogGroup, error) {
	b.mu.Lock()
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
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.groups[name]; !exists {
		return fmt.Errorf("%w: Log group %s not found", ErrLogGroupNotFound, name)
	}

	delete(b.groups, name)
	delete(b.streams, name)
	delete(b.events, name)

	return nil
}

// DescribeLogGroups returns log groups optionally filtered by prefix, with pagination.
func (b *InMemoryBackend) DescribeLogGroups(prefix, nextToken string, limit int) ([]LogGroup, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	all := make([]LogGroup, 0, len(b.groups))
	for _, g := range b.groups {
		if prefix == "" || strings.HasPrefix(g.LogGroupName, prefix) {
			all = append(all, *g)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].LogGroupName < all[j].LogGroupName })

	return paginateGroups(all, nextToken, limit)
}

// CreateLogStream creates a new log stream within a log group.
func (b *InMemoryBackend) CreateLogStream(groupName, streamName string) (*LogStream, error) {
	b.mu.Lock()
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
	b.mu.RLock()
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

	return paginateStreams(all, nextToken, limit)
}

// PutLogEvents appends log events to a stream and returns the next sequence token.
func (b *InMemoryBackend) PutLogEvents(groupName, streamName string, events []InputLogEvent) (string, error) {
	b.mu.Lock()
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

	return nextToken, nil
}

// GetLogEvents returns events for a stream with optional time bounds, limit, and pagination.
func (b *InMemoryBackend) GetLogEvents(groupName, streamName string, startTime, endTime *int64,
	limit int, nextToken string,
) ([]OutputLogEvent, string, string, error) {
	b.mu.RLock()
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
		limit = 10000
	}

	end := startIdx + limit
	if end > len(filtered) {
		end = len(filtered)
	}

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
	b.mu.RLock()
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
		limit = 10000
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

func paginateGroups(all []LogGroup, nextToken string, limit int) ([]LogGroup, string, error) {
	startIdx := parseNextToken(nextToken)
	if startIdx >= len(all) {
		return []LogGroup{}, "", nil
	}

	if limit <= 0 {
		limit = 50
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

func paginateStreams(all []LogStream, nextToken string, limit int) ([]LogStream, string, error) {
	startIdx := parseNextToken(nextToken)
	if startIdx >= len(all) {
		return []LogStream{}, "", nil
	}

	if limit <= 0 {
		limit = 50
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
