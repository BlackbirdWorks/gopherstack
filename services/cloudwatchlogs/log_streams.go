package cloudwatchlogs

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func (b *InMemoryBackend) streamARN(region, groupName, streamName string) string {
	return arn.Build("logs", region, b.accountID, "log-group:"+groupName+":log-stream:"+streamName)
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

	if !b.groupHas(region, groupName) {
		return nil, fmt.Errorf("%w: Log group %s not found", ErrLogGroupNotFound, groupName)
	}

	if b.streamHas(region, groupName, streamName) {
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
		region:        region,
		logGroupName:  groupName,
	}
	b.streamPut(s)

	return s, nil
}

// DeleteLogStream deletes a log stream and all its events from a log group.
func (b *InMemoryBackend) DeleteLogStream(ctx context.Context, groupName, streamName string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteLogStream")
	defer b.mu.Unlock()

	group, groupExists := b.groupGet(region, groupName)
	if !groupExists {
		return fmt.Errorf("%w: Log group %s not found", ErrLogGroupNotFound, groupName)
	}

	stream, exists := b.streamGet(region, groupName, streamName)
	if !exists {
		return fmt.Errorf("%w: Log stream %s not found", ErrLogStreamNotFound, streamName)
	}

	if stream != nil && group != nil {
		group.StoredBytes -= stream.StoredBytes
	}

	b.streamDelete(region, groupName, streamName)

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

	if !b.groupHas(region, groupName) {
		return nil, "", fmt.Errorf("%w: Log group %s not found", ErrLogGroupNotFound, groupName)
	}

	if limit > defaultDescribeLimit {
		limit = defaultDescribeLimit
	}

	groupStreams := b.streamsInGroup(region, groupName)
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
