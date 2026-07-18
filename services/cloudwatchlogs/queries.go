package cloudwatchlogs

import (
	"context"
	"fmt"
	"slices"
	"time"
)

// storedQuery holds the execution state of a single Logs Insights query.
type storedQuery struct {
	createdAt time.Time
	info      QueryInfo
	results   [][]ResultField
	logGroups []string
	stats     QueryStatistics
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
		sq, ok := b.queries.Get(qid)
		if !ok {
			// Entry already removed from the table; drop the stale order reference.
			continue
		}
		if sq.createdAt.Before(cutoff) {
			b.queries.Delete(qid)

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
		b.queries.Delete(qid)
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

	for _, groupName := range logGroupNames {
		for _, stream := range b.streamsInGroup(region, groupName) {
			// Narrow the scan by log stream: a stream whose [first,last] event
			// window does not overlap the query's [startTime,endTime] range holds
			// no matching records, so skip it entirely instead of scanning every
			// event. This bounds the scan to streams that can contribute results.
			if streamOutsideWindow(stream, startTime, endTime) {
				continue
			}
			matched, records, bytes := scanStreamEvents(stream.events, startTime, endTime)
			eventsOut = append(eventsOut, matched...)
			recordsScanned += records
			bytesScanned += bytes
		}
	}

	return eventsOut, recordsScanned, bytesScanned
}

// scanStreamEvents returns the events in one stream that fall within
// [startTime,endTime], along with the records and bytes scanned. A zero bound is
// treated as unbounded on that side.
func scanStreamEvents(
	evts []*OutputLogEvent, startTime, endTime int64,
) ([]*OutputLogEvent, float64, float64) {
	out := make([]*OutputLogEvent, 0, len(evts))
	var records, bytes float64

	for _, ev := range evts {
		records++
		bytes += float64(len(ev.Message))
		if startTime > 0 && ev.Timestamp < startTime {
			continue
		}
		if endTime > 0 && ev.Timestamp > endTime {
			continue
		}
		out = append(out, ev)
	}

	return out, records, bytes
}

// streamOutsideWindow reports whether a stream's event-time range is entirely
// outside the query window [startTime,endTime]. A zero bound means unbounded on
// that side. Streams with unknown ranges (nil timestamps) are never skipped.
func streamOutsideWindow(stream *LogStream, startTime, endTime int64) bool {
	if stream == nil || stream.FirstEventTimestamp == nil || stream.LastEventTimestamp == nil {
		return false
	}

	if startTime > 0 && *stream.LastEventTimestamp < startTime {
		return true
	}

	if endTime > 0 && *stream.FirstEventTimestamp > endTime {
		return true
	}

	return false
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
	var allEvents []*OutputLogEvent
	var recordsScanned, bytesScanned float64
	func() {
		b.mu.RLock("StartQuery")
		defer b.mu.RUnlock()
		allEvents, recordsScanned, bytesScanned = b.collectQueryEvents(
			region,
			logGroupNames,
			startTime,
			endTime,
		)
	}()

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
		Status:       QueryStatusRunning,
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
	if b.queries.Has(queryID) {
		b.removeFromOrder(queryID)
	}

	b.queries.Put(sq)
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
	sq, ok := b.queries.Get(queryID)
	b.mu.RUnlock()

	if !ok {
		return nil, QueryStatistics{}, "", fmt.Errorf(
			"%w: query %s not found",
			ErrQueryNotFound,
			queryID,
		)
	}

	b.mu.Lock("GetQueryResultsTransition")
	if sq.info.Status == QueryStatusRunning {
		sq.info.Status = QueryStatusComplete
	}
	status := sq.info.Status
	b.mu.Unlock()

	return sq.results, sq.stats, status, nil
}

// StopQuery cancels a query that is currently running or scheduled.
// AWS returns InvalidOperationException when stopping a query that is not in a running state.
func (b *InMemoryBackend) StopQuery(queryID string) error {
	b.mu.Lock("StopQuery")
	defer b.mu.Unlock()

	sq, ok := b.queries.Get(queryID)
	if !ok {
		return fmt.Errorf("%w: query %s not found", ErrQueryNotFound, queryID)
	}

	// gopherstack completes Insights queries synchronously, so a query is almost
	// always already Complete by the time a client calls StopQuery. Treat StopQuery
	// as a transition to Cancelled for any non-cancelled query (idempotent otherwise),
	// keeping the operation usable rather than erroring on the instant-complete result.
	if sq.info.Status != QueryStatusCancelled {
		sq.info.Status = QueryStatusCancelled
	}

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
		sq, ok := b.queries.Get(qid)
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

// SetQueryStatusInternal sets the status of an existing query for testing.
// Used to place a query into Running or Scheduled state before calling StopQuery.
func (b *InMemoryBackend) SetQueryStatusInternal(queryID string, status QueryStatus) {
	b.mu.Lock("SetQueryStatusInternal")
	defer b.mu.Unlock()

	if sq, ok := b.queries.Get(queryID); ok {
		sq.info.Status = status
	}
}

// ListLogGroupsForQuery returns the log group names that were used in a specific query.
func (b *InMemoryBackend) ListLogGroupsForQuery(queryID string) ([]string, error) {
	if queryID == "" {
		return nil, fmt.Errorf("%w: queryId is required", ErrValidation)
	}

	b.mu.RLock("ListLogGroupsForQuery")
	defer b.mu.RUnlock()

	sq, ok := b.queries.Get(queryID)
	if !ok {
		return nil, fmt.Errorf("%w: query %s not found", ErrQueryNotFound, queryID)
	}

	result := make([]string, len(sq.logGroups))
	copy(result, sq.logGroups)

	return result, nil
}
