package cloudtrail

import (
	"fmt"
	"math"
	"sort"
	"time"
)

// StartQuery creates a new query against an event data store. queryAlias is
// optional (StartQuery accepts QueryStatement alone); when set it is stored
// so a later DescribeQuery can resolve "the last query run for the alias" --
// see DescribeQueryByAlias.
func (b *InMemoryBackend) StartQuery(queryString, edsARN, deliveryS3URI, queryAlias string) (*Query, error) {
	b.mu.Lock("StartQuery")
	defer b.mu.Unlock()

	if queryString == "" {
		return nil, fmt.Errorf("%w: QueryStatement is required", ErrValidation)
	}

	b.queryCounter++
	qid := fmt.Sprintf("query-%06d", b.queryCounter)
	q := &Query{
		QueryID:           qid,
		EventDataStoreARN: edsARN,
		QueryString:       queryString,
		QueryStatus:       "QUEUED",
		DeliveryS3URI:     deliveryS3URI,
		QueryAlias:        queryAlias,
		CreationTime:      time.Now().UTC(),
	}
	b.queries.Put(q)

	cp := *q

	return &cp, nil
}

// CancelQuery cancels a running (non-terminal) query.
func (b *InMemoryBackend) CancelQuery(queryID string) (*Query, error) {
	b.mu.Lock("CancelQuery")
	defer b.mu.Unlock()

	if queryID == "" {
		return nil, fmt.Errorf("%w: QueryId is required", ErrValidation)
	}

	q, ok := b.queries.Get(queryID)
	if !ok {
		return nil, fmt.Errorf("%w: query %s not found", ErrQueryIDNotFound, queryID)
	}
	if q.QueryStatus == "FINISHED" || q.QueryStatus == "FAILED" ||
		q.QueryStatus == "CANCELLED" || q.QueryStatus == "TIMED_OUT" {
		return nil, fmt.Errorf("%w: query %s is already in terminal state %s", ErrQueryInactive, queryID, q.QueryStatus)
	}
	q.QueryStatus = "CANCELLED"
	cp := *q

	return &cp, nil
}

// DescribeQuery returns details about a specific query, materializing (lazily
// executing) it first if it hasn't been read yet. See materializeQueryLocked.
func (b *InMemoryBackend) DescribeQuery(queryID string) (*Query, error) {
	b.mu.Lock("DescribeQuery")
	defer b.mu.Unlock()

	if queryID == "" {
		return nil, fmt.Errorf("%w: QueryId is required", ErrValidation)
	}

	q, ok := b.queries.Get(queryID)
	if !ok {
		return nil, fmt.Errorf("%w: query %s not found", ErrQueryIDNotFound, queryID)
	}
	b.materializeQueryLocked(q)
	cp := *q

	return &cp, nil
}

// DescribeQueryByAlias returns details about the last query started under
// alias, per DescribeQueryInput's documented alternative to QueryId:
// "Specifying the QueryAlias parameter returns information about the last
// query run for the alias." Materializes the query first if unread, same as
// DescribeQuery. b.queriesByAlias groups queries in StartQuery call order, so
// the last element of the group is the most recently started one.
func (b *InMemoryBackend) DescribeQueryByAlias(alias string) (*Query, error) {
	b.mu.Lock("DescribeQueryByAlias")
	defer b.mu.Unlock()

	if alias == "" {
		return nil, fmt.Errorf("%w: QueryAlias is required", ErrValidation)
	}

	matches := b.queriesByAlias.Get(alias)
	if len(matches) == 0 {
		return nil, fmt.Errorf("%w: alias %s not found", ErrQueryIDNotFound, alias)
	}

	q := matches[len(matches)-1]
	b.materializeQueryLocked(q)
	cp := *q

	return &cp, nil
}

// GetQueryResults returns results for a query, materializing (lazily
// executing) it first if it hasn't been read yet. See materializeQueryLocked.
func (b *InMemoryBackend) GetQueryResults(queryID string) (*Query, error) {
	b.mu.Lock("GetQueryResults")
	defer b.mu.Unlock()

	q, ok := b.queries.Get(queryID)
	if !ok {
		return nil, fmt.Errorf("%w: query %s not found", ErrQueryIDNotFound, queryID)
	}
	b.materializeQueryLocked(q)
	cp := *q

	return &cp, nil
}

// materializeQueryLocked lazily executes a QUEUED query's SQL against the
// backend's recorded events (see query_exec.go), storing the resulting rows
// and scan statistics on q and transitioning it to FINISHED. Queries are
// deliberately left un-executed by StartQuery (mirroring AWS's async
// QUEUED->RUNNING->FINISHED lifecycle) so a query started and immediately
// cancelled is still cancellable -- only the first read (GetQueryResults or
// DescribeQuery) triggers execution. A no-op for queries already in a
// terminal state (e.g. CANCELLED before ever being read).
// Must be called with b.mu held for writing.
func (b *InMemoryBackend) materializeQueryLocked(q *Query) {
	if q.QueryStatus != "QUEUED" {
		return
	}

	start := time.Now()
	rows, stats := executeLakeQuery(q.QueryString, b.events)
	q.QueryResultRows = rows
	q.EventsScanned = stats.eventsScanned
	q.EventsMatched = stats.eventsMatched
	q.BytesScanned = stats.bytesScanned
	q.ExecutionTimeInMillis = clampToInt32Millis(time.Since(start).Milliseconds())
	q.QueryStatus = "FINISHED"
}

// clampToInt32Millis narrows a millisecond duration to int32
// (ExecutionTimeInMillis mirrors the real int32 QueryStatisticsForDescribeQuery
// field): an in-memory query never takes anywhere near ~24.8 days, but an
// unchecked int64->int32 conversion would silently wrap on that theoretical
// input, so this clamps explicitly instead.
func clampToInt32Millis(ms int64) int32 {
	if ms > math.MaxInt32 {
		return math.MaxInt32
	}
	if ms < 0 {
		return 0
	}

	return int32(ms)
}

// ListQueries returns all queries.
func (b *InMemoryBackend) ListQueries() []*Query {
	b.mu.RLock("ListQueries")
	defer b.mu.RUnlock()

	all := b.queries.All()
	list := make([]*Query, 0, len(all))
	for _, q := range all {
		cp := *q
		list = append(list, &cp)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].QueryID < list[j].QueryID })

	return list
}

// GenerateQuery synthesizes a CloudTrail Lake SQL query statement from a
// natural-language prompt against the given event data stores.
func (b *InMemoryBackend) GenerateQuery(eventDataStores []string, prompt string) *GeneratedQuery {
	b.mu.Lock("GenerateQuery")
	defer b.mu.Unlock()

	b.queryCounter++
	alias := fmt.Sprintf("query-%06d", b.queryCounter)
	stmt := fmt.Sprintf("SELECT * FROM %s -- generated from prompt: %s", eventDataStores[0], prompt)

	return &GeneratedQuery{
		QueryStatement: stmt,
		QueryAlias:     alias,
		OwnerAccountID: b.accountID,
	}
}

// SearchSampleQueries returns empty sample queries (stub).
func (b *InMemoryBackend) SearchSampleQueries() []map[string]any {
	return []map[string]any{}
}
