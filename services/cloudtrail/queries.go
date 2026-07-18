package cloudtrail

import (
	"fmt"
	"sort"
	"time"
)

// StartQuery creates a new query against an event data store.
func (b *InMemoryBackend) StartQuery(queryString, edsARN, deliveryS3URI string) (*Query, error) {
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
		CreationTime:      time.Now().UTC(),
	}
	b.queries.Put(q)

	cp := *q

	return &cp, nil
}

// CancelQuery cancels a running query.
func (b *InMemoryBackend) CancelQuery(queryID string) (*Query, error) {
	b.mu.Lock("CancelQuery")
	defer b.mu.Unlock()

	if queryID == "" {
		return nil, fmt.Errorf("%w: QueryId is required", ErrValidation)
	}

	q, ok := b.queries.Get(queryID)
	if !ok {
		return nil, fmt.Errorf("%w: query %s not found", ErrQueryNotFound, queryID)
	}
	if q.QueryStatus == "FINISHED" || q.QueryStatus == "FAILED" || q.QueryStatus == "CANCELLED" {
		return nil, fmt.Errorf("%w: query %s is already in terminal state %s", ErrValidation, queryID, q.QueryStatus)
	}
	q.QueryStatus = "CANCELLED"
	cp := *q

	return &cp, nil
}

// DescribeQuery returns details about a specific query.
func (b *InMemoryBackend) DescribeQuery(queryID string) (*Query, error) {
	b.mu.RLock("DescribeQuery")
	defer b.mu.RUnlock()

	if queryID == "" {
		return nil, fmt.Errorf("%w: QueryId is required", ErrValidation)
	}

	q, ok := b.queries.Get(queryID)
	if !ok {
		return nil, fmt.Errorf("%w: query %s not found", ErrQueryNotFound, queryID)
	}
	cp := *q

	return &cp, nil
}

// GetQueryResults returns results for a completed query (stub returns empty rows).
func (b *InMemoryBackend) GetQueryResults(queryID string) (*Query, error) {
	b.mu.RLock("GetQueryResults")
	defer b.mu.RUnlock()

	q, ok := b.queries.Get(queryID)
	if !ok {
		return nil, fmt.Errorf("%w: query %s not found", ErrQueryNotFound, queryID)
	}
	cp := *q

	return &cp, nil
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
