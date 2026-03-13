package timestreamquery

import (
	"errors"
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	scheduledQueryArnFormat = "arn:aws:timestream:%s:%s:scheduled-query/%s"
	defaultQueryState       = "ENABLED"
)

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = errors.New("ResourceNotFoundException")
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = errors.New("ConflictException")
)

// ScheduledQuery represents a Timestream scheduled query.
type ScheduledQuery struct {
	LastRunTime             time.Time
	CreationTime            time.Time
	Tags                    map[string]string
	NotificationTopicArn    string
	ScheduleExpression      string
	ExecutionRoleArn        string
	QueryString             string
	ErrorReportS3BucketName string
	TargetDatabase          string
	TargetTable             string
	State                   string
	Name                    string
	Arn                     string
}

// ScheduledQuerySummary is a reduced view used in list responses.
type ScheduledQuerySummary struct {
	Arn   string `json:"Arn"`
	Name  string `json:"Name"`
	State string `json:"State"`
}

// QueryResult represents the result of a Query call.
type QueryResult struct {
	QueryID     string
	QueryStatus string
	Rows        []map[string]any
	Columns     []map[string]any
}

// InMemoryBackend is the in-memory backend for the Timestream Query service.
type InMemoryBackend struct {
	mu               *lockmetrics.RWMutex
	scheduledQueries map[string]*ScheduledQuery
	tags             map[string]map[string]string
	queries          map[string]*QueryResult
	accountID        string
	region           string
	queriesOrder     []string
}

// NewInMemoryBackend creates a new in-memory Timestream Query backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		mu:               lockmetrics.New("timestreamquery"),
		scheduledQueries: make(map[string]*ScheduledQuery),
		queriesOrder:     []string{},
		tags:             make(map[string]map[string]string),
		queries:          make(map[string]*QueryResult),
		accountID:        accountID,
		region:           region,
	}
}

// AccountID returns the account ID for the backend.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the region for the backend.
func (b *InMemoryBackend) Region() string { return b.region }

// CreateScheduledQuery creates a new scheduled query.
func (b *InMemoryBackend) CreateScheduledQuery(
	name, queryString, scheduleExpression, executionRoleArn,
	notificationTopicArn, errorReportS3BucketName, targetDatabase, targetTable string,
	tags map[string]string,
) (*ScheduledQuery, error) {
	b.mu.Lock("CreateScheduledQuery")
	defer b.mu.Unlock()

	if _, exists := b.scheduledQueries[name]; exists {
		return nil, fmt.Errorf("%w: scheduled query %q already exists", ErrAlreadyExists, name)
	}

	arn := fmt.Sprintf(scheduledQueryArnFormat, b.region, b.accountID, name)

	sq := &ScheduledQuery{
		Arn:                     arn,
		Name:                    name,
		QueryString:             queryString,
		ScheduleExpression:      scheduleExpression,
		ExecutionRoleArn:        executionRoleArn,
		NotificationTopicArn:    notificationTopicArn,
		ErrorReportS3BucketName: errorReportS3BucketName,
		TargetDatabase:          targetDatabase,
		TargetTable:             targetTable,
		State:                   defaultQueryState,
		CreationTime:            time.Now(),
		Tags:                    tags,
	}

	b.scheduledQueries[name] = sq
	b.queriesOrder = append(b.queriesOrder, name)

	cp := *sq

	return &cp, nil
}

// DescribeScheduledQuery returns details of a scheduled query by ARN.
func (b *InMemoryBackend) DescribeScheduledQuery(arn string) (*ScheduledQuery, error) {
	b.mu.RLock("DescribeScheduledQuery")
	defer b.mu.RUnlock()

	for _, sq := range b.scheduledQueries {
		if sq.Arn == arn {
			cp := *sq

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("%w: scheduled query %q not found", ErrNotFound, arn)
}

// DeleteScheduledQuery deletes a scheduled query by ARN.
func (b *InMemoryBackend) DeleteScheduledQuery(arn string) error {
	b.mu.Lock("DeleteScheduledQuery")
	defer b.mu.Unlock()

	for name, sq := range b.scheduledQueries {
		if sq.Arn == arn {
			delete(b.scheduledQueries, name)
			delete(b.tags, arn)

			for i, n := range b.queriesOrder {
				if n == name {
					b.queriesOrder = append(b.queriesOrder[:i], b.queriesOrder[i+1:]...)

					break
				}
			}

			return nil
		}
	}

	return fmt.Errorf("%w: scheduled query %q not found", ErrNotFound, arn)
}

// ListScheduledQueries returns all scheduled queries sorted by name.
func (b *InMemoryBackend) ListScheduledQueries() []ScheduledQuerySummary {
	b.mu.RLock("ListScheduledQueries")
	defer b.mu.RUnlock()

	names := make([]string, 0, len(b.scheduledQueries))
	for name := range b.scheduledQueries {
		names = append(names, name)
	}

	sort.Strings(names)

	out := make([]ScheduledQuerySummary, 0, len(names))

	for _, name := range names {
		sq := b.scheduledQueries[name]
		out = append(out, ScheduledQuerySummary{
			Arn:   sq.Arn,
			Name:  sq.Name,
			State: sq.State,
		})
	}

	return out
}

// UpdateScheduledQuery updates the state of a scheduled query by ARN.
func (b *InMemoryBackend) UpdateScheduledQuery(arn, state string) error {
	b.mu.Lock("UpdateScheduledQuery")
	defer b.mu.Unlock()

	for _, sq := range b.scheduledQueries {
		if sq.Arn == arn {
			sq.State = state

			return nil
		}
	}

	return fmt.Errorf("%w: scheduled query %q not found", ErrNotFound, arn)
}

// ExecuteScheduledQuery marks a scheduled query as executed (simulated).
func (b *InMemoryBackend) ExecuteScheduledQuery(arn string) error {
	b.mu.Lock("ExecuteScheduledQuery")
	defer b.mu.Unlock()

	for _, sq := range b.scheduledQueries {
		if sq.Arn == arn {
			sq.LastRunTime = time.Now()

			return nil
		}
	}

	return fmt.Errorf("%w: scheduled query %q not found", ErrNotFound, arn)
}

// Query runs a query and returns an empty result set (simulated).
func (b *InMemoryBackend) Query(queryString string) *QueryResult {
	b.mu.Lock("Query")
	defer b.mu.Unlock()

	queryID := uuid.NewString()
	result := &QueryResult{
		QueryID:     queryID,
		QueryStatus: "SUCCEEDED",
		Rows:        []map[string]any{},
		Columns:     []map[string]any{},
	}

	b.queries[queryID] = result

	_ = queryString

	return result
}

// CancelQuery cancels a running query (simulated no-op if not found).
func (b *InMemoryBackend) CancelQuery(queryID string) error {
	b.mu.Lock("CancelQuery")
	defer b.mu.Unlock()

	if _, exists := b.queries[queryID]; !exists {
		return fmt.Errorf("%w: query %q not found", ErrNotFound, queryID)
	}

	delete(b.queries, queryID)

	return nil
}

// TagResource adds tags to a resource identified by its ARN.
func (b *InMemoryBackend) TagResource(arn string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if _, exists := b.tags[arn]; !exists {
		b.tags[arn] = make(map[string]string)
	}

	maps.Copy(b.tags[arn], tags)

	return nil
}

// UntagResource removes tags from a resource identified by its ARN.
func (b *InMemoryBackend) UntagResource(arn string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if t, exists := b.tags[arn]; exists {
		for _, k := range tagKeys {
			delete(t, k)
		}
	}

	return nil
}

// ListTagsForResource returns tags for a resource identified by its ARN.
func (b *InMemoryBackend) ListTagsForResource(arn string) ([]map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	t, exists := b.tags[arn]
	if !exists {
		return []map[string]string{}, nil
	}

	keys := make([]string, 0, len(t))
	for k := range t {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	out := make([]map[string]string, 0, len(keys))

	for _, k := range keys {
		out = append(out, map[string]string{"Key": k, "Value": t[k]})
	}

	return out, nil
}

// cloneScheduledQuery returns a deep copy of a scheduled query.
func cloneScheduledQuery(sq *ScheduledQuery) *ScheduledQuery {
	if sq == nil {
		return nil
	}

	cp := *sq

	if sq.Tags != nil {
		cp.Tags = make(map[string]string, len(sq.Tags))

		maps.Copy(cp.Tags, sq.Tags)
	}

	return &cp
}

// ListScheduledQueriesFull returns all scheduled queries with full details, sorted by name.
func (b *InMemoryBackend) ListScheduledQueriesFull() []*ScheduledQuery {
	b.mu.RLock("ListScheduledQueriesFull")
	defer b.mu.RUnlock()

	names := make([]string, 0, len(b.scheduledQueries))
	for name := range b.scheduledQueries {
		names = append(names, name)
	}

	sort.Strings(names)

	out := make([]*ScheduledQuery, 0, len(names))

	for _, name := range names {
		out = append(out, cloneScheduledQuery(b.scheduledQueries[name]))
	}

	return out
}
