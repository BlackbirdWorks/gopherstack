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
	scheduledQueries map[string]*ScheduledQuery // keyed by name
	arnIndex         map[string]string          // ARN → name
	queries          map[string]*QueryResult
	accountID        string
	region           string
}

// NewInMemoryBackend creates a new in-memory Timestream Query backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		mu:               lockmetrics.New("timestreamquery"),
		scheduledQueries: make(map[string]*ScheduledQuery),
		arnIndex:         make(map[string]string),
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
		Tags:                    make(map[string]string),
	}

	if tags != nil {
		maps.Copy(sq.Tags, tags)
	}

	b.scheduledQueries[name] = sq
	b.arnIndex[arn] = name

	cp := *sq

	return &cp, nil
}

// DescribeScheduledQuery returns details of a scheduled query by ARN.
func (b *InMemoryBackend) DescribeScheduledQuery(arnStr string) (*ScheduledQuery, error) {
	b.mu.RLock("DescribeScheduledQuery")
	defer b.mu.RUnlock()

	sq, err := b.lookupByARN(arnStr)
	if err != nil {
		return nil, err
	}

	cp := *sq

	return &cp, nil
}

// DeleteScheduledQuery deletes a scheduled query by ARN.
func (b *InMemoryBackend) DeleteScheduledQuery(arnStr string) error {
	b.mu.Lock("DeleteScheduledQuery")
	defer b.mu.Unlock()

	name, ok := b.arnIndex[arnStr]
	if !ok {
		return fmt.Errorf("%w: scheduled query %q not found", ErrNotFound, arnStr)
	}

	delete(b.scheduledQueries, name)
	delete(b.arnIndex, arnStr)

	return nil
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
func (b *InMemoryBackend) UpdateScheduledQuery(arnStr, state string) error {
	b.mu.Lock("UpdateScheduledQuery")
	defer b.mu.Unlock()

	sq, err := b.lookupByARN(arnStr)
	if err != nil {
		return err
	}

	sq.State = state

	return nil
}

// ExecuteScheduledQuery marks a scheduled query as executed at the given invocation time.
func (b *InMemoryBackend) ExecuteScheduledQuery(arnStr string, invocationTime time.Time) error {
	b.mu.Lock("ExecuteScheduledQuery")
	defer b.mu.Unlock()

	sq, err := b.lookupByARN(arnStr)
	if err != nil {
		return err
	}

	sq.LastRunTime = invocationTime

	return nil
}

// Query runs a query and returns an empty result set (simulated).
// The query string is accepted but not evaluated; this is an in-memory simulation
// that returns empty results regardless of the query content.
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

	// queryString is intentionally not evaluated — this is a simulation.
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

// lookupByARN finds a scheduled query by ARN using the ARN index. Must be called with the lock held.
// The double lookup (ARN index → name, then name → struct) is intentional: the ARN index
// may briefly diverge from scheduledQueries only if there is a bug, so the second check
// is a defensive guard against index inconsistency.
func (b *InMemoryBackend) lookupByARN(arnStr string) (*ScheduledQuery, error) {
	name, ok := b.arnIndex[arnStr]
	if !ok {
		return nil, fmt.Errorf("%w: scheduled query %q not found", ErrNotFound, arnStr)
	}

	sq, ok := b.scheduledQueries[name]
	if !ok {
		return nil, fmt.Errorf("%w: scheduled query %q not found", ErrNotFound, arnStr)
	}

	return sq, nil
}

// TagResource adds tags to a resource identified by its ARN.
func (b *InMemoryBackend) TagResource(arn string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	sq, err := b.lookupByARN(arn)
	if err != nil {
		return err
	}

	maps.Copy(sq.Tags, tags)

	return nil
}

// UntagResource removes tags from a resource identified by its ARN.
func (b *InMemoryBackend) UntagResource(arn string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	sq, err := b.lookupByARN(arn)
	if err != nil {
		return err
	}

	for _, k := range tagKeys {
		delete(sq.Tags, k)
	}

	return nil
}

// ListTagsForResource returns tags for a resource identified by its ARN.
func (b *InMemoryBackend) ListTagsForResource(arn string) ([]map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	sq, err := b.lookupByARN(arn)
	if err != nil {
		return nil, err
	}

	keys := make([]string, 0, len(sq.Tags))
	for k := range sq.Tags {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	out := make([]map[string]string, 0, len(keys))

	for _, k := range keys {
		out = append(out, map[string]string{"Key": k, "Value": sq.Tags[k]})
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
