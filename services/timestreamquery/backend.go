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
	scheduledQueryArnFormat     = "arn:aws:timestream:%s:%s:scheduled-query/%s"
	pricingModelBytesScanned    = "BYTES_SCANNED"
	pricingModelComputeUnits    = "COMPUTE_UNITS"
	scheduledQueryStateEnabled  = "ENABLED"
	scheduledQueryStateDisabled = "DISABLED"
)

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = errors.New("ResourceNotFoundException")
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = errors.New("ConflictException")
	// ErrValidation is returned when request input fails validation.
	ErrValidation = errors.New("ValidationException")
)

// ScheduledQuery represents a Timestream scheduled query.
type ScheduledQuery struct {
	LastRunTime             time.Time         `json:"last_run_time"`
	CreationTime            time.Time         `json:"creation_time"`
	Tags                    map[string]string `json:"tags"`
	NotificationTopicArn    string            `json:"notification_topic_arn"`
	ScheduleExpression      string            `json:"schedule_expression"`
	ExecutionRoleArn        string            `json:"execution_role_arn"`
	QueryString             string            `json:"query_string"`
	ErrorReportS3BucketName string            `json:"error_report_s3_bucket_name"`
	TargetDatabase          string            `json:"target_database"`
	TargetTable             string            `json:"target_table"`
	State                   string            `json:"state"`
	Name                    string            `json:"name"`
	Arn                     string            `json:"arn"`
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

// AccountSettings holds the account-level settings for Timestream Query.
type AccountSettings struct {
	MaxQueryTCU       *int32
	QueryPricingModel string
}

// PrepareQueryResult holds the result of a PrepareQuery call.
type PrepareQueryResult struct {
	QueryString string
	Columns     []map[string]any
	Parameters  []map[string]any
}

// maxRetainedQueries bounds the queries map so a long-running instance cannot
// leak memory through repeated Query calls. The map only stores queries that
// were not explicitly cancelled; in real Timestream, Query results are
// transient anyway.
const maxRetainedQueries = 10000

// InMemoryBackend is the in-memory backend for the Timestream Query service.
type InMemoryBackend struct {
	mu               *lockmetrics.RWMutex
	scheduledQueries map[string]*ScheduledQuery // keyed by name
	arnIndex         map[string]string          // ARN → name
	queries          map[string]*QueryResult
	accountSettings  AccountSettings
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
		accountSettings:  AccountSettings{QueryPricingModel: pricingModelBytesScanned},
		accountID:        accountID,
		region:           region,
	}
}

// Reset clears all backend state, returning it to a freshly initialised condition.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.scheduledQueries = make(map[string]*ScheduledQuery)
	b.arnIndex = make(map[string]string)
	b.queries = make(map[string]*QueryResult)
	b.accountSettings = AccountSettings{QueryPricingModel: pricingModelBytesScanned}
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
		State:                   scheduledQueryStateEnabled,
		CreationTime:            time.Now(),
		Tags:                    make(map[string]string),
	}

	if tags != nil {
		maps.Copy(sq.Tags, tags)
	}

	b.scheduledQueries[name] = sq
	b.arnIndex[arn] = name

	return cloneScheduledQuery(sq), nil
}

// DescribeScheduledQuery returns details of a scheduled query by ARN.
func (b *InMemoryBackend) DescribeScheduledQuery(arnStr string) (*ScheduledQuery, error) {
	b.mu.RLock("DescribeScheduledQuery")
	defer b.mu.RUnlock()

	sq, err := b.lookupByARN(arnStr)
	if err != nil {
		return nil, err
	}

	return cloneScheduledQuery(sq), nil
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
// Only ENABLED and DISABLED are valid states.
func (b *InMemoryBackend) UpdateScheduledQuery(arnStr, state string) error {
	if state != scheduledQueryStateEnabled && state != scheduledQueryStateDisabled {
		return fmt.Errorf("%w: State must be %s or %s",
			ErrValidation, scheduledQueryStateEnabled, scheduledQueryStateDisabled)
	}

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

	// Evict arbitrary entries when over the cap so a long-running instance
	// cannot leak memory through repeated Query calls. Map iteration order
	// is random in Go; for the simulator this is acceptable since only
	// CancelQuery interacts with stored results.
	// Evict before inserting so the new query is never randomly chosen for
	// eviction — callers that immediately CancelQuery on the returned ID
	// must not see a spurious ErrNotFound.
	for len(b.queries) >= maxRetainedQueries {
		for id := range b.queries {
			delete(b.queries, id)

			break
		}
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

// DescribeAccountSettings returns the current account-level settings.
func (b *InMemoryBackend) DescribeAccountSettings() AccountSettings {
	b.mu.RLock("DescribeAccountSettings")
	defer b.mu.RUnlock()

	return b.accountSettings
}

// PrepareQuery validates a query string and returns its column and parameter metadata.
// This is a simulated implementation: the query string is accepted but not evaluated.
func (b *InMemoryBackend) PrepareQuery(queryString string, _ bool) (*PrepareQueryResult, error) {
	if queryString == "" {
		return nil, fmt.Errorf("%w: QueryString is required", ErrValidation)
	}

	return &PrepareQueryResult{
		QueryString: queryString,
		Columns:     []map[string]any{},
		Parameters:  []map[string]any{},
	}, nil
}

// isValidPricingModel reports whether the given pricing model string is recognised.
func isValidPricingModel(model string) bool {
	return model == pricingModelBytesScanned || model == pricingModelComputeUnits
}

// UpdateAccountSettings updates the account-level settings and returns the new state.
// Only non-empty queryPricingModel and non-nil maxQueryTCU values are applied;
// omitted fields preserve their current values.
func (b *InMemoryBackend) UpdateAccountSettings(queryPricingModel string, maxQueryTCU *int32) (AccountSettings, error) {
	b.mu.Lock("UpdateAccountSettings")
	defer b.mu.Unlock()

	if queryPricingModel != "" {
		if !isValidPricingModel(queryPricingModel) {
			return AccountSettings{}, fmt.Errorf(
				"%w: invalid QueryPricingModel %q, must be one of BYTES_SCANNED or COMPUTE_UNITS",
				ErrValidation,
				queryPricingModel,
			)
		}

		b.accountSettings.QueryPricingModel = queryPricingModel
	}

	if maxQueryTCU != nil {
		if *maxQueryTCU <= 0 {
			return AccountSettings{}, fmt.Errorf("%w: MaxQueryTCU must be a positive integer", ErrValidation)
		}

		b.accountSettings.MaxQueryTCU = maxQueryTCU
	}

	return b.accountSettings, nil
}

// AddScheduledQueryInternal is a test-only seed helper that stores a scheduled query directly,
// bypassing normal validation. It is used to pre-populate backend state in tests.
func (b *InMemoryBackend) AddScheduledQueryInternal(sq *ScheduledQuery) {
	b.mu.Lock("AddScheduledQueryInternal")
	defer b.mu.Unlock()

	if sq.Tags == nil {
		sq.Tags = make(map[string]string)
	}

	b.scheduledQueries[sq.Name] = sq
	b.arnIndex[sq.Arn] = sq.Name
}
