package timestreamquery

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	scheduledQueryArnFormat     = "arn:aws:timestream:%s:%s:scheduled-query/%s"
	pricingModelBytesScanned    = "BYTES_SCANNED"
	pricingModelComputeUnits    = "COMPUTE_UNITS"
	scheduledQueryStateEnabled  = "ENABLED"
	scheduledQueryStateDisabled = "DISABLED"
	queryProgressComplete       = 100.0
)

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = errors.New("ResourceNotFoundException")
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = errors.New("ConflictException")
	// ErrValidation is returned when request input fails validation.
	ErrValidation = errors.New("ValidationException")
)

// regionContextKey is the context key under which the per-request AWS region is stored.
type regionContextKey struct{}

// getRegion extracts the region from ctx, falling back to defaultRegion when unset.
// Every backend operation resolves the caller's region from the request context and
// operates only on that region's nested store.
func getRegion(ctx context.Context, defaultRegion string) string {
	if r, ok := ctx.Value(regionContextKey{}).(string); ok && r != "" {
		return r
	}

	return defaultRegion
}

// regionFromARN extracts the region component (index 3) from an AWS ARN
// (arn:partition:service:region:account:resource), falling back to defaultRegion.
func regionFromARN(resourceARN, defaultRegion string) string {
	parts := strings.Split(resourceARN, ":")
	const regionIndex = 3
	if len(parts) > regionIndex && parts[regionIndex] != "" {
		return parts[regionIndex]
	}

	return defaultRegion
}

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

// QueryResult represents the result of a Query call (typed).
type QueryResult struct {
	QueryID     string
	Rows        []Row
	Columns     []ColumnInfo
	Insights    QueryInsightsResponse
	QueryStatus QueryStatusDetail
}

// AccountSettings holds the account-level settings for Timestream Query.
type AccountSettings struct {
	LastUpdatedTime   *time.Time
	MaxQueryTCU       *int32
	QueryCompute      *QueryCompute
	QueryPricingModel string
}

// PrepareQueryResult holds the result of a PrepareQuery call (typed).
type PrepareQueryResult struct {
	QueryString string
	Columns     []ColumnInfo
	Parameters  []ColumnInfo
}

// maxRetainedQueries bounds the queries map so a long-running instance cannot
// leak memory through repeated Query calls. The map only stores queries that
// were not explicitly cancelled; in real Timestream, Query results are
// transient anyway.
const maxRetainedQueries = 10000

// InMemoryBackend is the in-memory backend for the Timestream Query service.
type InMemoryBackend struct {
	mu               *lockmetrics.RWMutex
	scheduledQueries map[string]map[string]*ScheduledQuery // region → name → ScheduledQuery
	arnIndex         map[string]map[string]string          // region → ARN → name
	queries          map[string]*QueryResult               // UUID-keyed; not region-isolated
	accountSettings  map[string]AccountSettings            // region → settings
	clientTokens     *clientTokenCache
	pageStore        *nextTokenStore
	accountID        string
	defaultRegion    string
}

// NewInMemoryBackend creates a new in-memory Timestream Query backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		mu:               lockmetrics.New("timestreamquery"),
		scheduledQueries: make(map[string]map[string]*ScheduledQuery),
		arnIndex:         make(map[string]map[string]string),
		queries:          make(map[string]*QueryResult),
		accountSettings:  make(map[string]AccountSettings),
		clientTokens:     newClientTokenCache(),
		pageStore:        newNextTokenStore(),
		accountID:        accountID,
		defaultRegion:    region,
	}
}

// Reset clears all backend state, returning it to a freshly initialised condition.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.scheduledQueries = make(map[string]map[string]*ScheduledQuery)
	b.arnIndex = make(map[string]map[string]string)
	b.queries = make(map[string]*QueryResult)
	b.accountSettings = make(map[string]AccountSettings)
	b.clientTokens = newClientTokenCache()
	b.pageStore = newNextTokenStore()
}

// AccountID returns the account ID for the backend.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the default region for the backend.
func (b *InMemoryBackend) Region() string { return b.defaultRegion }

// The *Store helpers return the per-region inner map, lazily creating it.
// Callers must hold b.mu.

func (b *InMemoryBackend) scheduledQueriesStore(region string) map[string]*ScheduledQuery {
	if b.scheduledQueries[region] == nil {
		b.scheduledQueries[region] = make(map[string]*ScheduledQuery)
	}

	return b.scheduledQueries[region]
}

func (b *InMemoryBackend) arnIndexStore(region string) map[string]string {
	if b.arnIndex[region] == nil {
		b.arnIndex[region] = make(map[string]string)
	}

	return b.arnIndex[region]
}

// defaultAccountSettings returns the initial state for a region's account settings.
func defaultAccountSettings() AccountSettings {
	return AccountSettings{QueryPricingModel: pricingModelComputeUnits}
}

// accountSettingsFor returns the account settings for region, initialising defaults if absent.
// Callers must hold b.mu.
func (b *InMemoryBackend) accountSettingsFor(region string) AccountSettings {
	if s, ok := b.accountSettings[region]; ok {
		return s
	}

	return defaultAccountSettings()
}

// CreateScheduledQuery creates a new scheduled query.
func (b *InMemoryBackend) CreateScheduledQuery(
	ctx context.Context,
	name, queryString, scheduleExpression, executionRoleArn,
	notificationTopicArn, errorReportS3BucketName, targetDatabase, targetTable string,
	tags map[string]string,
) (*ScheduledQuery, error) {
	if err := validateScheduleExpression(scheduleExpression); err != nil {
		return nil, err
	}

	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("CreateScheduledQuery")
	defer b.mu.Unlock()

	sqs := b.scheduledQueriesStore(region)
	if _, exists := sqs[name]; exists {
		return nil, fmt.Errorf("%w: scheduled query %q already exists", ErrAlreadyExists, name)
	}

	arnStr := fmt.Sprintf(scheduledQueryArnFormat, region, b.accountID, name)

	sq := &ScheduledQuery{
		Arn:                     arnStr,
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

	sqs[name] = sq
	b.arnIndexStore(region)[arnStr] = name

	return cloneScheduledQuery(sq), nil
}

// DescribeScheduledQuery returns details of a scheduled query by ARN.
// The region is resolved from the ARN itself, with context region as fallback.
func (b *InMemoryBackend) DescribeScheduledQuery(ctx context.Context, arnStr string) (*ScheduledQuery, error) {
	region := regionFromARN(arnStr, getRegion(ctx, b.defaultRegion))

	b.mu.RLock("DescribeScheduledQuery")
	defer b.mu.RUnlock()

	sq, err := b.lookupByARN(region, arnStr)
	if err != nil {
		return nil, err
	}

	return cloneScheduledQuery(sq), nil
}

// DeleteScheduledQuery deletes a scheduled query by ARN.
// The region is resolved from the ARN itself, with context region as fallback.
func (b *InMemoryBackend) DeleteScheduledQuery(ctx context.Context, arnStr string) error {
	region := regionFromARN(arnStr, getRegion(ctx, b.defaultRegion))

	b.mu.Lock("DeleteScheduledQuery")
	defer b.mu.Unlock()

	idx := b.arnIndex[region]
	name, ok := idx[arnStr]
	if !ok {
		return fmt.Errorf("%w: scheduled query %q not found", ErrNotFound, arnStr)
	}

	delete(b.scheduledQueries[region], name)
	delete(idx, arnStr)

	return nil
}

// ListScheduledQueries returns all scheduled queries for the request region sorted by name.
func (b *InMemoryBackend) ListScheduledQueries(ctx context.Context) []ScheduledQuerySummary {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("ListScheduledQueries")
	defer b.mu.RUnlock()

	sqs := b.scheduledQueries[region]
	names := collections.SortedKeys(sqs)

	out := make([]ScheduledQuerySummary, 0, len(names))

	for _, name := range names {
		sq := sqs[name]
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
// The region is resolved from the ARN itself, with context region as fallback.
func (b *InMemoryBackend) UpdateScheduledQuery(ctx context.Context, arnStr, state string) error {
	if state != scheduledQueryStateEnabled && state != scheduledQueryStateDisabled {
		return fmt.Errorf("%w: State must be %s or %s",
			ErrValidation, scheduledQueryStateEnabled, scheduledQueryStateDisabled)
	}

	region := regionFromARN(arnStr, getRegion(ctx, b.defaultRegion))

	b.mu.Lock("UpdateScheduledQuery")
	defer b.mu.Unlock()

	sq, err := b.lookupByARN(region, arnStr)
	if err != nil {
		return err
	}

	sq.State = state

	return nil
}

// ExecuteScheduledQuery marks a scheduled query as executed at the given invocation time.
// The region is resolved from the ARN itself, with context region as fallback.
func (b *InMemoryBackend) ExecuteScheduledQuery(ctx context.Context, arnStr string, invocationTime time.Time) error {
	region := regionFromARN(arnStr, getRegion(ctx, b.defaultRegion))

	b.mu.Lock("ExecuteScheduledQuery")
	defer b.mu.Unlock()

	sq, err := b.lookupByARN(region, arnStr)
	if err != nil {
		return err
	}

	sq.LastRunTime = invocationTime

	return nil
}

// QueryOptions holds parameters for a Query call.
type QueryOptions struct {
	QueryString  string
	ClientToken  string
	NextToken    string
	InsightsMode string
	MaxRows      int32
}

// QueryPage is the page-level result returned by QueryWithOptions.
type QueryPage struct {
	Insights    *QueryInsightsResponse
	QueryID     string
	NextToken   string
	Rows        []Row
	Columns     []ColumnInfo
	QueryStatus QueryStatusDetail
}

// QueryWithOptions executes a query with full options support (clientToken, pagination).
// ctx is accepted for interface consistency; query results are not region-isolated.
func (b *InMemoryBackend) QueryWithOptions(_ context.Context, opts QueryOptions) (*QueryPage, error) {
	// Validate MaxRows.
	maxRows, err := validateMaxRows(opts.MaxRows)
	if err != nil {
		return nil, err
	}

	// Continuation page from NextToken.
	if opts.NextToken != "" {
		queryID, rows, cols, nextTok, resolveErr := b.pageStore.resolve(opts.NextToken, maxRows)
		if resolveErr != nil {
			return nil, resolveErr
		}
		scanned := estimateBytesScanned(len(rows), len(cols))

		return &QueryPage{
			QueryID: queryID,
			Rows:    rows,
			Columns: cols,
			QueryStatus: QueryStatusDetail{
				ProgressPercentage:     queryProgressComplete,
				CumulativeBytesScanned: scanned,
				CumulativeBytesMetered: estimateBytesMetered(scanned),
			},
			NextToken: nextTok,
		}, nil
	}

	// ClientToken idempotency check.
	if opts.ClientToken != "" {
		b.clientTokens.sweep()
		if cachedID := b.clientTokens.get(opts.ClientToken); cachedID != "" {
			// Return the first page of the cached result.
			page, nextTok, resumeErr := b.resumeFirstPage(cachedID, maxRows)
			if resumeErr == nil {
				return &QueryPage{QueryID: cachedID, Rows: page, NextToken: nextTok}, nil
			}
			// If cached result is gone, fall through and re-execute.
		}
	}

	// New query execution — infer schema from SQL.
	cols, _ := inferColumnsFromSQL(opts.QueryString)
	rows := []Row{} // Simulator: always empty (no live Timestream Write data available).

	queryID := newQueryID()
	scanned := estimateBytesScanned(len(rows), len(cols))

	result := &QueryResult{
		QueryID: queryID,
		QueryStatus: QueryStatusDetail{
			ProgressPercentage:     queryProgressComplete,
			CumulativeBytesScanned: scanned,
			CumulativeBytesMetered: estimateBytesMetered(scanned),
		},
		Rows:     rows,
		Columns:  cols,
		Insights: buildQueryInsightsResponse(rows, cols),
	}

	b.mu.Lock("QueryWithOptions")
	// Evict to stay under cap.
	for len(b.queries) >= maxRetainedQueries {
		for id := range b.queries {
			delete(b.queries, id)

			break
		}
	}
	b.queries[queryID] = result
	b.mu.Unlock()

	if opts.ClientToken != "" {
		b.clientTokens.set(opts.ClientToken, queryID)
	}

	page, nextTok := b.pageStore.store(queryID, rows, cols, maxRows)

	var insights *QueryInsightsResponse
	if opts.InsightsMode != "DISABLED" {
		ins := result.Insights
		insights = &ins
	}

	return &QueryPage{
		QueryID:     queryID,
		Rows:        page,
		Columns:     cols,
		QueryStatus: result.QueryStatus,
		Insights:    insights,
		NextToken:   nextTok,
	}, nil
}

// resumeFirstPage retrieves the first page of a previously-stored query result.
func (b *InMemoryBackend) resumeFirstPage(queryID string, maxRows int) ([]Row, string, error) {
	b.pageStore.mu.Lock()
	defer b.pageStore.mu.Unlock()
	r, ok := b.pageStore.results[queryID]
	if !ok {
		return nil, "", fmt.Errorf("%w: query %q not found in page store", ErrNotFound, queryID)
	}
	r.offset = 0
	page, next := r.nextPage(maxRows)

	return page, next, nil
}

// Query runs a query and returns a result (legacy path, calls QueryWithOptions).
// ctx is accepted for interface consistency; query results are not region-isolated.
func (b *InMemoryBackend) Query(_ context.Context, queryString string) *QueryResult {
	b.mu.Lock("Query")
	defer b.mu.Unlock()

	queryID := newQueryID()
	cols, _ := inferColumnsFromSQL(queryString)
	result := &QueryResult{
		QueryID: queryID,
		QueryStatus: QueryStatusDetail{
			ProgressPercentage:     queryProgressComplete,
			CumulativeBytesScanned: 0,
			CumulativeBytesMetered: 0,
		},
		Rows:    []Row{},
		Columns: cols,
	}

	for len(b.queries) >= maxRetainedQueries {
		for id := range b.queries {
			delete(b.queries, id)

			break
		}
	}

	b.queries[queryID] = result

	return result
}

// CancelQuery cancels a running query (simulated no-op if not found).
// ctx is accepted for interface consistency; query results are not region-isolated.
func (b *InMemoryBackend) CancelQuery(_ context.Context, queryID string) error {
	b.mu.Lock("CancelQuery")
	defer b.mu.Unlock()

	if _, exists := b.queries[queryID]; !exists {
		// Real Timestream returns ValidationException for unknown IDs (gap #9).
		return fmt.Errorf("%w: invalid identifier: query %q not found", ErrValidation, queryID)
	}

	delete(b.queries, queryID)

	return nil
}

// lookupByARN finds a scheduled query by ARN using the region's ARN index.
// Must be called with the lock held.
func (b *InMemoryBackend) lookupByARN(region, arnStr string) (*ScheduledQuery, error) {
	idx := b.arnIndex[region]
	name, ok := idx[arnStr]
	if !ok {
		return nil, fmt.Errorf("%w: scheduled query %q not found", ErrNotFound, arnStr)
	}

	sq, ok := b.scheduledQueries[region][name]
	if !ok {
		return nil, fmt.Errorf("%w: scheduled query %q not found", ErrNotFound, arnStr)
	}

	return sq, nil
}

// TagResource adds tags to a resource identified by its ARN.
// The region is resolved from the ARN itself, with context region as fallback.
func (b *InMemoryBackend) TagResource(ctx context.Context, arnStr string, tags map[string]string) error {
	region := regionFromARN(arnStr, getRegion(ctx, b.defaultRegion))

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	sq, err := b.lookupByARN(region, arnStr)
	if err != nil {
		return err
	}

	maps.Copy(sq.Tags, tags)

	return nil
}

// UntagResource removes tags from a resource identified by its ARN.
// The region is resolved from the ARN itself, with context region as fallback.
func (b *InMemoryBackend) UntagResource(ctx context.Context, arnStr string, tagKeys []string) error {
	region := regionFromARN(arnStr, getRegion(ctx, b.defaultRegion))

	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	sq, err := b.lookupByARN(region, arnStr)
	if err != nil {
		return err
	}

	for _, k := range tagKeys {
		delete(sq.Tags, k)
	}

	return nil
}

// ListTagsForResource returns tags for a resource identified by its ARN.
// The region is resolved from the ARN itself, with context region as fallback.
func (b *InMemoryBackend) ListTagsForResource(ctx context.Context, arnStr string) ([]map[string]string, error) {
	region := regionFromARN(arnStr, getRegion(ctx, b.defaultRegion))

	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	sq, err := b.lookupByARN(region, arnStr)
	if err != nil {
		return nil, err
	}

	keys := collections.SortedKeys(sq.Tags)

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

// ListScheduledQueriesFull returns all scheduled queries for the request region with full details, sorted by name.
func (b *InMemoryBackend) ListScheduledQueriesFull(ctx context.Context) []*ScheduledQuery {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("ListScheduledQueriesFull")
	defer b.mu.RUnlock()

	sqs := b.scheduledQueries[region]
	names := collections.SortedKeys(sqs)

	out := make([]*ScheduledQuery, 0, len(names))

	for _, name := range names {
		out = append(out, cloneScheduledQuery(sqs[name]))
	}

	return out
}

// DescribeAccountSettings returns the current account-level settings for the request region.
func (b *InMemoryBackend) DescribeAccountSettings(ctx context.Context) AccountSettings {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("DescribeAccountSettings")
	defer b.mu.RUnlock()

	return b.accountSettingsFor(region)
}

// PrepareQuery validates a query string and returns its column and parameter metadata.
// It infers columns from the SELECT projection and parameters from ? markers.
// When validateOnly is true, only parse errors are surfaced.
// ctx is accepted for interface consistency; PrepareQuery is stateless.
func (b *InMemoryBackend) PrepareQuery(
	_ context.Context, queryString string, validateOnly bool,
) (*PrepareQueryResult, error) {
	if queryString == "" {
		return nil, fmt.Errorf("%w: QueryString is required", ErrValidation)
	}

	cols, params := inferColumnsFromSQL(queryString)

	if validateOnly {
		// Surface syntax issues only — we use the inferred result either way.
		return &PrepareQueryResult{
			QueryString: queryString,
			Columns:     []ColumnInfo{},
			Parameters:  []ColumnInfo{},
		}, nil
	}

	return &PrepareQueryResult{
		QueryString: queryString,
		Columns:     cols,
		Parameters:  params,
	}, nil
}

// isValidPricingModel reports whether the given pricing model string is recognised.
func isValidPricingModel(model string) bool {
	return model == pricingModelBytesScanned || model == pricingModelComputeUnits
}

// UpdateAccountSettings updates the account-level settings for the request region and returns the new state.
// Only non-empty queryPricingModel and non-nil maxQueryTCU values are applied;
// omitted fields preserve their current values.
func (b *InMemoryBackend) UpdateAccountSettings(
	ctx context.Context, queryPricingModel string, maxQueryTCU *int32,
) (AccountSettings, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("UpdateAccountSettings")
	defer b.mu.Unlock()

	settings := b.accountSettingsFor(region)

	if queryPricingModel != "" {
		if !isValidPricingModel(queryPricingModel) {
			return AccountSettings{}, fmt.Errorf(
				"%w: invalid QueryPricingModel %q, must be one of BYTES_SCANNED or COMPUTE_UNITS",
				ErrValidation,
				queryPricingModel,
			)
		}

		settings.QueryPricingModel = queryPricingModel
	}

	if maxQueryTCU != nil {
		if *maxQueryTCU <= 0 {
			return AccountSettings{}, fmt.Errorf("%w: MaxQueryTCU must be a positive integer", ErrValidation)
		}

		settings.MaxQueryTCU = maxQueryTCU
	}

	now := time.Now()
	settings.LastUpdatedTime = &now
	b.accountSettings[region] = settings

	return settings, nil
}

// ListScheduledQueriesEnriched returns paged enriched scheduled query summaries for the request region.
func (b *InMemoryBackend) ListScheduledQueriesEnriched(
	ctx context.Context, nextToken string, maxResults int32,
) ListScheduledQueriesResult {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("ListScheduledQueriesEnriched")
	all := make([]*ScheduledQuery, 0, len(b.scheduledQueries[region]))
	for _, sq := range b.scheduledQueries[region] {
		all = append(all, cloneScheduledQuery(sq))
	}
	b.mu.RUnlock()

	return listScheduledQueriesPaged(all, nextToken, maxResults)
}

// AddScheduledQueryInternal is a test-only seed helper that stores a scheduled query directly,
// bypassing normal validation. It is used to pre-populate backend state in tests.
// The region is resolved from the ARN embedded in sq.Arn, falling back to defaultRegion.
func (b *InMemoryBackend) AddScheduledQueryInternal(sq *ScheduledQuery) {
	b.mu.Lock("AddScheduledQueryInternal")
	defer b.mu.Unlock()

	if sq.Tags == nil {
		sq.Tags = make(map[string]string)
	}

	region := regionFromARN(sq.Arn, b.defaultRegion)

	b.scheduledQueriesStore(region)[sq.Name] = sq
	b.arnIndexStore(region)[sq.Arn] = sq.Name
}
