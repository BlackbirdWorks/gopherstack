package timestreamquery

import (
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/google/uuid"
)

// ---------------------------------------------------------------------------
// Typed Datum / ColumnInfo / Row (gaps #2, #3)
// ---------------------------------------------------------------------------

// ScalarType is the Timestream type enum for scalar column values.
const (
	ScalarTypeBigint                = "BIGINT"
	ScalarTypeBoolean               = "BOOLEAN"
	ScalarTypeDate                  = "DATE"
	ScalarTypeDouble                = "DOUBLE"
	ScalarTypeIntervalDayToSecond   = "INTERVAL_DAY_TO_SECOND"
	ScalarTypeIntervalYearToMonth   = "INTERVAL_YEAR_TO_MONTH"
	ScalarTypeTime                  = "TIME"
	ScalarTypeTimestamp             = "TIMESTAMP"
	ScalarTypeTimestampWithTimezone = "TIMESTAMP_WITH_TIMEZONE"
	ScalarTypeUnknown               = "UNKNOWN"
	ScalarTypeVarchar               = "VARCHAR"
)

// TimeSeriesDataPoint is one entry in a TimeSeries datum.
type TimeSeriesDataPoint struct {
	Time  string `json:"Time"`
	Value Datum  `json:"Value"`
}

// Datum is the AWS Timestream Query typed union cell.
// Exactly one of the pointer fields is non-nil for any given cell.
type Datum struct {
	ScalarValue     *string               `json:"ScalarValue,omitempty"`
	NullValue       *bool                 `json:"NullValue,omitempty"`
	ArrayValue      []Datum               `json:"ArrayValue,omitempty"`
	RowValue        *Row                  `json:"RowValue,omitempty"`
	TimeSeriesValue []TimeSeriesDataPoint `json:"TimeSeriesValue,omitempty"`
}

// ScalarDatum returns a Datum wrapping a scalar string value.
func ScalarDatum(v string) Datum {
	return Datum{ScalarValue: &v}
}

// NullDatum returns a Datum representing a SQL NULL.
func NullDatum() Datum {
	t := true

	return Datum{NullValue: &t}
}

// Row is a result row containing typed Data cells.
type Row struct {
	Data []Datum `json:"Data"`
}

// ColumnType describes the type of a Timestream Query column.
// For scalar columns only ScalarType is set; for complex types the sub-
// fields carry the nested ColumnInfo.
type ColumnType struct {
	ArrayColumnInfo                  *ColumnInfo  `json:"ArrayColumnInfo,omitempty"`
	TimeSeriesMeasureValueColumnInfo *ColumnInfo  `json:"TimeSeriesMeasureValueColumnInfo,omitempty"`
	ScalarType                       string       `json:"ScalarType,omitempty"`
	RowColumnInfo                    []ColumnInfo `json:"RowColumnInfo,omitempty"`
}

// ColumnInfo describes one column in a Timestream Query result set.
type ColumnInfo struct {
	Name string     `json:"Name,omitempty"`
	Type ColumnType `json:"Type"`
}

// scalarColumnInfo returns a ColumnInfo for a plain scalar column.
func scalarColumnInfo(name, scalarType string) ColumnInfo {
	return ColumnInfo{Name: name, Type: ColumnType{ScalarType: scalarType}}
}

// ---------------------------------------------------------------------------
// QueryStatus (gap #4)
// ---------------------------------------------------------------------------

// QueryStatusDetail holds byte-level progress info for a Query call.
type QueryStatusDetail struct {
	ProgressPercentage     float64 `json:"ProgressPercentage"`
	CumulativeBytesScanned int64   `json:"CumulativeBytesScanned"`
	CumulativeBytesMetered int64   `json:"CumulativeBytesMetered"`
}

// estimateBytesScanned returns a rough byte estimate for n rows with c columns.
// Assumes ~32 bytes per cell (average scalar value) as a sim heuristic.
func estimateBytesScanned(rows int, cols int) int64 {
	if rows == 0 || cols == 0 {
		return 0
	}
	estimated := int64(rows) * int64(cols) * 32
	// Timestream bills a 10 MB minimum per query scan.
	const minBytes = 10 * 1024 * 1024
	if estimated < minBytes {
		return minBytes
	}

	return estimated
}

// estimateBytesMetered returns the billing unit. For BYTES_SCANNED model,
// it mirrors bytes scanned; for COMPUTE_UNITS, it is a fixed minimum.
func estimateBytesMetered(scanned int64) int64 {
	return scanned
}

// ---------------------------------------------------------------------------
// ClientToken idempotency cache (gap #6)
// ---------------------------------------------------------------------------

const clientTokenTTL = 8 * time.Hour

// clientTokenEntry holds a cached query result for idempotency.
type clientTokenEntry struct {
	expiresAt time.Time
	queryID   string
}

// clientTokenCache is a sharded TTL cache for ClientToken → QueryID mapping.
type clientTokenCache struct {
	entries map[string]clientTokenEntry
	mu      sync.Mutex
}

func newClientTokenCache() *clientTokenCache {
	return &clientTokenCache{entries: make(map[string]clientTokenEntry)}
}

// get returns the cached QueryID for token, or "" if absent / expired.
func (c *clientTokenCache) get(token string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[token]
	if !ok || time.Now().After(e.expiresAt) {
		delete(c.entries, token)

		return ""
	}

	return e.queryID
}

// set stores queryID under token for 8 hours.
func (c *clientTokenCache) set(token, queryID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[token] = clientTokenEntry{
		queryID:   queryID,
		expiresAt: time.Now().Add(clientTokenTTL),
	}
}

// sweep removes expired entries.
func (c *clientTokenCache) sweep() {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	for k, e := range c.entries {
		if now.After(e.expiresAt) {
			delete(c.entries, k)
		}
	}
}

// ---------------------------------------------------------------------------
// Paginated query result store (gap #7)
// ---------------------------------------------------------------------------

// pagedQueryResult stores the full result set for a query awaiting paging.
type pagedQueryResult struct {
	createdAt time.Time
	queryID   string
	rows      []Row
	columns   []ColumnInfo
	offset    int
}

// nextPage slices up to maxRows rows starting at result.offset and advances the cursor.
// It returns the page slice and a NextToken if more rows remain; empty string means last page.
func (r *pagedQueryResult) nextPage(maxRows int) ([]Row, string) {
	end := min(r.offset+maxRows, len(r.rows))
	page := r.rows[r.offset:end]
	r.offset = end
	if r.offset < len(r.rows) {
		return page, r.queryID + ":" + strconv.Itoa(r.offset)
	}

	return page, ""
}

// ---------------------------------------------------------------------------
// QueryInsightsResponse (gap #5)
// ---------------------------------------------------------------------------

// QueryInsightsResponse holds the insights payload returned alongside query results.
type QueryInsightsResponse struct {
	OutputRows           int64   `json:"OutputRows"`
	OutputBytes          int64   `json:"OutputBytes"`
	UnloadPartitionCount int64   `json:"UnloadPartitionCount,omitempty"`
	UnloadWrittenRows    int64   `json:"UnloadWrittenRows,omitempty"`
	UnloadWrittenBytes   int64   `json:"UnloadWrittenBytes,omitempty"`
	QuerySpatialCoverage float64 `json:"QuerySpatialCoverage,omitempty"`
}

// buildQueryInsightsResponse builds a QueryInsightsResponse for a completed query.
func buildQueryInsightsResponse(rows []Row, cols []ColumnInfo) QueryInsightsResponse {
	bytesOut := estimateBytesScanned(len(rows), len(cols))

	return QueryInsightsResponse{
		OutputRows:  int64(len(rows)),
		OutputBytes: bytesOut,
	}
}

// ---------------------------------------------------------------------------
// ScheduleExpression validation (gap #23)
// ---------------------------------------------------------------------------

var (
	rateExpr  = regexp.MustCompile(`^rate\(\d+\s+(minute|minutes|hour|hours|day|days)\)$`)
	cronParts = regexp.MustCompile(`^cron\((.+)\)$`)
)

// validateScheduleExpression checks that expr is a valid rate(...) or cron(...) expression.
func validateScheduleExpression(expr string) error {
	if expr == "" {
		return fmt.Errorf("%w: ScheduleExpression is required", ErrValidation)
	}
	if rateExpr.MatchString(expr) {
		return nil
	}
	if m := cronParts.FindStringSubmatch(expr); m != nil {
		// cron must have exactly 6 space-separated fields.
		fields := strings.Fields(m[1])
		if len(fields) != 6 {
			return fmt.Errorf(
				"%w: cron expression must have 6 fields (got %d): %s",
				ErrValidation, len(fields), expr,
			)
		}

		return nil
	}

	return fmt.Errorf(
		"%w: invalid ScheduleExpression %q — must be rate(N unit) or cron(f1 f2 f3 f4 f5 f6)",
		ErrValidation, expr,
	)
}

// ---------------------------------------------------------------------------
// Next/PreviousInvocationTime from schedule expression (gap #20)
// ---------------------------------------------------------------------------

// nextInvocationTime returns the next scheduled invocation time after `from` for the given expr.
// For rate expressions it adds the rate interval; for cron expressions it approximates
// to the nearest future minute boundary (simplified simulation).
func nextInvocationTime(expr string, from time.Time) time.Time {
	if m := regexp.MustCompile(`^rate\((\d+)\s+(minute|minutes|hour|hours|day|days)\)$`).
		FindStringSubmatch(expr); m != nil {
		n, _ := strconv.Atoi(m[1])
		unit := m[2]
		var dur time.Duration
		switch {
		case strings.HasPrefix(unit, "minute"):
			dur = time.Duration(n) * time.Minute
		case strings.HasPrefix(unit, "hour"):
			dur = time.Duration(n) * time.Hour
		case strings.HasPrefix(unit, "day"):
			dur = time.Duration(n) * 24 * time.Hour
		}

		return from.Add(dur)
	}
	// cron: advance to next round hour as a simple approximation.
	return from.Truncate(time.Hour).Add(time.Hour)
}

// previousInvocationTime returns the most recent invocation before `from`.
func previousInvocationTime(expr string, from time.Time) time.Time {
	if m := regexp.MustCompile(`^rate\((\d+)\s+(minute|minutes|hour|hours|day|days)\)$`).
		FindStringSubmatch(expr); m != nil {
		n, _ := strconv.Atoi(m[1])
		unit := m[2]
		var dur time.Duration
		switch {
		case strings.HasPrefix(unit, "minute"):
			dur = time.Duration(n) * time.Minute
		case strings.HasPrefix(unit, "hour"):
			dur = time.Duration(n) * time.Hour
		case strings.HasPrefix(unit, "day"):
			dur = time.Duration(n) * 24 * time.Hour
		}

		return from.Add(-dur)
	}

	return from.Truncate(time.Hour)
}

// ---------------------------------------------------------------------------
// MaxRows validation (gap #8)
// ---------------------------------------------------------------------------

const (
	maxRowsDefault = 1000
	maxRowsMin     = 1
	maxRowsMax     = 1000
)

// validateMaxRows returns the effective MaxRows value and an error if out of range.
func validateMaxRows(maxRows int32) (int, error) {
	if maxRows == 0 {
		return maxRowsDefault, nil
	}
	if maxRows < maxRowsMin || maxRows > maxRowsMax {
		return 0, fmt.Errorf(
			"%w: MaxRows must be between %d and %d, got %d",
			ErrValidation, maxRowsMin, maxRowsMax, maxRows,
		)
	}

	return int(maxRows), nil
}

// ---------------------------------------------------------------------------
// PrepareQuery SQL column inference (gap #10)
// ---------------------------------------------------------------------------

// inferColumnsFromSQL does minimal SQL projection parsing to derive ColumnInfo
// from a SELECT statement. It returns the inferred columns and parameter count.
func inferColumnsFromSQL(queryString string) ([]ColumnInfo, []ColumnInfo) {
	q := strings.TrimSpace(queryString)
	upper := strings.ToUpper(q)

	// Strip leading SELECT.
	if !strings.HasPrefix(upper, "SELECT") {
		return []ColumnInfo{}, []ColumnInfo{}
	}

	// Find FROM (or end-of-string if no FROM).
	fromIdx := indexKeyword(upper, "FROM")
	projection := q[len("SELECT"):]
	if fromIdx > len("SELECT") {
		projection = q[len("SELECT"):fromIdx]
	}

	projection = strings.TrimSpace(projection)

	var cols []ColumnInfo
	if projection == "*" {
		// Wildcard — return a generic time + measure column.
		cols = []ColumnInfo{
			scalarColumnInfo("time", ScalarTypeTimestamp),
			scalarColumnInfo("measure_name", ScalarTypeVarchar),
			scalarColumnInfo("measure_value", ScalarTypeDouble),
		}
	} else {
		// Parse comma-separated projection items.
		for _, part := range splitProjection(projection) {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			name := columnAlias(part)
			scalarType := inferScalarType(part)
			cols = append(cols, scalarColumnInfo(name, scalarType))
		}
	}

	// Count ? parameter markers.
	params := make([]ColumnInfo, 0)
	for i, ch := range q {
		if ch == '?' {
			name := fmt.Sprintf("param%d", len(params)+1)
			_ = i
			params = append(params, scalarColumnInfo(name, ScalarTypeVarchar))
		}
	}

	return cols, params
}

// indexKeyword finds the byte index of keyword word in s as a whole word.
func indexKeyword(s, word string) int {
	idx := 0
	for idx < len(s) {
		i := strings.Index(s[idx:], word)
		if i < 0 {
			return -1
		}
		pos := idx + i
		// Check word boundary before.
		if pos > 0 && (unicode.IsLetter(rune(s[pos-1])) || s[pos-1] == '_') {
			idx = pos + 1

			continue
		}
		// Check word boundary after.
		end := pos + len(word)
		if end < len(s) && (unicode.IsLetter(rune(s[end])) || s[end] == '_') {
			idx = pos + 1

			continue
		}

		return pos
	}

	return -1
}

// splitProjection splits a projection string on top-level commas (ignoring nested parens).
func splitProjection(s string) []string {
	var parts []string
	depth := 0
	start := 0
	for i, ch := range s {
		switch ch {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				parts = append(parts, s[start:i])
				start = i + 1
			}
		}
	}
	parts = append(parts, s[start:])

	return parts
}

// columnAlias extracts an alias (from AS clause or last identifier) for a projection item.
func columnAlias(item string) string {
	upper := strings.ToUpper(item)
	if i := strings.Index(upper, " AS "); i >= 0 {
		alias := strings.TrimSpace(item[i+4:])

		return stripQuotes(alias)
	}
	// Use last identifier token.
	tokens := strings.FieldsFunc(item, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_'
	})
	if len(tokens) > 0 {
		return tokens[len(tokens)-1]
	}

	return item
}

// stripQuotes removes surrounding double or back quotes.
func stripQuotes(s string) string {
	if len(s) >= 2 && ((s[0] == '"' && s[len(s)-1] == '"') || (s[0] == '`' && s[len(s)-1] == '`')) {
		return s[1 : len(s)-1]
	}

	return s
}

// inferScalarType returns the most likely ScalarType for a projection expression.
func inferScalarType(expr string) string {
	upper := strings.ToUpper(strings.TrimSpace(expr))
	switch {
	case strings.HasPrefix(upper, "COUNT("), strings.HasPrefix(upper, "SUM("),
		strings.HasPrefix(upper, "MIN("), strings.HasPrefix(upper, "MAX("):
		return ScalarTypeBigint
	case strings.HasPrefix(upper, "AVG("):
		return ScalarTypeDouble
	case strings.Contains(upper, "TIME") || strings.Contains(upper, "TIMESTAMP"):
		return ScalarTypeTimestamp
	case strings.Contains(upper, "MEASURE_VALUE::DOUBLE"), strings.Contains(upper, "MEASURE_VALUE::BIGINT"):
		if strings.Contains(upper, "DOUBLE") {
			return ScalarTypeDouble
		}

		return ScalarTypeBigint
	case strings.Contains(upper, "MEASURE_VALUE::BOOLEAN"):
		return ScalarTypeBoolean
	case strings.Contains(upper, "MEASURE_NAME"), strings.Contains(upper, "DATABASE"),
		strings.Contains(upper, "TABLE_NAME"):
		return ScalarTypeVarchar
	default:
		return ScalarTypeVarchar
	}
}

// ---------------------------------------------------------------------------
// QueryCompute (gap #14)
// ---------------------------------------------------------------------------

// ProvisionedCapacity holds the configuration for provisioned TCU.
type ProvisionedCapacity struct {
	TargetQueryTCU            *int32              `json:"TargetQueryTCU,omitempty"`
	NotificationConfiguration *NotificationConfig `json:"NotificationConfiguration,omitempty"`
}

// NotificationConfig is the SNS notification config for capacity alerts.
type NotificationConfig struct {
	SnsConfiguration *SnsConfig `json:"SnsConfiguration,omitempty"`
}

// SnsConfig holds an SNS topic ARN.
type SnsConfig struct {
	TopicArn string `json:"TopicArn"`
}

// QueryCompute holds the compute mode and optional provisioned capacity.
type QueryCompute struct {
	ProvisionedCapacity *ProvisionedCapacity `json:"ProvisionedCapacity,omitempty"`
	ComputeMode         string               `json:"ComputeMode,omitempty"` // ON_DEMAND | PROVISIONED
}

// ---------------------------------------------------------------------------
// Full ScheduledQuery types (gap #19, #21)
// ---------------------------------------------------------------------------

// ExecutionStats holds statistics from a scheduled query execution.
type ExecutionStats struct {
	ExecutionTimeInMillisecs int64 `json:"ExecutionTimeInMillisecs,omitempty"`
	DataWrites               int64 `json:"DataWrites,omitempty"`
	BytesMetered             int64 `json:"BytesMetered,omitempty"`
	CumulativeBytesScanned   int64 `json:"CumulativeBytesScanned,omitempty"`
	QueryResultRows          int64 `json:"QueryResultRows,omitempty"`
	RecordsIngested          int64 `json:"RecordsIngested,omitempty"`
}

// ErrorReportLocation holds the S3 error report location for a scheduled query run.
type ErrorReportLocation struct {
	S3ReportLocation *S3ReportLocation `json:"S3ReportLocation,omitempty"`
}

// S3ReportLocation is the S3 bucket + key for an error report.
type S3ReportLocation struct {
	ObjectKey  string `json:"ObjectKey,omitempty"`
	BucketName string `json:"BucketName,omitempty"`
}

// LastRunSummary holds the full summary of the most recent execution of a scheduled query.
type LastRunSummary struct {
	ErrorReportLocation *ErrorReportLocation `json:"ErrorReportLocation,omitempty"`
	ExecutionStats      *ExecutionStats      `json:"ExecutionStats,omitempty"`
	FailureReason       string               `json:"FailureReason,omitempty"`
	RunStatus           string               `json:"RunStatus,omitempty"`
	TriggerTime         string               `json:"TriggerTime,omitempty"`
	InvocationTime      string               `json:"InvocationTime,omitempty"`
}

// scheduledQueryRunStatus values.
const (
	runStatusAutoTriggerSuccess   = "AUTO_TRIGGER_SUCCESS"
	runStatusAutoTriggerFailure   = "AUTO_TRIGGER_FAILURE"
	runStatusManualTriggerSuccess = "MANUAL_TRIGGER_SUCCESS"
)

// TargetDestinationForList summarises the write destination for list responses.
type TargetDestinationForList struct {
	TimestreamDestination *TimestreamDestinationForList `json:"TimestreamDestination,omitempty"`
}

// TimestreamDestinationForList is the Timestream write target in a list summary.
type TimestreamDestinationForList struct {
	DatabaseName string `json:"DatabaseName,omitempty"`
	TableName    string `json:"TableName,omitempty"`
}

// ScheduledQueryListEntry is the enriched summary used in list responses (gap #19).
type ScheduledQueryListEntry struct {
	TargetDestination      *TargetDestinationForList `json:"TargetDestination,omitempty"`
	LastRunStatus          string                    `json:"LastRunStatus,omitempty"`
	NextInvocationTime     string                    `json:"NextInvocationTime,omitempty"`
	PreviousInvocationTime string                    `json:"PreviousInvocationTime,omitempty"`
	Arn                    string                    `json:"Arn"`
	Name                   string                    `json:"Name"`
	State                  string                    `json:"State"`
	CreationTime           string                    `json:"CreationTime,omitempty"`
}

// buildScheduledQueryListEntry converts a ScheduledQuery to an enriched list entry.
func buildScheduledQueryListEntry(sq *ScheduledQuery) ScheduledQueryListEntry {
	entry := ScheduledQueryListEntry{
		Arn:          sq.Arn,
		Name:         sq.Name,
		State:        sq.State,
		CreationTime: epochSecondsStr(sq.CreationTime),
	}
	if sq.TargetDatabase != "" || sq.TargetTable != "" {
		entry.TargetDestination = &TargetDestinationForList{
			TimestreamDestination: &TimestreamDestinationForList{
				DatabaseName: sq.TargetDatabase,
				TableName:    sq.TargetTable,
			},
		}
	}
	now := time.Now()
	if !sq.LastRunTime.IsZero() {
		entry.LastRunStatus = runStatusAutoTriggerSuccess
		entry.PreviousInvocationTime = epochSecondsStr(sq.LastRunTime)
	} else if sq.ScheduleExpression != "" {
		entry.PreviousInvocationTime = epochSecondsStr(previousInvocationTime(sq.ScheduleExpression, now))
	}
	entry.NextInvocationTime = epochSecondsStr(nextInvocationTime(sq.ScheduleExpression, now))

	return entry
}

// buildLastRunSummary constructs a full LastRunSummary for a ScheduledQuery.
func buildLastRunSummary(sq *ScheduledQuery) *LastRunSummary {
	if sq.LastRunTime.IsZero() {
		return nil
	}
	triggerTime := sq.LastRunTime.Add(-time.Second)

	return &LastRunSummary{
		RunStatus:      runStatusAutoTriggerSuccess,
		InvocationTime: epochSecondsStr(sq.LastRunTime),
		TriggerTime:    epochSecondsStr(triggerTime),
		ExecutionStats: &ExecutionStats{
			ExecutionTimeInMillisecs: 500,
			RecordsIngested:          10,
		},
	}
}

// epochSecondsStr returns t as a Unix epoch seconds string (for JSON).
func epochSecondsStr(t time.Time) string {
	if t.IsZero() {
		return ""
	}

	return strconv.FormatFloat(math.Round(float64(t.UnixNano())/1e9*1e3)/1e3, 'f', 3, 64)
}

// ---------------------------------------------------------------------------
// Pagination token store (gap #7)
// ---------------------------------------------------------------------------

// nextTokenStore manages paged query results.
type nextTokenStore struct {
	results map[string]*pagedQueryResult
	mu      sync.Mutex
}

func newNextTokenStore() *nextTokenStore {
	return &nextTokenStore{results: make(map[string]*pagedQueryResult)}
}

// store saves a paged result and returns the initial NextToken if rows > maxRows.
func (s *nextTokenStore) store(queryID string, rows []Row, cols []ColumnInfo, maxRows int) ([]Row, string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	r := &pagedQueryResult{
		queryID:   queryID,
		rows:      rows,
		columns:   cols,
		offset:    0,
		createdAt: time.Now(),
	}
	s.results[queryID] = r

	return r.nextPage(maxRows)
}

// resolve retrieves the next page for a continuation token.
// Token format: "queryID:offset".
func (s *nextTokenStore) resolve(token string, maxRows int) (string, []Row, []ColumnInfo, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	parts := strings.SplitN(token, ":", 2)
	if len(parts) != 2 {
		return "", nil, nil, "", fmt.Errorf("%w: invalid NextToken", ErrValidation)
	}
	queryID := parts[0]
	offset, err := strconv.Atoi(parts[1])
	if err != nil {
		return "", nil, nil, "", fmt.Errorf("%w: invalid NextToken offset", ErrValidation)
	}

	r, ok := s.results[queryID]
	if !ok {
		return "", nil, nil, "", fmt.Errorf("%w: NextToken refers to expired or unknown query", ErrValidation)
	}

	r.offset = offset
	page, nextToken := r.nextPage(maxRows)

	return queryID, page, r.columns, nextToken, nil
}

// ---------------------------------------------------------------------------
// UUID generator helper
// ---------------------------------------------------------------------------

// newQueryID returns a new unique query ID.
func newQueryID() string {
	return uuid.NewString()
}

// ---------------------------------------------------------------------------
// enriched ListScheduledQueries with pagination (gap #18)
// ---------------------------------------------------------------------------

// ListScheduledQueriesResult is the paginated result of a ListScheduledQueries call.
type ListScheduledQueriesResult struct {
	NextToken string
	Items     []ScheduledQueryListEntry
}

// listScheduledQueriesPaged returns a paged slice of scheduled query entries.
func listScheduledQueriesPaged(
	queries []*ScheduledQuery,
	nextToken string,
	maxResults int32,
) ListScheduledQueriesResult {
	if maxResults <= 0 {
		maxResults = 100
	}

	// Sort by name for stable ordering.
	sorted := make([]*ScheduledQuery, len(queries))
	copy(sorted, queries)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })

	start := 0
	if nextToken != "" {
		for i, sq := range sorted {
			if sq.Name == nextToken {
				start = i

				break
			}
		}
	}

	end := start + int(maxResults)
	var outNextToken string
	if end < len(sorted) {
		outNextToken = sorted[end].Name
	} else {
		end = len(sorted)
	}

	items := make([]ScheduledQueryListEntry, 0, end-start)
	for _, sq := range sorted[start:end] {
		items = append(items, buildScheduledQueryListEntry(sq))
	}

	return ListScheduledQueriesResult{Items: items, NextToken: outNextToken}
}
