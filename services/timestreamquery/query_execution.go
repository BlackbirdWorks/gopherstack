package timestreamquery

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/google/uuid"
)

// Numeric constants for estimation heuristics.
const (
	bytesPerCell          = 32               // avg bytes per scalar cell for scan estimate
	minScanBytes          = 10 * 1024 * 1024 // 10 MB minimum Timestream billing unit
	tokenParts            = 2                // NextToken format: "queryID:offset"
	queryProgressComplete = 100.0
)

// maxRetainedQueries bounds the queries map so a long-running instance cannot
// leak memory through repeated Query calls: once at the cap, an arbitrarily
// chosen entry is evicted regardless of cancellation status (CancelQuery
// marks an entry cancelled in place rather than deleting it, so that a
// repeat CancelQuery call remains idempotent -- see CancelQuery); in real
// Timestream, Query results are transient anyway.
const maxRetainedQueries = 10000

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
	evictOldestQueryLocked(b)
	b.queries.Put(result)
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

	evictOldestQueryLocked(b)
	b.queries.Put(result)

	return result
}

// CancelQuery cancels a running query (simulated no-op if not found).
// CancelQuery is documented as idempotent: cancelling a query that has
// already been cancelled must still succeed (with a CancellationMessage),
// not error, so the result is marked cancelled in place rather than deleted;
// an unknown QueryId still returns ValidationException (gap #9).
// ctx is accepted for interface consistency; query results are not region-isolated.
func (b *InMemoryBackend) CancelQuery(_ context.Context, queryID string) error {
	b.mu.Lock("CancelQuery")
	defer b.mu.Unlock()

	qr, ok := b.queries.Get(queryID)
	if !ok {
		// Real Timestream returns ValidationException for unknown IDs (gap #9).
		return fmt.Errorf("%w: invalid identifier: query %q not found", ErrValidation, queryID)
	}

	qr.Cancelled = true

	return nil
}

// evictOldestQueryLocked removes arbitrarily-chosen query results (Go/Table
// iteration order is unspecified, matching the pre-conversion map-based
// eviction) while b.queries is at or above maxRetainedQueries. Caller must
// hold b.mu.
func evictOldestQueryLocked(b *InMemoryBackend) {
	for b.queries.Len() >= maxRetainedQueries {
		var evictID string
		b.queries.Range(func(v *QueryResult) bool {
			evictID = v.QueryID

			return false
		})
		b.queries.Delete(evictID)
	}
}

// PrepareQuery validates a query string and returns its column and parameter metadata.
// It infers columns from the SELECT projection and parameters from ? markers.
//
// Real Timestream documents ValidateOnly=true as the only supported mode for
// this operation, and PrepareQueryOutput.Columns/Parameters are both required
// (non-optional) response fields regardless of ValidateOnly -- they are the
// entire point of the call (describing a query's shape before running it as a
// scheduled query). validateOnly is accepted for wire-compatibility with the
// input shape but does not change the response: an earlier version of this
// method returned an empty Columns/Parameters list whenever ValidateOnly was
// true, which discarded the inferred result for the one mode real clients
// actually use.
// ctx is accepted for interface consistency; PrepareQuery is stateless.
func (b *InMemoryBackend) PrepareQuery(
	_ context.Context, queryString string, _ bool,
) (*PrepareQueryResult, error) {
	if queryString == "" {
		return nil, fmt.Errorf("%w: QueryString is required", ErrValidation)
	}

	cols, params := inferColumnsFromSQL(queryString)

	return &PrepareQueryResult{
		QueryString: queryString,
		Columns:     cols,
		Parameters:  params,
	}, nil
}

// ---------------------------------------------------------------------------
// ScalarType is the Timestream type enum for scalar column values.
// ---------------------------------------------------------------------------

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

// scalarColumnInfo returns a ColumnInfo for a plain scalar column.
func scalarColumnInfo(name, scalarType string) ColumnInfo {
	return ColumnInfo{Name: name, Type: ColumnType{ScalarType: scalarType}}
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

// estimateBytesScanned returns a rough byte estimate for n rows with c columns.
// Assumes ~32 bytes per cell (average scalar value) as a sim heuristic.
func estimateBytesScanned(rows int, cols int) int64 {
	if rows == 0 || cols == 0 {
		return 0
	}
	estimated := int64(rows) * int64(cols) * bytesPerCell
	// Timestream bills a 10 MB minimum per query scan.
	// minScanBytes used from package-level constant
	if estimated < minScanBytes {
		return minScanBytes
	}

	return estimated
}

// estimateBytesMetered returns the billing unit. For BYTES_SCANNED model,
// it mirrors bytes scanned; for COMPUTE_UNITS, it is a fixed minimum.
func estimateBytesMetered(scanned int64) int64 {
	return scanned
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
// ClientToken idempotency cache (gap #6)
// ---------------------------------------------------------------------------

const (
	// queryClientTokenTTL matches the Query API doc, which states that after
	// 4 hours a request with the same ClientToken is treated as a new request.
	queryClientTokenTTL = 4 * time.Hour
	// createScheduledQueryClientTokenTTL matches the CreateScheduledQuery API
	// doc, which states that after 8 hours a request with the same
	// ClientToken is treated as a new request.
	createScheduledQueryClientTokenTTL = 8 * time.Hour
)

// clientTokenCache is a TTL cache for ClientToken → opaque-value mapping,
// shared by any operation that supports idempotency-token replay (Query,
// CreateScheduledQuery). Each instance owns its own TTL since different
// operations document different idempotency windows.
type clientTokenCache struct {
	entries map[string]clientTokenEntry
	ttl     time.Duration
	mu      sync.Mutex
}

func newClientTokenCache(ttl time.Duration) *clientTokenCache {
	return &clientTokenCache{entries: make(map[string]clientTokenEntry), ttl: ttl}
}

// get returns the cached value for token, or "" if absent / expired.
func (c *clientTokenCache) get(token string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[token]
	if !ok || time.Now().After(e.expiresAt) {
		delete(c.entries, token)

		return ""
	}

	return e.value
}

// set stores value under token for the cache's configured TTL.
func (c *clientTokenCache) set(token, value string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[token] = clientTokenEntry{
		value:     value,
		expiresAt: time.Now().Add(c.ttl),
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

	parts := strings.SplitN(token, ":", tokenParts)
	if len(parts) != tokenParts {
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

// newQueryID returns a new unique query ID.
func newQueryID() string {
	return uuid.NewString()
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
