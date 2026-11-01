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

const queryProgressComplete = 100.0

// maxRetainedQueries bounds the queries map so a long-running instance cannot
// leak memory through repeated Query calls: once at the cap, an arbitrarily
// chosen entry is evicted regardless of cancellation status (CancelQuery
// marks an entry cancelled in place rather than deleting it, so that a
// repeat CancelQuery call remains idempotent -- see CancelQuery); in real
// Timestream, Query results are transient anyway.
const maxRetainedQueries = 10000

// QueryWithOptions executes a query with full options support (clientToken, pagination).
// ctx is accepted for interface consistency; query results are not region-isolated.
func (b *InMemoryBackend) QueryWithOptions(ctx context.Context, opts QueryOptions) (*QueryPage, error) {
	// Validate MaxRows.
	maxRows, err := validateMaxRows(opts.MaxRows)
	if err != nil {
		return nil, err
	}

	if opts.NextToken != "" {
		pg, resolveErr := b.pageStore.resolve(opts.NextToken, maxRows, b.clock())
		if resolveErr != nil {
			return nil, resolveErr
		}

		return &QueryPage{
			QueryID:     pg.queryID,
			Rows:        pg.rows,
			Columns:     pg.cols,
			QueryStatus: pg.status,
			NextToken:   pg.next,
		}, nil
	}

	if err = validateQuerySyntax(opts.QueryString); err != nil {
		return nil, err
	}

	if err = b.checkTableRefs(ctx, opts.QueryString); err != nil {
		return nil, err
	}

	if opts.ClientToken != "" {
		b.clientTokens.sweep()
		if cachedID := b.clientTokens.get(opts.ClientToken); cachedID != "" {
			pg, resumeErr := b.pageStore.restart(cachedID, maxRows, b.clock())
			if resumeErr == nil {
				return &QueryPage{
					QueryID: cachedID, Rows: pg.rows, Columns: pg.cols, QueryStatus: pg.status, NextToken: pg.next,
				}, nil
			}
		}
	}

	rs, err := b.execute(ctx, opts.QueryString, nil)
	if err != nil {
		return nil, err
	}

	queryID := newQueryID()
	status := statusFor(rs)

	result := &QueryResult{
		QueryID:     queryID,
		QueryStatus: status,
		Rows:        rs.rows,
		Columns:     rs.cols,
		Insights:    buildQueryInsightsResponse(rs),
	}

	b.mu.Lock("QueryWithOptions")
	evictOldestQueryLocked(b)
	b.queries.Put(result)
	b.mu.Unlock()

	if opts.ClientToken != "" {
		b.clientTokens.set(opts.ClientToken, queryID)
	}

	pg := b.pageStore.store(queryID, rs.rows, rs.cols, status, maxRows, b.clock())

	var insights *QueryInsightsResponse
	if opts.InsightsMode != "DISABLED" {
		ins := result.Insights
		insights = &ins
	}

	return &QueryPage{
		QueryID:     queryID,
		Rows:        pg.rows,
		Columns:     rs.cols,
		QueryStatus: status,
		Insights:    insights,
		NextToken:   pg.next,
	}, nil
}

// Query runs a query and returns its full result (legacy path); a failing query yields an empty result.
func (b *InMemoryBackend) Query(ctx context.Context, queryString string) *QueryResult {
	rs, err := b.execute(ctx, queryString, nil)
	if err != nil {
		rs = &resultSet{rows: []Row{}}
	}

	result := &QueryResult{
		QueryID:     newQueryID(),
		QueryStatus: statusFor(rs),
		Rows:        rs.rows,
		Columns:     rs.cols,
	}

	b.mu.Lock("Query")
	defer b.mu.Unlock()

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
	b.pageStore.cancel(queryID)

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
	ctx context.Context, queryString string, _ bool,
) (*PrepareQueryResult, error) {
	if queryString == "" {
		return nil, fmt.Errorf("%w: QueryString is required", ErrValidation)
	}

	if err := validateQuerySyntax(queryString); err != nil {
		return nil, err
	}

	if err := b.checkTableRefs(ctx, queryString); err != nil {
		return nil, err
	}

	cols, params := inferColumnsFromSQL(queryString)

	return &PrepareQueryResult{
		QueryString: queryString,
		Columns:     cols,
		Selected:    inferSelectColumns(queryString, cols),
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

// statusFor reports the completed-query status: bytes scanned is the stored size of every
// record in the referenced tables; metered applies Timestream's 10 MB per-query minimum
// whenever a table was read.
func statusFor(rs *resultSet) QueryStatusDetail {
	metered := rs.scanned
	if rs.tables > 0 && metered < minMeteredBytes {
		metered = minMeteredBytes
	}

	return QueryStatusDetail{
		ProgressPercentage:     queryProgressComplete,
		CumulativeBytesScanned: rs.scanned,
		CumulativeBytesMetered: metered,
	}
}

// buildQueryInsightsResponse builds the insights payload for a completed query.
func buildQueryInsightsResponse(rs *resultSet) QueryInsightsResponse {
	return QueryInsightsResponse{
		OutputRows:  int64(len(rs.rows)),
		OutputBytes: rs.outBytes(),
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
// Paginated query result store
// ---------------------------------------------------------------------------

const (
	// pageTokenTTL and pageTokenMaxUses follow the Query API doc: "A pagination token can be
	// used for up to five Query invocations, OR for a duration of up to 1 hour".
	pageTokenTTL     = time.Hour
	pageTokenMaxUses = 5
)

// pagedQueryResult stores the full result set for a query awaiting paging.
type pagedQueryResult struct {
	createdAt time.Time
	uses      map[int]int
	queryID   string
	rows      []Row
	columns   []ColumnInfo
	status    QueryStatusDetail
	cancelled bool
}

// pageOut is one served page.
type pageOut struct {
	queryID string
	next    string
	rows    []Row
	cols    []ColumnInfo
	status  QueryStatusDetail
}

func (r *pagedQueryResult) slice(offset, maxRows int) pageOut {
	end := min(offset+maxRows, len(r.rows))
	out := pageOut{queryID: r.queryID, rows: r.rows[offset:end], cols: r.columns, status: r.status}

	if end < len(r.rows) {
		out.next = r.queryID + ":" + strconv.Itoa(end)
	}

	return out
}

// nextTokenStore manages paged query results.
type nextTokenStore struct {
	results map[string]*pagedQueryResult
	mu      sync.Mutex
}

func newNextTokenStore() *nextTokenStore {
	return &nextTokenStore{results: make(map[string]*pagedQueryResult)}
}

func (s *nextTokenStore) sweepLocked(now time.Time) {
	for id, r := range s.results {
		if now.Sub(r.createdAt) > pageTokenTTL {
			delete(s.results, id)
		}
	}
}

// store saves a materialised result and returns its first page.
func (s *nextTokenStore) store(
	queryID string, rows []Row, cols []ColumnInfo, status QueryStatusDetail, maxRows int, now time.Time,
) pageOut {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.sweepLocked(now)

	r := &pagedQueryResult{
		queryID: queryID, rows: rows, columns: cols, status: status, createdAt: now, uses: map[int]int{},
	}
	s.results[queryID] = r

	return r.slice(0, maxRows)
}

// restart returns the first page of a stored result (ClientToken replay).
func (s *nextTokenStore) restart(queryID string, maxRows int, now time.Time) (pageOut, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	r, ok := s.results[queryID]
	if !ok || now.Sub(r.createdAt) > pageTokenTTL {
		return pageOut{}, fmt.Errorf("%w: query %q not found in page store", ErrNotFound, queryID)
	}

	return r.slice(0, maxRows), nil
}

// cancel invalidates further paging of queryID.
func (s *nextTokenStore) cancel(queryID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if r, ok := s.results[queryID]; ok {
		r.cancelled = true
	}
}

// resolve serves the page named by a continuation token ("queryID:offset").
func (s *nextTokenStore) resolve(token string, maxRows int, now time.Time) (pageOut, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	queryID, offStr, ok := strings.Cut(token, ":")
	if !ok {
		return pageOut{}, fmt.Errorf("%w: Invalid pagination token", ErrValidation)
	}

	offset, err := strconv.Atoi(offStr)
	if err != nil || offset < 0 {
		return pageOut{}, fmt.Errorf("%w: Invalid pagination token", ErrValidation)
	}

	r, ok := s.results[queryID]
	if !ok || now.Sub(r.createdAt) > pageTokenTTL || offset > len(r.rows) {
		return pageOut{}, fmt.Errorf("%w: Invalid pagination token", ErrValidation)
	}

	if r.cancelled {
		return pageOut{}, fmt.Errorf("%w: Unable to poll results for a cancelled query", ErrAlreadyExists)
	}

	r.uses[offset]++
	if r.uses[offset] > pageTokenMaxUses {
		return pageOut{}, fmt.Errorf("%w: Invalid pagination token", ErrValidation)
	}

	return r.slice(offset, maxRows), nil
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
