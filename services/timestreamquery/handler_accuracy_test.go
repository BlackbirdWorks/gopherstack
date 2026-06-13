package timestreamquery_test

import (
	"encoding/json"
	"maps"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/timestreamquery"
)

// ---------------------------------------------------------------------------
// Query — typed Datum/ColumnInfo response (gaps #2, #3, #4)
// ---------------------------------------------------------------------------

func TestQuery_TypedResponse(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := doRequest(t, h, "Query", map[string]any{
		"QueryString": "SELECT time, measure_name, measure_value::double FROM mydb.mytable",
	})

	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)

	// QueryId must be present.
	assert.NotEmpty(t, resp["QueryId"])

	// Rows should be a list (empty for simulator).
	rows, ok := resp["Rows"].([]any)
	require.True(t, ok, "Rows must be a slice")
	assert.Empty(t, rows)

	// ColumnInfo must include inferred columns.
	cols, ok := resp["ColumnInfo"].([]any)
	require.True(t, ok, "ColumnInfo must be a slice")
	assert.NotEmpty(t, cols, "ColumnInfo should be inferred from SELECT projection")

	// Verify at least one column has Name and Type.ScalarType.
	first := cols[0].(map[string]any)
	assert.NotEmpty(t, first["Name"])
	typ, ok := first["Type"].(map[string]any)
	require.True(t, ok)
	assert.NotEmpty(t, typ["ScalarType"])

	// QueryStatus must have non-nil byte counters (gap #4).
	qs, ok := resp["QueryStatus"].(map[string]any)
	require.True(t, ok)
	assert.InDelta(t, float64(100), qs["ProgressPercentage"], 0.001)
	// Byte counters: may be 0 for empty result, but must be present.
	assert.Contains(t, qs, "CumulativeBytesScanned")
	assert.Contains(t, qs, "CumulativeBytesMetered")
}

func TestQuery_WildcardInfersSchemaColumns(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := doRequest(t, h, "Query", map[string]any{
		"QueryString": "SELECT * FROM mydb.mytable",
	})

	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)

	cols, ok := resp["ColumnInfo"].([]any)
	require.True(t, ok)
	// Wildcard should produce at least time + measure columns.
	assert.GreaterOrEqual(t, len(cols), 2)
}

// ---------------------------------------------------------------------------
// Query — MaxRows validation (gap #8)
// ---------------------------------------------------------------------------

func TestQuery_MaxRowsValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		maxRows  int
		wantCode int
	}{
		{"valid MaxRows 1", 1, http.StatusOK},
		{"valid MaxRows 1000", 1000, http.StatusOK},
		{"zero MaxRows uses default", 0, http.StatusOK},
		{"MaxRows 1001 is invalid", 1001, http.StatusBadRequest},
		{"MaxRows -1 is invalid", -1, http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			body := map[string]any{"QueryString": "SELECT 1", "MaxRows": tt.maxRows}
			if tt.maxRows == 0 {
				body = map[string]any{"QueryString": "SELECT 1"}
			}
			rec := doRequest(t, h, "Query", body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// ---------------------------------------------------------------------------
// Query — ClientToken idempotency (gap #6)
// ---------------------------------------------------------------------------

func TestQuery_ClientTokenIdempotency(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	body := map[string]any{
		"QueryString": "SELECT * FROM db.table",
		"ClientToken": "unique-token-abc",
	}

	rec1 := doRequest(t, h, "Query", body)
	require.Equal(t, http.StatusOK, rec1.Code)

	rec2 := doRequest(t, h, "Query", body)
	require.Equal(t, http.StatusOK, rec2.Code)

	resp1 := parseResponse(t, rec1)
	resp2 := parseResponse(t, rec2)

	// Idempotent: both calls return the same QueryId.
	assert.Equal(t, resp1["QueryId"], resp2["QueryId"])
}

func TestQuery_DifferentClientTokensDifferentQueryIds(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	body1 := map[string]any{"QueryString": "SELECT 1", "ClientToken": "token-1"}
	body2 := map[string]any{"QueryString": "SELECT 1", "ClientToken": "token-2"}

	rec1 := doRequest(t, h, "Query", body1)
	rec2 := doRequest(t, h, "Query", body2)

	require.Equal(t, http.StatusOK, rec1.Code)
	require.Equal(t, http.StatusOK, rec2.Code)

	assert.NotEqual(t, parseResponse(t, rec1)["QueryId"], parseResponse(t, rec2)["QueryId"])
}

// ---------------------------------------------------------------------------
// Query — QueryInsightsResponse (gap #5)
// ---------------------------------------------------------------------------

func TestQuery_QueryInsightsIncluded(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := doRequest(t, h, "Query", map[string]any{
		"QueryString":   "SELECT 1",
		"QueryInsights": map[string]any{"Mode": "ENABLED_WITH_RATE_CONTROL"},
	})

	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)

	insights, ok := resp["QueryInsightsResponse"]
	require.True(t, ok, "QueryInsightsResponse must be in response when insights enabled")
	assert.NotNil(t, insights)
}

func TestQuery_QueryInsightsExcludedWhenDisabled(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := doRequest(t, h, "Query", map[string]any{
		"QueryString":   "SELECT 1",
		"QueryInsights": map[string]any{"Mode": "DISABLED"},
	})

	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)

	_, hasInsights := resp["QueryInsightsResponse"]
	assert.False(t, hasInsights, "QueryInsightsResponse must be absent when insights disabled")
}

// ---------------------------------------------------------------------------
// CancelQuery — ValidationException for unknown ID (gap #9)
// ---------------------------------------------------------------------------

func TestCancelQuery_UnknownIDReturnsValidationException(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := doRequest(t, h, "CancelQuery", map[string]any{
		"QueryId": "nonexistent-query-id",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "ValidationException", body["__type"])
}

// ---------------------------------------------------------------------------
// PrepareQuery — inferred columns and parameters (gap #10)
// ---------------------------------------------------------------------------

func TestPrepareQuery_InfersColumns(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		query      string
		wantCols   int
		wantParams int
	}{
		{
			name:       "wildcard produces default columns",
			query:      "SELECT * FROM db.tbl",
			wantCols:   3, // time, measure_name, measure_value
			wantParams: 0,
		},
		{
			name:       "explicit projection infers column names",
			query:      "SELECT time, measure_name FROM db.tbl",
			wantCols:   2,
			wantParams: 0,
		},
		{
			name:       "COUNT aggregate infers BIGINT",
			query:      "SELECT COUNT(*) AS cnt FROM db.tbl",
			wantCols:   1,
			wantParams: 0,
		},
		{
			name:       "parameter markers inferred",
			query:      "SELECT * FROM db.tbl WHERE measure_name = ?",
			wantCols:   3,
			wantParams: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			rec := doRequest(t, h, "PrepareQuery", map[string]any{"QueryString": tt.query})
			require.Equal(t, http.StatusOK, rec.Code)
			resp := parseResponse(t, rec)

			cols := resp["Columns"].([]any)
			assert.Len(t, cols, tt.wantCols)
			params := resp["Parameters"].([]any)
			assert.Len(t, params, tt.wantParams)
		})
	}
}

func TestPrepareQuery_ValidateOnlyReturnsEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := doRequest(t, h, "PrepareQuery", map[string]any{
		"QueryString":  "SELECT * FROM db.tbl",
		"ValidateOnly": true,
	})

	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)

	cols := resp["Columns"].([]any)
	assert.Empty(t, cols, "ValidateOnly returns no columns")
}

// ---------------------------------------------------------------------------
// ScheduleExpression validation (gap #23)
// ---------------------------------------------------------------------------

func TestCreateScheduledQuery_ScheduleExpressionValidation(t *testing.T) {
	t.Parallel()

	base := map[string]any{
		"Name":                           "test-sq",
		"QueryString":                    "SELECT 1",
		"ScheduledQueryExecutionRoleArn": "arn:aws:iam::123:role/r",
		"ScheduleConfiguration": map[string]any{
			"ScheduleExpression": "PLACEHOLDER",
		},
	}

	tests := []struct {
		name     string
		expr     string
		wantCode int
	}{
		{"rate minutes valid", "rate(5 minutes)", http.StatusOK},
		{"rate hour valid", "rate(1 hour)", http.StatusOK},
		{"rate days valid", "rate(3 days)", http.StatusOK},
		{"cron 6 fields valid", "cron(0 12 * * ? *)", http.StatusOK},
		{"cron 5 fields invalid", "cron(0 12 * * ?)", http.StatusBadRequest},
		{"arbitrary string invalid", "every 5 minutes", http.StatusBadRequest},
		{"empty invalid", "", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			body := clone(base)
			if tt.expr == "" {
				body["ScheduleConfiguration"] = map[string]any{"ScheduleExpression": ""}
			} else {
				body["ScheduleConfiguration"] = map[string]any{"ScheduleExpression": tt.expr}
			}
			rec := doRequest(t, h, "CreateScheduledQuery", body)
			assert.Equal(t, tt.wantCode, rec.Code, "expr=%q", tt.expr)
		})
	}
}

// clone deep-copies the top-level string→any map for test isolation.
func clone(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	maps.Copy(out, m)

	return out
}

// ---------------------------------------------------------------------------
// ConflictException → HTTP 409 (gap #25)
// ---------------------------------------------------------------------------

func TestCreateScheduledQuery_ConflictReturns409(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	body := map[string]any{
		"Name":                           "dup-sq",
		"QueryString":                    "SELECT 1",
		"ScheduledQueryExecutionRoleArn": "arn:aws:iam::123:role/r",
		"ScheduleConfiguration":          map[string]any{"ScheduleExpression": "rate(1 hour)"},
	}

	rec1 := doRequest(t, h, "CreateScheduledQuery", body)
	require.Equal(t, http.StatusOK, rec1.Code)

	rec2 := doRequest(t, h, "CreateScheduledQuery", body)
	assert.Equal(t, http.StatusConflict, rec2.Code)

	var errBody map[string]string
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &errBody))
	assert.Equal(t, "ConflictException", errBody["__type"])
}

// ---------------------------------------------------------------------------
// ListScheduledQueries — enriched response with pagination (gaps #18, #19)
// ---------------------------------------------------------------------------

func TestListScheduledQueries_EnrichedResponse(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// Create a query so there's something to list.
	createBody := map[string]any{
		"Name":                           "enriched-sq",
		"QueryString":                    "SELECT 1",
		"ScheduledQueryExecutionRoleArn": "arn:aws:iam::123:role/r",
		"ScheduleConfiguration":          map[string]any{"ScheduleExpression": "rate(1 hour)"},
		"TargetConfiguration": map[string]any{
			"TimestreamConfiguration": map[string]any{
				"DatabaseName": "mydb",
				"TableName":    "mytable",
			},
		},
	}
	rec := doRequest(t, h, "CreateScheduledQuery", createBody)
	require.Equal(t, http.StatusOK, rec.Code)

	// List and verify enriched fields.
	listRec := doRequest(t, h, "ListScheduledQueries", map[string]any{})
	require.Equal(t, http.StatusOK, listRec.Code)

	listResp := parseResponse(t, listRec)
	items, ok := listResp["ScheduledQueries"].([]any)
	require.True(t, ok)
	require.Len(t, items, 1)

	item := items[0].(map[string]any)
	assert.Equal(t, "enriched-sq", item["Name"])
	assert.Equal(t, "ENABLED", item["State"])
	assert.NotEmpty(t, item["CreationTime"], "CreationTime should be populated")
	assert.NotEmpty(t, item["NextInvocationTime"], "NextInvocationTime should be derived from schedule")

	// Target destination should be populated.
	dest, hasDest := item["TargetDestination"]
	assert.True(t, hasDest)
	assert.NotNil(t, dest)
}

func TestListScheduledQueries_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// Create 3 queries.
	for i := range 3 {
		body := map[string]any{
			"Name":                           "paged-sq-" + string(rune('a'+i)),
			"QueryString":                    "SELECT 1",
			"ScheduledQueryExecutionRoleArn": "arn:aws:iam::123:role/r",
			"ScheduleConfiguration":          map[string]any{"ScheduleExpression": "rate(1 hour)"},
		}
		rec := doRequest(t, h, "CreateScheduledQuery", body)
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// Page 1: MaxResults=2.
	listRec1 := doRequest(t, h, "ListScheduledQueries", map[string]any{"MaxResults": 2})
	require.Equal(t, http.StatusOK, listRec1.Code)
	resp1 := parseResponse(t, listRec1)

	items1 := resp1["ScheduledQueries"].([]any)
	assert.Len(t, items1, 2)
	assert.NotEmpty(t, resp1["NextToken"], "NextToken must be set when more items remain")

	// Page 2 using NextToken.
	listRec2 := doRequest(t, h, "ListScheduledQueries", map[string]any{
		"NextToken": resp1["NextToken"],
	})
	require.Equal(t, http.StatusOK, listRec2.Code)
	resp2 := parseResponse(t, listRec2)

	items2 := resp2["ScheduledQueries"].([]any)
	assert.Len(t, items2, 1)
	_, hasNext := resp2["NextToken"]
	assert.False(t, hasNext, "No NextToken on last page")
}

// ---------------------------------------------------------------------------
// DescribeScheduledQuery — NextInvocationTime / LastRunSummary (gaps #20, #21)
// ---------------------------------------------------------------------------

func TestDescribeScheduledQuery_NextPreviousInvocationTime(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	backend := timestreamquery.ExportBackend(h)
	backend.AddScheduledQueryInternal(&timestreamquery.ScheduledQuery{
		Arn:                "arn:aws:timestream:us-east-1:123:scheduled-query/inv-test",
		Name:               "inv-test",
		State:              "ENABLED",
		ScheduleExpression: "rate(1 hour)",
		QueryString:        "SELECT 1",
	})

	rec := doRequest(t, h, "DescribeScheduledQuery", map[string]any{
		"ScheduledQueryArn": "arn:aws:timestream:us-east-1:123:scheduled-query/inv-test",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)

	sq, ok := resp["ScheduledQuery"].(map[string]any)
	require.True(t, ok, "response must have ScheduledQuery key")

	assert.NotEmpty(t, sq["NextInvocationTime"], "NextInvocationTime must be derived from schedule")
	// No previous invocation if never run.
	_, hasPrev := sq["PreviousInvocationTime"]
	assert.False(t, hasPrev)
}

func TestDescribeScheduledQuery_LastRunSummaryFullFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	backend := timestreamquery.ExportBackend(h)

	sq := &timestreamquery.ScheduledQuery{
		Arn:                "arn:aws:timestream:us-east-1:123:scheduled-query/run-test",
		Name:               "run-test",
		State:              "ENABLED",
		ScheduleExpression: "rate(30 minutes)",
		QueryString:        "SELECT 1",
	}
	backend.AddScheduledQueryInternal(sq)

	// Trigger an execution.
	doRequest(t, h, "ExecuteScheduledQuery", map[string]any{
		"ScheduledQueryArn": sq.Arn,
		"InvocationTime":    1715000000.0,
	})

	rec := doRequest(t, h, "DescribeScheduledQuery", map[string]any{
		"ScheduledQueryArn": sq.Arn,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)

	sqView, ok := resp["ScheduledQuery"].(map[string]any)
	require.True(t, ok)

	lastRun, ok := sqView["LastRunSummary"].(map[string]any)
	require.True(t, ok, "LastRunSummary must be present after execution")
	assert.NotEmpty(t, lastRun["RunStatus"])
	assert.NotEmpty(t, lastRun["InvocationTime"])
	assert.NotEmpty(t, lastRun["TriggerTime"])
	assert.NotNil(t, lastRun["ExecutionStats"])
	assert.NotEmpty(t, sqView["PreviousInvocationTime"])
	assert.NotEmpty(t, sqView["NextInvocationTime"])
}

// ---------------------------------------------------------------------------
// AccountSettings — LastUpdatedTime + default COMPUTE_UNITS (gaps #13, #14)
// ---------------------------------------------------------------------------

func TestAccountSettings_DefaultComputeUnits(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := doRequest(t, h, "DescribeAccountSettings", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)
	assert.Equal(t, "COMPUTE_UNITS", resp["QueryPricingModel"])
}

func TestAccountSettings_LastUpdatedTimeSetOnUpdate(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := doRequest(t, h, "UpdateAccountSettings", map[string]any{
		"QueryPricingModel": "BYTES_SCANNED",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)
	assert.NotNil(t, resp["LastUpdatedTime"], "LastUpdatedTime must be set after update")
}

// ---------------------------------------------------------------------------
// Backend unit tests for accuracy helpers
// ---------------------------------------------------------------------------

func TestValidateScheduleExpression(t *testing.T) {
	t.Parallel()

	backend := timestreamquery.NewInMemoryBackend("123", "us-east-1")

	validExprs := []string{
		"rate(1 minute)",
		"rate(5 minutes)",
		"rate(2 hours)",
		"rate(7 days)",
		"cron(0 12 * * ? *)",
		"cron(0/5 * * * ? *)",
	}
	invalidExprs := []string{
		"",
		"every 5 minutes",
		"cron(0 12 * * ?)", // only 5 fields
		"cron()",           // empty
		"rate()",
		"rate(five minutes)",
	}

	for _, expr := range validExprs {
		_, err := backend.CreateScheduledQuery(
			t.Context(), "valid-"+expr[:4], "SELECT 1", expr, "arn", "", "", "", "", nil,
		)
		require.NoError(t, err, "valid expr %q should be accepted", expr)
		_ = backend.DeleteScheduledQuery(
			t.Context(),
			"arn:aws:timestream:us-east-1:123:scheduled-query/valid-"+expr[:4],
		)
	}

	for _, expr := range invalidExprs {
		_, err := backend.CreateScheduledQuery(
			t.Context(), "inv", "SELECT 1", expr, "arn", "", "", "", "", nil,
		)
		require.Error(t, err, "invalid expr %q should be rejected", expr)
	}
}
