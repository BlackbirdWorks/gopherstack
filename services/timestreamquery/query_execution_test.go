package timestreamquery_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/timestreamquery"
)

func TestTimestreamQueryHandler_Query(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body        map[string]any
		name        string
		wantCode    int
		wantQueryID bool
	}{
		{
			name:        "success",
			body:        map[string]any{"QueryString": "SELECT * FROM my_table"},
			wantCode:    http.StatusOK,
			wantQueryID: true,
		},
		{
			name:     "missing query string",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			rec := doRequest(t, h, "Query", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantQueryID {
				resp := parseResponse(t, rec)
				assert.NotEmpty(t, resp["QueryId"])
			}
		})
	}
}

// TestQuery_TypedResponse — gaps #2, #3, #4.
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

// TestQuery_MaxRowsValidation — gap #8.
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

// TestQuery_ClientTokenIdempotency — gap #6.
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

// TestQuery_QueryInsightsIncluded — gap #5.
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

// TestParity_CancelQuery_IncludesCancellationMessage verifies that CancelQuery
// returns a CancellationMessage field. Real AWS CancelQueryOutput includes this
// field; the emulator previously returned an empty object.
func TestCancelQuery_IncludesCancellationMessage(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// Issue a query so there is a valid QueryId to cancel.
	qRec := doRequest(t, h, "Query", map[string]any{"QueryString": "SELECT 1"})
	require.Equal(t, http.StatusOK, qRec.Code)
	qResp := parseResponse(t, qRec)
	queryID, _ := qResp["QueryId"].(string)
	require.NotEmpty(t, queryID)

	rec := doRequest(t, h, "CancelQuery", map[string]any{"QueryId": queryID})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResponse(t, rec)
	msg, ok := resp["CancellationMessage"].(string)
	assert.True(t, ok, "CancellationMessage must be a string")
	assert.NotEmpty(t, msg, "CancellationMessage must not be empty")
}

func TestTimestreamQueryHandler_CancelQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *timestreamquery.Handler) string
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "cancel existing query",
			setup: func(t *testing.T, h *timestreamquery.Handler) string {
				t.Helper()
				rec := doRequest(t, h, "Query", map[string]any{"QueryString": "SELECT 1"})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseResponse(t, rec)

				return resp["QueryId"].(string)
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "missing query id",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "query not found",
			body:     map[string]any{"QueryId": "nonexistent"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()

			body := tt.body
			if tt.setup != nil {
				qid := tt.setup(t, h)
				body = map[string]any{"QueryId": qid}
			}

			rec := doRequest(t, h, "CancelQuery", body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestCancelQuery_UnknownIDReturnsValidationException — gap #9.
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

func TestTimestreamQueryBackend_QueryCap(t *testing.T) {
	t.Parallel()

	b := timestreamquery.NewInMemoryBackend("123456789012", "us-east-1")

	for range timestreamquery.MaxRetainedQueries + 100 {
		_ = b.Query(t.Context(), "SELECT 1")
	}

	assert.LessOrEqual(t, timestreamquery.QueryCount(b),
		timestreamquery.MaxRetainedQueries,
		"queries map must stay at or below the cap")
}

func TestTimestreamQueryBackend_CancelEvictedIsNotFound(t *testing.T) {
	t.Parallel()

	b := timestreamquery.NewInMemoryBackend("123456789012", "us-east-1")

	first := b.Query(t.Context(), "SELECT 1")
	for range timestreamquery.MaxRetainedQueries + 1 {
		_ = b.Query(t.Context(), "SELECT 1")
	}

	// `first` may or may not have been evicted (random map iter); if it
	// was, CancelQuery must report ErrNotFound rather than silently succeed
	// or panic.
	err := b.CancelQuery(t.Context(), first.QueryID)
	if err != nil {
		require.ErrorIs(t, err, timestreamquery.ErrNotFound)
	}
}

// TestQueryCount_Export verifies the exported QueryCount helper.
func TestQueryCount_Export(t *testing.T) {
	t.Parallel()

	b := timestreamquery.NewInMemoryBackend("000000000000", "us-east-1")
	assert.Equal(t, 0, timestreamquery.QueryCount(b))
}

func TestTimestreamQueryBackend_QueryCountTrack(t *testing.T) {
	t.Parallel()

	backend, h := newTestBackendAndHandler()

	assert.Equal(t, 0, timestreamquery.QueryCount(backend))

	rec := doRequest(t, h, "Query", map[string]any{"QueryString": "SELECT 1"})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, timestreamquery.QueryCount(backend))

	// CancelQuery is documented as idempotent: it marks the result cancelled
	// in place rather than deleting it, so a repeat cancellation of the same
	// QueryId still succeeds (CancelQueryOutput.CancellationMessage) instead
	// of 404ing, and the query still counts until evicted by the retention cap.
	qid := parseResponse(t, rec)["QueryId"].(string)
	rec = doRequest(t, h, "CancelQuery", map[string]any{"QueryId": qid})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, timestreamquery.QueryCount(backend))

	// A repeat cancellation of the same QueryId must still succeed.
	rec = doRequest(t, h, "CancelQuery", map[string]any{"QueryId": qid})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, timestreamquery.QueryCount(backend))
}

// TestPrepareQuery_InfersColumns — gap #10.
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

// TestPrepareQuery_ValidateOnlyStillInfersColumns verifies that ValidateOnly=true
// does not suppress the inferred Columns/Parameters. Real Timestream documents
// ValidateOnly=true as the only supported mode for PrepareQuery, and
// PrepareQueryOutput.Columns/Parameters are required response fields regardless
// of ValidateOnly -- describing the query's shape is the entire point of the
// call. An earlier version of this emulator returned an empty Columns list
// whenever ValidateOnly was true, which discarded the inferred result for the
// one mode real clients actually use.
func TestPrepareQuery_ValidateOnlyStillInfersColumns(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := doRequest(t, h, "PrepareQuery", map[string]any{
		"QueryString":  "SELECT * FROM db.tbl",
		"ValidateOnly": true,
	})

	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)

	cols := resp["Columns"].([]any)
	assert.Len(t, cols, 3, "ValidateOnly must still return the inferred columns (time, measure_name, measure_value)")
}

func TestTimestreamQueryHandler_PrepareQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body         map[string]any
		name         string
		wantQueryStr string
		wantCode     int
	}{
		{
			name:         "basic prepare",
			body:         map[string]any{"QueryString": "SELECT * FROM my_db.my_table"},
			wantCode:     http.StatusOK,
			wantQueryStr: "SELECT * FROM my_db.my_table",
		},
		{
			name:         "prepare with validate only flag",
			body:         map[string]any{"QueryString": "SELECT 1", "ValidateOnly": true},
			wantCode:     http.StatusOK,
			wantQueryStr: "SELECT 1",
		},
		{
			name:     "missing query string",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doRequest(t, h, "PrepareQuery", tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantQueryStr != "" {
				resp := parseResponse(t, rec)
				assert.Equal(t, tt.wantQueryStr, resp["QueryString"])

				cols, ok := resp["Columns"].([]any)
				require.True(t, ok, "Columns should be a list")
				// PrepareQuery infers columns from the projection; may be non-empty.
				_ = cols

				params, ok := resp["Parameters"].([]any)
				require.True(t, ok, "Parameters should be a list")
				assert.Empty(t, params)
			}
		})
	}
}

func TestTimestreamQueryHandler_PrepareQuery_BackendError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		queryString string
		wantErr     bool
	}{
		{
			name:        "empty query string returns error",
			queryString: "",
			wantErr:     true,
		},
		{
			name:        "non-empty query string succeeds",
			queryString: "SELECT 1",
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := newTestHandler().Backend
			result, err := backend.PrepareQuery(t.Context(), tt.queryString, false)

			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, result)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, result)
			}
		})
	}
}
