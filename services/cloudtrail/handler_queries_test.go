package cloudtrail_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/cloudtrail"
)

// TestCloudTrailQuery exercises CancelQuery and DescribeQuery.
func TestCloudTrailQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *cloudtrail.Handler)
		name string
	}{
		{
			name: "cancel_query_not_found",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "CancelQuery", map[string]any{
					"QueryId": "query-missing",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "describe_query_not_found",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "DescribeQuery", map[string]any{
					"QueryId": "query-missing",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "cancel_and_describe_query",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				// Create a query via StartQuery in backend directly
				q, err := h.Backend.StartQuery("SELECT eventName, eventSource FROM events LIMIT 10", "", "", "")
				require.NoError(t, err)
				require.NotEmpty(t, q.QueryID)

				cancelRec := doCloudTrailOp(t, h, "CancelQuery", map[string]any{
					"QueryId": q.QueryID,
				})
				assert.Equal(t, http.StatusOK, cancelRec.Code)
				cancelResp := parseCloudTrailResp(t, cancelRec)
				assert.Equal(t, "CANCELLED", cancelResp["QueryStatus"])

				descRec := doCloudTrailOp(t, h, "DescribeQuery", map[string]any{
					"QueryId": q.QueryID,
				})
				assert.Equal(t, http.StatusOK, descRec.Code)
				descResp := parseCloudTrailResp(t, descRec)
				assert.Equal(t, "CANCELLED", descResp["QueryStatus"])
				assert.Equal(t, "SELECT eventName, eventSource FROM events LIMIT 10", descResp["QueryString"])
			},
		},
		{
			name: "cancel_already_cancelled_query",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				q, err := h.Backend.StartQuery("SELECT 1", "", "", "")
				require.NoError(t, err)

				// Cancel it once
				_, err = h.Backend.CancelQuery(q.QueryID)
				require.NoError(t, err)

				// Try cancelling again - should fail with validation error
				rec := doCloudTrailOp(t, h, "CancelQuery", map[string]any{
					"QueryId": q.QueryID,
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestCloudTrailHandler()
			tt.ops(t, h)
		})
	}
}

// TestCloudTrailGenerateQuery exercises GenerateQuery: required-parameter
// validation and the AWS response shape (QueryStatement/QueryAlias/
// EventDataStoreOwnerAccountId -- not QueryId/QueryString).
func TestCloudTrailGenerateQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *cloudtrail.Handler)
		name string
	}{
		{
			name: "success",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				edsRec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{"Name": "gq-eds"})
				require.Equal(t, http.StatusOK, edsRec.Code)
				edsARN, _ := parseCloudTrailResp(t, edsRec)["EventDataStoreArn"].(string)

				rec := doCloudTrailOp(t, h, "GenerateQuery", map[string]any{
					"EventDataStores": []string{edsARN},
					"Prompt":          "show me all console logins",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)

				stmt, ok := resp["QueryStatement"].(string)
				require.True(t, ok, "QueryStatement must be present and a string")
				assert.NotEmpty(t, stmt)
				assert.NotEmpty(t, resp["QueryAlias"])
				assert.Equal(t, "123456789012", resp["EventDataStoreOwnerAccountId"])

				_, hasQueryID := resp["QueryId"]
				assert.False(t, hasQueryID, "GenerateQueryOutput has no QueryId field")
				_, hasQueryString := resp["QueryString"]
				assert.False(t, hasQueryString, "GenerateQueryOutput has no QueryString field")
			},
		},
		{
			name: "missing_event_data_stores",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "GenerateQuery", map[string]any{
					"Prompt": "show me all console logins",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "missing_prompt",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "GenerateQuery", map[string]any{
					"EventDataStores": []string{"eds-000001"},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestCloudTrailHandler()
			tt.ops(t, h)
		})
	}
}

// TestCloudTrailQueryLifecycle covers StartQuery, GetQueryResults, ListQueries.
func TestCloudTrailQueryLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()

	// Create an EDS first.
	rec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{"Name": "query-eds"})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseCloudTrailResp(t, rec)
	edsARN, _ := resp["EventDataStoreArn"].(string)
	require.NotEmpty(t, edsARN)

	// StartQuery.
	rec = doCloudTrailOp(t, h, "StartQuery", map[string]any{
		"QueryStatement": "SELECT * FROM " + edsARN + " LIMIT 10",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var queryID string

	if rec.Code == http.StatusOK {
		qresp := parseCloudTrailResp(t, rec)
		queryID, _ = qresp["QueryId"].(string)
	}

	// ListQueries.
	rec = doCloudTrailOp(t, h, "ListQueries", map[string]any{"EventDataStore": edsARN})
	assert.Equal(t, http.StatusOK, rec.Code)

	// GetQueryResults (if we have a query ID).
	if queryID != "" {
		rec = doCloudTrailOp(t, h, "GetQueryResults", map[string]any{
			"EventDataStore": edsARN,
			"QueryId":        queryID,
		})
		assert.Equal(t, http.StatusOK, rec.Code)
	}
}

// TestDescribeQueryCreationTime verifies DescribeQuery returns CreationTime
// nested inside QueryStatistics (QueryStatisticsForDescribeQuery), matching
// the real DescribeQueryOutput shape -- which has no top-level CreationTime
// field at all.
func TestDescribeQueryCreationTime(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()

	edsRec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{"Name": "dq-ct-eds"})
	require.Equal(t, http.StatusOK, edsRec.Code)
	edsARN, _ := parseCloudTrailResp(t, edsRec)["EventDataStoreArn"].(string)

	startRec := doCloudTrailOp(t, h, "StartQuery", map[string]any{
		"QueryStatement": "SELECT * FROM " + edsARN + " LIMIT 1",
	})
	require.Equal(t, http.StatusOK, startRec.Code)
	queryID, _ := parseCloudTrailResp(t, startRec)["QueryId"].(string)
	require.NotEmpty(t, queryID)

	descRec := doCloudTrailOp(t, h, "DescribeQuery", map[string]any{
		"QueryId": queryID,
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	resp := parseCloudTrailResp(t, descRec)
	_, hasTopLevel := resp["CreationTime"]
	assert.False(t, hasTopLevel, "DescribeQueryOutput has no top-level CreationTime field")

	stats, ok := resp["QueryStatistics"].(map[string]any)
	require.True(t, ok, "DescribeQuery must return QueryStatistics")
	assert.NotNil(t, stats["CreationTime"], "QueryStatistics.CreationTime must be present")
}

// TestGetQueryResultsQueryStatistics verifies GetQueryResults returns QueryStatistics.
func TestGetQueryResultsQueryStatistics(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()

	edsRec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{"Name": "gqr-qs-eds"})
	require.Equal(t, http.StatusOK, edsRec.Code)
	edsARN, _ := parseCloudTrailResp(t, edsRec)["EventDataStoreArn"].(string)

	startRec := doCloudTrailOp(t, h, "StartQuery", map[string]any{
		"QueryStatement": "SELECT * FROM " + edsARN + " LIMIT 1",
	})
	require.Equal(t, http.StatusOK, startRec.Code)
	queryID, _ := parseCloudTrailResp(t, startRec)["QueryId"].(string)
	require.NotEmpty(t, queryID)

	getRec := doCloudTrailOp(t, h, "GetQueryResults", map[string]any{
		"EventDataStore": edsARN,
		"QueryId":        queryID,
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	resp := parseCloudTrailResp(t, getRec)
	stats, hasStats := resp["QueryStatistics"]
	assert.True(t, hasStats, "GetQueryResults must return QueryStatistics")
	statsMap, ok := stats.(map[string]any)
	require.True(t, ok, "QueryStatistics must be an object")
	_, hasTRC := statsMap["TotalResultsCount"]
	assert.True(t, hasTRC, "QueryStatistics must contain TotalResultsCount")
}

// TestListQueries_NextTokenPagination verifies ListQueries honors
// NextToken/MaxResults pagination (previously always returned every query
// in one page) and filters by EventDataStore.
func TestListQueries_NextTokenPagination(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()

	edsRec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{"Name": "list-queries-eds"})
	require.Equal(t, http.StatusOK, edsRec.Code)
	edsARN, _ := parseCloudTrailResp(t, edsRec)["EventDataStoreArn"].(string)

	for i := range 3 {
		rec := doCloudTrailOp(t, h, "StartQuery", map[string]any{
			"QueryStatement": fmt.Sprintf("SELECT * FROM %s WHERE eventName = 'Op%d'", edsARN, i),
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := doCloudTrailOp(t, h, "ListQueries", map[string]any{"EventDataStore": edsARN, "MaxResults": 2})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseCloudTrailResp(t, rec)
	queries, ok := resp["Queries"].([]any)
	require.True(t, ok)
	require.Len(t, queries, 2)
	nextToken, _ := resp["NextToken"].(string)
	require.NotEmpty(t, nextToken)

	rec = doCloudTrailOp(t, h, "ListQueries", map[string]any{"EventDataStore": edsARN, "NextToken": nextToken})
	require.Equal(t, http.StatusOK, rec.Code)
	resp = parseCloudTrailResp(t, rec)
	queries, ok = resp["Queries"].([]any)
	require.True(t, ok)
	assert.Len(t, queries, 1)

	// A non-matching EventDataStore filter must exclude all three queries.
	rec = doCloudTrailOp(t, h, "ListQueries", map[string]any{"EventDataStore": "eds-does-not-exist"})
	require.Equal(t, http.StatusOK, rec.Code)
	resp = parseCloudTrailResp(t, rec)
	queries, ok = resp["Queries"].([]any)
	require.True(t, ok)
	assert.Empty(t, queries)
}

// TestGetQueryResults_RealRowsAndPagination verifies GetQueryResults
// executes the query for real (rows reflect recorded events, not an
// unconditional empty list) and paginates via NextToken/MaxQueryResults.
func TestGetQueryResults_RealRowsAndPagination(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()
	h.Backend.RecordManagementEvent(service.CloudTrailEventInput{EventName: "Op1", EventSource: "svc.amazonaws.com"})
	h.Backend.RecordManagementEvent(service.CloudTrailEventInput{EventName: "Op2", EventSource: "svc.amazonaws.com"})

	edsRec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{"Name": "gqr-rows-eds"})
	require.Equal(t, http.StatusOK, edsRec.Code)
	edsARN, _ := parseCloudTrailResp(t, edsRec)["EventDataStoreArn"].(string)

	startRec := doCloudTrailOp(t, h, "StartQuery", map[string]any{
		"QueryStatement": "SELECT * FROM " + edsARN,
	})
	require.Equal(t, http.StatusOK, startRec.Code)
	queryID, _ := parseCloudTrailResp(t, startRec)["QueryId"].(string)
	require.NotEmpty(t, queryID)

	getRec := doCloudTrailOp(t, h, "GetQueryResults", map[string]any{
		"QueryId": queryID, "MaxQueryResults": 1,
	})
	require.Equal(t, http.StatusOK, getRec.Code)
	resp := parseCloudTrailResp(t, getRec)

	rows, ok := resp["QueryResultRows"].([]any)
	require.True(t, ok)
	require.Len(t, rows, 1, "MaxQueryResults=1 must cap the page to one row")

	stats := resp["QueryStatistics"].(map[string]any)
	assert.EqualValues(t, 2, stats["TotalResultsCount"], "both recorded events must have matched SELECT * FROM <eds>")

	nextToken, _ := resp["NextToken"].(string)
	require.NotEmpty(t, nextToken)

	getRec2 := doCloudTrailOp(t, h, "GetQueryResults", map[string]any{
		"QueryId": queryID, "NextToken": nextToken,
	})
	require.Equal(t, http.StatusOK, getRec2.Code)
	resp2 := parseCloudTrailResp(t, getRec2)
	rows2, ok := resp2["QueryResultRows"].([]any)
	require.True(t, ok)
	assert.Len(t, rows2, 1, "the second page must contain the remaining row")
}
