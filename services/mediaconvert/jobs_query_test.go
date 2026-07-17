package mediaconvert_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediaconvert"
)

func TestMediaConvert_GetJobsQueryResults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		queryID    string
		wantStatus int
		wantJobs   bool
	}{
		{
			name:       "returns_empty_results_for_any_id",
			queryID:    "query-abc-123",
			wantStatus: http.StatusOK,
			wantJobs:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			resp, code := parseJSONResponse(t, h, http.MethodGet, "/2017-08-29/jobsQueries/"+tt.queryID, nil)
			assert.Equal(t, tt.wantStatus, code)

			if tt.wantJobs {
				_, ok := resp["jobs"]
				assert.True(t, ok, "response should contain 'jobs' key")
			}
		})
	}
}

// TestGetJobsQueryResults_UnknownQueryID_BackendReturnsEmpty verifies the
// backend returns an empty result set for a queryID that was never produced
// by StartJobsQuery.
func TestGetJobsQueryResults_UnknownQueryID_BackendReturnsEmpty(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b.CreateJob("arn:aws:iam::123:role/r", "", "", nil, nil, nil, "")
	require.NoError(t, err)

	results := b.GetJobsQueryResults("any-query-id")
	assert.Empty(t, results)
}

func TestGetJobsQueryResults_UnknownID_ViaHTTP_ReturnsEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	resp, code := parseJSONResponse(t, h, http.MethodGet, "/2017-08-29/jobsQueries/nonexistent-id", nil)
	assert.Equal(t, http.StatusOK, code)

	jobs, _ := resp["jobs"].([]any)
	assert.Empty(t, jobs)
}

func TestStartJobsQuery_ReturnsQueryID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	resp, code := parseJSONResponse(t, h, http.MethodPost, "/2017-08-29/jobsQueries", map[string]any{
		"filterList": []any{},
	})
	assert.Equal(t, http.StatusOK, code)

	queryID, _ := resp["id"].(string)
	assert.NotEmpty(t, queryID)
}

func TestStartJobsQuery_GetResults_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestJob(t, h)
	createTestJob(t, h)

	// Start a query with no filters — should match all jobs.
	startResp, startCode := parseJSONResponse(t, h, http.MethodPost, "/2017-08-29/jobsQueries", map[string]any{
		"filterList": []any{},
	})
	require.Equal(t, http.StatusOK, startCode)

	queryID, _ := startResp["id"].(string)
	require.NotEmpty(t, queryID)

	// Get results for that query ID.
	getResp, getCode := parseJSONResponse(t, h, http.MethodGet, "/2017-08-29/jobsQueries/"+queryID, nil)
	assert.Equal(t, http.StatusOK, getCode)

	jobs, _ := getResp["jobs"].([]any)
	assert.Len(t, jobs, 2)

	// Results are computed synchronously, so status must always be COMPLETE.
	assert.Equal(t, "COMPLETE", getResp["status"])
}

func TestStartJobsQuery_WithStatusFilter_ReturnsMatchingJobs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestJob(t, h)

	startResp, startCode := parseJSONResponse(t, h, http.MethodPost, "/2017-08-29/jobsQueries", map[string]any{
		"filterList": []any{
			map[string]any{
				"key":    "status",
				"values": []any{"SUBMITTED"},
			},
		},
	})
	require.Equal(t, http.StatusOK, startCode)

	queryID, _ := startResp["id"].(string)
	require.NotEmpty(t, queryID)

	getResp, getCode := parseJSONResponse(t, h, http.MethodGet, "/2017-08-29/jobsQueries/"+queryID, nil)
	assert.Equal(t, http.StatusOK, getCode)

	jobs, _ := getResp["jobs"].([]any)
	assert.Len(t, jobs, 1)
}

func TestStartJobsQuery_WithStatusFilter_NoMatch(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestJob(t, h)

	startResp, startCode := parseJSONResponse(t, h, http.MethodPost, "/2017-08-29/jobsQueries", map[string]any{
		"filterList": []any{
			map[string]any{
				"key":    "status",
				"values": []any{"COMPLETE"},
			},
		},
	})
	require.Equal(t, http.StatusOK, startCode)

	queryID, _ := startResp["id"].(string)
	require.NotEmpty(t, queryID)

	getResp, getCode := parseJSONResponse(t, h, http.MethodGet, "/2017-08-29/jobsQueries/"+queryID, nil)
	assert.Equal(t, http.StatusOK, getCode)

	jobs, _ := getResp["jobs"].([]any)
	assert.Empty(t, jobs)
}

func TestStartJobsQuery_WithMaxResults(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestJob(t, h)
	createTestJob(t, h)
	createTestJob(t, h)

	maxResults := 2

	startResp, startCode := parseJSONResponse(t, h, http.MethodPost, "/2017-08-29/jobsQueries", map[string]any{
		"filterList": []any{},
		"maxResults": maxResults,
	})
	require.Equal(t, http.StatusOK, startCode)

	queryID, _ := startResp["id"].(string)
	require.NotEmpty(t, queryID)

	getResp, getCode := parseJSONResponse(t, h, http.MethodGet, "/2017-08-29/jobsQueries/"+queryID, nil)
	assert.Equal(t, http.StatusOK, getCode)

	jobs, _ := getResp["jobs"].([]any)
	assert.Len(t, jobs, maxResults)
}

func TestStartJobsQuery_MultipleQueries_Independent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestJob(t, h)

	// Query 1: match SUBMITTED.
	startResp1, _ := parseJSONResponse(t, h, http.MethodPost, "/2017-08-29/jobsQueries", map[string]any{
		"filterList": []any{
			map[string]any{"key": "status", "values": []any{"SUBMITTED"}},
		},
	})
	qID1, _ := startResp1["id"].(string)
	require.NotEmpty(t, qID1)

	// Query 2: match COMPLETE (no match).
	startResp2, _ := parseJSONResponse(t, h, http.MethodPost, "/2017-08-29/jobsQueries", map[string]any{
		"filterList": []any{
			map[string]any{"key": "status", "values": []any{"COMPLETE"}},
		},
	})
	qID2, _ := startResp2["id"].(string)
	require.NotEmpty(t, qID2)

	assert.NotEqual(t, qID1, qID2, "each query should have a unique ID")

	// Validate q1 has results, q2 is empty.
	r1, _ := parseJSONResponse(t, h, http.MethodGet, "/2017-08-29/jobsQueries/"+qID1, nil)
	r2, _ := parseJSONResponse(t, h, http.MethodGet, "/2017-08-29/jobsQueries/"+qID2, nil)

	jobs1, _ := r1["jobs"].([]any)
	jobs2, _ := r2["jobs"].([]any)

	assert.Len(t, jobs1, 1)
	assert.Empty(t, jobs2)
}
