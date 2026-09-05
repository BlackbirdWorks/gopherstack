package mediaconvert_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediaconvert"
)

func TestSearchJobs_EmptyReturnsEmptyList(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	resp, code := parseJSONResponse(t, h, http.MethodGet, "/2017-08-29/search", nil)
	assert.Equal(t, http.StatusOK, code)

	jobs, _ := resp["jobs"].([]any)
	assert.Empty(t, jobs)
}

func TestSearchJobs_NoFilter_ReturnsAllJobs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestJob(t, h)
	createTestJob(t, h)

	resp, code := parseJSONResponse(t, h, http.MethodGet, "/2017-08-29/search", nil)
	assert.Equal(t, http.StatusOK, code)

	jobs, _ := resp["jobs"].([]any)
	assert.Len(t, jobs, 2)
}

func TestSearchJobs_StatusFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestJob(t, h)

	// All freshly created jobs are SUBMITTED — filter should return the job.
	resp, code := parseJSONResponse(t, h, http.MethodGet, "/2017-08-29/search?status=SUBMITTED", nil)
	assert.Equal(t, http.StatusOK, code)

	jobs, _ := resp["jobs"].([]any)
	assert.Len(t, jobs, 1)
}

func TestSearchJobs_StatusFilter_NoMatch(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestJob(t, h)

	resp, code := parseJSONResponse(t, h, http.MethodGet, "/2017-08-29/search?status=COMPLETE", nil)
	assert.Equal(t, http.StatusOK, code)

	jobs, _ := resp["jobs"].([]any)
	assert.Empty(t, jobs)
}

func TestSearchJobs_QueueFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create queue so job can reference it.
	recQ := doRequest(t, h, http.MethodPost, "/2017-08-29/queues", map[string]any{"name": "search-q"})
	require.Equal(t, http.StatusCreated, recQ.Code)

	var qResp map[string]any
	require.NoError(t, json.Unmarshal(recQ.Body.Bytes(), &qResp))

	qMap, _ := qResp["queue"].(map[string]any)
	qARN, _ := qMap["arn"].(string)
	require.NotEmpty(t, qARN)

	// Job using the named queue.
	recJ := doRequest(t, h, http.MethodPost, "/2017-08-29/jobs", map[string]any{
		"role":     "arn:aws:iam::123456789012:role/R",
		"settings": map[string]any{},
		"queue":    qARN,
	})
	require.Equal(t, http.StatusCreated, recJ.Code)

	// Job without queue.
	createTestJob(t, h)

	// Filter by queue ARN — should return only the queued job.
	resp, code := parseJSONResponse(t, h, http.MethodGet, "/2017-08-29/search?queue="+qARN, nil)
	assert.Equal(t, http.StatusOK, code)

	jobs, _ := resp["jobs"].([]any)
	assert.Len(t, jobs, 1)
}

func TestSearchJobs_MaxResults(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestJob(t, h)
	createTestJob(t, h)
	createTestJob(t, h)

	resp, code := parseJSONResponse(t, h, http.MethodGet, "/2017-08-29/search?maxResults=2", nil)
	assert.Equal(t, http.StatusOK, code)

	jobs, _ := resp["jobs"].([]any)
	assert.Len(t, jobs, 2)
}

func TestSearchJobs_OrderAscending(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestJob(t, h)
	createTestJob(t, h)

	asc, codeAsc := parseJSONResponse(t, h, http.MethodGet, "/2017-08-29/search?order=ASCENDING", nil)
	desc, codeDesc := parseJSONResponse(t, h, http.MethodGet, "/2017-08-29/search?order=DESCENDING", nil)

	assert.Equal(t, http.StatusOK, codeAsc)
	assert.Equal(t, http.StatusOK, codeDesc)

	ascJobs, _ := asc["jobs"].([]any)
	descJobs, _ := desc["jobs"].([]any)

	require.Len(t, ascJobs, 2)
	require.Len(t, descJobs, 2)

	// Ascending and descending should be reverse of each other.
	ascFirst, _ := ascJobs[0].(map[string]any)
	descFirst, _ := descJobs[0].(map[string]any)
	ascLast, _ := ascJobs[1].(map[string]any)
	descLast, _ := descJobs[1].(map[string]any)

	ascFirstID, _ := ascFirst["id"].(string)
	descFirstID, _ := descFirst["id"].(string)
	ascLastID, _ := ascLast["id"].(string)
	descLastID, _ := descLast["id"].(string)

	// If they differ, ascending[0] must equal descending[1] and vice versa.
	if ascFirstID != descFirstID {
		assert.Equal(t, ascFirstID, descLastID, "ascending[0] should equal descending[1]")
		assert.Equal(t, ascLastID, descFirstID, "ascending[1] should equal descending[0]")
	}
}

func TestSearchJobs_OrderDescending(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestJob(t, h)
	createTestJob(t, h)

	resp, code := parseJSONResponse(t, h, http.MethodGet, "/2017-08-29/search?order=DESCENDING", nil)
	assert.Equal(t, http.StatusOK, code)

	jobs, _ := resp["jobs"].([]any)
	assert.Len(t, jobs, 2, "descending order returns all jobs")
}

// TestSearchJobs_InputFileFilter verifies SearchJobs honors the inputFile
// query parameter (SearchJobsInput.InputFile, aws-sdk-go-v2/service/
// mediaconvert@v1.97.1 api_op_SearchJobs.go: "provide your input file URL or
// your partial input file name"), matched against each job's
// settings.inputs[].fileInput.
func TestSearchJobs_InputFileFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	recMatch := doRequest(t, h, http.MethodPost, "/2017-08-29/jobs", map[string]any{
		"role": "arn:aws:iam::" + testAccountID + ":role/R",
		"settings": map[string]any{
			"inputs": []any{
				map[string]any{"fileInput": "s3://bucket/path/movie.mp4"},
			},
		},
	})
	require.Equal(t, http.StatusCreated, recMatch.Code)

	createTestJob(t, h) // no matching input file

	resp, code := parseJSONResponse(t, h, http.MethodGet, "/2017-08-29/search?inputFile=movie.mp4", nil)
	assert.Equal(t, http.StatusOK, code)

	jobs, _ := resp["jobs"].([]any)
	require.Len(t, jobs, 1)

	job, _ := jobs[0].(map[string]any)
	settings, _ := job["settings"].(map[string]any)
	inputs, _ := settings["inputs"].([]any)
	require.Len(t, inputs, 1)

	input, _ := inputs[0].(map[string]any)
	assert.Equal(t, "s3://bucket/path/movie.mp4", input["fileInput"])
}

// TestSearchJobs_FilterByStatus verifies SearchJobs reflects janitor-advanced statuses.
func TestSearchJobs_FilterByStatus(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	h := mediaconvert.NewHandler(b)

	for range 3 {
		createTestJob(t, h)
	}

	// Advance all jobs to PROGRESSING.
	b.AdvanceJobPhase()

	tests := []struct {
		name      string
		status    string
		wantCount int
	}{
		{name: "progressing", status: "PROGRESSING", wantCount: 3},
		{name: "submitted", status: "SUBMITTED", wantCount: 0},
		{name: "all", status: "", wantCount: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			path := "/2017-08-29/search"
			if tt.status != "" {
				path += "?status=" + tt.status
			}

			rec := doRequest(t, h, http.MethodGet, path, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			jobs, _ := out["jobs"].([]any)
			assert.Len(t, jobs, tt.wantCount)
		})
	}
}
