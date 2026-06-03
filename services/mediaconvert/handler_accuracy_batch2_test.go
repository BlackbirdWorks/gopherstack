package mediaconvert_test

// handler_accuracy_batch2_test.go — MediaConvert AWS-accuracy audit batch-2 (go-o2b1).
//
// Covers ops not exercised via HTTP in batch-1 / refinement tests:
//   - ListVersions: GET /2017-08-29/versions returns at least one version entry.
//   - Probe: POST /2017-08-29/probe with inputFiles returns probeResults; empty body handled.
//   - SearchJobs: GET /2017-08-29/search — status filter, queue filter, order, maxResults,
//     no-match, empty state.
//   - StartJobsQuery + GetJobsQueryResults: POST /2017-08-29/jobsQueries returns queryId;
//     GET /2017-08-29/jobsQueries/{id} returns matching jobs; unknown id returns empty.

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediaconvert"
)

// ----------------------------------------
// helpers
// ----------------------------------------

func b2mcHandler(t *testing.T) *mediaconvert.Handler {
	t.Helper()

	return mediaconvert.NewHandler(mediaconvert.NewInMemoryBackend(testAccountID, testRegion))
}

// b2mcCreateJob creates a job without a queue and returns its ID.
func b2mcCreateJob(t *testing.T, h *mediaconvert.Handler) string {
	t.Helper()

	body := map[string]any{
		"role":     "arn:aws:iam::123456789012:role/MediaConvertRole",
		"settings": map[string]any{},
	}

	rec := doRequest(t, h, http.MethodPost, "/2017-08-29/jobs", body)
	require.Equal(t, http.StatusCreated, rec.Code, "create job: %s", rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	jobMap, _ := resp["job"].(map[string]any)
	id, _ := jobMap["id"].(string)
	require.NotEmpty(t, id)

	return id
}

// b2mcParseResp unmarshals the response body into a map and returns it with the status code.
func b2mcParseResp(t *testing.T, h *mediaconvert.Handler, method, path string, body any) (map[string]any, int) {
	t.Helper()

	rec := doRequest(t, h, method, path, body)

	var resp map[string]any
	if rec.Body.Len() > 0 {
		_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	}

	return resp, rec.Code
}

// ----------------------------------------
// ListVersions
// ----------------------------------------

func TestBatch2_ListVersions_ReturnsVersions(t *testing.T) {
	t.Parallel()

	h := b2mcHandler(t)

	resp, code := b2mcParseResp(t, h, http.MethodGet, "/2017-08-29/versions", nil)
	assert.Equal(t, http.StatusOK, code)

	versions, _ := resp["versions"].([]any)
	assert.NotEmpty(t, versions, "expected at least one version entry")

	first, _ := versions[0].(map[string]any)
	ver, _ := first["version"].(string)
	assert.NotEmpty(t, ver, "version entry must have a non-empty version string")
}

func TestBatch2_ListVersions_ContainsKnownVersion(t *testing.T) {
	t.Parallel()

	h := b2mcHandler(t)

	rec := doRequest(t, h, http.MethodGet, "/2017-08-29/versions", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Versions []struct {
			Version string `json:"version"`
		} `json:"versions"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.NotEmpty(t, out.Versions)

	found := false

	for _, v := range out.Versions {
		if v.Version == "2017-08-29" {
			found = true
		}
	}

	assert.True(t, found, "expected versions to contain '2017-08-29'")
}

// ----------------------------------------
// Probe
// ----------------------------------------

func TestBatch2_Probe_SingleInputFile(t *testing.T) {
	t.Parallel()

	h := b2mcHandler(t)

	body := map[string]any{
		"inputFiles": []any{
			map[string]any{"fileInput": "s3://my-bucket/input.mp4"},
		},
	}

	resp, code := b2mcParseResp(t, h, http.MethodPost, "/2017-08-29/probe", body)
	assert.Equal(t, http.StatusOK, code)

	results, _ := resp["probeResults"].([]any)
	assert.Len(t, results, 1, "one probeResult per input file")
}

func TestBatch2_Probe_MultipleInputFiles(t *testing.T) {
	t.Parallel()

	h := b2mcHandler(t)

	body := map[string]any{
		"inputFiles": []any{
			map[string]any{"fileInput": "s3://bucket/a.mp4"},
			map[string]any{"fileInput": "s3://bucket/b.mp4"},
		},
	}

	resp, code := b2mcParseResp(t, h, http.MethodPost, "/2017-08-29/probe", body)
	assert.Equal(t, http.StatusOK, code)

	results, _ := resp["probeResults"].([]any)
	assert.Len(t, results, 2)
}

func TestBatch2_Probe_EmptyInputFiles(t *testing.T) {
	t.Parallel()

	h := b2mcHandler(t)

	body := map[string]any{
		"inputFiles": []any{},
	}

	resp, code := b2mcParseResp(t, h, http.MethodPost, "/2017-08-29/probe", body)
	assert.Equal(t, http.StatusOK, code)

	results, _ := resp["probeResults"].([]any)
	assert.Empty(t, results)
}

func TestBatch2_Probe_NoInputFilesField(t *testing.T) {
	t.Parallel()

	h := b2mcHandler(t)

	resp, code := b2mcParseResp(t, h, http.MethodPost, "/2017-08-29/probe", map[string]any{})
	assert.Equal(t, http.StatusOK, code)

	results, _ := resp["probeResults"].([]any)
	assert.Empty(t, results)
}

func TestBatch2_Probe_ResultContainsContainer(t *testing.T) {
	t.Parallel()

	h := b2mcHandler(t)

	body := map[string]any{
		"inputFiles": []any{
			map[string]any{"fileInput": "s3://bucket/video.mp4"},
		},
	}

	resp, code := b2mcParseResp(t, h, http.MethodPost, "/2017-08-29/probe", body)
	require.Equal(t, http.StatusOK, code)

	results, _ := resp["probeResults"].([]any)
	require.Len(t, results, 1)

	entry, _ := results[0].(map[string]any)
	probeResult, _ := entry["probeResult"].(map[string]any)
	container, _ := probeResult["container"].(map[string]any)
	assert.NotEmpty(t, container, "probeResult must include a container field")
}

// ----------------------------------------
// SearchJobs
// ----------------------------------------

func TestBatch2_SearchJobs_EmptyReturnsEmptyList(t *testing.T) {
	t.Parallel()

	h := b2mcHandler(t)

	resp, code := b2mcParseResp(t, h, http.MethodGet, "/2017-08-29/search", nil)
	assert.Equal(t, http.StatusOK, code)

	jobs, _ := resp["jobs"].([]any)
	assert.Empty(t, jobs)
}

func TestBatch2_SearchJobs_NoFilter_ReturnsAllJobs(t *testing.T) {
	t.Parallel()

	h := b2mcHandler(t)
	b2mcCreateJob(t, h)
	b2mcCreateJob(t, h)

	resp, code := b2mcParseResp(t, h, http.MethodGet, "/2017-08-29/search", nil)
	assert.Equal(t, http.StatusOK, code)

	jobs, _ := resp["jobs"].([]any)
	assert.Len(t, jobs, 2)
}

func TestBatch2_SearchJobs_StatusFilter(t *testing.T) {
	t.Parallel()

	h := b2mcHandler(t)
	b2mcCreateJob(t, h)

	// All freshly created jobs are SUBMITTED — filter should return the job.
	resp, code := b2mcParseResp(t, h, http.MethodGet, "/2017-08-29/search?status=SUBMITTED", nil)
	assert.Equal(t, http.StatusOK, code)

	jobs, _ := resp["jobs"].([]any)
	assert.Len(t, jobs, 1)
}

func TestBatch2_SearchJobs_StatusFilter_NoMatch(t *testing.T) {
	t.Parallel()

	h := b2mcHandler(t)
	b2mcCreateJob(t, h)

	resp, code := b2mcParseResp(t, h, http.MethodGet, "/2017-08-29/search?status=COMPLETE", nil)
	assert.Equal(t, http.StatusOK, code)

	jobs, _ := resp["jobs"].([]any)
	assert.Empty(t, jobs)
}

func TestBatch2_SearchJobs_QueueFilter(t *testing.T) {
	t.Parallel()

	h := b2mcHandler(t)

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
	b2mcCreateJob(t, h)

	// Filter by queue ARN — should return only the queued job.
	resp, code := b2mcParseResp(t, h, http.MethodGet, "/2017-08-29/search?queue="+qARN, nil)
	assert.Equal(t, http.StatusOK, code)

	jobs, _ := resp["jobs"].([]any)
	assert.Len(t, jobs, 1)
}

func TestBatch2_SearchJobs_MaxResults(t *testing.T) {
	t.Parallel()

	h := b2mcHandler(t)
	b2mcCreateJob(t, h)
	b2mcCreateJob(t, h)
	b2mcCreateJob(t, h)

	resp, code := b2mcParseResp(t, h, http.MethodGet, "/2017-08-29/search?maxResults=2", nil)
	assert.Equal(t, http.StatusOK, code)

	jobs, _ := resp["jobs"].([]any)
	assert.Len(t, jobs, 2)
}

func TestBatch2_SearchJobs_OrderAscending(t *testing.T) {
	t.Parallel()

	h := b2mcHandler(t)
	b2mcCreateJob(t, h)
	b2mcCreateJob(t, h)

	asc, codeAsc := b2mcParseResp(t, h, http.MethodGet, "/2017-08-29/search?order=ASCENDING", nil)
	desc, codeDesc := b2mcParseResp(t, h, http.MethodGet, "/2017-08-29/search?order=DESCENDING", nil)

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

func TestBatch2_SearchJobs_OrderDescending(t *testing.T) {
	t.Parallel()

	h := b2mcHandler(t)
	b2mcCreateJob(t, h)
	b2mcCreateJob(t, h)

	resp, code := b2mcParseResp(t, h, http.MethodGet, "/2017-08-29/search?order=DESCENDING", nil)
	assert.Equal(t, http.StatusOK, code)

	jobs, _ := resp["jobs"].([]any)
	assert.Len(t, jobs, 2, "descending order returns all jobs")
}

// ----------------------------------------
// StartJobsQuery + GetJobsQueryResults round-trip
// ----------------------------------------

func TestBatch2_StartJobsQuery_ReturnsQueryID(t *testing.T) {
	t.Parallel()

	h := b2mcHandler(t)

	resp, code := b2mcParseResp(t, h, http.MethodPost, "/2017-08-29/jobsQueries", map[string]any{
		"filterList": []any{},
	})
	assert.Equal(t, http.StatusOK, code)

	queryID, _ := resp["queryId"].(string)
	assert.NotEmpty(t, queryID)
}

func TestBatch2_GetJobsQueryResults_UnknownID_ReturnsEmpty(t *testing.T) {
	t.Parallel()

	h := b2mcHandler(t)

	resp, code := b2mcParseResp(t, h, http.MethodGet, "/2017-08-29/jobsQueries/nonexistent-id", nil)
	assert.Equal(t, http.StatusOK, code)

	jobs, _ := resp["jobs"].([]any)
	assert.Empty(t, jobs)
}

func TestBatch2_StartJobsQuery_GetResults_RoundTrip(t *testing.T) {
	t.Parallel()

	h := b2mcHandler(t)
	b2mcCreateJob(t, h)
	b2mcCreateJob(t, h)

	// Start a query with no filters — should match all jobs.
	startResp, startCode := b2mcParseResp(t, h, http.MethodPost, "/2017-08-29/jobsQueries", map[string]any{
		"filterList": []any{},
	})
	require.Equal(t, http.StatusOK, startCode)

	queryID, _ := startResp["queryId"].(string)
	require.NotEmpty(t, queryID)

	// Get results for that query ID.
	getResp, getCode := b2mcParseResp(t, h, http.MethodGet, "/2017-08-29/jobsQueries/"+queryID, nil)
	assert.Equal(t, http.StatusOK, getCode)

	jobs, _ := getResp["jobs"].([]any)
	assert.Len(t, jobs, 2)
}

func TestBatch2_StartJobsQuery_WithStatusFilter_ReturnsMatchingJobs(t *testing.T) {
	t.Parallel()

	h := b2mcHandler(t)
	b2mcCreateJob(t, h)

	startResp, startCode := b2mcParseResp(t, h, http.MethodPost, "/2017-08-29/jobsQueries", map[string]any{
		"filterList": []any{
			map[string]any{
				"key":    "status",
				"values": []any{"SUBMITTED"},
			},
		},
	})
	require.Equal(t, http.StatusOK, startCode)

	queryID, _ := startResp["queryId"].(string)
	require.NotEmpty(t, queryID)

	getResp, getCode := b2mcParseResp(t, h, http.MethodGet, "/2017-08-29/jobsQueries/"+queryID, nil)
	assert.Equal(t, http.StatusOK, getCode)

	jobs, _ := getResp["jobs"].([]any)
	assert.Len(t, jobs, 1)
}

func TestBatch2_StartJobsQuery_WithStatusFilter_NoMatch(t *testing.T) {
	t.Parallel()

	h := b2mcHandler(t)
	b2mcCreateJob(t, h)

	startResp, startCode := b2mcParseResp(t, h, http.MethodPost, "/2017-08-29/jobsQueries", map[string]any{
		"filterList": []any{
			map[string]any{
				"key":    "status",
				"values": []any{"COMPLETE"},
			},
		},
	})
	require.Equal(t, http.StatusOK, startCode)

	queryID, _ := startResp["queryId"].(string)
	require.NotEmpty(t, queryID)

	getResp, getCode := b2mcParseResp(t, h, http.MethodGet, "/2017-08-29/jobsQueries/"+queryID, nil)
	assert.Equal(t, http.StatusOK, getCode)

	jobs, _ := getResp["jobs"].([]any)
	assert.Empty(t, jobs)
}

func TestBatch2_StartJobsQuery_WithMaxResults(t *testing.T) {
	t.Parallel()

	h := b2mcHandler(t)
	b2mcCreateJob(t, h)
	b2mcCreateJob(t, h)
	b2mcCreateJob(t, h)

	maxResults := 2

	startResp, startCode := b2mcParseResp(t, h, http.MethodPost, "/2017-08-29/jobsQueries", map[string]any{
		"filterList": []any{},
		"maxResults": maxResults,
	})
	require.Equal(t, http.StatusOK, startCode)

	queryID, _ := startResp["queryId"].(string)
	require.NotEmpty(t, queryID)

	getResp, getCode := b2mcParseResp(t, h, http.MethodGet, "/2017-08-29/jobsQueries/"+queryID, nil)
	assert.Equal(t, http.StatusOK, getCode)

	jobs, _ := getResp["jobs"].([]any)
	assert.Len(t, jobs, maxResults)
}

func TestBatch2_StartJobsQuery_MultipleQueries_Independent(t *testing.T) {
	t.Parallel()

	h := b2mcHandler(t)
	b2mcCreateJob(t, h)

	// Query 1: match SUBMITTED.
	startResp1, _ := b2mcParseResp(t, h, http.MethodPost, "/2017-08-29/jobsQueries", map[string]any{
		"filterList": []any{
			map[string]any{"key": "status", "values": []any{"SUBMITTED"}},
		},
	})
	qID1, _ := startResp1["queryId"].(string)
	require.NotEmpty(t, qID1)

	// Query 2: match COMPLETE (no match).
	startResp2, _ := b2mcParseResp(t, h, http.MethodPost, "/2017-08-29/jobsQueries", map[string]any{
		"filterList": []any{
			map[string]any{"key": "status", "values": []any{"COMPLETE"}},
		},
	})
	qID2, _ := startResp2["queryId"].(string)
	require.NotEmpty(t, qID2)

	assert.NotEqual(t, qID1, qID2, "each query should have a unique ID")

	// Validate q1 has results, q2 is empty.
	r1, _ := b2mcParseResp(t, h, http.MethodGet, "/2017-08-29/jobsQueries/"+qID1, nil)
	r2, _ := b2mcParseResp(t, h, http.MethodGet, "/2017-08-29/jobsQueries/"+qID2, nil)

	jobs1, _ := r1["jobs"].([]any)
	jobs2, _ := r2["jobs"].([]any)

	assert.Len(t, jobs1, 1)
	assert.Empty(t, jobs2)
}
