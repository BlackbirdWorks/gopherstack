package textract_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/textract"
)

func TestHandler_GetLendingAnalysis_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body := map[string]string{"JobId": "nonexistent-job"}
	rec := doTextractRequest(t, h, "GetLendingAnalysis", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_GetLendingAnalysis_MissingJobId(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body := map[string]string{}
	rec := doTextractRequest(t, h, "GetLendingAnalysis", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestHandler_GetLendingAnalysis_EmptyJobIdRejected validates required JobId
// when explicitly given as an empty string (as opposed to omitted).
func TestHandler_GetLendingAnalysis_EmptyJobIdRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTextractRequest(t, h, "GetLendingAnalysis", map[string]any{"JobId": ""})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_StartLendingAnalysis_ClientRequestTokenIdempotency(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body := map[string]any{
		"DocumentLocation": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "loan.pdf"},
		},
		"ClientRequestToken": "lending-token-xyz",
	}

	rec1 := doTextractRequest(t, h, "StartLendingAnalysis", body)
	require.Equal(t, http.StatusOK, rec1.Code)

	var resp1 map[string]string
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &resp1))
	jobID1 := resp1["JobId"]
	require.NotEmpty(t, jobID1)

	rec2 := doTextractRequest(t, h, "StartLendingAnalysis", body)
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp2 map[string]string
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))

	assert.Equal(t, jobID1, resp2["JobId"], "same ClientRequestToken must return same JobId")
	assert.Equal(t, 1, textract.LendingJobCount(h.Backend.(*textract.InMemoryBackend)))
}

func TestHandler_GetLendingAnalysisSummary_IncludesWarnings(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*textract.InMemoryBackend)

	startRec := doTextractRequest(t, h, "StartLendingAnalysis", map[string]any{
		"DocumentLocation": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "loan.pdf"},
		},
	})
	require.Equal(t, http.StatusOK, startRec.Code)

	var startResp map[string]string
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startResp))
	jobID := startResp["JobId"]

	textract.AddLendingJobInternal(b, &textract.LendingJob{
		JobID:     jobID,
		JobStatus: "SUCCEEDED",
		Warnings: []textract.WarningBlock{
			{ErrorCode: "InvalidPageException", Pages: []int{2}},
		},
	})

	rec := doTextractRequest(t, h, "GetLendingAnalysisSummary", map[string]any{"JobId": jobID})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Warnings []struct {
			ErrorCode string `json:"ErrorCode"`
			Pages     []int  `json:"Pages"`
		} `json:"Warnings"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Warnings, 1)
	assert.Equal(t, "InvalidPageException", resp.Warnings[0].ErrorCode)
}

// TestHandler_GetLendingAnalysis_ExpandedLendingResult verifies
// GetLendingAnalysis returns an expanded LendingResult with extractions.
func TestHandler_GetLendingAnalysis_ExpandedLendingResult(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	startRec := doTextractRequest(t, h, "StartLendingAnalysis", map[string]any{
		"DocumentLocation": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "loan.pdf"},
		},
	})
	require.Equal(t, http.StatusOK, startRec.Code)

	var startResp map[string]string
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startResp))
	jobID := startResp["JobId"]

	getRec := doTextractRequest(t, h, "GetLendingAnalysis", map[string]any{"JobId": jobID})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	assert.Equal(t, "SUCCEEDED", getResp["JobStatus"])

	results, ok := getResp["Results"].([]any)
	require.True(t, ok, "GetLendingAnalysis should return Results")
	require.NotEmpty(t, results)

	result, ok := results[0].(map[string]any)
	require.True(t, ok)

	_, hasPC := result["PageClassification"]
	assert.True(t, hasPC, "LendingResult should have PageClassification")

	extractions, ok := result["Extractions"].([]any)
	assert.True(t, ok, "LendingResult should have Extractions")
	assert.NotEmpty(t, extractions)
}

// TestHandler_GetLendingAnalysisSummary_PopulatedSummary verifies
// GetLendingAnalysisSummary returns a populated Summary.
func TestHandler_GetLendingAnalysisSummary_PopulatedSummary(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	startRec := doTextractRequest(t, h, "StartLendingAnalysis", map[string]any{
		"DocumentLocation": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "loan.pdf"},
		},
	})
	require.Equal(t, http.StatusOK, startRec.Code)

	var startResp map[string]string
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startResp))
	jobID := startResp["JobId"]

	getRec := doTextractRequest(t, h, "GetLendingAnalysisSummary", map[string]any{"JobId": jobID})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	assert.Equal(t, "SUCCEEDED", getResp["JobStatus"])

	summary, ok := getResp["Summary"].(map[string]any)
	require.True(t, ok, "GetLendingAnalysisSummary should return a Summary")

	docGroups, ok := summary["DocumentGroups"].([]any)
	require.True(t, ok, "Summary should have DocumentGroups")
	assert.NotEmpty(t, docGroups)
}

// TestHandler_GetLendingAnalysis_ModelVersion verifies GetLendingAnalysis
// includes AnalyzeLendingModelVersion.
func TestHandler_GetLendingAnalysis_ModelVersion(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	startRec := doTextractRequest(t, h, "StartLendingAnalysis", map[string]any{
		"DocumentLocation": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "loan.pdf"},
		},
	})
	require.Equal(t, http.StatusOK, startRec.Code)

	var startResp map[string]string
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startResp))
	jobID := startResp["JobId"]

	getRec := doTextractRequest(t, h, "GetLendingAnalysis", map[string]string{"JobId": jobID})
	require.Equal(t, http.StatusOK, getRec.Code)

	var m map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &m))
	ver, ok := m["AnalyzeLendingModelVersion"].(string)
	assert.True(t, ok, "GetLendingAnalysis must have AnalyzeLendingModelVersion")
	assert.Equal(t, "1.0", ver)
}

// TestHandler_StartLendingAnalysis_HappyPath tests the full async
// StartLendingAnalysis -> GetLendingAnalysis flow.
func TestHandler_StartLendingAnalysis_HappyPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	startRec := doTextractRequest(t, h, "StartLendingAnalysis", map[string]any{
		"DocumentLocation": map[string]any{
			"S3Object": map[string]any{"Bucket": "loans", "Name": "app.pdf"},
		},
	})

	require.Equal(t, http.StatusOK, startRec.Code)

	var startResp map[string]string
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startResp))
	jobID := startResp["JobId"]
	require.NotEmpty(t, jobID)

	getRec := doTextractRequest(t, h, "GetLendingAnalysis", map[string]string{"JobId": jobID})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	assert.Equal(t, "SUCCEEDED", getResp["JobStatus"])
}

// TestHandler_StartLendingAnalysis_MissingBucket validates required fields.
func TestHandler_StartLendingAnalysis_MissingBucket(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTextractRequest(t, h, "StartLendingAnalysis", map[string]any{
		"DocumentLocation": map[string]any{
			"S3Object": map[string]any{"Bucket": "", "Name": ""},
		},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestHandler_GetLendingAnalysisSummary_HappyPath tests the summary endpoint
// for a lending analysis job.
func TestHandler_GetLendingAnalysisSummary_HappyPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	startRec := doTextractRequest(t, h, "StartLendingAnalysis", map[string]any{
		"DocumentLocation": map[string]any{
			"S3Object": map[string]any{"Bucket": "loans", "Name": "app.pdf"},
		},
	})

	var startResp map[string]string
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startResp))
	jobID := startResp["JobId"]

	rec := doTextractRequest(t, h, "GetLendingAnalysisSummary", map[string]string{"JobId": jobID})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "SUCCEEDED", resp["JobStatus"])
	assert.Equal(t, "1.0", resp["AnalyzeLendingModelVersion"])
}

// TestHandler_GetLendingAnalysisSummary_NotFound ensures not-found returns 400.
func TestHandler_GetLendingAnalysisSummary_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTextractRequest(t, h, "GetLendingAnalysisSummary", map[string]string{"JobId": "no-such-job"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "InvalidJobIdException", errResp["__type"])
}
