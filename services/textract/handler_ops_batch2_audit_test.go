package textract_test

// Audit batch-2: AWS-accuracy coverage for Textract ops layer (go-gczw).
//
// Covers:
//   - Job type isolation guard: GetDocumentAnalysis rejects TextDetection job IDs
//     and GetDocumentTextDetection rejects DocumentAnalysis job IDs with
//     InvalidJobIdException (400).
//   - AnalyzeDocument feature-type isolation: no FeatureTypes → no structured
//     blocks (KEY_VALUE_SET, TABLE); QUERIES without QueriesConfig → no QUERY
//     blocks returned.
//   - AnalyzeID DocumentMetadata.Pages accuracy: Pages matches the number of
//     input DocumentPages (1 doc → 1, 3 docs → 3).
//   - UpdateAdapter field immutability: AdapterName and FeatureTypes are
//     unchanged after UpdateAdapter (only Description and AutoUpdate mutate).
//   - DeleteAdapterVersion terminal guard: GetAdapterVersion after deletion
//     returns InvalidParameterException (400).
//   - Async job initial status: StartDocumentAnalysis with a non-zero async
//     delay returns IN_PROGRESS immediately; GetDocumentAnalysis may flip to
//     SUCCEEDED after the delay.
//   - GetExpenseAnalysis cross-job-type guard: a DocumentAnalysis job ID is
//     not found in the expense-jobs store and returns InvalidJobIdException.

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/textract"
)

// --- helpers ---

func b2TextractHandler(t *testing.T) *textract.Handler {
	t.Helper()

	return textract.NewHandler(textract.NewInMemoryBackendSync("000000000000", "us-east-1"))
}

func b2TextractUnmarshal(t *testing.T, body []byte) map[string]any {
	t.Helper()

	var m map[string]any
	require.NoError(t, json.Unmarshal(body, &m))

	return m
}

// startDocumentAnalysisJob starts a DocumentAnalysis job and returns its JobId.
func startDocumentAnalysisJob(t *testing.T, h *textract.Handler) string {
	t.Helper()

	rec := doTextractRequest(t, h, "StartDocumentAnalysis", map[string]any{
		"DocumentLocation": map[string]any{
			"S3Object": map[string]any{"Bucket": "my-bucket", "Name": "file.pdf"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := b2TextractUnmarshal(t, rec.Body.Bytes())
	jobID, _ := resp["JobId"].(string)
	require.NotEmpty(t, jobID)

	return jobID
}

// startTextDetectionJob starts a TextDetection job and returns its JobId.
func startTextDetectionJob(t *testing.T, h *textract.Handler) string {
	t.Helper()

	rec := doTextractRequest(t, h, "StartDocumentTextDetection", map[string]any{
		"DocumentLocation": map[string]any{
			"S3Object": map[string]any{"Bucket": "my-bucket", "Name": "file.pdf"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := b2TextractUnmarshal(t, rec.Body.Bytes())
	jobID, _ := resp["JobId"].(string)
	require.NotEmpty(t, jobID)

	return jobID
}

// ---------------------------------------------------------------------------
// Job type isolation guard
// ---------------------------------------------------------------------------

// TestBatch2_JobTypeIsolation_GetDocumentAnalysis_RejectsTextDetectionJobID verifies
// that GetDocumentAnalysis returns InvalidJobIdException (400) when given a job ID
// that belongs to a StartDocumentTextDetection job. AWS segregates these job
// namespaces and a TextDetection job ID must not be accepted by GetDocumentAnalysis.
func TestBatch2_JobTypeIsolation_GetDocumentAnalysis_RejectsTextDetectionJobID(t *testing.T) {
	t.Parallel()

	h := b2TextractHandler(t)
	tdJobID := startTextDetectionJob(t, h)

	// GetDocumentAnalysis on a TextDetection job ID must fail.
	rec := doTextractRequest(t, h, "GetDocumentAnalysis", map[string]any{"JobId": tdJobID})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	resp := b2TextractUnmarshal(t, rec.Body.Bytes())
	assert.Equal(t, "InvalidJobIdException", resp["__type"],
		"cross-type access must return InvalidJobIdException, not a different error")
}

// TestBatch2_JobTypeIsolation_GetDocumentTextDetection_RejectsAnalysisJobID verifies
// that GetDocumentTextDetection returns InvalidJobIdException (400) when given a job
// ID that belongs to a StartDocumentAnalysis job. This is the mirror case of the
// above; both directions must be enforced.
func TestBatch2_JobTypeIsolation_GetDocumentTextDetection_RejectsAnalysisJobID(t *testing.T) {
	t.Parallel()

	h := b2TextractHandler(t)
	daJobID := startDocumentAnalysisJob(t, h)

	// GetDocumentTextDetection on a DocumentAnalysis job ID must fail.
	rec := doTextractRequest(t, h, "GetDocumentTextDetection", map[string]any{"JobId": daJobID})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	resp := b2TextractUnmarshal(t, rec.Body.Bytes())
	assert.Equal(t, "InvalidJobIdException", resp["__type"],
		"cross-type access must return InvalidJobIdException")
}

// ---------------------------------------------------------------------------
// AnalyzeDocument feature-type isolation
// ---------------------------------------------------------------------------

// TestBatch2_AnalyzeDocument_NoFeatureTypes_NoStructuredBlocks verifies that when
// AnalyzeDocument is called without any FeatureTypes, the response contains only
// basic text blocks (PAGE, LINE, WORD) and no structured blocks such as
// KEY_VALUE_SET or TABLE. AWS requires explicit feature-type opt-in.
func TestBatch2_AnalyzeDocument_NoFeatureTypes_NoStructuredBlocks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		blockType string
	}{
		{name: "no KEY_VALUE_SET without FORMS", blockType: "KEY_VALUE_SET"},
		{name: "no TABLE without TABLES", blockType: "TABLE"},
		{name: "no QUERY without QUERIES", blockType: "QUERY"},
		{name: "no SIGNATURE without SIGNATURES", blockType: "SIGNATURE"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := b2TextractHandler(t)
			rec := doTextractRequest(t, h, "AnalyzeDocument", map[string]any{
				"Document": map[string]any{
					"S3Object": map[string]any{"Bucket": "b", "Name": "doc.pdf"},
				},
				// FeatureTypes intentionally omitted (nil / empty slice).
			})
			require.Equal(t, http.StatusOK, rec.Code)

			resp := b2TextractUnmarshal(t, rec.Body.Bytes())
			raw, _ := resp["Blocks"].([]any)

			for _, blk := range raw {
				bm, _ := blk.(map[string]any)
				assert.NotEqual(t, tc.blockType, bm["BlockType"],
					"block type %s must not appear without the corresponding FeatureType", tc.blockType)
			}
		})
	}
}

// TestBatch2_AnalyzeDocument_QueriesWithoutQueriesConfig_NoQueryBlocks verifies that
// when QUERIES is listed in FeatureTypes but no QueriesConfig is provided, no QUERY
// or QUERY_RESULT blocks are returned. AWS requires QueriesConfig to be present
// alongside the QUERIES feature type.
func TestBatch2_AnalyzeDocument_QueriesWithoutQueriesConfig_NoQueryBlocks(t *testing.T) {
	t.Parallel()

	h := b2TextractHandler(t)
	rec := doTextractRequest(t, h, "AnalyzeDocument", map[string]any{
		"Document": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "doc.pdf"},
		},
		"FeatureTypes": []string{"QUERIES"},
		// QueriesConfig intentionally omitted.
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := b2TextractUnmarshal(t, rec.Body.Bytes())
	raw, _ := resp["Blocks"].([]any)

	for _, blk := range raw {
		bm, _ := blk.(map[string]any)
		bt, _ := bm["BlockType"].(string)
		assert.NotEqual(t, "QUERY", bt,
			"QUERY blocks must not appear without QueriesConfig")
		assert.NotEqual(t, "QUERY_RESULT", bt,
			"QUERY_RESULT blocks must not appear without QueriesConfig")
	}
}

// ---------------------------------------------------------------------------
// AnalyzeID DocumentMetadata.Pages accuracy
// ---------------------------------------------------------------------------

// TestBatch2_AnalyzeID_DocumentMetadataPages_MatchesInputCount verifies that the
// DocumentMetadata.Pages field in the AnalyzeID response equals the number of
// DocumentPages provided in the request. AWS sets Pages to reflect the actual
// document page count, not a fixed default.
func TestBatch2_AnalyzeID_DocumentMetadataPages_MatchesInputCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		pageCount int
	}{
		{name: "single doc → Pages=1", pageCount: 1},
		{name: "two docs → Pages=2", pageCount: 2},
		{name: "three docs → Pages=3", pageCount: 3},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := b2TextractHandler(t)

			pages := make([]map[string]any, tc.pageCount)
			for i := range tc.pageCount {
				pages[i] = map[string]any{
					"S3Object": map[string]any{
						"Bucket": "id-bucket",
						"Name":   "page.pdf",
					},
				}
			}

			rec := doTextractRequest(t, h, "AnalyzeID", map[string]any{
				"DocumentPages": pages,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			resp := b2TextractUnmarshal(t, rec.Body.Bytes())
			meta, _ := resp["DocumentMetadata"].(map[string]any)
			gotPages, _ := meta["Pages"].(float64)
			assert.InDelta(t, float64(tc.pageCount), gotPages, 0,
				"DocumentMetadata.Pages must match the number of DocumentPages in the request")
		})
	}
}

// ---------------------------------------------------------------------------
// UpdateAdapter field immutability
// ---------------------------------------------------------------------------

// TestBatch2_UpdateAdapter_PreservesNameAndFeatureTypes verifies that UpdateAdapter
// does not modify AdapterName or FeatureTypes. AWS's UpdateAdapter API accepts only
// Description and AutoUpdate as mutable fields; name and feature types are
// immutable after creation.
func TestBatch2_UpdateAdapter_PreservesNameAndFeatureTypes(t *testing.T) {
	t.Parallel()

	h := b2TextractHandler(t)

	createRec := doTextractRequest(t, h, "CreateAdapter", map[string]any{
		"AdapterName":  "immutable-name",
		"FeatureTypes": []string{"FORMS"},
		"Description":  "original",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]string
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	adapterID := createResp["AdapterId"]
	require.NotEmpty(t, adapterID)

	// Update Description and AutoUpdate — but NOT name or FeatureTypes.
	updateRec := doTextractRequest(t, h, "UpdateAdapter", map[string]any{
		"AdapterId":   adapterID,
		"Description": "updated description",
		"AutoUpdate":  "ENABLED",
	})
	require.Equal(t, http.StatusOK, updateRec.Code)

	updateResp := b2TextractUnmarshal(t, updateRec.Body.Bytes())

	assert.Equal(t, "immutable-name", updateResp["AdapterName"],
		"AdapterName must be unchanged after UpdateAdapter")

	featureTypes, _ := updateResp["FeatureTypes"].([]any)
	require.Len(t, featureTypes, 1, "FeatureTypes count must be unchanged")
	assert.Equal(t, "FORMS", featureTypes[0],
		"FeatureTypes must be unchanged after UpdateAdapter")

	// Verify mutable fields did change.
	assert.Equal(t, "updated description", updateResp["Description"])
	assert.Equal(t, "ENABLED", updateResp["AutoUpdate"])
}

// ---------------------------------------------------------------------------
// DeleteAdapterVersion terminal guard
// ---------------------------------------------------------------------------

// TestBatch2_DeleteAdapterVersion_GetReturnsErrorAfterDelete verifies that after
// DeleteAdapterVersion, calling GetAdapterVersion on the deleted version returns
// InvalidParameterException (400). AWS removes the version from its store
// permanently and subsequent Gets must fail.
func TestBatch2_DeleteAdapterVersion_GetReturnsErrorAfterDelete(t *testing.T) {
	t.Parallel()

	h := b2TextractHandler(t)

	// Create adapter.
	createAdapterRec := doTextractRequest(t, h, "CreateAdapter", map[string]any{
		"AdapterName":  "versioned",
		"FeatureTypes": []string{"FORMS"},
	})
	require.Equal(t, http.StatusOK, createAdapterRec.Code)
	var adapterResp map[string]string
	require.NoError(t, json.Unmarshal(createAdapterRec.Body.Bytes(), &adapterResp))
	adapterID := adapterResp["AdapterId"]

	// Create a version.
	createVerRec := doTextractRequest(t, h, "CreateAdapterVersion", map[string]any{
		"AdapterId": adapterID,
	})
	require.Equal(t, http.StatusOK, createVerRec.Code)
	var verResp map[string]string
	require.NoError(t, json.Unmarshal(createVerRec.Body.Bytes(), &verResp))
	version := verResp["AdapterVersion"]
	require.NotEmpty(t, version)

	// Confirm the version exists.
	getBeforeRec := doTextractRequest(t, h, "GetAdapterVersion", map[string]any{
		"AdapterId":      adapterID,
		"AdapterVersion": version,
	})
	require.Equal(t, http.StatusOK, getBeforeRec.Code, "version must exist before deletion")

	// Delete the version.
	deleteRec := doTextractRequest(t, h, "DeleteAdapterVersion", map[string]any{
		"AdapterId":      adapterID,
		"AdapterVersion": version,
	})
	require.Equal(t, http.StatusOK, deleteRec.Code, "delete must succeed")

	// GetAdapterVersion must now return InvalidParameterException.
	getAfterRec := doTextractRequest(t, h, "GetAdapterVersion", map[string]any{
		"AdapterId":      adapterID,
		"AdapterVersion": version,
	})
	assert.Equal(t, http.StatusBadRequest, getAfterRec.Code)
	errResp := b2TextractUnmarshal(t, getAfterRec.Body.Bytes())
	assert.Equal(t, "InvalidParameterException", errResp["__type"],
		"deleted version must return InvalidParameterException")
}

// ---------------------------------------------------------------------------
// Async job initial status
// ---------------------------------------------------------------------------

// TestBatch2_AsyncJob_InitialStatus_InProgress verifies that a StartDocumentAnalysis
// call returns IN_PROGRESS immediately when the backend is configured with a non-zero
// async delay. AWS always returns IN_PROGRESS on the Start* call; callers must poll
// GetDocumentAnalysis to observe the SUCCEEDED transition.
func TestBatch2_AsyncJob_InitialStatus_InProgress(t *testing.T) {
	t.Parallel()

	backend := textract.NewInMemoryBackend("000000000000", "us-east-1")
	textract.SetBackendAsyncDelay(backend, 500*time.Millisecond)
	h := textract.NewHandler(backend)

	startRec := doTextractRequest(t, h, "StartDocumentAnalysis", map[string]any{
		"DocumentLocation": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "k"},
		},
	})
	require.Equal(t, http.StatusOK, startRec.Code)

	var startResp map[string]string
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startResp))
	jobID := startResp["JobId"]
	require.NotEmpty(t, jobID)

	// Immediately poll – must be IN_PROGRESS (not yet SUCCEEDED).
	getRec := doTextractRequest(t, h, "GetDocumentAnalysis", map[string]any{"JobId": jobID})
	require.Equal(t, http.StatusOK, getRec.Code)

	getResp := b2TextractUnmarshal(t, getRec.Body.Bytes())
	assert.Equal(t, "IN_PROGRESS", getResp["JobStatus"],
		"job must be IN_PROGRESS immediately after Start when async delay is non-zero")
}

// ---------------------------------------------------------------------------
// GetExpenseAnalysis cross-job-type guard
// ---------------------------------------------------------------------------

// TestBatch2_GetExpenseAnalysis_RejectsDocumentAnalysisJobID verifies that
// GetExpenseAnalysis returns InvalidJobIdException (400) when given a job ID that
// belongs to a StartDocumentAnalysis (or StartDocumentTextDetection) job. AWS
// segregates expense jobs from document jobs: a document job ID is not findable
// in the expense-analysis job store.
func TestBatch2_GetExpenseAnalysis_RejectsDocumentAnalysisJobID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		startAction string
	}{
		{
			name:        "document_analysis_job_id",
			startAction: "StartDocumentAnalysis",
		},
		{
			name:        "text_detection_job_id",
			startAction: "StartDocumentTextDetection",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := b2TextractHandler(t)

			startRec := doTextractRequest(t, h, tc.startAction, map[string]any{
				"DocumentLocation": map[string]any{
					"S3Object": map[string]any{"Bucket": "b", "Name": "k"},
				},
			})
			require.Equal(t, http.StatusOK, startRec.Code)

			var startResp map[string]string
			require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startResp))
			jobID := startResp["JobId"]

			// GetExpenseAnalysis with a document job ID must fail.
			getRec := doTextractRequest(t, h, "GetExpenseAnalysis", map[string]any{"JobId": jobID})
			assert.Equal(t, http.StatusBadRequest, getRec.Code)

			errResp := b2TextractUnmarshal(t, getRec.Body.Bytes())
			assert.Equal(t, "InvalidJobIdException", errResp["__type"],
				"expense analysis must not accept a document job ID")
		})
	}
}
