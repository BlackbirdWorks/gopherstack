package textract_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/textract"
)

// startDocumentAnalysisJob starts a DocumentAnalysis job and returns its JobId.
func startDocumentAnalysisJob(t *testing.T, h *textract.Handler) string {
	t.Helper()

	rec := doTextractRequest(t, h, "StartDocumentAnalysis", map[string]any{
		"DocumentLocation": map[string]any{
			"S3Object": map[string]any{"Bucket": "my-bucket", "Name": "file.pdf"},
		},
		"FeatureTypes": []string{"TABLES"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	jobID := resp["JobId"]
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

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	jobID := resp["JobId"]
	require.NotEmpty(t, jobID)

	return jobID
}

// blocksOfType returns all blocks in the JSON response body that match blockType.
func blocksOfType(t *testing.T, body []byte, blockType string) []map[string]any {
	t.Helper()

	var resp map[string]any
	require.NoError(t, json.Unmarshal(body, &resp))

	raw, ok := resp["Blocks"].([]any)
	require.True(t, ok, "response should have Blocks array")

	var out []map[string]any

	for _, b := range raw {
		bm, ok2 := b.(map[string]any)
		if !ok2 {
			continue
		}

		if bm["BlockType"] == blockType {
			out = append(out, bm)
		}
	}

	return out
}

func TestHandler_AnalyzeDocument(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantStatus int
		wantBlocks bool
	}{
		{
			name: "success with S3 document",
			body: map[string]any{
				"Document": map[string]any{
					"S3Object": map[string]any{
						"Bucket": "my-bucket",
						"Name":   "invoice.pdf",
					},
				},
				"FeatureTypes": []string{"TABLES", "FORMS"},
			},
			wantStatus: http.StatusOK,
			wantBlocks: true,
		},
		{
			name:       "empty body rejects with 400 (FeatureTypes required)",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
			wantBlocks: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doTextractRequest(t, h, "AnalyzeDocument", tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantBlocks {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				blocks, ok := resp["Blocks"].([]any)
				assert.True(t, ok)
				assert.NotEmpty(t, blocks)
			}
		})
	}
}

func TestHandler_StartAndGetDocumentAnalysis(t *testing.T) {
	t.Parallel()

	tests := []struct {
		startBody  any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			startBody: map[string]any{
				"DocumentLocation": map[string]any{
					"S3Object": map[string]any{
						"Bucket": "my-bucket",
						"Name":   "document.pdf",
					},
				},
				"FeatureTypes": []string{"TABLES"},
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Start the job
			rec := doTextractRequest(t, h, "StartDocumentAnalysis", tt.startBody)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var startResp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
			jobID := startResp["JobId"]
			assert.NotEmpty(t, jobID)

			// Get results
			getBody := map[string]string{"JobId": jobID}
			getRec := doTextractRequest(t, h, "GetDocumentAnalysis", getBody)
			assert.Equal(t, http.StatusOK, getRec.Code)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
			assert.Equal(t, "SUCCEEDED", getResp["JobStatus"])
			blocks, ok := getResp["Blocks"].([]any)
			assert.True(t, ok)
			assert.NotEmpty(t, blocks)
		})
	}
}

func TestHandler_StartDocumentAnalysis_MissingBucket(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body := map[string]any{
		"DocumentLocation": map[string]any{
			"S3Object": map[string]any{
				"Bucket": "",
				"Name":   "",
			},
		},
	}

	rec := doTextractRequest(t, h, "StartDocumentAnalysis", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_GetDocumentAnalysis_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body := map[string]string{"JobId": "nonexistent-job"}

	rec := doTextractRequest(t, h, "GetDocumentAnalysis", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_GetDocumentAnalysis_MissingJobId(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body := map[string]string{}

	rec := doTextractRequest(t, h, "GetDocumentAnalysis", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestHandler_GetDocumentAnalysis_RejectsTextDetectionJobID verifies that
// GetDocumentAnalysis returns InvalidJobIdException (400) when given a job ID
// that belongs to a StartDocumentTextDetection job. AWS segregates these job
// namespaces and a TextDetection job ID must not be accepted by
// GetDocumentAnalysis.
func TestHandler_GetDocumentAnalysis_RejectsTextDetectionJobID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tdJobID := startTextDetectionJob(t, h)

	rec := doTextractRequest(t, h, "GetDocumentAnalysis", map[string]any{"JobId": tdJobID})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "InvalidJobIdException", resp["__type"],
		"cross-type access must return InvalidJobIdException, not a different error")
}

// TestHandler_AnalyzeDocument_FormsKeyValueSetBlocks verifies AnalyzeDocument
// FORMS produces KEY_VALUE_SET blocks with KEY and VALUE entity types.
func TestHandler_AnalyzeDocument_FormsKeyValueSetBlocks(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTextractRequest(t, h, "AnalyzeDocument", map[string]any{
		"Document": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "form.pdf"},
		},
		"FeatureTypes": []string{"FORMS"},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	kvBlocks := blocksOfType(t, rec.Body.Bytes(), "KEY_VALUE_SET")
	assert.NotEmpty(t, kvBlocks, "FORMS feature should produce KEY_VALUE_SET blocks")

	var hasKey, hasValue bool

	for _, blk := range kvBlocks {
		if ets, ok := blk["EntityTypes"].([]any); ok {
			for _, et := range ets {
				switch et.(string) {
				case "KEY":
					hasKey = true
				case "VALUE":
					hasValue = true
				}
			}
		}
	}

	assert.True(t, hasKey, "should have KEY blocks")
	assert.True(t, hasValue, "should have VALUE blocks")
}

// TestHandler_AnalyzeDocument_TablesTableCellBlocks verifies AnalyzeDocument
// TABLES produces TABLE and CELL blocks with row/column indices.
func TestHandler_AnalyzeDocument_TablesTableCellBlocks(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTextractRequest(t, h, "AnalyzeDocument", map[string]any{
		"Document": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "table.pdf"},
		},
		"FeatureTypes": []string{"TABLES"},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	tables := blocksOfType(t, rec.Body.Bytes(), "TABLE")
	cells := blocksOfType(t, rec.Body.Bytes(), "CELL")

	assert.NotEmpty(t, tables, "TABLES feature should produce TABLE blocks")
	assert.NotEmpty(t, cells, "TABLES feature should produce CELL blocks")

	for _, cell := range cells {
		_, hasRow := cell["RowIndex"]
		_, hasCol := cell["ColumnIndex"]
		assert.True(t, hasRow, "CELL should have RowIndex")
		assert.True(t, hasCol, "CELL should have ColumnIndex")
	}
}

// TestHandler_AnalyzeDocument_QueriesQueryBlocks verifies AnalyzeDocument
// QUERIES with QueriesConfig produces QUERY and QUERY_RESULT blocks.
func TestHandler_AnalyzeDocument_QueriesQueryBlocks(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTextractRequest(t, h, "AnalyzeDocument", map[string]any{
		"Document": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "doc.pdf"},
		},
		"FeatureTypes": []string{"QUERIES"},
		"QueriesConfig": map[string]any{
			"Queries": []any{
				map[string]any{"Text": "What is the total?", "Alias": "TOTAL"},
				map[string]any{"Text": "Who is the vendor?", "Alias": "VENDOR"},
			},
		},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	queryBlocks := blocksOfType(t, rec.Body.Bytes(), "QUERY")
	resultBlocks := blocksOfType(t, rec.Body.Bytes(), "QUERY_RESULT")

	assert.Len(t, queryBlocks, 2, "should have 2 QUERY blocks (one per query)")
	assert.Len(t, resultBlocks, 2, "should have 2 QUERY_RESULT blocks")

	for _, qb := range queryBlocks {
		rels, ok := qb["Relationships"].([]any)
		assert.True(t, ok, "QUERY block should have Relationships")

		var hasAnswer bool

		for _, rel := range rels {
			rm, ok2 := rel.(map[string]any)
			if ok2 && rm["Type"] == "ANSWER" {
				hasAnswer = true
			}
		}

		assert.True(t, hasAnswer, "QUERY block should have ANSWER relationship")
	}
}

// TestHandler_AnalyzeDocument_SignaturesSignatureBlock verifies AnalyzeDocument
// SIGNATURES produces a SIGNATURE block with Geometry.
func TestHandler_AnalyzeDocument_SignaturesSignatureBlock(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTextractRequest(t, h, "AnalyzeDocument", map[string]any{
		"Document": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "signed.pdf"},
		},
		"FeatureTypes": []string{"SIGNATURES"},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	sigBlocks := blocksOfType(t, rec.Body.Bytes(), "SIGNATURE")
	assert.NotEmpty(t, sigBlocks, "SIGNATURES feature should produce SIGNATURE blocks")

	sig := sigBlocks[0]
	_, hasGeom := sig["Geometry"]
	assert.True(t, hasGeom, "SIGNATURE block should have Geometry")
}

// TestHandler_AnalyzeDocument_LayoutBlocks verifies AnalyzeDocument LAYOUT
// produces LAYOUT_HEADER/LAYOUT_TEXT/LAYOUT_FOOTER blocks.
func TestHandler_AnalyzeDocument_LayoutBlocks(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTextractRequest(t, h, "AnalyzeDocument", map[string]any{
		"Document": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "layout.pdf"},
		},
		"FeatureTypes": []string{"LAYOUT"},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	layoutText := blocksOfType(t, rec.Body.Bytes(), "LAYOUT_TEXT")
	layoutHeader := blocksOfType(t, rec.Body.Bytes(), "LAYOUT_HEADER")
	layoutFooter := blocksOfType(t, rec.Body.Bytes(), "LAYOUT_FOOTER")

	assert.NotEmpty(t, layoutText, "LAYOUT feature should produce LAYOUT_TEXT blocks")
	assert.NotEmpty(t, layoutHeader, "LAYOUT feature should produce LAYOUT_HEADER blocks")
	assert.NotEmpty(t, layoutFooter, "LAYOUT feature should produce LAYOUT_FOOTER blocks")
}

// TestHandler_AnalyzeDocument_FormsSelectionElement verifies AnalyzeDocument
// FORMS includes a SELECTION_ELEMENT block with a SelectionStatus.
func TestHandler_AnalyzeDocument_FormsSelectionElement(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTextractRequest(t, h, "AnalyzeDocument", map[string]any{
		"Document": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "form.pdf"},
		},
		"FeatureTypes": []string{"FORMS"},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	selBlocks := blocksOfType(t, rec.Body.Bytes(), "SELECTION_ELEMENT")
	require.NotEmpty(t, selBlocks, "FORMS feature should include SELECTION_ELEMENT blocks")

	sel := selBlocks[0]
	status, _ := sel["SelectionStatus"].(string)
	assert.NotEmpty(t, status, "SELECTION_ELEMENT should have SelectionStatus")
}

// TestHandler_AnalyzeDocument_ModelVersion verifies the response includes
// AnalyzeDocumentModelVersion.
func TestHandler_AnalyzeDocument_ModelVersion(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTextractRequest(t, h, "AnalyzeDocument", map[string]any{
		"Document": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "doc.pdf"},
		},
		"FeatureTypes": []string{"FORMS"},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "1.0", resp["AnalyzeDocumentModelVersion"])
}

// TestHandler_AnalyzeDocument_DocumentMetadataPages verifies AnalyzeDocument's
// DocumentMetadata.Pages field is present and >= 1.
func TestHandler_AnalyzeDocument_DocumentMetadataPages(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTextractRequest(t, h, "AnalyzeDocument", map[string]any{
		"Document": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "form.pdf"},
		},
		"FeatureTypes": []string{"FORMS"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var m map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &m))
	meta, ok := m["DocumentMetadata"].(map[string]any)
	require.True(t, ok, "AnalyzeDocument response must have DocumentMetadata")
	pages, ok2 := meta["Pages"].(float64)
	assert.True(t, ok2, "DocumentMetadata must have Pages")
	assert.GreaterOrEqual(t, int(pages), 1)
}

// TestHandler_AnalyzeDocument_BytesInput verifies AnalyzeDocument accepts an
// inline Document.Bytes payload (not only S3Object).
func TestHandler_AnalyzeDocument_BytesInput(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTextractRequest(t, h, "AnalyzeDocument", map[string]any{
		"Document": map[string]any{
			"Bytes": []byte("fake-pdf-content"),
		},
		"FeatureTypes": []string{"FORMS"},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var m map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &m))
	blocks, ok := m["Blocks"].([]any)
	assert.True(t, ok && len(blocks) > 0, "AnalyzeDocument with Bytes must return Blocks")
}

// TestHandler_GetDocumentAnalysis_PaginationNextToken verifies GetDocumentAnalysis
// pagination with MaxResults produces a NextToken.
func TestHandler_GetDocumentAnalysis_PaginationNextToken(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	startRec := doTextractRequest(t, h, "StartDocumentAnalysis", map[string]any{
		"DocumentLocation": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "doc.pdf"},
		},
		"FeatureTypes": []string{"TABLES", "FORMS"},
	})
	require.Equal(t, http.StatusOK, startRec.Code)

	var startResp map[string]string
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startResp))
	jobID := startResp["JobId"]

	getRec := doTextractRequest(t, h, "GetDocumentAnalysis", map[string]any{
		"JobId":      jobID,
		"MaxResults": 2,
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))

	blocks, _ := getResp["Blocks"].([]any)
	assert.Len(t, blocks, 2)

	nextToken, hasToken := getResp["NextToken"].(string)
	assert.True(t, hasToken && nextToken != "", "NextToken should be set when more pages exist")
}

// TestHandler_GetDocumentAnalysis_BlocksHaveGeometry verifies at least one
// returned block carries a Geometry.
func TestHandler_GetDocumentAnalysis_BlocksHaveGeometry(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	startRec := doTextractRequest(t, h, "StartDocumentAnalysis", map[string]any{
		"DocumentLocation": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "doc.pdf"},
		},
		"FeatureTypes": []string{"FORMS"},
	})
	require.Equal(t, http.StatusOK, startRec.Code)

	var startResp map[string]string
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startResp))
	jobID := startResp["JobId"]

	getRec := doTextractRequest(t, h, "GetDocumentAnalysis", map[string]any{"JobId": jobID})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))

	blocks, _ := getResp["Blocks"].([]any)
	require.NotEmpty(t, blocks)

	var hasGeometry bool

	for _, b := range blocks {
		bm, ok := b.(map[string]any)
		if !ok {
			continue
		}

		if _, ok2 := bm["Geometry"]; ok2 {
			hasGeometry = true

			break
		}
	}

	assert.True(t, hasGeometry, "blocks should have Geometry")
}

// TestHandler_GetDocumentAnalysis_ModelVersion verifies GetDocumentAnalysis
// includes AnalyzeDocumentModelVersion.
func TestHandler_GetDocumentAnalysis_ModelVersion(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	startRec := doTextractRequest(t, h, "StartDocumentAnalysis", map[string]any{
		"DocumentLocation": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "doc.pdf"},
		},
		"FeatureTypes": []string{"FORMS"},
	})
	require.Equal(t, http.StatusOK, startRec.Code)

	var startResp map[string]string
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startResp))
	jobID := startResp["JobId"]

	getRec := doTextractRequest(t, h, "GetDocumentAnalysis", map[string]string{"JobId": jobID})
	require.Equal(t, http.StatusOK, getRec.Code)

	var m map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &m))
	ver, ok := m["AnalyzeDocumentModelVersion"].(string)
	assert.True(t, ok, "GetDocumentAnalysis must have AnalyzeDocumentModelVersion")
	assert.Equal(t, "1.0", ver)
}

// TestHandler_GetDocumentAnalysis_JobStatusSucceeded verifies a synchronous
// backend job reports SUCCEEDED immediately.
func TestHandler_GetDocumentAnalysis_JobStatusSucceeded(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	startRec := doTextractRequest(t, h, "StartDocumentAnalysis", map[string]any{
		"DocumentLocation": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "doc.pdf"},
		},
		"FeatureTypes": []string{"FORMS"},
	})
	require.Equal(t, http.StatusOK, startRec.Code)

	var startResp map[string]string
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startResp))
	jobID := startResp["JobId"]

	getRec := doTextractRequest(t, h, "GetDocumentAnalysis", map[string]string{"JobId": jobID})
	require.Equal(t, http.StatusOK, getRec.Code)

	var m map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &m))
	assert.Equal(t, "SUCCEEDED", m["JobStatus"], "synchronous backend job must SUCCEED immediately")
}

// TestHandler_StartDocumentAnalysis_AsyncInProgressThenSucceeded verifies a
// non-zero async delay makes GetDocumentAnalysis report IN_PROGRESS then
// SUCCEEDED.
func TestHandler_StartDocumentAnalysis_AsyncInProgressThenSucceeded(t *testing.T) {
	t.Parallel()

	// Use a backend with a short async delay (not zero, so we can observe IN_PROGRESS).
	b := textract.NewInMemoryBackendSync("123456789012", "us-east-1")
	textract.SetBackendAsyncDelay(b, 50*time.Millisecond)
	h := textract.NewHandler(b)

	startRec := doTextractRequest(t, h, "StartDocumentAnalysis", map[string]any{
		"DocumentLocation": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "doc.pdf"},
		},
		"FeatureTypes": []string{"FORMS"},
	})
	require.Equal(t, http.StatusOK, startRec.Code)

	var startResp map[string]string
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startResp))
	jobID := startResp["JobId"]

	// Immediately after start, job should be IN_PROGRESS.
	getRec1 := doTextractRequest(t, h, "GetDocumentAnalysis", map[string]any{"JobId": jobID})
	require.Equal(t, http.StatusOK, getRec1.Code)

	var getResp1 map[string]any
	require.NoError(t, json.Unmarshal(getRec1.Body.Bytes(), &getResp1))
	assert.Equal(t, "IN_PROGRESS", getResp1["JobStatus"])

	// After delay, job should be SUCCEEDED.
	time.Sleep(200 * time.Millisecond)

	getRec2 := doTextractRequest(t, h, "GetDocumentAnalysis", map[string]any{"JobId": jobID})
	require.Equal(t, http.StatusOK, getRec2.Code)

	var getResp2 map[string]any
	require.NoError(t, json.Unmarshal(getRec2.Body.Bytes(), &getResp2))
	assert.Equal(t, "SUCCEEDED", getResp2["JobStatus"])
}

// TestHandler_StartDocumentAnalysis_AsyncInitialStatusInProgress verifies that
// a StartDocumentAnalysis call returns IN_PROGRESS immediately when the
// backend is configured with a non-zero async delay. AWS always returns
// IN_PROGRESS on the Start* call; callers must poll GetDocumentAnalysis to
// observe the SUCCEEDED transition.
func TestHandler_StartDocumentAnalysis_AsyncInitialStatusInProgress(t *testing.T) {
	t.Parallel()

	backend := textract.NewInMemoryBackend("000000000000", "us-east-1")
	textract.SetBackendAsyncDelay(backend, 500*time.Millisecond)
	h := textract.NewHandler(backend)

	startRec := doTextractRequest(t, h, "StartDocumentAnalysis", map[string]any{
		"DocumentLocation": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "k"},
		},
		"FeatureTypes": []string{"TABLES"},
	})
	require.Equal(t, http.StatusOK, startRec.Code)

	var startResp map[string]string
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startResp))
	jobID := startResp["JobId"]
	require.NotEmpty(t, jobID)

	// Immediately poll – must be IN_PROGRESS (not yet SUCCEEDED).
	getRec := doTextractRequest(t, h, "GetDocumentAnalysis", map[string]any{"JobId": jobID})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	assert.Equal(t, "IN_PROGRESS", getResp["JobStatus"],
		"job must be IN_PROGRESS immediately after Start when async delay is non-zero")
}

// TestHandler_StartDocumentAnalysis_ClientRequestTokenIdempotency verifies that
// replaying the same ClientRequestToken returns the same JobId.
func TestHandler_StartDocumentAnalysis_ClientRequestTokenIdempotency(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	startBody := map[string]any{
		"DocumentLocation": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "doc.pdf"},
		},
		"FeatureTypes":       []string{"TABLES"},
		"ClientRequestToken": "unique-token-abc123",
	}

	rec1 := doTextractRequest(t, h, "StartDocumentAnalysis", startBody)
	require.Equal(t, http.StatusOK, rec1.Code)

	var resp1 map[string]string
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &resp1))
	jobID1 := resp1["JobId"]

	rec2 := doTextractRequest(t, h, "StartDocumentAnalysis", startBody)
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp2 map[string]string
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))
	jobID2 := resp2["JobId"]

	assert.Equal(t, jobID1, jobID2, "same ClientRequestToken should return the same JobId")
}

// TestHandler_StartDocumentAnalysis_JobIdNonEmpty verifies StartDocumentAnalysis
// returns a non-empty UUID-shaped JobId.
func TestHandler_StartDocumentAnalysis_JobIdNonEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTextractRequest(t, h, "StartDocumentAnalysis", map[string]any{
		"DocumentLocation": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "doc.pdf"},
		},
		"FeatureTypes": []string{"TABLES"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var m map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &m))
	jobID, ok := m["JobId"].(string)
	assert.True(t, ok, "StartDocumentAnalysis response must have string JobId")
	assert.NotEmpty(t, jobID, "JobId must not be empty")
}

// TestHandler_AnalyzeDocument_NoFeatureTypesRejected verifies AnalyzeDocument
// returns ValidationException (400) when FeatureTypes is omitted.
func TestHandler_AnalyzeDocument_NoFeatureTypesRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTextractRequest(t, h, "AnalyzeDocument", map[string]any{
		"Document": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "doc.pdf"},
		},
		// FeatureTypes intentionally omitted.
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code,
		"AnalyzeDocument without FeatureTypes must return 400")
}

// TestHandler_AnalyzeDocument_QueriesWithoutQueriesConfigRejected verifies that
// when QUERIES is listed in FeatureTypes but no QueriesConfig is provided,
// AnalyzeDocument returns a ValidationException (HTTP 400).
func TestHandler_AnalyzeDocument_QueriesWithoutQueriesConfigRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTextractRequest(t, h, "AnalyzeDocument", map[string]any{
		"Document": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "doc.pdf"},
		},
		"FeatureTypes": []string{"QUERIES"},
		// QueriesConfig intentionally omitted — must return 400.
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code,
		"QUERIES without QueriesConfig must return 400")
}

// TestHandler_AnalyzeDocument_FeatureTypesValidation verifies that
// AnalyzeDocument rejects unknown FeatureType strings with
// InvalidParameterException (HTTP 400).
func TestHandler_AnalyzeDocument_FeatureTypesValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		featureTypes []string
		wantCode     int
	}{
		{
			name:         "valid_tables_accepted",
			featureTypes: []string{"TABLES"},
			wantCode:     http.StatusOK,
		},
		{
			name:         "valid_forms_accepted",
			featureTypes: []string{"FORMS"},
			wantCode:     http.StatusOK,
		},
		{
			name:         "valid_queries_accepted",
			featureTypes: []string{"QUERIES"},
			wantCode:     http.StatusOK,
		},
		{
			name:         "valid_signatures_accepted",
			featureTypes: []string{"SIGNATURES"},
			wantCode:     http.StatusOK,
		},
		{
			name:         "valid_layout_accepted",
			featureTypes: []string{"LAYOUT"},
			wantCode:     http.StatusOK,
		},
		{
			name:         "multiple_valid_accepted",
			featureTypes: []string{"TABLES", "FORMS"},
			wantCode:     http.StatusOK,
		},
		{
			name:         "unknown_feature_type_rejected",
			featureTypes: []string{"UNKNOWN_FEATURE"},
			wantCode:     http.StatusBadRequest,
		},
		{
			name:         "mixed_valid_invalid_rejected",
			featureTypes: []string{"TABLES", "INVALID"},
			wantCode:     http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doTextractRequest(t, h, "AnalyzeDocument", map[string]any{
				"Document": map[string]any{
					"S3Object": map[string]any{
						"Bucket": "test-bucket",
						"Name":   "test-doc.pdf",
					},
				},
				"FeatureTypes": tt.featureTypes,
				// Always include QueriesConfig so QUERIES cases are not rejected
				// for the missing-config reason rather than the feature-type reason.
				"QueriesConfig": map[string]any{
					"Queries": []any{map[string]any{"Text": "What is the total?"}},
				},
			})

			assert.Equal(t, tt.wantCode, rec.Code, "FeatureTypes=%v", tt.featureTypes)
		})
	}
}

// TestHandler_StartDocumentAnalysis_FeatureTypesValidation verifies the same
// unknown-FeatureType validation applies to the async StartDocumentAnalysis path.
func TestHandler_StartDocumentAnalysis_FeatureTypesValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		featureTypes []string
		wantCode     int
	}{
		{
			name:         "valid_tables_starts_job",
			featureTypes: []string{"TABLES"},
			wantCode:     http.StatusOK,
		},
		{
			name:         "unknown_feature_type_rejected",
			featureTypes: []string{"BANANA"},
			wantCode:     http.StatusBadRequest,
		},
		{
			name:         "mixed_valid_invalid_rejected",
			featureTypes: []string{"FORMS", "NOT_A_TYPE"},
			wantCode:     http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doTextractRequest(t, h, "StartDocumentAnalysis", map[string]any{
				"DocumentLocation": map[string]any{
					"S3Object": map[string]any{
						"Bucket": "test-bucket",
						"Name":   "test-doc.pdf",
					},
				},
				"FeatureTypes": tt.featureTypes,
			})

			assert.Equal(t, tt.wantCode, rec.Code, "FeatureTypes=%v", tt.featureTypes)

			if tt.wantCode == http.StatusOK {
				require.Contains(t, rec.Body.String(), "JobId")
			}
		})
	}
}

// TestHandler_AnalyzeDocumentAndStartDocumentAnalysis_RequireNonEmptyFeatureTypes
// verifies that AnalyzeDocument and StartDocumentAnalysis reject requests with
// empty or absent FeatureTypes.
func TestHandler_AnalyzeDocumentAndStartDocumentAnalysis_RequireNonEmptyFeatureTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		op           string
		featureTypes []string
		wantCode     int
	}{
		{
			name:         "AnalyzeDocument_nil_feature_types_rejected",
			op:           "AnalyzeDocument",
			featureTypes: nil,
			wantCode:     http.StatusBadRequest,
		},
		{
			name:         "AnalyzeDocument_empty_feature_types_rejected",
			op:           "AnalyzeDocument",
			featureTypes: []string{},
			wantCode:     http.StatusBadRequest,
		},
		{
			name:         "StartDocumentAnalysis_nil_feature_types_rejected",
			op:           "StartDocumentAnalysis",
			featureTypes: nil,
			wantCode:     http.StatusBadRequest,
		},
		{
			name:         "StartDocumentAnalysis_empty_feature_types_rejected",
			op:           "StartDocumentAnalysis",
			featureTypes: []string{},
			wantCode:     http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var body map[string]any

			switch tt.op {
			case "AnalyzeDocument":
				body = map[string]any{
					"Document": map[string]any{
						"S3Object": map[string]any{
							"Bucket": "test-bucket",
							"Name":   "doc.pdf",
						},
					},
				}
			default:
				body = map[string]any{
					"DocumentLocation": map[string]any{
						"S3Object": map[string]any{
							"Bucket": "test-bucket",
							"Name":   "doc.pdf",
						},
					},
				}
			}

			if tt.featureTypes != nil {
				body["FeatureTypes"] = tt.featureTypes
			}

			rec := doTextractRequest(t, h, tt.op, body)
			assert.Equal(t, tt.wantCode, rec.Code,
				"%s with empty FeatureTypes must return 400", tt.op)
		})
	}
}

// TestHandler_AnalyzeDocument_QueriesRequiresQueriesConfig verifies that
// AnalyzeDocument with FeatureTypes=["QUERIES"] but no QueriesConfig returns
// HTTP 400.
func TestHandler_AnalyzeDocument_QueriesRequiresQueriesConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "queries_without_queriesconfig_rejected",
			body: map[string]any{
				"Document":     map[string]any{"S3Object": map[string]any{"Bucket": "b", "Name": "f.pdf"}},
				"FeatureTypes": []string{"QUERIES"},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "queries_with_empty_queries_rejected",
			body: map[string]any{
				"Document":      map[string]any{"S3Object": map[string]any{"Bucket": "b", "Name": "f.pdf"}},
				"FeatureTypes":  []string{"QUERIES"},
				"QueriesConfig": map[string]any{"Queries": []any{}},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "queries_with_queriesconfig_accepted",
			body: map[string]any{
				"Document":     map[string]any{"S3Object": map[string]any{"Bucket": "b", "Name": "f.pdf"}},
				"FeatureTypes": []string{"QUERIES"},
				"QueriesConfig": map[string]any{
					"Queries": []any{map[string]any{"Text": "What is the total?"}},
				},
			},
			wantCode: http.StatusOK,
		},
		{
			name: "other_features_without_queriesconfig_accepted",
			body: map[string]any{
				"Document":     map[string]any{"S3Object": map[string]any{"Bucket": "b", "Name": "f.pdf"}},
				"FeatureTypes": []string{"FORMS", "TABLES"},
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doTextractRequest(t, h, "AnalyzeDocument", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestHandler_StartDocumentAnalysis_QueriesRequiresQueriesConfig verifies the
// same QueriesConfig requirement applies to the async StartDocumentAnalysis path.
func TestHandler_StartDocumentAnalysis_QueriesRequiresQueriesConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTextractRequest(t, h, "StartDocumentAnalysis", map[string]any{
		"DocumentLocation": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "f.pdf"},
		},
		"FeatureTypes": []string{"QUERIES"},
		// QueriesConfig omitted — must return 400.
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code,
		"StartDocumentAnalysis with QUERIES but no QueriesConfig must return 400")
}
