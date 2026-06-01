package textract_test

// Audit batch-1: AWS-accuracy coverage for Textract (go-prm9).
//
// Covers:
//   - Protocol: Content-Type application/x-amz-json-1.1 on success and error responses
//   - Error envelope: __type + message fields present in all error responses
//   - Block field accuracy: UUID IDs, Confidence in [0,100], Page field set, TextType on WORD
//   - Geometry accuracy: BoundingBox values in [0,1], Polygon present
//   - Response field accuracy: ModelVersion fields, DocumentMetadata.Pages
//   - AnalyzeExpense: ExpenseIndex=1 on first document
//   - AnalyzeID: DocumentIndex starts at 1, AnalyzeIDModelVersion present
//   - GetExpenseAnalysis: AnalyzeExpenseModelVersion present
//   - GetLendingAnalysis: AnalyzeLendingModelVersion present
//   - Inline document (Bytes field) accepted by DetectDocumentText + AnalyzeDocument
//   - StartDocumentTextDetection ClientRequestToken idempotency
//   - Error paths: ValidationException, InvalidJobIdException, InvalidParameterException

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/textract"
)

// --- audit helpers ---

func a1TextractHandler(t *testing.T) *textract.Handler {
	t.Helper()

	return textract.NewHandler(textract.NewInMemoryBackendSync("000000000000", "us-east-1"))
}

func a1TextractUnmarshal(t *testing.T, body []byte) map[string]any {
	t.Helper()

	var m map[string]any
	require.NoError(t, json.Unmarshal(body, &m))

	return m
}

// --- Protocol accuracy: Content-Type ---

func TestAudit1_Textract_Protocol_ContentType_Success(t *testing.T) {
	t.Parallel()

	h := a1TextractHandler(t)
	rec := doTextractRequest(t, h, "DetectDocumentText", map[string]any{
		"Document": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "doc.pdf"},
		},
	})

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Header().Get("Content-Type"), "application/x-amz-json-1.1",
		"success response must have application/x-amz-json-1.1 content type")
}

func TestAudit1_Textract_Protocol_ContentType_Error(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		action string
		body   map[string]any
	}{
		{
			name:   "invalid_job_id",
			action: "GetDocumentAnalysis",
			body:   map[string]any{"JobId": "no-such-job"},
		},
		{
			name:   "validation_error",
			action: "AnalyzeID",
			body:   map[string]any{"DocumentPages": []any{}},
		},
		{
			name:   "adapter_not_found",
			action: "GetAdapter",
			body:   map[string]any{"AdapterId": "no-such-adapter"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := a1TextractHandler(t)
			rec := doTextractRequest(t, h, tt.action, tt.body)
			assert.GreaterOrEqual(t, rec.Code, http.StatusBadRequest)
			assert.Contains(t, rec.Header().Get("Content-Type"), "application/x-amz-json-1.1",
				"error response must have application/x-amz-json-1.1 content type")
		})
	}
}

// --- Protocol accuracy: Error envelope ---

func TestAudit1_Textract_Protocol_ErrorEnvelope(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		action   string
		body     map[string]any
		wantType string
	}{
		{
			name:     "job_not_found_gives_InvalidJobIdException",
			action:   "GetDocumentAnalysis",
			body:     map[string]any{"JobId": "no-such-job"},
			wantType: "InvalidJobIdException",
		},
		{
			name:     "adapter_not_found_gives_InvalidParameterException",
			action:   "GetAdapter",
			body:     map[string]any{"AdapterId": "no-such-adapter"},
			wantType: "InvalidParameterException",
		},
		{
			name:     "validation_error_gives_ValidationException",
			action:   "StartDocumentAnalysis",
			body:     map[string]any{"DocumentLocation": map[string]any{"S3Object": map[string]any{"Bucket": "", "Name": ""}}},
			wantType: "ValidationException",
		},
		{
			name:     "unknown_action_gives_ValidationException",
			action:   "NoSuchAction",
			body:     map[string]any{},
			wantType: "ValidationException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := a1TextractHandler(t)
			rec := doTextractRequest(t, h, tt.action, tt.body)
			assert.GreaterOrEqual(t, rec.Code, http.StatusBadRequest)

			m := a1TextractUnmarshal(t, rec.Body.Bytes())
			assert.Equal(t, tt.wantType, m["__type"], "__type must match expected error code")
			assert.NotEmpty(t, m["message"], "message must be present in error envelope")
		})
	}
}

// --- Block field accuracy: UUIDs ---

func TestAudit1_Textract_Block_IDs_Are_UUIDs(t *testing.T) {
	t.Parallel()

	h := a1TextractHandler(t)
	rec := doTextractRequest(t, h, "DetectDocumentText", map[string]any{
		"Document": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "doc.pdf"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	m := a1TextractUnmarshal(t, rec.Body.Bytes())
	blocks, ok := m["Blocks"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, blocks)

	for _, b := range blocks {
		bm, ok2 := b.(map[string]any)
		require.True(t, ok2)
		id, ok3 := bm["Id"].(string)
		require.True(t, ok3, "block must have string Id")
		assert.NotEmpty(t, id, "block Id must not be empty")
		// UUID format: 8-4-4-4-12 hex chars with dashes.
		parts := strings.Split(id, "-")
		assert.Len(t, parts, 5, "block Id should be UUID format (8-4-4-4-12): %q", id)
	}
}

// --- Block field accuracy: Confidence ---

func TestAudit1_Textract_Block_Confidence_InRange(t *testing.T) {
	t.Parallel()

	h := a1TextractHandler(t)
	rec := doTextractRequest(t, h, "DetectDocumentText", map[string]any{
		"Document": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "doc.pdf"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	m := a1TextractUnmarshal(t, rec.Body.Bytes())
	blocks, _ := m["Blocks"].([]any)
	require.NotEmpty(t, blocks)

	for _, b := range blocks {
		bm, ok := b.(map[string]any)
		if !ok {
			continue
		}

		conf, ok2 := bm["Confidence"].(float64)
		if !ok2 {
			continue
		}

		assert.GreaterOrEqual(t, conf, 0.0, "Confidence must be >= 0")
		assert.LessOrEqual(t, conf, 100.0, "Confidence must be <= 100")
	}
}

// --- Block field accuracy: TextType on WORD ---

func TestAudit1_Textract_Block_WORD_Has_TextType(t *testing.T) {
	t.Parallel()

	h := a1TextractHandler(t)
	rec := doTextractRequest(t, h, "DetectDocumentText", map[string]any{
		"Document": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "doc.pdf"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	m := a1TextractUnmarshal(t, rec.Body.Bytes())
	blocks, _ := m["Blocks"].([]any)
	require.NotEmpty(t, blocks)

	var wordCount int

	for _, b := range blocks {
		bm, ok := b.(map[string]any)
		if !ok || bm["BlockType"] != "WORD" {
			continue
		}

		wordCount++
		textType, ok2 := bm["TextType"].(string)
		assert.True(t, ok2 && textType != "", "WORD block must have non-empty TextType")
		assert.True(t, textType == "PRINTED" || textType == "HANDWRITING",
			"TextType must be PRINTED or HANDWRITING, got %q", textType)
	}

	assert.Greater(t, wordCount, 0, "expected at least one WORD block")
}

// --- Block field accuracy: Page field ---

func TestAudit1_Textract_Block_Page_IsSet(t *testing.T) {
	t.Parallel()

	h := a1TextractHandler(t)
	rec := doTextractRequest(t, h, "DetectDocumentText", map[string]any{
		"Document": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "doc.pdf"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	m := a1TextractUnmarshal(t, rec.Body.Bytes())
	blocks, _ := m["Blocks"].([]any)
	require.NotEmpty(t, blocks)

	for _, b := range blocks {
		bm, ok := b.(map[string]any)
		if !ok {
			continue
		}

		page, ok2 := bm["Page"].(float64)
		assert.True(t, ok2, "block %q should have Page field", bm["BlockType"])
		assert.GreaterOrEqual(t, int(page), 1, "Page must be >= 1")
	}
}

// --- Geometry accuracy: BoundingBox ---

func TestAudit1_Textract_Block_Geometry_BoundingBox_Valid(t *testing.T) {
	t.Parallel()

	h := a1TextractHandler(t)
	rec := doTextractRequest(t, h, "DetectDocumentText", map[string]any{
		"Document": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "doc.pdf"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	m := a1TextractUnmarshal(t, rec.Body.Bytes())
	blocks, _ := m["Blocks"].([]any)
	require.NotEmpty(t, blocks)

	for _, b := range blocks {
		bm, ok := b.(map[string]any)
		if !ok {
			continue
		}

		geo, ok2 := bm["Geometry"].(map[string]any)
		if !ok2 {
			continue
		}

		bb, ok3 := geo["BoundingBox"].(map[string]any)
		if !ok3 {
			continue
		}

		for _, key := range []string{"Left", "Top", "Width", "Height"} {
			val, ok4 := bb[key].(float64)
			assert.True(t, ok4, "BoundingBox.%s must be a number", key)
			assert.GreaterOrEqual(t, val, 0.0, "BoundingBox.%s must be >= 0", key)
			assert.LessOrEqual(t, val, 1.0, "BoundingBox.%s must be <= 1", key)
		}

		// Polygon must be present.
		poly, ok5 := geo["Polygon"].([]any)
		assert.True(t, ok5 && len(poly) >= 4, "Geometry.Polygon must have at least 4 points")
	}
}

// --- Response field accuracy: DocumentMetadata.Pages ---

func TestAudit1_Textract_DetectDocumentText_DocumentMetadata_Pages(t *testing.T) {
	t.Parallel()

	h := a1TextractHandler(t)
	rec := doTextractRequest(t, h, "DetectDocumentText", map[string]any{
		"Document": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "doc.pdf"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	m := a1TextractUnmarshal(t, rec.Body.Bytes())
	meta, ok := m["DocumentMetadata"].(map[string]any)
	require.True(t, ok, "response must have DocumentMetadata")
	pages, ok2 := meta["Pages"].(float64)
	assert.True(t, ok2, "DocumentMetadata must have Pages field")
	assert.GreaterOrEqual(t, int(pages), 1, "Pages must be >= 1")
}

func TestAudit1_Textract_AnalyzeDocument_DocumentMetadata_Pages(t *testing.T) {
	t.Parallel()

	h := a1TextractHandler(t)
	rec := doTextractRequest(t, h, "AnalyzeDocument", map[string]any{
		"Document": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "form.pdf"},
		},
		"FeatureTypes": []string{"FORMS"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	m := a1TextractUnmarshal(t, rec.Body.Bytes())
	meta, ok := m["DocumentMetadata"].(map[string]any)
	require.True(t, ok, "AnalyzeDocument response must have DocumentMetadata")
	pages, ok2 := meta["Pages"].(float64)
	assert.True(t, ok2, "DocumentMetadata must have Pages")
	assert.GreaterOrEqual(t, int(pages), 1)
}

// --- AnalyzeExpense: ExpenseIndex ---

func TestAudit1_Textract_AnalyzeExpense_ExpenseIndex(t *testing.T) {
	t.Parallel()

	h := a1TextractHandler(t)
	rec := doTextractRequest(t, h, "AnalyzeExpense", map[string]any{
		"Document": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "invoice.pdf"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	m := a1TextractUnmarshal(t, rec.Body.Bytes())
	docs, ok := m["ExpenseDocuments"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, docs)

	doc, ok2 := docs[0].(map[string]any)
	require.True(t, ok2)
	idx, ok3 := doc["ExpenseIndex"].(float64)
	assert.True(t, ok3, "ExpenseDocument must have ExpenseIndex")
	assert.Equal(t, 1.0, idx, "first ExpenseDocument.ExpenseIndex must be 1")
}

// --- AnalyzeID: DocumentIndex and AnalyzeIDModelVersion ---

func TestAudit1_Textract_AnalyzeID_DocumentIndex(t *testing.T) {
	t.Parallel()

	h := a1TextractHandler(t)
	rec := doTextractRequest(t, h, "AnalyzeID", map[string]any{
		"DocumentPages": []any{
			map[string]any{"S3Object": map[string]any{"Bucket": "b", "Name": "id.jpg"}},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	m := a1TextractUnmarshal(t, rec.Body.Bytes())
	docs, ok := m["IdentityDocuments"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, docs)

	doc, ok2 := docs[0].(map[string]any)
	require.True(t, ok2)
	idx, ok3 := doc["DocumentIndex"].(float64)
	assert.True(t, ok3, "IdentityDocument must have DocumentIndex")
	assert.Equal(t, 1.0, idx, "first IdentityDocument.DocumentIndex must be 1")
}

func TestAudit1_Textract_AnalyzeID_ModelVersion(t *testing.T) {
	t.Parallel()

	h := a1TextractHandler(t)
	rec := doTextractRequest(t, h, "AnalyzeID", map[string]any{
		"DocumentPages": []any{
			map[string]any{"S3Object": map[string]any{"Bucket": "b", "Name": "id.jpg"}},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	m := a1TextractUnmarshal(t, rec.Body.Bytes())
	ver, ok := m["AnalyzeIDModelVersion"].(string)
	assert.True(t, ok, "AnalyzeID response must have AnalyzeIDModelVersion")
	assert.Equal(t, "1.0", ver)
}

// --- GetExpenseAnalysis: AnalyzeExpenseModelVersion ---

func TestAudit1_Textract_GetExpenseAnalysis_ModelVersion(t *testing.T) {
	t.Parallel()

	h := a1TextractHandler(t)
	startRec := doTextractRequest(t, h, "StartExpenseAnalysis", map[string]any{
		"DocumentLocation": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "inv.pdf"},
		},
	})
	require.Equal(t, http.StatusOK, startRec.Code)

	var startResp map[string]string
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startResp))
	jobID := startResp["JobId"]
	require.NotEmpty(t, jobID)

	getRec := doTextractRequest(t, h, "GetExpenseAnalysis", map[string]string{"JobId": jobID})
	require.Equal(t, http.StatusOK, getRec.Code)

	m := a1TextractUnmarshal(t, getRec.Body.Bytes())
	ver, ok := m["AnalyzeExpenseModelVersion"].(string)
	assert.True(t, ok, "GetExpenseAnalysis must have AnalyzeExpenseModelVersion")
	assert.Equal(t, "1.0", ver)
}

// --- GetLendingAnalysis: AnalyzeLendingModelVersion ---

func TestAudit1_Textract_GetLendingAnalysis_ModelVersion(t *testing.T) {
	t.Parallel()

	h := a1TextractHandler(t)
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

	m := a1TextractUnmarshal(t, getRec.Body.Bytes())
	ver, ok := m["AnalyzeLendingModelVersion"].(string)
	assert.True(t, ok, "GetLendingAnalysis must have AnalyzeLendingModelVersion")
	assert.Equal(t, "1.0", ver)
}

// --- GetDocumentAnalysis: AnalyzeDocumentModelVersion ---

func TestAudit1_Textract_GetDocumentAnalysis_ModelVersion(t *testing.T) {
	t.Parallel()

	h := a1TextractHandler(t)
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

	m := a1TextractUnmarshal(t, getRec.Body.Bytes())
	ver, ok := m["AnalyzeDocumentModelVersion"].(string)
	assert.True(t, ok, "GetDocumentAnalysis must have AnalyzeDocumentModelVersion")
	assert.Equal(t, "1.0", ver)
}

// --- GetDocumentTextDetection: DetectDocumentTextModelVersion ---

func TestAudit1_Textract_GetDocumentTextDetection_ModelVersion(t *testing.T) {
	t.Parallel()

	h := a1TextractHandler(t)
	startRec := doTextractRequest(t, h, "StartDocumentTextDetection", map[string]any{
		"DocumentLocation": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "doc.pdf"},
		},
	})
	require.Equal(t, http.StatusOK, startRec.Code)

	var startResp map[string]string
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startResp))
	jobID := startResp["JobId"]

	getRec := doTextractRequest(t, h, "GetDocumentTextDetection", map[string]string{"JobId": jobID})
	require.Equal(t, http.StatusOK, getRec.Code)

	m := a1TextractUnmarshal(t, getRec.Body.Bytes())
	ver, ok := m["DetectDocumentTextModelVersion"].(string)
	assert.True(t, ok, "GetDocumentTextDetection must have DetectDocumentTextModelVersion")
	assert.Equal(t, "1.0", ver)
}

// --- Inline document (Bytes) ---

func TestAudit1_Textract_DetectDocumentText_Bytes_Input(t *testing.T) {
	t.Parallel()

	h := a1TextractHandler(t)
	rec := doTextractRequest(t, h, "DetectDocumentText", map[string]any{
		"Document": map[string]any{
			"Bytes": []byte("fake-pdf-content"),
		},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	m := a1TextractUnmarshal(t, rec.Body.Bytes())
	blocks, ok := m["Blocks"].([]any)
	assert.True(t, ok && len(blocks) > 0, "DetectDocumentText with Bytes must return Blocks")
}

func TestAudit1_Textract_AnalyzeDocument_Bytes_Input(t *testing.T) {
	t.Parallel()

	h := a1TextractHandler(t)
	rec := doTextractRequest(t, h, "AnalyzeDocument", map[string]any{
		"Document": map[string]any{
			"Bytes": []byte("fake-pdf-content"),
		},
		"FeatureTypes": []string{"FORMS"},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	m := a1TextractUnmarshal(t, rec.Body.Bytes())
	blocks, ok := m["Blocks"].([]any)
	assert.True(t, ok && len(blocks) > 0, "AnalyzeDocument with Bytes must return Blocks")
}

// --- StartDocumentTextDetection ClientRequestToken idempotency ---

func TestAudit1_Textract_StartDocumentTextDetection_ClientRequestToken_Idempotency(t *testing.T) {
	t.Parallel()

	h := a1TextractHandler(t)
	body := map[string]any{
		"DocumentLocation": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "doc.pdf"},
		},
		"ClientRequestToken": "txt-detect-token-xyz",
	}

	rec1 := doTextractRequest(t, h, "StartDocumentTextDetection", body)
	require.Equal(t, http.StatusOK, rec1.Code)

	var resp1 map[string]string
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &resp1))
	jobID1 := resp1["JobId"]
	require.NotEmpty(t, jobID1)

	rec2 := doTextractRequest(t, h, "StartDocumentTextDetection", body)
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp2 map[string]string
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))

	assert.Equal(t, jobID1, resp2["JobId"], "same ClientRequestToken must return same JobId")
}

// --- JobId is a non-empty string ---

func TestAudit1_Textract_StartDocumentAnalysis_JobId_NonEmpty(t *testing.T) {
	t.Parallel()

	h := a1TextractHandler(t)
	rec := doTextractRequest(t, h, "StartDocumentAnalysis", map[string]any{
		"DocumentLocation": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "doc.pdf"},
		},
		"FeatureTypes": []string{"TABLES"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	m := a1TextractUnmarshal(t, rec.Body.Bytes())
	jobID, ok := m["JobId"].(string)
	assert.True(t, ok, "StartDocumentAnalysis response must have string JobId")
	assert.NotEmpty(t, jobID, "JobId must not be empty")
	// UUID format check.
	parts := strings.Split(jobID, "-")
	assert.Len(t, parts, 5, "JobId should be UUID format: %q", jobID)
}

// --- AnalyzeID: multiple DocumentPages → DocumentIndex increments ---

func TestAudit1_Textract_AnalyzeID_MultiPage_DocumentIndex(t *testing.T) {
	t.Parallel()

	h := a1TextractHandler(t)
	rec := doTextractRequest(t, h, "AnalyzeID", map[string]any{
		"DocumentPages": []any{
			map[string]any{"S3Object": map[string]any{"Bucket": "b", "Name": "front.jpg"}},
			map[string]any{"S3Object": map[string]any{"Bucket": "b", "Name": "back.jpg"}},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	m := a1TextractUnmarshal(t, rec.Body.Bytes())
	docs, ok := m["IdentityDocuments"].([]any)
	require.True(t, ok)
	require.Len(t, docs, 2, "two DocumentPages should yield two IdentityDocuments")

	doc0, ok0 := docs[0].(map[string]any)
	doc1, ok1 := docs[1].(map[string]any)
	require.True(t, ok0 && ok1)

	assert.Equal(t, 1.0, doc0["DocumentIndex"], "first DocumentIndex must be 1")
	assert.Equal(t, 2.0, doc1["DocumentIndex"], "second DocumentIndex must be 2")
}

// --- GetDocumentAnalysis job status is SUCCEEDED ---

func TestAudit1_Textract_GetDocumentAnalysis_JobStatus(t *testing.T) {
	t.Parallel()

	h := a1TextractHandler(t)
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

	m := a1TextractUnmarshal(t, getRec.Body.Bytes())
	assert.Equal(t, "SUCCEEDED", m["JobStatus"], "synchronous backend job must SUCCEED immediately")
}
