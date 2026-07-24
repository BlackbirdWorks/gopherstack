package textract_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/textract"
)

func TestHandler_AnalyzeExpense(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantStatus int
		wantDocs   bool
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
			},
			wantStatus: http.StatusOK,
			wantDocs:   true,
		},
		{
			name:       "empty body still returns expense docs",
			body:       map[string]any{},
			wantStatus: http.StatusOK,
			wantDocs:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doTextractRequest(t, h, "AnalyzeExpense", tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantDocs {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				docs, ok := resp["ExpenseDocuments"].([]any)
				assert.True(t, ok)
				assert.NotEmpty(t, docs)
			}
		})
	}
}

func TestHandler_GetExpenseAnalysis_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body := map[string]string{"JobId": "nonexistent-job"}
	rec := doTextractRequest(t, h, "GetExpenseAnalysis", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_GetExpenseAnalysis_MissingJobId(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body := map[string]string{}
	rec := doTextractRequest(t, h, "GetExpenseAnalysis", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestHandler_GetExpenseAnalysis_EmptyJobIdRejected validates required JobId
// when explicitly given as an empty string (as opposed to omitted).
func TestHandler_GetExpenseAnalysis_EmptyJobIdRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTextractRequest(t, h, "GetExpenseAnalysis", map[string]any{"JobId": ""})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_StartExpenseAnalysis_ClientRequestTokenIdempotency(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body := map[string]any{
		"DocumentLocation": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "receipt.pdf"},
		},
		"ClientRequestToken": "expense-token-xyz",
	}

	rec1 := doTextractRequest(t, h, "StartExpenseAnalysis", body)
	require.Equal(t, http.StatusOK, rec1.Code)

	var resp1 map[string]string
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &resp1))
	jobID1 := resp1["JobId"]
	require.NotEmpty(t, jobID1)

	rec2 := doTextractRequest(t, h, "StartExpenseAnalysis", body)
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp2 map[string]string
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))

	assert.Equal(t, jobID1, resp2["JobId"], "same ClientRequestToken must return same JobId")
	assert.Equal(t, 1, textract.ExpenseJobCount(h.Backend.(*textract.InMemoryBackend)))
}

// TestHandler_StartExpenseAnalysis_HappyPath tests the full async
// StartExpenseAnalysis -> GetExpenseAnalysis flow.
func TestHandler_StartExpenseAnalysis_HappyPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	startRec := doTextractRequest(t, h, "StartExpenseAnalysis", map[string]any{
		"DocumentLocation": map[string]any{
			"S3Object": map[string]any{"Bucket": "invoices", "Name": "q4.pdf"},
		},
	})

	require.Equal(t, http.StatusOK, startRec.Code)

	var startResp map[string]string
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startResp))
	jobID := startResp["JobId"]
	require.NotEmpty(t, jobID)

	getRec := doTextractRequest(t, h, "GetExpenseAnalysis", map[string]string{"JobId": jobID})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	assert.Equal(t, "SUCCEEDED", getResp["JobStatus"])
	assert.Equal(t, "1.0", getResp["AnalyzeExpenseModelVersion"])
	docs, ok := getResp["ExpenseDocuments"].([]any)
	assert.True(t, ok)
	assert.NotEmpty(t, docs)
}

// TestHandler_StartExpenseAnalysis_MissingBucket validates required fields.
func TestHandler_StartExpenseAnalysis_MissingBucket(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTextractRequest(t, h, "StartExpenseAnalysis", map[string]any{
		"DocumentLocation": map[string]any{
			"S3Object": map[string]any{"Bucket": "", "Name": ""},
		},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestHandler_AnalyzeExpense_SummaryFields verifies AnalyzeExpense returns
// SummaryFields with VENDOR_NAME and TOTAL.
func TestHandler_AnalyzeExpense_SummaryFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTextractRequest(t, h, "AnalyzeExpense", map[string]any{
		"Document": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "invoice.pdf"},
		},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	docs, ok := resp["ExpenseDocuments"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, docs)

	doc, ok := docs[0].(map[string]any)
	require.True(t, ok)

	summaryFields, ok := doc["SummaryFields"].([]any)
	require.True(t, ok, "ExpenseDocument should have SummaryFields")
	require.NotEmpty(t, summaryFields)

	var fieldTypes []string

	for _, sf := range summaryFields {
		sfm, ok2 := sf.(map[string]any)
		if !ok2 {
			continue
		}

		if typeField, ok3 := sfm["Type"].(map[string]any); ok3 {
			if text, ok4 := typeField["Text"].(string); ok4 {
				fieldTypes = append(fieldTypes, text)
			}
		}
	}

	assert.Contains(t, fieldTypes, "VENDOR_NAME")
	assert.Contains(t, fieldTypes, "TOTAL")
}

// TestHandler_AnalyzeExpense_LineItemGroups verifies AnalyzeExpense returns
// LineItemGroups with items.
func TestHandler_AnalyzeExpense_LineItemGroups(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTextractRequest(t, h, "AnalyzeExpense", map[string]any{
		"Document": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "invoice.pdf"},
		},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	docs, _ := resp["ExpenseDocuments"].([]any)
	require.NotEmpty(t, docs)

	doc, _ := docs[0].(map[string]any)
	groups, ok := doc["LineItemGroups"].([]any)
	require.True(t, ok, "ExpenseDocument should have LineItemGroups")
	require.NotEmpty(t, groups)

	group, ok := groups[0].(map[string]any)
	require.True(t, ok)

	items, ok := group["LineItems"].([]any)
	require.True(t, ok)
	assert.NotEmpty(t, items, "LineItemGroup should have LineItems")
}

// TestHandler_AnalyzeExpense_ExpenseIndex verifies the first ExpenseDocument's
// ExpenseIndex is 1.
func TestHandler_AnalyzeExpense_ExpenseIndex(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTextractRequest(t, h, "AnalyzeExpense", map[string]any{
		"Document": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "invoice.pdf"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var m map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &m))
	docs, ok := m["ExpenseDocuments"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, docs)

	doc, ok2 := docs[0].(map[string]any)
	require.True(t, ok2)
	idx, ok3 := doc["ExpenseIndex"].(float64)
	assert.True(t, ok3, "ExpenseDocument must have ExpenseIndex")
	assert.InEpsilon(t, 1.0, idx, 0.001, "first ExpenseDocument.ExpenseIndex must be 1")
}

// TestHandler_GetExpenseAnalysis_ModelVersion verifies GetExpenseAnalysis
// includes AnalyzeExpenseModelVersion.
func TestHandler_GetExpenseAnalysis_ModelVersion(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
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

	var m map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &m))
	ver, ok := m["AnalyzeExpenseModelVersion"].(string)
	assert.True(t, ok, "GetExpenseAnalysis must have AnalyzeExpenseModelVersion")
	assert.Equal(t, "1.0", ver)
}

// TestHandler_GetExpenseAnalysis_RejectsDocumentJobID verifies that
// GetExpenseAnalysis returns InvalidJobIdException (400) when given a job ID
// that belongs to a StartDocumentAnalysis (or StartDocumentTextDetection) job.
// AWS segregates expense jobs from document jobs.
func TestHandler_GetExpenseAnalysis_RejectsDocumentJobID(t *testing.T) {
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

			h := newTestHandler(t)

			startBody := map[string]any{
				"DocumentLocation": map[string]any{
					"S3Object": map[string]any{"Bucket": "b", "Name": "k"},
				},
			}
			if tc.startAction == "StartDocumentAnalysis" {
				startBody["FeatureTypes"] = []string{"TABLES"}
			}

			startRec := doTextractRequest(t, h, tc.startAction, startBody)
			require.Equal(t, http.StatusOK, startRec.Code)

			var startResp map[string]string
			require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startResp))
			jobID := startResp["JobId"]

			getRec := doTextractRequest(t, h, "GetExpenseAnalysis", map[string]any{"JobId": jobID})
			assert.Equal(t, http.StatusBadRequest, getRec.Code)

			var errResp map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &errResp))
			assert.Equal(t, "InvalidJobIdException", errResp["__type"],
				"expense analysis must not accept a document job ID")
		})
	}
}

// TestHandler_AnalyzeExpense_CurrencyAndTypeWireShape locks the real SDK's
// ExpenseField shape: Currency is an ExpenseCurrency{Code, Confidence}
// object (not an ExpenseDetection with Text/Geometry), and Type is an
// ExpenseType{Text, Confidence} object with no Geometry member -- both
// diverge from the shape of ValueDetection/LabelDetection, which do use
// ExpenseDetection.
func TestHandler_AnalyzeExpense_CurrencyAndTypeWireShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTextractRequest(t, h, "AnalyzeExpense", map[string]any{
		"Document": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "invoice.pdf"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	docs, ok := resp["ExpenseDocuments"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, docs)

	doc, ok := docs[0].(map[string]any)
	require.True(t, ok)

	summaryFields, ok := doc["SummaryFields"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, summaryFields)

	var sawCurrency bool

	for _, sf := range summaryFields {
		sfm, ok2 := sf.(map[string]any)
		require.True(t, ok2)

		typeField, ok3 := sfm["Type"].(map[string]any)
		require.True(t, ok3, "every SummaryFields entry must have a Type object")
		_, hasGeometry := typeField["Geometry"]
		assert.False(t, hasGeometry, "ExpenseType must not carry a Geometry key")
		_, hasText := typeField["Text"]
		assert.True(t, hasText, "ExpenseType must carry a Text key")

		currency, hasCurrency := sfm["Currency"].(map[string]any)
		if !hasCurrency {
			continue
		}

		sawCurrency = true
		_, hasCode := currency["Code"]
		assert.True(t, hasCode, "ExpenseCurrency must carry a Code key")
		_, hasCurrencyText := currency["Text"]
		assert.False(t, hasCurrencyText, "ExpenseCurrency must not carry a Text key")
		_, hasCurrencyGeometry := currency["Geometry"]
		assert.False(t, hasCurrencyGeometry, "ExpenseCurrency must not carry a Geometry key")
	}

	assert.True(t, sawCurrency, "TOTAL summary field should carry a Currency object")
}

// TestHandler_GetExpenseAnalysis_Pagination verifies NextToken/MaxResults
// pagination over ExpenseDocuments, mirroring the same pattern
// GetDocumentAnalysis already applies to Blocks. Real AWS's
// GetExpenseAnalysisInput accepts MaxResults/NextToken and
// GetExpenseAnalysisOutput echoes NextToken -- this was previously accepted
// but silently ignored.
func TestHandler_GetExpenseAnalysis_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*textract.InMemoryBackend)

	jobID := "pagination-expense-job"
	docs := make([]textract.ExpenseDocument, 5)
	for i := range docs {
		docs[i] = textract.ExpenseDocument{ExpenseIndex: i + 1}
	}

	textract.AddExpenseJobInternal(b, &textract.ExpenseJob{
		JobID:            jobID,
		JobStatus:        "SUCCEEDED",
		ExpenseDocuments: docs,
	})

	rec1 := doTextractRequest(t, h, "GetExpenseAnalysis", map[string]any{
		"JobId":      jobID,
		"MaxResults": 2,
	})
	require.Equal(t, http.StatusOK, rec1.Code)

	var resp1 struct {
		NextToken        string           `json:"NextToken"`
		ExpenseDocuments []map[string]any `json:"ExpenseDocuments"`
	}
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &resp1))
	assert.Len(t, resp1.ExpenseDocuments, 2)
	require.NotEmpty(t, resp1.NextToken, "first page must return a NextToken")

	rec2 := doTextractRequest(t, h, "GetExpenseAnalysis", map[string]any{
		"JobId":     jobID,
		"NextToken": resp1.NextToken,
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp2 struct {
		NextToken        string           `json:"NextToken"`
		ExpenseDocuments []map[string]any `json:"ExpenseDocuments"`
	}
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))
	assert.Len(t, resp2.ExpenseDocuments, 3, "remaining 3 documents on the second page")
	assert.Empty(t, resp2.NextToken, "no more pages")
}
