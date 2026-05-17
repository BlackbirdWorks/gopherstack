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

// ---- helpers ----

func newAccuracyHandler(t *testing.T) *textract.Handler {
	t.Helper()

	return textract.NewHandler(textract.NewInMemoryBackendSync("123456789012", "us-east-1"))
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

// Test 1: DetectDocumentText returns PAGE/LINE/WORD blocks with Relationships.
func TestAccuracy_DetectDocumentText_BlockStructure(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler(t)
	rec := doTextractRequest(t, h, "DetectDocumentText", map[string]any{
		"Document": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "doc.pdf"},
		},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	pages := blocksOfType(t, rec.Body.Bytes(), "PAGE")
	lines := blocksOfType(t, rec.Body.Bytes(), "LINE")
	words := blocksOfType(t, rec.Body.Bytes(), "WORD")

	assert.NotEmpty(t, pages, "expected at least one PAGE block")
	assert.NotEmpty(t, lines, "expected at least one LINE block")
	assert.NotEmpty(t, words, "expected at least one WORD block")

	// PAGE block should have CHILD relationships.
	page := pages[0]
	rels, ok := page["Relationships"].([]any)
	assert.True(t, ok, "PAGE block should have Relationships")
	assert.NotEmpty(t, rels, "PAGE Relationships should not be empty")
}

// Test 2: DetectDocumentText returns DetectDocumentTextModelVersion.
func TestAccuracy_DetectDocumentText_ModelVersion(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler(t)
	rec := doTextractRequest(t, h, "DetectDocumentText", map[string]any{
		"Document": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "doc.pdf"},
		},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "1.0", resp["DetectDocumentTextModelVersion"])
}

// Test 3: AnalyzeDocument FORMS → KEY_VALUE_SET blocks present.
func TestAccuracy_AnalyzeDocument_Forms_KeyValueSetBlocks(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler(t)
	rec := doTextractRequest(t, h, "AnalyzeDocument", map[string]any{
		"Document": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "form.pdf"},
		},
		"FeatureTypes": []string{"FORMS"},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	kvBlocks := blocksOfType(t, rec.Body.Bytes(), "KEY_VALUE_SET")
	assert.NotEmpty(t, kvBlocks, "FORMS feature should produce KEY_VALUE_SET blocks")

	// Check for KEY and VALUE entity types.
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

// Test 4: AnalyzeDocument TABLES → TABLE+CELL blocks present.
func TestAccuracy_AnalyzeDocument_Tables_TableCellBlocks(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler(t)
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

	// CELL blocks should have RowIndex and ColumnIndex.
	for _, cell := range cells {
		_, hasRow := cell["RowIndex"]
		_, hasCol := cell["ColumnIndex"]
		assert.True(t, hasRow, "CELL should have RowIndex")
		assert.True(t, hasCol, "CELL should have ColumnIndex")
	}
}

// Test 5: AnalyzeDocument QUERIES with QueriesConfig → QUERY+QUERY_RESULT blocks.
func TestAccuracy_AnalyzeDocument_Queries_QueryBlocks(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler(t)
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

	// Each QUERY block should have an ANSWER relationship.
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

// Test 6: AnalyzeDocument SIGNATURES → SIGNATURE block.
func TestAccuracy_AnalyzeDocument_Signatures_SignatureBlock(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler(t)
	rec := doTextractRequest(t, h, "AnalyzeDocument", map[string]any{
		"Document": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "signed.pdf"},
		},
		"FeatureTypes": []string{"SIGNATURES"},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	sigBlocks := blocksOfType(t, rec.Body.Bytes(), "SIGNATURE")
	assert.NotEmpty(t, sigBlocks, "SIGNATURES feature should produce SIGNATURE blocks")

	// SIGNATURE block should have Geometry.
	sig := sigBlocks[0]
	_, hasGeom := sig["Geometry"]
	assert.True(t, hasGeom, "SIGNATURE block should have Geometry")
}

// Test 7: AnalyzeDocument LAYOUT → LAYOUT_TEXT block.
func TestAccuracy_AnalyzeDocument_Layout_LayoutBlocks(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler(t)
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

// Test 8: AnalyzeDocument includes AnalyzeDocumentModelVersion in response.
func TestAccuracy_AnalyzeDocument_ModelVersion(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler(t)
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

// Test 9: AnalyzeExpense returns SummaryFields with VENDOR_NAME and TOTAL.
func TestAccuracy_AnalyzeExpense_SummaryFields(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler(t)
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

// Test 10: AnalyzeExpense returns LineItemGroups with items.
func TestAccuracy_AnalyzeExpense_LineItemGroups(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler(t)
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

// Test 11: AnalyzeID returns IdentityDocumentFields with FIRST_NAME and DATE_OF_BIRTH.
func TestAccuracy_AnalyzeID_IdentityDocumentFields(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler(t)
	rec := doTextractRequest(t, h, "AnalyzeID", map[string]any{
		"DocumentPages": []any{
			map[string]any{
				"S3Object": map[string]any{"Bucket": "b", "Name": "id-front.jpg"},
			},
		},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	docs, ok := resp["IdentityDocuments"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, docs)

	doc, ok := docs[0].(map[string]any)
	require.True(t, ok)

	fields, ok := doc["IdentityDocumentFields"].([]any)
	require.True(t, ok, "IdentityDocument should have IdentityDocumentFields")
	require.NotEmpty(t, fields)

	var fieldTypes []string

	for _, f := range fields {
		fm, ok2 := f.(map[string]any)
		if !ok2 {
			continue
		}

		if typeField, ok3 := fm["Type"].(map[string]any); ok3 {
			if text, ok4 := typeField["Text"].(string); ok4 {
				fieldTypes = append(fieldTypes, text)
			}
		}
	}

	assert.Contains(t, fieldTypes, "FIRST_NAME")
	assert.Contains(t, fieldTypes, "DATE_OF_BIRTH")
	assert.Contains(t, fieldTypes, "DOCUMENT_NUMBER")
}

// Test 12: AnalyzeID DATE_OF_BIRTH has NormalizedValue with ValueType=DATE.
func TestAccuracy_AnalyzeID_NormalizedValue_Date(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler(t)
	rec := doTextractRequest(t, h, "AnalyzeID", map[string]any{
		"DocumentPages": []any{
			map[string]any{
				"S3Object": map[string]any{"Bucket": "b", "Name": "id.jpg"},
			},
		},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	docs, _ := resp["IdentityDocuments"].([]any)
	doc, _ := docs[0].(map[string]any)
	fields, _ := doc["IdentityDocumentFields"].([]any)

	var foundDateNormalized bool

	for _, f := range fields {
		fm, ok := f.(map[string]any)
		if !ok {
			continue
		}

		typeField, ok2 := fm["Type"].(map[string]any)
		if !ok2 || typeField["Text"] != "DATE_OF_BIRTH" {
			continue
		}

		valDet, ok3 := fm["ValueDetection"].(map[string]any)
		if !ok3 {
			continue
		}

		normVal, ok4 := valDet["NormalizedValue"].(map[string]any)
		if ok4 && normVal["ValueType"] == "DATE" {
			foundDateNormalized = true
		}
	}

	assert.True(t, foundDateNormalized, "DATE_OF_BIRTH should have NormalizedValue with ValueType=DATE")
}

// Test 13: StartDocumentTextDetection → GetDocumentTextDetection pagination (MaxResults).
func TestAccuracy_GetDocumentTextDetection_Pagination(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler(t)

	startRec := doTextractRequest(t, h, "StartDocumentTextDetection", map[string]any{
		"DocumentLocation": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "doc.pdf"},
		},
	})
	require.Equal(t, http.StatusOK, startRec.Code)

	var startResp map[string]string
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startResp))
	jobID := startResp["JobId"]

	// Get first page with MaxResults=3.
	getRec1 := doTextractRequest(t, h, "GetDocumentTextDetection", map[string]any{
		"JobId":      jobID,
		"MaxResults": 3,
	})
	require.Equal(t, http.StatusOK, getRec1.Code)

	var getResp1 map[string]any
	require.NoError(t, json.Unmarshal(getRec1.Body.Bytes(), &getResp1))

	blocks1, _ := getResp1["Blocks"].([]any)
	assert.Len(t, blocks1, 3, "first page should have 3 blocks")

	nextToken, _ := getResp1["NextToken"].(string)
	assert.NotEmpty(t, nextToken, "NextToken should be present when more blocks exist")

	// Get second page using the NextToken.
	getRec2 := doTextractRequest(t, h, "GetDocumentTextDetection", map[string]any{
		"JobId":      jobID,
		"MaxResults": 10,
		"NextToken":  nextToken,
	})
	require.Equal(t, http.StatusOK, getRec2.Code)

	var getResp2 map[string]any
	require.NoError(t, json.Unmarshal(getRec2.Body.Bytes(), &getResp2))

	blocks2, _ := getResp2["Blocks"].([]any)
	assert.NotEmpty(t, blocks2, "second page should have remaining blocks")
}

// Test 14: StartDocumentAnalysis with ClientRequestToken idempotency.
func TestAccuracy_StartDocumentAnalysis_ClientRequestToken_Idempotency(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler(t)

	startBody := map[string]any{
		"DocumentLocation": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "doc.pdf"},
		},
		"ClientRequestToken": "unique-token-abc123",
	}

	// First call.
	rec1 := doTextractRequest(t, h, "StartDocumentAnalysis", startBody)
	require.Equal(t, http.StatusOK, rec1.Code)

	var resp1 map[string]string
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &resp1))
	jobID1 := resp1["JobId"]

	// Second call with same token should return same JobId.
	rec2 := doTextractRequest(t, h, "StartDocumentAnalysis", startBody)
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp2 map[string]string
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))
	jobID2 := resp2["JobId"]

	assert.Equal(t, jobID1, jobID2, "same ClientRequestToken should return the same JobId")
}

// Test 15: Get* returns IN_PROGRESS then SUCCEEDED with proper async delay.
func TestAccuracy_AsyncJob_InProgressThenSucceeded(t *testing.T) {
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

// Test 16: Get* Blocks have Geometry present.
func TestAccuracy_GetDocumentAnalysis_Blocks_HaveGeometry(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler(t)

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

	// At least the PAGE block should have Geometry.
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

// Test 17: CreateAdapter with invalid FeatureType (TABLES) → error.
func TestAccuracy_CreateAdapter_InvalidFeatureType_TABLES(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler(t)
	rec := doTextractRequest(t, h, "CreateAdapter", map[string]any{
		"AdapterName":  "bad-adapter",
		"FeatureTypes": []string{"TABLES"},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code, "TABLES is not valid for adapters")
}

// Test 18: CreateAdapter ClientRequestToken dedup returns same AdapterId.
func TestAccuracy_CreateAdapter_ClientRequestToken_Dedup(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler(t)
	body := map[string]any{
		"AdapterName":        "dedup-adapter",
		"FeatureTypes":       []string{"FORMS"},
		"ClientRequestToken": "adapter-token-xyz",
	}

	rec1 := doTextractRequest(t, h, "CreateAdapter", body)
	require.Equal(t, http.StatusOK, rec1.Code)

	var resp1 map[string]string
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &resp1))

	rec2 := doTextractRequest(t, h, "CreateAdapter", body)
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp2 map[string]string
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))

	assert.Equal(t, resp1["AdapterId"], resp2["AdapterId"], "same token should return same AdapterId")
}

// Test 19: CreateAdapterVersion includes DatasetConfig in GetAdapterVersion.
func TestAccuracy_CreateAdapterVersion_DatasetConfig(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler(t)

	// Create adapter first.
	createAdapterRec := doTextractRequest(t, h, "CreateAdapter", map[string]any{
		"AdapterName":  "dataset-adapter",
		"FeatureTypes": []string{"FORMS"},
	})
	require.Equal(t, http.StatusOK, createAdapterRec.Code)

	var createAdapterResp map[string]string
	require.NoError(t, json.Unmarshal(createAdapterRec.Body.Bytes(), &createAdapterResp))
	adapterID := createAdapterResp["AdapterId"]

	// Create adapter version with DatasetConfig.
	createVersionRec := doTextractRequest(t, h, "CreateAdapterVersion", map[string]any{
		"AdapterId": adapterID,
		"DatasetConfig": map[string]any{
			"ManifestS3Object": map[string]any{
				"Bucket": "my-dataset-bucket",
				"Name":   "manifest.json",
			},
		},
		"KMSKeyId": "arn:aws:kms:us-east-1:123456789012:key/abcd1234",
	})
	require.Equal(t, http.StatusOK, createVersionRec.Code)

	var createVersionResp map[string]string
	require.NoError(t, json.Unmarshal(createVersionRec.Body.Bytes(), &createVersionResp))
	adapterVersion := createVersionResp["AdapterVersion"]

	// Get adapter version and check DatasetConfig is present.
	getVersionRec := doTextractRequest(t, h, "GetAdapterVersion", map[string]any{
		"AdapterId":      adapterID,
		"AdapterVersion": adapterVersion,
	})
	require.Equal(t, http.StatusOK, getVersionRec.Code)

	var getVersionResp map[string]any
	require.NoError(t, json.Unmarshal(getVersionRec.Body.Bytes(), &getVersionResp))

	datasetConfig, ok := getVersionResp["DatasetConfig"].(map[string]any)
	require.True(t, ok, "GetAdapterVersion should include DatasetConfig")
	manifest, ok2 := datasetConfig["ManifestS3Object"].(map[string]any)
	require.True(t, ok2)
	assert.Equal(t, "my-dataset-bucket", manifest["Bucket"])

	assert.Equal(t, "arn:aws:kms:us-east-1:123456789012:key/abcd1234", getVersionResp["KMSKeyId"])
}

// Test 20: AdapterVersion starts CREATION_IN_PROGRESS then becomes ACTIVE.
func TestAccuracy_AdapterVersion_InProgressThenActive(t *testing.T) {
	t.Parallel()

	// Use a backend with a short async delay.
	b := textract.NewInMemoryBackendSync("123456789012", "us-east-1")
	textract.SetBackendAsyncDelay(b, 50*time.Millisecond)
	h := textract.NewHandler(b)

	// Create adapter.
	createAdapterRec := doTextractRequest(t, h, "CreateAdapter", map[string]any{
		"AdapterName":  "lifecycle-adapter",
		"FeatureTypes": []string{"FORMS"},
	})
	require.Equal(t, http.StatusOK, createAdapterRec.Code)

	var createAdapterResp map[string]string
	require.NoError(t, json.Unmarshal(createAdapterRec.Body.Bytes(), &createAdapterResp))
	adapterID := createAdapterResp["AdapterId"]

	// Create version.
	createVersionRec := doTextractRequest(t, h, "CreateAdapterVersion", map[string]any{
		"AdapterId": adapterID,
	})
	require.Equal(t, http.StatusOK, createVersionRec.Code)

	var createVersionResp map[string]string
	require.NoError(t, json.Unmarshal(createVersionRec.Body.Bytes(), &createVersionResp))
	adapterVersion := createVersionResp["AdapterVersion"]

	// Immediately check: should be CREATION_IN_PROGRESS.
	getRec1 := doTextractRequest(t, h, "GetAdapterVersion", map[string]any{
		"AdapterId":      adapterID,
		"AdapterVersion": adapterVersion,
	})
	require.Equal(t, http.StatusOK, getRec1.Code)

	var getResp1 map[string]any
	require.NoError(t, json.Unmarshal(getRec1.Body.Bytes(), &getResp1))
	assert.Equal(t, "CREATION_IN_PROGRESS", getResp1["Status"])

	// After delay, should be ACTIVE.
	time.Sleep(200 * time.Millisecond)

	getRec2 := doTextractRequest(t, h, "GetAdapterVersion", map[string]any{
		"AdapterId":      adapterID,
		"AdapterVersion": adapterVersion,
	})
	require.Equal(t, http.StatusOK, getRec2.Code)

	var getResp2 map[string]any
	require.NoError(t, json.Unmarshal(getRec2.Body.Bytes(), &getResp2))
	assert.Equal(t, "ACTIVE", getResp2["Status"])
}

// Test 21: AdapterVersion EvaluationMetrics are returned in GetAdapterVersion.
func TestAccuracy_AdapterVersion_EvaluationMetrics(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler(t)

	createAdapterRec := doTextractRequest(t, h, "CreateAdapter", map[string]any{
		"AdapterName":  "metrics-adapter",
		"FeatureTypes": []string{"QUERIES"},
	})
	require.Equal(t, http.StatusOK, createAdapterRec.Code)

	var createAdapterResp map[string]string
	require.NoError(t, json.Unmarshal(createAdapterRec.Body.Bytes(), &createAdapterResp))
	adapterID := createAdapterResp["AdapterId"]

	createVersionRec := doTextractRequest(t, h, "CreateAdapterVersion", map[string]any{
		"AdapterId": adapterID,
	})
	require.Equal(t, http.StatusOK, createVersionRec.Code)

	var createVersionResp map[string]string
	require.NoError(t, json.Unmarshal(createVersionRec.Body.Bytes(), &createVersionResp))
	adapterVersion := createVersionResp["AdapterVersion"]

	getVersionRec := doTextractRequest(t, h, "GetAdapterVersion", map[string]any{
		"AdapterId":      adapterID,
		"AdapterVersion": adapterVersion,
	})
	require.Equal(t, http.StatusOK, getVersionRec.Code)

	var getVersionResp map[string]any
	require.NoError(t, json.Unmarshal(getVersionRec.Body.Bytes(), &getVersionResp))

	evalMetrics, ok := getVersionResp["EvaluationMetrics"].(map[string]any)
	require.True(t, ok, "GetAdapterVersion should include EvaluationMetrics")
	assert.InDelta(t, 0.85, evalMetrics["F1Score"], 0.001)
	assert.InDelta(t, 0.88, evalMetrics["Precision"], 0.001)
	assert.InDelta(t, 0.82, evalMetrics["Recall"], 0.001)
}

// Test 22: TagResource by ARN + ListTagsForResource round-trip.
func TestAccuracy_TagResource_ByARN_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler(t)
	b := h.Backend.(*textract.InMemoryBackend)

	createRec := doTextractRequest(t, h, "CreateAdapter", map[string]any{
		"AdapterName":  "arn-tag-adapter",
		"FeatureTypes": []string{"FORMS"},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]string
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	adapterID := createResp["AdapterId"]

	// Build an ARN using the backend helper.
	arn := b.BuildAdapterARN(adapterID)
	assert.Contains(t, arn, adapterID)
	assert.Contains(t, arn, "arn:aws:textract:")

	// Tag via ARN.
	tagRec := doTextractRequest(t, h, "TagResource", map[string]any{
		"ResourceARN": arn,
		"Tags":        map[string]string{"env": "prod", "purpose": "accuracy-test"},
	})
	require.Equal(t, http.StatusOK, tagRec.Code)

	// List via ARN.
	listRec := doTextractRequest(t, h, "ListTagsForResource", map[string]any{
		"ResourceARN": arn,
	})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	tags, ok := listResp["Tags"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "prod", tags["env"])
	assert.Equal(t, "accuracy-test", tags["purpose"])
}

// Test 23: AdapterVersion ARN tagging works.
func TestAccuracy_TagResource_AdapterVersionARN(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler(t)
	b := h.Backend.(*textract.InMemoryBackend)

	// Create adapter.
	createAdapterRec := doTextractRequest(t, h, "CreateAdapter", map[string]any{
		"AdapterName":  "version-tag-adapter",
		"FeatureTypes": []string{"QUERIES"},
	})
	require.Equal(t, http.StatusOK, createAdapterRec.Code)

	var createAdapterResp map[string]string
	require.NoError(t, json.Unmarshal(createAdapterRec.Body.Bytes(), &createAdapterResp))
	adapterID := createAdapterResp["AdapterId"]

	// Create adapter version.
	createVersionRec := doTextractRequest(t, h, "CreateAdapterVersion", map[string]any{
		"AdapterId": adapterID,
	})
	require.Equal(t, http.StatusOK, createVersionRec.Code)

	var createVersionResp map[string]string
	require.NoError(t, json.Unmarshal(createVersionRec.Body.Bytes(), &createVersionResp))
	adapterVersion := createVersionResp["AdapterVersion"]

	// Build ARN.
	versionARN := b.BuildAdapterVersionARN(adapterID, adapterVersion)
	assert.Contains(t, versionARN, "version/")

	// Tag via version ARN.
	tagRec := doTextractRequest(t, h, "TagResource", map[string]any{
		"ResourceARN": versionARN,
		"Tags":        map[string]string{"version-tag": "v1"},
	})
	require.Equal(t, http.StatusOK, tagRec.Code)

	// List via version ARN.
	listRec := doTextractRequest(t, h, "ListTagsForResource", map[string]any{
		"ResourceARN": versionARN,
	})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	tags, ok := listResp["Tags"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "v1", tags["version-tag"])
}

// Test 24: GetLendingAnalysis returns expanded LendingResult with extractions.
func TestAccuracy_GetLendingAnalysis_ExpandedLendingResult(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler(t)

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

	// Check PageClassification is present.
	_, hasPC := result["PageClassification"]
	assert.True(t, hasPC, "LendingResult should have PageClassification")

	// Check Extractions are present.
	extractions, ok := result["Extractions"].([]any)
	assert.True(t, ok, "LendingResult should have Extractions")
	assert.NotEmpty(t, extractions)
}

// Test 25: GetLendingAnalysisSummary returns populated Summary.
func TestAccuracy_GetLendingAnalysisSummary_PopulatedSummary(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler(t)

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

// Test 26: GetDocumentAnalysis pagination with MaxResults produces NextToken.
func TestAccuracy_GetDocumentAnalysis_Pagination_NextToken(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler(t)

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

	// Get with small MaxResults to trigger NextToken.
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

// Test 27: AnalyzeDocument FORMS has SELECTION_ELEMENT block.
func TestAccuracy_AnalyzeDocument_Forms_SelectionElement(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler(t)
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
