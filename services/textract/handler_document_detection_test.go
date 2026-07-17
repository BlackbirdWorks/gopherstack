package textract_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_DetectDocumentText(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			body: map[string]any{
				"Document": map[string]any{
					"S3Object": map[string]any{
						"Bucket": "my-bucket",
						"Name":   "page.png",
					},
				},
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doTextractRequest(t, h, "DetectDocumentText", tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_StartAndGetDocumentTextDetection(t *testing.T) {
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
						"Name":   "page.jpg",
					},
				},
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doTextractRequest(t, h, "StartDocumentTextDetection", tt.startBody)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var startResp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
			jobID := startResp["JobId"]
			assert.NotEmpty(t, jobID)

			getBody := map[string]string{"JobId": jobID}
			getRec := doTextractRequest(t, h, "GetDocumentTextDetection", getBody)
			assert.Equal(t, http.StatusOK, getRec.Code)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
			assert.Equal(t, "SUCCEEDED", getResp["JobStatus"])
		})
	}
}

func TestHandler_StartDocumentTextDetection_MissingBucket(t *testing.T) {
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

	rec := doTextractRequest(t, h, "StartDocumentTextDetection", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_GetDocumentTextDetection_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body := map[string]string{"JobId": "nonexistent-job"}

	rec := doTextractRequest(t, h, "GetDocumentTextDetection", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_GetDocumentTextDetection_MissingJobId(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body := map[string]string{}

	rec := doTextractRequest(t, h, "GetDocumentTextDetection", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestHandler_GetDocumentTextDetection_RejectsAnalysisJobID verifies that
// GetDocumentTextDetection returns InvalidJobIdException (400) when given a
// job ID that belongs to a StartDocumentAnalysis job. This is the mirror case
// of GetDocumentAnalysis rejecting a TextDetection job ID; both directions
// must be enforced.
func TestHandler_GetDocumentTextDetection_RejectsAnalysisJobID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	daJobID := startDocumentAnalysisJob(t, h)

	rec := doTextractRequest(t, h, "GetDocumentTextDetection", map[string]any{"JobId": daJobID})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "InvalidJobIdException", resp["__type"],
		"cross-type access must return InvalidJobIdException")
}

// TestHandler_DetectDocumentText_BlockStructure verifies DetectDocumentText
// returns PAGE/LINE/WORD blocks with Relationships.
func TestHandler_DetectDocumentText_BlockStructure(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
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

// TestHandler_DetectDocumentText_ModelVersion verifies DetectDocumentText
// returns DetectDocumentTextModelVersion.
func TestHandler_DetectDocumentText_ModelVersion(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
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

// TestHandler_GetDocumentTextDetection_Pagination verifies
// StartDocumentTextDetection -> GetDocumentTextDetection pagination via
// MaxResults/NextToken.
func TestHandler_GetDocumentTextDetection_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

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

// TestHandler_DetectDocumentText_BlockIDsAreUUIDs verifies every block Id is
// UUID-shaped.
func TestHandler_DetectDocumentText_BlockIDsAreUUIDs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTextractRequest(t, h, "DetectDocumentText", map[string]any{
		"Document": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "doc.pdf"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var m map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &m))
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

// TestHandler_DetectDocumentText_ConfidenceInRange verifies every block
// Confidence is within [0,100].
func TestHandler_DetectDocumentText_ConfidenceInRange(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTextractRequest(t, h, "DetectDocumentText", map[string]any{
		"Document": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "doc.pdf"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var m map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &m))
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

// TestHandler_DetectDocumentText_WordHasTextType verifies every WORD block has
// a non-empty TextType of PRINTED or HANDWRITING.
func TestHandler_DetectDocumentText_WordHasTextType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTextractRequest(t, h, "DetectDocumentText", map[string]any{
		"Document": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "doc.pdf"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var m map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &m))
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

	assert.Positive(t, wordCount, "expected at least one WORD block")
}

// TestHandler_DetectDocumentText_PageIsSet verifies every block's Page field
// is set and >= 1.
func TestHandler_DetectDocumentText_PageIsSet(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTextractRequest(t, h, "DetectDocumentText", map[string]any{
		"Document": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "doc.pdf"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var m map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &m))
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

// TestHandler_DetectDocumentText_GeometryBoundingBoxValid verifies every
// block's Geometry.BoundingBox values are within [0,1] and Polygon has at
// least 4 points.
func TestHandler_DetectDocumentText_GeometryBoundingBoxValid(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTextractRequest(t, h, "DetectDocumentText", map[string]any{
		"Document": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "doc.pdf"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var m map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &m))
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

// TestHandler_DetectDocumentText_DocumentMetadataPages verifies
// DetectDocumentText's DocumentMetadata.Pages field is present and >= 1.
func TestHandler_DetectDocumentText_DocumentMetadataPages(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTextractRequest(t, h, "DetectDocumentText", map[string]any{
		"Document": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "doc.pdf"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var m map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &m))
	meta, ok := m["DocumentMetadata"].(map[string]any)
	require.True(t, ok, "response must have DocumentMetadata")
	pages, ok2 := meta["Pages"].(float64)
	assert.True(t, ok2, "DocumentMetadata must have Pages field")
	assert.GreaterOrEqual(t, int(pages), 1, "Pages must be >= 1")
}

// TestHandler_DetectDocumentText_BytesInput verifies DetectDocumentText
// accepts an inline Document.Bytes payload (not only S3Object).
func TestHandler_DetectDocumentText_BytesInput(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTextractRequest(t, h, "DetectDocumentText", map[string]any{
		"Document": map[string]any{
			"Bytes": []byte("fake-pdf-content"),
		},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var m map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &m))
	blocks, ok := m["Blocks"].([]any)
	assert.True(t, ok && len(blocks) > 0, "DetectDocumentText with Bytes must return Blocks")
}

// TestHandler_GetDocumentTextDetection_ModelVersion verifies
// GetDocumentTextDetection includes DetectDocumentTextModelVersion.
func TestHandler_GetDocumentTextDetection_ModelVersion(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
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

	var m map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &m))
	ver, ok := m["DetectDocumentTextModelVersion"].(string)
	assert.True(t, ok, "GetDocumentTextDetection must have DetectDocumentTextModelVersion")
	assert.Equal(t, "1.0", ver)
}

// TestHandler_StartDocumentTextDetection_ClientRequestTokenIdempotency verifies
// that replaying the same ClientRequestToken returns the same JobId.
func TestHandler_StartDocumentTextDetection_ClientRequestTokenIdempotency(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
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
