package textract_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_AnalyzeID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantStatus int
		wantErr    bool
	}{
		{
			name: "success with document pages",
			body: map[string]any{
				"DocumentPages": []any{
					map[string]any{
						"S3Object": map[string]any{
							"Bucket": "my-bucket",
							"Name":   "id-front.jpg",
						},
					},
					map[string]any{
						"S3Object": map[string]any{
							"Bucket": "my-bucket",
							"Name":   "id-back.jpg",
						},
					},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "empty document pages returns error",
			body:       map[string]any{"DocumentPages": []any{}},
			wantStatus: http.StatusBadRequest,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doTextractRequest(t, h, "AnalyzeID", tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if !tt.wantErr {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				docs, ok := resp["IdentityDocuments"].([]any)
				assert.True(t, ok)
				assert.Len(t, docs, 2)
			}
		})
	}
}

// TestHandler_AnalyzeID_IdentityDocumentFields verifies AnalyzeID returns
// IdentityDocumentFields with FIRST_NAME and DATE_OF_BIRTH.
func TestHandler_AnalyzeID_IdentityDocumentFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
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

// TestHandler_AnalyzeID_NormalizedValueDate verifies DATE_OF_BIRTH has a
// NormalizedValue with ValueType=DATE.
func TestHandler_AnalyzeID_NormalizedValueDate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
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

// TestHandler_AnalyzeID_DocumentIndex verifies the first IdentityDocument's
// DocumentIndex is 1.
func TestHandler_AnalyzeID_DocumentIndex(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTextractRequest(t, h, "AnalyzeID", map[string]any{
		"DocumentPages": []any{
			map[string]any{"S3Object": map[string]any{"Bucket": "b", "Name": "id.jpg"}},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var m map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &m))
	docs, ok := m["IdentityDocuments"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, docs)

	doc, ok2 := docs[0].(map[string]any)
	require.True(t, ok2)
	idx, ok3 := doc["DocumentIndex"].(float64)
	assert.True(t, ok3, "IdentityDocument must have DocumentIndex")
	assert.InEpsilon(t, 1.0, idx, 0.001, "first IdentityDocument.DocumentIndex must be 1")
}

// TestHandler_AnalyzeID_ModelVersion verifies the response includes
// AnalyzeIDModelVersion.
func TestHandler_AnalyzeID_ModelVersion(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTextractRequest(t, h, "AnalyzeID", map[string]any{
		"DocumentPages": []any{
			map[string]any{"S3Object": map[string]any{"Bucket": "b", "Name": "id.jpg"}},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var m map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &m))
	ver, ok := m["AnalyzeIDModelVersion"].(string)
	assert.True(t, ok, "AnalyzeID response must have AnalyzeIDModelVersion")
	assert.Equal(t, "1.0", ver)
}

// TestHandler_AnalyzeID_MultiPageDocumentIndex verifies DocumentIndex
// increments across multiple DocumentPages.
func TestHandler_AnalyzeID_MultiPageDocumentIndex(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTextractRequest(t, h, "AnalyzeID", map[string]any{
		"DocumentPages": []any{
			map[string]any{"S3Object": map[string]any{"Bucket": "b", "Name": "front.jpg"}},
			map[string]any{"S3Object": map[string]any{"Bucket": "b", "Name": "back.jpg"}},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var m map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &m))
	docs, ok := m["IdentityDocuments"].([]any)
	require.True(t, ok)
	require.Len(t, docs, 2, "two DocumentPages should yield two IdentityDocuments")

	doc0, ok0 := docs[0].(map[string]any)
	doc1, ok1 := docs[1].(map[string]any)
	require.True(t, ok0 && ok1)

	assert.InEpsilon(t, 1.0, doc0["DocumentIndex"], 0.001, "first DocumentIndex must be 1")
	assert.InEpsilon(t, 2.0, doc1["DocumentIndex"], 0.001, "second DocumentIndex must be 2")
}

// TestHandler_AnalyzeID_DocumentMetadataPagesMatchesInputCount verifies that
// DocumentMetadata.Pages equals the number of DocumentPages provided in the
// request, rather than a fixed default.
func TestHandler_AnalyzeID_DocumentMetadataPagesMatchesInputCount(t *testing.T) {
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

			h := newTestHandler(t)

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

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			meta, _ := resp["DocumentMetadata"].(map[string]any)
			gotPages, _ := meta["Pages"].(float64)
			assert.InDelta(t, float64(tc.pageCount), gotPages, 0,
				"DocumentMetadata.Pages must match the number of DocumentPages in the request")
		})
	}
}

// TestHandler_AnalyzeID_NoFabricatedGeometry asserts that
// IdentityDocumentFields.Type/ValueDetection objects never carry a
// "Geometry" key. The real SDK's types.AnalyzeIDDetections has no Geometry
// member at all (Text, Confidence, NormalizedValue only) -- unlike the
// sibling LendingDetection/ExpenseDetection types, which do carry Geometry.
// A typed aws-sdk-go-v2 client can't observe this: its deserializer simply
// has no case for an unrecognized key and silently ignores it, so this
// raw-body assertion is the only way to prove the field never leaks onto
// the wire.
func TestHandler_AnalyzeID_NoFabricatedGeometry(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
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
	require.True(t, ok)
	require.NotEmpty(t, fields)

	for _, f := range fields {
		fm, fieldOK := f.(map[string]any)
		require.True(t, fieldOK)

		if typeField, typeOK := fm["Type"].(map[string]any); typeOK {
			assert.NotContains(t, typeField, "Geometry",
				"AnalyzeIDDetections.Type has no real Geometry member")
		}

		if valueField, valueOK := fm["ValueDetection"].(map[string]any); valueOK {
			assert.NotContains(t, valueField, "Geometry",
				"AnalyzeIDDetections.ValueDetection has no real Geometry member")
		}
	}
}
