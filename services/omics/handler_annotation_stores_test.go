package omics_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOmics_AnnotationStore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		check    func(t *testing.T, body []byte)
		body     any
		method   string
		path     string
		wantCode int
	}{
		{
			name:     "CreateAnnotationStore returns 201",
			method:   http.MethodPost,
			path:     "/annotationStore",
			body:     map[string]any{"name": "ann-store", "storeFormat": "VCF"},
			wantCode: http.StatusCreated,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Contains(t, resp["arn"], "arn:aws:omics:")
			},
		},
		{
			name:     "CreateAnnotationStore duplicate returns 409",
			method:   http.MethodPost,
			path:     "/annotationStore",
			body:     map[string]any{"name": "dup-store", "storeFormat": "VCF"},
			wantCode: http.StatusCreated,
		},
		{
			name:     "GetAnnotationStore unknown returns 404",
			method:   http.MethodGet,
			path:     "/annotationStore/doesnotexist",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "ListAnnotationStores empty returns 200",
			method:   http.MethodPost,
			path:     "/annotationStores",
			wantCode: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

// TestCreateAnnotationStoreStoresReference verifies that CreateAnnotationStore
// accepts and returns a reference field. Real AWS requires this for VCF/TSV stores.
func TestCreateAnnotationStoreStoresReference(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/annotationStore", map[string]any{
		"name":        "ann-store-ref",
		"storeFormat": "VCF",
		"reference": map[string]any{
			"referenceArn": "arn:aws:omics:us-east-1:000000000000:referencestore/abc/reference/xyz",
		},
		"sseConfig":    map[string]any{"type": "KMS"},
		"storeOptions": map[string]any{"tsvStoreOptions": map[string]any{}},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotNil(t, resp["reference"])
	assert.Equal(t, "CREATING", resp["status"])
}
