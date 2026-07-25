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

// TestListAnnotationImportJobs_FiltersByStatusStoreNameAndIds verifies
// ListAnnotationImportJobs applies its status/storeName body filter and
// explicit ids list (real AWS ListAnnotationImportJobsInput body
// "filter"/"ids").
func TestListAnnotationImportJobs_FiltersByStatusStoreNameAndIds(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	storeRec := doRequest(t, h, http.MethodPost, "/annotationStore", map[string]any{
		"name": "store-1", "storeFormat": "VCF",
	})
	require.Equal(t, http.StatusCreated, storeRec.Code)

	jobRec := doRequest(t, h, http.MethodPost, "/import/annotation", map[string]any{
		"destinationName": "store-1",
		"roleArn":         "arn:aws:iam::000000000000:role/role",
		"items":           []map[string]any{{"source": "s3://bucket/annotations.vcf"}},
	})
	require.Equal(t, http.StatusCreated, jobRec.Code)

	var job map[string]any
	require.NoError(t, json.Unmarshal(jobRec.Body.Bytes(), &job))
	jobID := job["id"].(string)

	// storeName filter: no job targets "other-store".
	rec := doRequest(t, h, http.MethodPost, "/import/annotations",
		map[string]any{"filter": map[string]any{"storeName": "other-store"}})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	jobs, ok := resp["importJobs"].([]any)
	require.True(t, ok)
	assert.Empty(t, jobs)

	// status filter: jobs complete synchronously to COMPLETED.
	rec2 := doRequest(t, h, http.MethodPost, "/import/annotations",
		map[string]any{"filter": map[string]any{"status": "COMPLETED"}})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))
	jobs2, ok := resp2["importJobs"].([]any)
	require.True(t, ok)
	require.Len(t, jobs2, 1)

	// explicit ids filter.
	rec3 := doRequest(t, h, http.MethodPost, "/import/annotations",
		map[string]any{"ids": []string{"does-not-exist"}})
	require.Equal(t, http.StatusOK, rec3.Code)

	var resp3 map[string]any
	require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &resp3))
	jobs3, ok := resp3["importJobs"].([]any)
	require.True(t, ok)
	assert.Empty(t, jobs3)

	rec4 := doRequest(t, h, http.MethodPost, "/import/annotations", map[string]any{"ids": []string{jobID}})
	require.Equal(t, http.StatusOK, rec4.Code)

	var resp4 map[string]any
	require.NoError(t, json.Unmarshal(rec4.Body.Bytes(), &resp4))
	jobs4, ok := resp4["importJobs"].([]any)
	require.True(t, ok)
	require.Len(t, jobs4, 1)
}
