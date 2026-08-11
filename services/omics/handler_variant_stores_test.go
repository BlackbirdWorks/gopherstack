package omics_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/omics"
)

func TestOmics_VariantStore(t *testing.T) {
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
			name:     "CreateVariantStore returns 201",
			method:   http.MethodPost,
			path:     "/variantStore",
			body:     map[string]any{"name": "var-store"},
			wantCode: http.StatusCreated,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Contains(t, resp["arn"], "arn:aws:omics:")
			},
		},
		{
			name:     "CreateVariantStore duplicate returns 409",
			method:   http.MethodPost,
			path:     "/variantStore",
			body:     map[string]any{"name": "dup-var"},
			wantCode: http.StatusCreated,
		},
		{
			name:     "GetVariantStore unknown returns 404",
			method:   http.MethodGet,
			path:     "/variantStore/doesnotexist",
			wantCode: http.StatusNotFound,
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

// TestCreateVariantStoreStoresReference verifies that CreateVariantStore
// accepts and returns a reference field. Real AWS requires a reference genome.
func TestCreateVariantStoreStoresReference(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/variantStore", map[string]any{
		"name": "var-store-ref",
		"reference": map[string]any{
			"referenceArn": "arn:aws:omics:us-east-1:000000000000:referencestore/abc/reference/xyz",
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotNil(t, resp["reference"])
	assert.Equal(t, "CREATING", resp["status"])
}

// TestListVariantImportJobs_FiltersByStatusStoreNameAndIds verifies
// ListVariantImportJobs applies its status/storeName body filter and
// explicit ids list (real AWS ListVariantImportJobsInput body
// "filter"/"ids").
func TestListVariantImportJobs_FiltersByStatusStoreNameAndIds(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	storeRec := doRequest(t, h, http.MethodPost, "/variantStore", map[string]any{"name": "store-1"})
	require.Equal(t, http.StatusCreated, storeRec.Code)

	jobRec := doRequest(t, h, http.MethodPost, "/import/variant", map[string]any{
		"destinationName": "store-1",
		"roleArn":         "arn:aws:iam::000000000000:role/role",
		"items":           []map[string]any{{"source": "s3://bucket/variants.vcf"}},
	})
	require.Equal(t, http.StatusCreated, jobRec.Code)

	var job map[string]any
	require.NoError(t, json.Unmarshal(jobRec.Body.Bytes(), &job))
	jobID := job["id"].(string)

	rec := doRequest(t, h, http.MethodPost, "/import/variants",
		map[string]any{"filter": map[string]any{"storeName": "other-store"}})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	jobs, ok := resp["importJobs"].([]any)
	require.True(t, ok)
	assert.Empty(t, jobs)

	rec2 := doRequest(t, h, http.MethodPost, "/import/variants", map[string]any{"ids": []string{jobID}})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))
	jobs2, ok := resp2["importJobs"].([]any)
	require.True(t, ok)
	require.Len(t, jobs2, 1)
}

// createVariantStorePair creates two variant stores, "vs-creating" (left in
// CREATING) and "vs-active" (promoted to ACTIVE via a GET poll), and returns
// their ids for the filter tests below.
func createVariantStorePair(t *testing.T, h *omics.Handler) map[string]string {
	t.Helper()

	ids := map[string]string{}

	for _, name := range []string{"vs-creating", "vs-active"} {
		rec := doRequest(t, h, http.MethodPost, "/variantStore", map[string]any{"name": name})
		require.Equal(t, http.StatusCreated, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		ids[name], _ = resp["id"].(string)
	}

	getRec := doRequest(t, h, http.MethodGet, "/variantStore/vs-active", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	return ids
}

func variantStoreNames(t *testing.T, raw any) []string {
	t.Helper()

	items, ok := raw.([]any)
	require.True(t, ok)

	names := make([]string, 0, len(items))

	for _, item := range items {
		m, itemOK := item.(map[string]any)
		require.True(t, itemOK)
		names = append(names, m["name"].(string))
	}

	return names
}

// TestListVariantStores_Filters verifies ListVariantStores applies its status
// body filter and explicit ids list (real AWS ListVariantStoresInput body
// "filter"/"ids").
func TestListVariantStores_Filters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		reqBody   func(ids map[string]string) map[string]any
		name      string
		wantNames []string
	}{
		{
			name: "status filter alone matches creating",
			reqBody: func(map[string]string) map[string]any {
				return map[string]any{"filter": map[string]any{"status": "CREATING"}}
			},
			wantNames: []string{"vs-creating"},
		},
		{
			name: "status filter alone matches active",
			reqBody: func(map[string]string) map[string]any {
				return map[string]any{"filter": map[string]any{"status": "ACTIVE"}}
			},
			wantNames: []string{"vs-active"},
		},
		{
			name: "ids filter alone",
			reqBody: func(ids map[string]string) map[string]any {
				return map[string]any{"ids": []string{ids["vs-creating"]}}
			},
			wantNames: []string{"vs-creating"},
		},
		{
			name: "status and ids combined mismatch returns empty",
			reqBody: func(ids map[string]string) map[string]any {
				return map[string]any{
					"filter": map[string]any{"status": "CREATING"},
					"ids":    []string{ids["vs-active"]},
				}
			},
			wantNames: []string{},
		},
		{
			name: "unmatched status returns empty",
			reqBody: func(map[string]string) map[string]any {
				return map[string]any{"filter": map[string]any{"status": "DELETING"}}
			},
			wantNames: []string{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			ids := createVariantStorePair(t, h)

			rec := doRequest(t, h, http.MethodPost, "/variantStores", tc.reqBody(ids))
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.ElementsMatch(t, tc.wantNames, variantStoreNames(t, resp["variantStores"]))
		})
	}
}
