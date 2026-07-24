package omics_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/omics"
)

func TestOmics_ReferenceStore(t *testing.T) {
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
			name:     "CreateReferenceStore returns 201 with arn",
			method:   http.MethodPost,
			path:     "/referencestore",
			body:     map[string]any{"name": "test-store"},
			wantCode: http.StatusCreated,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Contains(t, resp["arn"], "arn:aws:omics:")
				assert.Equal(t, "test-store", resp["name"])
			},
		},
		{
			name:     "CreateReferenceStore missing name returns 400",
			method:   http.MethodPost,
			path:     "/referencestore",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "GetReferenceStore unknown id returns 404",
			method:   http.MethodGet,
			path:     "/referencestore/doesnotexist",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "DeleteReferenceStore unknown id returns 404",
			method:   http.MethodDelete,
			path:     "/referencestore/doesnotexist",
			wantCode: http.StatusNotFound,
		},
	}
	// CRUD tests with dynamic IDs done separately
	t.Run("GetReferenceStore returns 200", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, http.MethodPost, "/referencestore", map[string]any{"name": "store1"})
		require.Equal(t, http.StatusCreated, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

		rec2 := doRequest(t, h, http.MethodGet, "/referencestore/"+resp["id"].(string), nil)
		assert.Equal(t, http.StatusOK, rec2.Code)
	})

	t.Run("DeleteReferenceStore returns 200", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, http.MethodPost, "/referencestore", map[string]any{"name": "to-delete"})
		require.Equal(t, http.StatusCreated, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

		rec2 := doRequest(t, h, http.MethodDelete, "/referencestore/"+resp["id"].(string), nil)
		assert.Equal(t, http.StatusOK, rec2.Code)
	})

	// List test done separately to avoid the "path == empty → create extra store" logic
	t.Run("ListReferenceStores returns stores", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doRequest(t, h, http.MethodPost, "/referencestore", map[string]any{"name": "s1"})
		doRequest(t, h, http.MethodPost, "/referencestore", map[string]any{"name": "s2"})

		rec := doRequest(t, h, http.MethodPost, "/referencestores", nil)
		assert.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		stores := resp["referenceStores"].([]any)
		assert.Len(t, stores, 2)
	})

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

func TestOmics_GetReference_Binary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *omics.Handler) string // returns reference path
		name     string
		wantCode int
	}{
		{
			name: "GetReference returns 404 for unknown store",
			setup: func(t *testing.T, _ *omics.Handler) string {
				t.Helper()

				return "/referencestore/unknownstore/reference/unknownref"
			},
			wantCode: http.StatusNotFound,
		},
		{
			name: "GetReference returns 404 for unknown reference",
			setup: func(t *testing.T, h *omics.Handler) string {
				t.Helper()

				rec := doRequest(t, h, http.MethodPost, "/referencestore", map[string]any{"name": "s"})
				require.Equal(t, http.StatusCreated, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				return fmt.Sprintf("/referencestore/%s/reference/doesnotexist", resp["id"])
			},
			wantCode: http.StatusNotFound,
		},
		{
			name: "GetReference returns 200 with binary body for imported reference",
			setup: func(t *testing.T, h *omics.Handler) string {
				t.Helper()
				rec := doRequest(t, h, http.MethodPost, "/referencestore", map[string]any{"name": "s"})
				require.Equal(t, http.StatusCreated, rec.Code)
				var storeResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &storeResp))
				storeID := storeResp["id"].(string)

				rec2 := doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/referencestore/%s/importjob", storeID),
					map[string]any{
						"roleArn": "arn:aws:iam::000000000000:role/test",
						"sources": []map[string]any{{"sourceFile": "s3://b/ref.fa", "name": "ref1"}},
					},
				)
				require.Equal(t, http.StatusCreated, rec2.Code)

				// List references to get the created reference ID.
				rec3 := doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/referencestore/%s/references", storeID),
					nil,
				)
				require.Equal(t, http.StatusOK, rec3.Code)
				var listResp map[string]any
				require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &listResp))
				refs := listResp["references"].([]any)
				require.Len(t, refs, 1)
				refID := refs[0].(map[string]any)["id"].(string)

				return fmt.Sprintf("/referencestore/%s/reference/%s", storeID, refID)
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			path := tc.setup(t, h)
			rec := doRequestRaw(t, h, http.MethodGet, path, "", nil)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

// TestDeleteReferenceStoreReturnsID verifies that DeleteReferenceStore returns
// {id: "..."} in the response body. Real AWS returns the deleted resource's ID.
func TestDeleteReferenceStoreReturnsID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doRequest(t, h, http.MethodPost, "/referencestore", map[string]any{
		"name": "to-delete",
	})
	require.Equal(t, http.StatusCreated, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	storeID := createResp["id"].(string)

	delRec := doRequest(t, h, http.MethodDelete, "/referencestore/"+storeID, nil)
	require.Equal(t, http.StatusOK, delRec.Code)

	var delResp map[string]any
	require.NoError(t, json.Unmarshal(delRec.Body.Bytes(), &delResp))
	assert.Equal(t, storeID, delResp["id"])
}

// TestListReferenceImportJobs_FiltersByStatus verifies ListReferenceImportJobs
// applies its status body filter (real AWS ListReferenceImportJobsInput
// body "filter"). This backend completes reference import jobs synchronously
// to COMPLETED, so filtering by a different status must exclude them.
func TestListReferenceImportJobs_FiltersByStatus(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	storeRec := doRequest(t, h, http.MethodPost, "/referencestore", map[string]any{"name": "store-1"})
	require.Equal(t, http.StatusCreated, storeRec.Code)

	var store map[string]any
	require.NoError(t, json.Unmarshal(storeRec.Body.Bytes(), &store))
	storeID := store["id"].(string)

	jobRec := doRequest(t, h, http.MethodPost, "/referencestore/"+storeID+"/importjob", map[string]any{
		"roleArn": "arn:aws:iam::000000000000:role/role",
		"sources": []map[string]any{{"sourceFile": "s3://bucket/ref.fa", "name": "ref-1"}},
	})
	require.Equal(t, http.StatusCreated, jobRec.Code)

	rec := doRequest(t, h, http.MethodPost, "/referencestore/"+storeID+"/importjobs",
		map[string]any{"filter": map[string]any{"status": "FAILED"}})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	jobs, ok := resp["importJobs"].([]any)
	require.True(t, ok)
	assert.Empty(t, jobs, "no import job has failed")

	rec2 := doRequest(t, h, http.MethodPost, "/referencestore/"+storeID+"/importjobs",
		map[string]any{"filter": map[string]any{"status": "COMPLETED"}})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))
	jobs2, ok := resp2["importJobs"].([]any)
	require.True(t, ok)
	assert.Len(t, jobs2, 1)
}
