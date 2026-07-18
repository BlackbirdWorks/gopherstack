package omics_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOmics_SequenceStore(t *testing.T) {
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
			name:     "CreateSequenceStore returns 201",
			method:   http.MethodPost,
			path:     "/sequencestore",
			body:     map[string]any{"name": "seq-store"},
			wantCode: http.StatusCreated,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Contains(t, resp["arn"], "arn:aws:omics:")
				assert.Equal(t, "seq-store", resp["name"])
			},
		},
		{
			name:     "CreateSequenceStore missing name returns 400",
			method:   http.MethodPost,
			path:     "/sequencestore",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "GetSequenceStore unknown returns 404",
			method:   http.MethodGet,
			path:     "/sequencestore/doesnotexist",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "ListSequenceStores empty returns 200",
			method:   http.MethodPost,
			path:     "/sequencestores",
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				stores := resp["sequenceStores"].([]any)
				assert.Empty(t, stores)
			},
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

// TestSequenceStoreHasStatusAndUpdateTime verifies that a sequence store
// exposes status and updateTime fields. AWS always includes these.
func TestSequenceStoreHasStatusAndUpdateTime(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/sequencestore", map[string]any{
		"name": "my-store",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ACTIVE", resp["status"])
	assert.NotEmpty(t, resp["updateTime"])
}

// TestDeleteSequenceStoreReturnsID verifies that DeleteSequenceStore returns
// {id: "..."} in the response body.
func TestDeleteSequenceStoreReturnsID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doRequest(t, h, http.MethodPost, "/sequencestore", map[string]any{
		"name": "to-delete",
	})
	require.Equal(t, http.StatusCreated, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	storeID := createResp["id"].(string)

	delRec := doRequest(t, h, http.MethodDelete, "/sequencestore/"+storeID, nil)
	require.Equal(t, http.StatusOK, delRec.Code)

	var delResp map[string]any
	require.NoError(t, json.Unmarshal(delRec.Body.Bytes(), &delResp))
	assert.Equal(t, storeID, delResp["id"])
}
