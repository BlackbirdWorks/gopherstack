package guardduty_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/guardduty"
)

func TestIPSet_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *guardduty.Handler, detectorID string)
		name string
	}{
		{
			name: "create_and_get",
			fn: func(t *testing.T, h *guardduty.Handler, detectorID string) {
				t.Helper()
				rec := doRequest(t, h, http.MethodPost, "/detector/"+detectorID+"/ipset", map[string]any{
					"name":     "my-ipset",
					"format":   "TXT",
					"location": "s3://bucket/ipset.txt",
					"activate": true,
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var cr map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cr))
				ipSetID := cr["ipSetId"].(string)
				require.NotEmpty(t, ipSetID)

				rec = doRequest(t, h, http.MethodGet, "/detector/"+detectorID+"/ipset/"+ipSetID, nil)
				require.Equal(t, http.StatusOK, rec.Code)

				var gr map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &gr))
				assert.Equal(t, "my-ipset", gr["name"])
				assert.Equal(t, "TXT", gr["format"])
				assert.Equal(t, "ACTIVE", gr["status"])
			},
		},
		{
			name: "update_ipset",
			fn: func(t *testing.T, h *guardduty.Handler, detectorID string) {
				t.Helper()
				rec := doRequest(t, h, http.MethodPost, "/detector/"+detectorID+"/ipset", map[string]any{
					"name": "upd-ipset", "format": "TXT", "location": "s3://b/f.txt",
				})
				var cr map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cr))
				ipSetID := cr["ipSetId"].(string)

				rec = doRequest(t, h, http.MethodPost, "/detector/"+detectorID+"/ipset/"+ipSetID, map[string]any{
					"name":     "updated-ipset",
					"activate": true,
				})
				require.Equal(t, http.StatusOK, rec.Code)

				rec = doRequest(t, h, http.MethodGet, "/detector/"+detectorID+"/ipset/"+ipSetID, nil)
				var gr map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &gr))
				assert.Equal(t, "updated-ipset", gr["name"])
				assert.Equal(t, "ACTIVE", gr["status"])
			},
		},
		{
			name: "delete_ipset",
			fn: func(t *testing.T, h *guardduty.Handler, detectorID string) {
				t.Helper()
				rec := doRequest(t, h, http.MethodPost, "/detector/"+detectorID+"/ipset", map[string]any{
					"name": "del-ipset", "format": "TXT", "location": "s3://b/f.txt",
				})
				var cr map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cr))
				ipSetID := cr["ipSetId"].(string)

				rec = doRequest(t, h, http.MethodDelete, "/detector/"+detectorID+"/ipset/"+ipSetID, nil)
				require.Equal(t, http.StatusOK, rec.Code)

				rec = doRequest(t, h, http.MethodGet, "/detector/"+detectorID+"/ipset/"+ipSetID, nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "list_ipsets",
			fn: func(t *testing.T, h *guardduty.Handler, detectorID string) {
				t.Helper()
				for i := range 3 {
					doRequest(t, h, http.MethodPost, "/detector/"+detectorID+"/ipset", map[string]any{
						"name": "ipset-" + string(rune('a'+i)), "format": "TXT", "location": "s3://b/f.txt",
					})
				}

				rec := doRequest(t, h, http.MethodGet, "/detector/"+detectorID+"/ipset", nil)
				require.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				ids, ok := resp["ipSetIds"].([]any)
				require.True(t, ok)
				assert.Len(t, ids, 3)
			},
		},
		{
			name: "create_inactive_by_default",
			fn: func(t *testing.T, h *guardduty.Handler, detectorID string) {
				t.Helper()
				rec := doRequest(t, h, http.MethodPost, "/detector/"+detectorID+"/ipset", map[string]any{
					"name": "inactive-ipset", "format": "TXT", "location": "s3://b/f.txt",
				})
				var cr map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cr))
				ipSetID := cr["ipSetId"].(string)

				rec = doRequest(t, h, http.MethodGet, "/detector/"+detectorID+"/ipset/"+ipSetID, nil)
				var gr map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &gr))
				assert.Equal(t, "INACTIVE", gr["status"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			detectorID := createTestDetector(t, h)
			tt.fn(t, h, detectorID)
		})
	}
}

func TestListIPSets_Empty_State(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	id := createTestDetector(t, h)

	rec := doRequest(t, h, http.MethodGet, "/detector/"+id+"/ipset", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	raw, exists := resp["ipSetIds"]
	require.True(t, exists, "ListIPSets must include 'ipSetIds' key")
	assert.NotNil(t, raw, "ipSetIds must be [] not null when empty")

	ids, ok := raw.([]any)
	require.True(t, ok, "ipSetIds must be an array, got %T", raw)
	assert.Empty(t, ids, "ipSetIds must be empty []")
}

func TestIPSet_Tags_EmptyMap_Not_Null(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	id := createTestDetector(t, h)

	rec := doRequest(t, h, http.MethodPost, "/detector/"+id+"/ipset", map[string]any{
		"name":     "batch2-ipset",
		"format":   "TXT",
		"location": "s3://bucket/ipset.txt",
		"activate": false,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	ipSetID := createResp["ipSetId"].(string)

	rec = doRequest(t, h, http.MethodGet, "/detector/"+id+"/ipset/"+ipSetID, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	raw, exists := resp["tags"]
	require.True(t, exists, "GetIPSet response must include 'tags' key")
	assert.NotNil(t, raw, "tags must be {} not null when no tags on create")

	tags, ok := raw.(map[string]any)
	require.True(t, ok, "tags must be an object, got %T", raw)
	assert.Empty(t, tags, "tags must be empty map {}")
}
