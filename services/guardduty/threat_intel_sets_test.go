package guardduty_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/guardduty"
)

func TestThreatIntelSet_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *guardduty.Handler, detectorID string)
		name string
	}{
		{
			name: "create_and_get",
			fn: func(t *testing.T, h *guardduty.Handler, detectorID string) {
				t.Helper()
				rec := doRequest(t, h, http.MethodPost, "/detector/"+detectorID+"/threatintelset", map[string]any{
					"name":     "my-threatset",
					"format":   "TXT",
					"location": "s3://bucket/threats.txt",
					"activate": true,
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var cr map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cr))
				setID := cr["threatIntelSetId"].(string)
				require.NotEmpty(t, setID)

				rec = doRequest(t, h, http.MethodGet, "/detector/"+detectorID+"/threatintelset/"+setID, nil)
				require.Equal(t, http.StatusOK, rec.Code)

				var gr map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &gr))
				assert.Equal(t, "my-threatset", gr["name"])
				assert.Equal(t, "ACTIVE", gr["status"])
			},
		},
		{
			name: "update_and_deactivate",
			fn: func(t *testing.T, h *guardduty.Handler, detectorID string) {
				t.Helper()
				rec := doRequest(t, h, http.MethodPost, "/detector/"+detectorID+"/threatintelset", map[string]any{
					"name": "upd-threat", "format": "TXT", "location": "s3://b/t.txt", "activate": true,
				})
				var cr map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cr))
				setID := cr["threatIntelSetId"].(string)

				activate := false
				rec = doRequest(t, h, http.MethodPost, "/detector/"+detectorID+"/threatintelset/"+setID, map[string]any{
					"activate": activate,
				})
				require.Equal(t, http.StatusOK, rec.Code)

				rec = doRequest(t, h, http.MethodGet, "/detector/"+detectorID+"/threatintelset/"+setID, nil)
				var gr map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &gr))
				assert.Equal(t, "INACTIVE", gr["status"])
			},
		},
		{
			name: "delete_threatintelset",
			fn: func(t *testing.T, h *guardduty.Handler, detectorID string) {
				t.Helper()
				rec := doRequest(t, h, http.MethodPost, "/detector/"+detectorID+"/threatintelset", map[string]any{
					"name": "del-threat", "format": "TXT", "location": "s3://b/t.txt",
				})
				var cr map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cr))
				setID := cr["threatIntelSetId"].(string)

				rec = doRequest(t, h, http.MethodDelete, "/detector/"+detectorID+"/threatintelset/"+setID, nil)
				require.Equal(t, http.StatusOK, rec.Code)

				rec = doRequest(t, h, http.MethodGet, "/detector/"+detectorID+"/threatintelset/"+setID, nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "list_threatintelsets",
			fn: func(t *testing.T, h *guardduty.Handler, detectorID string) {
				t.Helper()
				for i := range 2 {
					doRequest(t, h, http.MethodPost, "/detector/"+detectorID+"/threatintelset", map[string]any{
						"name": "threat-" + string(rune('a'+i)), "format": "TXT", "location": "s3://b/t.txt",
					})
				}

				rec := doRequest(t, h, http.MethodGet, "/detector/"+detectorID+"/threatintelset", nil)
				require.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				ids, ok := resp["threatIntelSetIds"].([]any)
				require.True(t, ok)
				assert.Len(t, ids, 2)
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

func TestListThreatIntelSets_Empty_State(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	id := createTestDetector(t, h)

	rec := doRequest(t, h, http.MethodGet, "/detector/"+id+"/threatintelset", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	raw, exists := resp["threatIntelSetIds"]
	require.True(t, exists, "ListThreatIntelSets must include 'threatIntelSetIds' key")
	assert.NotNil(t, raw, "threatIntelSetIds must be [] not null when empty")

	ids, ok := raw.([]any)
	require.True(t, ok, "threatIntelSetIds must be an array, got %T", raw)
	assert.Empty(t, ids, "threatIntelSetIds must be empty []")
}

func TestThreatIntelSet_Tags_EmptyMap_Not_Null(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	id := createTestDetector(t, h)

	rec := doRequest(t, h, http.MethodPost, "/detector/"+id+"/threatintelset", map[string]any{
		"name":     "batch2-tis",
		"format":   "TXT",
		"location": "s3://bucket/tis.txt",
		"activate": false,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	setID := createResp["threatIntelSetId"].(string)

	rec = doRequest(t, h, http.MethodGet, "/detector/"+id+"/threatintelset/"+setID, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	raw, exists := resp["tags"]
	require.True(t, exists, "GetThreatIntelSet response must include 'tags' key")
	assert.NotNil(t, raw, "tags must be {} not null when no tags on create")

	tags, ok := raw.(map[string]any)
	require.True(t, ok, "tags must be an object, got %T", raw)
	assert.Empty(t, tags, "tags must be empty map {}")
}
