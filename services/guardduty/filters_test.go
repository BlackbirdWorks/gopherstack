package guardduty_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/guardduty"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFilter_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *guardduty.Handler, detectorID string)
		name string
	}{
		{
			name: "create_and_get",
			fn: func(t *testing.T, h *guardduty.Handler, detectorID string) {
				t.Helper()
				rec := doRequest(t, h, http.MethodPost, "/detector/"+detectorID+"/filter", map[string]any{
					"name":        "my-filter",
					"action":      "ARCHIVE",
					"rank":        1,
					"description": "test filter",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var cr map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cr))
				assert.Equal(t, "my-filter", cr["name"])

				rec = doRequest(t, h, http.MethodGet, "/detector/"+detectorID+"/filter/my-filter", nil)
				require.Equal(t, http.StatusOK, rec.Code)

				var gr map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &gr))
				assert.Equal(t, "ARCHIVE", gr["action"])
				assert.InDelta(t, 1.0, gr["rank"], 0.01)
				assert.Equal(t, "test filter", gr["description"])
			},
		},
		{
			name: "update_filter",
			fn: func(t *testing.T, h *guardduty.Handler, detectorID string) {
				t.Helper()
				doRequest(t, h, http.MethodPost, "/detector/"+detectorID+"/filter", map[string]any{
					"name":   "upd-filter",
					"action": "NOOP",
					"rank":   1,
				})

				rec := doRequest(t, h, http.MethodPost, "/detector/"+detectorID+"/filter/upd-filter", map[string]any{
					"action": "ARCHIVE",
					"rank":   2,
				})
				require.Equal(t, http.StatusOK, rec.Code)

				rec = doRequest(t, h, http.MethodGet, "/detector/"+detectorID+"/filter/upd-filter", nil)
				var gr map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &gr))
				assert.Equal(t, "ARCHIVE", gr["action"])
				assert.InDelta(t, 2.0, gr["rank"], 0.01)
			},
		},
		{
			name: "delete_filter",
			fn: func(t *testing.T, h *guardduty.Handler, detectorID string) {
				t.Helper()
				doRequest(t, h, http.MethodPost, "/detector/"+detectorID+"/filter", map[string]any{
					"name": "del-filter", "action": "NOOP", "rank": 1,
				})

				rec := doRequest(t, h, http.MethodDelete, "/detector/"+detectorID+"/filter/del-filter", nil)
				require.Equal(t, http.StatusOK, rec.Code)

				rec = doRequest(t, h, http.MethodGet, "/detector/"+detectorID+"/filter/del-filter", nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "list_filters",
			fn: func(t *testing.T, h *guardduty.Handler, detectorID string) {
				t.Helper()
				for _, name := range []string{"filter-a", "filter-b", "filter-c"} {
					doRequest(t, h, http.MethodPost, "/detector/"+detectorID+"/filter", map[string]any{
						"name": name, "action": "NOOP", "rank": 1,
					})
				}

				rec := doRequest(t, h, http.MethodGet, "/detector/"+detectorID+"/filter", nil)
				require.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				names, ok := resp["filterNames"].([]any)
				require.True(t, ok)
				assert.Len(t, names, 3)
			},
		},
		{
			name: "filter_not_found",
			fn: func(t *testing.T, h *guardduty.Handler, detectorID string) {
				t.Helper()
				rec := doRequest(t, h, http.MethodGet, "/detector/"+detectorID+"/filter/nonexistent", nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
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

func TestListFilters_Empty_State(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	id := createTestDetector(t, h)

	rec := doRequest(t, h, http.MethodGet, "/detector/"+id+"/filter", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	raw, exists := resp["filterNames"]
	require.True(t, exists, "ListFilters must include 'filterNames' key")
	assert.NotNil(t, raw, "filterNames must be [] not null when empty")

	names, ok := raw.([]any)
	require.True(t, ok, "filterNames must be an array, got %T", raw)
	assert.Empty(t, names, "filterNames must be empty []")
}

func TestFilter_Tags_EmptyMap_Not_Null(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	id := createTestDetector(t, h)

	rec := doRequest(t, h, http.MethodPost, "/detector/"+id+"/filter", map[string]any{
		"name":   "batch2-filter",
		"action": "NOOP",
		"rank":   1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, http.MethodGet, "/detector/"+id+"/filter/batch2-filter", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	raw, exists := resp["tags"]
	require.True(t, exists, "GetFilter response must include 'tags' key")
	assert.NotNil(t, raw, "tags must be {} not null when no tags on create")

	tags, ok := raw.(map[string]any)
	require.True(t, ok, "tags must be an object, got %T", raw)
	assert.Empty(t, tags, "tags must be empty map {}")
}
