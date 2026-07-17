package macie2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/macie2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMacie2_FindingsFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		setup    func(h *macie2.Handler) string
		pathFn   func(id string) string
		check    func(t *testing.T, body []byte)
		name     string
		method   string
		wantCode int
	}{
		{
			name:   "CreateFindingsFilter returns arn and id",
			method: http.MethodPost,
			pathFn: func(_ string) string { return "/findingsfilters" },
			body: map[string]any{
				"name":            "high-severity",
				"action":          "ARCHIVE",
				"findingCriteria": map[string]any{"criterion": map[string]any{}},
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]string
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.NotEmpty(t, resp["arn"])
				assert.NotEmpty(t, resp["id"])
				assert.Contains(t, resp["arn"], "findings-filter")
			},
		},
		{
			name:   "GetFindingsFilter returns full detail",
			method: http.MethodGet,
			setup: func(h *macie2.Handler) string {
				rec := doRequest(t, h, http.MethodPost, "/findingsfilters", map[string]any{
					"name":        "test-filter",
					"action":      "NOOP",
					"description": "test desc",
					"position":    int32(3),
				})
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				return resp["id"]
			},
			pathFn:   func(id string) string { return "/findingsfilters/" + id },
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, "test-filter", resp["name"])
				assert.Equal(t, "NOOP", resp["action"])
				assert.Equal(t, "test desc", resp["description"])
				assert.InDelta(t, float64(3), resp["position"], 0.001)
			},
		},
		{
			name:     "ListFindingsFilters returns findingsFilterListItems key",
			method:   http.MethodGet,
			pathFn:   func(_ string) string { return "/findingsfilters" },
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Contains(t, resp, "findingsFilterListItems")
			},
		},
		{
			name:   "DeleteFindingsFilter returns 200",
			method: http.MethodDelete,
			setup: func(h *macie2.Handler) string {
				rec := doRequest(t, h, http.MethodPost, "/findingsfilters", map[string]any{
					"name":   "del-filter",
					"action": "ARCHIVE",
				})
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				return resp["id"]
			},
			pathFn:   func(id string) string { return "/findingsfilters/" + id },
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			id := ""
			if tt.setup != nil {
				id = tt.setup(h)
			}

			path := tt.pathFn(id)
			rec := doRequest(t, h, tt.method, path, tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.check != nil {
				tt.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestListFindingsFiltersEmptyNotNull(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/findingsfilters", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	v, ok := resp["findingsFilterListItems"]
	require.True(t, ok, "response must contain findingsFilterListItems key")
	assert.NotNil(t, v, "findingsFilterListItems must be [] not null when empty")

	arr, isArr := v.([]any)
	require.True(t, isArr, "findingsFilterListItems must be an array")
	assert.Empty(t, arr)
}
