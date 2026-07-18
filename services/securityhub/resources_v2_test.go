package securityhub_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResourcesV2(t *testing.T) {
	t.Parallel()

	type step struct {
		body   any
		check  func(t *testing.T, code int, resp map[string]any)
		name   string
		method string
		path   string
	}

	tests := []struct {
		name  string
		steps []step
	}{
		{
			name: "GetResourcesV2 GetResourcesStatisticsV2 GetResourcesTrendsV2",
			steps: []step{
				{
					name:   "get resources empty",
					method: http.MethodPost,
					path:   "/resourcesv2",
					body:   map[string]any{},
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						resources, _ := resp["Resources"].([]any)
						assert.Empty(t, resources)
					},
				},
				{
					name:   "get statistics",
					method: http.MethodPost,
					path:   "/resourcesv2/statistics",
					body:   map[string]any{"GroupByAttributes": []any{"Type"}},
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						assert.NotNil(t, resp["ResourceStatistics"])
					},
				},
				{
					name:   "get trends",
					method: http.MethodPost,
					path:   "/resourcesTrendsv2",
					body: map[string]any{
						"GroupByAttribute": "Type",
						"StartTime":        "2024-01-01T00:00:00Z",
						"EndTime":          "2024-12-31T23:59:59Z",
					},
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						assert.NotNil(t, resp["ResourcesTrends"])
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			for _, s := range tc.steps {
				rec := doRequest(t, h, s.method, s.path, s.body)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				s.check(t, rec.Code, resp)
			}
		})
	}
}
