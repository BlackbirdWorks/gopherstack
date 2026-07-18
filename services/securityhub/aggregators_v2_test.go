package securityhub_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAggregatorV2(t *testing.T) {
	t.Parallel()

	type step struct {
		body   any
		check  func(t *testing.T, code int, resp map[string]any) string
		name   string
		method string
		path   string
	}

	tests := []struct {
		name  string
		steps []step
	}{
		{
			name: "Create Get List Update Delete AggregatorV2",
			steps: []step{
				{
					name:   "create",
					method: http.MethodPost,
					path:   "/aggregatorv2/create",
					body: map[string]any{
						"RegionLinkingMode": "ALL_REGIONS",
						"Regions":           []any{},
					},
					check: func(t *testing.T, code int, resp map[string]any) string {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						arn, _ := resp["AggregatorV2Arn"].(string)
						assert.NotEmpty(t, arn)
						assert.Equal(t, "ALL_REGIONS", resp["RegionLinkingMode"])

						return arn
					},
				},
				{
					name:   "list",
					method: http.MethodGet,
					path:   "/aggregatorv2/list",
					body:   nil,
					check: func(t *testing.T, code int, resp map[string]any) string {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						aggs, _ := resp["Aggregators"].([]any)
						assert.Len(t, aggs, 1)
						a, _ := aggs[0].(map[string]any)

						return a["AggregatorV2Arn"].(string)
					},
				},
			},
		},
		{
			name: "Get non-existent AggregatorV2 returns 404",
			steps: []step{
				{
					name:   "get missing",
					method: http.MethodGet,
					path:   "/aggregatorv2/get/arn:aws:securityhub:us-east-1:000000000000:aggregator-v2/missing",
					body:   nil,
					check: func(t *testing.T, code int, _ map[string]any) string {
						t.Helper()
						assert.Equal(t, http.StatusNotFound, code)

						return ""
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

func TestHandler_UpdateAggregatorV2(t *testing.T) {
	t.Parallel()

	t.Run("create then update AggregatorV2", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)

		rec := doRequest(t, h, http.MethodPost, "/aggregatorv2/create", map[string]any{
			"RegionLinkingMode": "ALL_REGIONS",
			"Regions":           []any{},
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		arn := resp["AggregatorV2Arn"].(string)

		rec2 := doRequest(t, h, http.MethodPatch, "/aggregatorv2/update/"+arn, map[string]any{
			"RegionLinkingMode": "SPECIFIED_REGIONS",
			"Regions":           []any{"us-west-2"},
		})
		require.Equal(t, http.StatusOK, rec2.Code)
		var resp2 map[string]any
		require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))
		require.Equal(t, "SPECIFIED_REGIONS", resp2["RegionLinkingMode"])
	})

	t.Run("delete AggregatorV2", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)

		rec := doRequest(t, h, http.MethodPost, "/aggregatorv2/create", map[string]any{
			"RegionLinkingMode": "ALL_REGIONS",
			"Regions":           []any{},
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		arn := resp["AggregatorV2Arn"].(string)

		rec2 := doRequest(t, h, http.MethodDelete, "/aggregatorv2/delete/"+arn, nil)
		require.Equal(t, http.StatusOK, rec2.Code)

		rec3 := doRequest(t, h, http.MethodGet, "/aggregatorv2/get/"+arn, nil)
		require.Equal(t, http.StatusNotFound, rec3.Code)
	})
}
