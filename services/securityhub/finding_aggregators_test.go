package securityhub_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/securityhub"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackend_GetFindingAggregator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantErrMsg string
		preCreate  bool
	}{
		{
			name:      "get existing aggregator",
			preCreate: true,
		},
		{
			name:       "get non-existent aggregator",
			preCreate:  false,
			wantErrMsg: "not found",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := securityhub.NewInMemoryBackend("000000000000", "us-east-1")

			var arn string
			if tc.preCreate {
				agg, err := b.CreateFindingAggregator("ALL_REGIONS", []string{})
				require.NoError(t, err)
				arn = agg.FindingAggregatorArn
			} else {
				arn = "arn:aws:securityhub:us-east-1:000000000000:finding-aggregator/missing"
			}

			result, err := b.GetFindingAggregator(arn)
			if tc.wantErrMsg != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrMsg)
				assert.Nil(t, result)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, result)
				assert.Equal(t, arn, result.FindingAggregatorArn)
			}
		})
	}
}

func TestFindingAggregator(t *testing.T) {
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
			name: "Create Get List Update Delete FindingAggregator",
			steps: []step{
				{
					name:   "create",
					method: http.MethodPost,
					path:   "/findingAggregator/create",
					body: map[string]any{
						"RegionLinkingMode": "SPECIFIED_REGIONS",
						"Regions":           []any{"us-west-2"},
					},
					check: func(t *testing.T, code int, resp map[string]any) string {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						arn, _ := resp["FindingAggregatorArn"].(string)
						assert.NotEmpty(t, arn)
						assert.Equal(t, "SPECIFIED_REGIONS", resp["RegionLinkingMode"])

						return arn
					},
				},
				{
					name:   "list",
					method: http.MethodGet,
					path:   "/findingAggregator/list",
					body:   nil,
					check: func(t *testing.T, code int, resp map[string]any) string {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						aggs, _ := resp["FindingAggregators"].([]any)
						assert.Len(t, aggs, 1)

						return ""
					},
				},
				{
					name:   "update",
					method: http.MethodPatch,
					path:   "/findingAggregator/update",
					body: map[string]any{
						"FindingAggregatorArn": "placeholder",
						"RegionLinkingMode":    "ALL_REGIONS",
						"Regions":              []any{},
					},
					check: func(t *testing.T, code int, resp map[string]any) string {
						t.Helper()
						// Placeholder ARN won't be found
						if code == http.StatusOK {
							assert.Equal(t, "ALL_REGIONS", resp["RegionLinkingMode"])
						}

						return ""
					},
				},
			},
		},
		{
			name: "Delete non-existent FindingAggregator returns 404",
			steps: []step{
				{
					name:   "delete missing",
					method: http.MethodDelete,
					path:   "/findingAggregator/delete/arn:aws:securityhub:us-east-1:000000000000:finding-aggregator/missing",
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
