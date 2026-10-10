package emr_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEMR_RequestValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body    func(cluster string) map[string]any
		name    string
		action  string
		wantMsg string
	}{
		{
			name:    "bad release label",
			action:  "RunJobFlow",
			body:    func(string) map[string]any { return map[string]any{"Name": "c", "ReleaseLabel": "bogus"} },
			wantMsg: `invalid ReleaseLabel "bogus"`,
		},
		{
			name:   "bad instance type",
			action: "RunJobFlow",
			body: func(string) map[string]any {
				return map[string]any{"Name": "c", "Instances": map[string]any{
					"InstanceGroups": []any{map[string]any{
						"InstanceRole": "MASTER", "InstanceType": "bogus", "InstanceCount": 1,
					}},
				}}
			},
			wantMsg: "invalid instance type",
		},
		{
			name:   "bad action on failure",
			action: "AddJobFlowSteps",
			body: func(c string) map[string]any {
				return map[string]any{"JobFlowId": c, "Steps": []any{map[string]any{
					"Name": "s", "ActionOnFailure": "BOGUS",
					"HadoopJarStep": map[string]any{"Jar": "x.jar"},
				}}}
			},
			wantMsg: "invalid ActionOnFailure",
		},
		{
			name:    "bad marker",
			action:  "ListClusters",
			body:    func(string) map[string]any { return map[string]any{"Marker": "garbage!"} },
			wantMsg: "Invalid marker",
		},
		{
			name:    "unknown cluster",
			action:  "DescribeCluster",
			body:    func(string) map[string]any { return map[string]any{"ClusterId": "j-NOPE"} },
			wantMsg: "Cluster id 'j-NOPE' is not valid.",
		},
		{
			name:    "unknown cluster steps",
			action:  "ListSteps",
			body:    func(string) map[string]any { return map[string]any{"ClusterId": "j-NOPE"} },
			wantMsg: "Cluster id 'j-NOPE' is not valid.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			created := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "base"})
			require.Equal(t, http.StatusOK, created.Code)

			var out struct {
				JobFlowID string `json:"JobFlowId"`
			}
			require.NoError(t, json.Unmarshal(created.Body.Bytes(), &out))

			rec := doEMRRequest(t, h, tt.action, tt.body(out.JobFlowID))
			require.Equal(t, http.StatusBadRequest, rec.Code)

			var errOut map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errOut))
			assert.Equal(t, "InvalidRequestException", errOut["__type"])
			assert.Contains(t, errOut["message"], tt.wantMsg)
			assert.NotContains(t, errOut["message"], "Exception:")
		})
	}
}
