package omics_test

import (
	"encoding/json"
	"maps"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func decodeBody(t *testing.T, body []byte) map[string]any {
	t.Helper()

	var m map[string]any
	require.NoError(t, json.Unmarshal(body, &m))

	return m
}

func TestStartRunConfigurationAndDuplicate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCfg  string
		wantOut  string
		wantCode int
	}{
		{
			name:     "known configuration",
			body:     map[string]any{"configurationName": "cfg1"},
			wantCode: http.StatusCreated,
			wantCfg:  "cfg1",
		},
		{
			name:     "unknown configuration",
			body:     map[string]any{"configurationName": "missing"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "duplicate inherits settings",
			body:     map[string]any{"runId": "SRC"},
			wantCode: http.StatusCreated,
			wantCfg:  "cfg1",
			wantOut:  "s3://bucket/out",
		},
		{
			name:     "duplicate unknown run",
			body:     map[string]any{"runId": "nope"},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doRequest(t, h, http.MethodPost, "/configuration", map[string]any{
				"name":              "cfg1",
				"runConfigurations": map[string]any{"vpcConfig": map[string]any{"subnetIds": []string{"s-1"}}},
			})
			require.Equal(t, http.StatusCreated, rec.Code)

			rec = doRequest(t, h, http.MethodPost, "/run", map[string]any{
				"workflowId": "wf1", "roleArn": testRunBatchRoleArn, "configurationName": "cfg1",
				"outputUri": "s3://bucket/out", "parameters": map[string]any{"k": "v"},
			})
			require.Equal(t, http.StatusCreated, rec.Code)
			srcID := decodeBody(t, rec.Body.Bytes())["id"].(string)

			body := map[string]any{"workflowId": "wf1", "roleArn": testRunBatchRoleArn}
			maps.Copy(body, tt.body)

			if body["runId"] == "SRC" {
				body["runId"] = srcID
				delete(body, "workflowId")
			}

			rec = doRequest(t, h, http.MethodPost, "/run", body)
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode != http.StatusCreated {
				return
			}

			runID := decodeBody(t, rec.Body.Bytes())["id"].(string)
			got := decodeBody(t, doRequest(t, h, http.MethodGet, "/run/"+runID, nil).Body.Bytes())
			cfg, _ := got["configuration"].(map[string]any)
			assert.Equal(t, tt.wantCfg, cfg["name"])
			out, _ := got["runOutputUri"].(string)
			assert.Equal(t, tt.wantOut, out)
			assert.Equal(t, "wf1", got["workflowId"])
		})
	}
}

func TestRunBatchSubmissionsAndFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		query      string
		wantStatus []string
	}{
		{name: "all", query: "", wantStatus: []string{"SUCCESS", "FAILED"}},
		{name: "failed", query: "?submissionStatus=FAILED", wantStatus: []string{"FAILED"}},
		{name: "success", query: "?submissionStatus=SUCCESS", wantStatus: []string{"SUCCESS"}},
		{name: "cancel success", query: "?submissionStatus=CANCEL_SUCCESS", wantStatus: []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doRequest(t, h, http.MethodPost, "/runBatch", map[string]any{
				"requestId": "r1",
				"defaultRunSetting": map[string]any{
					"roleArn": testRunBatchRoleArn, "workflowId": "wf1", "runGroupId": "rg1",
				},
				"batchRunSettings": map[string]any{"inlineSettings": []map[string]any{
					{"runSettingId": "a"},
					{"runSettingId": "a"},
				}},
			})
			require.Equal(t, http.StatusCreated, rec.Code)
			batchID := decodeBody(t, rec.Body.Bytes())["id"].(string)

			rec = doRequest(t, h, http.MethodGet, "/runBatch/"+batchID+"/run"+tt.query, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			runs, _ := decodeBody(t, rec.Body.Bytes())["runs"].([]any)
			got := make([]string, 0, len(runs))

			for _, r := range runs {
				m := r.(map[string]any)
				got = append(got, m["submissionStatus"].(string))

				if m["submissionStatus"] == "FAILED" {
					assert.NotEmpty(t, m["submissionFailureMessage"])
					assert.Equal(t, "a", m["runSettingId"])
				}
			}

			assert.ElementsMatch(t, tt.wantStatus, got)
		})
	}
}

func TestListBatchRunGroupFilterAndOutputOwner(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	start := func(id, group, owner string) int {
		rec := doRequest(t, h, http.MethodPost, "/runBatch", map[string]any{
			"requestId": id,
			"defaultRunSetting": map[string]any{
				"roleArn": testRunBatchRoleArn, "workflowId": "wf1", "runGroupId": group,
				"outputUri": "s3://bucket/out", "outputBucketOwnerId": owner,
			},
			"batchRunSettings": map[string]any{"inlineSettings": []map[string]any{{"runSettingId": "a"}}},
		})

		return rec.Code
	}

	require.Equal(t, http.StatusCreated, start("b1", "rg1", ""))
	require.Equal(t, http.StatusCreated, start("b2", "rg2", "000000000000"))
	require.Equal(t, http.StatusBadRequest, start("b3", "rg3", "111111111111"))

	rec := doRequest(t, h, http.MethodGet, "/runBatch?runGroupId=rg2", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	items, _ := decodeBody(t, rec.Body.Bytes())["items"].([]any)
	assert.Len(t, items, 1)
}
