package macie2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/macie2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClassificationJobs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *macie2.Handler)
		name string
	}{
		{
			name: "create_describe_list_update",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				// CreateClassificationJob
				rec := doRequest(t, h, http.MethodPost, "/jobs", map[string]any{
					"name":    "test-job",
					"jobType": "ONE_TIME",
					"s3JobDefinition": map[string]any{
						"bucketDefinitions": []any{},
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				var createResp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
				jobID := createResp["jobId"]
				assert.NotEmpty(t, jobID)
				assert.Equal(t, "RUNNING", createResp["jobStatus"])
				// Real CreateClassificationJobOutput always includes jobArn.
				assert.Contains(t, createResp["jobArn"], "arn:aws:macie2:")

				// DescribeClassificationJob
				rec = doRequest(t, h, http.MethodGet, "/jobs/"+jobID, nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var descResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
				assert.Equal(t, jobID, descResp["jobId"])
				assert.Equal(t, "ONE_TIME", descResp["jobType"])
				assert.Equal(t, "test-job", descResp["name"])
				assert.Equal(t, createResp["jobArn"], descResp["jobArn"])

				// ListClassificationJobs
				rec = doRequest(t, h, http.MethodPost, "/jobs/list", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var listResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
				items, _ := listResp["items"].([]any)
				assert.Len(t, items, 1)

				// UpdateClassificationJob
				rec = doRequest(t, h, http.MethodPatch, "/jobs/"+jobID, map[string]any{
					"jobStatus": "CANCELLED",
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				// Verify update
				rec = doRequest(t, h, http.MethodGet, "/jobs/"+jobID, nil)
				var updated map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updated))
				assert.Equal(t, "CANCELLED", updated["jobStatus"])
			},
		},
		{
			name: "describe_missing_returns_404",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				rec := doRequest(t, h, http.MethodGet, "/jobs/nonexistent", nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "scheduled_job_starts_idle",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				rec := doRequest(t, h, http.MethodPost, "/jobs", map[string]any{
					"name":    "sched-job",
					"jobType": "SCHEDULED",
					"scheduleFrequency": map[string]any{
						"dailySchedule": map[string]any{},
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				rec = doRequest(t, h, http.MethodGet, "/jobs/"+resp["jobId"], nil)
				var desc map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &desc))
				assert.Equal(t, "IDLE", desc["jobStatus"])
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t, newTestHandler(t))
		})
	}
}

func TestClassificationConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *macie2.Handler)
		name string
	}{
		{
			name: "get_put_classification_export_config",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				// GetClassificationExportConfiguration — empty by default
				rec := doRequest(t, h, http.MethodGet, "/classification-export-configuration", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var getResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
				cfg, _ := getResp["configuration"].(map[string]any)
				assert.Nil(t, cfg["s3Destination"])

				// PutClassificationExportConfiguration
				rec = doRequest(t, h, http.MethodPut, "/classification-export-configuration", map[string]any{
					"configuration": map[string]any{
						"s3Destination": map[string]any{
							"bucketName": "my-export-bucket",
							"keyPrefix":  "macie/",
							"kmsKeyArn":  "arn:aws:kms:us-east-1:000000000000:key/abc",
						},
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				// Verify
				rec = doRequest(t, h, http.MethodGet, "/classification-export-configuration", nil)
				var updated map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updated))
				updCfg, _ := updated["configuration"].(map[string]any)
				s3Dest, _ := updCfg["s3Destination"].(map[string]any)
				assert.Equal(t, "my-export-bucket", s3Dest["bucketName"])
			},
		},
		{
			name: "list_and_get_and_update_classification_scope",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				// ListClassificationScopes — auto-creates default
				rec := doRequest(t, h, http.MethodGet, "/classification-scopes", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var listResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
				scopes, _ := listResp["classificationScopes"].([]any)
				require.Len(t, scopes, 1)

				scope0, _ := scopes[0].(map[string]any)
				scopeID, _ := scope0["id"].(string)
				require.NotEmpty(t, scopeID)

				// GetClassificationScope
				rec = doRequest(t, h, http.MethodGet, "/classification-scopes/"+scopeID, nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var scopeResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &scopeResp))
				assert.Equal(t, scopeID, scopeResp["id"])

				// UpdateClassificationScope
				rec = doRequest(t, h, http.MethodPatch, "/classification-scopes/"+scopeID, map[string]any{
					"s3": map[string]any{
						"excludes": map[string]any{
							"and": []any{},
						},
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "get_missing_scope_returns_404",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				rec := doRequest(t, h, http.MethodGet, "/classification-scopes/nonexistent", nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t, newTestHandler(t))
		})
	}
}
