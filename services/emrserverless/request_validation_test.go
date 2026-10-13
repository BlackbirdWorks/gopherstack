package emrserverless_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRequestValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		method   string
		path     string
		body     map[string]any
		name     string
		wantType string
		wantMsg  string
		wantCode int
	}{
		{
			name: "application name pattern", method: http.MethodPost, path: "/applications",
			body: map[string]any{
				"name": "bad name!", "type": "SPARK", "releaseLabel": "emr-6.10.0", "clientToken": "t1",
			},
			wantCode: http.StatusBadRequest, wantType: "ValidationException", wantMsg: "name must match",
		},
		{
			name: "garbage next token", method: http.MethodGet, path: "/applications?nextToken=garbage!",
			wantCode: http.StatusBadRequest, wantType: "ValidationException", wantMsg: "Invalid nextToken",
		},
		{
			name: "unknown application", method: http.MethodGet, path: "/applications/nope",
			wantCode: http.StatusNotFound, wantType: "ResourceNotFoundException", wantMsg: "application nope not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.method, tt.path, tt.body)
			require.Equal(t, tt.wantCode, rec.Code)

			var out map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Equal(t, tt.wantType, out["code"])
			assert.Contains(t, out["message"], tt.wantMsg)
			assert.NotContains(t, out["message"], "Exception:")
		})
	}
}

func TestJobRunLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	appID := createApp(t, h, "lifecycle-app")
	jobRunID := startJobRun(t, h, appID)

	got := make([]string, 0, 6)

	for range 6 {
		rec := doRequest(t, h, http.MethodGet, "/applications/"+appID+"/jobruns/"+jobRunID, nil)
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			JobRun struct {
				State string `json:"state"`
			} `json:"jobRun"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		got = append(got, out.JobRun.State)
	}

	assert.Equal(t, []string{"SUBMITTED", "PENDING", "SCHEDULED", "RUNNING", "SUCCESS", "SUCCESS"}, got)

	rec := doRequest(t, h, http.MethodDelete, "/applications/"+appID+"/jobruns/"+jobRunID, nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
