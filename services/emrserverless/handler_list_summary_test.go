package emrserverless_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/emrserverless"
)

// TestHandler_ListOps_SummaryShape locks that ListApplications/ListJobRuns/
// ListSessions each emit their real types.*Summary shape instead of reusing
// the corresponding Get op's full converter unscoped (gopherstack-tuh5).
// These assertions read the raw JSON response body rather than going
// through an AWS SDK client, since the SDK deserializer silently drops keys
// it does not recognise and cannot observe this class of bug.
func TestHandler_ListOps_SummaryShape(t *testing.T) {
	t.Parallel()

	t.Run("applications", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, http.MethodPost, "/applications", map[string]any{
			"name":         "list-leak-app",
			"type":         "SPARK",
			"releaseLabel": "emr-6.6.0",
			"architecture": "ARM64",
			"maximumCapacity": map[string]any{
				"cpu": "10 vCPU",
			},
			"networkConfiguration": map[string]any{
				"subnetIds": []string{"subnet-1"},
			},
			"autoStartConfiguration": map[string]any{
				"enabled": true,
			},
		})
		require.Equal(t, http.StatusOK, rec.Code)

		item := listSingleEMR(t, h, http.MethodGet, "/applications", "applications")

		for _, k := range []string{
			"id", "arn", "name", "type", "releaseLabel", "state", "createdAt", "updatedAt", "architecture",
		} {
			assert.Contains(t, item, k, "expected real ApplicationSummary member %q", k)
		}
		for _, k := range []string{
			"applicationId", "tags", "maximumCapacity", "networkConfiguration", "autoStartConfiguration",
		} {
			assert.NotContains(t, item, k, "leaked Get-only member %q", k)
		}
	})

	t.Run("jobRuns", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		appID := createApp(t, h, "list-leak-jr-app")

		rec := doRequest(t, h, http.MethodPost, fmt.Sprintf("/applications/%s/jobruns", appID), map[string]any{
			"executionRoleArn":        "arn:aws:iam::000000000000:role/r",
			"tags":                    map[string]string{"k": "v"},
			"executionTimeoutMinutes": 60,
			"jobDriver": map[string]any{
				"sparkSubmit": map[string]any{"entryPoint": "s3://bucket/job.py"},
			},
			"configurationOverrides": map[string]any{
				"monitoringConfiguration": map[string]any{},
			},
			"executionIamPolicy": map[string]any{
				"policy": "{}",
			},
			"retryPolicy": map[string]any{
				"maxAttempts": 2,
			},
		})
		require.Equal(t, http.StatusOK, rec.Code)

		item := listSingleEMR(t, h, http.MethodGet, "/applications/"+appID+"/jobruns", "jobRuns")

		for _, k := range []string{
			"applicationId", "id", "arn", "name", "state", "stateDetails", "mode",
			"executionRole", "createdBy", "createdAt", "updatedAt", "attempt",
		} {
			assert.Contains(t, item, k, "expected real JobRunSummary member %q", k)
		}
		for _, k := range []string{
			"jobRunId", "tags", "executionTimeoutMinutes", "jobDriver",
			"configurationOverrides", "executionIamPolicy", "retryPolicy",
		} {
			assert.NotContains(t, item, k, "leaked Get-only member %q", k)
		}
	})

	t.Run("sessions", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		appID := createStartedApp(t, h)

		rec := doRequest(t, h, http.MethodPost, "/applications/"+appID+"/sessions", map[string]any{
			"clientToken":        "list-leak-session",
			"executionRoleArn":   sessionRoleARN,
			"name":               "list-leak-session",
			"idleTimeoutMinutes": 30,
			"tags":               map[string]string{"purpose": "notebook"},
			"configurationOverrides": map[string]any{
				"monitoringConfiguration": map[string]any{},
			},
		})
		require.Equal(t, http.StatusOK, rec.Code)

		item := listSingleEMR(t, h, http.MethodGet, "/applications/"+appID+"/sessions", "sessions")

		for _, k := range []string{
			"applicationId", "sessionId", "arn", "name", "state", "stateDetails",
			"createdBy", "executionRoleArn", "releaseLabel", "createdAt", "updatedAt",
		} {
			assert.Contains(t, item, k, "expected real SessionSummary member %q", k)
		}
		for _, k := range []string{
			"id", "startedAt", "endedAt", "idleTimeoutMinutes", "configurationOverrides", "tags",
		} {
			assert.NotContains(t, item, k, "leaked Get-only member %q", k)
		}
	})
}

// listSingleEMR issues a GET against a List endpoint expecting exactly one
// item under listKey, and returns it.
func listSingleEMR(
	t *testing.T,
	h *emrserverless.Handler,
	method, path, listKey string,
) map[string]any {
	t.Helper()

	rec := doRequest(t, h, method, path, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	items, ok := resp[listKey].([]any)
	require.True(t, ok, "%s response missing %q list", path, listKey)
	require.Len(t, items, 1)

	item, ok := items[0].(map[string]any)
	require.True(t, ok)

	return item
}
