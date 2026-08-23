package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateWorkteam(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateWorkteam", map[string]any{
		"WorkteamName": "my-team",
		"Description":  "Test team",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["WorkteamArn"], "my-team")
}

func TestHandler_DescribeWorkteam(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateWorkteam", map[string]any{"WorkteamName": "team-1", "Description": "desc"})

	rec := doSageMakerRequest(t, h, "DescribeWorkteam", map[string]any{"WorkteamName": "team-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	wt := resp["Workteam"].(map[string]any)
	assert.Equal(t, "team-1", wt["WorkteamName"])
}

func TestHandler_DeleteWorkteam(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateWorkteam", map[string]any{"WorkteamName": "team-del"})
	rec := doSageMakerRequest(t, h, "DeleteWorkteam", map[string]any{"WorkteamName": "team-del"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeWorkteam", map[string]any{"WorkteamName": "team-del"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListWorkteams(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateWorkteam", map[string]any{"WorkteamName": "team-a"})
	doSageMakerRequest(t, h, "CreateWorkteam", map[string]any{"WorkteamName": "team-b"})

	rec := doSageMakerRequest(t, h, "ListWorkteams", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items := resp["Workteams"].([]any)
	assert.Len(t, items, 2)
}

func TestHandler_ListWorkteams_Filters(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateWorkteam", map[string]any{"WorkteamName": "alpha-team"})
	doSageMakerRequest(t, h, "CreateWorkteam", map[string]any{"WorkteamName": "beta-team"})

	tests := []struct {
		body      map[string]any
		name      string
		wantNames []string
	}{
		{name: "name contains", body: map[string]any{"NameContains": "alpha"}, wantNames: []string{"alpha-team"}},
		{
			name:      "sort by name ascending",
			body:      map[string]any{"SortBy": "Name", "SortOrder": "Ascending"},
			wantNames: []string{"alpha-team", "beta-team"},
		},
		{
			name:      "sort by name descending",
			body:      map[string]any{"SortBy": "Name", "SortOrder": "Descending"},
			wantNames: []string{"beta-team", "alpha-team"},
		},
		{name: "max results caps page", body: map[string]any{"MaxResults": 1}, wantNames: []string{"alpha-team"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := doSageMakerRequest(t, h, "ListWorkteams", tc.body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			items := resp["Workteams"].([]any)
			require.Len(t, items, len(tc.wantNames))

			for i, want := range tc.wantNames {
				assert.Equal(t, want, items[i].(map[string]any)["WorkteamName"])
			}
		})
	}
}

func TestHandler_CreateWorkteam_NotificationAndWorkerAccessConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateWorkteam", map[string]any{
		"WorkteamName": "team-config",
		"NotificationConfiguration": map[string]any{
			"NotificationTopicArn": "arn:aws:sns:us-east-1:123456789012:topic",
		},
		"WorkerAccessConfiguration": map[string]any{
			"S3Presign": map[string]any{
				"IamPolicyConstraints": map[string]any{
					"SourceIp": "Enabled",
				},
			},
		},
	})

	rec := doSageMakerRequest(t, h, "DescribeWorkteam", map[string]any{"WorkteamName": "team-config"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	wt := resp["Workteam"].(map[string]any)

	nc := wt["NotificationConfiguration"].(map[string]any)
	assert.Equal(t, "arn:aws:sns:us-east-1:123456789012:topic", nc["NotificationTopicArn"])

	wac := wt["WorkerAccessConfiguration"].(map[string]any)
	s3p := wac["S3Presign"].(map[string]any)
	ipc := s3p["IamPolicyConstraints"].(map[string]any)
	assert.Equal(t, "Enabled", ipc["SourceIp"])
}

func TestHandler_UpdateWorkteam_NotificationConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateWorkteam", map[string]any{"WorkteamName": "team-update-config"})

	rec := doSageMakerRequest(t, h, "UpdateWorkteam", map[string]any{
		"WorkteamName": "team-update-config",
		"NotificationConfiguration": map[string]any{
			"NotificationTopicArn": "arn:aws:sns:us-east-1:123456789012:updated",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	wt := resp["Workteam"].(map[string]any)
	nc := wt["NotificationConfiguration"].(map[string]any)
	assert.Equal(t, "arn:aws:sns:us-east-1:123456789012:updated", nc["NotificationTopicArn"])
}

// TestCompilationJob_InputOutputConfigRoundtrip verifies that InputConfig, OutputConfig,
// and StoppingCondition provided at CreateCompilationJob are persisted and returned by
// DescribeCompilationJob. Real AWS stores and returns these fields.
