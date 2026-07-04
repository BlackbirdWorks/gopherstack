package quicksight_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- Automation job lifecycle: start settles to SUCCEEDED on first describe ----

func TestQuickSight_AutomationJob_Lifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	startPath := accountPath("/automation-groups/group1/automations/automation1/jobs")
	startRec := doRequest(t, h, http.MethodPost, startPath, map[string]any{
		"InputPayload": `{"foo":"bar"}`,
	})
	require.Equal(t, http.StatusOK, startRec.Code)
	startBody := parseBody(t, startRec)
	jobID, ok := startBody["JobId"].(string)
	require.True(t, ok)
	require.NotEmpty(t, jobID)
	assert.Contains(t, startBody["Arn"], "automation-group/group1")

	describePath := accountPath("/automation-groups/group1/automations/automation1/jobs/" + jobID)

	// First describe settles the job to SUCCEEDED.
	describeRec := doRequest(t, h, http.MethodGet, describePath, nil)
	require.Equal(t, http.StatusOK, describeRec.Code)
	describeBody := parseBody(t, describeRec)
	assert.Equal(t, "SUCCEEDED", describeBody["JobStatus"])
	assert.NotContains(t, describeBody, "InputPayload")
	assert.NotContains(t, describeBody, "OutputPayload")

	// Second describe stays SUCCEEDED (deterministic, not re-settled/flapping).
	describeRec2 := doRequest(t, h, http.MethodGet, describePath, nil)
	require.Equal(t, http.StatusOK, describeRec2.Code)
	assert.Equal(t, "SUCCEEDED", parseBody(t, describeRec2)["JobStatus"])

	// includeInputPayload/includeOutputPayload=true surfaces the payload fields.
	withPayloads := describePath + "?includeInputPayload=true&includeOutputPayload=true"
	payloadRec := doRequest(t, h, http.MethodGet, withPayloads, nil)
	require.Equal(t, http.StatusOK, payloadRec.Code)
	payloadBody := parseBody(t, payloadRec)
	assert.JSONEq(t, `{"foo":"bar"}`, payloadBody["InputPayload"].(string))
	assert.NotEmpty(t, payloadBody["OutputPayload"])

	// Describe a nonexistent job -> 404.
	missingRec := doRequest(
		t, h, http.MethodGet,
		accountPath("/automation-groups/group1/automations/automation1/jobs/notexist"),
		nil,
	)
	assert.Equal(t, http.StatusNotFound, missingRec.Code)
	assert.Equal(t, "ResourceNotFoundException", parseBody(t, missingRec)["Code"])
}

// ---- StartAutomationJob validation ----

func TestQuickSight_AutomationJob_Validation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Missing automation ID segment is impossible via routing (empty automationID
	// still reaches the handler when both group/automation route segments are
	// present but blank isn't reachable through path routing); instead, exercise
	// two independent jobs to confirm each gets a distinct JobId.
	startPath := accountPath("/automation-groups/group1/automations/automation1/jobs")

	rec1 := doRequest(t, h, http.MethodPost, startPath, map[string]any{})
	require.Equal(t, http.StatusOK, rec1.Code)
	rec2 := doRequest(t, h, http.MethodPost, startPath, map[string]any{})
	require.Equal(t, http.StatusOK, rec2.Code)

	job1 := parseBody(t, rec1)["JobId"]
	job2 := parseBody(t, rec2)["JobId"]
	assert.NotEqual(t, job1, job2, "each StartAutomationJob call must mint a fresh JobId")
}
