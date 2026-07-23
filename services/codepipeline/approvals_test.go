package codepipeline_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandler_PutActionRevision covers PutActionRevision's real behavior:
// it tracks the submitted ActionRevision (surfaced back through
// GetPipelineState's actionStates[].currentRevision) and triggers a new
// pipeline execution, returning NewRevision=true the first time a given
// revisionId is seen and false on a repeat.
func TestHandler_PutActionRevision(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreatePipeline", map[string]any{"pipeline": samplePipeline("par-pipeline")})

	t.Run("missing pipelineName", func(t *testing.T) {
		t.Parallel()
		rec := doRequest(t, h, "PutActionRevision", map[string]any{})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("unknown action", func(t *testing.T) {
		t.Parallel()
		rec := doRequest(t, h, "PutActionRevision", map[string]any{
			"pipelineName": "par-pipeline", "stageName": "Source", "actionName": "NoSuchAction",
			"actionRevision": map[string]any{"revisionId": "r1", "revisionChangeId": "c1"},
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Equal(t, "ActionNotFoundException", decodeBody(t, rec.Body.Bytes())["__type"])
	})

	rec := doRequest(t, h, "PutActionRevision", map[string]any{
		"pipelineName": "par-pipeline", "stageName": "Source", "actionName": "SourceAction",
		"actionRevision": map[string]any{"revisionId": "abc123", "revisionChangeId": "change-1"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := decodeBody(t, rec.Body.Bytes())
	assert.Equal(t, true, out["newRevision"], "first time this revisionId is seen")
	execID, _ := out["pipelineExecutionId"].(string)
	require.NotEmpty(t, execID, "PutActionRevision must trigger a real, persisted pipeline execution")

	getRec := doRequest(t, h, "GetPipelineExecution", map[string]any{
		"pipelineName": "par-pipeline", "pipelineExecutionId": execID,
	})
	require.Equal(t, http.StatusOK, getRec.Code, "the triggered execution must actually be persisted")

	stateRec := doRequest(t, h, "GetPipelineState", map[string]any{"name": "par-pipeline"})
	body := decodeBody(t, stateRec.Body.Bytes())
	stageStates, _ := body["stageStates"].([]any)
	require.Len(t, stageStates, 1)
	stage, _ := stageStates[0].(map[string]any)
	actionStates, _ := stage["actionStates"].([]any)
	require.Len(t, actionStates, 1)
	action, _ := actionStates[0].(map[string]any)
	currentRevision, _ := action["currentRevision"].(map[string]any)
	require.NotNil(t, currentRevision, "PutActionRevision must be surfaced via GetPipelineState")
	assert.Equal(t, "abc123", currentRevision["revisionId"])
	assert.Equal(t, "change-1", currentRevision["revisionChangeId"])

	// Same revisionId again: NewRevision must now be false.
	rec = doRequest(t, h, "PutActionRevision", map[string]any{
		"pipelineName": "par-pipeline", "stageName": "Source", "actionName": "SourceAction",
		"actionRevision": map[string]any{"revisionId": "abc123", "revisionChangeId": "change-1"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, false, decodeBody(t, rec.Body.Bytes())["newRevision"], "repeat revisionId is not new")
}

// TestHandler_PutApprovalResult exercises the real token-based approval flow:
// StartPipelineExecution gates on an Approval action (action_engine.go), the
// pending token is obtained via GetPipelineState (the only real way a real
// AWS client can), and PutApprovalResult is validated against it. Before
// this fix, PutApprovalResult ignored its token entirely (never even parsed
// it) and accepted any status for any action regardless of category, so
// none of these real-AWS error paths were reachable.
func TestHandler_PutApprovalResult(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreatePipeline", map[string]any{"pipeline": approvalPipeline("approval-pipe")})
	startRec := doRequest(t, h, "StartPipelineExecution", map[string]any{"name": "approval-pipe"})
	execID, _ := decodeBody(t, startRec.Body.Bytes())["pipelineExecutionId"].(string)
	require.NotEmpty(t, execID)

	inProgressRec := doRequest(t, h, "GetPipelineExecution", map[string]any{
		"pipelineName": "approval-pipe", "pipelineExecutionId": execID,
	})
	pe, _ := decodeBody(t, inProgressRec.Body.Bytes())["pipelineExecution"].(map[string]any)
	require.Equal(t, "InProgress", pe["status"], "execution must pause at the Approval gate")

	stateRec := doRequest(t, h, "GetPipelineState", map[string]any{"name": "approval-pipe"})
	token := approvalToken(t, decodeBody(t, stateRec.Body.Bytes()), "Approve", "ApprovalAction")
	require.NotEmpty(t, token, "the pending approval token must be surfaced via GetPipelineState")

	t.Run("missing pipelineName", func(t *testing.T) {
		t.Parallel()
		rec := doRequest(t, h, "PutApprovalResult", map[string]any{})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	// The remaining subtests deliberately do NOT call t.Parallel(): they
	// probe error paths against the SAME pending approval consumed by the
	// "Correct token, Approved" call below, so they must run in declaration
	// order, before that call resolves it (t.Parallel() subtests are instead
	// deferred until the parent test function body returns, which would run
	// them all AFTER the approval below already consumed the token).
	//nolint:paralleltest // must run before the approval below consumes the token
	t.Run("missing token", func(t *testing.T) {
		rec := doRequest(t, h, "PutApprovalResult", map[string]any{
			"pipelineName": "approval-pipe", "stageName": "Approve", "actionName": "ApprovalAction",
			"result": map[string]any{"status": "Approved", "summary": "ok"},
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	//nolint:paralleltest // must run before the approval below consumes the token
	t.Run("action is not an Approval action", func(t *testing.T) {
		rec := doRequest(t, h, "PutApprovalResult", map[string]any{
			"pipelineName": "approval-pipe", "stageName": "Source", "actionName": "SourceAction",
			"token":  "irrelevant",
			"result": map[string]any{"status": "Approved", "summary": "ok"},
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Equal(t, "ActionNotFoundException", decodeBody(t, rec.Body.Bytes())["__type"])
	})

	//nolint:paralleltest // must run before the approval below consumes the token
	t.Run("wrong token is rejected", func(t *testing.T) {
		rec := doRequest(t, h, "PutApprovalResult", map[string]any{
			"pipelineName": "approval-pipe", "stageName": "Approve", "actionName": "ApprovalAction",
			"token":  "not-the-real-token",
			"result": map[string]any{"status": "Approved", "summary": "ok"},
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Equal(t, "InvalidApprovalTokenException", decodeBody(t, rec.Body.Bytes())["__type"])
	})

	//nolint:paralleltest // must run before the approval below consumes the token
	t.Run("invalid status", func(t *testing.T) {
		rec := doRequest(t, h, "PutApprovalResult", map[string]any{
			"pipelineName": "approval-pipe", "stageName": "Approve", "actionName": "ApprovalAction",
			"token":  token,
			"result": map[string]any{"status": "Maybe", "summary": "ok"},
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Equal(t, "ValidationException", decodeBody(t, rec.Body.Bytes())["__type"])
	})

	// Correct token, Approved: resumes the execution through to Deploy.
	rec := doRequest(t, h, "PutApprovalResult", map[string]any{
		"pipelineName": "approval-pipe", "stageName": "Approve", "actionName": "ApprovalAction",
		"token":  token,
		"result": map[string]any{"status": "Approved", "summary": "looks good"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := decodeBody(t, rec.Body.Bytes())
	approvedAt, ok := out["approvedAt"].(float64)
	require.True(t, ok, "approvedAt must be a JSON number (epoch seconds), not a string")
	assert.Positive(t, approvedAt)

	succeededRec := doRequest(t, h, "GetPipelineExecution", map[string]any{
		"pipelineName": "approval-pipe", "pipelineExecutionId": execID,
	})
	pe, _ = decodeBody(t, succeededRec.Body.Bytes())["pipelineExecution"].(map[string]any)
	assert.Equal(t, "Succeeded", pe["status"], "an approved gate must let the pipeline run to completion")

	// The approval is now resolved: reusing the same token must fail.
	t.Run("already completed", func(t *testing.T) {
		t.Parallel()
		againRec := doRequest(t, h, "PutApprovalResult", map[string]any{
			"pipelineName": "approval-pipe", "stageName": "Approve", "actionName": "ApprovalAction",
			"token":  token,
			"result": map[string]any{"status": "Approved", "summary": "again"},
		})
		assert.Equal(t, http.StatusBadRequest, againRec.Code)
		assert.Equal(t, "ApprovalAlreadyCompletedException", decodeBody(t, againRec.Body.Bytes())["__type"])
	})
}

// TestHandler_PutApprovalResult_Rejected verifies a Rejected result fails the
// stage and leaves the pipeline execution in a terminal Failed status,
// rather than the un-rejectable void response the pre-fix handler returned
// for every status string (including ones outside the real ApprovalStatus
// enum).
func TestHandler_PutApprovalResult_Rejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreatePipeline", map[string]any{"pipeline": approvalPipeline("approval-reject-pipe")})
	startRec := doRequest(t, h, "StartPipelineExecution", map[string]any{"name": "approval-reject-pipe"})
	execID, _ := decodeBody(t, startRec.Body.Bytes())["pipelineExecutionId"].(string)
	require.NotEmpty(t, execID)

	stateRec := doRequest(t, h, "GetPipelineState", map[string]any{"name": "approval-reject-pipe"})
	token := approvalToken(t, decodeBody(t, stateRec.Body.Bytes()), "Approve", "ApprovalAction")
	require.NotEmpty(t, token)

	rec := doRequest(t, h, "PutApprovalResult", map[string]any{
		"pipelineName": "approval-reject-pipe", "stageName": "Approve", "actionName": "ApprovalAction",
		"token":  token,
		"result": map[string]any{"status": "Rejected", "summary": "not ready"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := decodeBody(t, rec.Body.Bytes())
	_, ok := out["approvedAt"].(float64)
	assert.True(t, ok, "approvedAt is set even for a rejection (it records when the result was submitted)")

	getRec := doRequest(t, h, "GetPipelineExecution", map[string]any{
		"pipelineName": "approval-reject-pipe", "pipelineExecutionId": execID,
	})
	pe, _ := decodeBody(t, getRec.Body.Bytes())["pipelineExecution"].(map[string]any)
	assert.Equal(t, "Failed", pe["status"], "a rejected approval must fail the pipeline execution")
}
