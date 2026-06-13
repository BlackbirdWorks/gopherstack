package codepipeline_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/codepipeline"
)

// ---- Provider tests ----

func TestCPProvider_Name(t *testing.T) {
	t.Parallel()

	p := &codepipeline.Provider{}
	assert.Equal(t, "CodePipeline", p.Name())
}

func TestCPProvider_Init_NilCtx(t *testing.T) {
	t.Parallel()

	p := &codepipeline.Provider{}
	_, err := p.Init(nil)
	// CodePipeline requires a non-nil context
	require.Error(t, err)
}

func TestCPProvider_Init_WithCtx(t *testing.T) {
	t.Parallel()

	p := &codepipeline.Provider{}
	ctx := &service.AppContext{}
	reg, err := p.Init(ctx)
	require.NoError(t, err)
	require.NotNil(t, reg)
}

// decodeBody parses JSON from response body into a map.
func decodeBody(t *testing.T, data []byte) map[string]any {
	t.Helper()

	var m map[string]any
	require.NoError(t, json.Unmarshal(data, &m))

	return m
}

// ---- Pipeline Execution tests ----

func TestHandler_StartGetStopListPipelineExecution(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create pipeline first
	doRequest(t, h, "CreatePipeline", map[string]any{
		"pipeline": samplePipeline("exec-pipeline"),
	})

	// Start pipeline execution
	rec := doRequest(t, h, "StartPipelineExecution", map[string]any{
		"name": "exec-pipeline",
	})
	require.Equal(t, 200, rec.Code)

	resp := decodeBody(t, rec.Body.Bytes())
	execID, _ := resp["pipelineExecutionId"].(string)
	require.NotEmpty(t, execID)

	// Start with missing name
	rec = doRequest(t, h, "StartPipelineExecution", map[string]any{})
	assert.Equal(t, 400, rec.Code)

	// Get pipeline execution
	rec = doRequest(t, h, "GetPipelineExecution", map[string]any{
		"pipelineName":        "exec-pipeline",
		"pipelineExecutionId": execID,
	})
	require.Equal(t, 200, rec.Code)

	// Get with missing name
	rec = doRequest(t, h, "GetPipelineExecution", map[string]any{})
	assert.Equal(t, 400, rec.Code)

	// Stop pipeline execution
	rec = doRequest(t, h, "StopPipelineExecution", map[string]any{
		"pipelineName":        "exec-pipeline",
		"pipelineExecutionId": execID,
		"reason":              "manual stop",
	})
	require.Equal(t, 200, rec.Code)

	// Stop with missing name
	rec = doRequest(t, h, "StopPipelineExecution", map[string]any{})
	assert.Equal(t, 400, rec.Code)

	// List pipeline executions
	rec = doRequest(t, h, "ListPipelineExecutions", map[string]any{
		"pipelineName": "exec-pipeline",
	})
	require.Equal(t, 200, rec.Code)

	// List with missing name
	rec = doRequest(t, h, "ListPipelineExecutions", map[string]any{})
	assert.Equal(t, 400, rec.Code)
}

// ---- Pipeline State tests ----

func TestHandler_GetPipelineState(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreatePipeline", map[string]any{
		"pipeline": samplePipeline("state-pipeline"),
	})

	rec := doRequest(t, h, "GetPipelineState", map[string]any{
		"name": "state-pipeline",
	})
	require.Equal(t, 200, rec.Code)

	// Missing name
	rec = doRequest(t, h, "GetPipelineState", map[string]any{})
	assert.Equal(t, 400, rec.Code)
}

// ---- Retry / Rollback / Override tests ----

func TestHandler_RetryRollbackOverride(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreatePipeline", map[string]any{
		"pipeline": samplePipeline("retry-pipeline"),
	})
	startRec := doRequest(t, h, "StartPipelineExecution", map[string]any{"name": "retry-pipeline"})
	startResp := decodeBody(t, startRec.Body.Bytes())
	execID, _ := startResp["pipelineExecutionId"].(string)

	// Retry stage execution
	rec := doRequest(t, h, "RetryStageExecution", map[string]any{
		"pipelineName":        "retry-pipeline",
		"stageName":           "Source",
		"pipelineExecutionId": execID,
		"retryMode":           "FAILED_ACTIONS",
	})
	assert.Equal(t, 200, rec.Code)

	// Missing name
	rec = doRequest(t, h, "RetryStageExecution", map[string]any{})
	assert.Equal(t, 400, rec.Code)

	// Rollback stage
	rec = doRequest(t, h, "RollbackStage", map[string]any{
		"pipelineName":              "retry-pipeline",
		"stageName":                 "Source",
		"targetPipelineExecutionId": execID,
	})
	assert.Equal(t, 200, rec.Code)

	// Missing name
	rec = doRequest(t, h, "RollbackStage", map[string]any{})
	assert.Equal(t, 400, rec.Code)

	// Override stage condition
	rec = doRequest(t, h, "OverrideStageCondition", map[string]any{
		"pipelineName":        "retry-pipeline",
		"stageName":           "Source",
		"pipelineExecutionId": execID,
		"conditionType":       "BEFORE_ENTRY",
	})
	assert.Equal(t, 200, rec.Code)

	// Missing name
	rec = doRequest(t, h, "OverrideStageCondition", map[string]any{})
	assert.Equal(t, 400, rec.Code)
}

// ---- Webhook tests ----

func TestHandler_WebhookLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreatePipeline", map[string]any{
		"pipeline": samplePipeline("wh-pipeline"),
	})

	// Put webhook
	rec := doRequest(t, h, "PutWebhook", map[string]any{
		"webhook": map[string]any{
			"name":           "my-webhook",
			"targetPipeline": "wh-pipeline",
			"targetAction":   "Source",
			"authentication": "GITHUB_HMAC",
		},
	})
	require.Equal(t, 200, rec.Code)

	// Put with missing name
	rec = doRequest(t, h, "PutWebhook", map[string]any{"webhook": map[string]any{}})
	assert.Equal(t, 400, rec.Code)

	// Register webhook
	rec = doRequest(t, h, "RegisterWebhookWithThirdParty", map[string]any{
		"webhookName": "my-webhook",
	})
	assert.Equal(t, 200, rec.Code)

	// Register with missing name
	rec = doRequest(t, h, "RegisterWebhookWithThirdParty", map[string]any{})
	assert.Equal(t, 400, rec.Code)

	// List webhooks
	rec = doRequest(t, h, "ListWebhooks", map[string]any{})
	require.Equal(t, 200, rec.Code)

	// Deregister webhook
	rec = doRequest(t, h, "DeregisterWebhookWithThirdParty", map[string]any{
		"webhookName": "my-webhook",
	})
	assert.Equal(t, 200, rec.Code)

	// Deregister with missing name (returns 200 - no validation on webhook name)
	rec = doRequest(t, h, "DeregisterWebhookWithThirdParty", map[string]any{})
	assert.Equal(t, 200, rec.Code)

	// Delete webhook
	rec = doRequest(t, h, "DeleteWebhook", map[string]any{
		"name": "my-webhook",
	})
	assert.Equal(t, 200, rec.Code)

	// Delete with missing name
	rec = doRequest(t, h, "DeleteWebhook", map[string]any{})
	assert.Equal(t, 400, rec.Code)
}

// ---- Job tests ----

func TestHandler_JobOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	cat := &codepipeline.CustomActionType{
		Category: "Build",
		Provider: "MyBuild",
		Version:  "1",
	}

	_, err := h.Backend.CreateCustomActionType(context.Background(), cat)
	require.NoError(t, err)

	job := &codepipeline.Job{
		ID:    "job-001",
		Nonce: "nonce-001",
	}
	h.Backend.AddJobInternal(job)

	// Poll for jobs
	rec := doRequest(t, h, "PollForJobs", map[string]any{
		"actionTypeId": map[string]any{
			"category": "Build",
			"owner":    "Custom",
			"provider": "MyBuild",
			"version":  "1",
		},
	})
	require.Equal(t, 200, rec.Code)

	// Poll with empty action type id (no validation - returns 200)
	rec = doRequest(t, h, "PollForJobs", map[string]any{})
	assert.Equal(t, 200, rec.Code)

	// Acknowledge job
	rec = doRequest(t, h, "AcknowledgeJob", map[string]any{
		"jobId": "job-001",
		"nonce": "nonce-001",
	})
	assert.Equal(t, 200, rec.Code)

	// Acknowledge with missing jobId
	rec = doRequest(t, h, "AcknowledgeJob", map[string]any{})
	assert.Equal(t, 400, rec.Code)

	// Put job success
	rec = doRequest(t, h, "PutJobSuccessResult", map[string]any{
		"jobId": "job-001",
	})
	assert.Equal(t, 200, rec.Code)

	// Put job success with missing id
	rec = doRequest(t, h, "PutJobSuccessResult", map[string]any{})
	assert.Equal(t, 400, rec.Code)

	// Add another job and test failure result
	h.Backend.AddJobInternal(&codepipeline.Job{
		ID:    "job-002",
		Nonce: "nonce-002",
	})

	rec = doRequest(t, h, "PutJobFailureResult", map[string]any{
		"jobId": "job-002",
		"failureDetails": map[string]any{
			"message": "build failed",
			"type":    "JobFailed",
		},
	})
	assert.Equal(t, 200, rec.Code)

	// Missing jobId
	rec = doRequest(t, h, "PutJobFailureResult", map[string]any{})
	assert.Equal(t, 400, rec.Code)

	// Poll for third party jobs
	rec = doRequest(t, h, "PollForThirdPartyJobs", map[string]any{
		"actionTypeId": map[string]any{
			"category": "Build",
			"owner":    "Custom",
			"provider": "MyBuild",
			"version":  "1",
		},
	})
	assert.Equal(t, 200, rec.Code)

	// Poll third party with empty action type id (no validation - returns 200)
	rec = doRequest(t, h, "PollForThirdPartyJobs", map[string]any{})
	assert.Equal(t, 200, rec.Code)

	// Get third party job details
	rec = doRequest(t, h, "GetThirdPartyJobDetails", map[string]any{
		"jobId":       "job-001",
		"clientToken": "token",
	})
	assert.Equal(t, 200, rec.Code)

	// Get third party with missing jobId
	rec = doRequest(t, h, "GetThirdPartyJobDetails", map[string]any{})
	assert.Equal(t, 400, rec.Code)
}

// ---- Third Party Job tests ----

func TestHandler_ThirdPartyJobResults(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	h.Backend.AddJobInternal(&codepipeline.Job{
		ID:    "tp-job-001",
		Nonce: "nonce-tp-001",
	})

	// Acknowledge third party job
	rec := doRequest(t, h, "AcknowledgeThirdPartyJob", map[string]any{
		"jobId":       "tp-job-001",
		"nonce":       "nonce-tp-001",
		"clientToken": "token",
	})
	assert.Equal(t, 200, rec.Code)

	// Missing jobId
	rec = doRequest(t, h, "AcknowledgeThirdPartyJob", map[string]any{})
	assert.Equal(t, 400, rec.Code)

	// Put third party job success
	rec = doRequest(t, h, "PutThirdPartyJobSuccessResult", map[string]any{
		"jobId":       "tp-job-001",
		"clientToken": "token",
	})
	assert.Equal(t, 200, rec.Code)

	// Missing jobId
	rec = doRequest(t, h, "PutThirdPartyJobSuccessResult", map[string]any{})
	assert.Equal(t, 400, rec.Code)

	h.Backend.AddJobInternal(&codepipeline.Job{
		ID:    "tp-job-002",
		Nonce: "nonce-tp-002",
	})

	// Put third party job failure
	rec = doRequest(t, h, "PutThirdPartyJobFailureResult", map[string]any{
		"jobId":       "tp-job-002",
		"clientToken": "token",
		"failureDetails": map[string]any{
			"message": "failed",
			"type":    "JobFailed",
		},
	})
	assert.Equal(t, 200, rec.Code)

	// Missing jobId
	rec = doRequest(t, h, "PutThirdPartyJobFailureResult", map[string]any{})
	assert.Equal(t, 400, rec.Code)
}

// ---- Action revision and approval ----

func TestHandler_ActionRevisionAndApproval(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreatePipeline", map[string]any{
		"pipeline": samplePipeline("ap-pipeline"),
	})
	doRequest(t, h, "StartPipelineExecution", map[string]any{"name": "ap-pipeline"})

	// Put action revision
	rec := doRequest(t, h, "PutActionRevision", map[string]any{
		"pipelineName": "ap-pipeline",
		"stageName":    "Source",
		"actionName":   "SourceAction",
		"actionRevision": map[string]any{
			"revisionId":       "abc123",
			"revisionChangeId": "change-1",
		},
	})
	assert.Equal(t, 200, rec.Code)

	// Missing pipeline name
	rec = doRequest(t, h, "PutActionRevision", map[string]any{})
	assert.Equal(t, 400, rec.Code)

	// Put approval result
	rec = doRequest(t, h, "PutApprovalResult", map[string]any{
		"pipelineName": "ap-pipeline",
		"stageName":    "Source",
		"actionName":   "SourceAction",
		"result": map[string]any{
			"status":  "Approved",
			"summary": "looks good",
		},
		"token": "approval-token",
	})
	assert.Equal(t, 200, rec.Code)

	// Missing pipeline name
	rec = doRequest(t, h, "PutApprovalResult", map[string]any{})
	assert.Equal(t, 400, rec.Code)
}

// ---- Action type operations ----

func TestHandler_ActionTypeOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	cat := &codepipeline.CustomActionType{
		Category: "Test",
		Provider: "MyCIProvider",
		Version:  "1",
	}
	_, err := h.Backend.CreateCustomActionType(context.Background(), cat)
	require.NoError(t, err)

	// List action types
	rec := doRequest(t, h, "ListActionTypes", map[string]any{})
	require.Equal(t, 200, rec.Code)

	// Get action type
	rec = doRequest(t, h, "GetActionType", map[string]any{
		"category": "Test",
		"owner":    "Custom",
		"provider": "MyCIProvider",
		"version":  "1",
	})
	assert.Equal(t, 200, rec.Code)

	// Get action type with missing id
	rec = doRequest(t, h, "GetActionType", map[string]any{})
	assert.Equal(t, 400, rec.Code)

	// Update action type
	rec = doRequest(t, h, "UpdateActionType", map[string]any{
		"actionType": map[string]any{
			"id": map[string]any{
				"category": "Test",
				"owner":    "Custom",
				"provider": "MyCIProvider",
				"version":  "1",
			},
		},
	})
	assert.Equal(t, 200, rec.Code)

	// Update with missing actionType
	rec = doRequest(t, h, "UpdateActionType", map[string]any{})
	assert.Equal(t, 400, rec.Code)

	// Delete custom action type
	rec = doRequest(t, h, "DeleteCustomActionType", map[string]any{
		"category": "Test",
		"provider": "MyCIProvider",
		"version":  "1",
	})
	assert.Equal(t, 200, rec.Code)

	// Delete with missing category
	rec = doRequest(t, h, "DeleteCustomActionType", map[string]any{})
	assert.Equal(t, 400, rec.Code)
}

// ---- Execution list tests ----

func TestHandler_ListExecutionsAndRules(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreatePipeline", map[string]any{
		"pipeline": samplePipeline("el-pipeline"),
	})
	doRequest(t, h, "StartPipelineExecution", map[string]any{"name": "el-pipeline"})

	// List action executions
	rec := doRequest(t, h, "ListActionExecutions", map[string]any{
		"pipelineName": "el-pipeline",
	})
	require.Equal(t, 200, rec.Code)

	// Missing pipeline name
	rec = doRequest(t, h, "ListActionExecutions", map[string]any{})
	assert.Equal(t, 400, rec.Code)

	// List rule executions
	rec = doRequest(t, h, "ListRuleExecutions", map[string]any{
		"pipelineName": "el-pipeline",
	})
	require.Equal(t, 200, rec.Code)

	// Missing pipeline name
	rec = doRequest(t, h, "ListRuleExecutions", map[string]any{})
	assert.Equal(t, 400, rec.Code)

	// List rule types
	rec = doRequest(t, h, "ListRuleTypes", map[string]any{})
	require.Equal(t, 200, rec.Code)

	// List deploy action execution targets
	rec = doRequest(t, h, "ListDeployActionExecutionTargets", map[string]any{
		"pipelineName":        "el-pipeline",
		"pipelineExecutionId": "some-exec-id",
	})
	require.Equal(t, 200, rec.Code)
}

// ---- Persistence String coverage ----

func TestCPBackend_PersistenceString(t *testing.T) {
	t.Parallel()

	b := codepipeline.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreatePipeline(context.Background(), samplePipeline("snap-pipe"), nil)
	require.NoError(t, err)

	snap := b.Snapshot()
	require.NotNil(t, snap)
	assert.NotEmpty(t, snap)

	b2 := codepipeline.NewInMemoryBackend("000000000000", "us-east-1")
	err = b2.Restore(snap)
	require.NoError(t, err)
	assert.Equal(t, 1, b2.PipelineCount())
}
