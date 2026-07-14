package sagemaker_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sagemaker"
)

func TestHandler_ListAndDisassociateTrialComponents(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateExperiment", map[string]any{"ExperimentName": "exp-a"})
	doSageMakerRequest(t, h, "CreateTrial", map[string]any{"TrialName": "trial-a", "ExperimentName": "exp-a"})
	doSageMakerRequest(t, h, "CreateTrialComponent", map[string]any{"TrialComponentName": "tc-a"})
	doSageMakerRequest(t, h, "CreateTrialComponent", map[string]any{"TrialComponentName": "tc-b"})
	doSageMakerRequest(t, h, "AssociateTrialComponent", map[string]any{
		"TrialName": "trial-a", "TrialComponentName": "tc-a",
	})

	// Filtering by TrialName returns only the associated component.
	recByTrial := doSageMakerRequest(t, h, "ListTrialComponents", map[string]any{"TrialName": "trial-a"})
	require.Equal(t, http.StatusOK, recByTrial.Code)

	var byTrial map[string]any
	require.NoError(t, json.Unmarshal(recByTrial.Body.Bytes(), &byTrial))
	summaries, _ := byTrial["TrialComponentSummaries"].([]any)
	require.Len(t, summaries, 1)
	assert.Equal(t, "tc-a", summaries[0].(map[string]any)["TrialComponentName"])

	// Filtering by ExperimentName joins through the trial.
	recByExp := doSageMakerRequest(t, h, "ListTrialComponents", map[string]any{"ExperimentName": "exp-a"})
	require.Equal(t, http.StatusOK, recByExp.Code)

	var byExp map[string]any
	require.NoError(t, json.Unmarshal(recByExp.Body.Bytes(), &byExp))
	expSummaries, _ := byExp["TrialComponentSummaries"].([]any)
	require.Len(t, expSummaries, 1)

	// No filter returns every trial component regardless of association.
	recAll := doSageMakerRequest(t, h, "ListTrialComponents", map[string]any{})
	require.Equal(t, http.StatusOK, recAll.Code)

	var all map[string]any
	require.NoError(t, json.Unmarshal(recAll.Body.Bytes(), &all))
	allSummaries, _ := all["TrialComponentSummaries"].([]any)
	assert.Len(t, allSummaries, 2)

	// Disassociate, then the trial-scoped list is empty.
	recDisassoc := doSageMakerRequest(t, h, "DisassociateTrialComponent", map[string]any{
		"TrialName": "trial-a", "TrialComponentName": "tc-a",
	})
	require.Equal(t, http.StatusOK, recDisassoc.Code)

	var disassocOut map[string]any
	require.NoError(t, json.Unmarshal(recDisassoc.Body.Bytes(), &disassocOut))
	assert.NotEmpty(t, disassocOut["TrialArn"])
	assert.NotEmpty(t, disassocOut["TrialComponentArn"])

	recByTrial2 := doSageMakerRequest(t, h, "ListTrialComponents", map[string]any{"TrialName": "trial-a"})
	require.Equal(t, http.StatusOK, recByTrial2.Code)

	var byTrial2 map[string]any
	require.NoError(t, json.Unmarshal(recByTrial2.Body.Bytes(), &byTrial2))
	assert.Empty(t, byTrial2["TrialComponentSummaries"])
}

func TestHandler_DisassociateTrialComponent_Idempotent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Disassociating components that were never associated still succeeds
	// and returns the resources' ARNs (mirrors AssociateTrialComponent's
	// non-strict existence checks).
	rec := doSageMakerRequest(t, h, "DisassociateTrialComponent", map[string]any{
		"TrialName": "never-existed", "TrialComponentName": "never-existed-either",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Contains(t, out["TrialArn"], "never-existed")
}

// ---------------------------------------------------------------------------
// Image alias listing
// ---------------------------------------------------------------------------

func TestHandler_CreatePresignedDomainUrl(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	recDomain := doSageMakerRequest(t, h, "CreateDomain", map[string]any{"DomainName": "my-domain2"})
	var domainOut map[string]any
	require.NoError(t, json.Unmarshal(recDomain.Body.Bytes(), &domainOut))
	domainID, _ := domainOut["DomainId"].(string)
	require.NotEmpty(t, domainID)

	doSageMakerRequest(t, h, "CreateUserProfile", map[string]any{
		"DomainId": domainID, "UserProfileName": "my-user",
	})

	rec := doSageMakerRequest(t, h, "CreatePresignedDomainUrl", map[string]any{
		"DomainId": domainID, "UserProfileName": "my-user",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.NotEmpty(t, out["AuthorizedUrl"])
}

func TestHandler_CreatePresignedDomainUrl_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreatePresignedDomainUrl", map[string]any{
		"DomainId": "no-such-domain", "UserProfileName": "no-such-user",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// RenderUiTemplate / StartSession
// ---------------------------------------------------------------------------

func TestHandler_CreateDomain(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
		wantID   bool
	}{
		{
			name:     "success",
			body:     map[string]any{"DomainName": "my-domain", "AuthMode": "IAM"},
			wantCode: http.StatusOK,
			wantID:   true,
		},
		{
			name:     "missing domain name",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doSageMakerRequest(t, h, "CreateDomain", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantID {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp["DomainId"])
				assert.NotEmpty(t, resp["Url"])
			}
		})
	}
}

func TestHandler_DomainLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create domain.
	recCreate := doSageMakerRequest(t, h, "CreateDomain", map[string]any{
		"DomainName": "test-domain",
		"AuthMode":   "IAM",
	})
	require.Equal(t, http.StatusOK, recCreate.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(recCreate.Body.Bytes(), &createOut))
	domainID := createOut["DomainId"].(string)

	// Describe domain by ID.
	recDesc := doSageMakerRequest(t, h, "DescribeDomain", map[string]any{"DomainId": domainID})
	assert.Equal(t, http.StatusOK, recDesc.Code)

	var descOut map[string]any
	require.NoError(t, json.Unmarshal(recDesc.Body.Bytes(), &descOut))
	assert.Equal(t, "test-domain", descOut["DomainName"])

	// List domains.
	recList := doSageMakerRequest(t, h, "ListDomains", map[string]any{})
	assert.Equal(t, http.StatusOK, recList.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &listOut))
	assert.Len(t, listOut["Domains"].([]any), 1)

	// Update domain.
	recUpdate := doSageMakerRequest(t, h, "UpdateDomain", map[string]any{"DomainId": domainID})
	assert.Equal(t, http.StatusOK, recUpdate.Code)

	// Delete domain.
	recDelete := doSageMakerRequest(t, h, "DeleteDomain", map[string]any{"DomainId": domainID})
	assert.Equal(t, http.StatusOK, recDelete.Code)

	// Domain should be gone.
	recDesc2 := doSageMakerRequest(t, h, "DescribeDomain", map[string]any{"DomainId": domainID})
	assert.Equal(t, http.StatusBadRequest, recDesc2.Code)
}

func TestHandler_Domain_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, op := range []string{"DescribeDomain", "UpdateDomain", "DeleteDomain"} {
		t.Run(op, func(t *testing.T) {
			t.Parallel()

			rec := doSageMakerRequest(t, h, op, map[string]any{"DomainId": "nonexistent"})
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestHandler_CreateDomain_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doSageMakerRequest(t, h, "CreateDomain", map[string]any{"DomainName": "dup-domain"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doSageMakerRequest(t, h, "CreateDomain", map[string]any{"DomainName": "dup-domain"})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

func TestHandler_UserProfileLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create domain first.
	recDomain := doSageMakerRequest(t, h, "CreateDomain", map[string]any{"DomainName": "up-domain"})
	require.Equal(t, http.StatusOK, recDomain.Code)

	var domainOut map[string]any
	require.NoError(t, json.Unmarshal(recDomain.Body.Bytes(), &domainOut))
	domainID := domainOut["DomainId"].(string)

	// Create user profile.
	recCreate := doSageMakerRequest(t, h, "CreateUserProfile", map[string]any{
		"DomainId":        domainID,
		"UserProfileName": "my-user",
	})
	assert.Equal(t, http.StatusOK, recCreate.Code)

	// Describe user profile.
	recDesc := doSageMakerRequest(t, h, "DescribeUserProfile", map[string]any{
		"DomainId":        domainID,
		"UserProfileName": "my-user",
	})
	assert.Equal(t, http.StatusOK, recDesc.Code)

	// List user profiles.
	recList := doSageMakerRequest(
		t,
		h,
		"ListUserProfiles",
		map[string]any{"DomainIdEquals": domainID},
	)
	assert.Equal(t, http.StatusOK, recList.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &listOut))
	assert.Len(t, listOut["UserProfiles"].([]any), 1)

	// Delete user profile.
	recDelete := doSageMakerRequest(t, h, "DeleteUserProfile", map[string]any{
		"DomainId":        domainID,
		"UserProfileName": "my-user",
	})
	assert.Equal(t, http.StatusOK, recDelete.Code)
}

func TestHandler_UserProfile_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	recDomain := doSageMakerRequest(
		t,
		h,
		"CreateDomain",
		map[string]any{"DomainName": "up-notfound-domain"},
	)
	require.Equal(t, http.StatusOK, recDomain.Code)

	var domainOut map[string]any
	require.NoError(t, json.Unmarshal(recDomain.Body.Bytes(), &domainOut))
	domainID := domainOut["DomainId"].(string)

	for _, op := range []string{"DescribeUserProfile", "DeleteUserProfile"} {
		t.Run(op, func(t *testing.T) {
			t.Parallel()

			rec := doSageMakerRequest(t, h, op, map[string]any{
				"DomainId":        domainID,
				"UserProfileName": "nonexistent",
			})
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestHandler_AppLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create domain and user profile first.
	recDomain := doSageMakerRequest(
		t,
		h,
		"CreateDomain",
		map[string]any{"DomainName": "app-domain"},
	)
	require.Equal(t, http.StatusOK, recDomain.Code)

	var domainOut map[string]any
	require.NoError(t, json.Unmarshal(recDomain.Body.Bytes(), &domainOut))
	domainID := domainOut["DomainId"].(string)

	recUser := doSageMakerRequest(t, h, "CreateUserProfile", map[string]any{
		"DomainId":        domainID,
		"UserProfileName": "app-user",
	})
	require.Equal(t, http.StatusOK, recUser.Code)

	// Create app.
	recCreate := doSageMakerRequest(t, h, "CreateApp", map[string]any{
		"DomainId":        domainID,
		"UserProfileName": "app-user",
		"AppType":         "JupyterServer",
		"AppName":         "my-app",
	})
	assert.Equal(t, http.StatusOK, recCreate.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(recCreate.Body.Bytes(), &createOut))
	assert.NotEmpty(t, createOut["AppArn"])

	// Describe app.
	recDesc := doSageMakerRequest(t, h, "DescribeApp", map[string]any{
		"DomainId":        domainID,
		"UserProfileName": "app-user",
		"AppType":         "JupyterServer",
		"AppName":         "my-app",
	})
	assert.Equal(t, http.StatusOK, recDesc.Code)

	// List apps.
	recList := doSageMakerRequest(t, h, "ListApps", map[string]any{"DomainIdEquals": domainID})
	assert.Equal(t, http.StatusOK, recList.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &listOut))
	assert.Len(t, listOut["Apps"].([]any), 1)

	// Delete app.
	recDelete := doSageMakerRequest(t, h, "DeleteApp", map[string]any{
		"DomainId":        domainID,
		"UserProfileName": "app-user",
		"AppType":         "JupyterServer",
		"AppName":         "my-app",
	})
	assert.Equal(t, http.StatusOK, recDelete.Code)
}

// ---------------------------------------------------------------------------
// Feature Group lifecycle
// ---------------------------------------------------------------------------

func TestHandler_FeatureGroupLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create feature group.
	recCreate := doSageMakerRequest(t, h, "CreateFeatureGroup", map[string]any{
		"FeatureGroupName":                  "my-features",
		"RecordIdentifierFeatureDefinition": "id",
		"EventTimeFeatureName":              "event_time",
		"FeatureDefinitions": []map[string]any{
			{"FeatureName": "id", "FeatureType": "Integral"},
			{"FeatureName": "event_time", "FeatureType": "String"},
		},
	})
	assert.Equal(t, http.StatusOK, recCreate.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(recCreate.Body.Bytes(), &createOut))
	assert.NotEmpty(t, createOut["FeatureGroupArn"])

	// Describe feature group.
	recDesc := doSageMakerRequest(t, h, "DescribeFeatureGroup", map[string]any{
		"FeatureGroupName": "my-features",
	})
	assert.Equal(t, http.StatusOK, recDesc.Code)

	// List feature groups.
	recList := doSageMakerRequest(t, h, "ListFeatureGroups", map[string]any{})
	assert.Equal(t, http.StatusOK, recList.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &listOut))
	assert.Len(t, listOut["FeatureGroupSummaries"].([]any), 1)

	// Delete feature group.
	recDelete := doSageMakerRequest(t, h, "DeleteFeatureGroup", map[string]any{
		"FeatureGroupName": "my-features",
	})
	assert.Equal(t, http.StatusOK, recDelete.Code)

	// Should be gone.
	recDesc2 := doSageMakerRequest(t, h, "DescribeFeatureGroup", map[string]any{
		"FeatureGroupName": "my-features",
	})
	assert.Equal(t, http.StatusBadRequest, recDesc2.Code)
}

func TestHandler_FeatureGroup_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{"FeatureGroupName": "dup-features"}
	rec := doSageMakerRequest(t, h, "CreateFeatureGroup", body)
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doSageMakerRequest(t, h, "CreateFeatureGroup", body)
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

func TestHandler_FeatureGroup_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, op := range []string{"DescribeFeatureGroup", "DeleteFeatureGroup"} {
		t.Run(op, func(t *testing.T) {
			t.Parallel()

			rec := doSageMakerRequest(t, h, op, map[string]any{"FeatureGroupName": "nonexistent"})
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestHandler_PipelineLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create pipeline.
	recCreate := doSageMakerRequest(t, h, "CreatePipeline", map[string]any{
		"PipelineName":       "my-pipeline",
		"PipelineDefinition": `{"Version":"2020-12-01","Steps":[]}`,
	})
	assert.Equal(t, http.StatusOK, recCreate.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(recCreate.Body.Bytes(), &createOut))
	assert.NotEmpty(t, createOut["PipelineArn"])

	// Describe pipeline.
	recDesc := doSageMakerRequest(
		t,
		h,
		"DescribePipeline",
		map[string]any{"PipelineName": "my-pipeline"},
	)
	assert.Equal(t, http.StatusOK, recDesc.Code)

	// List pipelines.
	recList := doSageMakerRequest(t, h, "ListPipelines", map[string]any{})
	assert.Equal(t, http.StatusOK, recList.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &listOut))
	assert.Len(t, listOut["PipelineSummaries"].([]any), 1)

	// Update pipeline.
	recUpdate := doSageMakerRequest(t, h, "UpdatePipeline", map[string]any{
		"PipelineName":       "my-pipeline",
		"PipelineDefinition": `{"Version":"2020-12-01","Steps":[{"Name":"step1"}]}`,
	})
	assert.Equal(t, http.StatusOK, recUpdate.Code)

	// Start pipeline execution.
	recExec := doSageMakerRequest(t, h, "StartPipelineExecution", map[string]any{
		"PipelineName": "my-pipeline",
	})
	assert.Equal(t, http.StatusOK, recExec.Code)

	var execOut map[string]any
	require.NoError(t, json.Unmarshal(recExec.Body.Bytes(), &execOut))
	execArn := execOut["PipelineExecutionArn"].(string)
	assert.NotEmpty(t, execArn)

	// Describe pipeline execution.
	recDescExec := doSageMakerRequest(t, h, "DescribePipelineExecution", map[string]any{
		"PipelineExecutionArn": execArn,
	})
	assert.Equal(t, http.StatusOK, recDescExec.Code)

	// List pipeline executions.
	recListExec := doSageMakerRequest(t, h, "ListPipelineExecutions", map[string]any{
		"PipelineName": "my-pipeline",
	})
	assert.Equal(t, http.StatusOK, recListExec.Code)

	// Delete pipeline.
	recDelete := doSageMakerRequest(
		t,
		h,
		"DeletePipeline",
		map[string]any{"PipelineName": "my-pipeline"},
	)
	assert.Equal(t, http.StatusOK, recDelete.Code)
}

func TestHandler_Pipeline_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, op := range []string{"DescribePipeline", "UpdatePipeline", "DeletePipeline"} {
		t.Run(op, func(t *testing.T) {
			t.Parallel()

			rec := doSageMakerRequest(t, h, op, map[string]any{"PipelineName": "nonexistent"})
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestHandler_Pipeline_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{"PipelineName": "dup-pipeline"}
	rec := doSageMakerRequest(t, h, "CreatePipeline", body)
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doSageMakerRequest(t, h, "CreatePipeline", body)
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

// ---------------------------------------------------------------------------
// Pipeline execution step operations
// ---------------------------------------------------------------------------

func TestHandler_PipelineExecutionSteps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create and start pipeline.
	doSageMakerRequest(t, h, "CreatePipeline", map[string]any{"PipelineName": "step-pipeline"})
	recExec := doSageMakerRequest(t, h, "StartPipelineExecution", map[string]any{
		"PipelineName": "step-pipeline",
	})
	require.Equal(t, http.StatusOK, recExec.Code)

	var execOut map[string]any
	require.NoError(t, json.Unmarshal(recExec.Body.Bytes(), &execOut))
	execArn := execOut["PipelineExecutionArn"].(string)

	// ListPipelineExecutionSteps.
	recList := doSageMakerRequest(t, h, "ListPipelineExecutionSteps", map[string]any{
		"PipelineExecutionArn": execArn,
	})
	assert.Equal(t, http.StatusOK, recList.Code)

	// SendPipelineExecutionStepSuccess.
	recSuccess := doSageMakerRequest(t, h, "SendPipelineExecutionStepSuccess", map[string]any{
		"CallbackToken": execArn,
	})
	assert.Equal(t, http.StatusOK, recSuccess.Code)

	// SendPipelineExecutionStepFailure.
	recFail := doSageMakerRequest(t, h, "SendPipelineExecutionStepFailure", map[string]any{
		"CallbackToken": execArn,
		"FailureReason": "test failure",
	})
	assert.Equal(t, http.StatusOK, recFail.Code)

	// RetryPipelineExecution.
	recRetry := doSageMakerRequest(t, h, "RetryPipelineExecution", map[string]any{
		"PipelineExecutionArn": execArn,
	})
	assert.Equal(t, http.StatusOK, recRetry.Code)

	// StopPipelineExecution.
	recStop := doSageMakerRequest(t, h, "StopPipelineExecution", map[string]any{
		"PipelineExecutionArn": execArn,
	})
	assert.Equal(t, http.StatusOK, recStop.Code)
}

// ---------------------------------------------------------------------------
// Experiment / Trial / TrialComponent lifecycle
// ---------------------------------------------------------------------------

func TestHandler_ExperimentLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create experiment.
	recCreate := doSageMakerRequest(t, h, "CreateExperiment", map[string]any{
		"ExperimentName": "my-experiment",
	})
	assert.Equal(t, http.StatusOK, recCreate.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(recCreate.Body.Bytes(), &createOut))
	assert.NotEmpty(t, createOut["ExperimentArn"])

	// Describe experiment.
	recDesc := doSageMakerRequest(t, h, "DescribeExperiment", map[string]any{
		"ExperimentName": "my-experiment",
	})
	assert.Equal(t, http.StatusOK, recDesc.Code)

	// List experiments.
	recList := doSageMakerRequest(t, h, "ListExperiments", map[string]any{})
	assert.Equal(t, http.StatusOK, recList.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &listOut))
	assert.Len(t, listOut["ExperimentSummaries"].([]any), 1)

	// Delete experiment.
	recDelete := doSageMakerRequest(t, h, "DeleteExperiment", map[string]any{
		"ExperimentName": "my-experiment",
	})
	assert.Equal(t, http.StatusOK, recDelete.Code)
}

func TestHandler_Experiment_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, op := range []string{"DescribeExperiment", "DeleteExperiment"} {
		t.Run(op, func(t *testing.T) {
			t.Parallel()

			rec := doSageMakerRequest(t, h, op, map[string]any{"ExperimentName": "nonexistent"})
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestHandler_TrialLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create experiment first.
	doSageMakerRequest(
		t,
		h,
		"CreateExperiment",
		map[string]any{"ExperimentName": "trial-experiment"},
	)

	// Create trial.
	recCreate := doSageMakerRequest(t, h, "CreateTrial", map[string]any{
		"TrialName":      "my-trial",
		"ExperimentName": "trial-experiment",
	})
	assert.Equal(t, http.StatusOK, recCreate.Code)

	// Describe trial.
	recDesc := doSageMakerRequest(t, h, "DescribeTrial", map[string]any{"TrialName": "my-trial"})
	assert.Equal(t, http.StatusOK, recDesc.Code)

	// List trials.
	recList := doSageMakerRequest(t, h, "ListTrials", map[string]any{
		"ExperimentName": "trial-experiment",
	})
	assert.Equal(t, http.StatusOK, recList.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &listOut))
	assert.Len(t, listOut["TrialSummaries"].([]any), 1)

	// Delete trial.
	recDelete := doSageMakerRequest(t, h, "DeleteTrial", map[string]any{"TrialName": "my-trial"})
	assert.Equal(t, http.StatusOK, recDelete.Code)
}

func TestHandler_TrialComponent_Lifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create trial component.
	recCreate := doSageMakerRequest(t, h, "CreateTrialComponent", map[string]any{
		"TrialComponentName": "my-component",
	})
	assert.Equal(t, http.StatusOK, recCreate.Code)

	// Describe trial component.
	recDesc := doSageMakerRequest(t, h, "DescribeTrialComponent", map[string]any{
		"TrialComponentName": "my-component",
	})
	assert.Equal(t, http.StatusOK, recDesc.Code)

	// Delete trial component.
	recDelete := doSageMakerRequest(t, h, "DeleteTrialComponent", map[string]any{
		"TrialComponentName": "my-component",
	})
	assert.Equal(t, http.StatusOK, recDelete.Code)
}

func TestBackend_PipelineOps_Direct(t *testing.T) {
	t.Parallel()

	b := sagemaker.NewInMemoryBackend("000000000000", "us-east-1")

	// Create and start a pipeline.
	_, err := b.CreatePipeline(context.Background(), "direct-pipeline", `{"Version":"2020-12-01"}`, "", nil)
	require.NoError(t, err)

	exec, err := b.StartPipelineExecution(context.Background(), "direct-pipeline")
	require.NoError(t, err)
	execArn := exec.PipelineExecutionArn

	// ListPipelineExecutionSteps.
	steps, _ := b.ListPipelineExecutionSteps(context.Background(), execArn, "")
	assert.NotNil(t, steps)

	// SendPipelineExecutionStepSuccess.
	err = b.SendPipelineExecutionStepSuccess(context.Background(), execArn, "step1")
	require.NoError(t, err)

	// SendPipelineExecutionStepFailure.
	err = b.SendPipelineExecutionStepFailure(context.Background(), execArn, "step2", "out of memory")
	require.NoError(t, err)

	// RetryPipelineExecution.
	retried, err := b.RetryPipelineExecution(context.Background(), execArn)
	require.NoError(t, err)
	assert.NotEmpty(t, retried.PipelineExecutionArn)

	// StopPipelineExecution.
	stopped, err := b.StopPipelineExecution(context.Background(), execArn)
	require.NoError(t, err)
	assert.NotEmpty(t, stopped.PipelineExecutionArn)
}

func TestBackend_PipelineOps_NotFound(t *testing.T) {
	t.Parallel()

	b := sagemaker.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.RetryPipelineExecution(context.Background(), "nonexistent-exec-arn")
	require.Error(t, err)

	_, err = b.StopPipelineExecution(context.Background(), "nonexistent-exec-arn")
	require.Error(t, err)
}
