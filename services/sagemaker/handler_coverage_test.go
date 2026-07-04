package sagemaker_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sagemaker"
)

// ---------------------------------------------------------------------------
// Domain / UserProfile / App lifecycle
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

// ---------------------------------------------------------------------------
// Feature metadata operations
// ---------------------------------------------------------------------------

func TestHandler_UpdateAndDescribeFeatureMetadata(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateFeatureGroup", map[string]any{
		"FeatureGroupName":                  "meta-features",
		"RecordIdentifierFeatureDefinition": "id",
		"EventTimeFeatureName":              "event_time",
		"FeatureDefinitions": []map[string]any{
			{"FeatureName": "id", "FeatureType": "Integral"},
			{"FeatureName": "event_time", "FeatureType": "String"},
		},
	})

	recUpdate := doSageMakerRequest(t, h, "UpdateFeatureMetadata", map[string]any{
		"FeatureGroupName": "meta-features",
		"FeatureName":      "id",
		"Description":      "the record id",
	})
	assert.Equal(t, http.StatusOK, recUpdate.Code)

	recDescribe := doSageMakerRequest(t, h, "DescribeFeatureMetadata", map[string]any{
		"FeatureGroupName": "meta-features",
		"FeatureName":      "id",
	})
	assert.Equal(t, http.StatusOK, recDescribe.Code)

	var describeOut map[string]any
	require.NoError(t, json.Unmarshal(recDescribe.Body.Bytes(), &describeOut))
	assert.Equal(t, "the record id", describeOut["Description"])
	assert.Equal(t, "id", describeOut["FeatureName"])
	assert.Equal(t, "Integral", describeOut["FeatureType"])
	assert.NotEmpty(t, describeOut["FeatureGroupArn"])
}

func TestHandler_UpdateFeatureMetadata_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "UpdateFeatureMetadata", map[string]any{
		"FeatureGroupName": "no-such-group",
		"FeatureName":      "id",
		"Description":      "stub test",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// Pipeline lifecycle
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Endpoint lifecycle
// ---------------------------------------------------------------------------

func TestHandler_EndpointLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create endpoint config first.
	doSageMakerRequest(t, h, "CreateEndpointConfig", map[string]any{
		"EndpointConfigName": "my-ep-config",
		"ProductionVariants": []map[string]any{
			{
				"VariantName":          "main",
				"ModelName":            "m",
				"InstanceType":         "ml.m5.large",
				"InitialInstanceCount": 1,
			},
		},
	})

	// Create endpoint.
	recCreate := doSageMakerRequest(t, h, "CreateEndpoint", map[string]any{
		"EndpointName":       "my-endpoint",
		"EndpointConfigName": "my-ep-config",
	})
	assert.Equal(t, http.StatusOK, recCreate.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(recCreate.Body.Bytes(), &createOut))
	assert.NotEmpty(t, createOut["EndpointArn"])

	// Describe endpoint.
	recDesc := doSageMakerRequest(
		t,
		h,
		"DescribeEndpoint",
		map[string]any{"EndpointName": "my-endpoint"},
	)
	assert.Equal(t, http.StatusOK, recDesc.Code)

	// List endpoints.
	recList := doSageMakerRequest(t, h, "ListEndpoints", map[string]any{})
	assert.Equal(t, http.StatusOK, recList.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &listOut))
	assert.Len(t, listOut["Endpoints"].([]any), 1)

	// Update endpoint.
	recUpdate := doSageMakerRequest(t, h, "UpdateEndpoint", map[string]any{
		"EndpointName":       "my-endpoint",
		"EndpointConfigName": "my-ep-config",
	})
	assert.Equal(t, http.StatusOK, recUpdate.Code)

	// UpdateEndpointWeightsAndCapacities.
	recUpdateWeights := doSageMakerRequest(
		t,
		h,
		"UpdateEndpointWeightsAndCapacities",
		map[string]any{
			"EndpointName":                "my-endpoint",
			"DesiredWeightsAndCapacities": []map[string]any{},
		},
	)
	assert.Equal(t, http.StatusOK, recUpdateWeights.Code)

	// Delete endpoint.
	recDelete := doSageMakerRequest(
		t,
		h,
		"DeleteEndpoint",
		map[string]any{"EndpointName": "my-endpoint"},
	)
	assert.Equal(t, http.StatusOK, recDelete.Code)
}

func TestHandler_Endpoint_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, op := range []string{"DescribeEndpoint", "UpdateEndpoint", "DeleteEndpoint"} {
		t.Run(op, func(t *testing.T) {
			t.Parallel()

			rec := doSageMakerRequest(t, h, op, map[string]any{"EndpointName": "nonexistent"})
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// ---------------------------------------------------------------------------
// Training Job lifecycle
// ---------------------------------------------------------------------------

func TestHandler_TrainingJobLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create training job.
	recCreate := doSageMakerRequest(t, h, "CreateTrainingJob", map[string]any{
		"TrainingJobName":        "my-training-job",
		"AlgorithmSpecification": map[string]any{"TrainingInputMode": "File"},
		"OutputDataConfig":       map[string]any{"S3OutputPath": "s3://bucket/output"},
		"ResourceConfig": map[string]any{
			"InstanceType":   "ml.m5.large",
			"InstanceCount":  1,
			"VolumeSizeInGB": 20,
		},
		"StoppingCondition": map[string]any{"MaxRuntimeInSeconds": 3600},
	})
	assert.Equal(t, http.StatusOK, recCreate.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(recCreate.Body.Bytes(), &createOut))
	assert.NotEmpty(t, createOut["TrainingJobArn"])

	// Describe training job.
	recDesc := doSageMakerRequest(t, h, "DescribeTrainingJob", map[string]any{
		"TrainingJobName": "my-training-job",
	})
	assert.Equal(t, http.StatusOK, recDesc.Code)

	// List training jobs.
	recList := doSageMakerRequest(t, h, "ListTrainingJobs", map[string]any{})
	assert.Equal(t, http.StatusOK, recList.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &listOut))
	assert.Len(t, listOut["TrainingJobSummaries"].([]any), 1)

	// StopTrainingJob.
	recStop := doSageMakerRequest(t, h, "StopTrainingJob", map[string]any{
		"TrainingJobName": "my-training-job",
	})
	assert.Equal(t, http.StatusOK, recStop.Code)

	// UpdateTrainingJob.
	recUpdate := doSageMakerRequest(t, h, "UpdateTrainingJob", map[string]any{
		"TrainingJobName": "my-training-job",
	})
	assert.Equal(t, http.StatusOK, recUpdate.Code)

	// DeleteTrainingJob.
	recDelete := doSageMakerRequest(t, h, "DeleteTrainingJob", map[string]any{
		"TrainingJobName": "my-training-job",
	})
	assert.Equal(t, http.StatusOK, recDelete.Code)
}

// ---------------------------------------------------------------------------
// Notebook Instance lifecycle
// ---------------------------------------------------------------------------

func TestHandler_NotebookInstanceLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create notebook instance.
	recCreate := doSageMakerRequest(t, h, "CreateNotebookInstance", map[string]any{
		"NotebookInstanceName": "my-notebook",
		"InstanceType":         "ml.t2.medium",
		"RoleArn":              "arn:aws:iam::000000000000:role/notebook-role",
	})
	assert.Equal(t, http.StatusOK, recCreate.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(recCreate.Body.Bytes(), &createOut))
	assert.NotEmpty(t, createOut["NotebookInstanceArn"])

	// Describe.
	recDesc := doSageMakerRequest(t, h, "DescribeNotebookInstance", map[string]any{
		"NotebookInstanceName": "my-notebook",
	})
	assert.Equal(t, http.StatusOK, recDesc.Code)

	// List.
	recList := doSageMakerRequest(t, h, "ListNotebookInstances", map[string]any{})
	assert.Equal(t, http.StatusOK, recList.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &listOut))
	assert.Len(t, listOut["NotebookInstances"].([]any), 1)

	// Start.
	recStart := doSageMakerRequest(t, h, "StartNotebookInstance", map[string]any{
		"NotebookInstanceName": "my-notebook",
	})
	assert.Equal(t, http.StatusOK, recStart.Code)

	// Update.
	recUpdate := doSageMakerRequest(t, h, "UpdateNotebookInstance", map[string]any{
		"NotebookInstanceName": "my-notebook",
		"InstanceType":         "ml.t3.medium",
	})
	assert.Equal(t, http.StatusOK, recUpdate.Code)

	// Stop.
	recStop := doSageMakerRequest(t, h, "StopNotebookInstance", map[string]any{
		"NotebookInstanceName": "my-notebook",
	})
	assert.Equal(t, http.StatusOK, recStop.Code)

	// CreatePresignedNotebookInstanceUrl.
	recURL := doSageMakerRequest(t, h, "CreatePresignedNotebookInstanceUrl", map[string]any{
		"NotebookInstanceName": "my-notebook",
	})
	assert.Equal(t, http.StatusOK, recURL.Code)

	// Delete.
	recDelete := doSageMakerRequest(t, h, "DeleteNotebookInstance", map[string]any{
		"NotebookInstanceName": "my-notebook",
	})
	assert.Equal(t, http.StatusOK, recDelete.Code)
}

// ---------------------------------------------------------------------------
// HyperParameter Tuning Job
// ---------------------------------------------------------------------------

func TestHandler_HyperParameterTuningJobLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create HPT job.
	recCreate := doSageMakerRequest(t, h, "CreateHyperParameterTuningJob", map[string]any{
		"HyperParameterTuningJobName": "my-hpt-job",
		"HyperParameterTuningJobConfig": map[string]any{
			"Strategy": "Bayesian",
			"ResourceLimits": map[string]any{
				"MaxNumberOfTrainingJobs": 10,
				"MaxParallelTrainingJobs": 2,
			},
		},
	})
	assert.Equal(t, http.StatusOK, recCreate.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(recCreate.Body.Bytes(), &createOut))
	assert.NotEmpty(t, createOut["HyperParameterTuningJobArn"])

	// Describe.
	recDesc := doSageMakerRequest(t, h, "DescribeHyperParameterTuningJob", map[string]any{
		"HyperParameterTuningJobName": "my-hpt-job",
	})
	assert.Equal(t, http.StatusOK, recDesc.Code)

	// List.
	recList := doSageMakerRequest(t, h, "ListHyperParameterTuningJobs", map[string]any{})
	assert.Equal(t, http.StatusOK, recList.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &listOut))
	assert.Len(t, listOut["HyperParameterTuningJobSummaries"].([]any), 1)

	// Stop.
	recStop := doSageMakerRequest(t, h, "StopHyperParameterTuningJob", map[string]any{
		"HyperParameterTuningJobName": "my-hpt-job",
	})
	assert.Equal(t, http.StatusOK, recStop.Code)

	// Delete.
	recDelete := doSageMakerRequest(t, h, "DeleteHyperParameterTuningJob", map[string]any{
		"HyperParameterTuningJobName": "my-hpt-job",
	})
	assert.Equal(t, http.StatusOK, recDelete.Code)
}

// ---------------------------------------------------------------------------
// Reset test
// ---------------------------------------------------------------------------

func TestHandler_SageMakerReset(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create various resources.
	doSageMakerRequest(t, h, "CreateModel", map[string]any{
		"ModelName":        "reset-model",
		"ExecutionRoleArn": "arn:aws:iam::000000000000:role/test",
	})
	doSageMakerRequest(t, h, "CreateFeatureGroup", map[string]any{"FeatureGroupName": "reset-fg"})
	doSageMakerRequest(t, h, "CreatePipeline", map[string]any{"PipelineName": "reset-pipeline"})
	doSageMakerRequest(t, h, "CreateDomain", map[string]any{"DomainName": "reset-domain"})

	// Verify they exist.
	recList := doSageMakerRequest(t, h, "ListModels", map[string]any{})
	require.Equal(t, http.StatusOK, recList.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &listOut))
	require.NotEmpty(t, listOut["Models"])

	// Reset.
	h.Backend.Reset()

	// Models gone.
	recList2 := doSageMakerRequest(t, h, "ListModels", map[string]any{})
	require.Equal(t, http.StatusOK, recList2.Code)

	var listOut2 map[string]any
	require.NoError(t, json.Unmarshal(recList2.Body.Bytes(), &listOut2))
	assert.Empty(t, listOut2["Models"])

	// Feature groups gone.
	recFG := doSageMakerRequest(t, h, "ListFeatureGroups", map[string]any{})
	require.Equal(t, http.StatusOK, recFG.Code)

	var fgOut map[string]any
	require.NoError(t, json.Unmarshal(recFG.Body.Bytes(), &fgOut))
	assert.Empty(t, fgOut["FeatureGroupSummaries"])
}

// ---------------------------------------------------------------------------
// Missing name / input validation
// ---------------------------------------------------------------------------

func TestHandler_SageMaker_ValidationErrors(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tests := []struct {
		body map[string]any
		op   string
	}{
		{op: "CreateDomain", body: map[string]any{}},
		{op: "CreateUserProfile", body: map[string]any{}},
		{op: "CreateApp", body: map[string]any{}},
		{op: "CreateFeatureGroup", body: map[string]any{}},
		{op: "CreatePipeline", body: map[string]any{}},
		{op: "CreateExperiment", body: map[string]any{}},
		{op: "CreateTrial", body: map[string]any{}},
		{op: "CreateTrialComponent", body: map[string]any{}},
		{op: "CreateTrainingJob", body: map[string]any{}},
		{op: "CreateNotebookInstance", body: map[string]any{}},
		{op: "CreateHyperParameterTuningJob", body: map[string]any{}},
		{op: "CreateEndpoint", body: map[string]any{}},
	}

	for _, tt := range tests {
		t.Run(tt.op, func(t *testing.T) {
			t.Parallel()

			rec := doSageMakerRequest(t, h, tt.op, tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code, "op=%s", tt.op)
		})
	}
}

// ---------------------------------------------------------------------------
// GetSupportedOperations includes stateful ops
// ---------------------------------------------------------------------------

func TestHandler_GetSupportedOperations_StatefulOps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	for _, op := range []string{
		"CreateDomain", "DescribeDomain", "ListDomains", "DeleteDomain", "UpdateDomain",
		"CreateUserProfile", "DescribeUserProfile", "ListUserProfiles", "DeleteUserProfile",
		"CreateApp", "DescribeApp", "ListApps", "DeleteApp",
		"CreateFeatureGroup", "DescribeFeatureGroup", "ListFeatureGroups", "DeleteFeatureGroup",
		"CreatePipeline", "DescribePipeline", "ListPipelines", "UpdatePipeline", "DeletePipeline",
		"StartPipelineExecution", "DescribePipelineExecution", "ListPipelineExecutions",
		"CreateExperiment", "DescribeExperiment", "ListExperiments", "DeleteExperiment",
		"CreateTrial", "DescribeTrial", "ListTrials", "DeleteTrial",
		"CreateTrialComponent", "DescribeTrialComponent", "DeleteTrialComponent",
		"CreateEndpoint", "DescribeEndpoint", "ListEndpoints", "DeleteEndpoint", "UpdateEndpoint",
		"CreateTrainingJob", "DescribeTrainingJob", "ListTrainingJobs", "StopTrainingJob", "DeleteTrainingJob",
		"CreateNotebookInstance", "DescribeNotebookInstance", "ListNotebookInstances",
		"StartNotebookInstance", "StopNotebookInstance", "DeleteNotebookInstance",
		"CreateHyperParameterTuningJob", "DescribeHyperParameterTuningJob",
		"ListHyperParameterTuningJobs", "StopHyperParameterTuningJob", "DeleteHyperParameterTuningJob",
	} {
		assert.Contains(t, ops, op)
	}
}

// ---------------------------------------------------------------------------
// Async notebook instance timing test
// ---------------------------------------------------------------------------

func TestHandler_NotebookInstance_EventuallyInService(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateNotebookInstance", map[string]any{
		"NotebookInstanceName": "async-notebook",
		"InstanceType":         "ml.t2.medium",
		"RoleArn":              "arn:aws:iam::000000000000:role/notebook-role",
	})

	// Wait for async status transition.
	time.Sleep(300 * time.Millisecond)

	recDesc := doSageMakerRequest(t, h, "DescribeNotebookInstance", map[string]any{
		"NotebookInstanceName": "async-notebook",
	})
	assert.Equal(t, http.StatusOK, recDesc.Code)

	var descOut map[string]any
	require.NoError(t, json.Unmarshal(recDesc.Body.Bytes(), &descOut))
	assert.NotEmpty(t, descOut["NotebookInstanceStatus"])
}

// ---------------------------------------------------------------------------
// Backend-level tests for functions not accessible via HTTP stubs
// ---------------------------------------------------------------------------

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

func TestBackend_FeatureStore_Direct(t *testing.T) {
	t.Parallel()

	b := sagemaker.NewInMemoryBackend("000000000000", "us-east-1")

	// Create feature group with an identifier field.
	_, err := b.CreateFeatureGroup(context.Background(), "direct-fg", "id", "event_time", nil, nil)
	require.NoError(t, err)

	// PutRecord.
	err = b.PutRecord(context.Background(), "direct-fg", map[string]string{
		"id":         "rec-1",
		"event_time": "2024-01-01T00:00:00Z",
		"value":      "42",
	})
	require.NoError(t, err)

	// GetRecord.
	rec, err := b.GetRecord(context.Background(), "direct-fg", "rec-1", nil)
	require.NoError(t, err)
	assert.Equal(t, "rec-1", rec.Record["id"])

	// BatchGetRecord.
	results := b.BatchGetRecord(context.Background(), []struct {
		FeatureGroupName              string
		RecordIdentifierValueAsString string
		FeatureNames                  []string
	}{
		{FeatureGroupName: "direct-fg", RecordIdentifierValueAsString: "rec-1"},
	})
	assert.Len(t, results, 1)
	assert.Empty(t, results[0].ErrorCode)

	// DeleteRecord.
	err = b.DeleteRecord(context.Background(), "direct-fg", "rec-1")
	require.NoError(t, err)

	// Record should be gone.
	_, err = b.GetRecord(context.Background(), "direct-fg", "rec-1", nil)
	require.Error(t, err)
}

func TestBackend_FeatureMetadata_Direct(t *testing.T) {
	t.Parallel()

	b := sagemaker.NewInMemoryBackend("000000000000", "us-east-1")

	// Create feature group.
	_, err := b.CreateFeatureGroup(context.Background(), "meta-fg", "id", "event_time", []sagemaker.FeatureDefinition{
		{FeatureName: "id", FeatureType: "Integral"},
		{FeatureName: "event_time", FeatureType: "String"},
	}, nil)
	require.NoError(t, err)

	// UpdateFeatureMetadata.
	err = b.UpdateFeatureMetadata(context.Background(), "meta-fg", "id", "The record identifier", nil)
	require.NoError(t, err)

	// GetFeatureMetadata.
	meta, err := b.GetFeatureMetadata(context.Background(), "meta-fg", "id")
	require.NoError(t, err)
	assert.Equal(t, "The record identifier", meta.Description)
}

func TestBackend_Persistence_SnapshotRestore(t *testing.T) {
	t.Parallel()

	b := sagemaker.NewInMemoryBackend("000000000000", "us-east-1")

	// Create some resources.
	_, err := b.CreateModel(context.Background(), "snap-model", "arn:aws:iam::000000000000:role/test", nil, nil, nil)
	require.NoError(t, err)

	// Snapshot.
	snap := b.Snapshot()
	require.NotNil(t, snap)

	// Destroy some state.
	b.Reset()

	// Verify gone.
	_, err = b.DescribeModel(context.Background(), "snap-model")
	require.Error(t, err)

	// Restore.
	restErr := b.Restore(snap)
	require.NoError(t, restErr)

	// Verify restored.
	_, err = b.DescribeModel(context.Background(), "snap-model")
	assert.NoError(t, err)
}
