package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// EdgePackagingJob tests
// ---------------------------------------------------------------------------

func TestHandler_EdgePackagingJobLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create
	rec := doSageMakerRequest(t, h, "CreateEdgePackagingJob", map[string]any{
		"EdgePackagingJobName": "my-edge-job",
		"ModelName":            "my-model",
		"ModelVersion":         "1.0",
		"RoleArn":              "arn:aws:iam::000000000000:role/TestRole",
		"CompilationJobName":   "my-comp-job",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	assert.Contains(t, createResp["EdgePackagingJobArn"], "my-edge-job")

	// Describe
	rec = doSageMakerRequest(t, h, "DescribeEdgePackagingJob", map[string]any{
		"EdgePackagingJobName": "my-edge-job",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "my-edge-job", descResp["EdgePackagingJobName"])
	assert.Equal(t, "my-model", descResp["ModelName"])
	assert.Equal(t, "1.0", descResp["ModelVersion"])
	assert.Equal(t, "STARTING", descResp["EdgePackagingJobStatus"])

	// List
	rec = doSageMakerRequest(t, h, "ListEdgePackagingJobs", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	summaries := listResp["EdgePackagingJobSummaries"].([]any)
	assert.Len(t, summaries, 1)

	// Stop
	rec = doSageMakerRequest(t, h, "StopEdgePackagingJob", map[string]any{
		"EdgePackagingJobName": "my-edge-job",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify stopped
	rec = doSageMakerRequest(t, h, "DescribeEdgePackagingJob", map[string]any{
		"EdgePackagingJobName": "my-edge-job",
	})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "STOPPING", descResp["EdgePackagingJobStatus"])
}

func TestHandler_EdgePackagingJob_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "DescribeEdgePackagingJob", map[string]any{
		"EdgePackagingJobName": "nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_EdgePackagingJob_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{"EdgePackagingJobName": "dup-job"}
	doSageMakerRequest(t, h, "CreateEdgePackagingJob", body)

	rec := doSageMakerRequest(t, h, "CreateEdgePackagingJob", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListEdgePackagingJobs_StatusFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateEdgePackagingJob", map[string]any{"EdgePackagingJobName": "job-a"})
	doSageMakerRequest(t, h, "CreateEdgePackagingJob", map[string]any{"EdgePackagingJobName": "job-b"})
	doSageMakerRequest(t, h, "StopEdgePackagingJob", map[string]any{"EdgePackagingJobName": "job-a"})

	rec := doSageMakerRequest(t, h, "ListEdgePackagingJobs", map[string]any{
		"StatusEquals": "STARTING",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	summaries := resp["EdgePackagingJobSummaries"].([]any)
	assert.Len(t, summaries, 1)
}

// ---------------------------------------------------------------------------
// InferenceRecommendationsJob tests
// ---------------------------------------------------------------------------

func TestHandler_InferenceRecommendationsJobLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create
	rec := doSageMakerRequest(t, h, "CreateInferenceRecommendationsJob", map[string]any{
		"JobName":        "my-rec-job",
		"JobType":        "Default",
		"JobDescription": "Test recommendation job",
		"RoleArn":        "arn:aws:iam::000000000000:role/TestRole",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	assert.Contains(t, createResp["JobArn"], "my-rec-job")

	// Describe
	rec = doSageMakerRequest(t, h, "DescribeInferenceRecommendationsJob", map[string]any{
		"JobName": "my-rec-job",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "my-rec-job", descResp["JobName"])
	assert.Equal(t, "IN_PROGRESS", descResp["Status"])
	assert.Equal(t, "Default", descResp["JobType"])
	recs := descResp["InferenceRecommendations"].([]any)
	assert.Empty(t, recs)

	// List
	rec = doSageMakerRequest(t, h, "ListInferenceRecommendationsJobs", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	summaries := listResp["InferenceRecommendationsJobs"].([]any)
	assert.Len(t, summaries, 1)

	// List steps (always empty)
	rec = doSageMakerRequest(t, h, "ListInferenceRecommendationsJobSteps", map[string]any{
		"JobName": "my-rec-job",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var stepsResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &stepsResp))
	steps := stepsResp["Steps"].([]any)
	assert.Empty(t, steps)

	// Stop
	rec = doSageMakerRequest(t, h, "StopInferenceRecommendationsJob", map[string]any{
		"JobName": "my-rec-job",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeInferenceRecommendationsJob", map[string]any{
		"JobName": "my-rec-job",
	})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "STOPPING", descResp["Status"])
}

func TestHandler_InferenceRecommendationsJob_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "DescribeInferenceRecommendationsJob", map[string]any{
		"JobName": "nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListInferenceRecommendationsJobSteps_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "ListInferenceRecommendationsJobSteps", map[string]any{
		"JobName": "nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// ListMlflowTrackingServers tests
// ---------------------------------------------------------------------------

func TestHandler_ListMlflowTrackingServers(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Initially empty
	rec := doSageMakerRequest(t, h, "ListMlflowTrackingServers", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["TrackingServerSummaries"])

	// Create one
	doSageMakerRequest(t, h, "CreateMlflowTrackingServer", map[string]any{
		"TrackingServerName": "my-server",
		"RoleArn":            "arn:aws:iam::000000000000:role/TestRole",
		"MlflowVersion":      "2.0.0",
	})

	// List shows it
	rec = doSageMakerRequest(t, h, "ListMlflowTrackingServers", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	summaries := resp["TrackingServerSummaries"].([]any)
	assert.Len(t, summaries, 1)

	summary := summaries[0].(map[string]any)
	assert.Equal(t, "my-server", summary["TrackingServerName"])
	assert.Equal(t, "2.0.0", summary["MlflowVersion"])
}

// ---------------------------------------------------------------------------
// UpdateMlflowTrackingServer tests
// ---------------------------------------------------------------------------

func TestHandler_UpdateMlflowTrackingServer(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateMlflowTrackingServer", map[string]any{
		"TrackingServerName": "my-server",
		"RoleArn":            "arn:aws:iam::000000000000:role/TestRole",
		"MlflowVersion":      "2.0.0",
	})

	rec := doSageMakerRequest(t, h, "UpdateMlflowTrackingServer", map[string]any{
		"TrackingServerName": "my-server",
		"MlflowVersion":      "2.1.0",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["TrackingServerArn"])
}

func TestHandler_UpdateMlflowTrackingServer_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "UpdateMlflowTrackingServer", map[string]any{
		"TrackingServerName": "nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// ModelCard list tests
// ---------------------------------------------------------------------------

func TestHandler_ListModelCards(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Empty initially
	rec := doSageMakerRequest(t, h, "ListModelCards", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["ModelCardSummaries"])

	// Create one
	doSageMakerRequest(t, h, "CreateModelCard", map[string]any{
		"ModelCardName":   "my-card",
		"ModelCardStatus": "Draft",
	})

	rec = doSageMakerRequest(t, h, "ListModelCards", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	summaries := resp["ModelCardSummaries"].([]any)
	assert.Len(t, summaries, 1)

	s := summaries[0].(map[string]any)
	assert.Equal(t, "my-card", s["ModelCardName"])
	assert.Equal(t, "Draft", s["ModelCardStatus"])
	assert.EqualValues(t, 1, s["ModelCardVersion"])
}

func TestHandler_ListModelCardVersions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateModelCard", map[string]any{
		"ModelCardName": "my-card",
	})

	rec := doSageMakerRequest(t, h, "ListModelCardVersions", map[string]any{
		"ModelCardName": "my-card",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	versions := resp["ModelCardVersionSummaryList"].([]any)
	assert.Len(t, versions, 1)

	v := versions[0].(map[string]any)
	assert.Equal(t, "my-card", v["ModelCardName"])
	assert.EqualValues(t, 1, v["ModelCardVersion"])
}

func TestHandler_ListModelCardVersions_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "ListModelCardVersions", map[string]any{
		"ModelCardName": "nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListModelCardExportJobs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateModelCard", map[string]any{
		"ModelCardName": "my-card",
	})

	rec := doSageMakerRequest(t, h, "ListModelCardExportJobs", map[string]any{
		"ModelCardName": "my-card",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	jobs := resp["ModelCardExportJobSummaries"].([]any)
	assert.Empty(t, jobs)
}

// ---------------------------------------------------------------------------
// UpdateModelPackage tests
// ---------------------------------------------------------------------------

func TestHandler_UpdateModelPackage(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateModelPackage", map[string]any{
		"ModelPackageName": "my-pkg",
	})

	rec := doSageMakerRequest(t, h, "UpdateModelPackage", map[string]any{
		"ModelPackageName":    "my-pkg",
		"ModelApprovalStatus": "Approved",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["ModelPackageArn"], "my-pkg")

	// Describe and verify approval status
	rec = doSageMakerRequest(t, h, "DescribeModelPackage", map[string]any{
		"ModelPackageName": "my-pkg",
	})
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "Approved", descResp["ModelApprovalStatus"])
}

func TestHandler_UpdateModelPackage_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "UpdateModelPackage", map[string]any{
		"ModelPackageName": "nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// UpdateSpace tests
// ---------------------------------------------------------------------------

func TestHandler_UpdateSpace(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateDomain", map[string]any{
		"DomainName": "my-domain",
		"AuthMode":   "SSO",
	})

	var domainResp map[string]any
	rec := doSageMakerRequest(t, h, "ListDomains", map[string]any{})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &domainResp))
	domains := domainResp["Domains"].([]any)
	require.Len(t, domains, 1)
	domainID := domains[0].(map[string]any)["DomainId"].(string)

	doSageMakerRequest(t, h, "CreateSpace", map[string]any{
		"DomainId":  domainID,
		"SpaceName": "my-space",
	})

	rec = doSageMakerRequest(t, h, "UpdateSpace", map[string]any{
		"DomainId":  domainID,
		"SpaceName": "my-space",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["SpaceArn"])
}

func TestHandler_UpdateSpace_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "UpdateSpace", map[string]any{
		"DomainId":  "d-nonexistent",
		"SpaceName": "no-space",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// UpdateUserProfile tests
// ---------------------------------------------------------------------------

func TestHandler_UpdateUserProfile(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateDomain", map[string]any{
		"DomainName": "my-domain",
		"AuthMode":   "SSO",
	})

	var domainResp map[string]any
	rec := doSageMakerRequest(t, h, "ListDomains", map[string]any{})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &domainResp))
	domains := domainResp["Domains"].([]any)
	require.Len(t, domains, 1)
	domainID := domains[0].(map[string]any)["DomainId"].(string)

	doSageMakerRequest(t, h, "CreateUserProfile", map[string]any{
		"DomainId":        domainID,
		"UserProfileName": "my-user",
	})

	rec = doSageMakerRequest(t, h, "UpdateUserProfile", map[string]any{
		"DomainId":        domainID,
		"UserProfileName": "my-user",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["UserProfileArn"])
}

func TestHandler_UpdateUserProfile_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "UpdateUserProfile", map[string]any{
		"DomainId":        "d-nonexistent",
		"UserProfileName": "no-user",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// Batch3 list operation tests
// ---------------------------------------------------------------------------

func TestHandler_ListOptimizationJobs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Empty initially
	rec := doSageMakerRequest(t, h, "ListOptimizationJobs", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["OptimizationJobSummaries"])

	// Create and list
	doSageMakerRequest(t, h, "CreateOptimizationJob", map[string]any{
		"OptimizationJobName": "my-opt-job",
		"RoleArn":             "arn:aws:iam::000000000000:role/TestRole",
	})

	rec = doSageMakerRequest(t, h, "ListOptimizationJobs", map[string]any{})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	summaries := resp["OptimizationJobSummaries"].([]any)
	assert.Len(t, summaries, 1)
	s := summaries[0].(map[string]any)
	assert.Equal(t, "my-opt-job", s["OptimizationJobName"])
}

func TestHandler_ListStudioLifecycleConfigs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "ListStudioLifecycleConfigs", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["StudioLifecycleConfigs"])

	doSageMakerRequest(t, h, "CreateStudioLifecycleConfig", map[string]any{
		"StudioLifecycleConfigName":    "my-config",
		"StudioLifecycleConfigAppType": "JupyterServer",
	})

	rec = doSageMakerRequest(t, h, "ListStudioLifecycleConfigs", map[string]any{})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	configs := resp["StudioLifecycleConfigs"].([]any)
	assert.Len(t, configs, 1)
	c := configs[0].(map[string]any)
	assert.Equal(t, "my-config", c["StudioLifecycleConfigName"])
}

func TestHandler_ListInferenceExperiments(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "ListInferenceExperiments", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["InferenceExperiments"])

	doSageMakerRequest(t, h, "CreateInferenceExperiment", map[string]any{
		"Name":    "my-exp",
		"Type":    "ShadowMode",
		"RoleArn": "arn:aws:iam::000000000000:role/TestRole",
	})

	rec = doSageMakerRequest(t, h, "ListInferenceExperiments", map[string]any{})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	exps := resp["InferenceExperiments"].([]any)
	assert.Len(t, exps, 1)
	e := exps[0].(map[string]any)
	assert.Equal(t, "my-exp", e["Name"])
}

func TestHandler_ListFlowDefinitions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "ListFlowDefinitions", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["FlowDefinitionSummaries"])

	doSageMakerRequest(t, h, "CreateFlowDefinition", map[string]any{
		"FlowDefinitionName": "my-flow",
		"RoleArn":            "arn:aws:iam::000000000000:role/TestRole",
	})

	rec = doSageMakerRequest(t, h, "ListFlowDefinitions", map[string]any{})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	defs := resp["FlowDefinitionSummaries"].([]any)
	assert.Len(t, defs, 1)
	d := defs[0].(map[string]any)
	assert.Equal(t, "my-flow", d["FlowDefinitionName"])
}

func TestHandler_ListHumanTaskUIs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "ListHumanTaskUis", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["HumanTaskUiSummaries"])

	doSageMakerRequest(t, h, "CreateHumanTaskUi", map[string]any{
		"HumanTaskUiName": "my-ui",
	})

	rec = doSageMakerRequest(t, h, "ListHumanTaskUis", map[string]any{})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	uis := resp["HumanTaskUiSummaries"].([]any)
	assert.Len(t, uis, 1)
	u := uis[0].(map[string]any)
	assert.Equal(t, "my-ui", u["HumanTaskUiName"])
}

func TestHandler_ListAppImageConfigs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "ListAppImageConfigs", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["AppImageConfigs"])

	doSageMakerRequest(t, h, "CreateAppImageConfig", map[string]any{
		"AppImageConfigName": "my-config",
	})

	rec = doSageMakerRequest(t, h, "ListAppImageConfigs", map[string]any{})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	configs := resp["AppImageConfigs"].([]any)
	assert.Len(t, configs, 1)
	c := configs[0].(map[string]any)
	assert.Equal(t, "my-config", c["AppImageConfigName"])
}

// ---------------------------------------------------------------------------
// ListTrainingJobsForHyperParameterTuningJob tests
// ---------------------------------------------------------------------------

func TestHandler_ListTrainingJobsForHyperParameterTuningJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateHyperParameterTuningJob", map[string]any{
		"HyperParameterTuningJobName": "my-hp-job",
		"HyperParameterTuningJobConfig": map[string]any{
			"Strategy": "Bayesian",
		},
	})

	rec := doSageMakerRequest(t, h, "ListTrainingJobsForHyperParameterTuningJob", map[string]any{
		"HyperParameterTuningJobName": "my-hp-job",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	summaries := resp["TrainingJobSummaries"].([]any)
	assert.Empty(t, summaries)
}

func TestHandler_ListTrainingJobsForHyperParameterTuningJob_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "ListTrainingJobsForHyperParameterTuningJob", map[string]any{
		"HyperParameterTuningJobName": "nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
