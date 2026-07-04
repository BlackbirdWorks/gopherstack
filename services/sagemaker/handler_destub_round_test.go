package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// ModelPackageGroup resource policy
// ---------------------------------------------------------------------------

func TestHandler_ModelPackageGroupPolicy_Lifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateModelPackageGroup", map[string]any{
		"ModelPackageGroupName": "policy-group",
	})

	// No policy attached yet.
	recGet := doSageMakerRequest(t, h, "GetModelPackageGroupPolicy", map[string]any{
		"ModelPackageGroupName": "policy-group",
	})
	assert.Equal(t, http.StatusBadRequest, recGet.Code)

	policy := `{"Version":"2012-10-17","Statement":[]}`
	recPut := doSageMakerRequest(t, h, "PutModelPackageGroupPolicy", map[string]any{
		"ModelPackageGroupName": "policy-group",
		"ResourcePolicy":        policy,
	})
	require.Equal(t, http.StatusOK, recPut.Code)

	var putOut map[string]any
	require.NoError(t, json.Unmarshal(recPut.Body.Bytes(), &putOut))
	assert.NotEmpty(t, putOut["ModelPackageGroupArn"])

	recGet2 := doSageMakerRequest(t, h, "GetModelPackageGroupPolicy", map[string]any{
		"ModelPackageGroupName": "policy-group",
	})
	require.Equal(t, http.StatusOK, recGet2.Code)

	var getOut map[string]any
	require.NoError(t, json.Unmarshal(recGet2.Body.Bytes(), &getOut))
	assert.Equal(t, policy, getOut["ResourcePolicy"])

	recDelete := doSageMakerRequest(t, h, "DeleteModelPackageGroupPolicy", map[string]any{
		"ModelPackageGroupName": "policy-group",
	})
	require.Equal(t, http.StatusOK, recDelete.Code)

	recGet3 := doSageMakerRequest(t, h, "GetModelPackageGroupPolicy", map[string]any{
		"ModelPackageGroupName": "policy-group",
	})
	assert.Equal(t, http.StatusBadRequest, recGet3.Code)
}

func TestHandler_ModelPackageGroupPolicy_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tests := []struct {
		body map[string]any
		op   string
	}{
		{op: "GetModelPackageGroupPolicy", body: map[string]any{"ModelPackageGroupName": "no-such-group"}},
		{
			op: "PutModelPackageGroupPolicy",
			body: map[string]any{
				"ModelPackageGroupName": "no-such-group", "ResourcePolicy": "{}",
			},
		},
		{op: "DeleteModelPackageGroupPolicy", body: map[string]any{"ModelPackageGroupName": "no-such-group"}},
	}

	for _, tc := range tests {
		t.Run(tc.op, func(t *testing.T) {
			t.Parallel()

			rec := doSageMakerRequest(t, h, tc.op, tc.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// ---------------------------------------------------------------------------
// Service Catalog portfolio toggle
// ---------------------------------------------------------------------------

func TestHandler_ServicecatalogPortfolio_Toggle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	assertStatus := func(want string) {
		rec := doSageMakerRequest(t, h, "GetSagemakerServicecatalogPortfolioStatus", map[string]any{})
		require.Equal(t, http.StatusOK, rec.Code)

		var out map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		assert.Equal(t, want, out["Status"])
	}

	assertStatus("Disabled")

	rec := doSageMakerRequest(t, h, "EnableSagemakerServicecatalogPortfolio", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	assertStatus("Enabled")

	rec = doSageMakerRequest(t, h, "DisableSagemakerServicecatalogPortfolio", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	assertStatus("Disabled")
}

// ---------------------------------------------------------------------------
// ListResourceCatalogs
// ---------------------------------------------------------------------------

func TestHandler_ListResourceCatalogs_Empty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "ListResourceCatalogs", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Empty(t, out["ResourceCatalogs"])
}

// ---------------------------------------------------------------------------
// TrialComponent association extras
// ---------------------------------------------------------------------------

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

func TestHandler_ListAliases(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateImage", map[string]any{"ImageName": "alias-img"})
	doSageMakerRequest(t, h, "CreateImageVersion", map[string]any{"ImageName": "alias-img"})
	doSageMakerRequest(t, h, "UpdateImageVersion", map[string]any{
		"ImageName": "alias-img", "AliasesToAdd": []string{"latest", "stable"},
	})

	rec := doSageMakerRequest(t, h, "ListAliases", map[string]any{"ImageName": "alias-img"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	aliases, _ := out["SageMakerImageVersionAliases"].([]any)
	assert.ElementsMatch(t, []any{"latest", "stable"}, aliases)
}

func TestHandler_ListAliases_ImageNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "ListAliases", map[string]any{"ImageName": "no-such-image"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// UpdateProject
// ---------------------------------------------------------------------------

func TestHandler_UpdateProject(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateProject", map[string]any{"ProjectName": "proj-a"})

	rec := doSageMakerRequest(t, h, "UpdateProject", map[string]any{
		"ProjectName": "proj-a", "ProjectDescription": "updated description",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	recDescribe := doSageMakerRequest(t, h, "DescribeProject", map[string]any{"ProjectName": "proj-a"})
	require.Equal(t, http.StatusOK, recDescribe.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(recDescribe.Body.Bytes(), &out))
	assert.Equal(t, "updated description", out["ProjectDescription"])
}

func TestHandler_UpdateProject_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "UpdateProject", map[string]any{"ProjectName": "no-such-project"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// Pipeline versions and execution-definition extras
// ---------------------------------------------------------------------------

func TestHandler_PipelineVersions_Lifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreatePipeline", map[string]any{
		"PipelineName": "ver-pipeline", "PipelineDefinition": `{"Version":"v1"}`,
	})

	recList := doSageMakerRequest(t, h, "ListPipelineVersions", map[string]any{"PipelineName": "ver-pipeline"})
	require.Equal(t, http.StatusOK, recList.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &listOut))
	versions, _ := listOut["PipelineVersionSummaries"].([]any)
	require.Len(t, versions, 1)
	assert.InDelta(t, float64(1), versions[0].(map[string]any)["PipelineVersionId"], 0)

	doSageMakerRequest(t, h, "UpdatePipeline", map[string]any{
		"PipelineName": "ver-pipeline", "PipelineDefinition": `{"Version":"v2"}`,
	})

	recList2 := doSageMakerRequest(t, h, "ListPipelineVersions", map[string]any{"PipelineName": "ver-pipeline"})
	require.Equal(t, http.StatusOK, recList2.Code)

	var listOut2 map[string]any
	require.NoError(t, json.Unmarshal(recList2.Body.Bytes(), &listOut2))
	versions2, _ := listOut2["PipelineVersionSummaries"].([]any)
	require.Len(t, versions2, 2)
	// Newest first.
	assert.InDelta(t, float64(2), versions2[0].(map[string]any)["PipelineVersionId"], 0)

	recDescribe := doSageMakerRequest(t, h, "DescribePipeline", map[string]any{"PipelineName": "ver-pipeline"})
	var pipelineOut map[string]any
	require.NoError(t, json.Unmarshal(recDescribe.Body.Bytes(), &pipelineOut))
	pipelineArn, _ := pipelineOut["PipelineArn"].(string)
	require.NotEmpty(t, pipelineArn)

	recUpdateVersion := doSageMakerRequest(t, h, "UpdatePipelineVersion", map[string]any{
		"PipelineArn": pipelineArn, "PipelineVersionId": 1, "PipelineVersionDescription": "first cut",
	})
	require.Equal(t, http.StatusOK, recUpdateVersion.Code)

	recStart := doSageMakerRequest(t, h, "StartPipelineExecution", map[string]any{"PipelineName": "ver-pipeline"})
	require.Equal(t, http.StatusOK, recStart.Code)

	var startOut map[string]any
	require.NoError(t, json.Unmarshal(recStart.Body.Bytes(), &startOut))
	execArn, _ := startOut["PipelineExecutionArn"].(string)
	require.NotEmpty(t, execArn)

	recDefinition := doSageMakerRequest(t, h, "DescribePipelineDefinitionForExecution", map[string]any{
		"PipelineExecutionArn": execArn,
	})
	require.Equal(t, http.StatusOK, recDefinition.Code)

	var defOut map[string]any
	require.NoError(t, json.Unmarshal(recDefinition.Body.Bytes(), &defOut))
	assert.JSONEq(t, `{"Version":"v2"}`, defOut["PipelineDefinition"].(string))

	recUpdateExec := doSageMakerRequest(t, h, "UpdatePipelineExecution", map[string]any{
		"PipelineExecutionArn": execArn, "PipelineExecutionDescription": "manual re-run",
	})
	require.Equal(t, http.StatusOK, recUpdateExec.Code)

	var updateExecOut map[string]any
	require.NoError(t, json.Unmarshal(recUpdateExec.Body.Bytes(), &updateExecOut))
	assert.Equal(t, execArn, updateExecOut["PipelineExecutionArn"])
}

func TestHandler_PipelineVersions_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tests := []struct {
		body map[string]any
		op   string
	}{
		{op: "ListPipelineVersions", body: map[string]any{"PipelineName": "no-such-pipeline"}},
		{
			op: "UpdatePipelineVersion",
			body: map[string]any{
				"PipelineArn":       "arn:aws:sagemaker:us-east-1:000000000000:pipeline/no-such-pipeline",
				"PipelineVersionId": 1,
			},
		},
		{
			op:   "DescribePipelineDefinitionForExecution",
			body: map[string]any{"PipelineExecutionArn": "no-such-execution"},
		},
		{op: "UpdatePipelineExecution", body: map[string]any{"PipelineExecutionArn": "no-such-execution"}},
	}

	for _, tc := range tests {
		t.Run(tc.op, func(t *testing.T) {
			t.Parallel()

			rec := doSageMakerRequest(t, h, tc.op, tc.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// ---------------------------------------------------------------------------
// InferenceExperiment Start/Update
// ---------------------------------------------------------------------------

func TestHandler_InferenceExperiment_StartAndUpdate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateInferenceExperiment", map[string]any{"Name": "inf-exp"})

	recStart := doSageMakerRequest(t, h, "StartInferenceExperiment", map[string]any{"Name": "inf-exp"})
	require.Equal(t, http.StatusOK, recStart.Code)

	recUpdate := doSageMakerRequest(t, h, "UpdateInferenceExperiment", map[string]any{
		"Name": "inf-exp", "Description": "updated",
	})
	require.Equal(t, http.StatusOK, recUpdate.Code)

	recDescribe := doSageMakerRequest(t, h, "DescribeInferenceExperiment", map[string]any{"Name": "inf-exp"})
	var out map[string]any
	require.NoError(t, json.Unmarshal(recDescribe.Body.Bytes(), &out))
	assert.Equal(t, "Running", out["Status"])
	assert.Equal(t, "updated", out["Description"])
}

func TestHandler_InferenceExperiment_StartAndUpdate_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, op := range []string{"StartInferenceExperiment", "UpdateInferenceExperiment"} {
		t.Run(op, func(t *testing.T) {
			t.Parallel()

			rec := doSageMakerRequest(t, h, op, map[string]any{"Name": "no-such-experiment"})
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// ---------------------------------------------------------------------------
// HubContent Update/UpdateReference
// ---------------------------------------------------------------------------

func TestHandler_UpdateHubContent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateHub", map[string]any{"HubName": "my-hub2", "HubDescription": "test hub"})
	doSageMakerRequest(t, h, "ImportHubContent", map[string]any{
		"HubName": "my-hub2", "HubContentName": "model-a", "HubContentType": "Model",
		"HubContentVersion": "1.0.0", "DocumentSchemaVersion": "1.0.0", "HubContentDocument": "{}",
	})

	rec := doSageMakerRequest(t, h, "UpdateHubContent", map[string]any{
		"HubName": "my-hub2", "HubContentName": "model-a", "HubContentType": "Model",
		"HubContentVersion": "1.0.0", "HubContentDescription": "new description",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	recDescribe := doSageMakerRequest(t, h, "DescribeHubContent", map[string]any{
		"HubName": "my-hub2", "HubContentName": "model-a", "HubContentType": "Model",
	})
	var out map[string]any
	require.NoError(t, json.Unmarshal(recDescribe.Body.Bytes(), &out))
	assert.Equal(t, "new description", out["HubContentDescription"])
}

func TestHandler_UpdateHubContentReference(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateHub", map[string]any{"HubName": "my-hub3", "HubDescription": "test hub"})
	doSageMakerRequest(t, h, "CreateHubContentReference", map[string]any{
		"HubName": "my-hub3",
		"SageMakerPublicHubContentArn": "arn:aws:sagemaker:us-east-1:aws:hub-content/" +
			"SageMakerPublicHub/Model/public-model/1.0.0",
	})

	rec := doSageMakerRequest(t, h, "UpdateHubContentReference", map[string]any{
		"HubName": "my-hub3", "HubContentName": "public-model", "HubContentType": "ModelReference",
		"MinVersion": "2.0.0",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.NotEmpty(t, out["HubContentArn"])
}

func TestHandler_UpdateHubContent_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "UpdateHubContent", map[string]any{
		"HubName": "no-such-hub", "HubContentName": "x", "HubContentType": "Model", "HubContentVersion": "1.0.0",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// CreatePresignedDomainUrl
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

func TestHandler_RenderUiTemplate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "RenderUiTemplate", map[string]any{
		"RoleArn": "arn:aws:iam::000000000000:role/test",
		"Task":    map[string]any{"Input": `{"text":"hello world"}`},
		"UiTemplate": map[string]any{
			"Content": "<p>{{ task.input.text }}</p>",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "<p>hello world</p>", out["RenderedContent"])
	assert.Empty(t, out["Errors"])
}

func TestHandler_RenderUiTemplate_MissingRoleArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "RenderUiTemplate", map[string]any{
		"Task":       map[string]any{"Input": `{}`},
		"UiTemplate": map[string]any{"Content": "<p></p>"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_StartSession(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "StartSession", map[string]any{
		"ResourceIdentifier": "arn:aws:sagemaker:us-east-1:000000000000:space/my-domain/my-space",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.NotEmpty(t, out["SessionId"])
	assert.NotEmpty(t, out["StreamUrl"])
	assert.NotEmpty(t, out["TokenValue"])
}

func TestHandler_StartSession_MissingResourceIdentifier(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "StartSession", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// DeleteProcessingJob
// ---------------------------------------------------------------------------

func TestHandler_DeleteProcessingJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateProcessingJob", map[string]any{
		"ProcessingJobName": "del-pj",
		"AppSpecification":  map[string]any{"ImageUri": "img:latest"},
		"ProcessingResources": map[string]any{
			"ClusterConfig": map[string]any{"InstanceType": "ml.m5.large", "InstanceCount": 1, "VolumeSizeInGB": 10},
		},
	})

	// Cannot delete while still InProgress.
	recEarly := doSageMakerRequest(t, h, "DeleteProcessingJob", map[string]any{"ProcessingJobName": "del-pj"})
	assert.Equal(t, http.StatusBadRequest, recEarly.Code)

	// Wait for the simulated job to reach a terminal state.
	time.Sleep(400 * time.Millisecond)

	recDelete := doSageMakerRequest(t, h, "DeleteProcessingJob", map[string]any{"ProcessingJobName": "del-pj"})
	require.Equal(t, http.StatusOK, recDelete.Code)

	recDescribe := doSageMakerRequest(t, h, "DescribeProcessingJob", map[string]any{"ProcessingJobName": "del-pj"})
	assert.Equal(t, http.StatusBadRequest, recDescribe.Code)
}

func TestHandler_DeleteProcessingJob_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "DeleteProcessingJob", map[string]any{"ProcessingJobName": "no-such-job"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// Persistence: the new state introduced by this round of de-stubbing must
// survive a Snapshot/Restore roundtrip.
// ---------------------------------------------------------------------------

func TestHandler_Destub_PersistenceRoundtrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateModelPackageGroup", map[string]any{"ModelPackageGroupName": "persist-group"})
	doSageMakerRequest(t, h, "PutModelPackageGroupPolicy", map[string]any{
		"ModelPackageGroupName": "persist-group", "ResourcePolicy": `{"Version":"2012-10-17"}`,
	})

	doSageMakerRequest(t, h, "EnableSagemakerServicecatalogPortfolio", map[string]any{})

	doSageMakerRequest(t, h, "CreatePipeline", map[string]any{
		"PipelineName": "persist-pipeline", "PipelineDefinition": `{"Version":"v1"}`,
	})
	doSageMakerRequest(t, h, "UpdatePipeline", map[string]any{
		"PipelineName": "persist-pipeline", "PipelineDefinition": `{"Version":"v2"}`,
	})

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	h2 := newTestHandler(t)
	require.NoError(t, h2.Restore(t.Context(), snap))

	recPolicy := doSageMakerRequest(t, h2, "GetModelPackageGroupPolicy", map[string]any{
		"ModelPackageGroupName": "persist-group",
	})
	require.Equal(t, http.StatusOK, recPolicy.Code)

	var policyOut map[string]any
	require.NoError(t, json.Unmarshal(recPolicy.Body.Bytes(), &policyOut))
	assert.JSONEq(t, `{"Version":"2012-10-17"}`, policyOut["ResourcePolicy"].(string))

	recStatus := doSageMakerRequest(t, h2, "GetSagemakerServicecatalogPortfolioStatus", map[string]any{})
	var statusOut map[string]any
	require.NoError(t, json.Unmarshal(recStatus.Body.Bytes(), &statusOut))
	assert.Equal(t, "Enabled", statusOut["Status"])

	recVersions := doSageMakerRequest(t, h2, "ListPipelineVersions", map[string]any{"PipelineName": "persist-pipeline"})
	require.Equal(t, http.StatusOK, recVersions.Code)

	var versionsOut map[string]any
	require.NoError(t, json.Unmarshal(recVersions.Body.Bytes(), &versionsOut))
	versions, _ := versionsOut["PipelineVersionSummaries"].([]any)
	assert.Len(t, versions, 2)
}
