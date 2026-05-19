package bedrock_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/bedrock"
)

// --- EvaluationJob tests ---

func TestHandler_GetEvaluationJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/evaluation-jobs", map[string]any{"jobName": "my-eval-job"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var created map[string]any
	mustUnmarshal(t, rec, &created)
	jobARN := created["jobArn"].(string)

	rec2 := doRequest(t, h, http.MethodGet, "/evaluation-jobs/"+url.PathEscape(jobARN), nil)
	assert.Equal(t, http.StatusOK, rec2.Code)

	var out map[string]any
	mustUnmarshal(t, rec2, &out)
	assert.Equal(t, jobARN, out["jobArn"])
	assert.Equal(t, "my-eval-job", out["jobName"])
	assert.NotEmpty(t, out[keyStatus])
}

func TestHandler_GetEvaluationJob_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	path := "/evaluation-jobs/" + url.PathEscape("arn:aws:bedrock:us-east-1::evaluation-job/nope")
	rec := doRequest(t, h, http.MethodGet, path, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_ListEvaluationJobs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodGet, "/evaluation-jobs", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	assert.Empty(t, out["jobSummaries"])

	doRequest(t, h, http.MethodPost, "/evaluation-jobs", map[string]any{"jobName": "job-a"})
	doRequest(t, h, http.MethodPost, "/evaluation-jobs", map[string]any{"jobName": "job-b"})

	rec2 := doRequest(t, h, http.MethodGet, "/evaluation-jobs", nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var out2 map[string]any
	mustUnmarshal(t, rec2, &out2)
	assert.Len(t, out2["jobSummaries"], 2)
}

func TestHandler_StopEvaluationJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/evaluation-jobs", map[string]any{"jobName": "stop-me"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var created map[string]any
	mustUnmarshal(t, rec, &created)
	jobARN := created["jobArn"].(string)

	rec2 := doRequest(t, h, http.MethodDelete, "/evaluation-jobs/"+url.PathEscape(jobARN), nil)
	assert.Equal(t, http.StatusOK, rec2.Code)

	rec3 := doRequest(t, h, http.MethodGet, "/evaluation-jobs/"+url.PathEscape(jobARN), nil)
	var out map[string]any
	mustUnmarshal(t, rec3, &out)
	assert.Equal(t, "Stopped", out[keyStatus])
}

// --- ARP CRUD tests ---

func TestHandler_GetAutomatedReasoningPolicy(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/automated-reasoning-policies", map[string]any{"name": "my-arp"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var created map[string]any
	mustUnmarshal(t, rec, &created)
	policyARN := created["policyArn"].(string)

	rec2 := doRequest(t, h, http.MethodGet, "/automated-reasoning-policies/"+url.PathEscape(policyARN), nil)
	assert.Equal(t, http.StatusOK, rec2.Code)

	var out map[string]any
	mustUnmarshal(t, rec2, &out)
	assert.Equal(t, policyARN, out["policyArn"])
	assert.Equal(t, "my-arp", out["name"])
}

func TestHandler_GetAutomatedReasoningPolicy_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/automated-reasoning-policies/"+url.PathEscape("arn:aws:bedrock:us-east-1::arp/nope"), nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_ListAutomatedReasoningPolicies(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodGet, "/automated-reasoning-policies", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	assert.Empty(t, out["automatedReasoningPolicies"])

	doRequest(t, h, http.MethodPost, "/automated-reasoning-policies", map[string]any{"name": "pol-a"})
	doRequest(t, h, http.MethodPost, "/automated-reasoning-policies", map[string]any{"name": "pol-b"})

	rec2 := doRequest(t, h, http.MethodGet, "/automated-reasoning-policies", nil)
	var out2 map[string]any
	mustUnmarshal(t, rec2, &out2)
	assert.Len(t, out2["automatedReasoningPolicies"], 2)
}

func TestHandler_UpdateAutomatedReasoningPolicy(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/automated-reasoning-policies", map[string]any{"name": "upd-pol"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var created map[string]any
	mustUnmarshal(t, rec, &created)
	policyARN := created["policyArn"].(string)

	rec2 := doRequest(t, h, http.MethodPut, "/automated-reasoning-policies/"+url.PathEscape(policyARN),
		map[string]any{"description": "new-desc"})
	assert.Equal(t, http.StatusOK, rec2.Code)
}

func TestHandler_DeleteAutomatedReasoningPolicy(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/automated-reasoning-policies", map[string]any{"name": "del-pol"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var created map[string]any
	mustUnmarshal(t, rec, &created)
	policyARN := created["policyArn"].(string)

	rec2 := doRequest(t, h, http.MethodDelete, "/automated-reasoning-policies/"+url.PathEscape(policyARN), nil)
	assert.Equal(t, http.StatusNoContent, rec2.Code)

	rec3 := doRequest(t, h, http.MethodGet, "/automated-reasoning-policies/"+url.PathEscape(policyARN), nil)
	assert.Equal(t, http.StatusNotFound, rec3.Code)
}

// --- ARP Build Workflow tests ---

func TestHandler_StartARPBuildWorkflow(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/automated-reasoning-policies", map[string]any{"name": "wf-pol"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var created map[string]any
	mustUnmarshal(t, rec, &created)
	policyARN := created["policyArn"].(string)

	rec2 := doRequest(t, h, http.MethodPost,
		"/automated-reasoning-policies/"+url.PathEscape(policyARN)+"/build-workflows", nil)
	assert.Equal(t, http.StatusCreated, rec2.Code)

	var wf map[string]any
	mustUnmarshal(t, rec2, &wf)
	assert.NotEmpty(t, wf["buildWorkflowId"])
}

func TestHandler_GetListDeleteARPBuildWorkflow(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/automated-reasoning-policies", map[string]any{"name": "wf2-pol"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var created map[string]any
	mustUnmarshal(t, rec, &created)
	policyARN := created["policyArn"].(string)

	recWF := doRequest(t, h, http.MethodPost,
		"/automated-reasoning-policies/"+url.PathEscape(policyARN)+"/build-workflows", nil)
	require.Equal(t, http.StatusCreated, recWF.Code)

	var wf map[string]any
	mustUnmarshal(t, recWF, &wf)
	wfID := wf["buildWorkflowId"].(string)

	// Get
	recGet := doRequest(t, h, http.MethodGet,
		"/automated-reasoning-policies/"+url.PathEscape(policyARN)+"/build-workflows/"+wfID, nil)
	assert.Equal(t, http.StatusOK, recGet.Code)

	// List
	recList := doRequest(t, h, http.MethodGet,
		"/automated-reasoning-policies/"+url.PathEscape(policyARN)+"/build-workflows", nil)
	require.Equal(t, http.StatusOK, recList.Code)

	var listOut map[string]any
	mustUnmarshal(t, recList, &listOut)
	assert.Len(t, listOut["buildWorkflows"], 1)

	// Delete
	recDel := doRequest(t, h, http.MethodDelete,
		"/automated-reasoning-policies/"+url.PathEscape(policyARN)+"/build-workflows/"+wfID, nil)
	assert.Equal(t, http.StatusNoContent, recDel.Code)

	// List after delete
	recList2 := doRequest(t, h, http.MethodGet,
		"/automated-reasoning-policies/"+url.PathEscape(policyARN)+"/build-workflows", nil)
	var listOut2 map[string]any
	mustUnmarshal(t, recList2, &listOut2)
	assert.Empty(t, listOut2["buildWorkflows"])
}

// --- ARP Test Case tests ---

func TestHandler_GetListDeleteARPTestCase(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/automated-reasoning-policies", map[string]any{"name": "tc-pol"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var created map[string]any
	mustUnmarshal(t, rec, &created)
	policyARN := created["policyArn"].(string)

	recTC := doRequest(t, h, http.MethodPost,
		"/automated-reasoning-policies/"+url.PathEscape(policyARN)+"/test-cases", nil)
	require.Equal(t, http.StatusCreated, recTC.Code)

	var tc map[string]any
	mustUnmarshal(t, recTC, &tc)
	tcID := tc["testCaseId"].(string)

	// Get
	recGet := doRequest(t, h, http.MethodGet,
		"/automated-reasoning-policies/"+url.PathEscape(policyARN)+"/test-cases/"+tcID, nil)
	assert.Equal(t, http.StatusOK, recGet.Code)

	// List
	recList := doRequest(t, h, http.MethodGet,
		"/automated-reasoning-policies/"+url.PathEscape(policyARN)+"/test-cases", nil)
	require.Equal(t, http.StatusOK, recList.Code)

	var listOut map[string]any
	mustUnmarshal(t, recList, &listOut)
	assert.Len(t, listOut["testCases"], 1)

	// Update
	recUpd := doRequest(t, h, http.MethodPut,
		"/automated-reasoning-policies/"+url.PathEscape(policyARN)+"/test-cases/"+tcID, nil)
	assert.Equal(t, http.StatusOK, recUpd.Code)

	// Delete
	recDel := doRequest(t, h, http.MethodDelete,
		"/automated-reasoning-policies/"+url.PathEscape(policyARN)+"/test-cases/"+tcID, nil)
	assert.Equal(t, http.StatusNoContent, recDel.Code)

	// Get after delete
	recGet2 := doRequest(t, h, http.MethodGet,
		"/automated-reasoning-policies/"+url.PathEscape(policyARN)+"/test-cases/"+tcID, nil)
	assert.Equal(t, http.StatusNotFound, recGet2.Code)
}

// --- ModelInvocationJob tests ---

func TestHandler_ModelInvocationJob_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create
	rec := doRequest(t, h, http.MethodPost, "/model-invocation-jobs", map[string]any{"jobName": "inv-job-1"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var created map[string]any
	mustUnmarshal(t, rec, &created)
	jobARN := created["jobArn"].(string)
	assert.NotEmpty(t, jobARN)

	// Get
	rec2 := doRequest(t, h, http.MethodGet, "/model-invocation-jobs/"+url.PathEscape(jobARN), nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var out map[string]any
	mustUnmarshal(t, rec2, &out)
	assert.Equal(t, jobARN, out["jobArn"])
	assert.Equal(t, "inv-job-1", out["jobName"])

	// List
	rec3 := doRequest(t, h, http.MethodGet, "/model-invocation-jobs", nil)
	require.Equal(t, http.StatusOK, rec3.Code)

	var listOut map[string]any
	mustUnmarshal(t, rec3, &listOut)
	assert.Len(t, listOut["invocationJobSummaries"], 1)

	// Stop
	rec4 := doRequest(t, h, http.MethodDelete, "/model-invocation-jobs/"+url.PathEscape(jobARN), nil)
	assert.Equal(t, http.StatusNoContent, rec4.Code)

	// Verify stopped
	rec5 := doRequest(t, h, http.MethodGet, "/model-invocation-jobs/"+url.PathEscape(jobARN), nil)
	var stopped map[string]any
	mustUnmarshal(t, rec5, &stopped)
	assert.Equal(t, "Stopped", stopped[keyStatus])
}

func TestHandler_ModelInvocationJob_MissingName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model-invocation-jobs", map[string]any{"jobName": ""})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ModelInvocationJob_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/model-invocation-jobs", map[string]any{"jobName": "dup-job"})
	rec := doRequest(t, h, http.MethodPost, "/model-invocation-jobs", map[string]any{"jobName": "dup-job"})
	assert.Equal(t, http.StatusConflict, rec.Code)
}

// --- ImportedModel tests ---

func TestHandler_ImportedModel_ListAndGet(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	h := bedrock.NewHandler(b)

	// List before any imports
	rec := doRequest(t, h, http.MethodGet, "/imported-models", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	assert.Empty(t, out["modelSummaries"])

	// Create an import job (which seeds an imported model ARN)
	job, err := b.CreateModelImportJob("test-import", nil)
	require.NoError(t, err)

	// Advance so imported model ARN is set
	b.AdvanceCopyImportJobStatuses(0)

	// Fetch updated job
	updated, err := b.GetModelImportJob(job.JobArn)
	require.NoError(t, err)
	modelARN := updated.ImportedModelArn

	// List
	rec2 := doRequest(t, h, http.MethodGet, "/imported-models", nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var out2 map[string]any
	mustUnmarshal(t, rec2, &out2)
	summaries := out2["modelSummaries"].([]any)
	assert.Len(t, summaries, 1)

	// Get
	rec3 := doRequest(t, h, http.MethodGet, "/imported-models/"+url.PathEscape(modelARN), nil)
	assert.Equal(t, http.StatusOK, rec3.Code)

	// Delete
	rec4 := doRequest(t, h, http.MethodDelete, "/imported-models/"+url.PathEscape(modelARN), nil)
	assert.Equal(t, http.StatusNoContent, rec4.Code)

	// Get after delete
	rec5 := doRequest(t, h, http.MethodGet, "/imported-models/"+url.PathEscape(modelARN), nil)
	assert.Equal(t, http.StatusNotFound, rec5.Code)
}

// --- PromptRouter tests ---

func TestHandler_PromptRouter_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create
	rec := doRequest(t, h, http.MethodPost, "/prompt-routers", map[string]any{"promptRouterName": "my-router"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var created map[string]any
	mustUnmarshal(t, rec, &created)
	routerARN := created["promptRouterArn"].(string)
	assert.NotEmpty(t, routerARN)

	// Get
	rec2 := doRequest(t, h, http.MethodGet, "/prompt-routers/"+url.PathEscape(routerARN), nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var out map[string]any
	mustUnmarshal(t, rec2, &out)
	assert.Equal(t, routerARN, out["promptRouterArn"])

	// List
	rec3 := doRequest(t, h, http.MethodGet, "/prompt-routers", nil)
	require.Equal(t, http.StatusOK, rec3.Code)

	var listOut map[string]any
	mustUnmarshal(t, rec3, &listOut)
	assert.Len(t, listOut["promptRouters"], 1)

	// Delete
	rec4 := doRequest(t, h, http.MethodDelete, "/prompt-routers/"+url.PathEscape(routerARN), nil)
	assert.Equal(t, http.StatusNoContent, rec4.Code)

	// Get after delete
	rec5 := doRequest(t, h, http.MethodGet, "/prompt-routers/"+url.PathEscape(routerARN), nil)
	assert.Equal(t, http.StatusNotFound, rec5.Code)
}

func TestHandler_PromptRouter_MissingName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/prompt-routers", map[string]any{"promptRouterName": ""})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- CustomModelDeployment tests ---

func TestHandler_CustomModelDeployment_GetListUpdateDelete(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create via the existing path
	rec := doRequest(t, h, http.MethodPost, "/model-customization/custom-model-deployments",
		map[string]any{"modelArn": "arn:aws:bedrock:us-east-1::custom-model/m1", "modelDeploymentName": "deploy-1"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var created map[string]any
	mustUnmarshal(t, rec, &created)
	deployARN := created["customModelDeploymentArn"].(string)
	assert.NotEmpty(t, deployARN)

	// List
	rec2 := doRequest(t, h, http.MethodGet, "/custom-model-deployments", nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var listOut map[string]any
	mustUnmarshal(t, rec2, &listOut)
	assert.Len(t, listOut["deploymentSummaries"], 1)

	// Get
	rec3 := doRequest(t, h, http.MethodGet, "/custom-model-deployments/"+url.PathEscape(deployARN), nil)
	require.Equal(t, http.StatusOK, rec3.Code)

	var getOut map[string]any
	mustUnmarshal(t, rec3, &getOut)
	assert.Equal(t, deployARN, getOut["customModelDeploymentArn"])

	// Update
	rec4 := doRequest(t, h, http.MethodPatch, "/custom-model-deployments/"+url.PathEscape(deployARN), nil)
	assert.Equal(t, http.StatusOK, rec4.Code)

	// Delete
	rec5 := doRequest(t, h, http.MethodDelete, "/custom-model-deployments/"+url.PathEscape(deployARN), nil)
	assert.Equal(t, http.StatusNoContent, rec5.Code)

	// Get after delete
	rec6 := doRequest(t, h, http.MethodGet, "/custom-model-deployments/"+url.PathEscape(deployARN), nil)
	assert.Equal(t, http.StatusNotFound, rec6.Code)
}

// --- FoundationModelAgreement tests ---

func TestHandler_FoundationModelAgreement_ListAndDelete(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// List before any agreements
	rec := doRequest(t, h, http.MethodGet, "/foundation-model-agreement-offers", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	assert.Empty(t, out["modelAgreementOffers"])

	// Create an agreement
	doRequest(t, h, http.MethodPost, "/create-foundation-model-agreement",
		map[string]any{"modelId": "amazon.titan-text-express-v1"})

	// List now has one
	rec2 := doRequest(t, h, http.MethodGet, "/foundation-model-agreement-offers", nil)
	var out2 map[string]any
	mustUnmarshal(t, rec2, &out2)
	assert.Len(t, out2["modelAgreementOffers"], 1)
}

// --- UseCaseForModelAccess tests ---

func TestHandler_UseCaseForModelAccess(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Get default
	rec := doRequest(t, h, http.MethodGet, "/usecase-for-model-access", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	assert.Empty(t, out["useCaseType"])

	// Put
	rec2 := doRequest(t, h, http.MethodPut, "/usecase-for-model-access",
		map[string]any{"useCaseType": "RESEARCH", "useCaseDescription": "ML research"})
	assert.Equal(t, http.StatusNoContent, rec2.Code)

	// Get after put
	rec3 := doRequest(t, h, http.MethodGet, "/usecase-for-model-access", nil)
	var out3 map[string]any
	mustUnmarshal(t, rec3, &out3)
	assert.Equal(t, "RESEARCH", out3["useCaseType"])
	assert.Equal(t, "ML research", out3["useCaseDescription"])
}

// --- EnforcedGuardrailConfiguration tests ---

func TestHandler_EnforcedGuardrailConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// List empty
	rec := doRequest(t, h, http.MethodGet, "/enforced-guardrail-configuration", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	assert.Empty(t, out["enforcedGuardrailConfigurations"])

	// Put
	rec2 := doRequest(t, h, http.MethodPut, "/enforced-guardrail-configuration",
		map[string]any{"guardrailId": "g-001", "guardrailVersion": "DRAFT"})
	assert.Equal(t, http.StatusNoContent, rec2.Code)

	// List has one
	rec3 := doRequest(t, h, http.MethodGet, "/enforced-guardrail-configuration", nil)
	var out3 map[string]any
	mustUnmarshal(t, rec3, &out3)
	assert.Len(t, out3["enforcedGuardrailConfigurations"], 1)

	// Delete
	rec4 := doRequest(t, h, http.MethodDelete, "/enforced-guardrail-configuration?guardrailId=g-001", nil)
	assert.Equal(t, http.StatusNoContent, rec4.Code)

	// List empty again
	rec5 := doRequest(t, h, http.MethodGet, "/enforced-guardrail-configuration", nil)
	var out5 map[string]any
	mustUnmarshal(t, rec5, &out5)
	assert.Empty(t, out5["enforcedGuardrailConfigurations"])
}

// keyStatus is duplicated here to avoid an import cycle; it must match the package constant.
const keyStatus = "status"
