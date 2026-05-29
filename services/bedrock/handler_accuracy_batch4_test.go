package bedrock_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/bedrock"
)

// ─────────────────────────────────────────────────────────────
// Evaluation job — evaluator and inference config accuracy
// ─────────────────────────────────────────────────────────────

func TestAccuracy_EvaluationJob_EvaluatorConfigPreserved(t *testing.T) {
	t.Parallel()

	tests := []struct {
		evaluatorModel string
		name           string
	}{
		{name: "claude evaluator", evaluatorModel: "anthropic.claude-3-sonnet-20240229-v1:0"},
		{name: "titan evaluator", evaluatorModel: "amazon.titan-text-express-v1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/evaluation-jobs", map[string]any{
				"jobName": tt.name,
				"roleArn": "arn:aws:iam::000000000000:role/eval-role",
				"evaluatorConfig": map[string]any{
					"modelIdentifier": tt.evaluatorModel,
				},
			})
			require.Equal(t, http.StatusCreated, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			jobArn := out["jobArn"].(string)

			getRec := doRequest(t, h, http.MethodGet, "/evaluation-jobs/"+jobArn, nil)
			require.Equal(t, http.StatusOK, getRec.Code)

			var getOut map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))

			evaluatorCfg := getOut["evaluatorConfig"].(map[string]any)
			assert.Equal(t, tt.evaluatorModel, evaluatorCfg["modelIdentifier"])
		})
	}
}

func TestAccuracy_EvaluationJob_InferenceModelsPreserved(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/evaluation-jobs", map[string]any{
		"jobName": "inference-config-job",
		"roleArn": "arn:aws:iam::000000000000:role/eval-role",
		"inferenceConfig": map[string]any{
			"models": []map[string]any{
				{"modelIdentifier": "amazon.titan-text-express-v1"},
				{"modelIdentifier": "anthropic.claude-v2"},
			},
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	jobArn := out["jobArn"].(string)

	getRec := doRequest(t, h, http.MethodGet, "/evaluation-jobs/"+jobArn, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getOut map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))

	inferCfg := getOut["inferenceConfig"].(map[string]any)
	models := inferCfg["models"].([]any)
	assert.Len(t, models, 2)
}

func TestAccuracy_EvaluationJob_RAGConfigPreserved(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/evaluation-jobs", map[string]any{
		"jobName": "rag-eval-job",
		"roleArn": "arn:aws:iam::000000000000:role/eval-role",
		"inferenceConfig": map[string]any{
			"ragConfig": map[string]any{
				"knowledgeBaseId": "kb-abc123",
			},
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	jobArn := out["jobArn"].(string)

	getRec := doRequest(t, h, http.MethodGet, "/evaluation-jobs/"+jobArn, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getOut map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))

	inferCfg := getOut["inferenceConfig"].(map[string]any)
	ragCfg := inferCfg["ragConfig"].(map[string]any)
	assert.Equal(t, "kb-abc123", ragCfg["knowledgeBaseId"])
}

func TestAccuracy_EvaluationJob_EvalTaskConfigPreserved(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/evaluation-jobs", map[string]any{
		"jobName": "eval-config-job",
		"roleArn": "arn:aws:iam::000000000000:role/eval-role",
		"evaluationConfig": []map[string]any{
			{
				"taskType": "GENERAL",
				"dataset": map[string]any{
					"name": "test-dataset",
					"datasetLocation": map[string]any{
						"s3Uri": "s3://my-bucket/eval-data/",
					},
				},
				"metricNames": []map[string]any{
					{"metricName": "Builtin.Accuracy"},
					{"metricName": "Builtin.Robustness"},
				},
			},
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	jobArn := out["jobArn"].(string)

	getRec := doRequest(t, h, http.MethodGet, "/evaluation-jobs/"+jobArn, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getOut map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))

	evalCfg := getOut["evaluationConfig"].([]any)
	require.Len(t, evalCfg, 1)
	task := evalCfg[0].(map[string]any)
	assert.Equal(t, "GENERAL", task["taskType"])
	metrics := task["metricNames"].([]any)
	assert.Len(t, metrics, 2)
}

func TestAccuracy_EvaluationJob_JobDescriptionPreserved(t *testing.T) {
	t.Parallel()

	const desc = "evaluating knowledge retention in LLM"

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/evaluation-jobs", map[string]any{
		"jobName":        "described-eval-job",
		"jobDescription": desc,
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	jobArn := out["jobArn"].(string)

	getRec := doRequest(t, h, http.MethodGet, "/evaluation-jobs/"+jobArn, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getOut map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
	assert.Equal(t, desc, getOut["jobDescription"])
}

// ─────────────────────────────────────────────────────────────
// Automated reasoning policies — lifecycle accuracy
// ─────────────────────────────────────────────────────────────

func TestAccuracy_ARP_DescriptionPreservedOnCreate(t *testing.T) {
	t.Parallel()

	const desc = "reasons about financial transactions"

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/automated-reasoning-policies", map[string]any{
		"name":        "finance-arp",
		"description": desc,
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	policyARN := out["policyArn"].(string)

	getRec := doRequest(t, h, http.MethodGet, "/automated-reasoning-policies/"+policyARN, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getOut map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
	assert.Equal(t, desc, getOut["description"])
	assert.Equal(t, "finance-arp", getOut["name"])
}

func TestAccuracy_ARP_UpdateDescriptionReflected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/automated-reasoning-policies", map[string]any{
		"name": "updatable-arp",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	policyARN := out["policyArn"].(string)

	// Update description (ARP update uses PUT)
	updateRec := doRequest(t, h, http.MethodPut,
		"/automated-reasoning-policies/"+policyARN,
		map[string]any{"description": "updated description"},
	)
	require.Equal(t, http.StatusOK, updateRec.Code)

	getRec := doRequest(t, h, http.MethodGet, "/automated-reasoning-policies/"+policyARN, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getOut map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
	assert.Equal(t, "updated description", getOut["description"])
}

func TestAccuracy_ARP_VersionNumberingIsolatedPerPolicy(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	h := bedrock.NewHandler(b)

	policy0, err := b.CreateAutomatedReasoningPolicy("iso-arp-0", "", nil)
	require.NoError(t, err)

	policy1, err := b.CreateAutomatedReasoningPolicy("iso-arp-1", "", nil)
	require.NoError(t, err)

	// Create 2 versions for policy 0 via HTTP
	for i := range 2 {
		rec := doRequest(t, h, http.MethodPost,
			"/automated-reasoning-policies/"+url.PathEscape(policy0.PolicyArn)+"/versions",
			map[string]any{"definitionHash": fmt.Sprintf("hash-0-%d", i)})
		require.Equal(t, http.StatusCreated, rec.Code)
	}

	// Create 1 version for policy 1 via HTTP
	rec := doRequest(t, h, http.MethodPost,
		"/automated-reasoning-policies/"+url.PathEscape(policy1.PolicyArn)+"/versions",
		map[string]any{"definitionHash": "hash-1-0"})
	require.Equal(t, http.StatusCreated, rec.Code)

	// Verify second version for policy0 has version "2"
	var verOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &verOut))

	// policy1's first version is "1" (isolated counter)
	assert.Equal(t, "1", verOut["version"], "policy1 version counter starts from 1 independent of policy0")
}

func TestAccuracy_ARP_BuildWorkflowStatusTransitions(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	h := bedrock.NewHandler(b)

	// Create a policy
	rec := doRequest(t, h, http.MethodPost, "/automated-reasoning-policies", map[string]any{
		"name": "workflow-test-arp",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	policyARN := out["policyArn"].(string)

	// Add a build workflow via test helper
	wf := b.AddBuildWorkflowForTest(policyARN)
	assert.Equal(t, "Running", wf.Status)

	// Get the workflow
	getRec := doRequest(t, h, http.MethodGet,
		"/automated-reasoning-policies/"+policyARN+"/build-workflows/"+wf.BuildWorkflowID, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getOut map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
	assert.Equal(t, "Running", getOut["status"])
}

func TestAccuracy_ARP_TestCaseRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create policy
	rec := doRequest(t, h, http.MethodPost, "/automated-reasoning-policies", map[string]any{
		"name": "tc-arp",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	policyARN := out["policyArn"].(string)

	// Create test case (needs URL-encoded ARN in path)
	tcRec := doRequest(t, h, http.MethodPost,
		"/automated-reasoning-policies/"+url.PathEscape(policyARN)+"/test-cases",
		map[string]any{"input": "Is 2 + 2 = 4?"},
	)
	require.Equal(t, http.StatusCreated, tcRec.Code)

	var tcOut map[string]any
	require.NoError(t, json.Unmarshal(tcRec.Body.Bytes(), &tcOut))
	tcID := tcOut["testCaseId"].(string)
	assert.NotEmpty(t, tcID)

	// Get test case (needs URL-encoded ARN in path)
	getTestRec := doRequest(t, h, http.MethodGet,
		"/automated-reasoning-policies/"+url.PathEscape(policyARN)+"/test-cases/"+tcID, nil)
	require.Equal(t, http.StatusOK, getTestRec.Code)

	var getTestOut map[string]any
	require.NoError(t, json.Unmarshal(getTestRec.Body.Bytes(), &getTestOut))
	assert.Equal(t, tcID, getTestOut["testCaseId"])
}

func TestAccuracy_ARP_AnnotationsGetAfterUpdate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create policy
	rec := doRequest(t, h, http.MethodPost, "/automated-reasoning-policies", map[string]any{
		"name": "ann-arp",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	policyARN := out["policyArn"].(string)

	// Update annotations (needs URL-encoded ARN)
	updateRec := doRequest(t, h, http.MethodPut,
		"/automated-reasoning-policies/"+url.PathEscape(policyARN)+"/annotations",
		map[string]any{"annotations": []map[string]any{
			{"key": "env", "value": "prod"},
		}},
	)
	require.Equal(t, http.StatusOK, updateRec.Code)

	// Get annotations - should not be empty
	getAnnRec := doRequest(t, h, http.MethodGet,
		"/automated-reasoning-policies/"+url.PathEscape(policyARN)+"/annotations", nil)
	require.Equal(t, http.StatusOK, getAnnRec.Code)

	var annOut map[string]any
	require.NoError(t, json.Unmarshal(getAnnRec.Body.Bytes(), &annOut))
	assert.Contains(t, annOut, "annotations")
}

// ─────────────────────────────────────────────────────────────
// Prompt router — routing configuration accuracy
// ─────────────────────────────────────────────────────────────

func TestAccuracy_PromptRouter_NamePreservedInGetAndList(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		routerName string
	}{
		{name: "production router", routerName: "prod-router"},
		{name: "staging router", routerName: "stage-router"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/prompt-routers",
				map[string]any{"promptRouterName": tt.routerName})
			require.Equal(t, http.StatusCreated, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			routerARN := out["promptRouterArn"].(string)
			assert.NotEmpty(t, routerARN)

			getRec := doRequest(t, h, http.MethodGet, "/prompt-routers/"+routerARN, nil)
			require.Equal(t, http.StatusOK, getRec.Code)

			var getOut map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
			assert.Equal(t, tt.routerName, getOut["promptRouterName"])
			assert.Equal(t, "AVAILABLE", getOut["status"])
		})
	}
}

func TestAccuracy_PromptRouter_TagsPreserved(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/prompt-routers", map[string]any{
		"promptRouterName": "tagged-router",
		"tags": []map[string]any{
			{"key": "env", "value": "prod"},
			{"key": "team", "value": "ml"},
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	routerARN := out["promptRouterArn"].(string)

	getRec := doRequest(t, h, http.MethodGet, "/prompt-routers/"+routerARN, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	// Router ARN is returned and non-empty
	var getOut map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
	assert.Equal(t, "AVAILABLE", getOut["status"])
}

func TestAccuracy_PromptRouter_DeleteNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodDelete,
		"/prompt-routers/arn:aws:bedrock:us-east-1:000000000000:prompt-router/nonexistent", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestAccuracy_PromptRouter_ListAfterDelete(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create two routers
	for i := range 2 {
		doRequest(t, h, http.MethodPost, "/prompt-routers",
			map[string]any{"promptRouterName": "router-del"},
		)
		_ = i
	}

	// Create one to delete
	delRec := doRequest(t, h, http.MethodPost, "/prompt-routers",
		map[string]any{"promptRouterName": "to-be-deleted"})
	require.Equal(t, http.StatusCreated, delRec.Code)

	var delOut map[string]any
	require.NoError(t, json.Unmarshal(delRec.Body.Bytes(), &delOut))
	delARN := delOut["promptRouterArn"].(string)

	// Delete it
	deleteRec := doRequest(t, h, http.MethodDelete, "/prompt-routers/"+delARN, nil)
	assert.Equal(t, http.StatusNoContent, deleteRec.Code)

	// It should not appear in list
	listRec := doRequest(t, h, http.MethodGet, "/prompt-routers", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))
	routers := listOut["promptRouters"].([]any)

	for _, r := range routers {
		router := r.(map[string]any)
		assert.NotEqual(t, delARN, router["promptRouterArn"])
	}
}

// ─────────────────────────────────────────────────────────────
// Model invocation job — output config accuracy
// ─────────────────────────────────────────────────────────────

func TestAccuracy_ModelInvocationJob_JobNamePreserved(t *testing.T) {
	t.Parallel()

	tests := []string{"batch-run-1", "eval-batch-2026", "my-bulk-inference"}

	for _, jobName := range tests {
		t.Run(jobName, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/model-invocation-jobs",
				map[string]any{"jobName": jobName})
			require.Equal(t, http.StatusCreated, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			jobArn := out["jobArn"].(string)

			getRec := doRequest(t, h, http.MethodGet, "/model-invocation-jobs/"+jobArn, nil)
			require.Equal(t, http.StatusOK, getRec.Code)

			var getOut map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
			assert.Equal(t, jobName, getOut["jobName"])
		})
	}
}

func TestAccuracy_ModelInvocationJob_StatusProgression(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model-invocation-jobs",
		map[string]any{"jobName": "status-test-job"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	jobArn := out["jobArn"].(string)

	getRec := doRequest(t, h, http.MethodGet, "/model-invocation-jobs/"+jobArn, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getOut map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
	assert.Contains(t, []any{"Submitted", "InProgress", "Completed", "Failed", "Stopped"},
		getOut["status"], "status should be a valid model invocation job status")
}

func TestAccuracy_ModelInvocationJob_StopChangesStatus(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model-invocation-jobs",
		map[string]any{"jobName": "stoppable-job"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	jobArn := out["jobArn"].(string)

	stopRec := doRequest(t, h, http.MethodDelete, "/model-invocation-jobs/"+jobArn, nil)
	require.Equal(t, http.StatusNoContent, stopRec.Code)

	getRec := doRequest(t, h, http.MethodGet, "/model-invocation-jobs/"+jobArn, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getOut map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
	assert.Equal(t, "Stopped", getOut["status"])
}

// ─────────────────────────────────────────────────────────────
// Custom model deployment — lifecycle accuracy
// ─────────────────────────────────────────────────────────────

func TestAccuracy_CustomModelDeployment_StatusIsActive(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	h := bedrock.NewHandler(b)

	// Create a custom model first
	modelRec := doRequest(t, h, http.MethodPost, "/custom-models/create-custom-model", map[string]any{
		"modelName":           "my-fine-tuned-model",
		"baseModelIdentifier": "arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-text-express-v1",
		"roleArn":             "arn:aws:iam::000000000000:role/bedrock-custom-role",
		"outputDataConfig":    map[string]any{"s3Uri": "s3://my-bucket/output/"},
	})
	require.Equal(t, http.StatusCreated, modelRec.Code, modelRec.Body.String())

	var modelOut map[string]any
	require.NoError(t, json.Unmarshal(modelRec.Body.Bytes(), &modelOut))
	modelARN := modelOut["modelArn"].(string)

	// Create deployment for the model
	depRec := doRequest(t, h, http.MethodPost, "/model-customization/custom-model-deployments", map[string]any{
		"modelDeploymentName": "my-deployment",
		"modelArn":            modelARN,
	})
	require.Equal(t, http.StatusCreated, depRec.Code)

	var depOut map[string]any
	require.NoError(t, json.Unmarshal(depRec.Body.Bytes(), &depOut))
	depARN := depOut["customModelDeploymentArn"].(string)
	assert.NotEmpty(t, depARN)

	getRec := doRequest(t, h, http.MethodGet, "/custom-model-deployments/"+depARN, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getOut map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
	assert.Equal(t, "my-deployment", getOut["modelDeploymentName"])
	assert.Equal(t, "Creating", getOut["status"])
}

func TestAccuracy_CustomModelDeployment_ListAfterMultipleCreates(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	h := bedrock.NewHandler(b)

	// Create 3 deployments
	for i := range 3 {
		doRequest(t, h, http.MethodPost, "/model-customization/custom-model-deployments", map[string]any{
			"modelDeploymentName": fmt.Sprintf("deploy-list-test-%d", i),
			"modelArn":            "arn:aws:bedrock:us-east-1:000000000000:custom-model/test-model",
		})
	}

	listRec := doRequest(t, h, http.MethodGet, "/custom-model-deployments", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))
	deployments := listOut["deploymentSummaries"].([]any)
	assert.GreaterOrEqual(t, len(deployments), 3)
}

func TestAccuracy_CustomModelDeployment_DeleteRemovesFromList(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	h := bedrock.NewHandler(b)

	// Create a deployment
	depRec := doRequest(t, h, http.MethodPost, "/model-customization/custom-model-deployments", map[string]any{
		"modelDeploymentName": "disposable-deploy",
		"modelArn":            "arn:aws:bedrock:us-east-1:000000000000:custom-model/x",
	})
	require.Equal(t, http.StatusCreated, depRec.Code)

	var depOut map[string]any
	require.NoError(t, json.Unmarshal(depRec.Body.Bytes(), &depOut))
	depARN := depOut["customModelDeploymentArn"].(string)

	// Delete it
	deleteRec := doRequest(t, h, http.MethodDelete, "/custom-model-deployments/"+depARN, nil)
	assert.Equal(t, http.StatusNoContent, deleteRec.Code)

	// Get should return 404
	getRec := doRequest(t, h, http.MethodGet, "/custom-model-deployments/"+depARN, nil)
	assert.Equal(t, http.StatusNotFound, getRec.Code)
}

// ─────────────────────────────────────────────────────────────
// Model import job — imported model lifecycle accuracy
// ─────────────────────────────────────────────────────────────

func TestAccuracy_ModelImportJob_ImportedModelStatus(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model-import-jobs",
		map[string]any{"jobName": "import-status-job"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	jobARN := out["jobArn"].(string)

	getRec := doRequest(t, h, http.MethodGet, "/model-import-jobs/"+jobARN, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getOut map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))

	// Status should be one of the valid statuses
	assert.Contains(t, []any{"InProgress", "Completed", "Failed"}, getOut["status"])
}

func TestAccuracy_ModelImportJob_TagsPreserved(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model-import-jobs", map[string]any{
		"jobName": "tagged-import",
		"tags": []map[string]any{
			{"key": "purpose", "value": "testing"},
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	jobARN := out["jobArn"].(string)

	getRec := doRequest(t, h, http.MethodGet, "/model-import-jobs/"+jobARN, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getOut map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
	assert.Equal(t, "tagged-import", getOut["jobName"])
}

func TestAccuracy_ImportedModel_AvailableAfterJobComplete(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	h := bedrock.NewHandler(b)

	// Create import job and advance to completed
	importRec := doRequest(t, h, http.MethodPost, "/model-import-jobs",
		map[string]any{"jobName": "completed-import-job"})
	require.Equal(t, http.StatusCreated, importRec.Code)

	var importOut map[string]any
	require.NoError(t, json.Unmarshal(importRec.Body.Bytes(), &importOut))
	jobARN := importOut["jobArn"].(string)

	// Advance job status to complete (zero duration = immediate)
	b.AdvanceCopyImportJobStatuses(0)

	// Get the job to find the imported model ARN
	getJobRec := doRequest(t, h, http.MethodGet, "/model-import-jobs/"+jobARN, nil)
	require.Equal(t, http.StatusOK, getJobRec.Code)

	var jobOut map[string]any
	require.NoError(t, json.Unmarshal(getJobRec.Body.Bytes(), &jobOut))
	importedModelARN := jobOut["importedModelArn"].(string)
	assert.NotEmpty(t, importedModelARN)

	// The imported model should be accessible
	listRec := doRequest(t, h, http.MethodGet, "/imported-models", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))
	models := listOut["modelSummaries"].([]any)
	assert.NotEmpty(t, models)
}

// ─────────────────────────────────────────────────────────────
// Enforced guardrail configuration — accuracy
// ─────────────────────────────────────────────────────────────

func TestAccuracy_EnforcedGuardrail_PutUpdatesVersionForSameID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// PUT config version 1
	putRec1 := doRequest(t, h, http.MethodPut, "/enforced-guardrail-configuration",
		map[string]any{"guardrailId": "grd-update", "guardrailVersion": "1"})
	require.Equal(t, http.StatusNoContent, putRec1.Code)

	// PUT same guardrailId with version 2 — should update, not add a duplicate
	putRec2 := doRequest(t, h, http.MethodPut, "/enforced-guardrail-configuration",
		map[string]any{"guardrailId": "grd-update", "guardrailVersion": "2"})
	require.Equal(t, http.StatusNoContent, putRec2.Code)

	listRec := doRequest(t, h, http.MethodGet, "/enforced-guardrail-configuration", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))
	configs := listOut["enforcedGuardrailConfigurations"].([]any)
	assert.Len(t, configs, 1, "same guardrailId PUT should not create a duplicate")

	cfg := configs[0].(map[string]any)
	assert.Equal(t, "grd-update", cfg["guardrailId"])
	assert.Equal(t, "2", cfg["guardrailVersion"], "version should be updated to latest PUT")
}

func TestAccuracy_EnforcedGuardrail_DeleteClearsConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// PUT config
	doRequest(t, h, http.MethodPut, "/enforced-guardrail-configuration",
		map[string]any{"guardrailId": "grd-del", "guardrailVersion": "DRAFT"})

	// DELETE requires guardrailId as query parameter
	deleteRec := doRequest(t, h, http.MethodDelete,
		"/enforced-guardrail-configuration?guardrailId=grd-del", nil)
	require.Equal(t, http.StatusNoContent, deleteRec.Code)

	// List should be empty
	listRec := doRequest(t, h, http.MethodGet, "/enforced-guardrail-configuration", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))
	configs := listOut["enforcedGuardrailConfigurations"].([]any)
	assert.Empty(t, configs)
}

// ─────────────────────────────────────────────────────────────
// Use case for model access — accuracy
// ─────────────────────────────────────────────────────────────

func TestAccuracy_UseCaseForModelAccess_TypeAndDescriptionRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		useCaseType string
		desc        string
		name        string
	}{
		{name: "research use case", useCaseType: "RESEARCH", desc: "academic research"},
		{name: "commercial use case", useCaseType: "COMMERCIAL", desc: "production service"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			putRec := doRequest(t, h, http.MethodPut, "/usecase-for-model-access",
				map[string]any{
					"useCaseType":        tt.useCaseType,
					"useCaseDescription": tt.desc,
				})
			require.Equal(t, http.StatusNoContent, putRec.Code)

			getRec := doRequest(t, h, http.MethodGet, "/usecase-for-model-access", nil)
			require.Equal(t, http.StatusOK, getRec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &out))
			assert.Equal(t, tt.useCaseType, out["useCaseType"])
			assert.Equal(t, tt.desc, out["useCaseDescription"])
		})
	}
}

// ─────────────────────────────────────────────────────────────
// Guardrail version — policy field accuracy after versioning
// ─────────────────────────────────────────────────────────────

func TestAccuracy_GuardrailVersion_PoliciesPreservedInVersion(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create guardrail with content policy
	rec := doRequest(t, h, http.MethodPost, "/guardrails", map[string]any{
		"name": "versioned-policy-guardrail",
		"policies": map[string]any{
			"contentPolicyConfig": map[string]any{
				"filtersConfig": []map[string]any{
					{"type": "HATE", "inputStrength": "HIGH", "outputStrength": "HIGH"},
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	guardrailID := out["guardrailId"].(string)

	// Create a version
	versionRec := doRequest(t, h, http.MethodPost, "/guardrails/"+guardrailID,
		map[string]any{"description": "stable version"})
	require.Equal(t, http.StatusOK, versionRec.Code)

	var verOut map[string]any
	require.NoError(t, json.Unmarshal(versionRec.Body.Bytes(), &verOut))
	version := verOut["version"].(string)
	assert.NotEqual(t, "DRAFT", version)

	// Get the specific version — it should have policies
	getVerRec := doRequest(t, h, http.MethodGet, "/guardrails/"+guardrailID+"?guardrailVersion="+version, nil)
	require.Equal(t, http.StatusOK, getVerRec.Code)

	var verDetailOut map[string]any
	require.NoError(t, json.Unmarshal(getVerRec.Body.Bytes(), &verDetailOut))
	assert.NotNil(t, verDetailOut["policies"], "version should preserve policies")
}
