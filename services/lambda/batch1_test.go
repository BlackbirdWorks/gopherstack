package lambda_test

import (
	"encoding/json"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambda"
)

// --- DurableExecution real-state tests ---

const durableExecARN = "arn:aws:lambda:us-east-1:000000000000:durable:test-exec-1"

func durableExecURL(suffix string) string {
	return "/2025-12-01/durable-executions/" + url.PathEscape(durableExecARN) + suffix
}

func TestBatch1_DurableExecution_CheckpointCreatesExecution(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	// Before checkpoint: GET returns 404
	rec := callInMemoryHandler(t, h, http.MethodGet, durableExecURL(""), "{}")
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// Checkpoint creates the execution
	rec = callInMemoryHandler(t, h, http.MethodPost, durableExecURL("/checkpoint"), `{"marker":"step1"}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	// After checkpoint: GET returns 200
	rec = callInMemoryHandler(t, h, http.MethodGet, durableExecURL(""), "{}")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), durableExecARN)
	assert.Contains(t, rec.Body.String(), "RUNNING")
}

func TestBatch1_DurableExecution_GetHistory(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	// Checkpoint twice to build history
	callInMemoryHandler(t, h, http.MethodPost, durableExecURL("/checkpoint"), `{}`)
	callInMemoryHandler(t, h, http.MethodPost, durableExecURL("/checkpoint"), `{}`)

	rec := callInMemoryHandler(t, h, http.MethodGet, durableExecURL("/history"), "{}")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Events")
	assert.Contains(t, rec.Body.String(), "Checkpoint")
}

func TestBatch1_DurableExecution_GetHistoryEmpty(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	// ARN that was never touched
	noneARN := "arn:aws:lambda:us-east-1:000000000000:durable:none"
	path := "/2025-12-01/durable-executions/" + url.PathEscape(noneARN) + "/history"
	rec := callInMemoryHandler(t, h, http.MethodGet, path, "{}")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Events")
}

func TestBatch1_DurableExecution_GetState(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	// Checkpoint creates the execution
	callInMemoryHandler(t, h, http.MethodPost, durableExecURL("/checkpoint"), `{}`)

	rec := callInMemoryHandler(t, h, http.MethodGet, durableExecURL("/state"), "{}")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), durableExecARN)
	assert.Contains(t, rec.Body.String(), "Status")
}

func TestBatch1_DurableExecution_GetStateNotFound(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	nopeARN := "arn:aws:lambda:us-east-1:000000000000:durable:nope"
	path := "/2025-12-01/durable-executions/" + url.PathEscape(nopeARN) + "/state"
	rec := callInMemoryHandler(t, h, http.MethodGet, path, "{}")
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestBatch1_DurableExecution_Stop(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	// Checkpoint to create execution
	callInMemoryHandler(t, h, http.MethodPost, durableExecURL("/checkpoint"), `{}`)

	// Stop
	rec := callInMemoryHandler(t, h, http.MethodDelete, durableExecURL(""), "{}")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "STOPPED")

	// Get reflects stopped status
	rec = callInMemoryHandler(t, h, http.MethodGet, durableExecURL(""), "{}")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "STOPPED")
}

func TestBatch1_DurableExecution_StopNonExistent(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	path := "/2025-12-01/durable-executions/" + url.PathEscape("arn:aws:lambda:us-east-1:000000000000:durable:none")
	rec := callInMemoryHandler(t, h, http.MethodDelete, path, "{}")
	// Stop on non-existent returns 200 (idempotent)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "STOPPED")
}

func TestBatch1_DurableExecution_CallbackSuccess(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	// Checkpoint to create execution
	callInMemoryHandler(t, h, http.MethodPost, durableExecURL("/checkpoint"), `{}`)

	// Send success callback
	rec := callInMemoryHandler(t, h, http.MethodPost, durableExecURL("/callback/success"), `{}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Execution now shows SUCCEEDED
	rec = callInMemoryHandler(t, h, http.MethodGet, durableExecURL(""), "{}")
	assert.Contains(t, rec.Body.String(), "SUCCEEDED")
}

func TestBatch1_DurableExecution_CallbackFailure(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	callInMemoryHandler(t, h, http.MethodPost, durableExecURL("/checkpoint"), `{}`)

	rec := callInMemoryHandler(t, h, http.MethodPost, durableExecURL("/callback/failure"), `{}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = callInMemoryHandler(t, h, http.MethodGet, durableExecURL(""), "{}")
	assert.Contains(t, rec.Body.String(), "FAILED")
}

func TestBatch1_DurableExecution_CallbackHeartbeat(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	callInMemoryHandler(t, h, http.MethodPost, durableExecURL("/checkpoint"), `{}`)

	rec := callInMemoryHandler(t, h, http.MethodPost, durableExecURL("/callback/heartbeat"), `{}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Still running after heartbeat
	rec = callInMemoryHandler(t, h, http.MethodGet, durableExecURL(""), "{}")
	assert.Contains(t, rec.Body.String(), "RUNNING")
}

func TestBatch1_DurableExecution_ListByFunction(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	// Checkpoint two different executions
	e1 := "arn:aws:lambda:us-east-1:000000000000:durable:exec-1"
	e2 := "arn:aws:lambda:us-east-1:000000000000:durable:exec-2"
	p1 := "/2025-12-01/durable-executions/" + url.PathEscape(e1) + "/checkpoint"
	p2 := "/2025-12-01/durable-executions/" + url.PathEscape(e2) + "/checkpoint"
	callInMemoryHandler(t, h, http.MethodPost, p1, `{}`)
	callInMemoryHandler(t, h, http.MethodPost, p2, `{}`)

	// List all (no function filter)
	rec := callInMemoryHandler(t, h, http.MethodGet, "/2025-12-01/durable-executions/", "{}")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DurableExecutions")
}

// --- Layer permission HTTP tests ---

func TestBatch1_LayerVersionPermission_AddGetRemove(t *testing.T) {
	t.Parallel()

	h, bk := newInMemoryHandler(t)

	// Publish a layer version first
	_, err := bk.PublishLayerVersion(&lambda.PublishLayerVersionInput{
		LayerName:          "my-layer",
		CompatibleRuntimes: []string{"python3.12"},
		Description:        "test layer",
		Content:            &lambda.LayerVersionContentInput{},
	})
	require.NoError(t, err)

	// Add permission
	rec := callInMemoryHandler(t, h, http.MethodPost,
		"/2018-10-31/layers/my-layer/versions/1/policy",
		`{"StatementId":"allow-all","Action":"lambda:GetLayerVersion","Principal":"*"}`,
	)
	require.Equal(t, http.StatusCreated, rec.Code)
	assert.Contains(t, rec.Body.String(), "RevisionId")

	// Get policy
	rec = callInMemoryHandler(t, h, http.MethodGet,
		"/2018-10-31/layers/my-layer/versions/1/policy", "{}")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "allow-all")

	// Remove permission
	rec = callInMemoryHandler(t, h, http.MethodDelete,
		"/2018-10-31/layers/my-layer/versions/1/policy/allow-all", "{}")
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

func TestBatch1_LayerVersionPermission_AddNotFound(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	rec := callInMemoryHandler(t, h, http.MethodPost,
		"/2018-10-31/layers/nonexistent-layer/versions/1/policy",
		`{"StatementId":"s1","Action":"lambda:GetLayerVersion","Principal":"*"}`,
	)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- Alias HTTP tests ---

func TestBatch1_Alias_CreateGetListUpdateDelete(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	fnName := "alias-test-fn"
	createFunctionForTest(t, h, fnName)

	// Publish a version to alias
	rec := callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/functions/"+fnName+"/versions", "{}")
	require.Equal(t, http.StatusCreated, rec.Code)

	// Create alias
	rec = callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/functions/"+fnName+"/aliases",
		`{"Name":"live","FunctionVersion":"1","Description":"production alias"}`,
	)
	require.Equal(t, http.StatusCreated, rec.Code)
	assert.Contains(t, rec.Body.String(), "live")

	// Get alias
	rec = callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/"+fnName+"/aliases/live", "{}")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "production alias")

	// List aliases
	rec = callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/"+fnName+"/aliases", "{}")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "live")

	// Update alias
	rec = callInMemoryHandler(t, h, http.MethodPut,
		"/2015-03-31/functions/"+fnName+"/aliases/live",
		`{"FunctionVersion":"$LATEST","Description":"updated"}`,
	)
	require.Equal(t, http.StatusOK, rec.Code)

	// Delete alias
	rec = callInMemoryHandler(t, h, http.MethodDelete,
		"/2015-03-31/functions/"+fnName+"/aliases/live", "{}")
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Get after delete → 404
	rec = callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/"+fnName+"/aliases/live", "{}")
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- Provisioned Concurrency HTTP tests ---

func TestBatch1_ProvisionedConcurrency_PutGetDeleteList(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	fnName := "pc-test-fn"
	createFunctionForTest(t, h, fnName)

	// Publish version
	callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/functions/"+fnName+"/versions", "{}")

	// Put provisioned concurrency
	rec := callInMemoryHandler(t, h, http.MethodPut,
		"/2015-03-31/functions/"+fnName+"/provisioned-concurrency?Qualifier=1",
		`{"ProvisionedConcurrentExecutions":5}`,
	)
	require.Equal(t, http.StatusCreated, rec.Code)

	// Get provisioned concurrency
	rec = callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/"+fnName+"/provisioned-concurrency?Qualifier=1", "{}")
	require.Equal(t, http.StatusOK, rec.Code)

	// List provisioned concurrency configs
	rec = callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/"+fnName+"/provisioned-concurrency", "{}")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ProvisionedConcurrencyConfigs")

	// Delete provisioned concurrency
	rec = callInMemoryHandler(t, h, http.MethodDelete,
		"/2015-03-31/functions/"+fnName+"/provisioned-concurrency?Qualifier=1", "{}")
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

// --- EventSourceMapping edge cases ---

func TestBatch1_ESM_ListByFunctionName(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	fnName := "esm-fn"
	createFunctionForTest(t, h, fnName)

	// Create ESM
	rec := callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/event-source-mappings",
		`{"FunctionName":"esm-fn",`+
			`"EventSourceArn":"arn:aws:kinesis:us-east-1:000000000000:stream/my-stream",`+
			`"StartingPosition":"LATEST"}`,
	)
	require.Equal(t, http.StatusCreated, rec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	uuid := created["UUID"].(string)

	// List by function name
	rec = callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/event-source-mappings?FunctionName=esm-fn", "{}")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), uuid)

	// Get ESM
	rec = callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/event-source-mappings/"+uuid, "{}")
	require.Equal(t, http.StatusOK, rec.Code)

	// Update ESM
	rec = callInMemoryHandler(t, h, http.MethodPut,
		"/2015-03-31/event-source-mappings/"+uuid,
		`{"BatchSize":50}`,
	)
	require.Equal(t, http.StatusOK, rec.Code)

	// Delete ESM
	rec = callInMemoryHandler(t, h, http.MethodDelete,
		"/2015-03-31/event-source-mappings/"+uuid, "{}")
	assert.Equal(t, http.StatusOK, rec.Code)
}

// --- Function URL HTTP tests ---

func TestBatch1_FunctionURL_CreateGetUpdateDelete(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	fnName := "url-fn"
	createFunctionForTest(t, h, fnName)

	// Create Function URL config
	rec := callInMemoryHandler(t, h, http.MethodPost,
		"/2021-10-31/functions/"+fnName+"/url",
		`{"AuthType":"NONE"}`,
	)
	require.Equal(t, http.StatusCreated, rec.Code)
	assert.Contains(t, rec.Body.String(), "FunctionUrl")

	// Get Function URL config
	rec = callInMemoryHandler(t, h, http.MethodGet,
		"/2021-10-31/functions/"+fnName+"/url", "{}")
	require.Equal(t, http.StatusOK, rec.Code)

	// List Function URL configs
	rec = callInMemoryHandler(t, h, http.MethodGet,
		"/2021-10-31/functions/"+fnName+"/urls", "{}")
	require.Equal(t, http.StatusOK, rec.Code)

	// Update Function URL config
	rec = callInMemoryHandler(t, h, http.MethodPut,
		"/2021-10-31/functions/"+fnName+"/url",
		`{"AuthType":"AWS_IAM"}`,
	)
	require.Equal(t, http.StatusOK, rec.Code)

	// Delete Function URL config
	rec = callInMemoryHandler(t, h, http.MethodDelete,
		"/2021-10-31/functions/"+fnName+"/url", "{}")
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

// --- FunctionConfig state tests (waiter compatibility) ---

func TestBatch1_FunctionConfig_StateActiveAfterCreate(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	fnName := "state-test-fn"
	createFunctionForTest(t, h, fnName)

	// GetFunctionConfiguration should return State: Active
	rec := callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/"+fnName+"/configuration", "{}")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `"State":"Active"`)
	assert.Contains(t, rec.Body.String(), `"LastUpdateStatus":"Successful"`)
}

func TestBatch1_FunctionConfig_StateAfterUpdateCode(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	fnName := "update-state-fn"
	createFunctionForTest(t, h, fnName)

	// Update function code
	rec := callInMemoryHandler(t, h, http.MethodPut,
		"/2015-03-31/functions/"+fnName+"/code",
		`{"ImageUri":"ecr.example.com/my-image:v2"}`,
	)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `"LastUpdateStatus":"Successful"`)
}

func TestBatch1_FunctionConfig_StateAfterUpdateConfiguration(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	fnName := "update-config-fn"
	createFunctionForTest(t, h, fnName)

	// Update function configuration
	rec := callInMemoryHandler(t, h, http.MethodPut,
		"/2015-03-31/functions/"+fnName+"/configuration",
		`{"Description":"updated description"}`,
	)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `"LastUpdateStatus":"Successful"`)
}

// --- RecursionConfig, ScalingConfig, RuntimeManagementConfig HTTP tests ---

func TestBatch1_FunctionRecursionConfig_PutGet(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	fnName := "recursion-fn"
	createFunctionForTest(t, h, fnName)

	// Put recursion config
	rec := callInMemoryHandler(t, h, http.MethodPut,
		"/2024-08-28/functions/"+fnName+"/recursion-config",
		`{"RecursiveLoop":"Deny"}`,
	)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Deny")

	// Get recursion config
	rec = callInMemoryHandler(t, h, http.MethodGet,
		"/2024-08-28/functions/"+fnName+"/recursion-config", "{}")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Deny")
}

func TestBatch1_FunctionScalingConfig_PutGet(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	fnName := "scaling-fn"
	createFunctionForTest(t, h, fnName)

	maxConc := 10

	// Put scaling config
	rec := callInMemoryHandler(t, h, http.MethodPut,
		"/2023-10-26/functions/"+fnName+"/scaling-config",
		`{"MaximumConcurrency":10}`,
	)
	require.Equal(t, http.StatusOK, rec.Code)

	// Get scaling config
	rec = callInMemoryHandler(t, h, http.MethodGet,
		"/2023-10-26/functions/"+fnName+"/scaling-config", "{}")
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.InDelta(t, float64(maxConc), out["MaximumConcurrency"], 0.001)
}

func TestBatch1_RuntimeManagementConfig_PutGet(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	fnName := "runtime-mgmt-fn"
	createFunctionForTest(t, h, fnName)

	// Put runtime management config
	rec := callInMemoryHandler(t, h, http.MethodPut,
		"/2021-07-20/functions/"+fnName+"/runtime-management-config",
		`{"UpdateRuntimeOn":"Manual","RuntimeVersionArn":"arn:aws:lambda:us-east-1::runtime:python3.12:v1"}`,
	)
	require.Equal(t, http.StatusOK, rec.Code)

	// Get runtime management config
	rec = callInMemoryHandler(t, h, http.MethodGet,
		"/2021-07-20/functions/"+fnName+"/runtime-management-config", "{}")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Manual")
}

// --- FunctionEventInvokeConfig tests ---

func TestBatch1_FunctionEventInvokeConfig_PutGetUpdateDeleteList(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	fnName := "event-invoke-fn"
	createFunctionForTest(t, h, fnName)

	// Put event invoke config
	rec := callInMemoryHandler(t, h, http.MethodPut,
		"/2015-03-31/functions/"+fnName+"/event-invoke-config",
		`{"MaximumRetryAttempts":2,"MaximumEventAgeInSeconds":300}`,
	)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `"MaximumRetryAttempts":2`)

	// Get event invoke config
	rec = callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/"+fnName+"/event-invoke-config", "{}")
	require.Equal(t, http.StatusOK, rec.Code)

	// Update (POST) event invoke config
	rec = callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/functions/"+fnName+"/event-invoke-config",
		`{"MaximumRetryAttempts":1}`,
	)
	require.Equal(t, http.StatusOK, rec.Code)

	// List
	rec = callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/"+fnName+"/event-invoke-configs", "{}")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), fnName)

	// Delete
	rec = callInMemoryHandler(t, h, http.MethodDelete,
		"/2015-03-31/functions/"+fnName+"/event-invoke-config", "{}")
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

// --- GetLayerVersionByArn HTTP tests ---

func TestBatch1_GetLayerVersionByArn(t *testing.T) {
	t.Parallel()

	h, bk := newInMemoryHandler(t)

	// Publish a layer
	out, err := bk.PublishLayerVersion(&lambda.PublishLayerVersionInput{
		LayerName:          "arn-test-layer",
		CompatibleRuntimes: []string{"python3.12"},
		Content:            &lambda.LayerVersionContentInput{},
	})
	require.NoError(t, err)

	// Get by ARN using /2018-10-31/layers-by-arn?Arn={arn}
	rec := callInMemoryHandler(t, h, http.MethodGet,
		"/2018-10-31/layers-by-arn?Arn="+url.QueryEscape(out.LayerVersionArn), "{}")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "arn-test-layer")
}
