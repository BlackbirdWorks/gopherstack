package lambda_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambda"
)

// ============================================================
// GetSupportedOperations: verify batch-2 ops listed
// ============================================================

func TestGetSupportedOperations_IncludesExpectedOps(t *testing.T) {
	t.Parallel()

	h := lambda.NewHandler(nil)
	ops := h.GetSupportedOperations()

	opSet := make(map[string]bool, len(ops))
	for _, op := range ops {
		opSet[op] = true
	}

	batch2Ops := []string{
		"PublishVersion",
		"ListVersionsByFunction",
		"CreateAlias",
		"GetAlias",
		"UpdateAlias",
		"DeleteAlias",
		"ListAliases",
		"AddPermission",
		"RemovePermission",
		"GetPolicy",
		"PutFunctionEventInvokeConfig",
		"GetFunctionEventInvokeConfig",
		"UpdateFunctionEventInvokeConfig",
		"DeleteFunctionEventInvokeConfig",
		"ListFunctionEventInvokeConfigs",
		"PutFunctionConcurrency",
		"GetFunctionConcurrency",
		"DeleteFunctionConcurrency",
		"PutProvisionedConcurrencyConfig",
		"GetProvisionedConcurrencyConfig",
		"DeleteProvisionedConcurrencyConfig",
		"ListProvisionedConcurrencyConfigs",
		"CreateEventSourceMapping",
		"GetEventSourceMapping",
		"UpdateEventSourceMapping",
		"DeleteEventSourceMapping",
		"ListEventSourceMappings",
		"PublishLayerVersion",
		"GetLayerVersion",
		"ListLayerVersions",
		"ListLayers",
		"DeleteLayerVersion",
		"GetLayerVersionByArn",
		"AddLayerVersionPermission",
		"GetLayerVersionPolicy",
		"RemoveLayerVersionPermission",
		"TagResource",
		"UntagResource",
		"ListTags",
		"GetFunctionConfiguration",
		"UpdateFunctionConfiguration",
		"UpdateFunctionCode",
		"GetRuntimeManagementConfig",
		"PutRuntimeManagementConfig",
		"GetFunctionRecursionConfig",
		"PutFunctionRecursionConfig",
		"GetFunctionScalingConfig",
		"PutFunctionScalingConfig",
	}

	for _, op := range batch2Ops {
		assert.True(t, opSet[op], "expected op %q in GetSupportedOperations", op)
	}
}

// --- RouteMatcher tests ---

func TestRouteMatcher_AdditionalPaths(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	tests := []struct {
		name       string
		method     string
		path       string
		body       string
		wantStatus int
	}{
		{
			name:       "account_settings",
			method:     http.MethodGet,
			path:       "/2016-08-19/account-settings",
			wantStatus: http.StatusOK,
		},
		{
			name:       "code_signing_configs_list",
			method:     http.MethodGet,
			path:       "/2020-04-22/code-signing-configs",
			wantStatus: http.StatusOK,
		},
		{
			name:       "capacity_providers_list",
			method:     http.MethodGet,
			path:       "/2025-11-30/capacity-providers",
			wantStatus: http.StatusOK,
		},
		{
			name:       "durable_executions_checkpoint",
			method:     http.MethodPost,
			path:       "/2025-12-01/durable-executions/test-arn/checkpoint",
			body:       `{}`,
			wantStatus: http.StatusOK,
		},
		{
			// A distinct ARN from "durable_executions_checkpoint" above: this
			// table shares one handler/backend across parallel subtests, so
			// reusing "test-arn" here would make the result depend on
			// subtest execution order.
			name:       "durable_executions_stop",
			method:     http.MethodPost,
			path:       "/2025-12-01/durable-executions/test-arn-stop-only/stop",
			body:       `{}`,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "durable_executions_list_by_function",
			method:     http.MethodGet,
			path:       "/2025-12-01/functions/some-func/durable-executions",
			wantStatus: http.StatusOK,
		},
		{
			name:       "durable_execution_callback_unknown",
			method:     http.MethodPost,
			path:       "/2025-12-01/durable-execution-callbacks/cb-none/succeed",
			body:       "payload",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := callInMemoryHandler(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestGetSupportedOperations_IncludesAccountAndProviderOps validates all new ops are in GetSupportedOperations.
func TestGetSupportedOperations_IncludesAccountAndProviderOps(t *testing.T) {
	t.Parallel()

	h := lambda.NewHandler(nil)
	ops := h.GetSupportedOperations()

	newOps := []string{
		"AddPermission",
		"CheckpointDurableExecution",
		"CreateCapacityProvider",
		"CreateCodeSigningConfig",
		"CreateFunctionUrlConfig",
		"DeleteCapacityProvider",
		"DeleteCodeSigningConfig",
		"DeleteFunctionCodeSigningConfig",
		"DeleteFunctionUrlConfig",
		"GetAccountSettings",
		"GetCapacityProvider",
		"GetCodeSigningConfig",
		"GetFunctionCodeSigningConfig",
		"GetFunctionUrlConfig",
		"ListCapacityProviders",
		"ListCodeSigningConfigs",
		"ListFunctionsByCodeSigningConfig",
		"ListFunctionUrlConfigs",
		"ListFunctionVersionsByCapacityProvider",
		"PutFunctionCodeSigningConfig",
		"UpdateCapacityProvider",
		"UpdateCodeSigningConfig",
		"UpdateFunctionUrlConfig",
	}

	opSet := make(map[string]bool, len(ops))
	for _, op := range ops {
		opSet[op] = true
	}

	for _, op := range newOps {
		assert.True(t, opSet[op], "expected %q in GetSupportedOperations", op)
	}
}

// TestListFunctionURLConfigs_All tests listing all function URL configs.
func TestListFunctionURLConfigs_All(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	// Create two functions and URL configs
	createFunctionForTest(t, h, "fn-url-1")
	createFunctionForTest(t, h, "fn-url-2")

	createRec1 := callInMemoryHandler(t, h, http.MethodPost,
		"/2021-10-31/functions/fn-url-1/url", `{"AuthType":"NONE"}`)
	require.Equal(t, http.StatusCreated, createRec1.Code)

	createRec2 := callInMemoryHandler(t, h, http.MethodPost,
		"/2021-10-31/functions/fn-url-2/url", `{"AuthType":"AWS_IAM"}`)
	require.Equal(t, http.StatusCreated, createRec2.Code)

	// List all function URL configs (no name filter) via empty function name path
	listRec := callInMemoryHandler(t, h, http.MethodGet, "/2021-10-31/functions//urls", "")
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut lambda.ListFunctionURLConfigsOutput
	require.NoError(t, json.NewDecoder(listRec.Body).Decode(&listOut))
	assert.Len(t, listOut.FunctionURLConfigs, 2)
}

// TestHandlerReset_ClearsState tests that handler Reset clears state.
func TestHandlerReset_ClearsState(t *testing.T) {
	t.Parallel()

	h, bk := newInMemoryHandler(t)

	// Create a code signing config
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2020-04-22/code-signing-configs",
		`{"AllowedPublishers":{"SigningProfileVersionArns":[]}}`)
	require.Equal(t, http.StatusCreated, rec.Code)

	// Create a capacity provider
	rec2 := callInMemoryHandler(t, h, http.MethodPost, "/2025-11-30/capacity-providers",
		`{"CapacityProviderName":"cp1",`+
			`"PermissionsConfig":{"CapacityProviderOperatorRoleArn":"arn:aws:iam::000000000000:role/r"},`+
			`"VpcConfig":{"SubnetIds":["subnet-1"],"SecurityGroupIds":["sg-1"]}}`)
	require.Equal(t, http.StatusCreated, rec2.Code)

	// Verify they exist
	listCSC := callInMemoryHandler(t, h, http.MethodGet, "/2020-04-22/code-signing-configs", "")
	require.Equal(t, http.StatusOK, listCSC.Code)

	var listOut lambda.ListCodeSigningConfigsOutput
	require.NoError(t, json.NewDecoder(listCSC.Body).Decode(&listOut))
	require.NotEmpty(t, listOut.CodeSigningConfigs)

	// Reset
	bk.Reset()

	// Verify code signing configs are cleared
	listCSC2 := callInMemoryHandler(t, h, http.MethodGet, "/2020-04-22/code-signing-configs", "")
	require.Equal(t, http.StatusOK, listCSC2.Code)

	var listOut2 lambda.ListCodeSigningConfigsOutput
	require.NoError(t, json.NewDecoder(listCSC2.Body).Decode(&listOut2))
	assert.Empty(t, listOut2.CodeSigningConfigs)

	// Verify capacity providers are cleared
	listCP := callInMemoryHandler(t, h, http.MethodGet, "/2025-11-30/capacity-providers", "")
	require.Equal(t, http.StatusOK, listCP.Code)

	var listOutCP lambda.ListCapacityProvidersOutput
	require.NoError(t, json.NewDecoder(listCP.Body).Decode(&listOutCP))
	assert.Empty(t, listOutCP.CapacityProviders)
}
