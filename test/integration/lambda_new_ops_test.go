package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	lambdapkg "github.com/blackbirdworks/gopherstack/services/lambda"
)

const (
	newOpsPortStart = 22000
	newOpsPortEnd   = 22500
)

// setupLambdaTestServer creates a test server with a Lambda handler backed by an InMemoryBackend.
func setupLambdaTestServer(t *testing.T, portStart, portEnd int) *httptest.Server {
	t.Helper()

	pa, err := portalloc.New(portStart, portEnd)
	require.NoError(t, err)

	backend := lambdapkg.NewInMemoryBackend(
		nil,
		pa,
		lambdapkg.DefaultSettings(),
		"000000000000",
		"us-east-1",
	)
	handler := lambdapkg.NewHandler(backend)
	handler.AccountID = "000000000000"
	handler.DefaultRegion = "us-east-1"

	e := echo.New()
	e.Pre(logger.EchoMiddleware(slog.Default()))
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(handler))
	e.Use(service.NewServiceRouter(registry).RouteHandler())
	server := httptest.NewServer(e)
	t.Cleanup(server.Close)

	return server
}

// createLambdaFunction creates a minimal Lambda function via HTTP POST.
func createLambdaFunction(ctx context.Context, t *testing.T, serverURL, fnName string) {
	t.Helper()

	body, err := json.Marshal(map[string]any{
		"FunctionName": fnName,
		"PackageType":  "Image",
		"Code":         map[string]string{"ImageUri": "test:latest"},
		"Role":         "arn:aws:iam::000000000000:role/test",
	})
	require.NoError(t, err)

	resp, err := doLambdaRequest(ctx, http.MethodPost,
		serverURL+"/2015-03-31/functions", "application/json", bytes.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)
}

// TestLambda_AddPermission tests the AddPermission operation.
func TestLambda_AddPermission(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	server := setupLambdaTestServer(t, newOpsPortStart, newOpsPortStart+100)
	const fnName = "add-permission-fn"

	createLambdaFunction(ctx, t, server.URL, fnName)

	tests := []struct {
		body       map[string]any
		name       string
		wantErr    string
		wantStatus int
	}{
		{
			name: "valid_permission",
			body: map[string]any{
				"StatementId": "AllowS3Invoke",
				"Action":      "lambda:InvokeFunction",
				"Principal":   "s3.amazonaws.com",
			},
			wantStatus: http.StatusCreated,
		},
		{
			name: "missing_statement_id",
			body: map[string]any{
				"Action":    "lambda:InvokeFunction",
				"Principal": "s3.amazonaws.com",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_action",
			body: map[string]any{
				"StatementId": "AllowSNS",
				"Principal":   "sns.amazonaws.com",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_principal",
			body: map[string]any{
				"StatementId": "AllowSNS",
				"Action":      "lambda:InvokeFunction",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "function_not_found",
			body: map[string]any{
				"StatementId": "AllowS3",
				"Action":      "lambda:InvokeFunction",
				"Principal":   "s3.amazonaws.com",
			},
			wantStatus: http.StatusNotFound,
			wantErr:    "nonexistent-fn",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			body, err := json.Marshal(tt.body)
			require.NoError(t, err)

			targetFn := fnName
			if tt.wantErr != "" {
				targetFn = tt.wantErr
			}

			url := fmt.Sprintf("%s/2015-03-31/functions/%s/policy", server.URL, targetFn)
			resp, err := doLambdaRequest(ctx, http.MethodPost, url, "application/json", bytes.NewReader(body))
			require.NoError(t, err)
			defer resp.Body.Close()

			assert.Equal(t, tt.wantStatus, resp.StatusCode)

			if tt.wantStatus == http.StatusCreated {
				rawBody, readErr := io.ReadAll(resp.Body)
				require.NoError(t, readErr)

				var out map[string]any
				require.NoError(t, json.Unmarshal(rawBody, &out))
				assert.NotEmpty(t, out["Statement"])
			}
		})
	}
}

// TestLambda_AddPermission_DuplicateStatementId tests that duplicate statement IDs are rejected.
func TestLambda_AddPermission_DuplicateStatementId(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	server := setupLambdaTestServer(t, newOpsPortStart+100, newOpsPortStart+200)
	const fnName = "dup-perm-fn"

	createLambdaFunction(ctx, t, server.URL, fnName)

	body, err := json.Marshal(map[string]any{
		"StatementId": "AllowS3",
		"Action":      "lambda:InvokeFunction",
		"Principal":   "s3.amazonaws.com",
	})
	require.NoError(t, err)

	url := fmt.Sprintf("%s/2015-03-31/functions/%s/policy", server.URL, fnName)

	// First request should succeed
	resp1, err := doLambdaRequest(ctx, http.MethodPost, url, "application/json", bytes.NewReader(body))
	require.NoError(t, err)
	defer resp1.Body.Close()
	require.Equal(t, http.StatusCreated, resp1.StatusCode)

	// Second request with same statement ID should conflict
	resp2, err := doLambdaRequest(ctx, http.MethodPost, url, "application/json", bytes.NewReader(body))
	require.NoError(t, err)
	defer resp2.Body.Close()
	assert.Equal(t, http.StatusConflict, resp2.StatusCode)
}

// TestLambda_GetAccountSettings tests the GetAccountSettings operation.
func TestLambda_GetAccountSettings(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	server := setupLambdaTestServer(t, newOpsPortStart+200, newOpsPortStart+300)

	resp, err := doLambdaRequest(ctx, http.MethodGet,
		server.URL+"/2016-08-19/account-settings", "", nil)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	respBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var out lambdapkg.AccountSettingsOutput
	require.NoError(t, json.Unmarshal(respBody, &out))

	require.NotNil(t, out.AccountLimit)
	assert.Positive(t, out.AccountLimit.ConcurrentExecutions)
	assert.Positive(t, out.AccountLimit.TotalCodeSize)

	require.NotNil(t, out.AccountUsage)
	assert.GreaterOrEqual(t, out.AccountUsage.FunctionCount, 0)
}

// TestLambda_CodeSigningConfig tests the full lifecycle of code signing configs.
func TestLambda_CodeSigningConfig(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	server := setupLambdaTestServer(t, newOpsPortStart+300, newOpsPortStart+400)
	const fnName = "csc-fn"

	createLambdaFunction(ctx, t, server.URL, fnName)

	// Create code signing config
	createBody, err := json.Marshal(map[string]any{
		"AllowedPublishers": map[string]any{
			"SigningProfileVersionArns": []string{
				"arn:aws:signer:us-east-1:000000000000:/signing-profiles/myProfile/abc123",
			},
		},
		"Description": "test config",
	})
	require.NoError(t, err)

	createResp, err := doLambdaRequest(ctx, http.MethodPost,
		server.URL+"/2020-04-22/code-signing-configs", "application/json", bytes.NewReader(createBody))
	require.NoError(t, err)
	defer createResp.Body.Close()
	require.Equal(t, http.StatusCreated, createResp.StatusCode)

	createRespBody, err := io.ReadAll(createResp.Body)
	require.NoError(t, err)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(createRespBody, &createOut))

	cscData, ok := createOut["CodeSigningConfig"].(map[string]any)
	require.True(t, ok, "expected CodeSigningConfig in response")

	cscARN, ok := cscData["CodeSigningConfigArn"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, cscARN)
	assert.Equal(t, "test config", cscData["Description"])

	// Get code signing config
	getResp, err := doLambdaRequest(ctx, http.MethodGet,
		fmt.Sprintf("%s/2020-04-22/code-signing-configs/%s", server.URL, cscARN), "", nil)
	require.NoError(t, err)
	defer getResp.Body.Close()
	require.Equal(t, http.StatusOK, getResp.StatusCode)

	// List code signing configs
	listResp, err := doLambdaRequest(ctx, http.MethodGet,
		server.URL+"/2020-04-22/code-signing-configs", "", nil)
	require.NoError(t, err)
	defer listResp.Body.Close()
	require.Equal(t, http.StatusOK, listResp.StatusCode)

	listBody, err := io.ReadAll(listResp.Body)
	require.NoError(t, err)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listBody, &listOut))
	configs, ok := listOut["CodeSigningConfigs"].([]any)
	require.True(t, ok)
	assert.Len(t, configs, 1)

	// Put function code signing config
	putBody, err := json.Marshal(map[string]any{"CodeSigningConfigArn": cscARN})
	require.NoError(t, err)

	putResp, err := doLambdaRequest(ctx, http.MethodPut,
		fmt.Sprintf("%s/2020-06-30/functions/%s/code-signing-config", server.URL, fnName),
		"application/json", bytes.NewReader(putBody))
	require.NoError(t, err)
	defer putResp.Body.Close()
	require.Equal(t, http.StatusOK, putResp.StatusCode)

	// Get function code signing config
	getFnCSCResp, err := doLambdaRequest(ctx, http.MethodGet,
		fmt.Sprintf("%s/2020-06-30/functions/%s/code-signing-config", server.URL, fnName),
		"", nil)
	require.NoError(t, err)
	defer getFnCSCResp.Body.Close()
	require.Equal(t, http.StatusOK, getFnCSCResp.StatusCode)

	getFnCSCBody, err := io.ReadAll(getFnCSCResp.Body)
	require.NoError(t, err)

	var getFnCSCOut map[string]any
	require.NoError(t, json.Unmarshal(getFnCSCBody, &getFnCSCOut))
	assert.Equal(t, cscARN, getFnCSCOut["CodeSigningConfigArn"])

	// List functions by code signing config
	listFnResp, err := doLambdaRequest(ctx, http.MethodGet,
		fmt.Sprintf("%s/2020-04-22/code-signing-configs/%s/functions", server.URL, cscARN), "", nil)
	require.NoError(t, err)
	defer listFnResp.Body.Close()
	require.Equal(t, http.StatusOK, listFnResp.StatusCode)

	// Delete function code signing config
	delFnCSCResp, err := doLambdaRequest(ctx, http.MethodDelete,
		fmt.Sprintf("%s/2020-06-30/functions/%s/code-signing-config", server.URL, fnName),
		"", nil)
	require.NoError(t, err)
	defer delFnCSCResp.Body.Close()
	assert.Equal(t, http.StatusNoContent, delFnCSCResp.StatusCode)

	// Delete code signing config
	delResp, err := doLambdaRequest(ctx, http.MethodDelete,
		fmt.Sprintf("%s/2020-04-22/code-signing-configs/%s", server.URL, cscARN), "", nil)
	require.NoError(t, err)
	defer delResp.Body.Close()
	assert.Equal(t, http.StatusNoContent, delResp.StatusCode)

	// Get after delete should 404
	getAfterDel, err := doLambdaRequest(ctx, http.MethodGet,
		fmt.Sprintf("%s/2020-04-22/code-signing-configs/%s", server.URL, cscARN), "", nil)
	require.NoError(t, err)
	defer getAfterDel.Body.Close()
	assert.Equal(t, http.StatusNotFound, getAfterDel.StatusCode)
}

// TestLambda_CapacityProvider tests the full lifecycle of capacity providers.
//
// The real CreateCapacityProvider requires CapacityProviderName,
// PermissionsConfig and VpcConfig (api_op_CreateCapacityProvider.go:28-45 in
// the pinned SDK); there is no top-level Name or TargetOnDemandConcurrency
// field anywhere on the wire. UpdateCapacityProvider only accepts
// CapacityProviderScalingConfig, PropagateTags and TelemetryConfig
// (api_op_UpdateCapacityProvider.go:27-43).
func TestLambda_CapacityProvider(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	server := setupLambdaTestServer(t, newOpsPortStart+400, newOpsPortStart+500)

	// Create capacity provider
	createBody, err := json.Marshal(map[string]any{
		"CapacityProviderName": "test-provider",
		"PermissionsConfig": map[string]any{
			"CapacityProviderOperatorRoleArn": "arn:aws:iam::000000000000:role/cp-role",
		},
		"VpcConfig": map[string]any{
			"SubnetIds":        []string{"subnet-1"},
			"SecurityGroupIds": []string{"sg-1"},
		},
	})
	require.NoError(t, err)

	createResp, err := doLambdaRequest(ctx, http.MethodPost,
		server.URL+"/2025-11-30/capacity-providers", "application/json", bytes.NewReader(createBody))
	require.NoError(t, err)
	defer createResp.Body.Close()
	require.Equal(t, http.StatusCreated, createResp.StatusCode)

	createRespBody, err := io.ReadAll(createResp.Body)
	require.NoError(t, err)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(createRespBody, &createOut))

	cpData, ok := createOut["CapacityProvider"].(map[string]any)
	require.True(t, ok, "expected CapacityProvider in response")
	assert.Contains(t, cpData["CapacityProviderArn"], "test-provider")
	assert.Equal(t, "Active", cpData["State"])
	assert.NotEmpty(t, cpData["LastModified"])

	// Create duplicate should conflict
	createDupResp, err := doLambdaRequest(ctx, http.MethodPost,
		server.URL+"/2025-11-30/capacity-providers", "application/json", bytes.NewReader(createBody))
	require.NoError(t, err)
	defer createDupResp.Body.Close()
	assert.Equal(t, http.StatusConflict, createDupResp.StatusCode)

	// Get capacity provider
	getResp, err := doLambdaRequest(ctx, http.MethodGet,
		server.URL+"/2025-11-30/capacity-providers/test-provider", "", nil)
	require.NoError(t, err)
	defer getResp.Body.Close()
	require.Equal(t, http.StatusOK, getResp.StatusCode)

	// List capacity providers
	listResp, err := doLambdaRequest(ctx, http.MethodGet,
		server.URL+"/2025-11-30/capacity-providers", "", nil)
	require.NoError(t, err)
	defer listResp.Body.Close()
	require.Equal(t, http.StatusOK, listResp.StatusCode)

	listBody, err := io.ReadAll(listResp.Body)
	require.NoError(t, err)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listBody, &listOut))
	providers, ok := listOut["CapacityProviders"].([]any)
	require.True(t, ok)
	assert.Len(t, providers, 1)

	// Update capacity provider
	updateBody, err := json.Marshal(map[string]any{
		"CapacityProviderScalingConfig": map[string]any{"MaxVCpuCount": 200},
	})
	require.NoError(t, err)

	updateResp, err := doLambdaRequest(ctx, http.MethodPut,
		server.URL+"/2025-11-30/capacity-providers/test-provider",
		"application/json", bytes.NewReader(updateBody))
	require.NoError(t, err)
	defer updateResp.Body.Close()
	require.Equal(t, http.StatusOK, updateResp.StatusCode)

	updateRespBody, err := io.ReadAll(updateResp.Body)
	require.NoError(t, err)

	var updateOut map[string]any
	require.NoError(t, json.Unmarshal(updateRespBody, &updateOut))
	cpUpdated, ok := updateOut["CapacityProvider"].(map[string]any)
	require.True(t, ok)
	scalingConfig, ok := cpUpdated["CapacityProviderScalingConfig"].(map[string]any)
	require.True(t, ok, "expected CapacityProviderScalingConfig in response")
	assert.InEpsilon(t, float64(200), scalingConfig["MaxVCpuCount"].(float64), 0.001)

	// Delete capacity provider
	delResp, err := doLambdaRequest(ctx, http.MethodDelete,
		server.URL+"/2025-11-30/capacity-providers/test-provider", "", nil)
	require.NoError(t, err)
	defer delResp.Body.Close()
	assert.Equal(t, http.StatusNoContent, delResp.StatusCode)

	// Get after delete should 404
	getAfterDel, err := doLambdaRequest(ctx, http.MethodGet,
		server.URL+"/2025-11-30/capacity-providers/test-provider", "", nil)
	require.NoError(t, err)
	defer getAfterDel.Body.Close()
	assert.Equal(t, http.StatusNotFound, getAfterDel.StatusCode)
}

// TestLambda_CheckpointDurableExecution tests the CheckpointDurableExecution stub.
func TestLambda_CheckpointDurableExecution(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	server := setupLambdaTestServer(t, newOpsPortStart, newOpsPortEnd)

	resp, err := doLambdaRequest(
		ctx,
		http.MethodPost,
		server.URL+"/2025-12-01/durable-executions/arn:aws:lambda:us-east-1:000000000000:durable-execution:abc123/checkpoint",
		"application/json",
		bytes.NewReader([]byte(`{}`)),
	)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

// TestLambda_FunctionUrlConfig_2021 tests function URL config via the 2021-10-31 path prefix.
func TestLambda_FunctionUrlConfig_2021(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	server := setupLambdaTestServer(t, newOpsPortStart+150, newOpsPortStart+250)
	const fnName = "url-config-2021-fn"

	createLambdaFunction(ctx, t, server.URL, fnName)

	// Create function URL config via 2021 path
	createBody, err := json.Marshal(map[string]any{"AuthType": "NONE"})
	require.NoError(t, err)

	createResp, err := doLambdaRequest(ctx, http.MethodPost,
		fmt.Sprintf("%s/2021-10-31/functions/%s/url", server.URL, fnName),
		"application/json", bytes.NewReader(createBody))
	require.NoError(t, err)
	defer createResp.Body.Close()
	require.Equal(t, http.StatusCreated, createResp.StatusCode)

	// Get function URL config via 2021 path
	getResp, err := doLambdaRequest(ctx, http.MethodGet,
		fmt.Sprintf("%s/2021-10-31/functions/%s/url", server.URL, fnName), "", nil)
	require.NoError(t, err)
	defer getResp.Body.Close()
	require.Equal(t, http.StatusOK, getResp.StatusCode)

	getBody, err := io.ReadAll(getResp.Body)
	require.NoError(t, err)

	var cfg lambdapkg.FunctionURLConfig
	require.NoError(t, json.Unmarshal(getBody, &cfg))
	assert.Equal(t, "NONE", cfg.AuthType)

	// List function URL configs via 2021 path
	listResp, err := doLambdaRequest(ctx, http.MethodGet,
		fmt.Sprintf("%s/2021-10-31/functions/%s/urls", server.URL, fnName), "", nil)
	require.NoError(t, err)
	defer listResp.Body.Close()
	require.Equal(t, http.StatusOK, listResp.StatusCode)

	listBody, err := io.ReadAll(listResp.Body)
	require.NoError(t, err)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listBody, &listOut))
	urlConfigs, ok := listOut["FunctionUrlConfigs"].([]any)
	require.True(t, ok)
	assert.Len(t, urlConfigs, 1)

	// Update function URL config via 2021 path
	updateBody, err := json.Marshal(map[string]any{"AuthType": "AWS_IAM"})
	require.NoError(t, err)

	updateResp, err := doLambdaRequest(ctx, http.MethodPut,
		fmt.Sprintf("%s/2021-10-31/functions/%s/url", server.URL, fnName),
		"application/json", bytes.NewReader(updateBody))
	require.NoError(t, err)
	defer updateResp.Body.Close()
	require.Equal(t, http.StatusOK, updateResp.StatusCode)

	// Delete function URL config via 2021 path
	delResp, err := doLambdaRequest(ctx, http.MethodDelete,
		fmt.Sprintf("%s/2021-10-31/functions/%s/url", server.URL, fnName), "", nil)
	require.NoError(t, err)
	defer delResp.Body.Close()
	assert.Equal(t, http.StatusNoContent, delResp.StatusCode)

	// Get after delete should 404
	getAfterDel, err := doLambdaRequest(ctx, http.MethodGet,
		fmt.Sprintf("%s/2021-10-31/functions/%s/url", server.URL, fnName), "", nil)
	require.NoError(t, err)
	defer getAfterDel.Body.Close()
	assert.Equal(t, http.StatusNotFound, getAfterDel.StatusCode)
}
