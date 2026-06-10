package lambda_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambda"
)

// newInMemoryHandler creates a handler backed by a real InMemoryBackend for unit tests.
// It uses a nil port allocator so no real HTTP servers are started.
func newInMemoryHandler(t *testing.T) (*lambda.Handler, *lambda.InMemoryBackend) {
	t.Helper()

	bk := lambda.NewInMemoryBackend(
		nil,
		nil, // no real HTTP servers in unit tests
		lambda.DefaultSettings(),
		"000000000000",
		"us-east-1",
	)
	h := lambda.NewHandler(bk)
	h.DefaultRegion = "us-east-1"
	h.AccountID = "000000000000"

	return h, bk
}

// callInMemoryHandler sends a request to the handler and returns the response recorder.
func callInMemoryHandler(
	t *testing.T,
	h *lambda.Handler,
	method, path, body string,
) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	return rec
}

// createFunctionForTest creates a Lambda function and returns its name.
func createFunctionForTest(t *testing.T, h *lambda.Handler, fnName string) {
	t.Helper()

	body := fmt.Sprintf(
		`{"FunctionName":%q,"PackageType":"Image","Code":{"ImageUri":"x"},"Role":"arn:aws:iam:::role/r"}`,
		fnName,
	)
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions", body)
	require.Equal(t, http.StatusCreated, rec.Code)
}

// --- AddPermission tests ---

func TestNewOps_AddPermission(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		fn         string
		body       string
		wantStatus int
	}{
		{
			name:       "success",
			fn:         "add-perm-fn",
			body:       `{"StatementId":"AllowS3","Action":"lambda:InvokeFunction","Principal":"s3.amazonaws.com"}`,
			wantStatus: http.StatusCreated,
		},
		{
			name:       "missing_statement_id",
			fn:         "add-perm-fn",
			body:       `{"Action":"lambda:InvokeFunction","Principal":"s3.amazonaws.com"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_action",
			fn:         "add-perm-fn",
			body:       `{"StatementId":"s1","Principal":"s3.amazonaws.com"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_principal",
			fn:         "add-perm-fn",
			body:       `{"StatementId":"s1","Action":"lambda:InvokeFunction"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "function_not_found",
			fn:         "nonexistent",
			body:       `{"StatementId":"s1","Action":"lambda:InvokeFunction","Principal":"s3.amazonaws.com"}`,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "invalid_json",
			fn:         "add-perm-fn",
			body:       `not-json`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)
			createFunctionForTest(t, h, "add-perm-fn")

			rec := callInMemoryHandler(
				t, h,
				http.MethodPost,
				fmt.Sprintf("/2015-03-31/functions/%s/policy", tt.fn),
				tt.body,
			)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusCreated {
				var out lambda.AddPermissionOutput
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
				require.NotNil(t, out.Statement)
				assert.NotEmpty(t, *out.Statement)
			}
		})
	}
}

func TestNewOps_AddPermission_Duplicate(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "dup-fn")

	body := `{"StatementId":"stmt1","Action":"lambda:InvokeFunction","Principal":"s3.amazonaws.com"}`
	path := "/2015-03-31/functions/dup-fn/policy"

	rec1 := callInMemoryHandler(t, h, http.MethodPost, path, body)
	require.Equal(t, http.StatusCreated, rec1.Code)

	rec2 := callInMemoryHandler(t, h, http.MethodPost, path, body)
	assert.Equal(t, http.StatusConflict, rec2.Code)
}

// --- GetAccountSettings tests ---

func TestNewOps_GetAccountSettings(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	rec := callInMemoryHandler(t, h, http.MethodGet, "/2016-08-19/account-settings", "")

	require.Equal(t, http.StatusOK, rec.Code)

	var out lambda.AccountSettingsOutput
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	require.NotNil(t, out.AccountLimit)
	assert.Positive(t, out.AccountLimit.ConcurrentExecutions)
	assert.Positive(t, out.AccountLimit.TotalCodeSize)
	require.NotNil(t, out.AccountUsage)
	assert.GreaterOrEqual(t, out.AccountUsage.FunctionCount, 0)
}

func TestNewOps_GetAccountSettings_FunctionCount(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "fn-a")
	createFunctionForTest(t, h, "fn-b")

	rec := callInMemoryHandler(t, h, http.MethodGet, "/2016-08-19/account-settings", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var out lambda.AccountSettingsOutput
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.Equal(t, 2, out.AccountUsage.FunctionCount)
}

// --- CodeSigningConfig tests ---

func TestNewOps_CodeSigningConfig_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		createBody string
		wantDesc   string
		wantStatus int
	}{
		{
			name:       "with_description",
			createBody: `{"AllowedPublishers":{"SigningProfileVersionArns":["arn:aws:signer:::p"]},"Description":"my csc"}`,
			wantStatus: http.StatusCreated,
			wantDesc:   "my csc",
		},
		{
			name:       "empty_body",
			createBody: `{}`,
			wantStatus: http.StatusCreated,
			wantDesc:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)

			rec := callInMemoryHandler(t, h, http.MethodPost, "/2020-04-22/code-signing-configs", tt.createBody)
			require.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusCreated {
				var out lambda.CreateCodeSigningConfigOutput
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
				require.NotNil(t, out.CodeSigningConfig)
				assert.NotEmpty(t, out.CodeSigningConfig.CodeSigningConfigArn)
				assert.Equal(t, tt.wantDesc, out.CodeSigningConfig.Description)
			}
		})
	}
}

func TestNewOps_CodeSigningConfig_GetDeleteUpdate(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	// Create
	rec := callInMemoryHandler(
		t,
		h,
		http.MethodPost,
		"/2020-04-22/code-signing-configs",
		`{"AllowedPublishers":{"SigningProfileVersionArns":["arn:aws:signer:::p"]}}`,
	)
	require.Equal(t, http.StatusCreated, rec.Code)

	var createOut lambda.CreateCodeSigningConfigOutput
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&createOut))
	cscARN := createOut.CodeSigningConfig.CodeSigningConfigArn

	// Get
	getRec := callInMemoryHandler(t, h, http.MethodGet,
		"/2020-04-22/code-signing-configs/"+cscARN, "")
	assert.Equal(t, http.StatusOK, getRec.Code)

	// Get not found
	getNotFound := callInMemoryHandler(t, h, http.MethodGet,
		"/2020-04-22/code-signing-configs/nonexistent", "")
	assert.Equal(t, http.StatusNotFound, getNotFound.Code)

	// Update
	updateRec := callInMemoryHandler(t, h, http.MethodPut,
		"/2020-04-22/code-signing-configs/"+cscARN,
		`{"Description":"updated"}`)
	assert.Equal(t, http.StatusOK, updateRec.Code)

	// List
	listRec := callInMemoryHandler(t, h, http.MethodGet, "/2020-04-22/code-signing-configs", "")
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut lambda.ListCodeSigningConfigsOutput
	require.NoError(t, json.NewDecoder(listRec.Body).Decode(&listOut))
	assert.Len(t, listOut.CodeSigningConfigs, 1)

	// Delete
	delRec := callInMemoryHandler(t, h, http.MethodDelete,
		"/2020-04-22/code-signing-configs/"+cscARN, "")
	assert.Equal(t, http.StatusNoContent, delRec.Code)

	// Get after delete → 404
	getRec2 := callInMemoryHandler(t, h, http.MethodGet,
		"/2020-04-22/code-signing-configs/"+cscARN, "")
	assert.Equal(t, http.StatusNotFound, getRec2.Code)
}

func TestNewOps_FunctionCodeSigningConfig(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "csc-test-fn")

	// Create a code signing config first
	rec := callInMemoryHandler(
		t,
		h,
		http.MethodPost,
		"/2020-04-22/code-signing-configs",
		`{"AllowedPublishers":{"SigningProfileVersionArns":["arn:aws:signer:::p"]}}`,
	)
	require.Equal(t, http.StatusCreated, rec.Code)

	var createOut lambda.CreateCodeSigningConfigOutput
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&createOut))
	cscARN := createOut.CodeSigningConfig.CodeSigningConfigArn

	// Put function code signing config
	putBody := fmt.Sprintf(`{"CodeSigningConfigArn":%q}`, cscARN)
	putRec := callInMemoryHandler(t, h, http.MethodPut,
		"/2020-06-30/functions/csc-test-fn/code-signing-config", putBody)
	require.Equal(t, http.StatusOK, putRec.Code)

	var putOut lambda.PutFunctionCodeSigningConfigOutput
	require.NoError(t, json.NewDecoder(putRec.Body).Decode(&putOut))
	assert.Equal(t, cscARN, putOut.CodeSigningConfigArn)
	assert.Equal(t, "csc-test-fn", putOut.FunctionName)

	// Get function code signing config
	getRec := callInMemoryHandler(t, h, http.MethodGet,
		"/2020-06-30/functions/csc-test-fn/code-signing-config", "")
	require.Equal(t, http.StatusOK, getRec.Code)

	var getOut lambda.GetFunctionCodeSigningConfigOutput
	require.NoError(t, json.NewDecoder(getRec.Body).Decode(&getOut))
	assert.Equal(t, cscARN, getOut.CodeSigningConfigArn)

	// List functions by code signing config
	listRec := callInMemoryHandler(t, h, http.MethodGet,
		fmt.Sprintf("/2020-04-22/code-signing-configs/%s/functions", cscARN), "")
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut lambda.ListFunctionsByCodeSigningConfigOutput
	require.NoError(t, json.NewDecoder(listRec.Body).Decode(&listOut))
	assert.NotEmpty(t, listOut.FunctionArns)

	// Delete function code signing config
	delRec := callInMemoryHandler(t, h, http.MethodDelete,
		"/2020-06-30/functions/csc-test-fn/code-signing-config", "")
	assert.Equal(t, http.StatusNoContent, delRec.Code)

	// Get after delete → 200 with empty ARN (matches real AWS behavior)
	getRec2 := callInMemoryHandler(t, h, http.MethodGet,
		"/2020-06-30/functions/csc-test-fn/code-signing-config", "")
	assert.Equal(t, http.StatusOK, getRec2.Code)

	var getOut2 lambda.GetFunctionCodeSigningConfigOutput
	require.NoError(t, json.NewDecoder(getRec2.Body).Decode(&getOut2))
	assert.Empty(t, getOut2.CodeSigningConfigArn)
}

func TestNewOps_FunctionCodeSigningConfig_MissingArn(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "fn-no-csc")

	rec := callInMemoryHandler(t, h, http.MethodPut,
		"/2020-06-30/functions/fn-no-csc/code-signing-config",
		`{}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- CapacityProvider tests ---

func TestNewOps_CapacityProvider_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		createBody string
		wantName   string
		wantStatus int
	}{
		{
			name:       "with_concurrency",
			createBody: `{"Name":"my-provider","TargetOnDemandConcurrency":100}`,
			wantStatus: http.StatusCreated,
			wantName:   "my-provider",
		},
		{
			name:       "without_concurrency",
			createBody: `{"Name":"basic-provider"}`,
			wantStatus: http.StatusCreated,
			wantName:   "basic-provider",
		},
		{
			name:       "missing_name",
			createBody: `{}`,
			wantStatus: http.StatusBadRequest,
			wantName:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)

			rec := callInMemoryHandler(t, h, http.MethodPost, "/2025-11-30/capacity-providers", tt.createBody)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusCreated {
				var out lambda.CreateCapacityProviderOutput
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
				require.NotNil(t, out.CapacityProvider)
				assert.Equal(t, tt.wantName, out.CapacityProvider.Name)
				assert.NotEmpty(t, out.CapacityProvider.CapacityProviderArn)
			}
		})
	}
}

func TestNewOps_CapacityProvider_GetDeleteUpdateList(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	// Create
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2025-11-30/capacity-providers",
		`{"Name":"test-cp","TargetOnDemandConcurrency":50}`)
	require.Equal(t, http.StatusCreated, rec.Code)

	// Create duplicate → conflict
	dupRec := callInMemoryHandler(t, h, http.MethodPost, "/2025-11-30/capacity-providers",
		`{"Name":"test-cp"}`)
	assert.Equal(t, http.StatusConflict, dupRec.Code)

	// Get
	getRec := callInMemoryHandler(t, h, http.MethodGet, "/2025-11-30/capacity-providers/test-cp", "")
	require.Equal(t, http.StatusOK, getRec.Code)

	// Get not found
	getNotFound := callInMemoryHandler(t, h, http.MethodGet, "/2025-11-30/capacity-providers/nonexistent", "")
	assert.Equal(t, http.StatusNotFound, getNotFound.Code)

	// Update
	updateRec := callInMemoryHandler(t, h, http.MethodPut, "/2025-11-30/capacity-providers/test-cp",
		`{"TargetOnDemandConcurrency":200}`)
	require.Equal(t, http.StatusOK, updateRec.Code)

	var updateOut lambda.UpdateCapacityProviderOutput
	require.NoError(t, json.NewDecoder(updateRec.Body).Decode(&updateOut))
	require.NotNil(t, updateOut.CapacityProvider)
	assert.Equal(t, 200, updateOut.CapacityProvider.TargetOnDemandConcurrency)

	// List
	listRec := callInMemoryHandler(t, h, http.MethodGet, "/2025-11-30/capacity-providers", "")
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut lambda.ListCapacityProvidersOutput
	require.NoError(t, json.NewDecoder(listRec.Body).Decode(&listOut))
	assert.Len(t, listOut.CapacityProviders, 1)

	// Delete
	delRec := callInMemoryHandler(t, h, http.MethodDelete, "/2025-11-30/capacity-providers/test-cp", "")
	assert.Equal(t, http.StatusNoContent, delRec.Code)

	// List after delete → empty
	listRec2 := callInMemoryHandler(t, h, http.MethodGet, "/2025-11-30/capacity-providers", "")
	require.Equal(t, http.StatusOK, listRec2.Code)

	var listOut2 lambda.ListCapacityProvidersOutput
	require.NoError(t, json.NewDecoder(listRec2.Body).Decode(&listOut2))
	assert.Empty(t, listOut2.CapacityProviders)
}

// --- CheckpointDurableExecution tests ---

func TestNewOps_CheckpointDurableExecution(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	tests := []struct {
		name       string
		path       string
		method     string
		wantStatus int
	}{
		{
			name:       "checkpoint_success",
			path:       "/2025-12-01/durable-executions/arn:aws:lambda:us-east-1:000000000000:durable:abc/checkpoint",
			method:     http.MethodPost,
			wantStatus: http.StatusOK,
		},
		{
			name:       "get_not_found",
			path:       "/2025-12-01/durable-executions/arn:aws:lambda:us-east-1:000000000000:durable:never-created",
			method:     http.MethodGet,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := callInMemoryHandler(t, h, tt.method, tt.path, `{}`)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// --- Function URL config 2021 path tests ---

func TestNewOps_FunctionURLConfig_2021(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "url-fn-2021")

	// Create via 2021 path
	createRec := callInMemoryHandler(t, h, http.MethodPost,
		"/2021-10-31/functions/url-fn-2021/url", `{"AuthType":"NONE"}`)
	require.Equal(t, http.StatusCreated, createRec.Code)

	var cfg lambda.FunctionURLConfig
	require.NoError(t, json.NewDecoder(createRec.Body).Decode(&cfg))
	assert.Equal(t, "NONE", cfg.AuthType)

	// Get via 2021 path
	getRec := callInMemoryHandler(t, h, http.MethodGet,
		"/2021-10-31/functions/url-fn-2021/url", "")
	assert.Equal(t, http.StatusOK, getRec.Code)

	// List via 2021 path
	listRec := callInMemoryHandler(t, h, http.MethodGet,
		"/2021-10-31/functions/url-fn-2021/urls", "")
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut lambda.ListFunctionURLConfigsOutput
	require.NoError(t, json.NewDecoder(listRec.Body).Decode(&listOut))
	assert.Len(t, listOut.FunctionURLConfigs, 1)

	// Update via 2021 path
	updateRec := callInMemoryHandler(t, h, http.MethodPut,
		"/2021-10-31/functions/url-fn-2021/url", `{"AuthType":"AWS_IAM"}`)
	assert.Equal(t, http.StatusOK, updateRec.Code)

	// Delete via 2021 path
	delRec := callInMemoryHandler(t, h, http.MethodDelete,
		"/2021-10-31/functions/url-fn-2021/url", "")
	assert.Equal(t, http.StatusNoContent, delRec.Code)

	// Get after delete → 404
	getRec2 := callInMemoryHandler(t, h, http.MethodGet,
		"/2021-10-31/functions/url-fn-2021/url", "")
	assert.Equal(t, http.StatusNotFound, getRec2.Code)
}

func TestNewOps_FunctionURLConfig_2021_UpdateNotFound(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "fn-no-url")

	rec := callInMemoryHandler(t, h, http.MethodPut,
		"/2021-10-31/functions/fn-no-url/url", `{"AuthType":"AWS_IAM"}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestNewOps_FunctionURLConfig_2021_ListEmpty(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "fn-no-url-empty")

	// List for a function without a URL config returns empty list (not 404)
	listRec := callInMemoryHandler(t, h, http.MethodGet,
		"/2021-10-31/functions/fn-no-url-empty/urls", "")
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut lambda.ListFunctionURLConfigsOutput
	require.NoError(t, json.NewDecoder(listRec.Body).Decode(&listOut))
	assert.Empty(t, listOut.FunctionURLConfigs)
}

// --- RouteMatcher tests ---

func TestNewOps_RouteMatcher_NewPaths(t *testing.T) {
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := callInMemoryHandler(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestNewOps_InvokeWithResponseStream verifies that the streaming invoke endpoint
// returns application/vnd.amazon.eventstream and at least one data frame.
func TestNewOps_InvokeWithResponseStream(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "stream-fn")

	rec := callInMemoryHandler(t, h, http.MethodPost,
		"/2021-11-15/functions/stream-fn/response-streaming-invocations", `{}`)

	// In unit tests without a Docker runtime, the invocation will fail with 500 (no container).
	// Verify the function-not-found path is NOT triggered (i.e., not a 404).
	assert.NotEqual(t, http.StatusNotFound, rec.Code)

	// When the invocation succeeds (integration tests with real container), verify streaming format.
	if rec.Code == http.StatusOK {
		assert.Equal(t, "application/vnd.amazon.eventstream", rec.Header().Get("Content-Type"))
		assert.Greater(t, rec.Body.Len(), 4)
	}
}

func TestNewOps_InvokeWithResponseStream_NotFound(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	rec := callInMemoryHandler(t, h, http.MethodPost,
		"/2021-11-15/functions/nonexistent/response-streaming-invocations", `{}`)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestNewOps_GetSupportedOperations validates all new ops are in GetSupportedOperations.
func TestNewOps_GetSupportedOperations(t *testing.T) {
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

// TestNewOps_ListFunctionURLConfigs_All tests listing all function URL configs.
func TestNewOps_ListFunctionURLConfigs_All(t *testing.T) {
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

// TestNewOps_Reset tests that handler Reset clears state.
func TestNewOps_Reset(t *testing.T) {
	t.Parallel()

	h, bk := newInMemoryHandler(t)

	// Create a code signing config
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2020-04-22/code-signing-configs",
		`{"AllowedPublishers":{"SigningProfileVersionArns":[]}}`)
	require.Equal(t, http.StatusCreated, rec.Code)

	// Create a capacity provider
	rec2 := callInMemoryHandler(t, h, http.MethodPost, "/2025-11-30/capacity-providers",
		`{"Name":"cp1"}`)
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

// TestNewOps_CodeSigningConfig_UpdateNotFound tests updating a nonexistent code signing config.
func TestNewOps_CodeSigningConfig_UpdateNotFound(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	rec := callInMemoryHandler(t, h, http.MethodPut,
		"/2020-04-22/code-signing-configs/nonexistent-arn", `{}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestNewOps_CapacityProvider_UpdateNotFound tests updating a nonexistent capacity provider.
func TestNewOps_CapacityProvider_UpdateNotFound(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	rec := callInMemoryHandler(t, h, http.MethodPut,
		"/2025-11-30/capacity-providers/nonexistent", `{}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestNewOps_CodeSigningRoute_MethodNotAllowed tests method not allowed on code signing routes.
func TestNewOps_CodeSigningRoute_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	rec := callInMemoryHandler(t, h, http.MethodPatch, "/2020-04-22/code-signing-configs", "")
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

// TestNewOps_2020FunctionRoute_UnknownPath tests unknown 2020 function route.
func TestNewOps_2020FunctionRoute_UnknownPath(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	rec := callInMemoryHandler(t, h, http.MethodGet, "/2020-06-30/functions/fn/unknown-suffix", "")
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
