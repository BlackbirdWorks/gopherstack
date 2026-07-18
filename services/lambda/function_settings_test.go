package lambda_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/lambda"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- RecursionConfig, ScalingConfig, RuntimeManagementConfig HTTP tests ---

func TestFunctionRecursionConfig_PutGet(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	fnName := "recursion-fn"
	createFunctionForTest(t, h, fnName)

	// Put recursion config
	rec := callInMemoryHandler(
		t, h, http.MethodPut,
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

func TestFunctionScalingConfig_PutGet(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	fnName := "scaling-fn"
	createFunctionForTest(t, h, fnName)

	maxConc := 10

	// Put scaling config
	rec := callInMemoryHandler(
		t, h, http.MethodPut,
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

func TestRuntimeManagementConfig_PutGet(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	fnName := "runtime-mgmt-fn"
	createFunctionForTest(t, h, fnName)

	// Put runtime management config
	rec := callInMemoryHandler(
		t, h, http.MethodPut,
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

// ============================================================
// RuntimeManagementConfig
// ============================================================

func TestRuntimeManagementConfig_GetDefault(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "rmc-fn")

	rec := callInMemoryHandler(t, h, http.MethodGet,
		"/2021-07-20/functions/rmc-fn/runtime-management-config", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var cfg lambda.RuntimeManagementConfig
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&cfg))
	assert.Equal(t, "Auto", cfg.UpdateRuntimeOn)
}

func TestRuntimeManagementConfig_Put(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "rmc-put-fn")

	putRec := callInMemoryHandler(t, h, http.MethodPut,
		"/2021-07-20/functions/rmc-put-fn/runtime-management-config",
		`{"UpdateRuntimeOn":"FunctionUpdate"}`)
	require.Equal(t, http.StatusOK, putRec.Code)

	var cfg lambda.RuntimeManagementConfig
	require.NoError(t, json.NewDecoder(putRec.Body).Decode(&cfg))
	assert.Equal(t, "FunctionUpdate", cfg.UpdateRuntimeOn)
}

func TestRuntimeManagementConfig_PutManualWithARN(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "rmc-manual-fn")

	rva := "arn:aws:lambda:us-east-1::runtime:python3.12:abc123"
	body := fmt.Sprintf(`{"UpdateRuntimeOn":"Manual","RuntimeVersionArn":%q}`, rva)
	putRec := callInMemoryHandler(t, h, http.MethodPut,
		"/2021-07-20/functions/rmc-manual-fn/runtime-management-config", body)
	require.Equal(t, http.StatusOK, putRec.Code)

	var cfg lambda.RuntimeManagementConfig
	require.NoError(t, json.NewDecoder(putRec.Body).Decode(&cfg))
	assert.Equal(t, "Manual", cfg.UpdateRuntimeOn)
	assert.Equal(t, rva, cfg.RuntimeVersionArn)
}

func TestRuntimeManagementConfig_FunctionARNInResponse(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "rmc-arn-fn")

	rec := callInMemoryHandler(t, h, http.MethodGet,
		"/2021-07-20/functions/rmc-arn-fn/runtime-management-config", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var cfg lambda.RuntimeManagementConfig
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&cfg))
	assert.Contains(t, cfg.FunctionArn, "rmc-arn-fn")
}

// ============================================================
// FunctionRecursionConfig
// ============================================================

func TestRecursionConfig_GetDefault(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "rec-fn")

	rec := callInMemoryHandler(t, h, http.MethodGet,
		"/2024-08-28/functions/rec-fn/recursion-config", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var cfg lambda.FunctionRecursionConfig
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&cfg))
	assert.Equal(t, "Terminate", cfg.RecursiveLoop)
}

func TestRecursionConfig_Put_Allow(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "rec-allow-fn")

	putRec := callInMemoryHandler(t, h, http.MethodPut,
		"/2024-08-28/functions/rec-allow-fn/recursion-config",
		`{"RecursiveLoop":"Allow"}`)
	require.Equal(t, http.StatusOK, putRec.Code)

	var cfg lambda.FunctionRecursionConfig
	require.NoError(t, json.NewDecoder(putRec.Body).Decode(&cfg))
	assert.Equal(t, "Allow", cfg.RecursiveLoop)
}

func TestRecursionConfig_Put_Terminate(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "rec-term-fn")

	callInMemoryHandler(t, h, http.MethodPut,
		"/2024-08-28/functions/rec-term-fn/recursion-config",
		`{"RecursiveLoop":"Allow"}`)

	putRec := callInMemoryHandler(t, h, http.MethodPut,
		"/2024-08-28/functions/rec-term-fn/recursion-config",
		`{"RecursiveLoop":"Terminate"}`)
	require.Equal(t, http.StatusOK, putRec.Code)

	var cfg lambda.FunctionRecursionConfig
	require.NoError(t, json.NewDecoder(putRec.Body).Decode(&cfg))
	assert.Equal(t, "Terminate", cfg.RecursiveLoop)
}

// TestFunctionSettingsRoute_ErrorCases covers the common error paths shared
// by the runtime-management-config and recursion-config routes: a missing
// function returns 404 regardless of method, and an unsupported method on an
// existing function returns 405. Table-driven: both route families share the
// identical request/response shape and differ only in path and method.
func TestFunctionSettingsRoute_ErrorCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		pathSuffix string
		fnName     string
		method     string
		body       string
		wantCode   int
		createFn   bool
	}{
		{
			name:       "runtime-management-config GET on missing function is 404",
			pathSuffix: "runtime-management-config", fnName: "nonexistent",
			method: http.MethodGet, wantCode: http.StatusNotFound,
		},
		{
			name:       "runtime-management-config PUT on missing function is 404",
			pathSuffix: "runtime-management-config", fnName: "nonexistent",
			method: http.MethodPut, body: `{"UpdateRuntimeOn":"Auto"}`, wantCode: http.StatusNotFound,
		},
		{
			name:       "runtime-management-config DELETE is 405",
			pathSuffix: "runtime-management-config", fnName: "rmc-method-fn", createFn: true,
			method: http.MethodDelete, wantCode: http.StatusMethodNotAllowed,
		},
		{
			name:       "recursion-config GET on missing function is 404",
			pathSuffix: "recursion-config", fnName: "nonexistent",
			method: http.MethodGet, wantCode: http.StatusNotFound,
		},
		{
			name:       "recursion-config DELETE is 405",
			pathSuffix: "recursion-config", fnName: "rec-mna-fn", createFn: true,
			method: http.MethodDelete, wantCode: http.StatusMethodNotAllowed,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)
			if tc.createFn {
				createFunctionForTest(t, h, tc.fnName)
			}

			var pathPrefix string
			if tc.pathSuffix == "runtime-management-config" {
				pathPrefix = "/2021-07-20/functions/"
			} else {
				pathPrefix = "/2024-08-28/functions/"
			}

			rec := callInMemoryHandler(t, h, tc.method, pathPrefix+tc.fnName+"/"+tc.pathSuffix, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

// ---- Gap 8: ScalingConfig.MaximumConcurrency enforcement ----

func TestScalingConfig_MaximumConcurrency_Enforced(t *testing.T) {
	t.Parallel()

	h, bk := newInMemoryHandler(t)
	rec := auditCreateFunction(t, h, baseImageFn("scaling-fn"))
	require.Equal(t, http.StatusCreated, rec.Code)

	// Set MaximumConcurrency = 1
	maxConc := 1
	_, err := bk.PutFunctionScalingConfig(
		"scaling-fn",
		&lambda.PutFunctionScalingConfigInput{MaximumConcurrency: &maxConc},
	)
	require.NoError(t, err)

	// Manually hold 1 slot (simulate active invocation)
	held, err := lambda.AcquireConcurrencySlot(bk, "scaling-fn")
	require.NoError(t, err)
	require.True(t, held)

	// Next acquire must fail with TooManyRequests
	_, err = lambda.AcquireConcurrencySlot(bk, "scaling-fn")
	require.ErrorIs(t, err, lambda.ErrTooManyRequests)

	// Release and verify slot becomes available
	lambda.ReleaseConcurrencySlot(bk, "scaling-fn")
	held2, err := lambda.AcquireConcurrencySlot(bk, "scaling-fn")
	require.NoError(t, err)
	require.True(t, held2)
	lambda.ReleaseConcurrencySlot(bk, "scaling-fn")
}

func TestScalingConfig_ZeroConcurrency_Blocked(t *testing.T) {
	t.Parallel()

	h, bk := newInMemoryHandler(t)
	rec := auditCreateFunction(t, h, baseImageFn("scaling-zero-fn"))
	require.Equal(t, http.StatusCreated, rec.Code)

	// MaximumConcurrency = 0 → no invocations permitted
	zero := 0
	_, err := bk.PutFunctionScalingConfig(
		"scaling-zero-fn",
		&lambda.PutFunctionScalingConfigInput{MaximumConcurrency: &zero},
	)
	require.NoError(t, err)

	// No slots should be acquirable (returns false, nil because hasLimit=false and no reserved)
	// But MaximumConcurrency=0 with scaling config enforcement should block:
	_, err = lambda.AcquireConcurrencySlot(bk, "scaling-zero-fn")
	// With MaximumConcurrency=0, active(0) >= 0 is true so it blocks
	require.ErrorIs(t, err, lambda.ErrTooManyRequests)
}
