package lambda_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambda"
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
		"/2024-08-31/functions/"+fnName+"/recursion-config",
		`{"RecursiveLoop":"Deny"}`,
	)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Deny")

	// Get recursion config
	rec = callInMemoryHandler(t, h, http.MethodGet,
		"/2024-08-31/functions/"+fnName+"/recursion-config", "{}")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Deny")
}

func TestFunctionScalingConfig_PutGet(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	fnName := "scaling-fn"
	createFunctionForTest(t, h, fnName)

	maxEnv := 10

	// Put scaling config
	rec := callInMemoryHandler(
		t, h, http.MethodPut,
		"/2025-11-30/functions/"+fnName+"/function-scaling-config?Qualifier=$LATEST",
		`{"FunctionScalingConfig":{"MaxExecutionEnvironments":10}}`,
	)
	require.Equal(t, http.StatusOK, rec.Code)

	// Get scaling config
	rec = callInMemoryHandler(t, h, http.MethodGet,
		"/2025-11-30/functions/"+fnName+"/function-scaling-config?Qualifier=$LATEST", "{}")
	require.Equal(t, http.StatusOK, rec.Code)

	var out lambda.GetFunctionScalingConfigOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.NotNil(t, out.AppliedFunctionScalingConfig)
	require.NotNil(t, out.AppliedFunctionScalingConfig.MaxExecutionEnvironments)
	assert.Equal(t, int32(maxEnv), *out.AppliedFunctionScalingConfig.MaxExecutionEnvironments)
}

// TestFunctionScalingConfig_DistinctPerQualifier locks in gopherstack-gjn1:
// functionScalingConfigs was keyed only by function name, so writing scaling
// config for one qualifier silently overwrote every other qualifier's config.
func TestFunctionScalingConfig_DistinctPerQualifier(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	fnName := "scaling-multi-fn"
	createFunctionForTest(t, h, fnName)

	putScaling := func(qualifier string, maxEnv int) {
		rec := callInMemoryHandler(
			t, h, http.MethodPut,
			fmt.Sprintf(
				"/2025-11-30/functions/%s/function-scaling-config?Qualifier=%s",
				fnName, qualifier,
			),
			fmt.Sprintf(`{"FunctionScalingConfig":{"MaxExecutionEnvironments":%d}}`, maxEnv),
		)
		require.Equal(t, http.StatusOK, rec.Code)
	}

	getScaling := func(qualifier string) int32 {
		rec := callInMemoryHandler(t, h, http.MethodGet,
			fmt.Sprintf("/2025-11-30/functions/%s/function-scaling-config?Qualifier=%s", fnName, qualifier),
			"{}")
		require.Equal(t, http.StatusOK, rec.Code)

		var out lambda.GetFunctionScalingConfigOutput
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		require.NotNil(t, out.AppliedFunctionScalingConfig)
		require.NotNil(t, out.AppliedFunctionScalingConfig.MaxExecutionEnvironments)

		return *out.AppliedFunctionScalingConfig.MaxExecutionEnvironments
	}

	putScaling("$LATEST", 5)
	putScaling("v1", 20)

	assert.Equal(t, int32(5), getScaling("$LATEST"))
	assert.Equal(t, int32(20), getScaling("v1"))
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
		"/2024-08-31/functions/rec-fn/recursion-config", "")
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
		"/2024-08-31/functions/rec-allow-fn/recursion-config",
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
		"/2024-08-31/functions/rec-term-fn/recursion-config",
		`{"RecursiveLoop":"Allow"}`)

	putRec := callInMemoryHandler(t, h, http.MethodPut,
		"/2024-08-31/functions/rec-term-fn/recursion-config",
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
				pathPrefix = "/2024-08-31/functions/"
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

	// Set MaxExecutionEnvironments = 1
	maxEnv := int32(1)
	_, err := bk.PutFunctionScalingConfig(
		"scaling-fn",
		"$LATEST",
		&lambda.PutFunctionScalingConfigInput{
			FunctionScalingConfig: &lambda.FunctionScalingConfig{MaxExecutionEnvironments: &maxEnv},
		},
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

	// MaxExecutionEnvironments = 0 → no invocations permitted
	zero := int32(0)
	_, err := bk.PutFunctionScalingConfig(
		"scaling-zero-fn",
		"$LATEST",
		&lambda.PutFunctionScalingConfigInput{
			FunctionScalingConfig: &lambda.FunctionScalingConfig{MaxExecutionEnvironments: &zero},
		},
	)
	require.NoError(t, err)

	// No slots should be acquirable (returns false, nil because hasLimit=false and no reserved)
	// But MaxExecutionEnvironments=0 with scaling config enforcement should block:
	_, err = lambda.AcquireConcurrencySlot(bk, "scaling-zero-fn")
	// With MaxExecutionEnvironments=0, active(0) >= 0 is true so it blocks
	require.ErrorIs(t, err, lambda.ErrTooManyRequests)
}

// TestInvoke_ScalingConfig_EnforcedPerResolvedQualifier locks in gopherstack-tx8a5:
// Invoke now threads its resolved qualifier (an alias already resolved to the version
// it points at) into the scaling-config enforcement lookup, instead of hardcoding
// $LATEST. Both cases share one $LATEST invocation already holding the function's
// single shared active-execution slot (functionConcurrencies/activeConcurrencies stay
// keyed by function name alone, matching PutFunctionConcurrency's function-wide scope);
// only the qualifier being invoked differs.
func TestInvoke_ScalingConfig_EnforcedPerResolvedQualifier(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(t *testing.T, h *lambda.Handler, bk *lambda.InMemoryBackend)
		name        string
		fnName      string
		qualifier   string
		wantBlocked bool
	}{
		{
			name:   "unqualified_blocked_at_latest_own_limit",
			fnName: "inv-scale-a",
			setup: func(t *testing.T, _ *lambda.Handler, bk *lambda.InMemoryBackend) {
				t.Helper()

				latestMax := int32(1)
				_, err := bk.PutFunctionScalingConfig("inv-scale-a", "$LATEST",
					&lambda.PutFunctionScalingConfigInput{
						FunctionScalingConfig: &lambda.FunctionScalingConfig{MaxExecutionEnvironments: &latestMax},
					})
				require.NoError(t, err)

				held, acquireErr := lambda.AcquireConcurrencySlotQualified(bk, "inv-scale-a", "$LATEST")
				require.NoError(t, acquireErr)
				require.True(t, held)
			},
			qualifier:   "",
			wantBlocked: true,
		},
		{
			name:   "alias_uses_its_own_higher_limit_not_latest",
			fnName: "inv-scale-b",
			setup: func(t *testing.T, _ *lambda.Handler, bk *lambda.InMemoryBackend) {
				t.Helper()

				pub, pubErr := bk.PublishVersion("inv-scale-b", "")
				require.NoError(t, pubErr)

				_, aliasErr := bk.CreateAlias("inv-scale-b", &lambda.CreateAliasInput{
					Name:            "stable",
					FunctionVersion: pub.Version,
				})
				require.NoError(t, aliasErr)

				latestMax := int32(1)
				_, err := bk.PutFunctionScalingConfig("inv-scale-b", "$LATEST",
					&lambda.PutFunctionScalingConfigInput{
						FunctionScalingConfig: &lambda.FunctionScalingConfig{MaxExecutionEnvironments: &latestMax},
					})
				require.NoError(t, err)

				versionMax := int32(5)
				_, err = bk.PutFunctionScalingConfig("inv-scale-b", pub.Version,
					&lambda.PutFunctionScalingConfigInput{
						FunctionScalingConfig: &lambda.FunctionScalingConfig{MaxExecutionEnvironments: &versionMax},
					})
				require.NoError(t, err)

				// One in-flight $LATEST invocation exhausts $LATEST's limit of 1, but
				// leaves the alias's own version-1 limit of 5 untouched.
				held, acquireErr := lambda.AcquireConcurrencySlotQualified(bk, "inv-scale-b", "$LATEST")
				require.NoError(t, acquireErr)
				require.True(t, held)
			},
			qualifier:   "stable",
			wantBlocked: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, bk := newInMemoryHandler(t)
			rec := auditCreateFunction(t, h, baseImageFn(tc.fnName))
			require.Equal(t, http.StatusCreated, rec.Code)

			tc.setup(t, h, bk)

			_, _, _, statusCode, err := bk.InvokeFunctionWithQualifier(
				context.Background(),
				tc.fnName, tc.qualifier, "", "",
				lambda.InvocationTypeRequestResponse,
				[]byte("{}"),
			)

			if tc.wantBlocked {
				require.Error(t, err)
				require.ErrorIs(t, err, lambda.ErrTooManyRequests)
				assert.Equal(t, http.StatusTooManyRequests, statusCode)

				return
			}

			// Not blocked by the scaling limit: it may still fail past the concurrency
			// check (no real container runtime in this unit test), but never with
			// TooManyRequests, which would mean it was checked against $LATEST's
			// exhausted limit instead of its own.
			assert.NotEqual(t, http.StatusTooManyRequests, statusCode)

			if err != nil {
				assert.NotErrorIs(t, err, lambda.ErrTooManyRequests)
			}
		})
	}
}
