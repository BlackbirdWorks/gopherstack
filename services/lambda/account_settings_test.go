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

// ============================================================
// AccountUsage — FunctionCount and TotalCodeSize
// ============================================================

func TestBatch3_AccountUsage_FunctionCount_Increments(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	rec0 := callInMemoryHandler(t, h, http.MethodGet, "/2016-08-19/account-settings", "")
	require.Equal(t, http.StatusOK, rec0.Code)

	var s0 lambda.AccountSettingsOutput
	require.NoError(t, json.NewDecoder(rec0.Body).Decode(&s0))
	require.NotNil(t, s0.AccountUsage)
	assert.Equal(t, 0, s0.AccountUsage.FunctionCount)

	createFunctionForTest(t, h, "usage-fn-1")
	createFunctionForTest(t, h, "usage-fn-2")

	rec2 := callInMemoryHandler(t, h, http.MethodGet, "/2016-08-19/account-settings", "")
	require.Equal(t, http.StatusOK, rec2.Code)

	var s2 lambda.AccountSettingsOutput
	require.NoError(t, json.NewDecoder(rec2.Body).Decode(&s2))
	require.NotNil(t, s2.AccountUsage)
	assert.Equal(t, 2, s2.AccountUsage.FunctionCount)
}

func TestBatch3_AccountUsage_AccountLimit_Fields(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	rec := callInMemoryHandler(t, h, http.MethodGet, "/2016-08-19/account-settings", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var s lambda.AccountSettingsOutput
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&s))
	require.NotNil(t, s.AccountLimit)
	assert.Positive(t, s.AccountLimit.TotalCodeSize)
	assert.Positive(t, s.AccountLimit.ConcurrentExecutions)
	assert.Positive(t, s.AccountLimit.UnreservedConcurrentExecutions)
	assert.Positive(t, s.AccountLimit.CodeSizeUnzipped)
	assert.Positive(t, s.AccountLimit.CodeSizeZipped)
}

// TestAuditLambda_GetAccountSettings_UnreservedConcurrency verifies
// UnreservedConcurrentExecutions decreases when reserved concurrency is set.
func TestAuditLambda_GetAccountSettings_UnreservedConcurrency(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		reservePerFn   int
		numFunctions   int
		wantUnreserved int
	}{
		{
			name:           "no reserved concurrency — all unreserved",
			numFunctions:   2,
			reservePerFn:   0,
			wantUnreserved: 1000,
		},
		{
			name:           "100 reserved → 900 unreserved",
			numFunctions:   1,
			reservePerFn:   100,
			wantUnreserved: 900,
		},
		{
			name:           "two functions each reserving 200 → 600 unreserved",
			numFunctions:   2,
			reservePerFn:   200,
			wantUnreserved: 600,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, bk := newInMemoryHandler(t)

			for i := range tc.numFunctions {
				fnName := fmt.Sprintf("concurrency-fn-%d", i)
				createFunctionForTest(t, h, fnName)

				if tc.reservePerFn > 0 {
					_, putErr := bk.PutFunctionConcurrency(fnName, tc.reservePerFn)
					require.NoError(t, putErr)
				}
			}

			rec := callInMemoryHandler(t, h, http.MethodGet, "/2016-08-19/account-settings", "")
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			limit, ok := out["AccountLimit"].(map[string]any)
			require.True(t, ok, "AccountLimit must be present")

			got, ok := limit["UnreservedConcurrentExecutions"].(float64)
			require.True(t, ok, "UnreservedConcurrentExecutions must be numeric")
			assert.Equal(t, tc.wantUnreserved, int(got))
		})
	}
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
