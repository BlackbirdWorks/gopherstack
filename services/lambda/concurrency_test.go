package lambda_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambda"
)

// ---- PutFunctionConcurrency tests ----

func TestPutFunctionConcurrency(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*testing.T, *lambda.InMemoryBackend)
		body         string
		funcName     string
		name         string
		wantErrType  string
		wantCode     int
		wantReserved int
	}{
		{
			name:     "success_set_concurrency",
			funcName: "put-conc-fn",
			body:     `{"ReservedConcurrentExecutions":5}`,
			setup: func(t *testing.T, b *lambda.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.CreateFunction(&lambda.FunctionConfiguration{
					FunctionName: "put-conc-fn",
					PackageType:  lambda.PackageTypeImage,
					ImageURI:     "test:latest",
				}))
			},
			wantCode:     http.StatusOK,
			wantReserved: 5,
		},
		{
			name:     "success_set_zero_disables",
			funcName: "put-conc-fn-zero",
			body:     `{"ReservedConcurrentExecutions":0}`,
			setup: func(t *testing.T, b *lambda.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.CreateFunction(&lambda.FunctionConfiguration{
					FunctionName: "put-conc-fn-zero",
					PackageType:  lambda.PackageTypeImage,
					ImageURI:     "test:latest",
				}))
			},
			wantCode:     http.StatusOK,
			wantReserved: 0,
		},
		{
			name:        "function_not_found",
			funcName:    "put-conc-no-fn",
			body:        `{"ReservedConcurrentExecutions":1}`,
			wantCode:    http.StatusNotFound,
			wantErrType: "ResourceNotFoundException",
		},
		{
			name:        "invalid_json",
			funcName:    "irrelevant",
			body:        `not-json`,
			wantCode:    http.StatusBadRequest,
			wantErrType: "InvalidParameterValueException",
		},
		{
			name:        "empty_body",
			funcName:    "irrelevant",
			body:        ``,
			wantCode:    http.StatusBadRequest,
			wantErrType: "InvalidParameterValueException",
		},
		{
			name:        "missing_reserved_field",
			funcName:    "irrelevant",
			body:        `{}`,
			wantCode:    http.StatusBadRequest,
			wantErrType: "InvalidParameterValueException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "000000000000", "us-east-1")
			closeBackend(t, bk)
			h := lambda.NewHandler(bk)
			h.DefaultRegion = "us-east-1"
			h.AccountID = "000000000000"

			if tt.setup != nil {
				tt.setup(t, bk)
			}

			path := "/2015-03-31/functions/" + tt.funcName + "/concurrency"
			rec := callHandler(t, h, http.MethodPut, path, tt.body, nil)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantErrType != "" {
				assertLambdaError(t, rec, tt.wantErrType)

				return
			}

			var out lambda.FunctionConcurrency
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Equal(t, tt.wantReserved, out.ReservedConcurrentExecutions)
		})
	}
}

// ---- GetFunctionConcurrency tests ----

func TestGetFunctionConcurrency(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*testing.T, *lambda.InMemoryBackend)
		funcName     string
		name         string
		wantErrType  string
		wantCode     int
		wantReserved int
	}{
		{
			name:     "success",
			funcName: "get-conc-fn",
			setup: func(t *testing.T, b *lambda.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.CreateFunction(&lambda.FunctionConfiguration{
					FunctionName: "get-conc-fn",
					PackageType:  lambda.PackageTypeImage,
					ImageURI:     "test:latest",
				}))
				_, err := b.PutFunctionConcurrency("get-conc-fn", 3)
				require.NoError(t, err)
			},
			wantCode:     http.StatusOK,
			wantReserved: 3,
		},
		{
			name:        "function_not_found",
			funcName:    "get-conc-no-fn",
			wantCode:    http.StatusNotFound,
			wantErrType: "ResourceNotFoundException",
		},
		{
			name:     "no_concurrency_set",
			funcName: "get-conc-no-limit",
			setup: func(t *testing.T, b *lambda.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.CreateFunction(&lambda.FunctionConfiguration{
					FunctionName: "get-conc-no-limit",
					PackageType:  lambda.PackageTypeImage,
					ImageURI:     "test:latest",
				}))
			},
			wantCode:    http.StatusNotFound,
			wantErrType: "ResourceNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "000000000000", "us-east-1")
			closeBackend(t, bk)
			h := lambda.NewHandler(bk)
			h.DefaultRegion = "us-east-1"
			h.AccountID = "000000000000"

			if tt.setup != nil {
				tt.setup(t, bk)
			}

			path := "/2015-03-31/functions/" + tt.funcName + "/concurrency"
			rec := callHandler(t, h, http.MethodGet, path, "", nil)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantErrType != "" {
				assertLambdaError(t, rec, tt.wantErrType)

				return
			}

			var out lambda.FunctionConcurrency
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Equal(t, tt.wantReserved, out.ReservedConcurrentExecutions)
		})
	}
}

// ---- DeleteFunctionConcurrency tests ----

func TestDeleteFunctionConcurrency(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(*testing.T, *lambda.InMemoryBackend)
		funcName    string
		name        string
		wantErrType string
		wantCode    int
	}{
		{
			name:     "success",
			funcName: "del-conc-fn",
			setup: func(t *testing.T, b *lambda.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.CreateFunction(&lambda.FunctionConfiguration{
					FunctionName: "del-conc-fn",
					PackageType:  lambda.PackageTypeImage,
					ImageURI:     "test:latest",
				}))
				_, err := b.PutFunctionConcurrency("del-conc-fn", 5)
				require.NoError(t, err)
			},
			wantCode: http.StatusNoContent,
		},
		{
			name:        "function_not_found",
			funcName:    "del-conc-no-fn",
			wantCode:    http.StatusNotFound,
			wantErrType: "ResourceNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "000000000000", "us-east-1")
			closeBackend(t, bk)
			h := lambda.NewHandler(bk)
			h.DefaultRegion = "us-east-1"
			h.AccountID = "000000000000"

			if tt.setup != nil {
				tt.setup(t, bk)
			}

			path := "/2015-03-31/functions/" + tt.funcName + "/concurrency"
			rec := callHandler(t, h, http.MethodDelete, path, "", nil)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantErrType != "" {
				assertLambdaError(t, rec, tt.wantErrType)
			}
		})
	}
}

// ---- Concurrency enforcement tests ----

func TestConcurrencyEnforcement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup          func(*testing.T, *lambda.InMemoryBackend)
		funcName       string
		name           string
		invocationType string
		wantErrType    string
		wantCode       int
	}{
		{
			name:           "reserved_zero_blocks_request_response",
			funcName:       "conc-zero-fn",
			invocationType: lambda.InvocationTypeRequestResponse,
			setup: func(t *testing.T, b *lambda.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.CreateFunction(&lambda.FunctionConfiguration{
					FunctionName: "conc-zero-fn",
					PackageType:  lambda.PackageTypeImage,
					ImageURI:     "test:latest",
				}))
				_, err := b.PutFunctionConcurrency("conc-zero-fn", 0)
				require.NoError(t, err)
			},
			wantCode:    http.StatusTooManyRequests,
			wantErrType: "TooManyRequestsException",
		},
		{
			name:           "reserved_zero_blocks_event_invocation",
			funcName:       "conc-zero-event-fn",
			invocationType: lambda.InvocationTypeEvent,
			setup: func(t *testing.T, b *lambda.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.CreateFunction(&lambda.FunctionConfiguration{
					FunctionName: "conc-zero-event-fn",
					PackageType:  lambda.PackageTypeImage,
					ImageURI:     "test:latest",
				}))
				_, err := b.PutFunctionConcurrency("conc-zero-event-fn", 0)
				require.NoError(t, err)
			},
			wantCode:    http.StatusTooManyRequests,
			wantErrType: "TooManyRequestsException",
		},
		{
			name:           "reserved_nonzero_enforced_for_event",
			funcName:       "conc-event-limit-fn",
			invocationType: lambda.InvocationTypeEvent,
			setup: func(t *testing.T, b *lambda.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.CreateFunction(&lambda.FunctionConfiguration{
					FunctionName: "conc-event-limit-fn",
					PackageType:  lambda.PackageTypeImage,
					ImageURI:     "test:latest",
				}))
				_, err := b.PutFunctionConcurrency("conc-event-limit-fn", 1)
				require.NoError(t, err)
				// Manually acquire the only available slot to simulate a running execution.
				_, err = lambda.AcquireConcurrencySlot(b, "conc-event-limit-fn")
				require.NoError(t, err)
			},
			wantCode:    http.StatusTooManyRequests,
			wantErrType: "TooManyRequestsException",
		},
		{
			name:           "no_concurrency_limit_allows_invocation",
			funcName:       "conc-unlimited-fn",
			invocationType: lambda.InvocationTypeRequestResponse,
			setup: func(t *testing.T, b *lambda.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.CreateFunction(&lambda.FunctionConfiguration{
					FunctionName: "conc-unlimited-fn",
					PackageType:  lambda.PackageTypeImage,
					ImageURI:     "test:latest",
				}))
				// No concurrency limit set — should fail with ServiceException (no docker), not 429.
			},
			wantCode:    http.StatusInternalServerError,
			wantErrType: "ServiceException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "000000000000", "us-east-1")
			closeBackend(t, bk)
			h := lambda.NewHandler(bk)
			h.DefaultRegion = "us-east-1"
			h.AccountID = "000000000000"

			if tt.setup != nil {
				tt.setup(t, bk)
			}

			path := "/2015-03-31/functions/" + tt.funcName + "/invocations"
			headers := map[string]string{"X-Amz-Invocation-Type": tt.invocationType}
			rec := callHandler(t, h, http.MethodPost, path, `{}`, headers)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantErrType != "" {
				assertLambdaError(t, rec, tt.wantErrType)
			}
		})
	}
}

// ---- GetFunction includes ReservedConcurrentExecutions ----

func TestGetFunction_ReservedConcurrentExecutions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*testing.T, *lambda.InMemoryBackend)
		wantReserved *int
		funcName     string
		name         string
		wantCode     int
	}{
		{
			name:     "no_concurrency_set",
			funcName: "gf-no-conc",
			setup: func(t *testing.T, b *lambda.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.CreateFunction(&lambda.FunctionConfiguration{
					FunctionName: "gf-no-conc",
					PackageType:  lambda.PackageTypeImage,
					ImageURI:     "test:latest",
				}))
			},
			wantCode:     http.StatusOK,
			wantReserved: nil,
		},
		{
			name:     "with_concurrency_set",
			funcName: "gf-with-conc",
			setup: func(t *testing.T, b *lambda.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.CreateFunction(&lambda.FunctionConfiguration{
					FunctionName: "gf-with-conc",
					PackageType:  lambda.PackageTypeImage,
					ImageURI:     "test:latest",
				}))
				_, err := b.PutFunctionConcurrency("gf-with-conc", 7)
				require.NoError(t, err)
			},
			wantCode:     http.StatusOK,
			wantReserved: new(7),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "000000000000", "us-east-1")
			closeBackend(t, bk)
			h := lambda.NewHandler(bk)
			h.DefaultRegion = "us-east-1"
			h.AccountID = "000000000000"

			if tt.setup != nil {
				tt.setup(t, bk)
			}

			path := "/2015-03-31/functions/" + tt.funcName
			rec := callHandler(t, h, http.MethodGet, path, "", nil)
			assert.Equal(t, tt.wantCode, rec.Code)

			var out struct {
				Configuration *lambda.FunctionConfiguration `json:"Configuration"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			require.NotNil(t, out.Configuration)

			if tt.wantReserved == nil {
				assert.Nil(t, out.Configuration.ReservedConcurrentExecutions)
			} else {
				require.NotNil(t, out.Configuration.ReservedConcurrentExecutions)
				assert.Equal(t, *tt.wantReserved, *out.Configuration.ReservedConcurrentExecutions)
			}
		})
	}
}

// ---- Backend: concurrency tests ----

func TestBackend_PutGetDeleteFunctionConcurrency(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*testing.T, *lambda.InMemoryBackend)
		funcName     string
		name         string
		putReserved  int
		wantErr      bool
		wantReserved int
	}{
		{
			name:     "put_and_get",
			funcName: "be-conc-fn",
			setup: func(t *testing.T, b *lambda.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.CreateFunction(&lambda.FunctionConfiguration{
					FunctionName: "be-conc-fn",
					PackageType:  lambda.PackageTypeImage,
					ImageURI:     "test:latest",
				}))
			},
			putReserved:  10,
			wantReserved: 10,
		},
		{
			name:        "function_not_found",
			funcName:    "be-conc-no-fn",
			putReserved: 5,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "000000000000", "us-east-1")
			closeBackend(t, bk)

			if tt.setup != nil {
				tt.setup(t, bk)
			}

			concurrency, err := bk.PutFunctionConcurrency(tt.funcName, tt.putReserved)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantReserved, concurrency.ReservedConcurrentExecutions)

			// Verify GetFunctionConcurrency returns the same value.
			got, getErr := bk.GetFunctionConcurrency(tt.funcName)
			require.NoError(t, getErr)
			assert.Equal(t, tt.wantReserved, got.ReservedConcurrentExecutions)

			// Verify DeleteFunctionConcurrency removes the limit.
			delErr := bk.DeleteFunctionConcurrency(tt.funcName)
			require.NoError(t, delErr)

			_, getAfterDel := bk.GetFunctionConcurrency(tt.funcName)
			require.Error(t, getAfterDel)
		})
	}
}

// ============================================================
// Concurrency (reserved)
// ============================================================

func TestConcurrency_PutGetDelete(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "conc-fn")

	// Put
	putRec := callInMemoryHandler(t, h, http.MethodPut,
		"/2017-10-31/functions/conc-fn/concurrency",
		`{"ReservedConcurrentExecutions":50}`)
	require.Equal(t, http.StatusOK, putRec.Code)

	var conc lambda.FunctionConcurrency
	require.NoError(t, json.NewDecoder(putRec.Body).Decode(&conc))
	assert.Equal(t, 50, conc.ReservedConcurrentExecutions)

	// Get
	getRec := callInMemoryHandler(t, h, http.MethodGet,
		"/2019-09-30/functions/conc-fn/concurrency", "")
	require.Equal(t, http.StatusOK, getRec.Code)

	var getConc lambda.FunctionConcurrency
	require.NoError(t, json.NewDecoder(getRec.Body).Decode(&getConc))
	assert.Equal(t, 50, getConc.ReservedConcurrentExecutions)

	// Delete
	delRec := callInMemoryHandler(t, h, http.MethodDelete,
		"/2017-10-31/functions/conc-fn/concurrency", "")
	assert.Equal(t, http.StatusNoContent, delRec.Code)

	// Get after delete → 404 (no reserved concurrency configured)
	getRec2 := callInMemoryHandler(t, h, http.MethodGet,
		"/2019-09-30/functions/conc-fn/concurrency", "")
	assert.Equal(t, http.StatusNotFound, getRec2.Code)
}

func TestConcurrency_PutZero(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "conc-zero-fn")

	// Setting 0 reserved concurrency throttles the function
	rec := callInMemoryHandler(t, h, http.MethodPut,
		"/2017-10-31/functions/conc-zero-fn/concurrency",
		`{"ReservedConcurrentExecutions":0}`)
	assert.Equal(t, http.StatusOK, rec.Code)
}
