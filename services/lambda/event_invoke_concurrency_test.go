package lambda_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambda"
)

// ---- helpers ----

// ---- PutFunctionEventInvokeConfig tests ----

func TestPutFunctionEventInvokeConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(*testing.T, *lambda.InMemoryBackend)
		wantRetries *int
		wantAge     *int
		body        string
		funcName    string
		name        string
		wantErrType string
		wantCode    int
	}{
		{
			name:     "success_basic",
			funcName: "put-eic-fn",
			body:     `{}`,
			setup: func(t *testing.T, b *lambda.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.CreateFunction(&lambda.FunctionConfiguration{
					FunctionName: "put-eic-fn",
					FunctionArn:  "arn:aws:lambda:us-east-1:000000000000:function:put-eic-fn",
					PackageType:  lambda.PackageTypeImage,
					ImageURI:     "test:latest",
				}))
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "success_with_retry_and_age",
			funcName: "put-eic-fn2",
			body:     `{"MaximumRetryAttempts":1,"MaximumEventAgeInSeconds":300}`,
			setup: func(t *testing.T, b *lambda.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.CreateFunction(&lambda.FunctionConfiguration{
					FunctionName: "put-eic-fn2",
					FunctionArn:  "arn:aws:lambda:us-east-1:000000000000:function:put-eic-fn2",
					PackageType:  lambda.PackageTypeImage,
					ImageURI:     "test:latest",
				}))
			},
			wantCode:    http.StatusOK,
			wantRetries: new(1),
			wantAge:     new(300),
		},
		{
			name:     "success_with_destination",
			funcName: "put-eic-fn3",
			body:     `{"DestinationConfig":{"OnFailure":{"Destination":"arn:aws:sqs:us-east-1:000000000000:dlq"}}}`,
			setup: func(t *testing.T, b *lambda.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.CreateFunction(&lambda.FunctionConfiguration{
					FunctionName: "put-eic-fn3",
					FunctionArn:  "arn:aws:lambda:us-east-1:000000000000:function:put-eic-fn3",
					PackageType:  lambda.PackageTypeImage,
					ImageURI:     "test:latest",
				}))
			},
			wantCode: http.StatusOK,
		},
		{
			name:        "function_not_found",
			funcName:    "nonexistent",
			body:        `{}`,
			wantCode:    http.StatusNotFound,
			wantErrType: "ResourceNotFoundException",
		},
		{
			name:     "invalid_retry_attempts",
			funcName: "put-eic-fn4",
			body:     `{"MaximumRetryAttempts":5}`,
			setup: func(t *testing.T, b *lambda.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.CreateFunction(&lambda.FunctionConfiguration{
					FunctionName: "put-eic-fn4",
					FunctionArn:  "arn:aws:lambda:us-east-1:000000000000:function:put-eic-fn4",
					PackageType:  lambda.PackageTypeImage,
					ImageURI:     "test:latest",
				}))
			},
			wantCode:    http.StatusBadRequest,
			wantErrType: "InvalidParameterValueException",
		},
		{
			name:     "invalid_event_age",
			funcName: "put-eic-fn5",
			body:     `{"MaximumEventAgeInSeconds":10}`,
			setup: func(t *testing.T, b *lambda.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.CreateFunction(&lambda.FunctionConfiguration{
					FunctionName: "put-eic-fn5",
					FunctionArn:  "arn:aws:lambda:us-east-1:000000000000:function:put-eic-fn5",
					PackageType:  lambda.PackageTypeImage,
					ImageURI:     "test:latest",
				}))
			},
			wantCode:    http.StatusBadRequest,
			wantErrType: "InvalidParameterValueException",
		},
		{
			name:        "invalid_json",
			funcName:    "irrelevant",
			body:        `not-json{`,
			wantCode:    http.StatusBadRequest,
			wantErrType: "InvalidParameterValueException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "000000000000", "us-east-1")
			h := lambda.NewHandler(bk)
			h.DefaultRegion = "us-east-1"
			h.AccountID = "000000000000"

			if tt.setup != nil {
				tt.setup(t, bk)
			}

			path := "/2015-03-31/functions/" + tt.funcName + "/event-invoke-config"
			rec := callHandler(t, h, http.MethodPut, path, tt.body, nil)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantErrType != "" {
				assertLambdaError(t, rec, tt.wantErrType)

				return
			}

			if tt.wantRetries != nil || tt.wantAge != nil {
				var cfg lambda.FunctionEventInvokeConfig
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cfg))

				if tt.wantRetries != nil {
					require.NotNil(t, cfg.MaximumRetryAttempts)
					assert.Equal(t, *tt.wantRetries, *cfg.MaximumRetryAttempts)
				}

				if tt.wantAge != nil {
					require.NotNil(t, cfg.MaximumEventAgeInSeconds)
					assert.Equal(t, *tt.wantAge, *cfg.MaximumEventAgeInSeconds)
				}
			}
		})
	}
}

// ---- GetFunctionEventInvokeConfig tests ----

func TestGetFunctionEventInvokeConfig(t *testing.T) {
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
			funcName: "get-eic-fn",
			setup: func(t *testing.T, b *lambda.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.CreateFunction(&lambda.FunctionConfiguration{
					FunctionName: "get-eic-fn",
					FunctionArn:  "arn:aws:lambda:us-east-1:000000000000:function:get-eic-fn",
					PackageType:  lambda.PackageTypeImage,
					ImageURI:     "test:latest",
				}))
				_, err := b.PutFunctionEventInvokeConfig("get-eic-fn", &lambda.PutFunctionEventInvokeConfigInput{
					MaximumRetryAttempts: new(2),
				})
				require.NoError(t, err)
			},
			wantCode: http.StatusOK,
		},
		{
			name:        "function_not_found",
			funcName:    "no-fn",
			wantCode:    http.StatusNotFound,
			wantErrType: "ResourceNotFoundException",
		},
		{
			name:     "config_not_found",
			funcName: "fn-no-config",
			setup: func(t *testing.T, b *lambda.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.CreateFunction(&lambda.FunctionConfiguration{
					FunctionName: "fn-no-config",
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
			h := lambda.NewHandler(bk)
			h.DefaultRegion = "us-east-1"
			h.AccountID = "000000000000"

			if tt.setup != nil {
				tt.setup(t, bk)
			}

			path := "/2015-03-31/functions/" + tt.funcName + "/event-invoke-config"
			rec := callHandler(t, h, http.MethodGet, path, "", nil)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantErrType != "" {
				assertLambdaError(t, rec, tt.wantErrType)
			}
		})
	}
}

// ---- UpdateFunctionEventInvokeConfig tests ----

func TestUpdateFunctionEventInvokeConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(*testing.T, *lambda.InMemoryBackend)
		wantRetries *int
		body        string
		funcName    string
		name        string
		wantErrType string
		wantCode    int
	}{
		{
			name:     "success_updates_retries",
			funcName: "upd-eic-fn",
			body:     `{"MaximumRetryAttempts":0}`,
			setup: func(t *testing.T, b *lambda.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.CreateFunction(&lambda.FunctionConfiguration{
					FunctionName: "upd-eic-fn",
					FunctionArn:  "arn:aws:lambda:us-east-1:000000000000:function:upd-eic-fn",
					PackageType:  lambda.PackageTypeImage,
					ImageURI:     "test:latest",
				}))
				_, err := b.PutFunctionEventInvokeConfig("upd-eic-fn", &lambda.PutFunctionEventInvokeConfigInput{
					MaximumRetryAttempts: new(2),
				})
				require.NoError(t, err)
			},
			wantCode:    http.StatusOK,
			wantRetries: new(0),
		},
		{
			name:        "config_not_found",
			funcName:    "upd-eic-no-config",
			body:        `{"MaximumRetryAttempts":1}`,
			wantCode:    http.StatusNotFound,
			wantErrType: "ResourceNotFoundException",
			setup: func(t *testing.T, b *lambda.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.CreateFunction(&lambda.FunctionConfiguration{
					FunctionName: "upd-eic-no-config",
					PackageType:  lambda.PackageTypeImage,
					ImageURI:     "test:latest",
				}))
			},
		},
		{
			name:        "function_not_found",
			funcName:    "upd-eic-no-fn",
			body:        `{}`,
			wantCode:    http.StatusNotFound,
			wantErrType: "ResourceNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "000000000000", "us-east-1")
			h := lambda.NewHandler(bk)
			h.DefaultRegion = "us-east-1"
			h.AccountID = "000000000000"

			if tt.setup != nil {
				tt.setup(t, bk)
			}

			path := "/2015-03-31/functions/" + tt.funcName + "/event-invoke-config"
			rec := callHandler(t, h, http.MethodPost, path, tt.body, nil)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantErrType != "" {
				assertLambdaError(t, rec, tt.wantErrType)

				return
			}

			if tt.wantRetries != nil {
				var cfg lambda.FunctionEventInvokeConfig
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cfg))
				require.NotNil(t, cfg.MaximumRetryAttempts)
				assert.Equal(t, *tt.wantRetries, *cfg.MaximumRetryAttempts)
			}
		})
	}
}

// ---- DeleteFunctionEventInvokeConfig tests ----

func TestDeleteFunctionEventInvokeConfig(t *testing.T) {
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
			funcName: "del-eic-fn",
			setup: func(t *testing.T, b *lambda.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.CreateFunction(&lambda.FunctionConfiguration{
					FunctionName: "del-eic-fn",
					PackageType:  lambda.PackageTypeImage,
					ImageURI:     "test:latest",
				}))
				_, err := b.PutFunctionEventInvokeConfig("del-eic-fn", &lambda.PutFunctionEventInvokeConfigInput{})
				require.NoError(t, err)
			},
			wantCode: http.StatusNoContent,
		},
		{
			name:        "function_not_found",
			funcName:    "del-eic-no-fn",
			wantCode:    http.StatusNotFound,
			wantErrType: "ResourceNotFoundException",
		},
		{
			name:     "config_not_found",
			funcName: "del-eic-no-config",
			setup: func(t *testing.T, b *lambda.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.CreateFunction(&lambda.FunctionConfiguration{
					FunctionName: "del-eic-no-config",
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
			h := lambda.NewHandler(bk)
			h.DefaultRegion = "us-east-1"
			h.AccountID = "000000000000"

			if tt.setup != nil {
				tt.setup(t, bk)
			}

			path := "/2015-03-31/functions/" + tt.funcName + "/event-invoke-config"
			rec := callHandler(t, h, http.MethodDelete, path, "", nil)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantErrType != "" {
				assertLambdaError(t, rec, tt.wantErrType)
			}
		})
	}
}

// ---- ListFunctionEventInvokeConfigs tests ----

func TestListFunctionEventInvokeConfigs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(*testing.T, *lambda.InMemoryBackend)
		funcName    string
		name        string
		wantErrType string
		wantCode    int
		wantCount   int
	}{
		{
			name:     "no_configs",
			funcName: "list-eic-fn-empty",
			setup: func(t *testing.T, b *lambda.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.CreateFunction(&lambda.FunctionConfiguration{
					FunctionName: "list-eic-fn-empty",
					PackageType:  lambda.PackageTypeImage,
					ImageURI:     "test:latest",
				}))
			},
			wantCode:  http.StatusOK,
			wantCount: 0,
		},
		{
			name:     "with_one_config",
			funcName: "list-eic-fn-one",
			setup: func(t *testing.T, b *lambda.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.CreateFunction(&lambda.FunctionConfiguration{
					FunctionName: "list-eic-fn-one",
					PackageType:  lambda.PackageTypeImage,
					ImageURI:     "test:latest",
				}))
				_, err := b.PutFunctionEventInvokeConfig("list-eic-fn-one", &lambda.PutFunctionEventInvokeConfigInput{
					MaximumRetryAttempts: new(1),
				})
				require.NoError(t, err)
			},
			wantCode:  http.StatusOK,
			wantCount: 1,
		},
		{
			name:        "function_not_found",
			funcName:    "list-eic-no-fn",
			wantCode:    http.StatusNotFound,
			wantErrType: "ResourceNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "000000000000", "us-east-1")
			h := lambda.NewHandler(bk)
			h.DefaultRegion = "us-east-1"
			h.AccountID = "000000000000"

			if tt.setup != nil {
				tt.setup(t, bk)
			}

			path := "/2015-03-31/functions/" + tt.funcName + "/event-invoke-configs"
			rec := callHandler(t, h, http.MethodGet, path, "", nil)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantErrType != "" {
				assertLambdaError(t, rec, tt.wantErrType)

				return
			}

			var out lambda.ListFunctionEventInvokeConfigsOutput
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Len(t, out.FunctionEventInvokeConfigs, tt.wantCount)
		})
	}
}

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

// ---- Backend: PutFunctionEventInvokeConfig tests ----

func TestBackend_PutFunctionEventInvokeConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(*testing.T, *lambda.InMemoryBackend)
		input       *lambda.PutFunctionEventInvokeConfigInput
		wantRetries *int
		wantAge     *int
		funcName    string
		name        string
		wantErr     bool
	}{
		{
			name:     "success_replaces_existing",
			funcName: "be-put-eic",
			input: &lambda.PutFunctionEventInvokeConfigInput{
				MaximumRetryAttempts:     new(1),
				MaximumEventAgeInSeconds: new(120),
			},
			setup: func(t *testing.T, b *lambda.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.CreateFunction(&lambda.FunctionConfiguration{
					FunctionName: "be-put-eic",
					FunctionArn:  "arn:aws:lambda:us-east-1:000000000000:function:be-put-eic",
					PackageType:  lambda.PackageTypeImage,
					ImageURI:     "test:latest",
				}))
				_, err := b.PutFunctionEventInvokeConfig("be-put-eic", &lambda.PutFunctionEventInvokeConfigInput{
					MaximumRetryAttempts: new(2),
				})
				require.NoError(t, err)
			},
			wantRetries: new(1),
			wantAge:     new(120),
		},
		{
			name:     "function_not_found",
			funcName: "be-put-eic-no-fn",
			input:    &lambda.PutFunctionEventInvokeConfigInput{},
			wantErr:  true,
		},
		{
			name:     "invalid_max_retries",
			funcName: "be-put-eic-bad",
			input:    &lambda.PutFunctionEventInvokeConfigInput{MaximumRetryAttempts: new(3)},
			setup: func(t *testing.T, b *lambda.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.CreateFunction(&lambda.FunctionConfiguration{
					FunctionName: "be-put-eic-bad",
					PackageType:  lambda.PackageTypeImage,
					ImageURI:     "test:latest",
				}))
			},
			wantErr: true,
		},
		{
			name:     "invalid_event_age_too_low",
			funcName: "be-put-eic-age-low",
			input:    &lambda.PutFunctionEventInvokeConfigInput{MaximumEventAgeInSeconds: new(30)},
			setup: func(t *testing.T, b *lambda.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.CreateFunction(&lambda.FunctionConfiguration{
					FunctionName: "be-put-eic-age-low",
					PackageType:  lambda.PackageTypeImage,
					ImageURI:     "test:latest",
				}))
			},
			wantErr: true,
		},
		{
			name:     "invalid_event_age_too_high",
			funcName: "be-put-eic-age-high",
			input:    &lambda.PutFunctionEventInvokeConfigInput{MaximumEventAgeInSeconds: new(30000)},
			setup: func(t *testing.T, b *lambda.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.CreateFunction(&lambda.FunctionConfiguration{
					FunctionName: "be-put-eic-age-high",
					PackageType:  lambda.PackageTypeImage,
					ImageURI:     "test:latest",
				}))
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "000000000000", "us-east-1")

			if tt.setup != nil {
				tt.setup(t, bk)
			}

			cfg, err := bk.PutFunctionEventInvokeConfig(tt.funcName, tt.input)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, cfg)
			assert.NotEmpty(t, cfg.FunctionArn)

			if tt.wantRetries != nil {
				require.NotNil(t, cfg.MaximumRetryAttempts)
				assert.Equal(t, *tt.wantRetries, *cfg.MaximumRetryAttempts)
			}

			if tt.wantAge != nil {
				require.NotNil(t, cfg.MaximumEventAgeInSeconds)
				assert.Equal(t, *tt.wantAge, *cfg.MaximumEventAgeInSeconds)
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
