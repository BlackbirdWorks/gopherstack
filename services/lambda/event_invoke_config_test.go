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
			closeBackend(t, bk)
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

// TestFunctionEventInvokeConfig_LastModifiedIsEpochSeconds locks in a
// wire-shape bug found by field-diffing against the aws-sdk-go-v2 deserializer
// (deserializers.go's PutFunctionEventInvokeConfig/GetFunctionEventInvokeConfig
// case for "LastModified" parses a json.Number, not a string): unlike
// FunctionConfiguration.LastModified (an ISO8601 string),
// FunctionEventInvokeConfig.LastModified must serialize as a JSON number of
// seconds since the Unix epoch, or a real aws-sdk-go-v2 client would reject
// the response with "expected Timestamp to be a JSON Number, got string
// instead".
func TestFunctionEventInvokeConfig_LastModifiedIsEpochSeconds(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "eic-epoch-fn")

	rec := callInMemoryHandler(t, h, http.MethodPut,
		"/2015-03-31/functions/eic-epoch-fn/event-invoke-config", `{"MaximumRetryAttempts":1}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

	lastModified, ok := raw["LastModified"]
	require.True(t, ok, "response must include LastModified")

	numeric, isNumber := lastModified.(float64)
	require.True(t, isNumber, "LastModified must be a JSON number (epoch seconds), got %T: %v",
		lastModified, lastModified)
	assert.Greater(t, numeric, float64(0))
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
			closeBackend(t, bk)
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
			closeBackend(t, bk)
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
			closeBackend(t, bk)
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
			closeBackend(t, bk)
			h := lambda.NewHandler(bk)
			h.DefaultRegion = "us-east-1"
			h.AccountID = "000000000000"

			if tt.setup != nil {
				tt.setup(t, bk)
			}

			path := "/2015-03-31/functions/" + tt.funcName + "/event-invoke-config/list"
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
			closeBackend(t, bk)

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

// --- FunctionEventInvokeConfig tests ---

func TestFunctionEventInvokeConfig_PutGetUpdateDeleteList(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	fnName := "event-invoke-fn"
	createFunctionForTest(t, h, fnName)

	// Put event invoke config
	rec := callInMemoryHandler(
		t, h, http.MethodPut,
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
	rec = callInMemoryHandler(
		t, h, http.MethodPost,
		"/2015-03-31/functions/"+fnName+"/event-invoke-config",
		`{"MaximumRetryAttempts":1}`,
	)
	require.Equal(t, http.StatusOK, rec.Code)

	// List
	rec = callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/"+fnName+"/event-invoke-config/list", "{}")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), fnName)

	// Delete
	rec = callInMemoryHandler(t, h, http.MethodDelete,
		"/2015-03-31/functions/"+fnName+"/event-invoke-config", "{}")
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

// ============================================================
// FunctionEventInvokeConfig
// ============================================================

func TestEventInvokeConfig_Lifecycle(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "eic-fn")

	// Put config
	maxRetry := 2
	maxAge := 3600
	body := fmt.Sprintf(`{"MaximumRetryAttempts":%d,"MaximumEventAgeInSeconds":%d}`, maxRetry, maxAge)
	putRec := callInMemoryHandler(t, h, http.MethodPut,
		"/2015-03-31/functions/eic-fn/event-invoke-config", body)
	require.Equal(t, http.StatusOK, putRec.Code)

	var cfg lambda.FunctionEventInvokeConfig
	require.NoError(t, json.NewDecoder(putRec.Body).Decode(&cfg))
	require.NotNil(t, cfg.MaximumRetryAttempts)
	assert.Equal(t, maxRetry, *cfg.MaximumRetryAttempts)
	require.NotNil(t, cfg.MaximumEventAgeInSeconds)
	assert.Equal(t, maxAge, *cfg.MaximumEventAgeInSeconds)

	// Get config
	getRec := callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/eic-fn/event-invoke-config", "")
	require.Equal(t, http.StatusOK, getRec.Code)

	var getCfg lambda.FunctionEventInvokeConfig
	require.NoError(t, json.NewDecoder(getRec.Body).Decode(&getCfg))
	assert.Equal(t, maxRetry, *getCfg.MaximumRetryAttempts)

	// Update config
	updRec := callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/functions/eic-fn/event-invoke-config",
		`{"MaximumRetryAttempts":0}`)
	require.Equal(t, http.StatusOK, updRec.Code)

	// List configs
	listRec := callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/eic-fn/event-invoke-config/list", "")
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut lambda.ListFunctionEventInvokeConfigsOutput
	require.NoError(t, json.NewDecoder(listRec.Body).Decode(&listOut))
	assert.NotEmpty(t, listOut.FunctionEventInvokeConfigs)

	// Delete config
	delRec := callInMemoryHandler(t, h, http.MethodDelete,
		"/2015-03-31/functions/eic-fn/event-invoke-config", "")
	assert.Equal(t, http.StatusNoContent, delRec.Code)

	// Get after delete → 404
	getRec2 := callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/eic-fn/event-invoke-config", "")
	assert.Equal(t, http.StatusNotFound, getRec2.Code)
}

func TestEventInvokeConfig_WithDestinations(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "eic-dest-fn")

	body := `{
		"DestinationConfig":{
			"OnSuccess":{"Destination":"arn:aws:sqs:us-east-1:000000000000:success-q"},
			"OnFailure":{"Destination":"arn:aws:sqs:us-east-1:000000000000:failure-q"}
		}
	}`
	rec := callInMemoryHandler(t, h, http.MethodPut,
		"/2015-03-31/functions/eic-dest-fn/event-invoke-config", body)
	require.Equal(t, http.StatusOK, rec.Code)

	var cfg lambda.FunctionEventInvokeConfig
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&cfg))
	require.NotNil(t, cfg.DestinationConfig)
	require.NotNil(t, cfg.DestinationConfig.OnSuccess)
	assert.Contains(t, cfg.DestinationConfig.OnSuccess.Destination, "success-q")
	require.NotNil(t, cfg.DestinationConfig.OnFailure)
	assert.Contains(t, cfg.DestinationConfig.OnFailure.Destination, "failure-q")
}

// TestFunctionDestinations verifies PutFunctionEventInvokeConfig with DestinationConfig.
func TestFunctionDestinations(t *testing.T) {
	t.Parallel()

	h, bk := newInMemoryHandler(t)

	require.NoError(t, bk.CreateFunction(&lambda.FunctionConfiguration{
		FunctionName: "dest-fn",
		PackageType:  lambda.PackageTypeImage,
		ImageURI:     "test:latest",
		State:        lambda.FunctionStateActive,
	}))

	body := `{
		"MaximumRetryAttempts":2,
		"MaximumEventAgeInSeconds":300,
		"DestinationConfig":{
			"OnSuccess":{"Destination":"arn:aws:sqs:us-east-1:000000000000:success-q"},
			"OnFailure":{"Destination":"arn:aws:sqs:us-east-1:000000000000:failure-q"}
		}
	}`

	rec := callInMemoryHandler(t, h, http.MethodPut,
		"/2019-09-30/functions/dest-fn/event-invoke-config", body)
	require.Equal(t, http.StatusOK, rec.Code)

	var out lambda.FunctionEventInvokeConfig
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.NotNil(t, out.DestinationConfig)
	require.NotNil(t, out.DestinationConfig.OnSuccess)
	require.NotNil(t, out.DestinationConfig.OnFailure)
	assert.Equal(t, "arn:aws:sqs:us-east-1:000000000000:success-q", out.DestinationConfig.OnSuccess.Destination)
	assert.Equal(t, "arn:aws:sqs:us-east-1:000000000000:failure-q", out.DestinationConfig.OnFailure.Destination)

	// GetFunctionEventInvokeConfig.
	rec = callInMemoryHandler(t, h, http.MethodGet,
		"/2019-09-30/functions/dest-fn/event-invoke-config", "")
	require.Equal(t, http.StatusOK, rec.Code)
}
