package lambda_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambda"
)

// ---- PutProvisionedConcurrencyConfig tests ----

func TestPutProvisionedConcurrencyConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup         func(*testing.T, *lambda.InMemoryBackend)
		body          string
		funcName      string
		qualifier     string
		name          string
		wantErrType   string
		wantRequested int
		wantCode      int
	}{
		{
			name:      "success_set_provisioned_concurrency",
			funcName:  "put-prov-fn",
			qualifier: "1",
			body:      `{"ProvisionedConcurrentExecutions":10}`,
			setup: func(t *testing.T, b *lambda.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.CreateFunction(&lambda.FunctionConfiguration{
					FunctionName: "put-prov-fn",
					PackageType:  lambda.PackageTypeImage,
					ImageURI:     "test:latest",
				}))
			},
			wantCode:      http.StatusCreated,
			wantRequested: 10,
		},
		{
			name:      "success_set_alias_qualifier",
			funcName:  "put-prov-alias-fn",
			qualifier: "myalias",
			body:      `{"ProvisionedConcurrentExecutions":5}`,
			setup: func(t *testing.T, b *lambda.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.CreateFunction(&lambda.FunctionConfiguration{
					FunctionName: "put-prov-alias-fn",
					PackageType:  lambda.PackageTypeImage,
					ImageURI:     "test:latest",
				}))
			},
			wantCode:      http.StatusCreated,
			wantRequested: 5,
		},
		{
			name:        "missing_qualifier",
			funcName:    "put-prov-no-qual",
			qualifier:   "",
			body:        `{"ProvisionedConcurrentExecutions":10}`,
			wantCode:    http.StatusBadRequest,
			wantErrType: "InvalidParameterValueException",
		},
		{
			name:        "function_not_found",
			funcName:    "put-prov-no-fn",
			qualifier:   "1",
			body:        `{"ProvisionedConcurrentExecutions":10}`,
			wantCode:    http.StatusNotFound,
			wantErrType: "ResourceNotFoundException",
		},
		{
			name:      "zero_executions_invalid",
			funcName:  "put-prov-zero-fn",
			qualifier: "1",
			body:      `{"ProvisionedConcurrentExecutions":0}`,
			setup: func(t *testing.T, b *lambda.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.CreateFunction(&lambda.FunctionConfiguration{
					FunctionName: "put-prov-zero-fn",
					PackageType:  lambda.PackageTypeImage,
					ImageURI:     "test:latest",
				}))
			},
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

			path := "/2015-03-31/functions/" + tt.funcName + "/provisioned-concurrency"
			if tt.qualifier != "" {
				path += "?Qualifier=" + tt.qualifier
			}

			rec := callHandler(t, h, http.MethodPut, path, tt.body, nil)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantErrType != "" {
				assertLambdaError(t, rec, tt.wantErrType)

				return
			}

			var out lambda.ProvisionedConcurrencyConfig
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Equal(t, tt.wantRequested, out.RequestedProvisionedConcurrentExecutions)
			assert.Equal(t, tt.wantRequested, out.AllocatedProvisionedConcurrentExecutions)
			assert.Equal(t, "READY", out.Status)
			assert.NotEmpty(t, out.LastModified)
		})
	}
}

// ---- GetProvisionedConcurrencyConfig tests ----

func TestGetProvisionedConcurrencyConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup         func(*testing.T, *lambda.InMemoryBackend)
		funcName      string
		qualifier     string
		name          string
		wantErrType   string
		wantRequested int
		wantCode      int
	}{
		{
			name:      "success",
			funcName:  "get-prov-fn",
			qualifier: "1",
			setup: func(t *testing.T, b *lambda.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.CreateFunction(&lambda.FunctionConfiguration{
					FunctionName: "get-prov-fn",
					PackageType:  lambda.PackageTypeImage,
					ImageURI:     "test:latest",
				}))
				_, err := b.PutProvisionedConcurrencyConfig("get-prov-fn", "1", 7)
				require.NoError(t, err)
			},
			wantCode:      http.StatusOK,
			wantRequested: 7,
		},
		{
			name:        "function_not_found",
			funcName:    "get-prov-no-fn",
			qualifier:   "1",
			wantCode:    http.StatusNotFound,
			wantErrType: "ResourceNotFoundException",
		},
		{
			name:      "config_not_found",
			funcName:  "get-prov-no-config",
			qualifier: "1",
			setup: func(t *testing.T, b *lambda.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.CreateFunction(&lambda.FunctionConfiguration{
					FunctionName: "get-prov-no-config",
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

			path := "/2015-03-31/functions/" + tt.funcName + "/provisioned-concurrency?Qualifier=" + tt.qualifier
			rec := callHandler(t, h, http.MethodGet, path, "", nil)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantErrType != "" {
				assertLambdaError(t, rec, tt.wantErrType)

				return
			}

			var out lambda.ProvisionedConcurrencyConfig
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Equal(t, tt.wantRequested, out.RequestedProvisionedConcurrentExecutions)
			assert.Equal(t, "READY", out.Status)
		})
	}
}

// ---- DeleteProvisionedConcurrencyConfig tests ----

func TestDeleteProvisionedConcurrencyConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(*testing.T, *lambda.InMemoryBackend)
		funcName    string
		qualifier   string
		name        string
		wantErrType string
		wantCode    int
	}{
		{
			name:      "success",
			funcName:  "del-prov-fn",
			qualifier: "1",
			setup: func(t *testing.T, b *lambda.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.CreateFunction(&lambda.FunctionConfiguration{
					FunctionName: "del-prov-fn",
					PackageType:  lambda.PackageTypeImage,
					ImageURI:     "test:latest",
				}))
				_, err := b.PutProvisionedConcurrencyConfig("del-prov-fn", "1", 5)
				require.NoError(t, err)
			},
			wantCode: http.StatusNoContent,
		},
		{
			name:        "missing_qualifier",
			funcName:    "del-prov-no-qual",
			qualifier:   "",
			wantCode:    http.StatusBadRequest,
			wantErrType: "InvalidParameterValueException",
		},
		{
			name:        "function_not_found",
			funcName:    "del-prov-no-fn",
			qualifier:   "1",
			wantCode:    http.StatusNotFound,
			wantErrType: "ResourceNotFoundException",
		},
		{
			name:      "config_not_found",
			funcName:  "del-prov-no-config",
			qualifier: "1",
			setup: func(t *testing.T, b *lambda.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.CreateFunction(&lambda.FunctionConfiguration{
					FunctionName: "del-prov-no-config",
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

			path := "/2015-03-31/functions/" + tt.funcName + "/provisioned-concurrency"
			if tt.qualifier != "" {
				path += "?Qualifier=" + tt.qualifier
			}

			rec := callHandler(t, h, http.MethodDelete, path, "", nil)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantErrType != "" {
				assertLambdaError(t, rec, tt.wantErrType)
			}
		})
	}
}

// ---- ListProvisionedConcurrencyConfigs tests ----

func TestListProvisionedConcurrencyConfigs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(*testing.T, *lambda.InMemoryBackend)
		funcName    string
		name        string
		wantErrType string
		wantCount   int
		wantCode    int
	}{
		{
			name:     "empty_list",
			funcName: "list-prov-empty-fn",
			setup: func(t *testing.T, b *lambda.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.CreateFunction(&lambda.FunctionConfiguration{
					FunctionName: "list-prov-empty-fn",
					PackageType:  lambda.PackageTypeImage,
					ImageURI:     "test:latest",
				}))
			},
			wantCode:  http.StatusOK,
			wantCount: 0,
		},
		{
			name:     "returns_all_configs",
			funcName: "list-prov-multi-fn",
			setup: func(t *testing.T, b *lambda.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.CreateFunction(&lambda.FunctionConfiguration{
					FunctionName: "list-prov-multi-fn",
					PackageType:  lambda.PackageTypeImage,
					ImageURI:     "test:latest",
				}))
				_, err := b.PutProvisionedConcurrencyConfig("list-prov-multi-fn", "1", 3)
				require.NoError(t, err)
				_, err = b.PutProvisionedConcurrencyConfig("list-prov-multi-fn", "myalias", 7)
				require.NoError(t, err)
			},
			wantCode:  http.StatusOK,
			wantCount: 2,
		},
		{
			name:        "function_not_found",
			funcName:    "list-prov-no-fn",
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

			path := "/2015-03-31/functions/" + tt.funcName + "/provisioned-concurrency"
			rec := callHandler(t, h, http.MethodGet, path, "", nil)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantErrType != "" {
				assertLambdaError(t, rec, tt.wantErrType)

				return
			}

			var out lambda.ListProvisionedConcurrencyConfigsOutput
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Len(t, out.ProvisionedConcurrencyConfigs, tt.wantCount)
		})
	}
}
