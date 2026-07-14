package lambda_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

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
		{
			name:      "latest_qualifier_rejected",
			funcName:  "put-prov-latest-fn",
			qualifier: "$LATEST",
			body:      `{"ProvisionedConcurrentExecutions":5}`,
			setup: func(t *testing.T, b *lambda.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.CreateFunction(&lambda.FunctionConfiguration{
					FunctionName: "put-prov-latest-fn",
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
			closeBackend(t, bk)
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
			closeBackend(t, bk)
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
			closeBackend(t, bk)
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
			closeBackend(t, bk)
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

// --- Provisioned Concurrency HTTP tests ---

func TestBatch1_ProvisionedConcurrency_PutGetDeleteList(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	fnName := "pc-test-fn"
	createFunctionForTest(t, h, fnName)

	// Publish version
	callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/functions/"+fnName+"/versions", "{}")

	// Put provisioned concurrency
	rec := callInMemoryHandler(
		t, h, http.MethodPut,
		"/2015-03-31/functions/"+fnName+"/provisioned-concurrency?Qualifier=1",
		`{"ProvisionedConcurrentExecutions":5}`,
	)
	require.Equal(t, http.StatusCreated, rec.Code)

	// Get provisioned concurrency
	rec = callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/"+fnName+"/provisioned-concurrency?Qualifier=1", "{}")
	require.Equal(t, http.StatusOK, rec.Code)

	// List provisioned concurrency configs
	rec = callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/"+fnName+"/provisioned-concurrency", "{}")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ProvisionedConcurrencyConfigs")

	// Delete provisioned concurrency
	rec = callInMemoryHandler(t, h, http.MethodDelete,
		"/2015-03-31/functions/"+fnName+"/provisioned-concurrency?Qualifier=1", "{}")
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

// ============================================================
// ProvisionedConcurrencyConfig
// ============================================================

func TestBatch2_ProvisionedConcurrency_Lifecycle(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "pc-fn")
	callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions/pc-fn/versions", `{}`)

	// Put
	putRec := callInMemoryHandler(t, h, http.MethodPut,
		"/2019-09-30/functions/pc-fn/provisioned-concurrency?Qualifier=1",
		`{"ProvisionedConcurrentExecutions":5}`)
	require.Equal(t, http.StatusCreated, putRec.Code)

	var pc lambda.ProvisionedConcurrencyConfig
	require.NoError(t, json.NewDecoder(putRec.Body).Decode(&pc))
	assert.Equal(t, 5, pc.RequestedProvisionedConcurrentExecutions)
	assert.Equal(t, "READY", pc.Status)

	// Get
	getRec := callInMemoryHandler(t, h, http.MethodGet,
		"/2019-09-30/functions/pc-fn/provisioned-concurrency?Qualifier=1", "")
	require.Equal(t, http.StatusOK, getRec.Code)

	var getPC lambda.ProvisionedConcurrencyConfig
	require.NoError(t, json.NewDecoder(getRec.Body).Decode(&getPC))
	assert.Equal(t, 5, getPC.RequestedProvisionedConcurrentExecutions)

	// List
	listRec := callInMemoryHandler(t, h, http.MethodGet,
		"/2019-09-30/functions/pc-fn/provisioned-concurrency", "")
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut lambda.ListProvisionedConcurrencyConfigsOutput
	require.NoError(t, json.NewDecoder(listRec.Body).Decode(&listOut))
	assert.Len(t, listOut.ProvisionedConcurrencyConfigs, 1)

	// Delete
	delRec := callInMemoryHandler(t, h, http.MethodDelete,
		"/2019-09-30/functions/pc-fn/provisioned-concurrency?Qualifier=1", "")
	assert.Equal(t, http.StatusNoContent, delRec.Code)

	// List after delete
	listRec2 := callInMemoryHandler(t, h, http.MethodGet,
		"/2019-09-30/functions/pc-fn/provisioned-concurrency", "")
	var listOut2 lambda.ListProvisionedConcurrencyConfigsOutput
	require.NoError(t, json.NewDecoder(listRec2.Body).Decode(&listOut2))
	assert.Empty(t, listOut2.ProvisionedConcurrencyConfigs)
}

func TestBatch2_ProvisionedConcurrency_AliasQualifier(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "pc-alias-fn")
	callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions/pc-alias-fn/versions", `{}`)
	callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/functions/pc-alias-fn/aliases",
		`{"Name":"stable","FunctionVersion":"1"}`)

	rec := callInMemoryHandler(t, h, http.MethodPut,
		"/2019-09-30/functions/pc-alias-fn/provisioned-concurrency?Qualifier=stable",
		`{"ProvisionedConcurrentExecutions":3}`)
	require.Equal(t, http.StatusCreated, rec.Code)

	getRec := callInMemoryHandler(t, h, http.MethodGet,
		"/2019-09-30/functions/pc-alias-fn/provisioned-concurrency?Qualifier=stable", "")
	assert.Equal(t, http.StatusOK, getRec.Code)
}

// ============================================================
// Provisioned concurrency — List and status accuracy
// ============================================================

func TestBatch3_ProvisionedConcurrency_StatusReady(t *testing.T) {
	t.Parallel()

	h, bk := newInMemoryHandler(t)
	require.NoError(t, bk.CreateFunction(&lambda.FunctionConfiguration{
		FunctionName: "prov-status-fn",
		PackageType:  lambda.PackageTypeImage,
		ImageURI:     "test:latest",
	}))

	_, err := bk.PublishVersion("prov-status-fn", "")
	require.NoError(t, err)

	rec := callInMemoryHandler(
		t, h, http.MethodPut,
		"/2015-03-31/functions/prov-status-fn/provisioned-concurrency?Qualifier=1",
		`{"ProvisionedConcurrentExecutions":5}`,
	)
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.InDelta(t, float64(5), out["RequestedProvisionedConcurrentExecutions"], 0)
	assert.InDelta(t, float64(5), out["AllocatedProvisionedConcurrentExecutions"], 0)
	assert.InDelta(t, float64(5), out["AvailableProvisionedConcurrentExecutions"], 0)
	assert.Equal(t, "READY", out["Status"])
}

func TestBatch3_ProvisionedConcurrency_List_IncludesAllQualifiers(t *testing.T) {
	t.Parallel()

	h, bk := newInMemoryHandler(t)
	require.NoError(t, bk.CreateFunction(&lambda.FunctionConfiguration{
		FunctionName: "prov-list-fn",
		PackageType:  lambda.PackageTypeImage,
		ImageURI:     "test:latest",
	}))

	_, err := bk.PublishVersion("prov-list-fn", "")
	require.NoError(t, err)
	_, err = bk.PublishVersion("prov-list-fn", "")
	require.NoError(t, err)

	callInMemoryHandler(
		t, h, http.MethodPut,
		"/2015-03-31/functions/prov-list-fn/provisioned-concurrency?Qualifier=1",
		`{"ProvisionedConcurrentExecutions":3}`,
	)
	callInMemoryHandler(
		t, h, http.MethodPut,
		"/2015-03-31/functions/prov-list-fn/provisioned-concurrency?Qualifier=2",
		`{"ProvisionedConcurrentExecutions":7}`,
	)

	listRec := callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/prov-list-fn/provisioned-concurrency", "")
	require.Equal(t, http.StatusOK, listRec.Code)

	var out lambda.ListProvisionedConcurrencyConfigsOutput
	require.NoError(t, json.NewDecoder(listRec.Body).Decode(&out))
	assert.Len(t, out.ProvisionedConcurrencyConfigs, 2)
}

func TestLambda_ProvisionedConcurrencyLifecycle(t *testing.T) {
	t.Parallel()

	backend := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "000000000000", "us-east-1")
	closeBackend(t, backend)

	require.NoError(t, backend.CreateFunction(&lambda.FunctionConfiguration{
		FunctionName: "pc-fn",
		FunctionArn:  "arn:aws:lambda:us-east-1:000000000000:function:pc-fn",
	}))

	backend.SetProvisionedConcurrencyDelay(200 * time.Millisecond)

	cfg, err := backend.PutProvisionedConcurrencyConfig("pc-fn", "1", 5)
	require.NoError(t, err)
	assert.Equal(t, "IN_PROGRESS", cfg.Status)
	assert.Equal(t, 0, cfg.AvailableProvisionedConcurrentExecutions)

	require.Eventually(t, func() bool {
		got, getErr := backend.GetProvisionedConcurrencyConfig("pc-fn", "1")

		return getErr == nil && got.Status == "READY" && got.AvailableProvisionedConcurrentExecutions == 5
	}, 3*time.Second, 20*time.Millisecond)
}

func TestLambda_ProvisionedConcurrency_NoDelayReadyImmediately(t *testing.T) {
	t.Parallel()

	backend := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "000000000000", "us-east-1")
	closeBackend(t, backend)

	require.NoError(t, backend.CreateFunction(&lambda.FunctionConfiguration{FunctionName: "pc-fn2"}))

	cfg, err := backend.PutProvisionedConcurrencyConfig("pc-fn2", "1", 3)
	require.NoError(t, err)
	assert.Equal(t, "READY", cfg.Status)
	assert.Equal(t, 3, cfg.AvailableProvisionedConcurrentExecutions)
}

// TestComprehensive_ProvisionedConcurrency verifies provisioned concurrency CRUD.
func TestComprehensive_ProvisionedConcurrency(t *testing.T) {
	t.Parallel()

	h, bk := newInMemoryHandler(t)

	require.NoError(t, bk.CreateFunction(&lambda.FunctionConfiguration{
		FunctionName: "prov-conc-fn",
		PackageType:  lambda.PackageTypeImage,
		ImageURI:     "test:latest",
		State:        lambda.FunctionStateActive,
	}))

	// Publish a version so we can set provisioned concurrency on it.
	v, err := bk.PublishVersion("prov-conc-fn", "")
	require.NoError(t, err)

	// PutProvisionedConcurrencyConfig.
	rec := callInMemoryHandler(t, h, http.MethodPut,
		"/2019-09-30/functions/prov-conc-fn/provisioned-concurrency?Qualifier="+v.Version,
		`{"ProvisionedConcurrentExecutions":5}`)
	require.Equal(t, http.StatusCreated, rec.Code)

	var pcOut lambda.ProvisionedConcurrencyConfig
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &pcOut))
	assert.Equal(t, 5, pcOut.RequestedProvisionedConcurrentExecutions)
	assert.Equal(t, "READY", pcOut.Status)

	// GetProvisionedConcurrencyConfig.
	rec = callInMemoryHandler(t, h, http.MethodGet,
		"/2019-09-30/functions/prov-conc-fn/provisioned-concurrency?Qualifier="+v.Version, "")
	require.Equal(t, http.StatusOK, rec.Code)

	// ListProvisionedConcurrencyConfigs.
	rec = callInMemoryHandler(t, h, http.MethodGet,
		"/2019-09-30/functions/prov-conc-fn/provisioned-concurrency", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var listOut lambda.ListProvisionedConcurrencyConfigsOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listOut))
	require.Len(t, listOut.ProvisionedConcurrencyConfigs, 1)

	// DeleteProvisionedConcurrencyConfig.
	rec = callInMemoryHandler(t, h, http.MethodDelete,
		"/2019-09-30/functions/prov-conc-fn/provisioned-concurrency?Qualifier="+v.Version, "")
	require.Equal(t, http.StatusNoContent, rec.Code)
}
