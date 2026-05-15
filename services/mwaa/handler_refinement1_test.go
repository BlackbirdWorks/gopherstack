package mwaa_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/mwaa"
)

// makeEchoContext creates an echo.Context for the given method and path.
func makeEchoContext(t *testing.T, method, path string) *echo.Context {
	t.Helper()

	e := echo.New()
	req := httptest.NewRequest(method, path, nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	return c
}

// makeEchoContextWithAuth creates an echo.Context with an Authorization header for a given service.
func makeEchoContextWithAuth(t *testing.T, method, path, svcName string) *echo.Context {
	t.Helper()

	e := echo.New()
	req := httptest.NewRequest(method, path, nil)
	req.Header.Set(
		"Authorization",
		"AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/"+svcName+"/aws4_request",
	)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	return c
}

// seedEnv creates an environment via the backend directly.
func seedEnv(t *testing.T, b *mwaa.InMemoryBackend, name string) {
	t.Helper()

	_, err := b.CreateEnvironment("us-east-1", testAccountID, name, &mwaa.ExportedCreateEnvironmentRequest{
		DagS3Path:        "dags/",
		ExecutionRoleArn: "arn:aws:iam::123456789012:role/role",
		SourceBucketArn:  "arn:aws:s3:::bucket",
	})
	require.NoError(t, err)
}

// ----------------------------------------
// Refinement tests
// ----------------------------------------

func TestRefinement1_BackendReset(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	seedEnv(t, b, "env1")
	seedEnv(t, b, "env2")

	require.Equal(t, 2, mwaa.EnvironmentCount(b))

	b.Reset()

	assert.Equal(t, 0, mwaa.EnvironmentCount(b))
	assert.Equal(t, 0, mwaa.ARNIndexSize(b))
}

func TestRefinement1_MultipleResetCycle(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)

	for range 3 {
		seedEnv(t, b, "env1")
		b.Reset()
		assert.Equal(t, 0, mwaa.EnvironmentCount(b))
	}
}

func TestRefinement1_HandlerReset(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	backend := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	h2 := mwaa.NewHandler(backend)
	h2.AccountID = testAccountID
	h2.DefaultRegion = testRegion

	seedEnv(t, backend, "env-a")
	require.Equal(t, 1, mwaa.EnvironmentCount(backend))

	h2.Reset()

	assert.Equal(t, 0, mwaa.EnvironmentCount(backend))

	// Ensure original handler still works (not broken by reset of h2).
	_ = h
}

func TestRefinement1_ProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	p := &mwaa.Provider{}
	_, err := p.Init(nil)

	require.ErrorIs(t, err, mwaa.ErrNilAppContext)
}

func TestRefinement1_ProviderInit_ValidCtx(t *testing.T) {
	t.Parallel()

	p := &mwaa.Provider{}
	ctx := &service.AppContext{}
	reg, err := p.Init(ctx)

	require.NoError(t, err)
	require.NotNil(t, reg)
}

func TestRefinement1_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	h := mwaa.NewHandler(b)

	ops := h.GetSupportedOperations()

	expectedOps := []string{
		"CreateEnvironment", "GetEnvironment", "DeleteEnvironment",
		"UpdateEnvironment", "ListEnvironments", "ListTagsForResource",
		"TagResource", "UntagResource", "CreateCliToken",
		"CreateWebLoginToken", "InvokeRestApi", "PublishMetrics", "GetMetrics",
	}

	for _, op := range expectedOps {
		assert.Contains(t, ops, op, "missing operation %s", op)
	}

	assert.Equal(t, len(expectedOps), mwaa.HandlerOpsLen(h))
}

func TestRefinement1_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	seedEnv(t, b, "persist-env")

	snap := b.Snapshot()
	require.NotNil(t, snap)

	b2 := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	err := b2.Restore(snap)
	require.NoError(t, err)

	assert.Equal(t, 1, mwaa.EnvironmentCount(b2))
	assert.Equal(t, 1, mwaa.ARNIndexSize(b2))

	env, err := b2.GetEnvironment("persist-env")
	require.NoError(t, err)
	assert.Equal(t, "persist-env", env.Name)
}

func TestRefinement1_CreateEnvironment_RequiredFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		req     *mwaa.ExportedCreateEnvironmentRequest
		name    string
		wantMsg string
	}{
		{
			name: "missing_DagS3Path",
			req: &mwaa.ExportedCreateEnvironmentRequest{
				ExecutionRoleArn: "arn:aws:iam::123456789012:role/role",
				SourceBucketArn:  "arn:aws:s3:::bucket",
			},
			wantMsg: "DagS3Path",
		},
		{
			name: "missing_ExecutionRoleArn",
			req: &mwaa.ExportedCreateEnvironmentRequest{
				DagS3Path:       "dags/",
				SourceBucketArn: "arn:aws:s3:::bucket",
			},
			wantMsg: "ExecutionRoleArn",
		},
		{
			name: "missing_SourceBucketArn",
			req: &mwaa.ExportedCreateEnvironmentRequest{
				DagS3Path:        "dags/",
				ExecutionRoleArn: "arn:aws:iam::123456789012:role/role",
			},
			wantMsg: "SourceBucketArn",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			_, err := b.CreateEnvironment(testRegion, testAccountID, "env", tt.req)

			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantMsg)
		})
	}
}

func TestRefinement1_CreateEnvironment_WebserverAccessMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		accessMode string
		wantErr    bool
	}{
		{name: "public_only_valid", accessMode: "PUBLIC_ONLY", wantErr: false},
		{name: "private_only_valid", accessMode: "PRIVATE_ONLY", wantErr: false},
		{name: "empty_uses_default", accessMode: "", wantErr: false},
		{name: "invalid_mode", accessMode: "INVALID_MODE", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			_, err := b.CreateEnvironment(testRegion, testAccountID, "env", &mwaa.ExportedCreateEnvironmentRequest{
				DagS3Path:           "dags/",
				ExecutionRoleArn:    "arn:aws:iam::123456789012:role/role",
				SourceBucketArn:     "arn:aws:s3:::bucket",
				WebserverAccessMode: tt.accessMode,
			})

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRefinement1_CreateEnvironment_EnvironmentClass(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		class   string
		wantErr bool
	}{
		{name: "mw1_small_valid", class: "mw1.small", wantErr: false},
		{name: "mw1_medium_valid", class: "mw1.medium", wantErr: false},
		{name: "mw1_large_valid", class: "mw1.large", wantErr: false},
		{name: "mw1_xlarge_valid", class: "mw1.xlarge", wantErr: false},
		{name: "mw1_2xlarge_valid", class: "mw1.2xlarge", wantErr: false},
		{name: "empty_uses_default", class: "", wantErr: false},
		{name: "invalid_class", class: "mw1.huge", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			_, err := b.CreateEnvironment(testRegion, testAccountID, "env", &mwaa.ExportedCreateEnvironmentRequest{
				DagS3Path:        "dags/",
				ExecutionRoleArn: "arn:aws:iam::123456789012:role/role",
				SourceBucketArn:  "arn:aws:s3:::bucket",
				EnvironmentClass: tt.class,
			})

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRefinement1_CreateEnvironment_Duplicate(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	seedEnv(t, b, "dup-env")

	_, err := b.CreateEnvironment(testRegion, testAccountID, "dup-env", &mwaa.ExportedCreateEnvironmentRequest{
		DagS3Path:        "dags/",
		ExecutionRoleArn: "arn:aws:iam::123456789012:role/role",
		SourceBucketArn:  "arn:aws:s3:::bucket",
	})

	require.Error(t, err)
	require.ErrorIs(t, err, mwaa.ErrEnvironmentAlreadyExists)
}

func TestRefinement1_GetEnvironment_DeepCopy(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	seedEnv(t, b, "deep-copy-env")

	env1, err := b.GetEnvironment("deep-copy-env")
	require.NoError(t, err)

	// Mutate the returned copy.
	env1.Name = "mutated"
	env1.Tags["injected"] = "value"

	// Re-fetch should have original name.
	env2, err := b.GetEnvironment("deep-copy-env")
	require.NoError(t, err)

	assert.Equal(t, "deep-copy-env", env2.Name)
	assert.NotContains(t, env2.Tags, "injected")
}

func TestRefinement1_DeleteEnvironment_CleansUpMetrics(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	seedEnv(t, b, "metrics-env")

	v := float64(1.0)
	err := b.PublishMetrics("metrics-env", &mwaa.ExportedPublishMetricsRequest{
		MetricData: []mwaa.ExportedMetricDatum{
			{MetricName: "Workers", Value: &v},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, 1, mwaa.MetricsCount(b, "metrics-env"))

	_, err = b.DeleteEnvironment("metrics-env")
	require.NoError(t, err)

	assert.Equal(t, 0, mwaa.MetricsCount(b, "metrics-env"))
}

func TestRefinement1_PublishMetrics_EnvNotFound(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	err := b.PublishMetrics("nonexistent", &mwaa.ExportedPublishMetricsRequest{})

	require.Error(t, err)
	require.ErrorIs(t, err, mwaa.ErrEnvironmentNotFound)
}

func TestRefinement1_CreateCliToken_NotFound(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateCliToken("missing-env")

	require.Error(t, err)
	require.ErrorIs(t, err, mwaa.ErrEnvironmentNotFound)
}

func TestRefinement1_CreateWebLoginToken_NotFound(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateWebLoginToken("missing-env")

	require.Error(t, err)
	require.ErrorIs(t, err, mwaa.ErrEnvironmentNotFound)
}

func TestRefinement1_CreateCliToken_HappyPath(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	seedEnv(t, b, "cli-env")

	token, err := b.CreateCliToken("cli-env")

	require.NoError(t, err)
	assert.NotEmpty(t, token)
	// Token is JWT-shaped: three dot-separated base64url segments.
	parts := strings.Split(token, ".")
	assert.Len(t, parts, 3, "expected JWT-shaped token with 3 dot-separated parts")
}

func TestRefinement1_CreateWebLoginToken_HappyPath(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	seedEnv(t, b, "web-env")

	token, err := b.CreateWebLoginToken("web-env")

	require.NoError(t, err)
	assert.NotEmpty(t, token)
	// Token is JWT-shaped: three dot-separated base64url segments.
	parts := strings.Split(token, ".")
	assert.Len(t, parts, 3, "expected JWT-shaped token with 3 dot-separated parts")
}

func TestRefinement1_Handler_CliToken_NotFound(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	rec := doMWAARequest(t, h, http.MethodPost, "/clitoken/nonexistent", nil)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestRefinement1_Handler_WebToken_NotFound(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	rec := doMWAARequest(t, h, http.MethodPost, "/webtoken/nonexistent", nil)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestRefinement1_Handler_CliToken_HappyPath(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	doMWAARequest(t, h, http.MethodPut, "/environments/cli-happy", map[string]any{
		"DagS3Path":        "dags/",
		"ExecutionRoleArn": "arn:aws:iam::123456789012:role/role",
		"SourceBucketArn":  "arn:aws:s3:::bucket",
	})

	rec := doMWAARequest(t, h, http.MethodPost, "/clitoken/cli-happy", nil)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.NotEmpty(t, resp["CliToken"])
}

func TestRefinement1_Handler_WebToken_HappyPath(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	doMWAARequest(t, h, http.MethodPut, "/environments/web-happy", map[string]any{
		"DagS3Path":        "dags/",
		"ExecutionRoleArn": "arn:aws:iam::123456789012:role/role",
		"SourceBucketArn":  "arn:aws:s3:::bucket",
	})

	rec := doMWAARequest(t, h, http.MethodPost, "/webtoken/web-happy", nil)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.NotEmpty(t, resp["WebToken"])
}

func TestRefinement1_UpdateEnvironment_MinMaxWorkers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		update  *mwaa.ExportedUpdateEnvironmentRequest
		name    string
		wantErr bool
	}{
		{
			name: "valid_min_max",
			update: &mwaa.ExportedUpdateEnvironmentRequest{
				MinWorkers: 2,
				MaxWorkers: 5,
			},
			wantErr: false,
		},
		{
			name: "min_greater_than_max",
			update: &mwaa.ExportedUpdateEnvironmentRequest{
				MinWorkers: 10,
				MaxWorkers: 5,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			seedEnv(t, b, "worker-env")

			_, err := b.UpdateEnvironment("worker-env", tt.update)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRefinement1_ExtractOperation(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	h := mwaa.NewHandler(b)

	tests := []struct {
		wantOp string
		path   string
		method string
	}{
		{path: "/clitoken/env", method: http.MethodPost, wantOp: "CreateCliToken"},
		{path: "/webtoken/env", method: http.MethodPost, wantOp: "CreateWebLoginToken"},
		{path: "/restapi/env", method: http.MethodPost, wantOp: "InvokeRestApi"},
		{path: "/metrics/environments/env", method: http.MethodPost, wantOp: "PublishMetrics"},
		{path: "/tags/some-arn", method: http.MethodGet, wantOp: "ListTagsForResource"},
		{path: "/tags/some-arn", method: http.MethodPost, wantOp: "TagResource"},
		{path: "/tags/some-arn", method: http.MethodDelete, wantOp: "UntagResource"},
		{path: "/environments", method: http.MethodGet, wantOp: "ListEnvironments"},
		{path: "/environments/", method: http.MethodGet, wantOp: "ListEnvironments"},
		{path: "/environments/my-env", method: http.MethodGet, wantOp: "GetEnvironment"},
		{path: "/environments/my-env", method: http.MethodPut, wantOp: "CreateEnvironment"},
		{path: "/environments/my-env", method: http.MethodDelete, wantOp: "DeleteEnvironment"},
		{path: "/environments/my-env", method: http.MethodPatch, wantOp: "UpdateEnvironment"},
	}

	for _, tt := range tests {
		t.Run(tt.wantOp+"_"+tt.method, func(t *testing.T) {
			t.Parallel()

			c := makeEchoContext(t, tt.method, tt.path)

			op := h.ExtractOperation(c)
			assert.Equal(t, tt.wantOp, op)
		})
	}
}

func TestRefinement1_ExtractResource(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	h := mwaa.NewHandler(b)

	tests := []struct {
		path    string
		wantRes string
	}{
		{path: "/environments/my-env", wantRes: "my-env"},
		{path: "/clitoken/my-env", wantRes: "my-env"},
		{path: "/webtoken/my-env", wantRes: "my-env"},
		{path: "/restapi/my-env", wantRes: "my-env"},
		{path: "/metrics/environments/my-env", wantRes: "my-env"},
		{
			path:    "/tags/arn:aws:airflow:us-east-1:123:environment/my-env",
			wantRes: "arn:aws:airflow:us-east-1:123:environment/my-env",
		},
		{path: "/unknown", wantRes: ""},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			t.Parallel()

			c := makeEchoContext(t, http.MethodGet, tt.path)
			res := h.ExtractResource(c)
			assert.Equal(t, tt.wantRes, res)
		})
	}
}

func TestRefinement1_RouteMatcher(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	h := mwaa.NewHandler(b)
	matcher := h.RouteMatcher()

	tests := []struct {
		path      string
		authSvc   string
		wantMatch bool
	}{
		{path: "/environments", authSvc: "airflow", wantMatch: true},
		{path: "/environments/my-env", authSvc: "airflow", wantMatch: true},
		{path: "/tags/some-arn", authSvc: "airflow", wantMatch: true},
		{path: "/clitoken/env", authSvc: "airflow", wantMatch: true},
		{path: "/webtoken/env", authSvc: "airflow", wantMatch: true},
		{path: "/restapi/env", authSvc: "airflow", wantMatch: true},
		{path: "/metrics/environments/env", authSvc: "airflow", wantMatch: true},
		{path: "/environments", authSvc: "s3", wantMatch: false},
		{path: "/other-path", authSvc: "airflow", wantMatch: false},
	}

	for _, tt := range tests {
		t.Run(tt.path+"_"+tt.authSvc, func(t *testing.T) {
			t.Parallel()

			c := makeEchoContextWithAuth(t, http.MethodGet, tt.path, tt.authSvc)
			got := matcher(c)
			assert.Equal(t, tt.wantMatch, got)
		})
	}
}

func TestRefinement1_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		path   string
		method string
		name   string
	}{
		{name: "clitoken_get", path: "/clitoken/env", method: http.MethodGet},
		{name: "webtoken_get", path: "/webtoken/env", method: http.MethodGet},
		{name: "restapi_get", path: "/restapi/env", method: http.MethodGet},
		{name: "environments_list_post", path: "/environments", method: http.MethodPost},
		{name: "environment_options", path: "/environments/env", method: http.MethodOptions},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			rec := doMWAARequest(t, h, tt.method, tt.path, nil)

			assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
		})
	}
}
