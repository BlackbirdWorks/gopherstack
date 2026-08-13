package lambda_test

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/lambda"
)

func TestHandler_UnknownRoute(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "unknown_sub_path", wantCode: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newHandler(t)
			rec := callHandler(t, h, http.MethodGet, "/2015-03-31/functions/foo/unknown-sub", "", nil)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
		target string
		want   bool
	}{
		{name: "lambda_path", method: http.MethodGet, path: "/2015-03-31/functions", want: true},
		{
			name:   "amz_target_header",
			method: http.MethodGet,
			path:   "/other",
			target: "AWSLambda.ListFunctions",
			want:   true,
		},
		{name: "no_match", method: http.MethodGet, path: "/other", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newHandler(t)
			matcher := h.RouteMatcher()
			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, nil)
			if tt.target != "" {
				req.Header.Set("X-Amz-Target", tt.target)
			}
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.want, matcher(c))
		})
	}
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains []string
	}{
		{
			// "Invoke" is the real SDK operation name (lambda.Client.Invoke); the
			// list previously (incorrectly) asserted the phantom "InvokeFunction",
			// which does not exist on the SDK client — see gopherstack-vhw2.
			name:         "returns_operations",
			wantContains: []string{"CreateFunction", "Invoke"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newHandler(t)
			ops := h.GetSupportedOperations()
			assert.NotEmpty(t, ops)
			for _, op := range tt.wantContains {
				assert.Contains(t, ops, op)
			}
		})
	}
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want int
	}{
		{name: "returns_95", want: 95},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newHandler(t)
			assert.Equal(t, tt.want, h.MatchPriority())
		})
	}
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
		wantOp string
	}{
		{name: "create_function", method: http.MethodPost, path: "/2015-03-31/functions", wantOp: "CreateFunction"},
		{name: "list_functions", method: http.MethodGet, path: "/2015-03-31/functions", wantOp: "ListFunctions"},
		{name: "get_function", method: http.MethodGet, path: "/2015-03-31/functions/my-func", wantOp: "GetFunction"},
		{
			name:   "delete_function",
			method: http.MethodDelete,
			path:   "/2015-03-31/functions/my-func",
			wantOp: "DeleteFunction",
		},
		{
			name:   "update_code",
			method: http.MethodPut,
			path:   "/2015-03-31/functions/my-func/code",
			wantOp: "UpdateFunctionCode",
		},
		{
			name:   "update_config",
			method: http.MethodPut,
			path:   "/2015-03-31/functions/my-func/configuration",
			wantOp: "UpdateFunctionConfiguration",
		},
		{
			// The real SDK operation name is "Invoke" (gopherstack-l5ir); "InvokeFunction"
			// is the IAM *action* name for this same op, a documented AWS naming quirk
			// -- see ExtractOperation's doc comment.
			name:   "invoke",
			method: http.MethodPost,
			path:   "/2015-03-31/functions/my-func/invocations",
			wantOp: "Invoke",
		},
		{name: "unknown", method: http.MethodGet, path: "/2015-03-31/functions/my-func/unknown", wantOp: "Unknown"},
		{
			// Layer list path exercises extractLayerOperation with rest == "" branch (correct prefix).
			name:   "layers_list",
			method: http.MethodGet,
			path:   "/2018-10-31/layers",
			wantOp: "ListLayers",
		},
		{
			// gopherstack-l5ir: extractLayerOperation previously left lastSeg empty for
			// n==2, so this real ListLayerVersions path never resolved. Fixed.
			name:   "layer_versions_list",
			method: http.MethodGet,
			path:   "/2018-10-31/layers/my-layer/versions",
			wantOp: "ListLayerVersions",
		},
		{
			// Layer version get exercises extractLayerOperation with numParts==3 branch.
			name:   "layer_version_get",
			method: http.MethodGet,
			path:   "/2018-10-31/layers/my-layer/versions/1",
			wantOp: "GetLayerVersion",
		},
		{
			// Layer version policy exercises extractLayerOperation with numParts==4, lastSeg="policy".
			name:   "layer_version_policy_get",
			method: http.MethodGet,
			path:   "/2018-10-31/layers/my-layer/versions/1/policy",
			wantOp: "GetLayerVersionPolicy",
		},
		{
			// Layer path with bad format (parts[1]!="versions") → extractLayerOperation returns "" → "Unknown".
			name:   "layer_bad_format",
			method: http.MethodGet,
			path:   "/2018-10-31/layers/my-layer/bad",
			wantOp: "Unknown",
		},
		{
			name:   "durable_exec_get",
			method: http.MethodGet,
			path:   "/2025-12-01/durable-executions/test-arn",
			wantOp: "GetDurableExecution",
		},
		{
			name:   "durable_exec_history",
			method: http.MethodGet,
			path:   "/2025-12-01/durable-executions/test-arn/history",
			wantOp: "GetDurableExecutionHistory",
		},
		{
			name:   "durable_exec_state",
			method: http.MethodGet,
			path:   "/2025-12-01/durable-executions/test-arn/state",
			wantOp: "GetDurableExecutionState",
		},
		{
			name:   "durable_exec_checkpoint",
			method: http.MethodPost,
			path:   "/2025-12-01/durable-executions/test-arn/checkpoint",
			wantOp: "CheckpointDurableExecution",
		},
		{
			name:   "durable_exec_stop",
			method: http.MethodPost,
			path:   "/2025-12-01/durable-executions/test-arn/stop",
			wantOp: "StopDurableExecution",
		},
		{
			name:   "durable_exec_list_by_function",
			method: http.MethodGet,
			path:   "/2025-12-01/functions/my-func/durable-executions",
			wantOp: "ListDurableExecutionsByFunction",
		},
		{
			name:   "durable_exec_callback_succeed",
			method: http.MethodPost,
			path:   "/2025-12-01/durable-execution-callbacks/cb-1/succeed",
			wantOp: "SendDurableExecutionCallbackSuccess",
		},
		{
			name:   "durable_exec_callback_fail",
			method: http.MethodPost,
			path:   "/2025-12-01/durable-execution-callbacks/cb-1/fail",
			wantOp: "SendDurableExecutionCallbackFailure",
		},
		{
			name:   "durable_exec_callback_heartbeat",
			method: http.MethodPost,
			path:   "/2025-12-01/durable-execution-callbacks/cb-1/heartbeat",
			wantOp: "SendDurableExecutionCallbackHeartbeat",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newHandler(t)
			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
		})
	}
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		method       string
		path         string
		wantResource string
	}{
		{
			name:         "with_function_name",
			method:       http.MethodGet,
			path:         "/2015-03-31/functions/my-func",
			wantResource: "my-func",
		},
		{
			name:         "without_function_name",
			method:       http.MethodGet,
			path:         "/2015-03-31/functions",
			wantResource: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newHandler(t)
			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantResource, h.ExtractResource(c))
		})
	}
}

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
	}{
		{name: "returns_lambda", want: "Lambda"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newHandler(t)
			assert.Equal(t, tt.want, h.Name())
		})
	}
}

func TestDefaultSettings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		wantDockerHost  string
		wantPoolSize    int
		wantIdleTimeout time.Duration
	}{
		{
			name: "platform_defaults",
			wantDockerHost: func() string {
				if runtime.GOOS == "darwin" || runtime.GOOS == "windows" {
					return "host.docker.internal"
				}

				return "172.17.0.1"
			}(),
			wantPoolSize:    3,
			wantIdleTimeout: 10 * time.Minute,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := lambda.DefaultSettings()
			assert.Equal(t, tt.wantDockerHost, s.DockerHost)
			assert.Equal(t, tt.wantPoolSize, s.PoolSize)
			assert.Equal(t, tt.wantIdleTimeout, s.IdleTimeout)
		})
	}
}

func TestProvider_Name(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
	}{
		{name: "returns_lambda", want: "Lambda"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &lambda.Provider{}
			assert.Equal(t, tt.want, p.Name())
		})
	}
}

func TestProvider_Init(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		config interface {
			GetLambdaSettings() lambda.Settings
			GetGlobalConfig() *config.GlobalConfig
		}
		wantAccountID string
		wantRegion    string
	}{
		{
			name:          "no_config",
			config:        nil,
			wantAccountID: "",
			wantRegion:    "",
		},
		{
			name:          "with_config",
			config:        &mockConfig{accountID: "111111111111", region: "eu-west-1"},
			wantAccountID: "111111111111",
			wantRegion:    "eu-west-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &lambda.Provider{}
			appCtx := &service.AppContext{
				Logger:    slog.Default(),
				PortAlloc: nil,
			}
			if tt.config != nil {
				appCtx.Config = tt.config
			}

			svc, err := p.Init(appCtx)
			require.NoError(t, err)
			assert.NotNil(t, svc)
			assert.Equal(t, "Lambda", svc.Name())

			if tt.wantAccountID != "" || tt.wantRegion != "" {
				h, ok := svc.(*lambda.Handler)
				require.True(t, ok)
				assert.Equal(t, tt.wantAccountID, h.AccountID)
				assert.Equal(t, tt.wantRegion, h.DefaultRegion)
			}
		})
	}
}

// mockConfig implements lambda.SettingsProvider and config.Provider for provider tests.
type mockConfig struct {
	accountID string
	region    string
}

func (m *mockConfig) GetLambdaSettings() lambda.Settings {
	return lambda.DefaultSettings()
}

func (m *mockConfig) GetGlobalConfig() *config.GlobalConfig {
	return config.NewGlobalConfig(m.accountID, m.region, 0, 0, false, 0)
}

// ---- Function URL tests ----
