package lambda_test

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dockercontainer "github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/container"
	"github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/image"
	gophercontainer "github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	"github.com/blackbirdworks/gopherstack/services/lambda"
)

const (
	runtimeEnvPortBase = 20700
	testRoleArn        = "arn:aws:iam::000000000000:role/exec"
)

var errIssuerDown = errors.New("issuer down")

type envCapturingAPI struct {
	envs    [][]string
	stopped []string
	mu      sync.Mutex
}

func (m *envCapturingAPI) ImagePull(_ context.Context, _ string, _ image.PullOptions) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("")), nil
}

func (m *envCapturingAPI) ImageList(_ context.Context, _ image.ListOptions) ([]image.Summary, error) {
	return nil, nil
}

func (m *envCapturingAPI) ContainerCreate(
	_ context.Context,
	cfg *dockercontainer.Config,
	_ *dockercontainer.HostConfig,
	_ any,
	_ any,
	name string,
) (dockercontainer.CreateResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.envs = append(m.envs, append([]string(nil), cfg.Env...))

	return dockercontainer.CreateResponse{ID: fmt.Sprintf("c-%s-%d", name, len(m.envs))}, nil
}

func (m *envCapturingAPI) ContainerStart(_ context.Context, _ string, _ dockercontainer.StartOptions) error {
	return nil
}

func (m *envCapturingAPI) ContainerStop(_ context.Context, id string, _ dockercontainer.StopOptions) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stopped = append(m.stopped, id)

	return nil
}

func (m *envCapturingAPI) ContainerRemove(_ context.Context, _ string, _ dockercontainer.RemoveOptions) error {
	return nil
}

func (m *envCapturingAPI) Ping(_ context.Context) (any, error) { return struct{}{}, nil }
func (m *envCapturingAPI) Close() error                        { return nil }

func (m *envCapturingAPI) created() [][]string {
	m.mu.Lock()
	defer m.mu.Unlock()

	return append([][]string(nil), m.envs...)
}

type fakeIssuer struct {
	err     error
	roles   []string
	expires time.Duration
	mu      sync.Mutex
	calls   int
}

func (f *fakeIssuer) IssueFunctionCredentials(role, session string) (lambda.RoleCredentials, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls++
	f.roles = append(f.roles, role+"|"+session)

	if f.err != nil {
		return lambda.RoleCredentials{}, f.err
	}

	return lambda.RoleCredentials{
		AccessKeyID:     fmt.Sprintf("ASIAFAKE%d", f.calls),
		SecretAccessKey: "secret",
		SessionToken:    "token",
		Expires:         time.Now().Add(f.expires),
	}, nil
}

func envMap(env []string) map[string]string {
	out := make(map[string]string, len(env))

	for _, kv := range env {
		k, v, _ := strings.Cut(kv, "=")
		out[k] = v
	}

	return out
}

func newEnvBackend(t *testing.T, portBase int, api *envCapturingAPI) *lambda.InMemoryBackend {
	t.Helper()

	pa, err := portalloc.New(portBase, portBase+9)
	require.NoError(t, err)

	rt := gophercontainer.NewDockerRuntimeWithAPI(api, gophercontainer.Config{PoolSize: 3, IdleTimeout: time.Minute})
	b := lambda.NewInMemoryBackend(rt, pa, lambda.DefaultSettings(), "000000000000", "us-east-1")
	closeBackend(t, b)

	return b
}

func TestContainerEnv(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want       map[string]string
		fn         *lambda.FunctionConfiguration
		issuer     *fakeIssuer
		name       string
		endpoint   string
		absent     []string
		portOffset int
	}{
		{
			name:       "injected",
			portOffset: 0,
			endpoint:   "http://172.17.0.1:8000",
			issuer:     &fakeIssuer{expires: time.Hour},
			fn: &lambda.FunctionConfiguration{
				FunctionName: "env-fn", PackageType: lambda.PackageTypeImage, ImageURI: "x:1", Role: testRoleArn,
				MemorySize: 256, Timeout: 30,
			},
			want: map[string]string{
				"AWS_ENDPOINT_URL":                "http://172.17.0.1:8000",
				"AWS_ACCESS_KEY_ID":               "ASIAFAKE1",
				"AWS_SECRET_ACCESS_KEY":           "secret",
				"AWS_SESSION_TOKEN":               "token",
				"AWS_REGION":                      "us-east-1",
				"AWS_DEFAULT_REGION":              "us-east-1",
				"AWS_LAMBDA_FUNCTION_NAME":        "env-fn",
				"AWS_LAMBDA_FUNCTION_VERSION":     "$LATEST",
				"AWS_LAMBDA_FUNCTION_MEMORY_SIZE": "256",
				"AWS_LAMBDA_LOG_GROUP_NAME":       "/aws/lambda/env-fn",
				"AWS_LAMBDA_INITIALIZATION_TYPE":  "on-demand",
				"LAMBDA_TASK_ROOT":                "/var/task",
				"LAMBDA_RUNTIME_DIR":              "/var/runtime",
				"TZ":                              ":UTC",
			},
			absent: []string{"AWS_EXECUTION_ENV", "_HANDLER"},
		},
		{
			name:       "handler_set",
			portOffset: 1,
			issuer:     &fakeIssuer{expires: time.Hour},
			fn: &lambda.FunctionConfiguration{
				FunctionName: "zip-fn", PackageType: lambda.PackageTypeImage, ImageURI: "x:1", Role: testRoleArn,
				Runtime: "python3.12", Handler: "index.handler", Timeout: 30,
			},
			want:   map[string]string{"_HANDLER": "index.handler"},
			absent: []string{"AWS_ENDPOINT_URL"},
		},
		{
			name:       "custom_log_group",
			portOffset: 2,
			issuer:     &fakeIssuer{expires: time.Hour},
			fn: &lambda.FunctionConfiguration{
				FunctionName: "log-fn", PackageType: lambda.PackageTypeImage, ImageURI: "x:1", Role: testRoleArn,
				Timeout: 30, LoggingConfig: &lambda.LoggingConfig{LogGroup: "/custom/group"},
			},
			want: map[string]string{"AWS_LAMBDA_LOG_GROUP_NAME": "/custom/group"},
		},
		{
			name:       "user_overrides_unreserved_and_reserved_dropped",
			portOffset: 3,
			endpoint:   "http://172.17.0.1:8000",
			issuer:     &fakeIssuer{expires: time.Hour},
			fn: &lambda.FunctionConfiguration{
				FunctionName: "user-fn", PackageType: lambda.PackageTypeImage, ImageURI: "x:1", Role: testRoleArn,
				Timeout: 30,
				Environment: &lambda.EnvironmentConfig{Variables: map[string]string{
					"AWS_ENDPOINT_URL":  "http://custom:1",
					"TZ":                "America/New_York",
					"AWS_ACCESS_KEY_ID": "AKIAUSERSET",
					"MY_VAR":            "1",
				}},
			},
			want: map[string]string{
				"AWS_ENDPOINT_URL":  "http://custom:1",
				"TZ":                "America/New_York",
				"AWS_ACCESS_KEY_ID": "ASIAFAKE1",
				"MY_VAR":            "1",
			},
		},
		{
			name:       "no_issuer_no_credentials",
			portOffset: 4,
			fn: &lambda.FunctionConfiguration{
				FunctionName: "nocred-fn", PackageType: lambda.PackageTypeImage, ImageURI: "x:1", Role: testRoleArn,
				Timeout: 30,
			},
			absent: []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"},
		},
		{
			name:       "issuer_error_no_credentials",
			portOffset: 5,
			issuer:     &fakeIssuer{err: errIssuerDown},
			fn: &lambda.FunctionConfiguration{
				FunctionName: "err-fn", PackageType: lambda.PackageTypeImage, ImageURI: "x:1", Role: testRoleArn,
				Timeout: 30,
			},
			absent: []string{"AWS_ACCESS_KEY_ID", "AWS_SESSION_TOKEN"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			api := &envCapturingAPI{}
			b := newEnvBackend(t, runtimeEnvPortBase+tt.portOffset*10, api)

			if tt.issuer != nil {
				b.SetRoleCredentialIssuer(tt.issuer)
			}

			if tt.endpoint != "" {
				b.SetEndpointURL(tt.endpoint)
			}

			require.NoError(t, b.CreateFunction(tt.fn))

			_, _, err := b.InvokeFunction(t.Context(), tt.fn.FunctionName, lambda.InvocationTypeEvent, []byte(`{}`))
			require.NoError(t, err)

			created := api.created()
			require.Len(t, created, 1)

			got := envMap(created[0])
			for k, v := range tt.want {
				assert.Equal(t, v, got[k], k)
			}

			for _, k := range tt.absent {
				assert.NotContains(t, got, k)
			}

			if tt.issuer != nil && tt.issuer.err == nil {
				assert.Equal(t, []string{testRoleArn + "|" + tt.fn.FunctionName}, tt.issuer.roles)
			}
		})
	}
}

func TestContainerEnvZipFunction(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	zw := zip.NewWriter(&buf)
	w, err := zw.Create("index.py")
	require.NoError(t, err)
	_, err = w.Write([]byte("def handler(e, c): return e"))
	require.NoError(t, err)
	require.NoError(t, zw.Close())

	api := &envCapturingAPI{}
	b := newEnvBackend(t, runtimeEnvPortBase+100, api)
	fn := &lambda.FunctionConfiguration{
		FunctionName: "zip-fn", PackageType: lambda.PackageTypeZip, Runtime: "python3.12", Handler: "index.handler",
		ZipData: buf.Bytes(), Timeout: 30,
	}
	require.NoError(t, b.CreateFunction(fn))

	_, _, err = b.InvokeFunction(t.Context(), "zip-fn", lambda.InvocationTypeEvent, []byte(`{}`))
	require.NoError(t, err)

	created := api.created()
	require.Len(t, created, 1)

	got := envMap(created[0])
	assert.Equal(t, "AWS_Lambda_python3.12", got["AWS_EXECUTION_ENV"])
	assert.Equal(t, "index.handler", got["_HANDLER"])
	assert.Regexp(t, `^\d{4}/\d{2}/\d{2}/\[\$LATEST\][0-9a-f]{32}$`, got["AWS_LAMBDA_LOG_STREAM_NAME"])
}

func TestContainerCredentialRefresh(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		expires     time.Duration
		portOffset  int
		wantStarts  int
		wantStopped int
	}{
		{name: "fresh_reused", expires: time.Hour, portOffset: 0, wantStarts: 1, wantStopped: 0},
		{name: "expiring_recycled", expires: time.Minute, portOffset: 1, wantStarts: 2, wantStopped: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			api := &envCapturingAPI{}
			b := newEnvBackend(t, runtimeEnvPortBase+200+tt.portOffset*10, api)
			b.SetRoleCredentialIssuer(&fakeIssuer{expires: tt.expires})

			fn := &lambda.FunctionConfiguration{
				FunctionName: "refresh-fn", PackageType: lambda.PackageTypeImage, ImageURI: "x:1",
				Role: testRoleArn, Timeout: 30,
			}
			require.NoError(t, b.CreateFunction(fn))

			for range 2 {
				_, _, err := b.InvokeFunction(t.Context(), "refresh-fn", lambda.InvocationTypeEvent, []byte(`{}`))
				require.NoError(t, err)
			}

			created := api.created()
			require.Len(t, created, tt.wantStarts)

			if tt.wantStarts == 2 {
				assert.NotEqual(t, envMap(created[0])["AWS_ACCESS_KEY_ID"], envMap(created[1])["AWS_ACCESS_KEY_ID"])
			}

			api.mu.Lock()
			defer api.mu.Unlock()
			assert.Len(t, api.stopped, tt.wantStopped)
		})
	}
}

func TestRuntimeTraceHeader(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		mode       string
		portOffset int
		wantTrace  bool
	}{
		{name: "active", mode: "Active", portOffset: 0, wantTrace: true},
		{name: "passthrough", mode: "PassThrough", portOffset: 1, wantTrace: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			base := runtimeEnvPortBase + 300 + tt.portOffset*10
			b := newEnvBackend(t, base, &envCapturingAPI{})
			fn := &lambda.FunctionConfiguration{
				FunctionName: "trace-fn", PackageType: lambda.PackageTypeImage, ImageURI: "x:1", Timeout: 30,
				TracingConfig: &lambda.TracingConfig{Mode: tt.mode},
			}
			require.NoError(t, b.CreateFunction(fn))

			_, _, err := b.InvokeFunction(t.Context(), "trace-fn", lambda.InvocationTypeEvent, []byte(`{}`))
			require.NoError(t, err)

			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet,
				fmt.Sprintf("http://127.0.0.1:%d/2018-06-01/runtime/invocation/next", base), nil)
			require.NoError(t, err)

			resp, err := testclient.New(t).Do(req)
			require.NoError(t, err)

			defer resp.Body.Close()

			trace := resp.Header.Get("Lambda-Runtime-Trace-Id")
			if tt.wantTrace {
				assert.Regexp(t, `^Root=1-[0-9a-f]{8}-[0-9a-f]{24};Parent=[0-9a-f]{16};Sampled=1$`, trace)
			} else {
				assert.Empty(t, trace)
			}
		})
	}
}

func TestEnvironmentReservedKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		key      string
		wantCode int
	}{
		{name: "access_key_id", key: "AWS_ACCESS_KEY_ID", wantCode: http.StatusBadRequest},
		{name: "secret", key: "AWS_SECRET_ACCESS_KEY", wantCode: http.StatusBadRequest},
		{name: "session_token", key: "AWS_SESSION_TOKEN", wantCode: http.StatusBadRequest},
		{name: "function_name", key: "AWS_LAMBDA_FUNCTION_NAME", wantCode: http.StatusBadRequest},
		{name: "function_version", key: "AWS_LAMBDA_FUNCTION_VERSION", wantCode: http.StatusBadRequest},
		{name: "log_group", key: "AWS_LAMBDA_LOG_GROUP_NAME", wantCode: http.StatusBadRequest},
		{name: "task_root", key: "LAMBDA_TASK_ROOT", wantCode: http.StatusBadRequest},
		{name: "execution_env", key: "AWS_EXECUTION_ENV", wantCode: http.StatusBadRequest},
		{name: "endpoint_allowed", key: "AWS_ENDPOINT_URL", wantCode: http.StatusCreated},
		{name: "tz_allowed", key: "TZ", wantCode: http.StatusCreated},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)
			body := fmt.Sprintf(
				`{"FunctionName":"rk-fn","PackageType":"Image","Code":{"ImageUri":"x"},`+
					`"Role":"arn:aws:iam:::role/r","Environment":{"Variables":{%q:"v"}}}`, tt.key)
			rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions", body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusBadRequest {
				assert.Contains(t, rec.Body.String(), "Reserved keys used in this request: "+tt.key)
			}
		})
	}
}
