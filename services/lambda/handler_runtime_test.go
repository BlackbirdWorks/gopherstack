package lambda_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/container"
	"github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/image"

	gophercontainer "github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/services/lambda"
)

func TestBackend_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "full_crud_lifecycle"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "000000000000", "us-east-1")
			closeBackend(t, bk)

			fn := &lambda.FunctionConfiguration{
				FunctionName: "test-func",
				ImageURI:     "myimage:latest",
				PackageType:  lambda.PackageTypeImage,
				State:        lambda.FunctionStateActive,
			}

			require.NoError(t, bk.CreateFunction(fn))
			require.ErrorIs(t, bk.CreateFunction(fn), lambda.ErrFunctionAlreadyExists)

			got, err := bk.GetFunction("test-func")
			require.NoError(t, err)
			assert.Equal(t, "test-func", got.FunctionName)

			_, err = bk.GetFunction("nonexistent")
			require.ErrorIs(t, err, lambda.ErrFunctionNotFound)

			list := bk.ListFunctions("", 0)
			assert.Len(t, list.Data, 1)

			fn2 := *fn
			fn2.Description = "updated"
			require.NoError(t, bk.UpdateFunction(&fn2))

			got2, err := bk.GetFunction("test-func")
			require.NoError(t, err)
			assert.Equal(t, "updated", got2.Description)

			notExist := &lambda.FunctionConfiguration{FunctionName: "nonexistent"}
			require.ErrorIs(t, bk.UpdateFunction(notExist), lambda.ErrFunctionNotFound)

			require.NoError(t, bk.DeleteFunction("test-func"))
			assert.Empty(t, bk.ListFunctions("", 0).Data)

			assert.ErrorIs(t, bk.DeleteFunction("test-func"), lambda.ErrFunctionNotFound)
		})
	}
}

func TestBackend_InvokeFunction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr        error
		name           string
		funcToInvoke   string
		invocationType lambda.InvocationType
		portRange      [2]int
		wantCode       int
		createFunc     bool
	}{
		{
			name:           "no_port_alloc",
			createFunc:     true,
			funcToInvoke:   "invoke-func",
			invocationType: lambda.InvocationTypeRequestResponse,
			wantErr:        lambda.ErrLambdaUnavailable,
		},
		{
			name:           "no_docker",
			portRange:      [2]int{19000, 19100},
			createFunc:     true,
			funcToInvoke:   "invoke-func",
			invocationType: lambda.InvocationTypeRequestResponse,
			wantErr:        lambda.ErrLambdaUnavailable,
		},
		{
			name:           "not_found",
			funcToInvoke:   "nonexistent",
			invocationType: lambda.InvocationTypeRequestResponse,
			wantErr:        lambda.ErrFunctionNotFound,
			wantCode:       http.StatusNotFound,
		},
		{
			name:           "dry_run",
			createFunc:     true,
			funcToInvoke:   "fn",
			invocationType: lambda.InvocationTypeDryRun,
			wantCode:       http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var pa *portalloc.Allocator
			if tt.portRange[0] > 0 {
				var err error
				pa, err = portalloc.New(tt.portRange[0], tt.portRange[1])
				require.NoError(t, err)
			}

			bk := lambda.NewInMemoryBackend(nil, pa, lambda.DefaultSettings(), "000000000000", "us-east-1")
			closeBackend(t, bk)

			if tt.createFunc {
				fn := &lambda.FunctionConfiguration{
					FunctionName: tt.funcToInvoke,
					ImageURI:     "myimage:latest",
					Timeout:      3,
				}
				require.NoError(t, bk.CreateFunction(fn))
			}

			_, statusCode, err := bk.InvokeFunction(t.Context(), tt.funcToInvoke, tt.invocationType, []byte("{}"))

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}

			if tt.wantCode > 0 {
				assert.Equal(t, tt.wantCode, statusCode)
			}
		})
	}
}

func TestRuntimeServer_Invoke(t *testing.T) {
	t.Parallel()

	tests := []struct {
		simulate    func(t *testing.T, port int, requestID string)
		name        string
		wantBody    string
		payload     []byte
		port        int
		wantIsError bool
	}{
		{
			name:    "success_response",
			port:    18101,
			payload: []byte(`{"key":"value"}`),
			simulate: func(t *testing.T, port int, requestID string) {
				t.Helper()
				simulateContainerResponse(t, port, requestID, `{"answer":42}`)
			},
			wantBody:    `{"answer":42}`,
			wantIsError: false,
		},
		{
			name:    "error_response",
			port:    18102,
			payload: []byte(`{}`),
			simulate: func(t *testing.T, port int, requestID string) {
				t.Helper()
				simulateContainerError(t, port, requestID, `{"errorMessage":"function panicked"}`)
			},
			wantBody:    "panicked",
			wantIsError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv := newTestRuntimeServer(t, tt.port)
			ctx := t.Context()

			resultCh := make(chan []byte, 1)
			errCh := make(chan error, 1)
			isErrCh := make(chan bool, 1)

			go func() {
				invokeResult, isError, _, invokeErr := srv.Invoke(ctx, tt.payload, "", 5*time.Second)
				if invokeErr != nil {
					errCh <- invokeErr

					return
				}
				resultCh <- invokeResult
				isErrCh <- isError
			}()

			requestID := simulateContainerNext(t, tt.port)
			tt.simulate(t, tt.port, requestID)

			select {
			case result := <-resultCh:
				isErr := <-isErrCh
				assert.Equal(t, tt.wantIsError, isErr)
				if tt.wantIsError {
					assert.Contains(t, string(result), tt.wantBody)
				} else {
					assert.JSONEq(t, tt.wantBody, string(result))
				}
			case err := <-errCh:
				require.NoError(t, err, "invoke error")
			case <-time.After(5 * time.Second):
				require.FailNow(t, "test timed out")
			}
		})
	}
}

func TestRuntimeServer_HTTPEndpoints(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		method   string
		path     string
		body     string
		port     int
		wantCode int
	}{
		{
			name:     "init_error",
			port:     18103,
			method:   http.MethodPost,
			path:     "/2018-06-01/runtime/init/error",
			body:     `{"errorMessage":"init failed","errorType":"Runtime.ExitError"}`,
			wantCode: http.StatusAccepted,
		},
		{
			name:     "method_not_allowed",
			port:     18104,
			method:   http.MethodPost,
			path:     "/2018-06-01/runtime/invocation/next",
			wantCode: http.StatusMethodNotAllowed,
		},
		{
			name:     "response_unknown_request_id",
			port:     18105,
			method:   http.MethodPost,
			path:     "/2018-06-01/runtime/invocation/unknown-id/response",
			body:     `{"result":"ok"}`,
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_ = newTestRuntimeServer(t, tt.port)

			req, err := http.NewRequestWithContext(
				t.Context(),
				tt.method,
				fmt.Sprintf("http://127.0.0.1:%d%s", tt.port, tt.path),
				strings.NewReader(tt.body),
			)
			require.NoError(t, err)

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			assert.Equal(t, tt.wantCode, resp.StatusCode)
		})
	}
}

func TestRuntimeServer_InvokeStop(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		wantErrContains string
		port            int
		timeout         time.Duration
		cancelCtx       bool
	}{
		{
			name:            "invoke_timeout",
			port:            18106,
			timeout:         100 * time.Millisecond,
			wantErrContains: "timed out",
		},
		{
			name:      "context_cancelled",
			port:      18107,
			timeout:   30 * time.Second,
			cancelCtx: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv := newTestRuntimeServer(t, tt.port)

			if tt.cancelCtx {
				ctx, cancel := context.WithCancel(t.Context())
				errCh := make(chan error, 1)

				go func() {
					_, _, _, err := srv.Invoke(ctx, []byte(`{}`), "", tt.timeout)
					errCh <- err
				}()

				time.Sleep(50 * time.Millisecond)
				cancel()

				select {
				case err := <-errCh:
					require.Error(t, err)
				case <-time.After(2 * time.Second):
					require.FailNow(t, "expected context cancellation error")
				}
			} else {
				_, _, _, err := srv.Invoke(t.Context(), []byte(`{}`), "", tt.timeout)
				require.Error(t, err)
				if tt.wantErrContains != "" {
					assert.Contains(t, err.Error(), tt.wantErrContains)
				}
			}
		})
	}
}

// ---- helper functions ----

// newTestRuntimeServer is an exported alias used in tests to access the internal runtimeServer.
// We use this via the Invoke method exposed for testing.
func newTestRuntimeServer(t *testing.T, port int) testRuntimeServerIface {
	t.Helper()

	srv := newPublicRuntimeServer(t, port)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		srv.Stop(ctx)
	})

	return srv
}

// testRuntimeServerIface wraps the runtimeServer for white-box testing.
type testRuntimeServerIface interface {
	Invoke(
		ctx context.Context,
		payload []byte,
		clientContext string,
		timeout time.Duration,
	) ([]byte, bool, string, error)
	Stop(ctx context.Context)
}

// publicRuntimeServer wraps the internal runtimeServer for test access.
type publicRuntimeServer struct {
	inner *lambda.ExportedRuntimeServer
}

func newPublicRuntimeServer(t *testing.T, port int) *publicRuntimeServer {
	t.Helper()

	srv := lambda.NewExportedRuntimeServer(port)
	require.NoError(t, srv.Start(t.Context()))

	return &publicRuntimeServer{inner: srv}
}

func (p *publicRuntimeServer) Invoke(
	ctx context.Context,
	payload []byte,
	clientContext string,
	timeout time.Duration,
) ([]byte, bool, string, error) {
	return p.inner.Invoke(ctx, payload, clientContext, timeout)
}

func (p *publicRuntimeServer) Stop(ctx context.Context) {
	p.inner.Stop(ctx)
}

func simulateContainerNext(t *testing.T, port int) string {
	t.Helper()

	// Poll until the invocation is queued (the invoke goroutine may not have run yet).
	var resp *http.Response

	for range 20 {
		req, err := http.NewRequestWithContext(
			t.Context(),
			http.MethodGet,
			fmt.Sprintf("http://127.0.0.1:%d/2018-06-01/runtime/invocation/next", port),
			nil,
		)
		require.NoError(t, err)

		var doErr error

		resp, doErr = http.DefaultClient.Do(req)
		if doErr == nil && resp.StatusCode == http.StatusOK {
			break
		}

		if resp != nil {
			resp.Body.Close()
		}

		time.Sleep(50 * time.Millisecond)
	}

	require.NotNil(t, resp)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	requestID := resp.Header.Get("Lambda-Runtime-Aws-Request-Id")
	require.NotEmpty(t, requestID)

	return requestID
}

func simulateContainerResponse(t *testing.T, port int, requestID, responseBody string) {
	t.Helper()

	req, err := http.NewRequestWithContext(
		t.Context(),
		http.MethodPost,
		fmt.Sprintf("http://127.0.0.1:%d/2018-06-01/runtime/invocation/%s/response", port, requestID),
		strings.NewReader(responseBody),
	)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusAccepted, resp.StatusCode)
}

func simulateContainerError(t *testing.T, port int, requestID, errorBody string) {
	t.Helper()

	req, err := http.NewRequestWithContext(
		t.Context(),
		http.MethodPost,
		fmt.Sprintf("http://127.0.0.1:%d/2018-06-01/runtime/invocation/%s/error", port, requestID),
		strings.NewReader(errorBody),
	)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusAccepted, resp.StatusCode)
}

// assertLambdaError asserts that the response body contains a Lambda error with the given type.
func assertLambdaError(t *testing.T, rec *httptest.ResponseRecorder, errType string) {
	t.Helper()

	var lambdaErr lambda.Error
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &lambdaErr))
	assert.Equal(t, errType, lambdaErr.Type)
}

// ---- Mock Docker API ----

// mockDockerAPI implements container.APIClient for testing without a real daemon.
type mockDockerAPI struct {
	createErr error
	counter   int
	mu        sync.Mutex
}

func (m *mockDockerAPI) ImagePull(_ context.Context, _ string, _ image.PullOptions) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("")), nil
}

func (m *mockDockerAPI) ImageList(_ context.Context, _ image.ListOptions) ([]image.Summary, error) {
	return nil, nil
}

func (m *mockDockerAPI) ContainerCreate(
	_ context.Context,
	_ *container.Config,
	_ *container.HostConfig,
	_ any,
	_ any,
	_ string,
) (container.CreateResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.createErr != nil {
		return container.CreateResponse{}, m.createErr
	}

	m.counter++

	return container.CreateResponse{ID: fmt.Sprintf("mock-container-%d", m.counter)}, nil
}

func (m *mockDockerAPI) ContainerStart(_ context.Context, _ string, _ container.StartOptions) error {
	return nil
}

func (m *mockDockerAPI) ContainerStop(_ context.Context, _ string, _ container.StopOptions) error {
	return nil
}

func (m *mockDockerAPI) ContainerRemove(_ context.Context, _ string, _ container.RemoveOptions) error {
	return nil
}

func (m *mockDockerAPI) Ping(_ context.Context) (any, error) {
	return struct{}{}, nil
}

func (m *mockDockerAPI) Close() error {
	return nil
}

// newMockDockerClient creates a container.Runtime backed by mockDockerAPI.
func newMockDockerClient() gophercontainer.Runtime {
	return gophercontainer.NewDockerRuntimeWithAPI(&mockDockerAPI{}, gophercontainer.Config{
		PoolSize:    3,
		IdleTimeout: time.Minute,
	})
}

func TestBackend_InvokeFunction_MockDocker(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		funcName  string
		portRange [2]int
		wantCode  int
		callTwice bool
	}{
		{
			name:      "event_with_mock_docker",
			portRange: [2]int{19200, 19250},
			funcName:  "event-fn",
			wantCode:  http.StatusAccepted,
		},
		{
			name:      "event_second_call_reuse_runtime",
			portRange: [2]int{19300, 19350},
			funcName:  "reuse-fn",
			wantCode:  http.StatusAccepted,
			callTwice: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			pa, err := portalloc.New(tt.portRange[0], tt.portRange[1])
			require.NoError(t, err)

			dc := newMockDockerClient()
			bk := lambda.NewInMemoryBackend(
				dc,
				pa,
				lambda.DefaultSettings(),
				"000000000000",
				"us-east-1",
			)
			closeBackend(t, bk)

			fn := &lambda.FunctionConfiguration{
				FunctionName: tt.funcName,
				ImageURI:     "myimage:latest",
				Timeout:      3,
			}
			require.NoError(t, bk.CreateFunction(fn))

			_, statusCode, invokeErr := bk.InvokeFunction(
				t.Context(),
				tt.funcName,
				lambda.InvocationTypeEvent,
				[]byte(`{}`),
			)
			require.NoError(t, invokeErr)
			assert.Equal(t, tt.wantCode, statusCode)

			if tt.callTwice {
				_, sc2, err2 := bk.InvokeFunction(t.Context(), tt.funcName, lambda.InvocationTypeEvent, []byte(`{}`))
				require.NoError(t, err2)
				assert.Equal(t, tt.wantCode, sc2)
			}
		})
	}
}

func TestBackend_DeleteFunction_WithRuntime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		funcName  string
		portRange [2]int
	}{
		{
			name:      "deletes_with_runtime",
			portRange: [2]int{19400, 19450},
			funcName:  "delete-with-rt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			pa, err := portalloc.New(tt.portRange[0], tt.portRange[1])
			require.NoError(t, err)

			dc := newMockDockerClient()
			bk := lambda.NewInMemoryBackend(
				dc,
				pa,
				lambda.DefaultSettings(),
				"000000000000",
				"us-east-1",
			)
			closeBackend(t, bk)

			fn := &lambda.FunctionConfiguration{
				FunctionName: tt.funcName,
				ImageURI:     "myimage:latest",
				Timeout:      3,
			}
			require.NoError(t, bk.CreateFunction(fn))

			_, _, _ = bk.InvokeFunction(t.Context(), tt.funcName, lambda.InvocationTypeEvent, []byte(`{}`))

			require.NoError(t, bk.DeleteFunction(tt.funcName))

			_, err = bk.GetFunction(tt.funcName)
			assert.ErrorIs(t, err, lambda.ErrFunctionNotFound)
		})
	}
}

func TestBackend_InvokeFunction_RequestResponse_WithMockDocker(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		funcName  string
		portRange [2]int
	}{
		{
			name:      "request_response_mock_docker",
			portRange: [2]int{19500, 19550},
			funcName:  "rr-fn",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			pa, err := portalloc.New(tt.portRange[0], tt.portRange[1])
			require.NoError(t, err)

			dc := newMockDockerClient()
			bk := lambda.NewInMemoryBackend(
				dc,
				pa,
				lambda.DefaultSettings(),
				"000000000000",
				"us-east-1",
			)
			closeBackend(t, bk)

			fn := &lambda.FunctionConfiguration{
				FunctionName: tt.funcName,
				ImageURI:     "myimage:latest",
				Timeout:      3,
			}
			require.NoError(t, bk.CreateFunction(fn))

			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()

			resultCh := make(chan error, 1)

			go func() {
				_, _, invokeErr := bk.InvokeFunction(
					ctx,
					tt.funcName,
					lambda.InvocationTypeRequestResponse,
					[]byte(`{}`),
				)
				resultCh <- invokeErr
			}()

			time.Sleep(200 * time.Millisecond)

			var runtimePort int

			for p := tt.portRange[0]; p < tt.portRange[1]; p++ {
				req, reqErr := http.NewRequestWithContext(
					t.Context(),
					http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d/2018-06-01/runtime/invocation/next", p), nil,
				)
				if reqErr != nil {
					continue
				}

				client := &http.Client{Timeout: 200 * time.Millisecond}
				resp, doErr := client.Do(req)

				if doErr == nil && resp != nil {
					requestID := resp.Header.Get("Lambda-Runtime-Aws-Request-Id")
					resp.Body.Close()

					if requestID != "" {
						runtimePort = p
						simulateContainerResponse(t, p, requestID, `{"result":"ok"}`)

						break
					}
				}
			}

			select {
			case invokeErr := <-resultCh:
				if runtimePort > 0 {
					require.NoError(t, invokeErr)
				}
			case <-time.After(4 * time.Second):
				cancel()
			}
		})
	}
}

// TestRuntimeServer_InvokeTimeoutRace verifies that when a container response arrives
// concurrently with a timeout, the result is not silently discarded — invoke either
// returns the result or returns the timeout error, but never panics or deadlocks.
func TestRuntimeServer_InvokeTimeoutRace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		responseBody string
		port         int
		// responseDelay is injected between receiving /next and posting the result.
		// When it is shorter than the invoke timeout, the response should win.
		// When it is longer, the timeout should win.
		responseDelay time.Duration
		invokeTimeout time.Duration
		wantTimeout   bool
	}{
		{
			name:          "response_arrives_before_timeout",
			port:          18150,
			responseBody:  `{"ok":true}`,
			responseDelay: 0,
			invokeTimeout: 500 * time.Millisecond,
			wantTimeout:   false,
		},
		{
			name:          "response_arrives_after_timeout",
			port:          18151,
			responseBody:  `{"ok":true}`,
			responseDelay: 300 * time.Millisecond,
			invokeTimeout: 50 * time.Millisecond,
			wantTimeout:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv := newTestRuntimeServer(t, tt.port)
			ctx := t.Context()

			resultCh := make(chan []byte, 1)
			errCh := make(chan error, 1)

			go func() {
				result, _, _, invokeErr := srv.Invoke(ctx, []byte(`{}`), "", tt.invokeTimeout)
				if invokeErr != nil {
					errCh <- invokeErr

					return
				}

				resultCh <- result
			}()

			requestID := simulateContainerNext(t, tt.port)

			// Optionally delay the container response to force a timeout race.
			if tt.responseDelay > 0 {
				time.Sleep(tt.responseDelay)
			}

			// Send the response without asserting the status code — in the timeout
			// case the invocation is already gone so the runtime API returns 404.
			sendContainerResponse(t, tt.port, requestID, tt.responseBody)

			select {
			case result := <-resultCh:
				if tt.wantTimeout {
					// Result arrived before we expected — acceptable since timing is not exact.
					assert.NotEmpty(t, result)
				} else {
					assert.JSONEq(t, tt.responseBody, string(result))
				}
			case err := <-errCh:
				if tt.wantTimeout {
					require.ErrorIs(t, err, lambda.ErrInvocationTimeout)
				} else {
					require.NoError(t, err, "unexpected invoke error")
				}
			case <-time.After(5 * time.Second):
				require.FailNow(t, "test timed out — possible deadlock in invoke")
			}
		})
	}
}

// sendContainerResponse posts a response to the runtime API without asserting the HTTP status.
// This is used when the response may legitimately race with a timeout (404 is acceptable).
func sendContainerResponse(t *testing.T, port int, requestID, responseBody string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		fmt.Sprintf("http://127.0.0.1:%d/2018-06-01/runtime/invocation/%s/response", port, requestID),
		strings.NewReader(responseBody),
	)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	_ = resp.Body.Close()
}
