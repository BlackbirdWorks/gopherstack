package lambda_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/lambda"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
)

// mockS3Fetcher implements lambda.S3CodeFetcher for testing.
type mockS3Fetcher struct {
	err  error
	data []byte
}

func (m *mockS3Fetcher) GetObjectBytes(_ context.Context, _, _ string) ([]byte, error) {
	return m.data, m.err
}

// mockDNSRegistrar is a simple in-memory DNSRegistrar for testing.
type mockDNSRegistrar struct {
	registered   []string
	deregistered []string
	mu           sync.Mutex
}

func (m *mockDNSRegistrar) Register(hostname string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.registered = append(m.registered, hostname)
}

func (m *mockDNSRegistrar) Deregister(hostname string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deregistered = append(m.deregistered, hostname)
}

// mockCWLogsBackend is a test mock for the CWLogsBackend interface.
type mockCWLogsBackend struct {
	ensureCalls []string
	putCalls    [][]string
}

func (m *mockCWLogsBackend) EnsureLogGroupAndStream(groupName, _ string) error {
	m.ensureCalls = append(m.ensureCalls, groupName)

	return nil
}

func (m *mockCWLogsBackend) PutLogLines(_, _ string, messages []string) error {
	m.putCalls = append(m.putCalls, messages)

	return nil
}

func newSimpleBackend() *lambda.InMemoryBackend {
	return lambda.NewInMemoryBackend(
		nil, nil, lambda.DefaultSettings(), "123456789012", "us-east-1", slog.Default(),
	)
}

func TestInMemoryBackend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "SetS3CodeFetcher",
			run: func(t *testing.T) {
				backend := newSimpleBackend()
				fetcher := &mockS3Fetcher{data: []byte("zip-data")}
				// SetS3CodeFetcher should not panic
				backend.SetS3CodeFetcher(fetcher)
			},
		},
		{
			name: "InvokeFunction_NoPortAlloc",
			run: func(t *testing.T) {
				ctx := context.Background()
				backend := newSimpleBackend()

				fn := &lambda.FunctionConfiguration{
					FunctionName: "no-port-fn",
					PackageType:  lambda.PackageTypeImage,
					ImageURI:     "test:latest",
				}
				require.NoError(t, backend.CreateFunction(fn))

				_, _, err := backend.InvokeFunction(ctx, "no-port-fn", lambda.InvocationTypeRequestResponse, []byte("{}"))
				require.Error(t, err)
			},
		},
		{
			name: "InvokeFunction_NotFound",
			run: func(t *testing.T) {
				ctx := context.Background()
				backend := newSimpleBackend()

				_, statusCode, err := backend.InvokeFunction(ctx, "nonexistent", lambda.InvocationTypeRequestResponse, []byte("{}"))
				require.Error(t, err)
				assert.Equal(t, http.StatusNotFound, statusCode)
			},
		},
		{
			name: "InvokeFunction_DryRun",
			run: func(t *testing.T) {
				ctx := context.Background()
				backend := newSimpleBackend()

				fn := &lambda.FunctionConfiguration{
					FunctionName: "dry-run-fn",
					PackageType:  lambda.PackageTypeImage,
					ImageURI:     "test:latest",
				}
				require.NoError(t, backend.CreateFunction(fn))

				result, statusCode, err := backend.InvokeFunction(ctx, "dry-run-fn", lambda.InvocationTypeDryRun, []byte("{}"))
				require.NoError(t, err)
				assert.Equal(t, http.StatusNoContent, statusCode)
				assert.Nil(t, result)
			},
		},
		{
			name: "InvokeFunction_EventType_NoDocker",
			run: func(t *testing.T) {
				ctx := context.Background()
				backend := newSimpleBackend()

				fn := &lambda.FunctionConfiguration{
					FunctionName: "event-fn",
					PackageType:  lambda.PackageTypeImage,
					ImageURI:     "test:latest",
				}
				require.NoError(t, backend.CreateFunction(fn))

				_, _, err := backend.InvokeFunction(ctx, "event-fn", lambda.InvocationTypeEvent, []byte("{}"))
				require.Error(t, err) // Fails because no portAlloc
			},
		},
		{
			name: "CreateAndGet",
			run: func(t *testing.T) {
				backend := newSimpleBackend()

				fn := &lambda.FunctionConfiguration{
					FunctionName: "test-create-get",
					PackageType:  lambda.PackageTypeZip,
					Runtime:      "python3.12",
				}
				require.NoError(t, backend.CreateFunction(fn))

				got, err := backend.GetFunction("test-create-get")
				require.NoError(t, err)
				assert.Equal(t, "test-create-get", got.FunctionName)
				assert.Equal(t, "python3.12", got.Runtime)
			},
		},
		{
			name: "CreateDuplicate",
			run: func(t *testing.T) {
				backend := newSimpleBackend()

				fn := &lambda.FunctionConfiguration{FunctionName: "dup-fn"}
				require.NoError(t, backend.CreateFunction(fn))

				err := backend.CreateFunction(fn)
				require.ErrorIs(t, err, lambda.ErrFunctionAlreadyExists)
			},
		},
		{
			name: "ListFunctions",
			run: func(t *testing.T) {
				backend := newSimpleBackend()

				for _, name := range []string{"fn-b", "fn-a", "fn-c"} {
					require.NoError(t, backend.CreateFunction(&lambda.FunctionConfiguration{FunctionName: name}))
				}

				fns := backend.ListFunctions()
				require.Len(t, fns, 3)
				// Should be sorted alphabetically
				assert.Equal(t, "fn-a", fns[0].FunctionName)
				assert.Equal(t, "fn-b", fns[1].FunctionName)
				assert.Equal(t, "fn-c", fns[2].FunctionName)
			},
		},
		{
			name: "UpdateFunction_NotFound",
			run: func(t *testing.T) {
				backend := newSimpleBackend()

				err := backend.UpdateFunction(&lambda.FunctionConfiguration{FunctionName: "nonexistent"})
				require.ErrorIs(t, err, lambda.ErrFunctionNotFound)
			},
		},
		{
			name: "DeleteFunction_NotFound",
			run: func(t *testing.T) {
				backend := newSimpleBackend()

				err := backend.DeleteFunction("nonexistent")
				require.ErrorIs(t, err, lambda.ErrFunctionNotFound)
			},
		},
		{
			name: "DeleteFunction_WithRuntime",
			run: func(t *testing.T) {
				backend := newSimpleBackend()

				fn := &lambda.FunctionConfiguration{
					FunctionName: "delete-with-rt",
					PackageType:  lambda.PackageTypeImage,
				}
				require.NoError(t, backend.CreateFunction(fn))
				require.NoError(t, backend.DeleteFunction("delete-with-rt"))

				_, err := backend.GetFunction("delete-with-rt")
				require.ErrorIs(t, err, lambda.ErrFunctionNotFound)
			},
		},
		{
			name: "Zip_InvokeWithMockDocker",
			run: func(t *testing.T) {
				pa, paErr := portalloc.New(19600, 19650)
				require.NoError(t, paErr)

				dc := newMockDockerClient()
				backend := lambda.NewInMemoryBackend(dc, pa, lambda.DefaultSettings(), "000000000000", "us-east-1", slog.Default())

				zipBytes := makeTestZip(t, `def handler(event, context): return "hello"`)
				fn := &lambda.FunctionConfiguration{
					FunctionName: "zip-invoke-fn",
					PackageType:  lambda.PackageTypeZip,
					Runtime:      "python3.12",
					Handler:      "index.handler",
					Timeout:      3,
					ZipData:      zipBytes,
				}
				require.NoError(t, backend.CreateFunction(fn))

				// Event invocation (fire-and-forget) — should start container with Zip mount
				_, statusCode, err := backend.InvokeFunction(
					context.Background(),
					"zip-invoke-fn",
					lambda.InvocationTypeEvent,
					[]byte(`{}`),
				)
				require.NoError(t, err)
				assert.Equal(t, http.StatusAccepted, statusCode)
			},
		},
		{
			name: "Zip_UnknownRuntime",
			run: func(t *testing.T) {
				pa, paErr := portalloc.New(19700, 19750)
				require.NoError(t, paErr)

				dc := newMockDockerClient()
				backend := lambda.NewInMemoryBackend(dc, pa, lambda.DefaultSettings(), "000000000000", "us-east-1", slog.Default())

				zipBytes := makeTestZip(t, `def handler(e, c): return "hi"`)
				fn := &lambda.FunctionConfiguration{
					FunctionName: "unknown-runtime-fn",
					PackageType:  lambda.PackageTypeZip,
					Runtime:      "cobol99",
					Timeout:      3,
					ZipData:      zipBytes,
				}
				require.NoError(t, backend.CreateFunction(fn))

				// Should fail: unknown runtime has no base image
				_, _, err := backend.InvokeFunction(
					context.Background(),
					"unknown-runtime-fn",
					lambda.InvocationTypeEvent,
					[]byte(`{}`),
				)
				require.NoError(t, err) // Event invocations log errors but don't return them
			},
		},
		{
			name: "Zip_S3Fetcher",
			run: func(t *testing.T) {
				pa, paErr := portalloc.New(19800, 19850)
				require.NoError(t, paErr)

				dc := newMockDockerClient()
				backend := lambda.NewInMemoryBackend(dc, pa, lambda.DefaultSettings(), "000000000000", "us-east-1", slog.Default())

				zipBytes := makeTestZip(t, `def handler(e, c): return "hello"`)
				fetcher := &mockS3Fetcher{data: zipBytes}
				backend.SetS3CodeFetcher(fetcher)

				fn := &lambda.FunctionConfiguration{
					FunctionName: "s3-zip-fn",
					PackageType:  lambda.PackageTypeZip,
					Runtime:      "python3.12",
					Handler:      "index.handler",
					Timeout:      3,
					S3BucketCode: "my-bucket",
					S3KeyCode:    "my-key.zip",
				}
				require.NoError(t, backend.CreateFunction(fn))

				// Event invocation - should fetch from S3
				_, statusCode, err := backend.InvokeFunction(
					context.Background(),
					"s3-zip-fn",
					lambda.InvocationTypeEvent,
					[]byte(`{}`),
				)
				require.NoError(t, err)
				assert.Equal(t, http.StatusAccepted, statusCode)
			},
		},
		{
			name: "Zip_S3FetcherNoFetcher",
			run: func(t *testing.T) {
				pa, paErr := portalloc.New(19900, 19950)
				require.NoError(t, paErr)

				dc := newMockDockerClient()
				backend := lambda.NewInMemoryBackend(dc, pa, lambda.DefaultSettings(), "000000000000", "us-east-1", slog.Default())
				// No S3 fetcher set

				fn := &lambda.FunctionConfiguration{
					FunctionName: "s3-no-fetcher",
					PackageType:  lambda.PackageTypeZip,
					Runtime:      "python3.12",
					Timeout:      3,
					S3BucketCode: "my-bucket",
					S3KeyCode:    "my-key.zip",
				}
				require.NoError(t, backend.CreateFunction(fn))

				// Event invocation - should fail gracefully (logs error, returns 202 for fire-and-forget)
				_, _, _ = backend.InvokeFunction(context.Background(), "s3-no-fetcher", lambda.InvocationTypeEvent, []byte(`{}`))
				// Just verify no panic
			},
		},
		{
			name: "DeleteZipFunction_CleansUpDir",
			run: func(t *testing.T) {
				pa, paErr := portalloc.New(20000, 20050)
				require.NoError(t, paErr)

				dc := newMockDockerClient()
				backend := lambda.NewInMemoryBackend(dc, pa, lambda.DefaultSettings(), "000000000000", "us-east-1", slog.Default())

				zipBytes := makeTestZip(t, `def handler(e, c): return "hello"`)
				fn := &lambda.FunctionConfiguration{
					FunctionName: "zip-cleanup",
					PackageType:  lambda.PackageTypeZip,
					Runtime:      "python3.12",
					Timeout:      3,
					ZipData:      zipBytes,
				}
				require.NoError(t, backend.CreateFunction(fn))

				// Trigger zip extraction by invoking
				_, _, _ = backend.InvokeFunction(context.Background(), "zip-cleanup", lambda.InvocationTypeEvent, []byte(`{}`))

				// Delete should clean up temp dir without error
				require.NoError(t, backend.DeleteFunction("zip-cleanup"))
			},
		},
		{
			name: "SetDNSRegistrar",
			run: func(t *testing.T) {
				pa, paErr := portalloc.New(20300, 20350)
				require.NoError(t, paErr)

				backend := lambda.NewInMemoryBackend(
					nil, pa, lambda.DefaultSettings(), "000000000000", "us-east-1", slog.Default(),
				)

				dns := &mockDNSRegistrar{}
				lambda.SetDNSRegistrarExported(backend, dns)

				fn := &lambda.FunctionConfiguration{
					FunctionName: "dns-test-fn",
					PackageType:  lambda.PackageTypeImage,
					ImageURI:     "test:latest",
				}
				require.NoError(t, backend.CreateFunction(fn))

				cfg, err := backend.CreateFunctionURLConfig("dns-test-fn", "NONE")
				require.NoError(t, err)
				assert.NotEmpty(t, cfg.FunctionURL)

				// DNS should have been registered
				dns.mu.Lock()
				assert.NotEmpty(t, dns.registered)
				dns.mu.Unlock()

				// Delete should deregister
				require.NoError(t, backend.DeleteFunctionURLConfig("dns-test-fn"))

				dns.mu.Lock()
				assert.NotEmpty(t, dns.deregistered)
				dns.mu.Unlock()
			},
		},
		{
			name: "SetCWLogsBackend",
			run: func(t *testing.T) {
				backend := lambda.NewInMemoryBackend(
					nil, nil, lambda.DefaultSettings(), "000000000000", "us-east-1", slog.Default(),
				)

				mock := &mockCWLogsBackend{}
				backend.SetCWLogsBackend(mock) // should not panic
			},
		},
		{
			name: "GetVersion",
			run: func(t *testing.T) {
				pa, paErr := portalloc.New(19680, 19700)
				require.NoError(t, paErr)

				backend := lambda.NewInMemoryBackend(
					nil, pa, lambda.DefaultSettings(), "000000000000", "us-east-1", slog.Default(),
				)

				fn := &lambda.FunctionConfiguration{
					FunctionName: "get-ver-fn",
					PackageType:  lambda.PackageTypeImage,
					ImageURI:     "test:latest",
					State:        lambda.FunctionStateActive,
				}
				require.NoError(t, backend.CreateFunction(fn))

				// $LATEST version
				v, err := backend.GetVersion("get-ver-fn", "$LATEST")
				require.NoError(t, err)
				assert.Equal(t, "$LATEST", v.Version)
				assert.Equal(t, "get-ver-fn", v.FunctionName)

				// Publish version 1
				_, err = backend.PublishVersion("get-ver-fn", "desc")
				require.NoError(t, err)

				v1, err := backend.GetVersion("get-ver-fn", "1")
				require.NoError(t, err)
				assert.Equal(t, "1", v1.Version)

				// Non-existent version
				_, err = backend.GetVersion("get-ver-fn", "999")
				require.ErrorIs(t, err, lambda.ErrVersionNotFound)

				// Non-existent function with $LATEST
				_, err = backend.GetVersion("no-fn", "$LATEST")
				require.ErrorIs(t, err, lambda.ErrFunctionNotFound)
			},
		},
		{
			name: "ResolveQualifier",
			run: func(t *testing.T) {
				pa, paErr := portalloc.New(19700, 19720)
				require.NoError(t, paErr)

				backend := lambda.NewInMemoryBackend(
					nil, pa, lambda.DefaultSettings(), "000000000000", "us-east-1", slog.Default(),
				)

				fn := &lambda.FunctionConfiguration{
					FunctionName: "resolve-fn",
					PackageType:  lambda.PackageTypeImage,
					ImageURI:     "test:latest",
					State:        lambda.FunctionStateActive,
				}
				require.NoError(t, backend.CreateFunction(fn))

				// Publish two versions
				_, err := backend.PublishVersion("resolve-fn", "v1")
				require.NoError(t, err)
				_, err = backend.PublishVersion("resolve-fn", "v2")
				require.NoError(t, err)

				// Create alias pointing to v1
				_, err = backend.CreateAlias("resolve-fn", &lambda.CreateAliasInput{
					Name:            "stable",
					FunctionVersion: "1",
				})
				require.NoError(t, err)

				// Invoke with no qualifier should succeed (resolves to ErrLambdaUnavailable, not ErrFunctionNotFound)
				_, _, invokeErr := backend.InvokeFunctionWithQualifier(
					context.Background(), "resolve-fn", "", lambda.InvocationTypeRequestResponse, []byte("{}"),
				)
				require.ErrorIs(t, invokeErr, lambda.ErrLambdaUnavailable)

				// Invoke with alias qualifier (resolves alias → version → config)
				_, _, invokeErr = backend.InvokeFunctionWithQualifier(
					context.Background(), "resolve-fn", "stable", lambda.InvocationTypeRequestResponse, []byte("{}"),
				)
				require.ErrorIs(t, invokeErr, lambda.ErrLambdaUnavailable)

				// Invoke with version qualifier
				_, _, invokeErr = backend.InvokeFunctionWithQualifier(
					context.Background(), "resolve-fn", "1", lambda.InvocationTypeRequestResponse, []byte("{}"),
				)
				require.ErrorIs(t, invokeErr, lambda.ErrLambdaUnavailable)

				// Invoke with non-existent version should return ErrVersionNotFound
				_, _, invokeErr = backend.InvokeFunctionWithQualifier(
					context.Background(), "resolve-fn", "999", lambda.InvocationTypeRequestResponse, []byte("{}"),
				)
				require.ErrorIs(t, invokeErr, lambda.ErrVersionNotFound)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

func TestBuildURLEventPayload(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "WithBody",
			run: func(t *testing.T) {
				backend := lambda.NewInMemoryBackend(
					nil, nil, lambda.DefaultSettings(), "000000000000", "us-east-1", slog.Default(),
				)

				req, err := http.NewRequestWithContext(
					context.Background(),
					http.MethodPost,
					"http://example.com/my/path?foo=bar",
					strings.NewReader("hello world"),
				)
				require.NoError(t, err)
				req.Header.Set("Content-Type", "text/plain")

				payload, err := lambda.BuildURLEventPayload(backend, req)
				require.NoError(t, err)

				var event map[string]any
				require.NoError(t, json.Unmarshal(payload, &event))

				assert.Equal(t, "2.0", event["version"])
				assert.Equal(t, "$default", event["routeKey"])
				assert.Equal(t, "/my/path", event["rawPath"])
				assert.Equal(t, "foo=bar", event["rawQueryString"])
				assert.True(t, event["isBase64Encoded"].(bool), "body should be base64-encoded")
			},
		},
		{
			name: "EmptyBody",
			run: func(t *testing.T) {
				backend := lambda.NewInMemoryBackend(
					nil, nil, lambda.DefaultSettings(), "000000000000", "us-east-1", slog.Default(),
				)

				req, err := http.NewRequestWithContext(
					context.Background(),
					http.MethodGet,
					"http://example.com/",
					nil,
				)
				require.NoError(t, err)

				payload, err := lambda.BuildURLEventPayload(backend, req)
				require.NoError(t, err)

				var event map[string]any
				require.NoError(t, json.Unmarshal(payload, &event))
				_, hasBody := event["body"]
				assert.False(t, hasBody, "empty body should not include body field")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

func TestWriteFunctionURLResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "StructuredResponse",
			run: func(t *testing.T) {
				result := []byte(`{"statusCode":200,"headers":{"content-type":"application/json"},"body":"{\"ok\":true}"}`)
				rec := httptest.NewRecorder()
				lambda.WriteFunctionURLResponse(rec, result)

				assert.Equal(t, http.StatusOK, rec.Code)
				assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
				assert.Equal(t, `{"ok":true}`, rec.Body.String())
			},
		},
		{
			name: "Base64Body",
			run: func(t *testing.T) {
				encoded := base64.StdEncoding.EncodeToString([]byte("binary data"))
				result := []byte(`{"statusCode":200,"body":"` + encoded + `","isBase64Encoded":true}`)
				rec := httptest.NewRecorder()
				lambda.WriteFunctionURLResponse(rec, result)

				assert.Equal(t, http.StatusOK, rec.Code)
				assert.Equal(t, "binary data", rec.Body.String())
			},
		},
		{
			name: "RawFallback",
			run: func(t *testing.T) {
				result := []byte(`{"result":"plain"}`)
				rec := httptest.NewRecorder()
				lambda.WriteFunctionURLResponse(rec, result)

				assert.Equal(t, http.StatusOK, rec.Code)
				assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
				assert.JSONEq(t, `{"result":"plain"}`, rec.Body.String())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

func TestFunctionURLConfig_HTTPEndpoint(t *testing.T) {
	t.Parallel()

	pa, paErr := portalloc.New(20400, 20450)
	require.NoError(t, paErr)

	backend := lambda.NewInMemoryBackend(
		nil, pa, lambda.DefaultSettings(), "000000000000", "us-east-1", slog.Default(),
	)

	fn := &lambda.FunctionConfiguration{
		FunctionName: "http-test-fn",
		PackageType:  lambda.PackageTypeImage,
		ImageURI:     "test:latest",
	}
	require.NoError(t, backend.CreateFunction(fn))

	cfg, err := backend.CreateFunctionURLConfig("http-test-fn", "NONE")
	require.NoError(t, err)
	assert.Contains(t, cfg.FunctionURL, "127.0.0.1", "URL should use loopback when no DNS")

	// The listener is running — make an HTTP request to it.
	client := &http.Client{Timeout: 5 * time.Second}
	req, err := http.NewRequestWithContext(
		context.Background(), http.MethodGet, cfg.FunctionURL, nil,
	)
	require.NoError(t, err)

	resp, err := client.Do(req)
	require.NoError(t, err, "listener should respond")
	defer resp.Body.Close()

	// Without Docker the invocation fails, so we expect a 500 error response.
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}
