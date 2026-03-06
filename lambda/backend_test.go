package lambda_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
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
	return lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "123456789012", "us-east-1")
}

func TestInMemoryBackend_SetS3CodeFetcher(t *testing.T) {
	t.Parallel()

	backend := newSimpleBackend()
	fetcher := &mockS3Fetcher{data: []byte("zip-data")}
	// SetS3CodeFetcher should not panic
	backend.SetS3CodeFetcher(fetcher)
}

func TestInMemoryBackend_InvokeFunction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn             *lambda.FunctionConfiguration
		name           string
		invokeName     string
		invocationType lambda.InvocationType
		wantStatus     int
		wantErr        bool
		wantNilResult  bool
	}{
		{
			name: "NoPortAlloc",
			fn: &lambda.FunctionConfiguration{
				FunctionName: "no-port-fn",
				PackageType:  lambda.PackageTypeImage,
				ImageURI:     "test:latest",
			},
			invokeName:     "no-port-fn",
			invocationType: lambda.InvocationTypeRequestResponse,
			wantErr:        true,
		},
		{
			name:           "NotFound",
			invokeName:     "nonexistent",
			invocationType: lambda.InvocationTypeRequestResponse,
			wantErr:        true,
			wantStatus:     http.StatusNotFound,
		},
		{
			name: "DryRun",
			fn: &lambda.FunctionConfiguration{
				FunctionName: "dry-run-fn",
				PackageType:  lambda.PackageTypeImage,
				ImageURI:     "test:latest",
			},
			invokeName:     "dry-run-fn",
			invocationType: lambda.InvocationTypeDryRun,
			wantStatus:     http.StatusNoContent,
			wantNilResult:  true,
		},
		{
			name: "EventType_NoDocker",
			fn: &lambda.FunctionConfiguration{
				FunctionName: "event-fn",
				PackageType:  lambda.PackageTypeImage,
				ImageURI:     "test:latest",
			},
			invokeName:     "event-fn",
			invocationType: lambda.InvocationTypeEvent,
			wantErr:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := newSimpleBackend()
			if tt.fn != nil {
				require.NoError(t, backend.CreateFunction(tt.fn))
			}

			result, statusCode, err := backend.InvokeFunction(
				t.Context(), tt.invokeName, tt.invocationType, []byte("{}"),
			)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			if tt.wantStatus != 0 {
				assert.Equal(t, tt.wantStatus, statusCode)
			}
			if tt.wantNilResult {
				assert.Nil(t, result)
			}
		})
	}
}

func TestInMemoryBackend_CreateAndGet(t *testing.T) {
	t.Parallel()

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
}

func TestInMemoryBackend_CreateDuplicate(t *testing.T) {
	t.Parallel()

	backend := newSimpleBackend()

	fn := &lambda.FunctionConfiguration{FunctionName: "dup-fn"}
	require.NoError(t, backend.CreateFunction(fn))

	err := backend.CreateFunction(fn)
	require.ErrorIs(t, err, lambda.ErrFunctionAlreadyExists)
}

func TestInMemoryBackend_ListFunctions(t *testing.T) {
	t.Parallel()

	backend := newSimpleBackend()

	for _, name := range []string{"fn-b", "fn-a", "fn-c"} {
		require.NoError(t, backend.CreateFunction(&lambda.FunctionConfiguration{FunctionName: name}))
	}

	fns := backend.ListFunctions("", 0)
	require.Len(t, fns.Data, 3)
	// Should be sorted alphabetically
	assert.Equal(t, "fn-a", fns.Data[0].FunctionName)
	assert.Equal(t, "fn-b", fns.Data[1].FunctionName)
	assert.Equal(t, "fn-c", fns.Data[2].FunctionName)
}

func TestInMemoryBackend_UpdateFunctionNotFound(t *testing.T) {
	t.Parallel()

	backend := newSimpleBackend()

	err := backend.UpdateFunction(&lambda.FunctionConfiguration{FunctionName: "nonexistent"})
	require.ErrorIs(t, err, lambda.ErrFunctionNotFound)
}

func TestInMemoryBackend_DeleteFunctionNotFound(t *testing.T) {
	t.Parallel()

	backend := newSimpleBackend()

	err := backend.DeleteFunction("nonexistent")
	require.ErrorIs(t, err, lambda.ErrFunctionNotFound)
}

func TestInMemoryBackend_DeleteFunctionWithRuntime(t *testing.T) {
	t.Parallel()

	backend := newSimpleBackend()

	fn := &lambda.FunctionConfiguration{
		FunctionName: "delete-with-rt",
		PackageType:  lambda.PackageTypeImage,
	}
	require.NoError(t, backend.CreateFunction(fn))
	require.NoError(t, backend.DeleteFunction("delete-with-rt"))

	_, err := backend.GetFunction("delete-with-rt")
	require.ErrorIs(t, err, lambda.ErrFunctionNotFound)
}

func TestInMemoryBackend_ZipInvoke(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		runtime      string
		handler      string
		s3Bucket     string
		s3Key        string
		portStart    int
		portEnd      int
		wantStatus   int
		setS3Fetcher bool
		skipErrCheck bool
	}{
		{
			name:       "WithMockDocker",
			portStart:  19600,
			portEnd:    19650,
			runtime:    "python3.12",
			handler:    "index.handler",
			wantStatus: http.StatusAccepted,
		},
		{
			name:      "UnknownRuntime",
			portStart: 19700,
			portEnd:   19750,
			runtime:   "cobol99",
		},
		{
			name:         "S3Fetcher",
			portStart:    19800,
			portEnd:      19850,
			runtime:      "python3.12",
			handler:      "index.handler",
			s3Bucket:     "my-bucket",
			s3Key:        "my-key.zip",
			setS3Fetcher: true,
			wantStatus:   http.StatusAccepted,
		},
		{
			name:         "S3FetcherNoFetcher",
			portStart:    19900,
			portEnd:      19950,
			runtime:      "python3.12",
			s3Bucket:     "my-bucket",
			s3Key:        "my-key.zip",
			skipErrCheck: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			pa, paErr := portalloc.New(tt.portStart, tt.portEnd)
			require.NoError(t, paErr)

			dc := newMockDockerClient()
			backend := lambda.NewInMemoryBackend(
				dc,
				pa,
				lambda.DefaultSettings(),
				"000000000000",
				"us-east-1",
			)

			zipBytes := makeTestZip(t, `def handler(e, c): return "hello"`)

			if tt.setS3Fetcher {
				backend.SetS3CodeFetcher(&mockS3Fetcher{data: zipBytes})
			}

			fn := &lambda.FunctionConfiguration{
				FunctionName: tt.name + "-fn",
				PackageType:  lambda.PackageTypeZip,
				Runtime:      tt.runtime,
				Handler:      tt.handler,
				Timeout:      3,
			}
			if tt.s3Bucket != "" {
				fn.S3BucketCode = tt.s3Bucket
				fn.S3KeyCode = tt.s3Key
			} else {
				fn.ZipData = zipBytes
			}

			require.NoError(t, backend.CreateFunction(fn))

			_, statusCode, err := backend.InvokeFunction(
				t.Context(), fn.FunctionName, lambda.InvocationTypeEvent, []byte(`{}`),
			)
			if !tt.skipErrCheck {
				require.NoError(t, err)
			}
			if tt.wantStatus != 0 {
				assert.Equal(t, tt.wantStatus, statusCode)
			}
		})
	}
}

func TestInMemoryBackend_DeleteZipFunction(t *testing.T) {
	t.Parallel()

	pa, paErr := portalloc.New(20000, 20050)
	require.NoError(t, paErr)

	dc := newMockDockerClient()
	backend := lambda.NewInMemoryBackend(dc, pa, lambda.DefaultSettings(), "000000000000", "us-east-1")

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
	_, _, _ = backend.InvokeFunction(t.Context(), "zip-cleanup", lambda.InvocationTypeEvent, []byte(`{}`))

	// Delete should clean up temp dir without error
	require.NoError(t, backend.DeleteFunction("zip-cleanup"))
}

func TestInMemoryBackend_SetDNSRegistrar(t *testing.T) {
	t.Parallel()

	pa, paErr := portalloc.New(20300, 20350)
	require.NoError(t, paErr)

	backend := lambda.NewInMemoryBackend(
		nil, pa, lambda.DefaultSettings(), "000000000000", "us-east-1",
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
}

func TestInMemoryBackend_SetCWLogsBackend(t *testing.T) {
	t.Parallel()

	backend := lambda.NewInMemoryBackend(
		nil, nil, lambda.DefaultSettings(), "000000000000", "us-east-1",
	)

	mock := &mockCWLogsBackend{}
	backend.SetCWLogsBackend(mock) // should not panic
}

func TestInMemoryBackend_GetVersion(t *testing.T) {
	t.Parallel()

	pa, paErr := portalloc.New(19680, 19700)
	require.NoError(t, paErr)

	backend := lambda.NewInMemoryBackend(
		nil, pa, lambda.DefaultSettings(), "000000000000", "us-east-1",
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
}

func TestInMemoryBackend_ResolveQualifier(t *testing.T) {
	t.Parallel()

	pa, paErr := portalloc.New(19700, 19720)
	require.NoError(t, paErr)

	backend := lambda.NewInMemoryBackend(
		nil, pa, lambda.DefaultSettings(), "000000000000", "us-east-1",
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
		t.Context(), "resolve-fn", "", lambda.InvocationTypeRequestResponse, []byte("{}"),
	)
	require.ErrorIs(t, invokeErr, lambda.ErrLambdaUnavailable)

	// Invoke with alias qualifier (resolves alias → version → config)
	_, _, invokeErr = backend.InvokeFunctionWithQualifier(
		t.Context(), "resolve-fn", "stable", lambda.InvocationTypeRequestResponse, []byte("{}"),
	)
	require.ErrorIs(t, invokeErr, lambda.ErrLambdaUnavailable)

	// Invoke with version qualifier
	_, _, invokeErr = backend.InvokeFunctionWithQualifier(
		t.Context(), "resolve-fn", "1", lambda.InvocationTypeRequestResponse, []byte("{}"),
	)
	require.ErrorIs(t, invokeErr, lambda.ErrLambdaUnavailable)

	// Invoke with non-existent version should return ErrVersionNotFound
	_, _, invokeErr = backend.InvokeFunctionWithQualifier(
		t.Context(), "resolve-fn", "999", lambda.InvocationTypeRequestResponse, []byte("{}"),
	)
	require.ErrorIs(t, invokeErr, lambda.ErrVersionNotFound)
}

func TestBuildURLEventPayload(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		method             string
		url                string
		body               string
		contentType        string
		wantVersion        string
		wantRouteKey       string
		wantRawPath        string
		wantRawQueryString string
		wantBase64Encoded  bool
		wantNoBody         bool
	}{
		{
			name:               "WithBody",
			method:             http.MethodPost,
			url:                "http://example.com/my/path?foo=bar",
			body:               "hello world",
			contentType:        "text/plain",
			wantVersion:        "2.0",
			wantRouteKey:       "$default",
			wantRawPath:        "/my/path",
			wantRawQueryString: "foo=bar",
			wantBase64Encoded:  true,
		},
		{
			name:       "EmptyBody",
			method:     http.MethodGet,
			url:        "http://example.com/",
			wantNoBody: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := lambda.NewInMemoryBackend(
				nil, nil, lambda.DefaultSettings(), "000000000000", "us-east-1",
			)

			var bodyReader io.Reader
			if tt.body != "" {
				bodyReader = strings.NewReader(tt.body)
			}

			req, err := http.NewRequestWithContext(t.Context(), tt.method, tt.url, bodyReader)
			require.NoError(t, err)

			if tt.contentType != "" {
				req.Header.Set("Content-Type", tt.contentType)
			}

			payload, err := lambda.BuildURLEventPayload(backend, req)
			require.NoError(t, err)

			var event map[string]any
			require.NoError(t, json.Unmarshal(payload, &event))

			if tt.wantVersion != "" {
				assert.Equal(t, tt.wantVersion, event["version"])
			}
			if tt.wantRouteKey != "" {
				assert.Equal(t, tt.wantRouteKey, event["routeKey"])
			}
			if tt.wantRawPath != "" {
				assert.Equal(t, tt.wantRawPath, event["rawPath"])
			}
			if tt.wantRawQueryString != "" {
				assert.Equal(t, tt.wantRawQueryString, event["rawQueryString"])
			}
			if tt.wantBase64Encoded {
				assert.True(t, event["isBase64Encoded"].(bool), "body should be base64-encoded")
			}
			if tt.wantNoBody {
				_, hasBody := event["body"]
				assert.False(t, hasBody, "empty body should not include body field")
			}
		})
	}
}

func TestWriteFunctionURLResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		wantContentType string
		wantBody        string
		wantBodyJSON    string
		result          []byte
		wantCode        int
	}{
		{
			name: "StructuredResponse",
			result: []byte(
				`{"statusCode":200,"headers":{"content-type":"application/json"},"body":"{\"ok\":true}"}`,
			),
			wantCode:        http.StatusOK,
			wantContentType: "application/json",
			wantBody:        `{"ok":true}`,
		},
		{
			name: "Base64Body",
			result: []byte(
				`{"statusCode":200,"body":"` + base64.StdEncoding.EncodeToString(
					[]byte("binary data"),
				) + `","isBase64Encoded":true}`,
			),
			wantCode: http.StatusOK,
			wantBody: "binary data",
		},
		{
			name:            "RawFallback",
			result:          []byte(`{"result":"plain"}`),
			wantCode:        http.StatusOK,
			wantContentType: "application/json",
			wantBodyJSON:    `{"result":"plain"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := httptest.NewRecorder()
			lambda.WriteFunctionURLResponse(rec, tt.result)

			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.wantContentType != "" {
				assert.Equal(t, tt.wantContentType, rec.Header().Get("Content-Type"))
			}
			if tt.wantBodyJSON != "" {
				assert.JSONEq(t, tt.wantBodyJSON, rec.Body.String())
			} else if tt.wantBody != "" {
				assert.Equal(t, tt.wantBody, rec.Body.String())
			}
		})
	}
}

func TestFunctionURLConfig_HTTPEndpoint(t *testing.T) {
	t.Parallel()

	pa, paErr := portalloc.New(20400, 20450)
	require.NoError(t, paErr)

	backend := lambda.NewInMemoryBackend(
		nil, pa, lambda.DefaultSettings(), "000000000000", "us-east-1",
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
		t.Context(), http.MethodGet, cfg.FunctionURL, nil,
	)
	require.NoError(t, err)

	resp, err := client.Do(req)
	require.NoError(t, err, "listener should respond")
	defer resp.Body.Close()

	// Without Docker the invocation fails, so we expect a 500 error response.
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}
