package lambda_test

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/lambda"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
)

// mockS3Fetcher implements lambda.S3CodeFetcher for testing.
type mockS3Fetcher struct {
	data []byte
	err  error
}

func (m *mockS3Fetcher) GetObjectBytes(_ context.Context, _, _ string) ([]byte, error) {
	return m.data, m.err
}

func TestInMemoryBackend_SetS3CodeFetcher(t *testing.T) {
	t.Parallel()

	backend := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "123456789012", "us-east-1", slog.Default())
	fetcher := &mockS3Fetcher{data: []byte("zip-data")}
	// SetS3CodeFetcher should not panic
	backend.SetS3CodeFetcher(fetcher)
}

func TestInMemoryBackend_InvokeFunction_NoPortAlloc(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	backend := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "123456789012", "us-east-1", slog.Default())

	fn := &lambda.FunctionConfiguration{
		FunctionName: "no-port-fn",
		PackageType:  lambda.PackageTypeImage,
		ImageURI:     "test:latest",
	}
	require.NoError(t, backend.CreateFunction(fn))

	_, _, err := backend.InvokeFunction(ctx, "no-port-fn", lambda.InvocationTypeRequestResponse, []byte("{}"))
	require.Error(t, err)
}

func TestInMemoryBackend_InvokeFunction_NotFound(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	backend := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "123456789012", "us-east-1", slog.Default())

	_, statusCode, err := backend.InvokeFunction(ctx, "nonexistent", lambda.InvocationTypeRequestResponse, []byte("{}"))
	require.Error(t, err)
	assert.Equal(t, 404, statusCode) //nolint:mnd // HTTP 404 Not Found
}

func TestInMemoryBackend_InvokeFunction_DryRun(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	backend := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "123456789012", "us-east-1", slog.Default())

	fn := &lambda.FunctionConfiguration{
		FunctionName: "dry-run-fn",
		PackageType:  lambda.PackageTypeImage,
		ImageURI:     "test:latest",
	}
	require.NoError(t, backend.CreateFunction(fn))

	result, statusCode, err := backend.InvokeFunction(ctx, "dry-run-fn", lambda.InvocationTypeDryRun, []byte("{}"))
	require.NoError(t, err)
	assert.Equal(t, 204, statusCode) //nolint:mnd // HTTP 204 No Content
	assert.Nil(t, result)
}

func TestInMemoryBackend_InvokeFunction_EventType_NoDocker(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	backend := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "123456789012", "us-east-1", slog.Default())

	fn := &lambda.FunctionConfiguration{
		FunctionName: "event-fn",
		PackageType:  lambda.PackageTypeImage,
		ImageURI:     "test:latest",
	}
	require.NoError(t, backend.CreateFunction(fn))

	_, _, err := backend.InvokeFunction(ctx, "event-fn", lambda.InvocationTypeEvent, []byte("{}"))
	require.Error(t, err) // Fails because no portAlloc
}

func TestInMemoryBackend_CreateAndGet(t *testing.T) {
	t.Parallel()

	backend := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "123456789012", "us-east-1", slog.Default())

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

	backend := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "123456789012", "us-east-1", slog.Default())

	fn := &lambda.FunctionConfiguration{FunctionName: "dup-fn"}
	require.NoError(t, backend.CreateFunction(fn))

	err := backend.CreateFunction(fn)
	require.ErrorIs(t, err, lambda.ErrFunctionAlreadyExists)
}

func TestInMemoryBackend_ListFunctions(t *testing.T) {
	t.Parallel()

	backend := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "123456789012", "us-east-1", slog.Default())

	for _, name := range []string{"fn-b", "fn-a", "fn-c"} {
		require.NoError(t, backend.CreateFunction(&lambda.FunctionConfiguration{FunctionName: name}))
	}

	fns := backend.ListFunctions()
	require.Len(t, fns, 3)
	// Should be sorted alphabetically
	assert.Equal(t, "fn-a", fns[0].FunctionName)
	assert.Equal(t, "fn-b", fns[1].FunctionName)
	assert.Equal(t, "fn-c", fns[2].FunctionName)
}

func TestInMemoryBackend_UpdateFunction_NotFound(t *testing.T) {
	t.Parallel()

	backend := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "123456789012", "us-east-1", slog.Default())

	err := backend.UpdateFunction(&lambda.FunctionConfiguration{FunctionName: "nonexistent"})
	require.ErrorIs(t, err, lambda.ErrFunctionNotFound)
}

func TestInMemoryBackend_DeleteFunction_NotFound(t *testing.T) {
	t.Parallel()

	backend := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "123456789012", "us-east-1", slog.Default())

	err := backend.DeleteFunction("nonexistent")
	require.ErrorIs(t, err, lambda.ErrFunctionNotFound)
}

func TestInMemoryBackend_DeleteFunction_WithRuntime(t *testing.T) {
	t.Parallel()

	backend := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "123456789012", "us-east-1", slog.Default())

	fn := &lambda.FunctionConfiguration{
		FunctionName: "delete-with-rt",
		PackageType:  lambda.PackageTypeImage,
	}
	require.NoError(t, backend.CreateFunction(fn))
	require.NoError(t, backend.DeleteFunction("delete-with-rt"))

	_, err := backend.GetFunction("delete-with-rt")
	require.ErrorIs(t, err, lambda.ErrFunctionNotFound)
}

func TestInMemoryBackend_Zip_InvokeWithMockDocker(t *testing.T) {
t.Parallel()

pa, paErr := portalloc.New(19600, 19650)
require.NoError(t, paErr)

dc := newMockDockerClient()
backend := lambda.NewInMemoryBackend(dc, pa, lambda.DefaultSettings(), "000000000000", "us-east-1", slog.Default())

zipBytes := makeTestZip(t, "index.py", `def handler(event, context): return "hello"`)
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
_, statusCode, err := backend.InvokeFunction(context.Background(), "zip-invoke-fn", lambda.InvocationTypeEvent, []byte(`{}`))
require.NoError(t, err)
assert.Equal(t, 202, statusCode) //nolint:mnd // HTTP 202 Accepted
}

func TestInMemoryBackend_Zip_UnknownRuntime(t *testing.T) {
t.Parallel()

pa, paErr := portalloc.New(19700, 19750)
require.NoError(t, paErr)

dc := newMockDockerClient()
backend := lambda.NewInMemoryBackend(dc, pa, lambda.DefaultSettings(), "000000000000", "us-east-1", slog.Default())

zipBytes := makeTestZip(t, "index.py", `def handler(e, c): return "hi"`)
fn := &lambda.FunctionConfiguration{
FunctionName: "unknown-runtime-fn",
PackageType:  lambda.PackageTypeZip,
Runtime:      "cobol99",
Timeout:      3,
ZipData:      zipBytes,
}
require.NoError(t, backend.CreateFunction(fn))

// Should fail: unknown runtime has no base image
_, _, err := backend.InvokeFunction(context.Background(), "unknown-runtime-fn", lambda.InvocationTypeEvent, []byte(`{}`))
require.NoError(t, err) // Event invocations log errors but don't return them
}

func TestInMemoryBackend_Zip_S3Fetcher(t *testing.T) {
t.Parallel()

pa, paErr := portalloc.New(19800, 19850)
require.NoError(t, paErr)

dc := newMockDockerClient()
backend := lambda.NewInMemoryBackend(dc, pa, lambda.DefaultSettings(), "000000000000", "us-east-1", slog.Default())

zipBytes := makeTestZip(t, "index.py", `def handler(e, c): return "hello"`)
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
_, statusCode, err := backend.InvokeFunction(context.Background(), "s3-zip-fn", lambda.InvocationTypeEvent, []byte(`{}`))
require.NoError(t, err)
assert.Equal(t, 202, statusCode) //nolint:mnd // HTTP 202 Accepted
}

func TestInMemoryBackend_Zip_S3FetcherNoFetcher(t *testing.T) {
t.Parallel()

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
}

func TestInMemoryBackend_DeleteZipFunction_CleansUpDir(t *testing.T) {
t.Parallel()

pa, paErr := portalloc.New(20000, 20050)
require.NoError(t, paErr)

dc := newMockDockerClient()
backend := lambda.NewInMemoryBackend(dc, pa, lambda.DefaultSettings(), "000000000000", "us-east-1", slog.Default())

zipBytes := makeTestZip(t, "index.py", `def handler(e, c): return "hello"`)
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
}
