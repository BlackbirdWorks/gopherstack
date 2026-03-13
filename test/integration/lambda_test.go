package integration_test

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	dockerapibuild "github.com/docker/docker/api/types/build"
	dockerapicontainer "github.com/docker/docker/api/types/container"
	dockerapifilters "github.com/docker/docker/api/types/filters"
	dockerapiimage "github.com/docker/docker/api/types/image"
	dockerclient "github.com/docker/docker/client"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	lambdapkg "github.com/blackbirdworks/gopherstack/services/lambda"
)

const (
	// lambdaTestPortStart is the start of the port range reserved for Lambda runtime API servers
	// during integration tests. This range is separate from the default (10000-10100) to avoid
	// conflicts with other test infrastructure.
	lambdaTestPortStart = 19000
	// lambdaTestPortEnd is the exclusive end of the Lambda test port range.
	lambdaTestPortEnd = 19010
	// lambdaEchoImage is the local image tag for the minimal Go echo Lambda handler.
	lambdaEchoImage = "gopherstack-lambda-echo-test:latest"
	// lambdaFunctionName is the function name used throughout the lifecycle test.
	lambdaFunctionName = "lambda-echo-integration"
	// lambdaContainerPoolSize limits warm containers per function during tests.
	lambdaContainerPoolSize = 1
	// lambdaContainerIdleTimeout is how long an idle container is kept in the test pool.
	lambdaContainerIdleTimeout = 30 * time.Second
	// lambdaInvokeTimeout is the per-invocation deadline including container cold-start time.
	lambdaInvokeTimeout = 30 * time.Second
)

// echoHandlerGo is the source of a minimal Lambda runtime interface client written in Go.
// It loops forever: fetch the next invocation from the Runtime API and echo the payload back.
//
//nolint:gochecknoglobals // compile-time constant used only in buildEchoLambdaImage
var echoHandlerGo = `package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
)

func main() {
	api := os.Getenv("AWS_LAMBDA_RUNTIME_API")
	if api == "" {
		fmt.Fprintln(os.Stderr, "AWS_LAMBDA_RUNTIME_API is not set")
		os.Exit(1)
	}

	c := &http.Client{}

	for {
		resp, err := c.Get(fmt.Sprintf("http://%s/2018-06-01/runtime/invocation/next", api))
		if err != nil {
			continue
		}

		payload, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		requestID := resp.Header.Get("Lambda-Runtime-Aws-Request-Id")
		if requestID == "" {
			continue
		}

		_, _ = c.Post(
			fmt.Sprintf("http://%s/2018-06-01/runtime/invocation/%s/response", api, requestID),
			"application/json",
			bytes.NewReader(payload),
		)
	}
}
`

// echoHandlerDockerfile is a multi-stage Dockerfile that compiles the echo handler using the
// Go toolchain, then produces a minimal Alpine-based image with only the compiled binary.
//
//nolint:gochecknoglobals // compile-time constant used only in buildEchoLambdaImage
var echoHandlerDockerfile = `FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY handler.go .
RUN go mod init echohandler && go build -o handler .

FROM alpine:3.21
COPY --from=builder /app/handler /handler
ENTRYPOINT ["/handler"]
`

// TestLambdaIntegration_Invoke_DockerEchoContainer verifies a full Lambda function lifecycle
// against a real Docker daemon running on the test host.
//
// Test flow:
//  1. Build a minimal Go echo handler into a local Docker image.
//  2. Start an in-process Gopherstack Lambda service with a real portalloc and Docker client.
//  3. CreateFunction → GetFunction → Invoke (container echoes payload) → DeleteFunction.
//
// The Lambda container communicates with Gopherstack's Runtime API server through the Docker
// bridge gateway (172.17.0.1 by default on Linux), so no Docker-in-Docker setup is required.
//
// The test is automatically skipped in short mode (handled by TestMain) and when Docker is
// not available on the host.
func TestLambdaIntegration_Invoke_DockerEchoContainer(t *testing.T) {
	t.Parallel()
	// TestMain already calls os.Exit(0) in short mode, so no t.Skip needed here.
	ctx := t.Context()

	// Attempt to create a container runtime. Skip gracefully if Docker is unavailable.
	dc, err := container.NewRuntime(container.Config{
		Logger:      slog.Default(),
		PoolSize:    lambdaContainerPoolSize,
		IdleTimeout: lambdaContainerIdleTimeout,
	})
	if err != nil {
		t.Skipf("container runtime unavailable for Lambda integration test: %v", err)
	}

	t.Cleanup(func() { _ = dc.Close() })

	// Build (or rebuild) the echo Lambda image before starting the service.
	buildEchoLambdaImage(ctx, t)

	// Create a port allocator for Lambda Runtime API servers.
	alloc, err := portalloc.New(lambdaTestPortStart, lambdaTestPortEnd)
	require.NoError(t, err)

	// Wire up the Lambda service with real Docker and portalloc.
	settings := lambdapkg.DefaultSettings() // DockerHost defaults to 172.17.0.1 (Docker bridge)
	backend := lambdapkg.NewInMemoryBackend(dc, alloc, settings, "000000000000", "us-east-1")
	handler := lambdapkg.NewHandler(backend)
	handler.AccountID = "000000000000"
	handler.DefaultRegion = "us-east-1"

	// Start an in-process HTTP server with the Lambda handler registered.
	e := echo.New()
	e.Pre(logger.EchoMiddleware(slog.Default()))
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(handler))
	e.Use(service.NewServiceRouter(registry).RouteHandler())
	server := httptest.NewServer(e)
	t.Cleanup(server.Close)

	// --- Step 1: CreateFunction ---
	createBody, err := json.Marshal(map[string]any{
		"FunctionName": lambdaFunctionName,
		"PackageType":  "Image",
		"Code":         map[string]string{"ImageUri": lambdaEchoImage},
		"Role":         "arn:aws:iam::000000000000:role/lambda-test",
		"Timeout":      15, // 15s gives enough time for container cold start in CI
	})
	require.NoError(t, err)

	createResp, err := doLambdaRequest(ctx, http.MethodPost, server.URL+"/2015-03-31/functions",
		"application/json", bytes.NewReader(createBody))
	require.NoError(t, err)
	defer createResp.Body.Close()
	assert.Equal(t, http.StatusCreated, createResp.StatusCode, "CreateFunction should return 201")

	// --- Step 2: GetFunction ---
	getResp, err := doLambdaRequest(ctx, http.MethodGet,
		server.URL+"/2015-03-31/functions/"+lambdaFunctionName, "", nil)
	require.NoError(t, err)
	defer getResp.Body.Close()
	assert.Equal(t, http.StatusOK, getResp.StatusCode, "GetFunction should return 200")

	// --- Step 3: Invoke (synchronous RequestResponse) ---
	// The first Invoke triggers a cold start: Gopherstack allocates a runtime API port, starts
	// the HTTP server, and creates the container. The container connects back to
	// AWS_LAMBDA_RUNTIME_API (172.17.0.1:PORT) to fetch the queued invocation and echo it.
	invPayload := `{"message":"hello from Lambda integration test"}`

	invokeCtx, invokeCancel := context.WithTimeout(ctx, lambdaInvokeTimeout)
	defer invokeCancel()

	invokeResp, err := doLambdaRequest(invokeCtx, http.MethodPost,
		server.URL+"/2015-03-31/functions/"+lambdaFunctionName+"/invocations",
		"application/json", strings.NewReader(invPayload))
	require.NoError(t, err)
	defer invokeResp.Body.Close()

	require.Equal(t, http.StatusOK, invokeResp.StatusCode, "Invoke should return 200")

	got, err := io.ReadAll(invokeResp.Body)
	require.NoError(t, err)
	assert.JSONEq(t, invPayload, string(got), "echo handler must return the input payload unchanged")

	// --- Step 4: DeleteFunction ---
	delResp, err := doLambdaRequest(ctx, http.MethodDelete,
		server.URL+"/2015-03-31/functions/"+lambdaFunctionName, "", nil)
	require.NoError(t, err)
	defer delResp.Body.Close()
	assert.Equal(t, http.StatusNoContent, delResp.StatusCode, "DeleteFunction should return 204")
}

// doLambdaRequest sends an HTTP request and returns the response.
// contentType and body are optional; pass "" / nil to omit them.
func doLambdaRequest(
	ctx context.Context,
	method, url, contentType string,
	body io.Reader,
) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, err
	}

	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	return http.DefaultClient.Do(req) //nolint:wrapcheck // test helper
}

// buildEchoLambdaImage builds the gopherstack-lambda-echo-test image from an in-memory build
// context containing the echo handler source and a multi-stage Dockerfile.
//
// The image is tagged with [lambdaEchoImage] and removed when the test finishes.
// If the image already exists from a previous run, it is rebuilt to ensure freshness.
func buildEchoLambdaImage(ctx context.Context, t *testing.T) {
	t.Helper()

	// Pack handler.go + Dockerfile into an in-memory tar archive (the Docker build context).
	buildCtx, err := createInMemoryTar(map[string]string{
		"handler.go": echoHandlerGo,
		"Dockerfile": echoHandlerDockerfile,
	})
	require.NoError(t, err, "failed to create Docker build context")

	cli, err := dockerclient.NewClientWithOpts(dockerclient.FromEnv, dockerclient.WithAPIVersionNegotiation())
	require.NoError(t, err, "failed to create Docker SDK client")

	defer cli.Close()

	t.Logf("Building Lambda echo image %s (this may take a while on first run)...", lambdaEchoImage)

	buildResp, err := cli.ImageBuild(ctx, buildCtx, dockerapibuild.ImageBuildOptions{
		Dockerfile: "Dockerfile",
		Tags:       []string{lambdaEchoImage},
		Remove:     true,
	})
	require.NoError(t, err, "failed to start Docker image build")

	defer buildResp.Body.Close()

	// Drain build output — must be consumed or the build may stall.
	buildOutput, _ := io.ReadAll(buildResp.Body)
	t.Logf("Docker build output: %s", truncateOutput(string(buildOutput), buildOutputMaxBytes))

	// Check each JSON line from the build stream for error messages.
	// A failed pull (e.g. Docker Hub rate limit) does not cause ImageBuild to return an
	// error — the failure is embedded in the output stream instead.  Skip the test when
	// the image cannot be built due to an infrastructure issue so that CI is not broken
	// by transient external dependencies.
	for line := range strings.SplitSeq(string(buildOutput), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		var msg struct {
			Error string `json:"error"`
		}

		if jsonErr := json.Unmarshal([]byte(line), &msg); jsonErr == nil && msg.Error != "" {
			t.Skipf("Docker image build failed (infrastructure error — skipping test): %s", msg.Error)
		}
	}

	// Register cleanup: stop all containers descended from the test image, then remove the image.
	t.Cleanup(func() { removeLambdaTestArtifacts(lambdaEchoImage) })
}

// buildOutputMaxBytes is the maximum number of bytes shown from Docker build output in logs.
const buildOutputMaxBytes = 2048

// truncateOutput trims a string to at most maxBytes characters, appending "..." if truncated.
func truncateOutput(s string, maxBytes int) string {
	if len(s) <= maxBytes {
		return s
	}

	return s[:maxBytes] + "..."
}

// removeLambdaTestArtifacts forcibly removes all containers created from the test image
// and then deletes the image itself. Errors are intentionally ignored so test cleanup
// never fails a test that otherwise passed.
func removeLambdaTestArtifacts(imageTag string) {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cli, err := dockerclient.NewClientWithOpts(dockerclient.FromEnv, dockerclient.WithAPIVersionNegotiation())
	if err != nil {
		return
	}

	defer cli.Close()

	// Stop and remove any containers that were started from the test image.
	f := dockerapifilters.NewArgs(dockerapifilters.KeyValuePair{Key: "ancestor", Value: imageTag})
	containers, err := cli.ContainerList(cleanupCtx, dockerapicontainer.ListOptions{All: true, Filters: f})

	if err == nil {
		for _, c := range containers {
			_ = cli.ContainerRemove(cleanupCtx, c.ID, dockerapicontainer.RemoveOptions{Force: true})
		}
	}

	// Remove the image itself.
	_, _ = cli.ImageRemove(cleanupCtx, imageTag, dockerapiimage.RemoveOptions{Force: true})
}

// createInMemoryTar creates a tar archive in memory from the provided filename → content map.
func createInMemoryTar(files map[string]string) (io.Reader, error) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	for name, content := range files {
		data := []byte(content)
		hdr := &tar.Header{
			Name: name,
			Mode: 0600,
			Size: int64(len(data)),
		}

		if err := tw.WriteHeader(hdr); err != nil {
			return nil, fmt.Errorf("tar header %q: %w", name, err)
		}

		if _, err := tw.Write(data); err != nil {
			return nil, fmt.Errorf("tar write %q: %w", name, err)
		}
	}

	if err := tw.Close(); err != nil {
		return nil, fmt.Errorf("tar close: %w", err)
	}

	return &buf, nil
}
