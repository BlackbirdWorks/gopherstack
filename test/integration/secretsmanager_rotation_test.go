package integration_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	lambdaclientsdk "github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	secretsmanagersdk "github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	dockerapibuild "github.com/docker/docker/api/types/build"
	dockerclient "github.com/docker/docker/client"
	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	lambdapkg "github.com/blackbirdworks/gopherstack/services/lambda"
	smpkg "github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

const (
	// rotationLambdaPortStart is the start of the port range for the rotation Lambda runtime API.
	rotationLambdaPortStart = 19100
	// rotationLambdaPortEnd is the exclusive end of the rotation Lambda port range.
	rotationLambdaPortEnd = 19110
	// rotationLambdaImage is the Docker image tag for the rotation Lambda handler.
	rotationLambdaImage = "gopherstack-lambda-rotation-test:latest"
	// rotationFunctionName is the function name for the rotation Lambda.
	rotationFunctionName = "sm-rotation-lambda-integration"
	// rotationInvokeTimeout is the per-rotation call timeout (all 4 steps sequentially).
	rotationInvokeTimeout = 120 * time.Second
	// rotationContainerPoolSize limits warm containers per function.
	rotationContainerPoolSize = 1
	// rotationContainerIdleTimeout is how long an idle container is kept before reaping.
	rotationContainerIdleTimeout = 30 * time.Second
)

// rotationHandlerGo is the source of a minimal Lambda rotation handler written in Go.
//
// On the "createSecret" step it calls SecretsManager PutSecretValue via the endpoint injected
// through the SM_ENDPOINT environment variable.  All other steps return an empty success
// response so the rotation state machine (createSecret → setSecret → testSecret → finishSecret)
// completes without error.
//
//nolint:gochecknoglobals // compile-time constant used only in buildRotationLambdaImage
var rotationHandlerGo = `package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
)

type rotationEvent struct {
	SecretId           string ` + "`" + `json:"SecretId"` + "`" + `
	ClientRequestToken string ` + "`" + `json:"ClientRequestToken"` + "`" + `
	Step               string ` + "`" + `json:"Step"` + "`" + `
}

func putSecretValue(smEndpoint, secretID, token, value string) error {
	body := map[string]string{
		"SecretId":           secretID,
		"ClientRequestToken": token,
		"SecretString":       value,
	}

	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return err
	}

	req, err := http.NewRequest(http.MethodPost, smEndpoint+"/", bytes.NewReader(bodyBytes))
	if err != nil {
		return err
	}

	req.Header.Set("X-Amz-Target", "secretsmanager.PutSecretValue")
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)

		return fmt.Errorf("PutSecretValue returned %d: %s", resp.StatusCode, string(b))
	}

	return nil
}

func main() {
	api := os.Getenv("AWS_LAMBDA_RUNTIME_API")
	if api == "" {
		fmt.Fprintln(os.Stderr, "AWS_LAMBDA_RUNTIME_API not set")
		os.Exit(1)
	}

	smEndpoint := os.Getenv("SM_ENDPOINT")

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

		var event rotationEvent
		if jsonErr := json.Unmarshal(payload, &event); jsonErr != nil {
			errBody, _ := json.Marshal(map[string]string{"errorMessage": "bad event JSON"})
			_, _ = c.Post(
				fmt.Sprintf("http://%s/2018-06-01/runtime/invocation/%s/error", api, requestID),
				"application/json",
				bytes.NewReader(errBody),
			)

			continue
		}

		var handlerErr error

		if event.Step == "createSecret" && smEndpoint != "" {
			handlerErr = putSecretValue(smEndpoint, event.SecretId, event.ClientRequestToken, "rotated-by-lambda")
		}

		if handlerErr != nil {
			errBody, _ := json.Marshal(map[string]string{"errorMessage": handlerErr.Error()})
			_, _ = c.Post(
				fmt.Sprintf("http://%s/2018-06-01/runtime/invocation/%s/error", api, requestID),
				"application/json",
				bytes.NewReader(errBody),
			)
		} else {
			_, _ = c.Post(
				fmt.Sprintf("http://%s/2018-06-01/runtime/invocation/%s/response", api, requestID),
				"application/json",
				bytes.NewReader([]byte(` + "`" + `{}` + "`" + `)),
			)
		}
	}
}
`

// rotationHandlerDockerfile is the multi-stage Dockerfile for the rotation Lambda image.
//
//nolint:gochecknoglobals // compile-time constant used only in buildRotationLambdaImage
var rotationHandlerDockerfile = `FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY handler.go .
RUN go mod init rotationhandler && go build -o handler .

FROM alpine:3.21
COPY --from=builder /app/handler /handler
ENTRYPOINT ["/handler"]
`

// TestIntegration_SecretsManager_RotateSecret_WithLambda verifies the full rotation pipeline:
//
//  1. Build a minimal Go rotation Lambda Docker image.
//  2. Start an in-process server with Lambda and SecretsManager handlers wired together.
//  3. Create a secret with an initial value.
//  4. Register the rotation Lambda function, injecting the in-process SM endpoint.
//  5. Call RotateSecret with the rotation Lambda ARN.
//  6. Assert that the secret value was updated by the Lambda to "rotated-by-lambda".
//
// The Lambda container reaches the in-process test server via the Docker bridge gateway
// (172.17.0.1 on Linux / host.docker.internal on macOS/Windows); no Docker-in-Docker
// setup is required.
func TestIntegration_SecretsManager_RotateSecret_WithLambda(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	// Attempt to create a container runtime; skip gracefully if Docker is unavailable.
	dc, err := container.NewRuntime(container.Config{
		Logger:      logger.Load(ctx),
		PoolSize:    rotationContainerPoolSize,
		IdleTimeout: rotationContainerIdleTimeout,
	})
	if err != nil {
		t.Skipf("container runtime unavailable for rotation integration test: %v", err)
	}

	t.Cleanup(func() { _ = dc.Close() })

	// Build the rotation Lambda Docker image.
	buildRotationLambdaImage(ctx, t)

	// Create a port allocator for Lambda runtime API servers.
	alloc, err := portalloc.New(rotationLambdaPortStart, rotationLambdaPortEnd)
	require.NoError(t, err)

	// Wire up the Lambda backend with real Docker.
	settings := lambdapkg.DefaultSettings()
	lambdaBackend := lambdapkg.NewInMemoryBackend(dc, alloc, settings, "000000000000", "us-east-1")
	lambdaHandler := lambdapkg.NewHandler(lambdaBackend)
	lambdaHandler.AccountID = "000000000000"
	lambdaHandler.DefaultRegion = "us-east-1"

	// Wire up the SecretsManager backend, injecting the Lambda invoker.
	smBackend := smpkg.NewInMemoryBackend()
	smHandler := smpkg.NewHandler(smBackend)
	smHandler.DefaultRegion = "us-east-1"
	smHandler.SetLambdaInvoker(lambdaBackend)

	// Start a combined in-process HTTP server serving both services.
	e := echo.New()
	e.Pre(logger.EchoMiddleware(logger.Load(ctx)))
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(lambdaHandler))
	require.NoError(t, registry.Register(smHandler))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	server := httptest.NewServer(e)
	t.Cleanup(server.Close)

	// Derive the server port so Lambda containers can reach it via the Docker bridge.
	_, serverPort, err := net.SplitHostPort(server.Listener.Addr().String())
	require.NoError(t, err)

	smContainerEndpoint := "http://" + net.JoinHostPort(settings.DockerHost, serverPort)

	// Build AWS SDK clients that talk to the in-process server.
	sdkCfg := newSDKConfig(t)
	smClient := secretsmanagersdk.NewFromConfig(sdkCfg, func(o *secretsmanagersdk.Options) {
		o.BaseEndpoint = aws.String(server.URL)
	})
	lambdaClient := lambdaclientsdk.NewFromConfig(sdkCfg, func(o *lambdaclientsdk.Options) {
		o.BaseEndpoint = aws.String(server.URL)
	})

	// --- Step 1: Create a secret with an initial value. ---
	secretName := "rotation-test-" + uuid.NewString()

	_, err = smClient.CreateSecret(ctx, &secretsmanagersdk.CreateSecretInput{
		Name:         aws.String(secretName),
		SecretString: aws.String("initial-value"),
	})
	require.NoError(t, err, "CreateSecret should succeed")

	// --- Step 2: Register the rotation Lambda function. ---
	lambdaARN := fmt.Sprintf(
		"arn:aws:lambda:us-east-1:000000000000:function:%s",
		rotationFunctionName,
	)

	_, err = lambdaClient.CreateFunction(ctx, &lambdaclientsdk.CreateFunctionInput{
		FunctionName: aws.String(rotationFunctionName),
		PackageType:  lambdatypes.PackageTypeImage,
		Code:         &lambdatypes.FunctionCode{ImageUri: aws.String(rotationLambdaImage)},
		Role:         aws.String("arn:aws:iam::000000000000:role/lambda-rotation-test"),
		Timeout:      aws.Int32(30),
		Environment: &lambdatypes.Environment{
			Variables: map[string]string{
				"SM_ENDPOINT": smContainerEndpoint,
			},
		},
	})
	require.NoError(t, err, "CreateFunction should succeed")

	// --- Step 3: Trigger rotation via the Lambda ARN. ---
	rotateCtx, rotateCancel := context.WithTimeout(ctx, rotationInvokeTimeout)
	defer rotateCancel()

	rotateOut, err := smClient.RotateSecret(rotateCtx, &secretsmanagersdk.RotateSecretInput{
		SecretId:          aws.String(secretName),
		RotationLambdaARN: aws.String(lambdaARN),
	})
	require.NoError(t, err, "RotateSecret should succeed")
	assert.NotEmpty(t, aws.ToString(rotateOut.VersionId), "RotateSecret should return a VersionId")

	// --- Step 4: Verify the Lambda updated the secret value. ---
	getOut, err := smClient.GetSecretValue(ctx, &secretsmanagersdk.GetSecretValueInput{
		SecretId: aws.String(secretName),
	})
	require.NoError(t, err, "GetSecretValue should succeed after rotation")
	require.NotNil(t, getOut.SecretString, "SecretString should not be nil after rotation")
	assert.Equal(t, "rotated-by-lambda", aws.ToString(getOut.SecretString),
		"the rotation Lambda should have updated the secret to 'rotated-by-lambda'")
}

// newSDKConfig returns an AWS SDK config with static credentials pointing at no real endpoint.
// The actual endpoint is overridden per-client with BaseEndpoint.
func newSDKConfig(t *testing.T) aws.Config {
	t.Helper()

	cfg, err := awsconfig.LoadDefaultConfig(
		t.Context(),
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return cfg
}

// buildRotationLambdaImage builds the rotation Lambda Docker image from an in-memory tar.
func buildRotationLambdaImage(ctx context.Context, t *testing.T) {
	t.Helper()

	buildCtx, err := createInMemoryTar(map[string]string{
		"handler.go": rotationHandlerGo,
		"Dockerfile": rotationHandlerDockerfile,
	})
	require.NoError(t, err, "failed to create rotation Lambda Docker build context")

	cli, err := dockerclient.NewClientWithOpts(dockerclient.FromEnv, dockerclient.WithAPIVersionNegotiation())
	require.NoError(t, err, "failed to create Docker SDK client")

	defer cli.Close()

	t.Logf("Building rotation Lambda image %s (may take a while on first run)...", rotationLambdaImage)

	buildResp, err := cli.ImageBuild(ctx, buildCtx, dockerapibuild.ImageBuildOptions{
		Dockerfile: "Dockerfile",
		Tags:       []string{rotationLambdaImage},
		Remove:     true,
	})
	require.NoError(t, err, "failed to start rotation Lambda image build")

	defer buildResp.Body.Close()

	buildOutput, _ := io.ReadAll(buildResp.Body)
	t.Logf("Docker build output: %s", truncateOutput(string(buildOutput), buildOutputMaxBytes))

	t.Cleanup(func() { removeLambdaTestArtifacts(rotationLambdaImage) })
}
