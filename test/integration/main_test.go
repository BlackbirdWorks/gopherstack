package integration_test

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/dynamodb/models"
	"github.com/blackbirdworks/gopherstack/pkgs/dynamoattr"

	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/docker/docker/api/types/build"
	"github.com/google/go-cmp/cmp"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// endpoint is the base URL for the running Gopherstack container.
// Both DynamoDB and S3 clients connect to this single endpoint.
// This is initialized by TestMain before running integration tests.
//
//nolint:gochecknoglobals // Set in TestMain for integration tests.
var endpoint string

// sharedContainer holds a reference to the container for cleanup and log dumping on test failures.
// This is initialized by TestMain before running integration tests.
//
//nolint:gochecknoglobals // Set in TestMain for integration tests.
var sharedContainer testcontainers.Container

// ErrDockerPanic is returned when the Docker availability check panics.
var ErrDockerPanic = errors.New("docker check panicked")

func TestMain(m *testing.M) {
	flag.Parse()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	if testing.Short() {
		logger.Info("skipping integration tests in short mode")
		os.Exit(0)
	}

	if err := checkDocker(); err != nil {
		logger.Error("integration tests require docker", "error", err)
		os.Exit(1)
	}

	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		FromDockerfile: testcontainers.FromDockerfile{
			Context:       "../../",
			Dockerfile:    "Dockerfile",
			PrintBuildLog: true,
			BuildOptionsModifier: func(options *build.ImageBuildOptions) {
				options.NoCache = false
				options.PullParent = false
			},
		},
		AutoRemove:   true,
		ExposedPorts: []string{"8000/tcp"},
		WaitingFor: wait.ForHTTP("/").
			WithStatusCodeMatcher(func(_ int) bool { return true }).
			WithStartupTimeout(60 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		logger.Error("failed to start container", "error", err)

		os.Exit(1)
	}

	sharedContainer = container

	mappedPort, err := container.MappedPort(ctx, "8000")
	if err != nil {
		logger.Error("failed to get mapped port", "error", err)
		os.Exit(1)
	}

	endpoint = fmt.Sprintf("http://localhost:%s", mappedPort.Port())
	logger.Info("Gopherstack running", "endpoint", endpoint)

	code := m.Run()

	if tErr := container.Terminate(ctx); tErr != nil {
		logger.Error("failed to terminate container", "error", tErr)
	}

	os.Exit(code)
}

// checkDocker safely checks if the Docker daemon is available by attempting
// to create a provider and recovering from any potential panics.
func checkDocker() (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %v", ErrDockerPanic, r)
		}
	}()

	_, err = testcontainers.NewDockerProvider()

	return err
}

// createDynamoDBClient returns a DynamoDB client pointed at the shared test container.

func createDynamoDBClient(t *testing.T) *dynamodb.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	if err != nil {
		t.Fatalf("unable to load SDK config: %v", err)
	}

	return dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createDynamoDBStreamsClient returns a DynamoDB Streams client pointed at the shared test container.
func createDynamoDBStreamsClient(t *testing.T) *dynamodbstreams.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	if err != nil {
		t.Fatalf("unable to load SDK config: %v", err)
	}

	return dynamodbstreams.NewFromConfig(cfg, func(o *dynamodbstreams.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createS3Client returns an S3 client pointed at the shared test container.
func createS3Client(t *testing.T) *s3.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	if err != nil {
		t.Fatalf("unable to load SDK config: %v", err)
	}

	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createSSMClient returns an SSM client pointed at the shared test container.
func createSSMClient(t *testing.T) *ssm.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	if err != nil {
		t.Fatalf("unable to load SDK config: %v", err)
	}

	return ssm.NewFromConfig(cfg, func(o *ssm.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// dumpContainerLogsOnFailure dumps the container logs to stdout if the test failed.
// Call this with t.Cleanup to automatically dump logs on test failure.
func dumpContainerLogsOnFailure(t *testing.T) {
	t.Helper()

	t.Cleanup(func() {
		if !t.Failed() {
			return
		}

		if sharedContainer == nil {
			t.Log("Cannot dump logs: container reference not available")

			return
		}

		ctx := context.Background()
		t.Logf("\n========== CONTAINER LOGS FOR FAILED TEST: %s ==========\n", t.Name())

		logs, err := sharedContainer.Logs(ctx)
		if err != nil {
			t.Logf("Failed to retrieve container logs: %v", err)

			return
		}
		defer logs.Close()

		logBytes, err := io.ReadAll(logs)
		if err != nil {
			t.Logf("Failed to read container logs: %v", err)

			return
		}

		t.Logf("%s", string(logBytes))
		t.Log("\n========== END CONTAINER LOGS ==========\n")
	})
}

// AssertItem performs a deep comparison between a DynamoDB item and an expected map.
// It automatically unwraps the SDK's internal representation for easier testing.
func AssertItem(t *testing.T, item map[string]types.AttributeValue, expected map[string]any) {
	t.Helper()

	actual := unwrapItem(models.FromSDKItem(item))
	if diff := cmp.Diff(expected, actual); diff != "" {
		t.Errorf("Item mismatch (-want +got):\n%s", diff)
	}
}

func unwrapItem(item map[string]any) map[string]any {
	res := make(map[string]any)
	for k, v := range item {
		res[k] = unwrapValue(v)
	}

	return res
}

func unwrapValue(v any) any {
	unwrapped := dynamoattr.UnwrapAttributeValue(v)

	switch val := unwrapped.(type) {
	case map[string]any:
		res := make(map[string]any)
		for mk, mv := range val {
			res[mk] = unwrapValue(mv)
		}

		return res
	case []any:
		res := make([]any, len(val))
		for i, iv := range val {
			res[i] = unwrapValue(iv)
		}

		return res
	default:
		return val
	}
}
