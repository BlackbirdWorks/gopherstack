package integration_test

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/docker/docker/api/types/build"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// endpoint is the base URL for the running Gopherstack container.
// Both DynamoDB and S3 clients connect to this single endpoint.
//
//nolint:gochecknoglobals // TestMain initializes the shared endpoint for clients.
var endpoint string

func TestMain(m *testing.M) {
	flag.Parse()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	if testing.Short() {
		logger.Info("skipping integration tests in short mode")
		os.Exit(0)
	}

	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		FromDockerfile: testcontainers.FromDockerfile{
			Context:       "../../",
			Dockerfile:    "Dockerfile",
			PrintBuildLog: true,
			BuildOptionsModifier: func(options *build.ImageBuildOptions) {
				options.NoCache = true
				options.PullParent = true
			},
		},
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

// createDynamoDBClient returns a DynamoDB client pointed at the shared test container.
func createDynamoDBClient(t *testing.T) *dynamodb.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("unable to load SDK config: %v", err)
	}

	return dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createS3Client returns an S3 client pointed at the shared test container.
func createS3Client(t *testing.T) *s3.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("unable to load SDK config: %v", err)
	}

	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String(endpoint)
	})
}
