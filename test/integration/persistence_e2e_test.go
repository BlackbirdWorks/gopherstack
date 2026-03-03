package integration_test

import (
	"context"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	dockercontainer "github.com/docker/docker/api/types/container"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sqssdk "github.com/aws/aws-sdk-go-v2/service/sqs"
	ssmsdk "github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmtypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// startPersistenceContainer starts a Gopherstack container with persistence enabled,
// mounting dataDir to /data inside the container.
func startPersistenceContainer(t *testing.T, dataDir string) (testcontainers.Container, string) {
	t.Helper()

	ctx := t.Context()

	dockerfile := "Dockerfile"
	if _, err := os.Stat("../../bin/gopherstack"); err == nil {
		dockerfile = "Dockerfile.test"
	}

	req := testcontainers.ContainerRequest{
		FromDockerfile: testcontainers.FromDockerfile{
			Context:    "../../",
			Dockerfile: dockerfile,
		},
		Env: map[string]string{
			"PERSIST":              "true",
			"GOPHERSTACK_DATA_DIR": "/data",
		},
		HostConfigModifier: func(hc *dockercontainer.HostConfig) {
			hc.Binds = append(hc.Binds, dataDir+":/data")
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
	require.NoError(t, err, "failed to start persistence container")

	mappedPort, err := container.MappedPort(ctx, "8000")
	require.NoError(t, err)

	ep := fmt.Sprintf("http://localhost:%s", mappedPort.Port())

	return container, ep
}

// makeSQSClient creates an SQS client pointed at the given endpoint.
func makeSQSClient(t *testing.T, ep string) *sqssdk.Client {
	t.Helper()

	cfg, err := awsconfig.LoadDefaultConfig(
		t.Context(),
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return sqssdk.NewFromConfig(cfg, func(o *sqssdk.Options) {
		o.BaseEndpoint = aws.String(ep)
	})
}

// makeSSMClient creates an SSM client pointed at the given endpoint.
func makeSSMClient(t *testing.T, ep string) *ssmsdk.Client {
	t.Helper()

	cfg, err := awsconfig.LoadDefaultConfig(
		t.Context(),
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return ssmsdk.NewFromConfig(cfg, func(o *ssmsdk.Options) {
		o.BaseEndpoint = aws.String(ep)
	})
}

// TestPersistence_E2E_ContainerRestart verifies that state created in one container
// instance is restored after the container is stopped and restarted with the same
// data volume mounted.
//
//nolint:paralleltest // intentionally sequential — manages its own container lifecycle
func TestPersistence_E2E_ContainerRestart(t *testing.T) {
	dataDir := t.TempDir()
	ctx := context.Background()

	// --- Phase 1: start container, create resources ---
	container1, ep1 := startPersistenceContainer(t, dataDir)

	sqsClient1 := makeSQSClient(t, ep1)
	ssmClient1 := makeSSMClient(t, ep1)

	// Create an SQS queue.
	createQueueOut, err := sqsClient1.CreateQueue(ctx, &sqssdk.CreateQueueInput{
		QueueName: aws.String("persist-test-queue"),
	})
	require.NoError(t, err)
	require.NotNil(t, createQueueOut.QueueUrl)

	// Create an SSM parameter.
	_, err = ssmClient1.PutParameter(ctx, &ssmsdk.PutParameterInput{
		Name:  aws.String("/persist/test"),
		Value: aws.String("hello-persistence"),
		Type:  ssmtypes.ParameterTypeString,
	})
	require.NoError(t, err)

	// Wait long enough for the debounced save to fire (>500 ms).
	time.Sleep(1200 * time.Millisecond)

	// Stop the container gracefully (SIGTERM → SaveAll → flush snapshots).
	gracePeriod := 10 * time.Second
	require.NoError(t, container1.Stop(ctx, &gracePeriod))

	// --- Phase 2: restart container with same data dir, verify state ---
	container2, ep2 := startPersistenceContainer(t, dataDir)
	t.Cleanup(func() {
		_ = container2.Terminate(ctx)
	})

	sqsClient2 := makeSQSClient(t, ep2)
	ssmClient2 := makeSSMClient(t, ep2)

	// Verify the SQS queue survived.
	listOut, err := sqsClient2.ListQueues(ctx, &sqssdk.ListQueuesInput{})
	require.NoError(t, err)
	found := slices.Contains(listOut.QueueUrls, *createQueueOut.QueueUrl)

	assert.True(t, found, "SQS queue %q should survive container restart", *createQueueOut.QueueUrl)

	// Verify the SSM parameter survived.
	getOut, err := ssmClient2.GetParameter(ctx, &ssmsdk.GetParameterInput{
		Name: aws.String("/persist/test"),
	})
	require.NoError(t, err)
	assert.Equal(t, "hello-persistence", aws.ToString(getOut.Parameter.Value))
}
