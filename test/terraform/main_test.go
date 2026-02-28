package terraform_test

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	acmsvc "github.com/aws/aws-sdk-go-v2/service/acm"
	cwsvc "github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwlogssvc "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ebsvc "github.com/aws/aws-sdk-go-v2/service/eventbridge"
	iamsvc "github.com/aws/aws-sdk-go-v2/service/iam"
	kinesissvc "github.com/aws/aws-sdk-go-v2/service/kinesis"
	kmssvc "github.com/aws/aws-sdk-go-v2/service/kms"
	lambdasvc "github.com/aws/aws-sdk-go-v2/service/lambda"
	rdssvc "github.com/aws/aws-sdk-go-v2/service/rds"
	route53svc "github.com/aws/aws-sdk-go-v2/service/route53"
	s3svc "github.com/aws/aws-sdk-go-v2/service/s3"
	secretssvc "github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	sessvc "github.com/aws/aws-sdk-go-v2/service/ses"
	sfnsvc "github.com/aws/aws-sdk-go-v2/service/sfn"
	snssvc "github.com/aws/aws-sdk-go-v2/service/sns"
	sqssvc "github.com/aws/aws-sdk-go-v2/service/sqs"
	ssmsvc "github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/docker/docker/api/types/build"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// endpoint is the base URL for the running Gopherstack container.
//
//nolint:gochecknoglobals // Set in TestMain for terraform tests.
var endpoint string

// sharedContainer holds a reference to the container for cleanup and log dumping on test failures.
//
//nolint:gochecknoglobals // Set in TestMain for terraform tests.
var sharedContainer testcontainers.Container

// ErrDockerPanic is returned when the Docker availability check panics.
var ErrDockerPanic = errors.New("docker check panicked")

func TestMain(m *testing.M) {
	flag.Parse()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	if testing.Short() {
		logger.Info("skipping terraform tests in short mode")
		os.Exit(0)
	}

	if err := checkDocker(); err != nil {
		logger.Error("terraform tests require docker", "error", err)
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

// createS3Client returns an S3 client pointed at the shared test container.
func createS3Client(t *testing.T) *s3svc.Client {
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

	return s3svc.NewFromConfig(cfg, func(o *s3svc.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createSQSClient returns an SQS client pointed at the shared test container.
func createSQSClient(t *testing.T) *sqssvc.Client {
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

	return sqssvc.NewFromConfig(cfg, func(o *sqssvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createRDSClient returns an RDS client pointed at the shared test container.
func createRDSClient(t *testing.T) *rdssvc.Client {
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

	return rdssvc.NewFromConfig(cfg, func(o *rdssvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createIAMClient returns an IAM client pointed at the shared test container.
func createIAMClient(t *testing.T) *iamsvc.Client {
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

	return iamsvc.NewFromConfig(cfg, func(o *iamsvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createKMSClient returns a KMS client pointed at the shared test container.
func createKMSClient(t *testing.T) *kmssvc.Client {
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

	return kmssvc.NewFromConfig(cfg, func(o *kmssvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createSNSClient returns an SNS client pointed at the shared test container.
func createSNSClient(t *testing.T) *snssvc.Client {
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

	return snssvc.NewFromConfig(cfg, func(o *snssvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createSecretsManagerClient returns a SecretsManager client pointed at the shared test container.
func createSecretsManagerClient(t *testing.T) *secretssvc.Client {
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

	return secretssvc.NewFromConfig(cfg, func(o *secretssvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createSSMClient returns an SSM client pointed at the shared test container.
func createSSMClient(t *testing.T) *ssmsvc.Client {
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

	return ssmsvc.NewFromConfig(cfg, func(o *ssmsvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createCloudWatchLogsClient returns a CloudWatchLogs client pointed at the shared test container.
func createCloudWatchLogsClient(t *testing.T) *cwlogssvc.Client {
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

	return cwlogssvc.NewFromConfig(cfg, func(o *cwlogssvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createRoute53Client returns a Route53 client pointed at the shared test container.
func createRoute53Client(t *testing.T) *route53svc.Client {
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

	return route53svc.NewFromConfig(cfg, func(o *route53svc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createLambdaClient returns a Lambda client pointed at the shared test container.
func createLambdaClient(t *testing.T) *lambdasvc.Client {
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

	return lambdasvc.NewFromConfig(cfg, func(o *lambdasvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createSESClient returns an SES client pointed at the shared test container.
func createSESClient(t *testing.T) *sessvc.Client {
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

	return sessvc.NewFromConfig(cfg, func(o *sessvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createSFNClient returns a StepFunctions client pointed at the shared test container.
func createSFNClient(t *testing.T) *sfnsvc.Client {
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

	return sfnsvc.NewFromConfig(cfg, func(o *sfnsvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createEventBridgeClient returns an EventBridge client pointed at the shared test container.
func createEventBridgeClient(t *testing.T) *ebsvc.Client {
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

	return ebsvc.NewFromConfig(cfg, func(o *ebsvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createCloudWatchClient returns a CloudWatch client pointed at the shared test container.
func createCloudWatchClient(t *testing.T) *cwsvc.Client {
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

	return cwsvc.NewFromConfig(cfg, func(o *cwsvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createKinesisClient returns a Kinesis client pointed at the shared test container.
func createKinesisClient(t *testing.T) *kinesissvc.Client {
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

	return kinesissvc.NewFromConfig(cfg, func(o *kinesissvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createACMClient returns an ACM client pointed at the shared test container.
func createACMClient(t *testing.T) *acmsvc.Client {
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

	return acmsvc.NewFromConfig(cfg, func(o *acmsvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// dumpContainerLogsOnFailure dumps the container logs to stdout if the test failed.
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
