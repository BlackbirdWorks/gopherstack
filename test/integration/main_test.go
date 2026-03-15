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

	"github.com/blackbirdworks/gopherstack/pkgs/dynamoattr"
	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"

	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	acmsdk "github.com/aws/aws-sdk-go-v2/service/acm"
	appsyncsdkv2 "github.com/aws/aws-sdk-go-v2/service/appsync"
	autoscalingsdk "github.com/aws/aws-sdk-go-v2/service/autoscaling"
	batchsdk "github.com/aws/aws-sdk-go-v2/service/batch"
	bedrocksdk "github.com/aws/aws-sdk-go-v2/service/bedrock"
	cloudformationsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cloudfrontsdk "github.com/aws/aws-sdk-go-v2/service/cloudfront"
	cloudtrailsdk "github.com/aws/aws-sdk-go-v2/service/cloudtrail"
	cloudwatchsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cloudwatchlogssdk "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	codepipelinesdk "github.com/aws/aws-sdk-go-v2/service/codepipeline"
	cognitoidentitysdk "github.com/aws/aws-sdk-go-v2/service/cognitoidentity"
	cognitoidpsdk "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	docdbsdk "github.com/aws/aws-sdk-go-v2/service/docdb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	ecrsdk "github.com/aws/aws-sdk-go-v2/service/ecr"
	ecssdk "github.com/aws/aws-sdk-go-v2/service/ecs"
	efssdk "github.com/aws/aws-sdk-go-v2/service/efs"
	ekssdk "github.com/aws/aws-sdk-go-v2/service/eks"
	elasticachesdk "github.com/aws/aws-sdk-go-v2/service/elasticache"
	emrsdk "github.com/aws/aws-sdk-go-v2/service/emr"
	eventbridgesdk "github.com/aws/aws-sdk-go-v2/service/eventbridge"
	iamsdk "github.com/aws/aws-sdk-go-v2/service/iam"
	kinesissdk "github.com/aws/aws-sdk-go-v2/service/kinesis"
	kmssdk "github.com/aws/aws-sdk-go-v2/service/kms"
	lambdaclientsdk "github.com/aws/aws-sdk-go-v2/service/lambda"
	rdssdk "github.com/aws/aws-sdk-go-v2/service/rds"
	route53sdk "github.com/aws/aws-sdk-go-v2/service/route53"
	route53resolversdk "github.com/aws/aws-sdk-go-v2/service/route53resolver"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3tablesclientsdk "github.com/aws/aws-sdk-go-v2/service/s3tables"
	schedulersdk "github.com/aws/aws-sdk-go-v2/service/scheduler"
	secretsmanagersdk "github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	sfnsdk "github.com/aws/aws-sdk-go-v2/service/sfn"
	snssdk "github.com/aws/aws-sdk-go-v2/service/sns"
	sqssdk "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	stssdk "github.com/aws/aws-sdk-go-v2/service/sts"
	swfsdk "github.com/aws/aws-sdk-go-v2/service/swf"
	wafv2sdk "github.com/aws/aws-sdk-go-v2/service/wafv2"
	"github.com/docker/docker/api/types/build"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// endpoint is the base URL for the running Gopherstack container.
// Both DynamoDB and S3 clients connect to this single endpoint.
// This is initialized by TestMain before running integration tests.
//
//nolint:gochecknoglobals // Set in TestMain for integration tests.
var endpoint string

// mqttEndpoint is the MQTT broker URL for the running Gopherstack container.
// This is initialized by TestMain before running integration tests.
//
//nolint:gochecknoglobals // Set in TestMain for integration tests.
var mqttEndpoint string

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

	dockerfile := "Dockerfile"
	if _, err := os.Stat("../../bin/gopherstack"); err == nil {
		dockerfile = "Dockerfile.test"
		logger.Info("using pre-built binary via Dockerfile.test")
	} else {
		logger.Info("no pre-built binary found, building from source via Dockerfile")
	}

	req := testcontainers.ContainerRequest{
		FromDockerfile: testcontainers.FromDockerfile{
			Context:       "../../",
			Dockerfile:    dockerfile,
			PrintBuildLog: true,
			BuildOptionsModifier: func(options *build.ImageBuildOptions) {
				options.NoCache = false
				options.PullParent = false
			},
		},
		AutoRemove:   true,
		ExposedPorts: []string{"8000/tcp", "1883/tcp"},
		WaitingFor: wait.ForAll(
			wait.ForHTTP("/").
				WithPort("8000/tcp").
				WithStatusCodeMatcher(func(_ int) bool { return true }).
				WithStartupTimeout(60*time.Second),
			wait.ForListeningPort("1883/tcp").
				WithStartupTimeout(60*time.Second),
		),
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

	mqttPort, err := container.MappedPort(ctx, "1883")
	if err != nil {
		logger.Warn("failed to get MQTT mapped port; IoT tests will be skipped", "error", err)
	} else {
		mqttEndpoint = fmt.Sprintf("tcp://localhost:%s", mqttPort.Port())
		logger.Info("MQTT broker running", "endpoint", mqttEndpoint)
	}

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
		require.NoError(t, err, "unable to load SDK config")
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
		require.NoError(t, err, "unable to load SDK config")
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
		require.NoError(t, err, "unable to load SDK config")
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
		require.NoError(t, err, "unable to load SDK config")
	}

	return ssm.NewFromConfig(cfg, func(o *ssm.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createSQSClient returns an SQS client pointed at the shared test container.
func createSQSClient(t *testing.T) *sqssdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	if err != nil {
		require.NoError(t, err, "unable to load SDK config")
	}

	return sqssdk.NewFromConfig(cfg, func(o *sqssdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createSNSClient returns an SNS client pointed at the shared test container.
func createSNSClient(t *testing.T) *snssdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	if err != nil {
		require.NoError(t, err, "unable to load SDK config")
	}

	return snssdk.NewFromConfig(cfg, func(o *snssdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createSTSClientWithCreds returns an STS client pointed at the shared test container
// using the provided assumed-role credentials.
func createSTSClientWithCreds(t *testing.T, accessKeyID, secretKey, sessionToken string) *stssdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(accessKeyID, secretKey, sessionToken),
		),
	)
	if err != nil {
		require.NoError(t, err, "unable to load SDK config")
	}

	return stssdk.NewFromConfig(cfg, func(o *stssdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createSTSClient returns an STS client pointed at the shared test container.
func createSTSClient(t *testing.T) *stssdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	if err != nil {
		require.NoError(t, err, "unable to load SDK config")
	}

	return stssdk.NewFromConfig(cfg, func(o *stssdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createKMSClient returns a KMS client pointed at the shared test container.
func createKMSClient(t *testing.T) *kmssdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	if err != nil {
		require.NoError(t, err, "unable to load SDK config")
	}

	return kmssdk.NewFromConfig(cfg, func(o *kmssdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createSecretsManagerClient returns a Secrets Manager client pointed at the shared test container.
func createSecretsManagerClient(t *testing.T) *secretsmanagersdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	if err != nil {
		require.NoError(t, err, "unable to load SDK config")
	}

	return secretsmanagersdk.NewFromConfig(cfg, func(o *secretsmanagersdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createIAMClient returns an IAM client pointed at the shared test container.
func createIAMClient(t *testing.T) *iamsdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	if err != nil {
		require.NoError(t, err, "unable to load SDK config")
	}

	return iamsdk.NewFromConfig(cfg, func(o *iamsdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createEventBridgeClient returns an EventBridge client pointed at the shared test container.
func createEventBridgeClient(t *testing.T) *eventbridgesdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	if err != nil {
		require.NoError(t, err, "unable to load SDK config")
	}

	return eventbridgesdk.NewFromConfig(cfg, func(o *eventbridgesdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createCloudWatchClient returns a CloudWatch client pointed at the shared test container.
func createCloudWatchClient(t *testing.T) *cloudwatchsdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	if err != nil {
		require.NoError(t, err, "unable to load SDK config")
	}

	return cloudwatchsdk.NewFromConfig(cfg, func(o *cloudwatchsdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createCloudWatchLogsClient returns a CloudWatch Logs client pointed at the shared test container.
func createCloudWatchLogsClient(t *testing.T) *cloudwatchlogssdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	if err != nil {
		require.NoError(t, err, "unable to load SDK config")
	}

	return cloudwatchlogssdk.NewFromConfig(cfg, func(o *cloudwatchlogssdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createStepFunctionsClient returns a Step Functions client pointed at the shared test container.
func createStepFunctionsClient(t *testing.T) *sfnsdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	if err != nil {
		require.NoError(t, err, "unable to load SDK config")
	}

	return sfnsdk.NewFromConfig(cfg, func(o *sfnsdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createCloudFormationClient returns a CloudFormation client pointed at the shared test container.
func createCloudFormationClient(t *testing.T) *cloudformationsdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	if err != nil {
		require.NoError(t, err, "unable to load SDK config")
	}

	return cloudformationsdk.NewFromConfig(cfg, func(o *cloudformationsdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createKinesisClient returns a Kinesis client pointed at the shared test container.
func createKinesisClient(t *testing.T) *kinesissdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	if err != nil {
		require.NoError(t, err, "unable to load SDK config")
	}

	return kinesissdk.NewFromConfig(cfg, func(o *kinesissdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createLambdaClient returns a Lambda client pointed at the shared test container.
func createLambdaClient(t *testing.T) *lambdaclientsdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	if err != nil {
		require.NoError(t, err, "unable to load SDK config")
	}

	return lambdaclientsdk.NewFromConfig(cfg, func(o *lambdaclientsdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createElastiCacheClient returns an ElastiCache client pointed at the shared test container.
func createElastiCacheClient(t *testing.T) *elasticachesdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	if err != nil {
		require.NoError(t, err, "unable to load SDK config")
	}

	return elasticachesdk.NewFromConfig(cfg, func(o *elasticachesdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createRDSClient returns an RDS client pointed at the shared test container.
func createRDSClient(t *testing.T) *rdssdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	if err != nil {
		require.NoError(t, err, "unable to load SDK config")
	}

	return rdssdk.NewFromConfig(cfg, func(o *rdssdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createSWFClient returns a SWF client pointed at the shared test container.
func createSWFClient(t *testing.T) *swfsdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	if err != nil {
		require.NoError(t, err, "unable to load SDK config")
	}

	return swfsdk.NewFromConfig(cfg, func(o *swfsdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createEC2Client returns an EC2 client pointed at the shared test container.
func createEC2Client(t *testing.T) *ec2sdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	if err != nil {
		require.NoError(t, err, "unable to load SDK config")
	}

	return ec2sdk.NewFromConfig(cfg, func(o *ec2sdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createECRClient returns an ECR client pointed at the shared test container.
func createECRClient(t *testing.T) *ecrsdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	if err != nil {
		require.NoError(t, err, "unable to load SDK config")
	}

	return ecrsdk.NewFromConfig(cfg, func(o *ecrsdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createSchedulerClient returns an EventBridge Scheduler client pointed at the shared test container.
func createSchedulerClient(t *testing.T) *schedulersdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	if err != nil {
		require.NoError(t, err, "unable to load SDK config")
	}

	return schedulersdk.NewFromConfig(cfg, func(o *schedulersdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createACMClient returns an ACM client pointed at the shared test container.
func createACMClient(t *testing.T) *acmsdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	if err != nil {
		require.NoError(t, err, "unable to load SDK config")
	}

	return acmsdk.NewFromConfig(cfg, func(o *acmsdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createRoute53Client returns a Route53 client pointed at the shared test container.
func createRoute53Client(t *testing.T) *route53sdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	if err != nil {
		require.NoError(t, err, "unable to load SDK config")
	}

	return route53sdk.NewFromConfig(cfg, func(o *route53sdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createRoute53ResolverClient returns a Route53Resolver client pointed at the shared test container.
func createRoute53ResolverClient(t *testing.T) *route53resolversdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	if err != nil {
		require.NoError(t, err, "unable to load SDK config")
	}

	return route53resolversdk.NewFromConfig(cfg, func(o *route53resolversdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createECSClient returns an ECS client pointed at the shared test container.
func createECSClient(t *testing.T) *ecssdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	if err != nil {
		require.NoError(t, err, "unable to load SDK config")
	}

	return ecssdk.NewFromConfig(cfg, func(o *ecssdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createWafv2Client returns a WAFv2 client pointed at the shared test container.
func createWafv2Client(t *testing.T) *wafv2sdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return wafv2sdk.NewFromConfig(cfg, func(o *wafv2sdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

func createS3TablesClient(t *testing.T) *s3tablesclientsdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return s3tablesclientsdk.NewFromConfig(cfg, func(o *s3tablesclientsdk.Options) {
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
	assert.Empty(t, cmp.Diff(expected, actual), "Item mismatch")
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

// createCognitoIDPClient returns a Cognito IDP client pointed at the shared test container.
func createCognitoIDPClient(t *testing.T) *cognitoidpsdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	if err != nil {
		require.NoError(t, err, "unable to load SDK config")
	}

	return cognitoidpsdk.NewFromConfig(cfg, func(o *cognitoidpsdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createAppSyncClient returns an AppSync management client pointed at the shared test container.
func createAppSyncClient(t *testing.T) *appsyncsdkv2.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	if err != nil {
		require.NoError(t, err, "unable to load SDK config")
	}

	return appsyncsdkv2.NewFromConfig(cfg, func(o *appsyncsdkv2.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createCognitoIdentityClient returns a Cognito Identity client pointed at the shared test container.
func createCognitoIdentityClient(t *testing.T) *cognitoidentitysdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	if err != nil {
		require.NoError(t, err, "unable to load SDK config")
	}

	return cognitoidentitysdk.NewFromConfig(cfg, func(o *cognitoidentitysdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createBedrockClient returns a Bedrock client pointed at the shared test container.
func createBedrockClient(t *testing.T) *bedrocksdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	if err != nil {
		require.NoError(t, err, "unable to load SDK config")
	}

	return bedrocksdk.NewFromConfig(cfg, func(o *bedrocksdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createAutoScalingClient returns an AutoScaling client pointed at the shared test container.
func createAutoScalingClient(t *testing.T) *autoscalingsdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return autoscalingsdk.NewFromConfig(cfg, func(o *autoscalingsdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createCloudFrontClient returns a CloudFront client pointed at the shared test container.
func createCloudFrontClient(t *testing.T) *cloudfrontsdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return cloudfrontsdk.NewFromConfig(cfg, func(o *cloudfrontsdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createCloudTrailClient returns a CloudTrail client pointed at the shared test container.
func createCloudTrailClient(t *testing.T) *cloudtrailsdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return cloudtrailsdk.NewFromConfig(cfg, func(o *cloudtrailsdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createEKSClient returns an EKS client pointed at the shared test container.
func createEKSClient(t *testing.T) *ekssdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return ekssdk.NewFromConfig(cfg, func(o *ekssdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createEFSClient returns an EFS client pointed at the shared test container.
func createEFSClient(t *testing.T) *efssdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return efssdk.NewFromConfig(cfg, func(o *efssdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createDocDBClient returns a DocumentDB client pointed at the shared test container.
func createDocDBClient(t *testing.T) *docdbsdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return docdbsdk.NewFromConfig(cfg, func(o *docdbsdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createBatchClient returns a Batch client pointed at the shared test container.
func createBatchClient(t *testing.T) *batchsdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return batchsdk.NewFromConfig(cfg, func(o *batchsdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createCodePipelineClient returns a CodePipeline client pointed at the shared test container.
func createCodePipelineClient(t *testing.T) *codepipelinesdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return codepipelinesdk.NewFromConfig(cfg, func(o *codepipelinesdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createEMRClient returns an EMR client pointed at the shared test container.
func createEMRClient(t *testing.T) *emrsdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return emrsdk.NewFromConfig(cfg, func(o *emrsdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}
