package terraform_test

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	acmsvc "github.com/aws/aws-sdk-go-v2/service/acm"
	acmpcasvc "github.com/aws/aws-sdk-go-v2/service/acmpca"
	amplifysdkv2 "github.com/aws/aws-sdk-go-v2/service/amplify"
	apigwsvc "github.com/aws/aws-sdk-go-v2/service/apigateway"
	apigwv2svc "github.com/aws/aws-sdk-go-v2/service/apigatewayv2"
	appconfigsvc "github.com/aws/aws-sdk-go-v2/service/appconfig"
	appconfigdatasvc "github.com/aws/aws-sdk-go-v2/service/appconfigdata"
	applicationautoscalingsvc "github.com/aws/aws-sdk-go-v2/service/applicationautoscaling"
	appsyncsdkv2 "github.com/aws/aws-sdk-go-v2/service/appsync"
	athenasdkv2 "github.com/aws/aws-sdk-go-v2/service/athena"
	backupsvc "github.com/aws/aws-sdk-go-v2/service/backup"
	batchsvc "github.com/aws/aws-sdk-go-v2/service/batch"
	cfnsvc "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cwsvc "github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwlogssvc "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cognitoidentitysvc "github.com/aws/aws-sdk-go-v2/service/cognitoidentity"
	cognitoidpsvc "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	configsvc "github.com/aws/aws-sdk-go-v2/service/configservice"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ec2svc "github.com/aws/aws-sdk-go-v2/service/ec2"
	ecrsvc "github.com/aws/aws-sdk-go-v2/service/ecr"
	ecssvc "github.com/aws/aws-sdk-go-v2/service/ecs"
	elasticachesvc "github.com/aws/aws-sdk-go-v2/service/elasticache"
	ebsvc "github.com/aws/aws-sdk-go-v2/service/eventbridge"
	firehosesvc "github.com/aws/aws-sdk-go-v2/service/firehose"
	iamsvc "github.com/aws/aws-sdk-go-v2/service/iam"
	iotsvc "github.com/aws/aws-sdk-go-v2/service/iot"
	kinesissvc "github.com/aws/aws-sdk-go-v2/service/kinesis"
	kmssvc "github.com/aws/aws-sdk-go-v2/service/kms"
	lambdasvc "github.com/aws/aws-sdk-go-v2/service/lambda"
	opensearchsvc "github.com/aws/aws-sdk-go-v2/service/opensearch"
	rdssvc "github.com/aws/aws-sdk-go-v2/service/rds"
	redshiftsvc "github.com/aws/aws-sdk-go-v2/service/redshift"
	resourcegroupssvc "github.com/aws/aws-sdk-go-v2/service/resourcegroups"
	taggingsvc "github.com/aws/aws-sdk-go-v2/service/resourcegroupstaggingapi"
	route53svc "github.com/aws/aws-sdk-go-v2/service/route53"
	route53resolversvc "github.com/aws/aws-sdk-go-v2/service/route53resolver"
	s3svc "github.com/aws/aws-sdk-go-v2/service/s3"
	s3controlsvc "github.com/aws/aws-sdk-go-v2/service/s3control"
	schedulersvc "github.com/aws/aws-sdk-go-v2/service/scheduler"
	secretssvc "github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	sessvc "github.com/aws/aws-sdk-go-v2/service/ses"
	sfnsvc "github.com/aws/aws-sdk-go-v2/service/sfn"
	snssvc "github.com/aws/aws-sdk-go-v2/service/sns"
	sqssvc "github.com/aws/aws-sdk-go-v2/service/sqs"
	ssmsvc "github.com/aws/aws-sdk-go-v2/service/ssm"
	stssvc "github.com/aws/aws-sdk-go-v2/service/sts"
	supportsvc "github.com/aws/aws-sdk-go-v2/service/support"
	swfsvc "github.com/aws/aws-sdk-go-v2/service/swf"
	"github.com/docker/docker/api/types/build"
	"github.com/stretchr/testify/require"
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

	// Use the lightweight Dockerfile.test when a pre-built binary exists
	// (e.g. from CI or `go build`), otherwise fall back to the full
	// multi-stage Dockerfile that compiles from source.
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

	// Pre-download the tofu binary once in single-threaded setup so that no
	// parallel test pays the download cost.
	initTofuBinary(logger)

	// Warm the shared provider cache with a single tofu init so that parallel
	// tests don't all race to download the ~300 MB hashicorp/aws provider.
	warmProviderCache(logger)

	code := m.Run()

	// Clean up pre-initialized directories kept open for parallel tests.
	for _, d := range []string{preInitDirMain, preInitDirRDS} {
		if d != "" {
			os.RemoveAll(d)
		}
	}

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
		require.NoError(t, err, "unable to load SDK config")
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
		require.NoError(t, err, "unable to load SDK config")
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
		require.NoError(t, err, "unable to load SDK config")
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
		require.NoError(t, err, "unable to load SDK config")
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
		require.NoError(t, err, "unable to load SDK config")
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
		require.NoError(t, err, "unable to load SDK config")
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
		require.NoError(t, err, "unable to load SDK config")
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
		require.NoError(t, err, "unable to load SDK config")
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
		require.NoError(t, err, "unable to load SDK config")
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
		require.NoError(t, err, "unable to load SDK config")
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
		require.NoError(t, err, "unable to load SDK config")
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
		require.NoError(t, err, "unable to load SDK config")
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
		require.NoError(t, err, "unable to load SDK config")
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
		require.NoError(t, err, "unable to load SDK config")
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
		require.NoError(t, err, "unable to load SDK config")
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
		require.NoError(t, err, "unable to load SDK config")
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
		require.NoError(t, err, "unable to load SDK config")
	}

	return acmsvc.NewFromConfig(cfg, func(o *acmsvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createACMPCAClient returns an ACM PCA client pointed at the shared test container.
func createACMPCAClient(t *testing.T) *acmpcasvc.Client {
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

	return acmpcasvc.NewFromConfig(cfg, func(o *acmpcasvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createCloudFormationClient returns a CloudFormation client pointed at the shared test container.
func createCloudFormationClient(t *testing.T) *cfnsvc.Client {
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

	return cfnsvc.NewFromConfig(cfg, func(o *cfnsvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createElastiCacheClient returns an ElastiCache client pointed at the shared test container.
func createElastiCacheClient(t *testing.T) *elasticachesvc.Client {
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

	return elasticachesvc.NewFromConfig(cfg, func(o *elasticachesvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createOpenSearchClient returns an OpenSearch client pointed at the shared test container.
func createOpenSearchClient(t *testing.T) *opensearchsvc.Client {
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

	return opensearchsvc.NewFromConfig(cfg, func(o *opensearchsvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createRedshiftClient returns a Redshift client pointed at the shared test container.
func createRedshiftClient(t *testing.T) *redshiftsvc.Client {
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

	return redshiftsvc.NewFromConfig(cfg, func(o *redshiftsvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createFirehoseClient returns a Firehose client pointed at the shared test container.
func createFirehoseClient(t *testing.T) *firehosesvc.Client {
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

	return firehosesvc.NewFromConfig(cfg, func(o *firehosesvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// initTofuBinary eagerly resolves the tofu binary path (downloading it if
// necessary) during single-threaded TestMain setup. This ensures that no
// parallel test blocks waiting for the [sync.Once]-guarded download.
func initTofuBinary(logger *slog.Logger) {
	tofuBinaryOnce.Do(func() {
		if path, err := exec.LookPath("tofu"); err == nil {
			tofuBinaryPath = path

			return
		}

		logger.Info("tofu not found in PATH; downloading from OpenTofu releases...")

		tofuBinaryPath, errTofuBinary = downloadTofuBinary(logger)
	})

	if errTofuBinary != nil {
		logger.Error("could not obtain tofu binary", "error", errTofuBinary)
		os.Exit(1)
	}
}

// warmProviderCache runs a single tofu init to ensure the shared provider cache
// is populated before parallel tests start. This avoids 8+ concurrent tests all
// racing to download the ~300 MB hashicorp/aws provider simultaneously.
// It also keeps the initialized directories so applyTofu can hard-link the
// .terraform/ directory tree instead of re-running init (which serializes on
// the plugin-cache file lock).
func warmProviderCache(logger *slog.Logger) {
	if tofuBinaryPath == "" {
		logger.Warn("skipping provider cache warm-up: tofu binary not available")

		return
	}

	if mkdirErr := os.MkdirAll(tofuProviderCacheDir, 0o755); mkdirErr != nil {
		logger.Warn("skipping provider cache warm-up", "error", mkdirErr)

		return
	}

	// Warm all provider block variants used by tests so no test pays the
	// first-access initialization cost.
	preInitDirMain = warmWithHCL(tofuBinaryPath, tofuProviderCacheDir, providerBlock(endpoint), logger)
	preInitDirRDS = warmWithHCL(tofuBinaryPath, tofuProviderCacheDir, rdsProviderBlock(endpoint), logger)
}

// warmWithHCL runs `tofu init` in a temporary directory with the given HCL to
// populate the shared provider cache and produce a fully initialized .terraform/
// subtree (including .terraform/terraform.tfstate). It returns the directory path
// so callers can reuse the initialized .terraform/ subtree via hardLinkDir; the
// caller is responsible for cleanup (os.RemoveAll). Returns an empty string on
// failure.
func warmWithHCL(tofuBin, cacheDir, hcl string, logger *slog.Logger) string {
	dir, err := os.MkdirTemp("", "tofu-warmup-*")
	if err != nil {
		logger.Warn("skipping provider cache warm-up", "error", err)

		return ""
	}

	if writeErr := os.WriteFile(filepath.Join(dir, "main.tf"), []byte(hcl), 0o644); writeErr != nil {
		logger.Warn("skipping provider cache warm-up", "error", writeErr)
		if rmErr := os.RemoveAll(dir); rmErr != nil {
			logger.Warn("failed to remove warm-up temp dir", "dir", dir, "error", rmErr)
		}

		return ""
	}

	cmd := exec.Command(tofuBin, "init", "-no-color")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(),
		"TF_IN_AUTOMATION=1",
		"TF_PLUGIN_CACHE_DIR="+cacheDir,
		"TF_PLUGIN_CACHE_MAY_BREAK_DEPENDENCY_LOCK_FILE=true",
	)

	out, err := cmd.CombinedOutput()
	if err != nil {
		logger.Warn("provider cache warm-up failed", "error", err, "output", string(out))
		if rmErr := os.RemoveAll(dir); rmErr != nil {
			logger.Warn("failed to remove warm-up temp dir", "dir", dir, "error", rmErr)
		}

		return ""
	}

	logger.Info("provider cache warmed successfully")

	return dir
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

// createEC2Client returns an EC2 client pointed at the shared test container.
func createEC2Client(t *testing.T) *ec2svc.Client {
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

	return ec2svc.NewFromConfig(cfg, func(o *ec2svc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createAPIGatewayClient returns an API Gateway client pointed at the shared test container.
func createAPIGatewayClient(t *testing.T) *apigwsvc.Client {
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

	return apigwsvc.NewFromConfig(cfg, func(o *apigwsvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createAPIGatewayV2Client returns an API Gateway V2 client pointed at the shared test container.
func createAPIGatewayV2Client(t *testing.T) *apigwv2svc.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return apigwv2svc.NewFromConfig(cfg, func(o *apigwv2svc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createSchedulerClient returns a Scheduler client pointed at the shared test container.
func createSchedulerClient(t *testing.T) *schedulersvc.Client {
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

	return schedulersvc.NewFromConfig(cfg, func(o *schedulersvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createRoute53ResolverClient returns a Route53 Resolver client pointed at the shared test container.
func createRoute53ResolverClient(t *testing.T) *route53resolversvc.Client {
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

	return route53resolversvc.NewFromConfig(cfg, func(o *route53resolversvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createS3ControlClient returns an S3 Control client pointed at the shared test container.
func createS3ControlClient(t *testing.T) *s3controlsvc.Client {
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

	return s3controlsvc.NewFromConfig(cfg, func(o *s3controlsvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createAWSConfigClient returns an AWS Config client pointed at the shared test container.
func createAWSConfigClient(t *testing.T) *configsvc.Client {
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

	return configsvc.NewFromConfig(cfg, func(o *configsvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createResourceGroupsClient returns a Resource Groups client pointed at the shared test container.
func createResourceGroupsClient(t *testing.T) *resourcegroupssvc.Client {
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

	return resourcegroupssvc.NewFromConfig(cfg, func(o *resourcegroupssvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createResourceGroupsTaggingAPIClient returns a Resource Groups Tagging API client
// pointed at the shared test container.
func createResourceGroupsTaggingAPIClient(t *testing.T) *taggingsvc.Client {
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

	return taggingsvc.NewFromConfig(cfg, func(o *taggingsvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createSWFClient returns an SWF client pointed at the shared test container.
func createSWFClient(t *testing.T) *swfsvc.Client {
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

	return swfsvc.NewFromConfig(cfg, func(o *swfsvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createAppSyncClient returns an AppSync client pointed at the shared test container.
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

// createECRClient returns an ECR client pointed at the shared test container.
func createECRClient(t *testing.T) *ecrsvc.Client {
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

	return ecrsvc.NewFromConfig(cfg, func(o *ecrsvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createECSClient returns an ECS client pointed at the shared test container.
func createECSClient(t *testing.T) *ecssvc.Client {
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

	return ecssvc.NewFromConfig(cfg, func(o *ecssvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createCognitoIdentityClient returns a Cognito Identity client pointed at the shared test container.
func createCognitoIdentityClient(t *testing.T) *cognitoidentitysvc.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return cognitoidentitysvc.NewFromConfig(cfg, func(o *cognitoidentitysvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createCognitoIDPClient returns a Cognito IDP client pointed at the shared test container.
func createCognitoIDPClient(t *testing.T) *cognitoidpsvc.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return cognitoidpsvc.NewFromConfig(cfg, func(o *cognitoidpsvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createIoTClient returns an IoT client pointed at the shared test container.
func createIoTClient(t *testing.T) *iotsvc.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return iotsvc.NewFromConfig(cfg, func(o *iotsvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createSTSClient returns an STS client pointed at the shared test container.
func createSTSClient(t *testing.T) *stssvc.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return stssvc.NewFromConfig(cfg, func(o *stssvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createSupportClient returns a Support client pointed at the shared test container.
func createSupportClient(t *testing.T) *supportsvc.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return supportsvc.NewFromConfig(cfg, func(o *supportsvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createAmplifyClient returns an Amplify client pointed at the shared test container.
func createAmplifyClient(t *testing.T) *amplifysdkv2.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return amplifysdkv2.NewFromConfig(cfg, func(o *amplifysdkv2.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createAppConfigClient returns an AppConfig client pointed at the shared test container.
func createAppConfigClient(t *testing.T) *appconfigsvc.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return appconfigsvc.NewFromConfig(cfg, func(o *appconfigsvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createAppConfigDataClient returns an AppConfigData client pointed at the shared test container.
func createAppConfigDataClient(t *testing.T) *appconfigdatasvc.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return appconfigdatasvc.NewFromConfig(cfg, func(o *appconfigdatasvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createApplicationAutoscalingClient returns an Application Auto Scaling client pointed at the shared test container.
func createApplicationAutoscalingClient(t *testing.T) *applicationautoscalingsvc.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return applicationautoscalingsvc.NewFromConfig(cfg, func(o *applicationautoscalingsvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createAthenaClient returns an Athena client pointed at the shared test container.
func createAthenaClient(t *testing.T) *athenasdkv2.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return athenasdkv2.NewFromConfig(cfg, func(o *athenasdkv2.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createBackupClient returns a Backup client pointed at the shared test container.
func createBackupClient(t *testing.T) *backupsvc.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return backupsvc.NewFromConfig(cfg, func(o *backupsvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// createBatchClient returns a Batch client pointed at the shared test container.
func createBatchClient(t *testing.T) *batchsvc.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return batchsvc.NewFromConfig(cfg, func(o *batchsvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}
