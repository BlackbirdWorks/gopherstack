package terraform_test

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	acmsvc "github.com/aws/aws-sdk-go-v2/service/acm"
	cfnsvc "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cwsvc "github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	cwlogssvc "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	elasticachesvc "github.com/aws/aws-sdk-go-v2/service/elasticache"
	ebsvc "github.com/aws/aws-sdk-go-v2/service/eventbridge"
	firehosesvc "github.com/aws/aws-sdk-go-v2/service/firehose"
	iamsvc "github.com/aws/aws-sdk-go-v2/service/iam"
	kinesissvc "github.com/aws/aws-sdk-go-v2/service/kinesis"
	kmssvc "github.com/aws/aws-sdk-go-v2/service/kms"
	lambdasvc "github.com/aws/aws-sdk-go-v2/service/lambda"
	opensearchsvc "github.com/aws/aws-sdk-go-v2/service/opensearch"
	rdssvc "github.com/aws/aws-sdk-go-v2/service/rds"
	redshiftsvc "github.com/aws/aws-sdk-go-v2/service/redshift"
	route53svc "github.com/aws/aws-sdk-go-v2/service/route53"
	s3svc "github.com/aws/aws-sdk-go-v2/service/s3"
	secretssvc "github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	sessvc "github.com/aws/aws-sdk-go-v2/service/ses"
	sfnsvc "github.com/aws/aws-sdk-go-v2/service/sfn"
	snssvc "github.com/aws/aws-sdk-go-v2/service/sns"
	sqssvc "github.com/aws/aws-sdk-go-v2/service/sqs"
	ssmsvc "github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// tofuProviderCacheDir is a shared provider cache so both tests only download
// the hashicorp/aws provider once per test run.
//
//nolint:gochecknoglobals // shared provider cache path, read-only after init
var tofuProviderCacheDir = filepath.Join(os.TempDir(), "gopherstack-tofu-provider-cache")

// tofuBinaryOnce ensures tofu is only downloaded once per test run.
//
//nolint:gochecknoglobals // lazy-init singleton for the tofu binary path
var (
	tofuBinaryOnce sync.Once
	tofuBinaryPath string
	errTofuBinary  error
)

// Sentinel errors for the OpenTofu binary download helper.
var (
	errNoTofuVersions = errors.New("no stable versions found in OpenTofu API response")
	errTofuNotInZip   = errors.New("tofu binary not found in zip archive")
)

// providerBlock returns the OpenTofu required_providers + provider "aws" block
// pointing all service endpoints at addr (e.g. "http://localhost:32768").
func providerBlock(addr string) string {
	return fmt.Sprintf(`terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  required_version = ">= 1.0"
}

provider "aws" {
  region                      = "us-east-1"
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true
  s3_use_path_style           = true

  # Endpoints are listed alphabetically — keep them sorted when adding new ones.
  endpoints {
    acm            = %[1]q
    cloudformation = %[1]q
    cloudwatch     = %[1]q
    cloudwatchlogs = %[1]q
    dynamodb       = %[1]q
    elasticache    = %[1]q
    events         = %[1]q
    firehose       = %[1]q
    iam            = %[1]q
    kinesis        = %[1]q
    kms            = %[1]q
    lambda         = %[1]q
    opensearch     = %[1]q
    redshift       = %[1]q
    route53        = %[1]q
    s3             = %[1]q
    secretsmanager = %[1]q
    ses            = %[1]q
    sfn            = %[1]q
    sns            = %[1]q
    sqs            = %[1]q
    ssm            = %[1]q
    sts            = %[1]q
  }
}
`, addr)
}

// ensureTofuBinary returns the path to the tofu binary, downloading it
// automatically from the OpenTofu GitHub releases if it is not already present
// in PATH. The download happens at most once per test run (guarded by a
// [sync.Once]).
func ensureTofuBinary(t *testing.T) string {
	t.Helper()

	tofuBinaryOnce.Do(func() {
		// First, check if tofu is already in PATH.
		if path, err := exec.LookPath("tofu"); err == nil {
			tofuBinaryPath = path

			return
		}

		// Not found in PATH — download from OpenTofu releases.
		t.Log("tofu not found in PATH; downloading from OpenTofu releases...")

		tofuBinaryPath, errTofuBinary = downloadTofuBinary(t)
	})

	if errTofuBinary != nil {
		t.Fatalf("could not obtain tofu binary: %v", errTofuBinary)
	}

	return tofuBinaryPath
}

// downloadTofuBinary fetches the latest stable OpenTofu release for the current
// platform, extracts the binary to [os.TempDir], and returns its path.
func downloadTofuBinary(t *testing.T) (string, error) {
	t.Helper()

	ctx := context.Background()

	versionReq, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://get.opentofu.org/tofu/api.json", nil)
	if err != nil {
		return "", fmt.Errorf("creating OpenTofu version request: %w", err)
	}
	resp, err := http.DefaultClient.Do(versionReq)
	if err != nil {
		return "", fmt.Errorf("fetching OpenTofu version list: %w", err)
	}
	defer resp.Body.Close()

	var api struct {
		Versions []struct {
			ID string `json:"id"`
		} `json:"versions"`
	}
	err = json.NewDecoder(resp.Body).Decode(&api)
	if err != nil {
		return "", fmt.Errorf("decoding OpenTofu version list: %w", err)
	}

	// The API lists versions in most-recently-published order. Pick the first
	// entry that has no pre-release suffix (no hyphen), which is the latest
	// stable release.
	version := latestStableTofuVersion(api.Versions)
	if version == "" {
		return "", errNoTofuVersions
	}
	goos := runtime.GOOS
	goarch := runtime.GOARCH

	downloadURL := fmt.Sprintf(
		"https://github.com/opentofu/opentofu/releases/download/v%s/tofu_%s_%s_%s.zip",
		version, version, goos, goarch,
	)
	t.Logf("downloading OpenTofu %s from %s", version, downloadURL)

	zipReq, err := http.NewRequestWithContext(ctx, http.MethodGet, downloadURL, nil)
	if err != nil {
		return "", fmt.Errorf("creating OpenTofu download request: %w", err)
	}
	zipResp, err := http.DefaultClient.Do(zipReq)
	if err != nil {
		return "", fmt.Errorf("downloading OpenTofu zip: %w", err)
	}
	defer zipResp.Body.Close()

	zipData, err := io.ReadAll(zipResp.Body)
	if err != nil {
		return "", fmt.Errorf("reading OpenTofu zip: %w", err)
	}

	zr, err := zip.NewReader(bytes.NewReader(zipData), int64(len(zipData)))
	if err != nil {
		return "", fmt.Errorf("opening OpenTofu zip: %w", err)
	}

	binaryName := "tofu"
	if goos == "windows" {
		binaryName = "tofu.exe"
	}

	for _, f := range zr.File {
		if f.Name != binaryName {
			continue
		}

		return extractTofuBinary(f, binaryName)
	}

	return "", errTofuNotInZip
}

// extractTofuBinary writes a single [zip.File] to [os.TempDir] with executable
// permissions and returns its path.
func extractTofuBinary(f *zip.File, binaryName string) (string, error) {
	destPath := filepath.Join(os.TempDir(), binaryName)

	rc, err := f.Open()
	if err != nil {
		return "", fmt.Errorf("opening %s in zip: %w", binaryName, err)
	}
	defer rc.Close()

	out, err := os.OpenFile(destPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
	if err != nil {
		return "", fmt.Errorf("creating tofu binary: %w", err)
	}
	defer out.Close()

	if _, err = io.Copy(out, rc); err != nil {
		return "", fmt.Errorf("writing tofu binary: %w", err)
	}

	return destPath, nil
}

// latestStableTofuVersion returns the first stable (non-pre-release) version ID
// from the OpenTofu API versions list, or an empty string if none is found.
// Versions with a hyphen in the ID (e.g. "1.8.0-alpha1") are considered
// pre-release and are skipped.
func latestStableTofuVersion(versions []struct {
	ID string `json:"id"`
}) string {
	for _, v := range versions {
		if !strings.Contains(v.ID, "-") {
			return v.ID
		}
	}

	return ""
}

// applyTofu writes hcl to a main.tf in dir, runs tofu init and
// tofu apply -auto-approve, then registers a cleanup that destroys
// all created resources.
func applyTofu(t *testing.T, tofuBin, dir, hcl string) {
	t.Helper()

	cfgPath := dir + "/main.tf"
	require.NoError(t, os.WriteFile(cfgPath, []byte(hcl), 0o644))

	if err := os.MkdirAll(tofuProviderCacheDir, 0o755); err != nil {
		t.Logf("could not create provider cache dir: %v", err)
	}

	env := append(os.Environ(),
		"TF_IN_AUTOMATION=1",
		"TF_PLUGIN_CACHE_DIR="+tofuProviderCacheDir,
		"TF_PLUGIN_CACHE_MAY_BREAK_DEPENDENCY_LOCK_FILE=true",
	)

	run := func(failFatal bool, args ...string) bool {
		t.Helper()

		cmd := exec.Command(
			tofuBin,
			args...)
		cmd.Dir = dir
		cmd.Env = env

		out, err := cmd.CombinedOutput()
		t.Logf("tofu %v:\n%s", args, out)

		if err != nil {
			if failFatal {
				t.Fatalf("tofu %v failed: %v", args, err)
			}

			t.Logf("tofu %v failed (non-fatal): %v", args, err)

			return false
		}

		return true
	}

	run(true, "init", "-no-color")
	run(true, "apply", "-auto-approve", "-no-color")

	t.Cleanup(func() {
		run(false, "destroy", "-auto-approve", "-no-color")
	})
}

// TestTerraform_DynamoDB provisions a DynamoDB table via Terraform and verifies
// it exists and has the expected key schema.
func TestTerraform_DynamoDB(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tofuBin := ensureTofuBinary(t)
	ctx := context.Background()

	tableName := "tf-ddb-" + uuid.NewString()

	hcl := providerBlock(endpoint) + fmt.Sprintf(`
resource "aws_dynamodb_table" "this" {
  name         = %q
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"

  attribute {
    name = "pk"
    type = "S"
  }
}
`, tableName)

	applyTofu(t, tofuBin, t.TempDir(), hcl)

	// Verify the table exists and has the correct key schema.
	client := createDynamoDBClient(t)

	descOut, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	require.NoError(t, err, "DescribeTable should succeed after terraform apply")
	require.NotNil(t, descOut.Table)

	assert.Equal(t, tableName, aws.ToString(descOut.Table.TableName))
	require.Len(t, descOut.Table.KeySchema, 1)
	assert.Equal(t, "pk", aws.ToString(descOut.Table.KeySchema[0].AttributeName))
	assert.Equal(t, ddbtypes.KeyTypeHash, descOut.Table.KeySchema[0].KeyType)
}

// TestTerraform_S3AndSQS provisions an S3 bucket and an SQS queue via Terraform
// and verifies both are visible through their respective AWS APIs.
func TestTerraform_S3AndSQS(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tofuBin := ensureTofuBinary(t)
	ctx := context.Background()

	id := uuid.NewString()
	bucketName := "tf-s3-" + id
	queueName := "tf-sqs-" + id

	hcl := providerBlock(endpoint) + fmt.Sprintf(`
resource "aws_s3_bucket" "this" {
  bucket        = %q
  force_destroy = true
}

resource "aws_sqs_queue" "this" {
  name = %q
}
`, bucketName, queueName)

	applyTofu(t, tofuBin, t.TempDir(), hcl)

	// Verify S3 bucket exists.
	s3Client := createS3Client(t)

	_, err := s3Client.HeadBucket(ctx, &s3svc.HeadBucketInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err, "HeadBucket should succeed after terraform apply")

	// Verify SQS queue exists and the URL contains the queue name.
	sqsClient := createSQSClient(t)

	getURLOut, err := sqsClient.GetQueueUrl(ctx, &sqssvc.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	})
	require.NoError(t, err, "GetQueueUrl should succeed after terraform apply")
	assert.Contains(t, aws.ToString(getURLOut.QueueUrl), queueName)
}

// rdsProviderBlock returns an OpenTofu provider block that includes the rds endpoint.
func rdsProviderBlock(addr string) string {
	return fmt.Sprintf(`terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  required_version = ">= 1.0"
}

provider "aws" {
  region                      = "us-east-1"
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  endpoints {
    rds = %[1]q
    sts = %[1]q
  }
}
`, addr)
}

// TestTerraform_RDS provisions an RDS DB instance via Terraform,
// verifies it exists through the AWS SDK, then lets Terraform destroy it.
func TestTerraform_RDS(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tofuBin := ensureTofuBinary(t)
	ctx := context.Background()

	id := "tf-rds-" + uuid.NewString()[:8]

	hcl := rdsProviderBlock(endpoint) + fmt.Sprintf(`
resource "aws_db_instance" "this" {
  identifier         = %q
  engine             = "postgres"
  instance_class     = "db.t3.micro"
  username           = "admin"
  password           = "password123"
  db_name            = "testdb"
  allocated_storage  = 20
  skip_final_snapshot = true
}
`, id)

	applyTofu(t, tofuBin, t.TempDir(), hcl)

	// Verify the instance exists via RDS SDK.
	client := createRDSClient(t)

	descOut, err := client.DescribeDBInstances(ctx, &rdssvc.DescribeDBInstancesInput{
		DBInstanceIdentifier: aws.String(id),
	})
	require.NoError(t, err, "DescribeDBInstances should succeed after terraform apply")
	require.Len(t, descOut.DBInstances, 1)
	assert.Equal(t, id, aws.ToString(descOut.DBInstances[0].DBInstanceIdentifier))
	assert.Equal(t, "postgres", aws.ToString(descOut.DBInstances[0].Engine))
	assert.Equal(t, "available", aws.ToString(descOut.DBInstances[0].DBInstanceStatus))
}

// TestTerraform_Lambda provisions an aws_lambda_function via Terraform and
// verifies it exists through the AWS Lambda SDK.
func TestTerraform_Lambda(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tofuBin := ensureTofuBinary(t)
	ctx := context.Background()

	id := uuid.NewString()[:8]
	funcName := "tf-lambda-" + id

	// Create a minimal zip file containing a Python handler.
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	f, err := zw.Create("index.py")
	require.NoError(t, err)
	_, err = f.Write([]byte("def handler(event, context):\n    return {}\n"))
	require.NoError(t, err)
	require.NoError(t, zw.Close())

	dir := t.TempDir()
	zipPath := filepath.Join(dir, "function.zip")
	require.NoError(t, os.WriteFile(zipPath, buf.Bytes(), 0o644))

	hcl := providerBlock(endpoint) + fmt.Sprintf(`
resource "aws_iam_role" "lambda" {
  name = "tf-lambda-role-%s"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_lambda_function" "this" {
  filename         = %q
  function_name    = %q
  role             = aws_iam_role.lambda.arn
  handler          = "index.handler"
  runtime          = "python3.12"
  source_code_hash = filebase64sha256(%q)
}
`, id, zipPath, funcName, zipPath)

	applyTofu(t, tofuBin, dir, hcl)

	// Verify the function exists via Lambda SDK.
	client := createLambdaClient(t)

	out, err := client.GetFunction(ctx, &lambdasvc.GetFunctionInput{
		FunctionName: aws.String(funcName),
	})
	require.NoError(t, err, "GetFunction should succeed after terraform apply")
	require.NotNil(t, out.Configuration)
	assert.Equal(t, funcName, aws.ToString(out.Configuration.FunctionName))
}

// TestTerraform_IAM provisions an aws_iam_role, aws_iam_policy, and
// aws_iam_role_policy_attachment via Terraform and verifies the role exists.
func TestTerraform_IAM(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tofuBin := ensureTofuBinary(t)
	ctx := context.Background()

	id := uuid.NewString()[:8]
	roleName := "tf-role-" + id
	policyName := "tf-policy-" + id

	hcl := providerBlock(endpoint) + fmt.Sprintf(`
resource "aws_iam_role" "this" {
  name = %q
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_policy" "this" {
  name   = %q
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject"]
      Resource = "*"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "this" {
  role       = aws_iam_role.this.name
  policy_arn = aws_iam_policy.this.arn
}
`, roleName, policyName)

	applyTofu(t, tofuBin, t.TempDir(), hcl)

	// Verify the role exists via IAM SDK.
	client := createIAMClient(t)

	out, err := client.GetRole(ctx, &iamsvc.GetRoleInput{
		RoleName: aws.String(roleName),
	})
	require.NoError(t, err, "GetRole should succeed after terraform apply")
	require.NotNil(t, out.Role)
	assert.Equal(t, roleName, aws.ToString(out.Role.RoleName))
}

// TestTerraform_SNSSQSSubscription provisions an aws_sns_topic, aws_sqs_queue,
// and aws_sns_topic_subscription via Terraform and verifies the topic exists.
func TestTerraform_SNSSQSSubscription(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tofuBin := ensureTofuBinary(t)
	ctx := context.Background()

	id := uuid.NewString()[:8]
	topicName := "tf-topic-" + id
	queueName := "tf-queue-" + id

	hcl := providerBlock(endpoint) + fmt.Sprintf(`
resource "aws_sns_topic" "this" {
  name = %q
}

resource "aws_sqs_queue" "this" {
  name = %q
}

resource "aws_sns_topic_subscription" "this" {
  topic_arn = aws_sns_topic.this.arn
  protocol  = "sqs"
  endpoint  = "arn:aws:sqs:us-east-1:000000000000:${aws_sqs_queue.this.name}"
}
`, topicName, queueName)

	applyTofu(t, tofuBin, t.TempDir(), hcl)

	// Verify the SNS topic exists via SNS SDK.
	client := createSNSClient(t)

	out, err := client.GetTopicAttributes(ctx, &snssvc.GetTopicAttributesInput{
		TopicArn: aws.String("arn:aws:sns:us-east-1:000000000000:" + topicName),
	})
	require.NoError(t, err, "GetTopicAttributes should succeed after terraform apply")
	assert.NotEmpty(t, out.Attributes)
}

// TestTerraform_KMS provisions an aws_kms_key and aws_kms_alias via Terraform
// and verifies the key exists through the AWS KMS SDK.
func TestTerraform_KMS(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tofuBin := ensureTofuBinary(t)
	ctx := context.Background()

	id := uuid.NewString()[:8]
	aliasName := "alias/tf-test-" + id

	hcl := providerBlock(endpoint) + fmt.Sprintf(`
resource "aws_kms_key" "this" {
  description             = "tf-test-key-%s"
  deletion_window_in_days = 7
}

resource "aws_kms_alias" "this" {
  name          = %q
  target_key_id = aws_kms_key.this.key_id
}
`, id, aliasName)

	applyTofu(t, tofuBin, t.TempDir(), hcl)

	// Verify the key alias exists via KMS SDK.
	client := createKMSClient(t)

	out, err := client.DescribeKey(ctx, &kmssvc.DescribeKeyInput{
		KeyId: aws.String(aliasName),
	})
	require.NoError(t, err, "DescribeKey should succeed after terraform apply")
	require.NotNil(t, out.KeyMetadata)
}

// TestTerraform_SecretsManager provisions an aws_secretsmanager_secret and
// aws_secretsmanager_secret_version via Terraform and verifies the secret exists.
func TestTerraform_SecretsManager(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tofuBin := ensureTofuBinary(t)
	ctx := context.Background()

	id := uuid.NewString()[:8]
	secretName := "tf-secret-" + id

	hcl := providerBlock(endpoint) + fmt.Sprintf(`
resource "aws_secretsmanager_secret" "this" {
  name                    = %q
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "this" {
  secret_id     = aws_secretsmanager_secret.this.id
  secret_string = "my-test-secret-value"
}
`, secretName)

	applyTofu(t, tofuBin, t.TempDir(), hcl)

	// Verify the secret exists via SecretsManager SDK.
	client := createSecretsManagerClient(t)

	out, err := client.DescribeSecret(ctx, &secretssvc.DescribeSecretInput{
		SecretId: aws.String(secretName),
	})
	require.NoError(t, err, "DescribeSecret should succeed after terraform apply")
	assert.Equal(t, secretName, aws.ToString(out.Name))
}

// TestTerraform_SSMParameter provisions an aws_ssm_parameter via Terraform
// and verifies it exists through the AWS SSM SDK.
func TestTerraform_SSMParameter(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tofuBin := ensureTofuBinary(t)
	ctx := context.Background()

	id := uuid.NewString()[:8]
	paramName := "/tf/test/" + id

	hcl := providerBlock(endpoint) + fmt.Sprintf(`
resource "aws_ssm_parameter" "this" {
  name  = %q
  type  = "String"
  value = "test-value"
}
`, paramName)

	applyTofu(t, tofuBin, t.TempDir(), hcl)

	// Verify the parameter exists via SSM SDK.
	client := createSSMClient(t)

	out, err := client.GetParameter(ctx, &ssmsvc.GetParameterInput{
		Name: aws.String(paramName),
	})
	require.NoError(t, err, "GetParameter should succeed after terraform apply")
	require.NotNil(t, out.Parameter)
	assert.Equal(t, paramName, aws.ToString(out.Parameter.Name))
}

// TestTerraform_Route53 provisions an aws_route53_zone and aws_route53_record
// via Terraform and verifies the hosted zone exists through the AWS Route53 SDK.
func TestTerraform_Route53(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tofuBin := ensureTofuBinary(t)
	ctx := context.Background()

	id := uuid.NewString()[:8]
	zoneName := "tf-test-" + id + ".example.com"

	hcl := providerBlock(endpoint) + fmt.Sprintf(`
resource "aws_route53_zone" "this" {
  name = %q
}

resource "aws_route53_record" "this" {
  zone_id = aws_route53_zone.this.zone_id
  name    = "www.%s"
  type    = "A"
  ttl     = 300
  records = ["1.2.3.4"]
}
`, zoneName, zoneName)

	applyTofu(t, tofuBin, t.TempDir(), hcl)

	// Verify the hosted zone exists via Route53 SDK.
	client := createRoute53Client(t)

	out, err := client.ListHostedZones(ctx, &route53svc.ListHostedZonesInput{})
	require.NoError(t, err, "ListHostedZones should succeed after terraform apply")

	found := false
	for _, zone := range out.HostedZones {
		if aws.ToString(zone.Name) == zoneName+"." || aws.ToString(zone.Name) == zoneName {
			found = true

			break
		}
	}
	assert.True(t, found, "hosted zone %q should exist after terraform apply", zoneName)
}

// TestTerraform_CloudWatchLogGroup provisions an aws_cloudwatch_log_group via
// Terraform and verifies it exists through the AWS CloudWatchLogs SDK.
func TestTerraform_CloudWatchLogGroup(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tofuBin := ensureTofuBinary(t)
	ctx := context.Background()

	id := uuid.NewString()[:8]
	logGroupName := "/tf/test/" + id

	hcl := providerBlock(endpoint) + fmt.Sprintf(`
resource "aws_cloudwatch_log_group" "this" {
  name              = %q
  retention_in_days = 7
}
`, logGroupName)

	applyTofu(t, tofuBin, t.TempDir(), hcl)

	// Verify the log group exists via CloudWatchLogs SDK.
	client := createCloudWatchLogsClient(t)

	out, err := client.DescribeLogGroups(ctx, &cwlogssvc.DescribeLogGroupsInput{
		LogGroupNamePrefix: aws.String(logGroupName),
	})
	require.NoError(t, err, "DescribeLogGroups should succeed after terraform apply")

	found := false
	for _, lg := range out.LogGroups {
		if aws.ToString(lg.LogGroupName) == logGroupName {
			found = true

			break
		}
	}
	assert.True(t, found, "log group %q should exist after terraform apply", logGroupName)
}

// TestTerraform_SESEmailIdentity provisions an aws_ses_email_identity via
// Terraform and verifies it is listed through the AWS SES SDK.
func TestTerraform_SESEmailIdentity(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tofuBin := ensureTofuBinary(t)
	ctx := context.Background()

	id := uuid.NewString()[:8]
	email := "tf-test-" + id + "@example.com"

	hcl := providerBlock(endpoint) + fmt.Sprintf(`
resource "aws_ses_email_identity" "this" {
  email = %q
}
`, email)

	applyTofu(t, tofuBin, t.TempDir(), hcl)

	// Verify the email identity is listed via SES SDK.
	client := createSESClient(t)

	out, err := client.ListIdentities(ctx, &sessvc.ListIdentitiesInput{})
	require.NoError(t, err, "ListIdentities should succeed after terraform apply")

	found := slices.Contains(out.Identities, email)
	assert.True(t, found, "email identity %q should be listed after terraform apply", email)
}

// TestTerraform_DataSources provisions an S3 bucket and various data sources
// via Terraform and verifies the apply succeeds.
func TestTerraform_DataSources(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tofuBin := ensureTofuBinary(t)

	id := uuid.NewString()[:8]
	bucketName := "tf-ds-test-" + id

	hcl := providerBlock(endpoint) + fmt.Sprintf(`
data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

data "aws_iam_policy_document" "example" {
  statement {
    effect    = "Allow"
    actions   = ["s3:GetObject"]
    resources = ["*"]
  }
}

resource "aws_s3_bucket" "this" {
  bucket        = %q
  force_destroy = true
}

data "aws_s3_bucket" "this" {
  bucket     = aws_s3_bucket.this.bucket
  depends_on = [aws_s3_bucket.this]
}

output "account_id" {
  value = data.aws_caller_identity.current.account_id
}

output "region" {
  value = data.aws_region.current.name
}

output "policy_json" {
  value = data.aws_iam_policy_document.example.json
}
`, bucketName)

	applyTofu(t, tofuBin, t.TempDir(), hcl)
}

// TestTerraform_StepFunctions provisions an aws_sfn_state_machine via Terraform
// and verifies it exists through the AWS Step Functions SDK.
func TestTerraform_StepFunctions(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tofuBin := ensureTofuBinary(t)
	ctx := context.Background()

	id := uuid.NewString()[:8]
	smName := "tf-sfn-" + id

	hcl := providerBlock(endpoint) + fmt.Sprintf(`
resource "aws_iam_role" "sfn" {
  name = "tf-sfn-role-%s"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "states.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_sfn_state_machine" "this" {
  name     = %q
  role_arn = aws_iam_role.sfn.arn
  definition = jsonencode({
    Comment = "test"
    StartAt = "Pass"
    States  = {
      Pass = { Type = "Pass", End = true }
    }
  })
}
`, id, smName)

	applyTofu(t, tofuBin, t.TempDir(), hcl)

	client := createSFNClient(t)

	out, err := client.ListStateMachines(ctx, &sfnsvc.ListStateMachinesInput{})
	require.NoError(t, err, "ListStateMachines should succeed after terraform apply")

	found := false

	for _, sm := range out.StateMachines {
		if aws.ToString(sm.Name) == smName {
			found = true

			break
		}
	}

	assert.True(t, found, "state machine %q should exist after terraform apply", smName)
}

// TestTerraform_EventBridge provisions an aws_cloudwatch_event_bus and
// aws_cloudwatch_event_rule via Terraform and verifies both exist.
func TestTerraform_EventBridge(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tofuBin := ensureTofuBinary(t)
	ctx := context.Background()

	id := uuid.NewString()[:8]
	busName := "tf-bus-" + id
	ruleName := "tf-rule-" + id

	hcl := providerBlock(endpoint) + fmt.Sprintf(`
resource "aws_cloudwatch_event_bus" "this" {
  name = %q
}

resource "aws_cloudwatch_event_rule" "this" {
  name           = %q
  event_bus_name = aws_cloudwatch_event_bus.this.name
  schedule_expression = "rate(5 minutes)"
}
`, busName, ruleName)

	applyTofu(t, tofuBin, t.TempDir(), hcl)

	client := createEventBridgeClient(t)

	out, err := client.DescribeRule(ctx, &ebsvc.DescribeRuleInput{
		Name:         aws.String(ruleName),
		EventBusName: aws.String(busName),
	})
	require.NoError(t, err, "DescribeRule should succeed after terraform apply")
	assert.Equal(t, ruleName, aws.ToString(out.Name))
}

// TestTerraform_CloudWatchAlarm provisions an aws_cloudwatch_metric_alarm via
// Terraform and verifies it exists through the AWS CloudWatch SDK.
func TestTerraform_CloudWatchAlarm(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tofuBin := ensureTofuBinary(t)
	ctx := context.Background()

	id := uuid.NewString()[:8]
	alarmName := "tf-alarm-" + id

	hcl := providerBlock(endpoint) + fmt.Sprintf(`
resource "aws_cloudwatch_metric_alarm" "this" {
  alarm_name          = %q
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "CPUUtilization"
  namespace           = "AWS/EC2"
  period              = 60
  statistic           = "Average"
  threshold           = 80
}
`, alarmName)

	applyTofu(t, tofuBin, t.TempDir(), hcl)

	client := createCloudWatchClient(t)

	out, err := client.DescribeAlarms(ctx, &cwsvc.DescribeAlarmsInput{
		AlarmNames: []string{alarmName},
	})
	require.NoError(t, err, "DescribeAlarms should succeed after terraform apply")
	require.Len(t, out.MetricAlarms, 1)
	assert.Equal(t, alarmName, aws.ToString(out.MetricAlarms[0].AlarmName))
	assert.Equal(t, cwtypes.ComparisonOperatorGreaterThanThreshold, out.MetricAlarms[0].ComparisonOperator)
}

// TestTerraform_Kinesis provisions an aws_kinesis_stream via Terraform
// and verifies it exists through the AWS Kinesis SDK.
func TestTerraform_Kinesis(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tofuBin := ensureTofuBinary(t)
	ctx := context.Background()

	id := uuid.NewString()[:8]
	streamName := "tf-kinesis-" + id

	hcl := providerBlock(endpoint) + fmt.Sprintf(`
resource "aws_kinesis_stream" "this" {
  name        = %q
  shard_count = 1
}
`, streamName)

	applyTofu(t, tofuBin, t.TempDir(), hcl)

	client := createKinesisClient(t)

	out, err := client.DescribeStreamSummary(ctx, &kinesissvc.DescribeStreamSummaryInput{
		StreamName: aws.String(streamName),
	})
	require.NoError(t, err, "DescribeStreamSummary should succeed after terraform apply")
	require.NotNil(t, out.StreamDescriptionSummary)
	assert.Equal(t, streamName, aws.ToString(out.StreamDescriptionSummary.StreamName))
}

// TestTerraform_ACM provisions an aws_acm_certificate via Terraform
// and verifies it exists through the AWS ACM SDK.
func TestTerraform_ACM(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tofuBin := ensureTofuBinary(t)
	ctx := context.Background()

	id := uuid.NewString()[:8]
	domain := "tf-acm-" + id + ".example.com"

	hcl := providerBlock(endpoint) + fmt.Sprintf(`
resource "aws_acm_certificate" "this" {
  domain_name       = %q
  validation_method = "DNS"

  lifecycle {
    create_before_destroy = true
  }
}
`, domain)

	applyTofu(t, tofuBin, t.TempDir(), hcl)

	client := createACMClient(t)

	out, err := client.ListCertificates(ctx, &acmsvc.ListCertificatesInput{})
	require.NoError(t, err, "ListCertificates should succeed after terraform apply")

	found := false

	for _, cert := range out.CertificateSummaryList {
		if aws.ToString(cert.DomainName) == domain {
			found = true

			break
		}
	}

	assert.True(t, found, "certificate for %q should exist after terraform apply", domain)
}

// TestTerraform_CloudFormation provisions an aws_cloudformation_stack via Terraform
// and verifies it exists through the AWS CloudFormation SDK.
func TestTerraform_CloudFormation(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tofuBin := ensureTofuBinary(t)
	ctx := context.Background()

	id := uuid.NewString()[:8]
	stackName := "tf-cfn-" + id

	hcl := providerBlock(endpoint) + fmt.Sprintf(`
resource "aws_cloudformation_stack" "this" {
  name = %q

  timeouts {
    create = "2m"
    delete = "2m"
  }

  template_body = <<TEMPLATE
{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Description": "Gopherstack test stack",
  "Resources": {
    "WaitHandle": {
      "Type": "AWS::CloudFormation::WaitConditionHandle"
    }
  }
}
TEMPLATE
}
`, stackName)

	applyTofu(t, tofuBin, t.TempDir(), hcl)

	client := createCloudFormationClient(t)

	out, err := client.DescribeStacks(ctx, &cfnsvc.DescribeStacksInput{
		StackName: aws.String(stackName),
	})
	require.NoError(t, err, "DescribeStacks should succeed after terraform apply")
	require.Len(t, out.Stacks, 1)
	assert.Equal(t, stackName, aws.ToString(out.Stacks[0].StackName))
}

// TestTerraform_ElastiCache provisions an aws_elasticache_cluster via Terraform
// and verifies it exists through the AWS ElastiCache SDK.
func TestTerraform_ElastiCache(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tofuBin := ensureTofuBinary(t)
	ctx := context.Background()

	id := uuid.NewString()[:8]
	clusterID := "tf-ec-" + id

	hcl := providerBlock(endpoint) + fmt.Sprintf(`
resource "aws_elasticache_cluster" "this" {
  cluster_id           = %q
  engine               = "memcached"
  node_type            = "cache.t3.micro"
  num_cache_nodes      = 1
  parameter_group_name = "default.memcached1.6"

  timeouts {
    create = "2m"
    delete = "2m"
  }
}
`, clusterID)

	applyTofu(t, tofuBin, t.TempDir(), hcl)

	client := createElastiCacheClient(t)

	out, err := client.DescribeCacheClusters(ctx, &elasticachesvc.DescribeCacheClustersInput{
		CacheClusterId: aws.String(clusterID),
	})
	require.NoError(t, err, "DescribeCacheClusters should succeed after terraform apply")
	require.Len(t, out.CacheClusters, 1)
	assert.Equal(t, clusterID, aws.ToString(out.CacheClusters[0].CacheClusterId))
}

// TestTerraform_OpenSearch provisions an aws_opensearch_domain via Terraform
// and verifies it exists through the AWS OpenSearch SDK.
func TestTerraform_OpenSearch(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tofuBin := ensureTofuBinary(t)
	ctx := context.Background()

	id := uuid.NewString()[:8]
	domainName := "tf-os-" + id

	hcl := providerBlock(endpoint) + fmt.Sprintf(`
resource "aws_opensearch_domain" "this" {
  domain_name    = %q
  engine_version = "OpenSearch_2.3"

  timeouts {
    create = "2m"
    delete = "2m"
    update = "2m"
  }
}
`, domainName)

	applyTofu(t, tofuBin, t.TempDir(), hcl)

	client := createOpenSearchClient(t)

	out, err := client.DescribeDomain(ctx, &opensearchsvc.DescribeDomainInput{
		DomainName: aws.String(domainName),
	})
	require.NoError(t, err, "DescribeDomain should succeed after terraform apply")
	require.NotNil(t, out.DomainStatus)
	assert.Equal(t, domainName, aws.ToString(out.DomainStatus.DomainName))
}

// TestTerraform_Redshift provisions an aws_redshift_cluster via Terraform
// and verifies it exists through the AWS Redshift SDK.
func TestTerraform_Redshift(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tofuBin := ensureTofuBinary(t)
	ctx := context.Background()

	id := uuid.NewString()[:8]
	clusterID := "tf-rs-" + id

	hcl := providerBlock(endpoint) + fmt.Sprintf(`
resource "aws_redshift_cluster" "this" {
  cluster_identifier  = %q
  database_name       = "testdb"
  master_username     = "admin"
  master_password     = "Test1234!"
  node_type           = "dc2.large"
  cluster_type        = "single-node"
  skip_final_snapshot = true

  timeouts {
    create = "2m"
    delete = "2m"
    update = "2m"
  }
}
`, clusterID)

	applyTofu(t, tofuBin, t.TempDir(), hcl)

	client := createRedshiftClient(t)

	out, err := client.DescribeClusters(ctx, &redshiftsvc.DescribeClustersInput{
		ClusterIdentifier: aws.String(clusterID),
	})
	require.NoError(t, err, "DescribeClusters should succeed after terraform apply")
	require.Len(t, out.Clusters, 1)
	assert.Equal(t, clusterID, aws.ToString(out.Clusters[0].ClusterIdentifier))
}

// TestTerraform_Firehose provisions an aws_kinesis_firehose_delivery_stream via Terraform
// and verifies it exists through the AWS Firehose SDK.
func TestTerraform_Firehose(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tofuBin := ensureTofuBinary(t)
	ctx := context.Background()

	id := uuid.NewString()[:8]
	streamName := "tf-fh-" + id
	roleName := "tf-fh-role-" + id
	bucketName := "tf-fh-bucket-" + id

	hcl := providerBlock(endpoint) + fmt.Sprintf(`
resource "aws_iam_role" "firehose" {
  name = %q
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "firehose.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_s3_bucket" "firehose" {
  bucket = %q
}

resource "aws_kinesis_firehose_delivery_stream" "this" {
  name        = %q
  destination = "extended_s3"

  timeouts {
    create = "2m"
    delete = "2m"
    update = "2m"
  }

  extended_s3_configuration {
    role_arn   = aws_iam_role.firehose.arn
    bucket_arn = aws_s3_bucket.firehose.arn
  }
}
`, roleName, bucketName, streamName)

	applyTofu(t, tofuBin, t.TempDir(), hcl)

	client := createFirehoseClient(t)

	out, err := client.DescribeDeliveryStream(ctx, &firehosesvc.DescribeDeliveryStreamInput{
		DeliveryStreamName: aws.String(streamName),
	})
	require.NoError(t, err, "DescribeDeliveryStream should succeed after terraform apply")
	require.NotNil(t, out.DeliveryStreamDescription)
	assert.Equal(t, streamName, aws.ToString(out.DeliveryStreamDescription.DeliveryStreamName))
}
