package integration_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	s3svc "github.com/aws/aws-sdk-go-v2/service/s3"
	sqssvc "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// tfProviderCacheDir is a shared provider cache so both tests only download
// the hashicorp/aws provider once per test run.
//
//nolint:gochecknoglobals // shared provider cache path, read-only after init
var tfProviderCacheDir = filepath.Join(os.TempDir(), "gopherstack-tf-provider-cache")

// providerBlock returns the Terraform required_providers + provider "aws" block
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

  endpoints {
    dynamodb       = %[1]q
    s3             = %[1]q
    sqs            = %[1]q
    sns            = %[1]q
    ssm            = %[1]q
    kms            = %[1]q
    secretsmanager = %[1]q
    sts            = %[1]q
  }
}
`, addr)
}

// findTerraformOrSkip returns the path to the terraform binary or skips the test.
func findTerraformOrSkip(t *testing.T) string {
	t.Helper()

	tfBin, err := exec.LookPath("terraform")
	if err != nil {
		t.Skip("terraform binary not found in PATH; skipping Terraform integration test")
	}

	return tfBin
}

// applyTerraform writes hcl to a main.tf in dir, runs terraform init and
// terraform apply -auto-approve, then registers a cleanup that destroys
// all created resources.
func applyTerraform(t *testing.T, tfBin, dir, hcl string) {
	t.Helper()

	cfgPath := dir + "/main.tf"
	require.NoError(t, os.WriteFile(cfgPath, []byte(hcl), 0o644))

	if err := os.MkdirAll(tfProviderCacheDir, 0o755); err != nil {
		t.Logf("could not create provider cache dir: %v", err)
	}

	env := append(os.Environ(),
		"TF_IN_AUTOMATION=1",
		"TF_PLUGIN_CACHE_DIR="+tfProviderCacheDir,
		"TF_PLUGIN_CACHE_MAY_BREAK_DEPENDENCY_LOCK_FILE=true",
	)

	run := func(failFatal bool, args ...string) bool {
		t.Helper()

		cmd := exec.Command(
			tfBin,
			args...)
		cmd.Dir = dir
		cmd.Env = env

		out, err := cmd.CombinedOutput()
		t.Logf("terraform %v:\n%s", args, out)

		if err != nil {
			if failFatal {
				t.Fatalf("terraform %v failed: %v", args, err)
			}

			t.Logf("terraform %v failed (non-fatal): %v", args, err)

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

	tfBin := findTerraformOrSkip(t)
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

	applyTerraform(t, tfBin, t.TempDir(), hcl)

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

	tfBin := findTerraformOrSkip(t)
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

	applyTerraform(t, tfBin, t.TempDir(), hcl)

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
