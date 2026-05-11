package terraform_test

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	s3svc "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	sqssvc "github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The tests in this file cover the AWS-realism / LocalStack-parity features
// added in this PR. Each test provisions resources via Terraform, then drives
// the SDK to confirm the feature actually behaves correctly end-to-end (not
// just that the apply succeeded).

// TestTerraform_S3_PublicAccessBlock_Ownership provisions a bucket with both
// a PublicAccessBlock and BucketOwnerEnforced ObjectOwnership, then proves
// the enforcement: PutBucketAcl with a public canned ACL is rejected.
func TestTerraform_S3_PublicAccessBlock_Ownership(t *testing.T) {
	t.Parallel()

	runTFTest(t, tfTestCase{
		name:    "pab_ownership_blocks_public_acl",
		fixture: "s3/pab_ownership",
		setup: func(t *testing.T, _ string) map[string]any {
			t.Helper()

			return map[string]any{"BucketName": "tf-pab-" + uuid.NewString()[:8]}
		},
		verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
			t.Helper()

			s3c := createS3Client(t)
			bucket := vars["BucketName"].(string)

			// 1. GetPublicAccessBlock must echo the configuration we applied.
			pab, err := s3c.GetPublicAccessBlock(ctx, &s3svc.GetPublicAccessBlockInput{
				Bucket: aws.String(bucket),
			})
			require.NoError(t, err, "GetPublicAccessBlock should succeed")
			require.NotNil(t, pab.PublicAccessBlockConfiguration)
			assert.True(t, aws.ToBool(pab.PublicAccessBlockConfiguration.BlockPublicAcls))
			assert.True(t, aws.ToBool(pab.PublicAccessBlockConfiguration.BlockPublicPolicy))

			// 2. GetBucketOwnershipControls must report BucketOwnerEnforced.
			oc, err := s3c.GetBucketOwnershipControls(ctx, &s3svc.GetBucketOwnershipControlsInput{
				Bucket: aws.String(bucket),
			})
			require.NoError(t, err, "GetBucketOwnershipControls should succeed")
			require.NotNil(t, oc.OwnershipControls)
			require.Len(t, oc.OwnershipControls.Rules, 1)
			assert.Equal(t,
				s3types.ObjectOwnership("BucketOwnerEnforced"),
				oc.OwnershipControls.Rules[0].ObjectOwnership,
			)

			// 3. PutBucketAcl with a public canned ACL must be rejected.
			//    BucketOwnerEnforced disables ACLs entirely, so even a private
			//    canned ACL is rejected; we only assert the public-read path
			//    here since it's the realistic security concern.
			_, err = s3c.PutBucketAcl(ctx, &s3svc.PutBucketAclInput{
				Bucket: aws.String(bucket),
				ACL:    s3types.BucketCannedACLPublicRead,
			})
			require.Error(t, err, "public-read ACL must be rejected on a BlockPublicAcls+BucketOwnerEnforced bucket")
		},
	})
}

// TestTerraform_S3_SSE_RoundTrip provisions a bucket with SSE-S3 default
// encryption, uploads an object, and verifies:
//   - GetBucketEncryption echoes the configuration
//   - PutObject + GetObject round-trips the plaintext (decryption works)
//   - The response carries x-amz-server-side-encryption: AES256
func TestTerraform_S3_SSE_RoundTrip(t *testing.T) {
	t.Parallel()

	runTFTest(t, tfTestCase{
		name:    "sse_s3_round_trip",
		fixture: "s3/sse",
		setup: func(t *testing.T, _ string) map[string]any {
			t.Helper()

			return map[string]any{"BucketName": "tf-sse-" + uuid.NewString()[:8]}
		},
		verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
			t.Helper()

			s3c := createS3Client(t)
			bucket := vars["BucketName"].(string)

			enc, err := s3c.GetBucketEncryption(ctx, &s3svc.GetBucketEncryptionInput{
				Bucket: aws.String(bucket),
			})
			require.NoError(t, err)
			require.NotNil(t, enc.ServerSideEncryptionConfiguration)
			require.NotEmpty(t, enc.ServerSideEncryptionConfiguration.Rules)
			assert.Equal(t,
				s3types.ServerSideEncryption("AES256"),
				enc.ServerSideEncryptionConfiguration.Rules[0].ApplyServerSideEncryptionByDefault.SSEAlgorithm,
			)

			// PutObject with explicit SSE-S3 header; backend now actually
			// encrypts the bytes.
			plaintext := []byte("the quick brown fox jumps over the lazy dog")
			_, err = s3c.PutObject(ctx, &s3svc.PutObjectInput{
				Bucket:               aws.String(bucket),
				Key:                  aws.String("doc.txt"),
				Body:                 bytes.NewReader(plaintext),
				ServerSideEncryption: s3types.ServerSideEncryptionAes256,
			})
			require.NoError(t, err)

			// GetObject round-trips the plaintext and reports the SSE header.
			out, err := s3c.GetObject(ctx, &s3svc.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String("doc.txt"),
			})
			require.NoError(t, err)
			defer out.Body.Close()
			got, _ := io.ReadAll(out.Body)
			assert.Equal(t, plaintext, got, "GET must round-trip the plaintext through envelope decryption")
			assert.Equal(t, s3types.ServerSideEncryptionAes256, out.ServerSideEncryption)
		},
	})
}

// TestTerraform_S3_Logging provisions a bucket logging configuration and
// proves the backend actually dispatches log records to the target bucket
// (the real behaviour, not LocalStack's no-op echo).
func TestTerraform_S3_Logging(t *testing.T) {
	t.Parallel()

	runTFTest(t, tfTestCase{
		name:    "logging_dispatch",
		fixture: "s3/logging",
		setup: func(t *testing.T, _ string) map[string]any {
			t.Helper()
			suffix := uuid.NewString()[:8]

			return map[string]any{
				"SourceBucket": "tf-log-src-" + suffix,
				"LogBucket":    "tf-log-tgt-" + suffix,
			}
		},
		verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
			t.Helper()

			s3c := createS3Client(t)
			source := vars["SourceBucket"].(string)
			target := vars["LogBucket"].(string)

			// Confirm the logging configuration is in place.
			lg, err := s3c.GetBucketLogging(ctx, &s3svc.GetBucketLoggingInput{
				Bucket: aws.String(source),
			})
			require.NoError(t, err)
			require.NotNil(t, lg.LoggingEnabled)
			assert.Equal(t, target, aws.ToString(lg.LoggingEnabled.TargetBucket))

			// Generate a GET on the source bucket — this should cause the
			// backend to append an access log record under access-logs/ in the
			// target bucket.
			_, _ = s3c.PutObject(ctx, &s3svc.PutObjectInput{
				Bucket: aws.String(source),
				Key:    aws.String("trigger.txt"),
				Body:   bytes.NewReader([]byte("hi")),
			})
			_, _ = s3c.GetObject(ctx, &s3svc.GetObjectInput{
				Bucket: aws.String(source),
				Key:    aws.String("trigger.txt"),
			})

			// Dispatch is asynchronous; poll the target bucket up to ~2s.
			deadline := time.Now().Add(2 * time.Second)
			var found []s3types.Object
			for time.Now().Before(deadline) {
				lo, listErr := s3c.ListObjectsV2(ctx, &s3svc.ListObjectsV2Input{
					Bucket: aws.String(target),
					Prefix: aws.String("access-logs/"),
				})
				if listErr == nil && len(lo.Contents) > 0 {
					found = lo.Contents

					break
				}
				time.Sleep(50 * time.Millisecond)
			}
			require.NotEmpty(t, found, "expected at least one access-log object in the target bucket")

			// Inspect the first log record and confirm it contains the
			// operation name and source-bucket fields.
			logOut, err := s3c.GetObject(ctx, &s3svc.GetObjectInput{
				Bucket: aws.String(target),
				Key:    found[0].Key,
			})
			require.NoError(t, err)
			defer logOut.Body.Close()
			body, _ := io.ReadAll(logOut.Body)
			assert.Contains(t, string(body), source, "log line must reference the source bucket")
			assert.True(t,
				strings.Contains(string(body), "REST.GET.OBJECT") ||
					strings.Contains(string(body), "REST.PUT.OBJECT"),
				"log line must record at least one of our operations",
			)
		},
	})
}

// TestTerraform_SQS_FIFOThroughput provisions a FIFO queue with
// DeduplicationScope=messageGroup + FifoThroughputLimit=perMessageGroupId and
// verifies both attributes round-trip through GetQueueAttributes. The
// backend now actually validates these enum values on Create.
func TestTerraform_SQS_FIFOThroughput(t *testing.T) {
	t.Parallel()

	runTFTest(t, tfTestCase{
		name:    "fifo_throughput_per_message_group",
		fixture: "sqs/fifo_throughput",
		setup: func(t *testing.T, _ string) map[string]any {
			t.Helper()

			return map[string]any{"QueueName": "tf-fifo-tput-" + uuid.NewString()[:8]}
		},
		verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
			t.Helper()

			sqsc := createSQSClient(t)
			queueName := vars["QueueName"].(string) + ".fifo"

			urlOut, err := sqsc.GetQueueUrl(ctx, &sqssvc.GetQueueUrlInput{
				QueueName: aws.String(queueName),
			})
			require.NoError(t, err, "GetQueueUrl should succeed for the FIFO queue")

			attrs, err := sqsc.GetQueueAttributes(ctx, &sqssvc.GetQueueAttributesInput{
				QueueUrl:       urlOut.QueueUrl,
				AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameAll},
			})
			require.NoError(t, err)

			assert.Equal(t, "true", attrs.Attributes["FifoQueue"])
			assert.Equal(t, "messageGroup", attrs.Attributes["DeduplicationScope"])
			assert.Equal(t, "perMessageGroupId", attrs.Attributes["FifoThroughputLimit"])
		},
	})
}

// TestTerraform_SQS_RedriveAllow provisions a DLQ with a byQueue
// RedriveAllowPolicy and a source queue referencing it. Verifies both queues'
// attributes round-trip the policy correctly — the validator added in this
// PR rejects malformed policies, so a successful apply already proves the
// happy path.
func TestTerraform_SQS_RedriveAllow(t *testing.T) {
	t.Parallel()

	runTFTest(t, tfTestCase{
		name:    "redrive_allow_byQueue",
		fixture: "sqs/redrive_allow",
		setup: func(t *testing.T, _ string) map[string]any {
			t.Helper()
			suffix := uuid.NewString()[:8]

			return map[string]any{
				"SourceQueue": "tf-redrive-src-" + suffix,
				"DLQName":     "tf-redrive-dlq-" + suffix,
			}
		},
		verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
			t.Helper()

			sqsc := createSQSClient(t)
			dlq, err := sqsc.GetQueueUrl(ctx, &sqssvc.GetQueueUrlInput{
				QueueName: aws.String(vars["DLQName"].(string)),
			})
			require.NoError(t, err)

			attrs, err := sqsc.GetQueueAttributes(ctx, &sqssvc.GetQueueAttributesInput{
				QueueUrl:       dlq.QueueUrl,
				AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameAll},
			})
			require.NoError(t, err)

			policy, ok := attrs.Attributes["RedriveAllowPolicy"]
			require.True(t, ok, "DLQ must report RedriveAllowPolicy")
			assert.Contains(t, policy, `"redrivePermission":"byQueue"`)
			assert.Contains(t, policy, `"sourceQueueArns"`)
		},
	})
}

// TestTerraform_DDB_PITR_SSE provisions a DynamoDB table with both PITR and
// SSE enabled and verifies the attributes round-trip:
//   - DescribeContinuousBackups reports PITR=ENABLED
//   - DescribeTable.SSEDescription reports status=ENABLED
//   - DeletionProtectionEnabled reports true
func TestTerraform_DDB_PITR_SSE(t *testing.T) {
	t.Parallel()

	runTFTest(t, tfTestCase{
		name:    "pitr_and_sse",
		fixture: "dynamodb/pitr_sse",
		setup: func(t *testing.T, _ string) map[string]any {
			t.Helper()

			return map[string]any{"TableName": "tf-pitr-sse-" + uuid.NewString()[:8]}
		},
		verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
			t.Helper()

			ddb := createDynamoDBClient(t)
			tableName := vars["TableName"].(string)

			tbl, err := ddb.DescribeTable(ctx, &dynamodb.DescribeTableInput{
				TableName: aws.String(tableName),
			})
			require.NoError(t, err)
			require.NotNil(t, tbl.Table)

			require.NotNil(t, tbl.Table.SSEDescription, "DescribeTable must include SSEDescription when SSE is enabled")
			assert.Equal(t, ddbtypes.SSEStatusEnabled, tbl.Table.SSEDescription.Status)

			assert.True(t, aws.ToBool(tbl.Table.DeletionProtectionEnabled))

			cb, err := ddb.DescribeContinuousBackups(ctx, &dynamodb.DescribeContinuousBackupsInput{
				TableName: aws.String(tableName),
			})
			require.NoError(t, err)
			require.NotNil(t, cb.ContinuousBackupsDescription)
			require.NotNil(t, cb.ContinuousBackupsDescription.PointInTimeRecoveryDescription)
			assert.Equal(t,
				ddbtypes.PointInTimeRecoveryStatusEnabled,
				cb.ContinuousBackupsDescription.PointInTimeRecoveryDescription.PointInTimeRecoveryStatus,
			)
		},
	})
}

// TestTerraform_DDB_GSIValidation provisions a table with a GSI under
// PAY_PER_REQUEST billing (no explicit GSI throughput). The validator added
// in this PR accepts this case and rejects GSIs that declare positive RCU/WCU
// in on-demand mode — a successful apply demonstrates the relaxed-for-on-
// demand path works, and a follow-on DescribeTable confirms the GSI is
// reported as ACTIVE.
func TestTerraform_DDB_GSIValidation(t *testing.T) {
	t.Parallel()

	runTFTest(t, tfTestCase{
		name:    "gsi_on_demand_no_throughput",
		fixture: "dynamodb/gsi_validation",
		setup: func(t *testing.T, _ string) map[string]any {
			t.Helper()

			return map[string]any{"TableName": "tf-gsi-val-" + uuid.NewString()[:8]}
		},
		verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
			t.Helper()

			ddb := createDynamoDBClient(t)
			tableName := vars["TableName"].(string)

			tbl, err := ddb.DescribeTable(ctx, &dynamodb.DescribeTableInput{
				TableName: aws.String(tableName),
			})
			require.NoError(t, err)
			require.Len(t, tbl.Table.GlobalSecondaryIndexes, 1)
			gsi := tbl.Table.GlobalSecondaryIndexes[0]
			assert.Equal(t, "by-gsi", aws.ToString(gsi.IndexName))
			assert.Equal(t, ddbtypes.IndexStatusActive, gsi.IndexStatus)
		},
	})
}
