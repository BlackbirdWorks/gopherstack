package s3tables_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3tablessdk "github.com/aws/aws-sdk-go-v2/service/s3tables"
	s3tablestypes "github.com/aws/aws-sdk-go-v2/service/s3tables/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3tables"
)

// Test_SDKRoundTrip_GetTableBucketEncryption_NeverNotFound proves
// GetTableBucketEncryption's required "encryptionConfiguration"
// (GetTableBucketEncryptionOutput, deserializers.go's
// awsRestjson1_deserializeOpDocumentGetTableBucketEncryptionOutput) decodes
// as the AES256 default for a freshly created bucket -- a real, reachable
// state, since encryptionConfiguration is optional on CreateTableBucketInput
// and most callers never set it. Before the fix, gopherstack returned
// NotFoundException whenever no PutTableBucketEncryption had ever been
// called, so a real SDK client's GetTableBucketEncryption call errored
// instead of decoding the required field -- even though every S3 Tables
// bucket has encryption at rest by default (matching the same
// table-override -> bucket-default -> AES256 fallback GetTableEncryption
// already implements at the table level, tables.go's defaultSSEAlgorithm).
func Test_SDKRoundTrip_GetTableBucketEncryption_NeverNotFound(t *testing.T) {
	t.Parallel()

	backend := s3tables.NewInMemoryBackend("000000000000", rtTestRegion)
	h := s3tables.NewHandler(backend)
	client := newTestS3TablesClient(t, h)
	ctx := t.Context()

	bucket, err := backend.CreateTableBucket("rt-enc-bucket", s3tables.CreateTableBucketOptions{})
	require.NoError(t, err)

	out, err := client.GetTableBucketEncryption(ctx, &s3tablessdk.GetTableBucketEncryptionInput{
		TableBucketARN: aws.String(bucket.ARN),
	})
	require.NoError(t, err, "GetTableBucketEncryption must succeed for a bucket with no explicit override")
	require.NotNil(t, out.EncryptionConfiguration, "encryptionConfiguration is required and must never be nil")
	require.Equal(t, "AES256", string(out.EncryptionConfiguration.SseAlgorithm))

	// PutTableBucketEncryption sets a KMS override; DeleteTableBucketEncryption
	// must revert to the same AES256 default, not to NotFound.
	_, err = client.PutTableBucketEncryption(ctx, &s3tablessdk.PutTableBucketEncryptionInput{
		TableBucketARN: aws.String(bucket.ARN),
		EncryptionConfiguration: &s3tablestypes.EncryptionConfiguration{
			SseAlgorithm: s3tablestypes.SSEAlgorithmAwsKms,
			KmsKeyArn:    aws.String("arn:aws:kms:us-east-1:000000000000:key/test"),
		},
	})
	require.NoError(t, err)

	_, err = client.DeleteTableBucketEncryption(ctx, &s3tablessdk.DeleteTableBucketEncryptionInput{
		TableBucketARN: aws.String(bucket.ARN),
	})
	require.NoError(t, err)

	afterDelete, err := client.GetTableBucketEncryption(ctx, &s3tablessdk.GetTableBucketEncryptionInput{
		TableBucketARN: aws.String(bucket.ARN),
	})
	require.NoError(t, err, "GetTableBucketEncryption must still succeed after deleting the override")
	require.NotNil(t, afterDelete.EncryptionConfiguration)
	require.Equal(t, "AES256", string(afterDelete.EncryptionConfiguration.SseAlgorithm))
}
