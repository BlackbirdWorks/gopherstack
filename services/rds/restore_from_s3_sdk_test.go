package rds_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	rdssdk "github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRestoreDBInstanceFromS3_RealSDKClient drives RestoreDBInstanceFromS3
// through the real aws-sdk-go-v2 client, whose query-protocol serializer
// binds S3IngestionRoleArn/SourceEngine/SourceEngineVersion to those exact
// case-sensitive keys (serializers.go: awsAwsquery_serializeOpDocument
// RestoreDBInstanceFromS3Input). The handler previously never read
// S3IngestionRoleArn, SourceEngine or SourceEngineVersion from url.Values at
// all, so every restored instance silently lost them; a request built by
// hand with the wrong key names would have passed a map-asserting test just
// as easily as the real bug did.
func TestRestoreDBInstanceFromS3_RealSDKClient(t *testing.T) {
	t.Parallel()

	h := newTestRDSHandler()
	client := newTestRDSClient(t, h)

	out, err := client.RestoreDBInstanceFromS3(t.Context(), &rdssdk.RestoreDBInstanceFromS3Input{
		DBInstanceIdentifier: aws.String("restored-sdk"),
		DBInstanceClass:      aws.String("db.t3.micro"),
		Engine:               aws.String("mysql"),
		S3BucketName:         aws.String("my-backup-bucket"),
		S3IngestionRoleArn:   aws.String("arn:aws:iam::000000000000:role/rds-s3-ingestion"),
		SourceEngine:         aws.String("mysql"),
		SourceEngineVersion:  aws.String("5.7.40"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.DBInstance)
	assert.Equal(t, "restored-sdk", aws.ToString(out.DBInstance.DBInstanceIdentifier))
	assert.Equal(t, "mysql", aws.ToString(out.DBInstance.Engine))

	// The SDK's own client-side validation middleware blocks a request
	// missing a required member before it is ever sent, so the reachable
	// case for this emulator is a required member present-but-empty --
	// exercised directly against the backend in
	// TestRestoreDBInstanceFromS3 (db_instances_operations_test.go).
}

// TestRestoreDBClusterFromS3_RealSDKClient is the DB cluster analogue of
// TestRestoreDBInstanceFromS3_RealSDKClient.
func TestRestoreDBClusterFromS3_RealSDKClient(t *testing.T) {
	t.Parallel()

	h := newTestRDSHandler()
	client := newTestRDSClient(t, h)

	out, err := client.RestoreDBClusterFromS3(t.Context(), &rdssdk.RestoreDBClusterFromS3Input{
		DBClusterIdentifier: aws.String("restored-cluster-sdk"),
		Engine:              aws.String("aurora-mysql"),
		MasterUsername:      aws.String("admin"),
		S3BucketName:        aws.String("my-backup-bucket"),
		S3IngestionRoleArn:  aws.String("arn:aws:iam::000000000000:role/rds-s3-ingestion"),
		SourceEngine:        aws.String("mysql"),
		SourceEngineVersion: aws.String("5.7.40"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.DBCluster)
	assert.Equal(t, "restored-cluster-sdk", aws.ToString(out.DBCluster.DBClusterIdentifier))
	assert.Equal(t, "aurora-mysql", aws.ToString(out.DBCluster.Engine))
}
