package s3tables_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3tablessdk "github.com/aws/aws-sdk-go-v2/service/s3tables"
	s3tablestypes "github.com/aws/aws-sdk-go-v2/service/s3tables/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3tables"
)

func newTestSlice33Handler() *s3tables.Handler {
	return s3tables.NewHandler(s3tables.NewInMemoryBackend("123456789012", "us-east-1"))
}

// TestSlice33_S3Tables_RealClient drives every gopherstack-n3zi typed-slice-33
// uncovered s3tables op through the real aws-sdk-go-v2 client
// (newTestS3TablesClient, shared with handler_sdk_roundtrip_test.go).
func TestSlice33_S3Tables_RealClient(t *testing.T) {
	t.Parallel()

	cases := []struct {
		fn   func(t *testing.T)
		name string
	}{
		{testTableBucketEncryptionRealClient, "table_bucket_encryption"},
		{testTableBucketMetricsRealClient, "table_bucket_metrics"},
		{testTableBucketStorageClassRealClient, "table_bucket_storage_class"},
		{testTableBucketReplicationRealClient, "table_bucket_replication"},
		{testTableReplicationRealClient, "table_replication"},
		{testTableRecordExpirationRealClient, "table_record_expiration"},
		{testTableExtrasRealClient, "table_extras"},
		{testTagsRealClient, "tags"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t)
		})
	}
}

// testTableBucketEncryptionRealClient covers PutTableBucketEncryption,
// GetTableBucketEncryption, DeleteTableBucketEncryption.
func testTableBucketEncryptionRealClient(t *testing.T) {
	t.Helper()

	h := newTestSlice33Handler()
	client := newTestS3TablesClient(t, h)
	ctx := t.Context()

	bucketOut, err := client.CreateTableBucket(ctx, &s3tablessdk.CreateTableBucketInput{
		Name: aws.String("enc-bucket"),
	})
	require.NoError(t, err)
	bucketARN := aws.ToString(bucketOut.Arn)

	_, err = client.PutTableBucketEncryption(ctx, &s3tablessdk.PutTableBucketEncryptionInput{
		TableBucketARN: aws.String(bucketARN),
		EncryptionConfiguration: &s3tablestypes.EncryptionConfiguration{
			SseAlgorithm: s3tablestypes.SSEAlgorithmAes256,
		},
	})
	require.NoError(t, err)

	getOut, err := client.GetTableBucketEncryption(ctx, &s3tablessdk.GetTableBucketEncryptionInput{
		TableBucketARN: aws.String(bucketARN),
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.EncryptionConfiguration)
	assert.Equal(t, s3tablestypes.SSEAlgorithmAes256, getOut.EncryptionConfiguration.SseAlgorithm)

	_, err = client.DeleteTableBucketEncryption(ctx, &s3tablessdk.DeleteTableBucketEncryptionInput{
		TableBucketARN: aws.String(bucketARN),
	})
	require.NoError(t, err)

	_, err = client.GetTableBucketEncryption(ctx, &s3tablessdk.GetTableBucketEncryptionInput{
		TableBucketARN: aws.String(bucketARN),
	})
	require.Error(t, err)
}

// testTableBucketMetricsRealClient covers PutTableBucketMetricsConfiguration,
// GetTableBucketMetricsConfiguration, DeleteTableBucketMetricsConfiguration.
func testTableBucketMetricsRealClient(t *testing.T) {
	t.Helper()

	h := newTestSlice33Handler()
	client := newTestS3TablesClient(t, h)
	ctx := t.Context()

	bucketOut, err := client.CreateTableBucket(ctx, &s3tablessdk.CreateTableBucketInput{
		Name: aws.String("metrics-bucket"),
	})
	require.NoError(t, err)
	bucketARN := aws.ToString(bucketOut.Arn)

	getOut1, err := client.GetTableBucketMetricsConfiguration(
		ctx, &s3tablessdk.GetTableBucketMetricsConfigurationInput{TableBucketARN: aws.String(bucketARN)},
	)
	require.NoError(t, err)
	assert.Equal(t, bucketARN, aws.ToString(getOut1.TableBucketARN))
	assert.Empty(t, aws.ToString(getOut1.Id))

	_, err = client.PutTableBucketMetricsConfiguration(
		ctx, &s3tablessdk.PutTableBucketMetricsConfigurationInput{TableBucketARN: aws.String(bucketARN)},
	)
	require.NoError(t, err)

	getOut2, err := client.GetTableBucketMetricsConfiguration(
		ctx, &s3tablessdk.GetTableBucketMetricsConfigurationInput{TableBucketARN: aws.String(bucketARN)},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(getOut2.Id))

	_, err = client.DeleteTableBucketMetricsConfiguration(
		ctx, &s3tablessdk.DeleteTableBucketMetricsConfigurationInput{TableBucketARN: aws.String(bucketARN)},
	)
	require.NoError(t, err)

	getOut3, err := client.GetTableBucketMetricsConfiguration(
		ctx, &s3tablessdk.GetTableBucketMetricsConfigurationInput{TableBucketARN: aws.String(bucketARN)},
	)
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(getOut3.Id))
}

// testTableBucketStorageClassRealClient covers PutTableBucketStorageClass
// and GetTableBucketStorageClass.
func testTableBucketStorageClassRealClient(t *testing.T) {
	t.Helper()

	h := newTestSlice33Handler()
	client := newTestS3TablesClient(t, h)
	ctx := t.Context()

	bucketOut, err := client.CreateTableBucket(ctx, &s3tablessdk.CreateTableBucketInput{
		Name: aws.String("sc-bucket"),
	})
	require.NoError(t, err)
	bucketARN := aws.ToString(bucketOut.Arn)

	_, err = client.PutTableBucketStorageClass(ctx, &s3tablessdk.PutTableBucketStorageClassInput{
		TableBucketARN: aws.String(bucketARN),
		StorageClassConfiguration: &s3tablestypes.StorageClassConfiguration{
			StorageClass: s3tablestypes.StorageClassStandard,
		},
	})
	require.NoError(t, err)

	getOut, err := client.GetTableBucketStorageClass(ctx, &s3tablessdk.GetTableBucketStorageClassInput{
		TableBucketARN: aws.String(bucketARN),
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.StorageClassConfiguration)
	assert.Equal(t, s3tablestypes.StorageClassStandard, getOut.StorageClassConfiguration.StorageClass)
}

// testTableBucketReplicationRealClient covers PutTableBucketReplication,
// GetTableBucketReplication, DeleteTableBucketReplication.
func testTableBucketReplicationRealClient(t *testing.T) {
	t.Helper()

	h := newTestSlice33Handler()
	client := newTestS3TablesClient(t, h)
	ctx := t.Context()

	srcOut, err := client.CreateTableBucket(ctx, &s3tablessdk.CreateTableBucketInput{
		Name: aws.String("repl-src-bucket"),
	})
	require.NoError(t, err)
	srcARN := aws.ToString(srcOut.Arn)

	destOut, err := client.CreateTableBucket(ctx, &s3tablessdk.CreateTableBucketInput{
		Name: aws.String("repl-dest-bucket"),
	})
	require.NoError(t, err)
	destARN := aws.ToString(destOut.Arn)

	putOut, err := client.PutTableBucketReplication(ctx, &s3tablessdk.PutTableBucketReplicationInput{
		TableBucketARN: aws.String(srcARN),
		Configuration: &s3tablestypes.TableBucketReplicationConfiguration{
			Role: aws.String("arn:aws:iam::123456789012:role/s3tables-replication"),
			Rules: []s3tablestypes.TableBucketReplicationRule{
				{
					Destinations: []s3tablestypes.ReplicationDestination{
						{DestinationTableBucketARN: aws.String(destARN)},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	versionToken := aws.ToString(putOut.VersionToken)
	require.NotEmpty(t, versionToken)

	getOut, err := client.GetTableBucketReplication(ctx, &s3tablessdk.GetTableBucketReplicationInput{
		TableBucketARN: aws.String(srcARN),
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.Configuration)
	assert.Equal(t, "arn:aws:iam::123456789012:role/s3tables-replication", aws.ToString(getOut.Configuration.Role))
	require.Len(t, getOut.Configuration.Rules, 1)
	require.Len(t, getOut.Configuration.Rules[0].Destinations, 1)
	assert.Equal(t, destARN, aws.ToString(getOut.Configuration.Rules[0].Destinations[0].DestinationTableBucketARN))

	_, err = client.DeleteTableBucketReplication(ctx, &s3tablessdk.DeleteTableBucketReplicationInput{
		TableBucketARN: aws.String(srcARN),
		VersionToken:   aws.String(versionToken),
	})
	require.NoError(t, err)

	_, err = client.GetTableBucketReplication(ctx, &s3tablessdk.GetTableBucketReplicationInput{
		TableBucketARN: aws.String(srcARN),
	})
	require.Error(t, err)
}

// testTableReplicationRealClient covers PutTableReplication,
// GetTableReplication, DeleteTableReplication, GetTableReplicationStatus.
func testTableReplicationRealClient(t *testing.T) {
	t.Helper()

	h := newTestSlice33Handler()
	client := newTestS3TablesClient(t, h)
	ctx := t.Context()

	bucketOut, err := client.CreateTableBucket(ctx, &s3tablessdk.CreateTableBucketInput{
		Name: aws.String("tbl-repl-bucket"),
	})
	require.NoError(t, err)
	bucketARN := aws.ToString(bucketOut.Arn)

	_, err = client.CreateNamespace(ctx, &s3tablessdk.CreateNamespaceInput{
		TableBucketARN: aws.String(bucketARN),
		Namespace:      []string{"ns1"},
	})
	require.NoError(t, err)

	tableOut, err := client.CreateTable(ctx, &s3tablessdk.CreateTableInput{
		TableBucketARN: aws.String(bucketARN),
		Namespace:      aws.String("ns1"),
		Name:           aws.String("tbl1"),
		Format:         s3tablestypes.OpenTableFormatIceberg,
	})
	require.NoError(t, err)
	tableARN := aws.ToString(tableOut.TableARN)

	destOut, err := client.CreateTableBucket(ctx, &s3tablessdk.CreateTableBucketInput{
		Name: aws.String("tbl-repl-dest-bucket"),
	})
	require.NoError(t, err)
	destARN := aws.ToString(destOut.Arn)

	putOut, err := client.PutTableReplication(ctx, &s3tablessdk.PutTableReplicationInput{
		TableArn: aws.String(tableARN),
		Configuration: &s3tablestypes.TableReplicationConfiguration{
			Role: aws.String("arn:aws:iam::123456789012:role/s3tables-replication"),
			Rules: []s3tablestypes.TableReplicationRule{
				{
					Destinations: []s3tablestypes.ReplicationDestination{
						{DestinationTableBucketARN: aws.String(destARN)},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	versionToken := aws.ToString(putOut.VersionToken)

	getOut, err := client.GetTableReplication(ctx, &s3tablessdk.GetTableReplicationInput{
		TableArn: aws.String(tableARN),
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.Configuration)
	require.Len(t, getOut.Configuration.Rules, 1)
	assert.Equal(t, destARN, aws.ToString(getOut.Configuration.Rules[0].Destinations[0].DestinationTableBucketARN))

	statusOut, err := client.GetTableReplicationStatus(ctx, &s3tablessdk.GetTableReplicationStatusInput{
		TableArn: aws.String(tableARN),
	})
	require.NoError(t, err)
	assert.Equal(t, tableARN, aws.ToString(statusOut.SourceTableArn))
	require.Len(t, statusOut.Destinations, 1)
	assert.Equal(t, destARN, aws.ToString(statusOut.Destinations[0].DestinationTableBucketArn))
	assert.Equal(t, s3tablestypes.ReplicationStatusCompleted, statusOut.Destinations[0].ReplicationStatus)

	_, err = client.DeleteTableReplication(ctx, &s3tablessdk.DeleteTableReplicationInput{
		TableArn:     aws.String(tableARN),
		VersionToken: aws.String(versionToken),
	})
	require.NoError(t, err)

	_, err = client.GetTableReplication(ctx, &s3tablessdk.GetTableReplicationInput{
		TableArn: aws.String(tableARN),
	})
	require.Error(t, err)
}

// testTableRecordExpirationRealClient covers
// PutTableRecordExpirationConfiguration,
// GetTableRecordExpirationConfiguration, GetTableRecordExpirationJobStatus.
func testTableRecordExpirationRealClient(t *testing.T) {
	t.Helper()

	h := newTestSlice33Handler()
	client := newTestS3TablesClient(t, h)
	ctx := t.Context()

	bucketOut, err := client.CreateTableBucket(ctx, &s3tablessdk.CreateTableBucketInput{
		Name: aws.String("rec-exp-bucket"),
	})
	require.NoError(t, err)
	bucketARN := aws.ToString(bucketOut.Arn)

	_, err = client.CreateNamespace(ctx, &s3tablessdk.CreateNamespaceInput{
		TableBucketARN: aws.String(bucketARN),
		Namespace:      []string{"ns1"},
	})
	require.NoError(t, err)

	tableOut, err := client.CreateTable(ctx, &s3tablessdk.CreateTableInput{
		TableBucketARN: aws.String(bucketARN),
		Namespace:      aws.String("ns1"),
		Name:           aws.String("tbl1"),
		Format:         s3tablestypes.OpenTableFormatIceberg,
	})
	require.NoError(t, err)
	tableARN := aws.ToString(tableOut.TableARN)

	statusOut0, err := client.GetTableRecordExpirationJobStatus(
		ctx, &s3tablessdk.GetTableRecordExpirationJobStatusInput{TableArn: aws.String(tableARN)},
	)
	require.NoError(t, err)
	assert.Equal(t, s3tablestypes.TableRecordExpirationJobStatusDisabled, statusOut0.Status)

	_, err = client.PutTableRecordExpirationConfiguration(
		ctx, &s3tablessdk.PutTableRecordExpirationConfigurationInput{
			TableArn: aws.String(tableARN),
			Value: &s3tablestypes.TableRecordExpirationConfigurationValue{
				Status:   s3tablestypes.TableRecordExpirationStatusEnabled,
				Settings: &s3tablestypes.TableRecordExpirationSettings{Days: aws.Int32(30)},
			},
		},
	)
	require.NoError(t, err)

	getOut, err := client.GetTableRecordExpirationConfiguration(
		ctx, &s3tablessdk.GetTableRecordExpirationConfigurationInput{TableArn: aws.String(tableARN)},
	)
	require.NoError(t, err)
	require.NotNil(t, getOut.Configuration)
	assert.Equal(t, s3tablestypes.TableRecordExpirationStatusEnabled, getOut.Configuration.Status)
	require.NotNil(t, getOut.Configuration.Settings)
	assert.Equal(t, int32(30), aws.ToInt32(getOut.Configuration.Settings.Days))

	statusOut, err := client.GetTableRecordExpirationJobStatus(
		ctx, &s3tablessdk.GetTableRecordExpirationJobStatusInput{TableArn: aws.String(tableARN)},
	)
	require.NoError(t, err)
	assert.Equal(t, s3tablestypes.TableRecordExpirationJobStatusNotYetRun, statusOut.Status)
}

// testTableExtrasRealClient covers GetTableEncryption,
// GetTableMaintenanceJobStatus, GetTableMetadataLocation,
// GetTableStorageClass, RenameTable.
func testTableExtrasRealClient(t *testing.T) {
	t.Helper()

	h := newTestSlice33Handler()
	client := newTestS3TablesClient(t, h)
	ctx := t.Context()

	bucketOut, err := client.CreateTableBucket(ctx, &s3tablessdk.CreateTableBucketInput{
		Name: aws.String("extras-bucket"),
	})
	require.NoError(t, err)
	bucketARN := aws.ToString(bucketOut.Arn)

	_, err = client.CreateNamespace(ctx, &s3tablessdk.CreateNamespaceInput{
		TableBucketARN: aws.String(bucketARN),
		Namespace:      []string{"ns1"},
	})
	require.NoError(t, err)

	_, err = client.CreateTable(ctx, &s3tablessdk.CreateTableInput{
		TableBucketARN: aws.String(bucketARN),
		Namespace:      aws.String("ns1"),
		Name:           aws.String("tbl1"),
		Format:         s3tablestypes.OpenTableFormatIceberg,
		EncryptionConfiguration: &s3tablestypes.EncryptionConfiguration{
			SseAlgorithm: s3tablestypes.SSEAlgorithmAes256,
		},
	})
	require.NoError(t, err)

	encOut, err := client.GetTableEncryption(ctx, &s3tablessdk.GetTableEncryptionInput{
		TableBucketARN: aws.String(bucketARN),
		Namespace:      aws.String("ns1"),
		Name:           aws.String("tbl1"),
	})
	require.NoError(t, err)
	require.NotNil(t, encOut.EncryptionConfiguration)
	assert.Equal(t, s3tablestypes.SSEAlgorithmAes256, encOut.EncryptionConfiguration.SseAlgorithm)

	jobOut, err := client.GetTableMaintenanceJobStatus(ctx, &s3tablessdk.GetTableMaintenanceJobStatusInput{
		TableBucketARN: aws.String(bucketARN),
		Namespace:      aws.String("ns1"),
		Name:           aws.String("tbl1"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(jobOut.TableARN))
	// Every new table gets the two default maintenance types configured
	// (icebergCompaction, icebergSnapshotManagement); this in-memory backend
	// runs no background jobs, so both report Not_Yet_Run.
	require.Len(t, jobOut.Status, 2)
	for _, maintenanceType := range []string{"icebergCompaction", "icebergSnapshotManagement"} {
		v, ok := jobOut.Status[maintenanceType]
		require.True(t, ok, "expected %s in maintenance job status", maintenanceType)
		assert.Equal(t, s3tablestypes.JobStatusNotYetRun, v.Status)
	}

	metaOut, err := client.GetTableMetadataLocation(ctx, &s3tablessdk.GetTableMetadataLocationInput{
		TableBucketARN: aws.String(bucketARN),
		Namespace:      aws.String("ns1"),
		Name:           aws.String("tbl1"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(metaOut.VersionToken))

	scOut, err := client.GetTableStorageClass(ctx, &s3tablessdk.GetTableStorageClassInput{
		TableBucketARN: aws.String(bucketARN),
		Namespace:      aws.String("ns1"),
		Name:           aws.String("tbl1"),
	})
	require.NoError(t, err)
	require.NotNil(t, scOut.StorageClassConfiguration)
	assert.Equal(t, s3tablestypes.StorageClassStandard, scOut.StorageClassConfiguration.StorageClass)

	_, err = client.RenameTable(ctx, &s3tablessdk.RenameTableInput{
		TableBucketARN: aws.String(bucketARN),
		Namespace:      aws.String("ns1"),
		Name:           aws.String("tbl1"),
		NewName:        aws.String("tbl1-renamed"),
	})
	require.NoError(t, err)

	getOut, err := client.GetTable(ctx, &s3tablessdk.GetTableInput{
		TableBucketARN: aws.String(bucketARN),
		Namespace:      aws.String("ns1"),
		Name:           aws.String("tbl1-renamed"),
	})
	require.NoError(t, err)
	assert.Equal(t, "tbl1-renamed", aws.ToString(getOut.Name))
}

// testTagsRealClient covers TagResource, UntagResource,
// ListTagsForResource.
func testTagsRealClient(t *testing.T) {
	t.Helper()

	h := newTestSlice33Handler()
	client := newTestS3TablesClient(t, h)
	ctx := t.Context()

	bucketOut, err := client.CreateTableBucket(ctx, &s3tablessdk.CreateTableBucketInput{
		Name: aws.String("tag-bucket"),
	})
	require.NoError(t, err)
	bucketARN := aws.ToString(bucketOut.Arn)

	_, err = client.TagResource(ctx, &s3tablessdk.TagResourceInput{
		ResourceArn: aws.String(bucketARN),
		Tags: map[string]string{
			"env":  "prod",
			"team": "data",
		},
	})
	require.NoError(t, err)

	listOut, err := client.ListTagsForResource(ctx, &s3tablessdk.ListTagsForResourceInput{
		ResourceArn: aws.String(bucketARN),
	})
	require.NoError(t, err)
	require.Len(t, listOut.Tags, 2)
	assert.Equal(t, "prod", listOut.Tags["env"])

	_, err = client.UntagResource(ctx, &s3tablessdk.UntagResourceInput{
		ResourceArn: aws.String(bucketARN),
		TagKeys:     []string{"team"},
	})
	require.NoError(t, err)

	listOut2, err := client.ListTagsForResource(ctx, &s3tablessdk.ListTagsForResourceInput{
		ResourceArn: aws.String(bucketARN),
	})
	require.NoError(t, err)
	require.Len(t, listOut2.Tags, 1)
	assert.Equal(t, "prod", listOut2.Tags["env"])
}
