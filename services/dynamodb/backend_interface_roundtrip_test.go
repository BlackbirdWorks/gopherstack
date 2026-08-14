// Package dynamodb_test covers the eight ops moved onto StorageBackend in
// gopherstack-za0c (DescribeContinuousBackups, UpdateContinuousBackups,
// ExportTableToPointInTime, DescribeExport, ListExports,
// RestoreTableFromBackup, RestoreTableToPointInTime -- and
// DescribeTableReplicaAutoScaling, already covered end-to-end in
// autoscaling_test.go). Each test here drives the real aws-sdk-go-v2 client
// over HTTP, proving the op still works after the move -- not just that the
// backend method returns the right Go value.
package dynamodb_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

func createPPRTableViaClient(t *testing.T, client *sdk.Client, tableName string) *sdk.CreateTableOutput {
	t.Helper()

	out, err := client.CreateTable(t.Context(), &sdk.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	return out
}

func TestContinuousBackups_RoundTrip(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	t.Cleanup(backend.Close)
	client := newTestDynamoDBClient(t, dynamodb.NewHandler(backend))

	createPPRTableViaClient(t, client, "cb-table")

	updateOut, err := client.UpdateContinuousBackups(t.Context(), &sdk.UpdateContinuousBackupsInput{
		TableName: aws.String("cb-table"),
		PointInTimeRecoverySpecification: &types.PointInTimeRecoverySpecification{
			PointInTimeRecoveryEnabled: aws.Bool(true),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, updateOut.ContinuousBackupsDescription)
	require.NotNil(t, updateOut.ContinuousBackupsDescription.PointInTimeRecoveryDescription)
	assert.Equal(
		t,
		types.PointInTimeRecoveryStatusEnabled,
		updateOut.ContinuousBackupsDescription.PointInTimeRecoveryDescription.PointInTimeRecoveryStatus,
	)

	descOut, err := client.DescribeContinuousBackups(t.Context(), &sdk.DescribeContinuousBackupsInput{
		TableName: aws.String("cb-table"),
	})
	require.NoError(t, err)
	require.NotNil(t, descOut.ContinuousBackupsDescription)
	require.NotNil(t, descOut.ContinuousBackupsDescription.PointInTimeRecoveryDescription)
	assert.Equal(
		t,
		types.PointInTimeRecoveryStatusEnabled,
		descOut.ContinuousBackupsDescription.PointInTimeRecoveryDescription.PointInTimeRecoveryStatus,
	)
}

func TestExportLifecycle_RoundTrip(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	t.Cleanup(backend.Close)
	client := newTestDynamoDBClient(t, dynamodb.NewHandler(backend))

	createOut := createPPRTableViaClient(t, client, "export-table")
	tableArn := aws.ToString(createOut.TableDescription.TableArn)

	exportOut, err := client.ExportTableToPointInTime(t.Context(), &sdk.ExportTableToPointInTimeInput{
		TableArn: aws.String(tableArn),
		S3Bucket: aws.String("export-bucket"),
	})
	require.NoError(t, err)
	require.NotNil(t, exportOut.ExportDescription)
	exportArn := aws.ToString(exportOut.ExportDescription.ExportArn)
	require.NotEmpty(t, exportArn)
	assert.Equal(t, tableArn, aws.ToString(exportOut.ExportDescription.TableArn))

	descOut, err := client.DescribeExport(t.Context(), &sdk.DescribeExportInput{
		ExportArn: aws.String(exportArn),
	})
	require.NoError(t, err)
	require.NotNil(t, descOut.ExportDescription)
	assert.Equal(t, exportArn, aws.ToString(descOut.ExportDescription.ExportArn))
	assert.Equal(t, tableArn, aws.ToString(descOut.ExportDescription.TableArn))

	listOut, err := client.ListExports(t.Context(), &sdk.ListExportsInput{
		TableArn: aws.String(tableArn),
	})
	require.NoError(t, err)
	require.Len(t, listOut.ExportSummaries, 1)
	assert.Equal(t, exportArn, aws.ToString(listOut.ExportSummaries[0].ExportArn))
}

func TestRestoreTableFromBackup_RoundTrip(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	t.Cleanup(backend.Close)
	client := newTestDynamoDBClient(t, dynamodb.NewHandler(backend))

	createPPRTableViaClient(t, client, "restore-src")

	_, err := client.PutItem(t.Context(), &sdk.PutItemInput{
		TableName: aws.String("restore-src"),
		Item: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "item1"},
		},
	})
	require.NoError(t, err)

	backupOut, err := client.CreateBackup(t.Context(), &sdk.CreateBackupInput{
		TableName:  aws.String("restore-src"),
		BackupName: aws.String("restore-src-backup"),
	})
	require.NoError(t, err)
	backupArn := aws.ToString(backupOut.BackupDetails.BackupArn)

	restoreOut, err := client.RestoreTableFromBackup(t.Context(), &sdk.RestoreTableFromBackupInput{
		BackupArn:       aws.String(backupArn),
		TargetTableName: aws.String("restore-dst"),
	})
	require.NoError(t, err)
	require.NotNil(t, restoreOut.TableDescription)
	assert.Equal(t, "restore-dst", aws.ToString(restoreOut.TableDescription.TableName))
	assert.Equal(t, types.TableStatusActive, restoreOut.TableDescription.TableStatus)

	got, err := client.GetItem(t.Context(), &sdk.GetItemInput{
		TableName: aws.String("restore-dst"),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "item1"},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, got.Item)
}

func TestRestoreTableToPointInTime_RoundTrip(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	t.Cleanup(backend.Close)
	client := newTestDynamoDBClient(t, dynamodb.NewHandler(backend))

	createPPRTableViaClient(t, client, "pitr-src")

	_, err := client.UpdateContinuousBackups(t.Context(), &sdk.UpdateContinuousBackupsInput{
		TableName: aws.String("pitr-src"),
		PointInTimeRecoverySpecification: &types.PointInTimeRecoverySpecification{
			PointInTimeRecoveryEnabled: aws.Bool(true),
		},
	})
	require.NoError(t, err)

	_, err = client.PutItem(t.Context(), &sdk.PutItemInput{
		TableName: aws.String("pitr-src"),
		Item: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "item1"},
		},
	})
	require.NoError(t, err)

	// Force a synchronous PITR snapshot rather than waiting on the janitor's
	// ~1-minute ticker (see pitr_test.go's identical use of SweepOnce).
	j := dynamodb.NewJanitor(backend, dynamodb.Settings{JanitorInterval: time.Hour})
	j.SweepOnce(t.Context())

	// A margin well past the snapshot avoids a false InvalidRestoreTimeException:
	// the wire format floors RestoreDateTime to millisecond precision (smithy-go's
	// FormatEpochSeconds), while the in-memory snapshot keeps full ns precision,
	// so a restore time only microseconds after the snapshot can round down to
	// before it.
	restoreOut, err := client.RestoreTableToPointInTime(t.Context(), &sdk.RestoreTableToPointInTimeInput{
		SourceTableName: aws.String("pitr-src"),
		TargetTableName: aws.String("pitr-dst"),
		RestoreDateTime: aws.Time(time.Now().Add(time.Second)),
	})
	require.NoError(t, err)
	require.NotNil(t, restoreOut.TableDescription)
	assert.Equal(t, "pitr-dst", aws.ToString(restoreOut.TableDescription.TableName))
	assert.Equal(t, types.TableStatusActive, restoreOut.TableDescription.TableStatus)

	got, err := client.GetItem(t.Context(), &sdk.GetItemInput{
		TableName: aws.String("pitr-dst"),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "item1"},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, got.Item)
}
