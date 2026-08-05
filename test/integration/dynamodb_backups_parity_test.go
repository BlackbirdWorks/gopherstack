package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_DDB_BackupsParity drives CreateBackup, DescribeBackup,
// ListBackups, DeleteBackup and RestoreTableToPointInTime through the real
// aws-sdk-go-v2 DynamoDB client. It exists to catch wire-shape regressions
// (e.g. a timestamp field serialized as a JSON string where the real
// aws-sdk-go-v2 awsjson1_0 protocol requires a JSON number) that unit tests --
// which populate our own structs on both sides of the round trip -- cannot
// catch, since no wire type can ever be wrong when the client-side decode
// uses the same Go struct as the server-side encode.
func TestIntegration_DDB_BackupsParity(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createDynamoDBClient(t)
	ctx := t.Context()

	tableName := "BackupsParityTable-" + uuid.NewString()

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
	})
	require.NoError(t, err, "CreateTable should succeed")

	t.Cleanup(func() {
		// Not ctx: t.Context() is cancelled just before cleanups run.
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		_, _ = client.DeleteTable(cleanupCtx, &dynamodb.DeleteTableInput{
			TableName: aws.String(tableName),
		})
	})

	// Enable PITR so RestoreTableToPointInTime is exercised too.
	_, err = client.UpdateContinuousBackups(ctx, &dynamodb.UpdateContinuousBackupsInput{
		TableName: aws.String(tableName),
		PointInTimeRecoverySpecification: &types.PointInTimeRecoverySpecification{
			PointInTimeRecoveryEnabled: aws.Bool(true),
		},
	})
	require.NoError(t, err, "UpdateContinuousBackups should succeed")

	// CreateBackup: this is the operation the reported bug broke -- the real
	// SDK's deserializer rejects BackupCreationDateTime unless it decodes as
	// a JSON number.
	createOut, err := client.CreateBackup(ctx, &dynamodb.CreateBackupInput{
		TableName:  aws.String(tableName),
		BackupName: aws.String("integ-backup"),
	})
	require.NoError(t, err, "CreateBackup should succeed against the real SDK client")
	require.NotNil(t, createOut.BackupDetails)
	backupArn := aws.ToString(createOut.BackupDetails.BackupArn)
	require.NotEmpty(t, backupArn, "BackupArn should be a real, non-empty value")
	assert.Equal(t, "integ-backup", aws.ToString(createOut.BackupDetails.BackupName))
	assert.Equal(t, types.BackupStatusAvailable, createOut.BackupDetails.BackupStatus)
	require.NotNil(
		t,
		createOut.BackupDetails.BackupCreationDateTime,
		"BackupCreationDateTime should decode into a real timestamp",
	)
	assert.False(t, createOut.BackupDetails.BackupCreationDateTime.IsZero())

	// DescribeBackup.
	descOut, err := client.DescribeBackup(ctx, &dynamodb.DescribeBackupInput{
		BackupArn: aws.String(backupArn),
	})
	require.NoError(t, err, "DescribeBackup should succeed against the real SDK client")
	require.NotNil(t, descOut.BackupDescription)
	require.NotNil(t, descOut.BackupDescription.BackupDetails)
	assert.Equal(t, backupArn, aws.ToString(descOut.BackupDescription.BackupDetails.BackupArn))
	require.NotNil(t, descOut.BackupDescription.BackupDetails.BackupCreationDateTime)
	require.NotNil(t, descOut.BackupDescription.SourceTableDetails)
	assert.Equal(t, tableName, aws.ToString(descOut.BackupDescription.SourceTableDetails.TableName))

	// ListBackups.
	listOut, err := client.ListBackups(ctx, &dynamodb.ListBackupsInput{
		TableName: aws.String(tableName),
	})
	require.NoError(t, err, "ListBackups should succeed against the real SDK client")

	found := false

	for _, s := range listOut.BackupSummaries {
		if aws.ToString(s.BackupArn) == backupArn {
			found = true

			require.NotNil(
				t,
				s.BackupCreationDateTime,
				"BackupSummary.BackupCreationDateTime should decode into a real timestamp",
			)
			assert.False(t, s.BackupCreationDateTime.IsZero())
		}
	}

	assert.True(t, found, "created backup should appear in ListBackups")

	// RestoreTableToPointInTime: restores into a fresh table name, using
	// UseLatestRestorableTime so the test does not depend on the janitor's
	// 1-minute PITR snapshot cadence.
	restoredName := tableName + "-restored"

	restoreOut, err := client.RestoreTableToPointInTime(ctx, &dynamodb.RestoreTableToPointInTimeInput{
		SourceTableName:         aws.String(tableName),
		TargetTableName:         aws.String(restoredName),
		UseLatestRestorableTime: aws.Bool(true),
	})
	require.NoError(t, err, "RestoreTableToPointInTime should succeed against the real SDK client")
	require.NotNil(t, restoreOut.TableDescription)
	assert.Equal(t, restoredName, aws.ToString(restoreOut.TableDescription.TableName))
	assert.Equal(t, types.TableStatusActive, restoreOut.TableDescription.TableStatus)

	t.Cleanup(func() {
		// Not ctx: t.Context() is cancelled just before cleanups run.
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		_, _ = client.DeleteTable(cleanupCtx, &dynamodb.DeleteTableInput{
			TableName: aws.String(restoredName),
		})
	})

	// DeleteBackup.
	deleteOut, err := client.DeleteBackup(ctx, &dynamodb.DeleteBackupInput{
		BackupArn: aws.String(backupArn),
	})
	require.NoError(t, err, "DeleteBackup should succeed against the real SDK client")
	require.NotNil(t, deleteOut.BackupDescription)
	require.NotNil(t, deleteOut.BackupDescription.BackupDetails)
	assert.Equal(t, types.BackupStatusDeleted, deleteOut.BackupDescription.BackupDetails.BackupStatus)
	require.NotNil(t, deleteOut.BackupDescription.BackupDetails.BackupCreationDateTime)

	// DescribeBackup after delete: the backend removes the backup from its
	// store on delete, so it now reports not-found.
	_, err = client.DescribeBackup(ctx, &dynamodb.DescribeBackupInput{
		BackupArn: aws.String(backupArn),
	})
	require.Error(t, err, "DescribeBackup should fail after the backup was deleted")
}
