package backup_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	backupsdk "github.com/aws/aws-sdk-go-v2/service/backup"
	"github.com/aws/aws-sdk-go-v2/service/backup/types"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

// seedBackupAccessPointFixture creates a vault and a recovery point in it,
// returning the client to drive against and the seeded recovery point's ARN.
func seedBackupAccessPointFixture(t *testing.T) (*backup.InMemoryBackend, *backupsdk.Client, string) {
	t.Helper()

	b := newTestBackend(t)
	mustVault(t, b, "bap-vault")

	const rpArn = "arn:aws:backup:us-east-1:123456789012:recovery-point:bap-rp-1"
	require.NoError(t, b.AddRecoveryPoint("bap-vault", &backup.RecoveryPoint{
		RecoveryPointArn: rpArn,
		ResourceArn:      "arn:aws:s3:::bap-bucket",
		ResourceType:     "S3",
		Status:           "COMPLETED",
		CreationDate:     time.Now().UTC(),
	}))

	client := newTestBackupClient(t, backup.NewHandler(b))

	return b, client, rpArn
}

func TestRealClient_BackupAccessPointLifecycle(t *testing.T) {
	t.Parallel()

	b, client, rpArn := seedBackupAccessPointFixture(t)
	ctx := t.Context()

	createOut, err := client.CreateBackupAccessPoint(ctx, &backupsdk.CreateBackupAccessPointInput{
		Name:             aws.String("bap-1"),
		RecoveryPointArn: aws.String(rpArn),
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.AccessPointArn)
	assert.Equal(t, types.AccessPointStatusCreating, createOut.Status)

	accessPointArn := aws.ToString(createOut.AccessPointArn)

	descOut, err := client.DescribeBackupAccessPoint(ctx, &backupsdk.DescribeBackupAccessPointInput{
		AccessPointArn: aws.String(accessPointArn),
	})
	require.NoError(t, err)
	assert.Equal(t, "bap-1", aws.ToString(descOut.Name))
	assert.Equal(t, rpArn, aws.ToString(descOut.RecoveryPointArn))
	assert.Equal(t, "bap-vault", aws.ToString(descOut.BackupVaultName))
	assert.Equal(t, "arn:aws:s3:::bap-bucket", aws.ToString(descOut.ResourceArn))
	assert.Equal(t, "S3", aws.ToString(descOut.ResourceType))
	assert.Equal(t, types.AccessPointStatusCreating, descOut.Status)

	// Advance CREATING -> AVAILABLE via the janitor, the same deterministic
	// completion pattern advanceRestoreAccessVaults uses.
	backup.NewJanitor(b, time.Hour, time.Hour).SweepOnce(ctx)

	descOut, err = client.DescribeBackupAccessPoint(ctx, &backupsdk.DescribeBackupAccessPointInput{
		AccessPointArn: aws.String(accessPointArn),
	})
	require.NoError(t, err)
	assert.Equal(t, types.AccessPointStatusAvailable, descOut.Status)
	require.NotEmpty(t, descOut.AccessPointMetadata)
	assert.NotEmpty(t, descOut.AccessPointMetadata["S3AccessPointArn"])
	assert.NotEmpty(t, descOut.AccessPointMetadata["S3AccessPointAlias"])

	listOut, err := client.ListBackupAccessPoints(ctx, &backupsdk.ListBackupAccessPointsInput{})
	require.NoError(t, err)
	require.Len(t, listOut.BackupAccessPoints, 1)
	assert.Equal(t, accessPointArn, aws.ToString(listOut.BackupAccessPoints[0].AccessPointArn))
	assert.NotNil(t, listOut.BackupAccessPoints[0].AccessPointMetadata)

	byRPOut, err := client.ListBackupAccessPointsByRecoveryPoint(
		ctx, &backupsdk.ListBackupAccessPointsByRecoveryPointInput{RecoveryPointArn: aws.String(rpArn)},
	)
	require.NoError(t, err)
	require.Len(t, byRPOut.BackupAccessPoints, 1)
	assert.Equal(t, accessPointArn, aws.ToString(byRPOut.BackupAccessPoints[0].AccessPointArn))

	byResourceOut, err := client.ListBackupAccessPointsByResource(
		ctx,
		&backupsdk.ListBackupAccessPointsByResourceInput{
			ResourceArn: aws.String("arn:aws:s3:::bap-bucket"),
		},
	)
	require.NoError(t, err)
	require.Len(t, byResourceOut.BackupAccessPoints, 1)
	assert.Equal(t, accessPointArn, aws.ToString(byResourceOut.BackupAccessPoints[0].AccessPointArn))

	// A non-matching filter value excludes the record, proving the By* lists
	// actually filter rather than always returning every access point.
	emptyByRPOut, err := client.ListBackupAccessPointsByRecoveryPoint(
		ctx,
		&backupsdk.ListBackupAccessPointsByRecoveryPointInput{
			RecoveryPointArn: aws.String("arn:aws:backup:us-east-1:123456789012:recovery-point:other"),
		},
	)
	require.NoError(t, err)
	assert.Empty(t, emptyByRPOut.BackupAccessPoints)

	_, err = client.DeleteBackupAccessPoint(ctx, &backupsdk.DeleteBackupAccessPointInput{
		AccessPointArn: aws.String(accessPointArn),
	})
	require.NoError(t, err)

	_, err = client.DescribeBackupAccessPoint(ctx, &backupsdk.DescribeBackupAccessPointInput{
		AccessPointArn: aws.String(accessPointArn),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "ResourceNotFoundException", apiErr.ErrorCode())
}

func TestRealClient_CreateBackupAccessPoint_UnknownRecoveryPointNotFound(t *testing.T) {
	t.Parallel()

	_, client, _ := seedBackupAccessPointFixture(t)
	ctx := t.Context()

	_, err := client.CreateBackupAccessPoint(ctx, &backupsdk.CreateBackupAccessPointInput{
		Name:             aws.String("bap-missing-rp"),
		RecoveryPointArn: aws.String("arn:aws:backup:us-east-1:123456789012:recovery-point:does-not-exist"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "ResourceNotFoundException", apiErr.ErrorCode())
}
