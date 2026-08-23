package backup_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	backupsdk "github.com/aws/aws-sdk-go-v2/service/backup"
	"github.com/aws/aws-sdk-go-v2/service/backup/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

// Test_UpdateRecoveryPointLifecycle_UnknownVaultIsResourceNotFound proves
// that errVaultNotFoundB1 (recovery_points.go) does not wrap the shared
// ErrNotFound sentinel: handleUpdateRecoveryPointLifecycle
// (handler_recovery_points.go) routes its error through h.handleError,
// which type-switches on errors.Is(err, ErrNotFound) and falls through to
// its default case -- 500 InternalFailure -- for any error that doesn't
// wrap ErrNotFound. AWS Backup uses 400 ResourceNotFoundException for
// every not-found case (see PARITY.md's "Backup does NOT use 404/409"
// note); a real client calling UpdateRecoveryPointLifecycle against an
// unknown vault must see a typed ResourceNotFoundException, not a generic
// 500.
func Test_UpdateRecoveryPointLifecycle_UnknownVaultIsResourceNotFound(t *testing.T) {
	t.Parallel()

	backend := backup.NewInMemoryBackend("000000000000", "us-east-1")
	h := backup.NewHandler(backend)
	client := newTestBackupClient(t, h)

	_, err := client.UpdateRecoveryPointLifecycle(
		t.Context(),
		&backupsdk.UpdateRecoveryPointLifecycleInput{
			BackupVaultName:  aws.String("no-such-vault"),
			RecoveryPointArn: aws.String("arn:aws:backup:us-east-1:000000000000:recovery-point:missing"),
			Lifecycle: &types.Lifecycle{
				MoveToColdStorageAfterDays: aws.Int64(30),
				DeleteAfterDays:            aws.Int64(365),
			},
		},
	)
	require.Error(t, err)

	var rnf *types.ResourceNotFoundException
	require.ErrorAs(t, err, &rnf,
		"expected a typed ResourceNotFoundException, got: %v", err)
}

// Test_UpdateRecoveryPointLifecycle_UnknownRecoveryPointIsResourceNotFound
// is the sibling case: a real vault but an unknown recovery point ARN.
// errRecoveryPointNotFound has the identical bug -- see the test above.
func Test_UpdateRecoveryPointLifecycle_UnknownRecoveryPointIsResourceNotFound(t *testing.T) {
	t.Parallel()

	backend := backup.NewInMemoryBackend("000000000000", "us-east-1")
	h := backup.NewHandler(backend)
	client := newTestBackupClient(t, h)

	_, err := client.CreateBackupVault(t.Context(), &backupsdk.CreateBackupVaultInput{
		BackupVaultName: aws.String("real-vault"),
	})
	require.NoError(t, err)

	_, err = client.UpdateRecoveryPointLifecycle(
		t.Context(),
		&backupsdk.UpdateRecoveryPointLifecycleInput{
			BackupVaultName:  aws.String("real-vault"),
			RecoveryPointArn: aws.String("arn:aws:backup:us-east-1:000000000000:recovery-point:missing"),
			Lifecycle: &types.Lifecycle{
				MoveToColdStorageAfterDays: aws.Int64(30),
				DeleteAfterDays:            aws.Int64(365),
			},
		},
	)
	require.Error(t, err)

	var rnf *types.ResourceNotFoundException
	require.ErrorAs(t, err, &rnf,
		"expected a typed ResourceNotFoundException, got: %v", err)
}
