package backup_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	backupsdk "github.com/aws/aws-sdk-go-v2/service/backup"
	"github.com/aws/aws-sdk-go-v2/service/backup/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

// Test_SDKRoundTrip_UpdateRestoreTesting_UpdateTime proves
// UpdateRestoreTestingPlanOutput.UpdateTime and
// UpdateRestoreTestingSelectionOutput.UpdateTime -- both "This member is
// required." (api_op_UpdateRestoreTestingPlan.go:74-77,
// api_op_UpdateRestoreTestingSelection.go:80-83, backup@v1.64.0) -- are
// actually populated. Before this fix, RestoreTestingPlan/
// RestoreTestingSelection had no UpdateTime field at all, so both handlers
// silently omitted the required member on every Update call.
func Test_SDKRoundTrip_UpdateRestoreTesting_UpdateTime(t *testing.T) {
	t.Parallel()

	t.Run("plan", func(t *testing.T) {
		t.Parallel()

		backend := backup.NewInMemoryBackend("000000000000", "us-east-1")
		h := backup.NewHandler(backend)
		client := newTestBackupClient(t, h)

		_, err := client.CreateRestoreTestingPlan(t.Context(), &backupsdk.CreateRestoreTestingPlanInput{
			RestoreTestingPlan: &types.RestoreTestingPlanForCreate{
				RestoreTestingPlanName: aws.String("rtp-update-time"),
				ScheduleExpression:     aws.String("cron(0 5 ? * * *)"),
				RecoveryPointSelection: &types.RestoreTestingRecoveryPointSelection{
					Algorithm:     types.RestoreTestingRecoveryPointSelectionAlgorithmLatestWithinWindow,
					IncludeVaults: []string{"*"},
				},
			},
		})
		require.NoError(t, err)

		out, err := client.UpdateRestoreTestingPlan(t.Context(), &backupsdk.UpdateRestoreTestingPlanInput{
			RestoreTestingPlanName: aws.String("rtp-update-time"),
			RestoreTestingPlan: &types.RestoreTestingPlanForUpdate{
				ScheduleExpression: aws.String("cron(0 6 ? * * *)"),
			},
		})
		require.NoError(t, err)
		require.NotNil(t, out.UpdateTime, "required member UpdateTime must be present")
	})

	t.Run("selection", func(t *testing.T) {
		t.Parallel()

		backend := backup.NewInMemoryBackend("000000000000", "us-east-1")
		h := backup.NewHandler(backend)
		client := newTestBackupClient(t, h)

		_, err := client.CreateRestoreTestingPlan(t.Context(), &backupsdk.CreateRestoreTestingPlanInput{
			RestoreTestingPlan: &types.RestoreTestingPlanForCreate{
				RestoreTestingPlanName: aws.String("rtp-sel-update-time"),
				ScheduleExpression:     aws.String("cron(0 5 ? * * *)"),
				RecoveryPointSelection: &types.RestoreTestingRecoveryPointSelection{
					Algorithm:     types.RestoreTestingRecoveryPointSelectionAlgorithmLatestWithinWindow,
					IncludeVaults: []string{"*"},
				},
			},
		})
		require.NoError(t, err)

		_, err = client.CreateRestoreTestingSelection(t.Context(), &backupsdk.CreateRestoreTestingSelectionInput{
			RestoreTestingPlanName: aws.String("rtp-sel-update-time"),
			RestoreTestingSelection: &types.RestoreTestingSelectionForCreate{
				RestoreTestingSelectionName: aws.String("sel1"),
				ProtectedResourceType:       aws.String("EC2"),
				IamRoleArn:                  aws.String("arn:aws:iam::000000000000:role/restore-test"),
			},
		})
		require.NoError(t, err)

		out, err := client.UpdateRestoreTestingSelection(t.Context(), &backupsdk.UpdateRestoreTestingSelectionInput{
			RestoreTestingPlanName:      aws.String("rtp-sel-update-time"),
			RestoreTestingSelectionName: aws.String("sel1"),
			RestoreTestingSelection: &types.RestoreTestingSelectionForUpdate{
				ValidationWindowHours: 12,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, out.UpdateTime, "required member UpdateTime must be present")
	})
}

// Test_SDKRoundTrip_RestoreTestingSelection_IamRoleArn proves
// RestoreTestingSelectionForGet.IamRoleArn and
// RestoreTestingSelectionForList.IamRoleArn -- both "This member is
// required." (types.go) -- stay present after an Update that omits
// IamRoleArn (RestoreTestingSelectionForUpdate.IamRoleArn is optional).
// Before this fix the Get/List rendering used a conditional
// setOptionalStr, so once an Update cleared IAMRoleArn to "" the required
// member vanished from the wire instead of being emitted present-and-empty.
func Test_SDKRoundTrip_RestoreTestingSelection_IamRoleArn(t *testing.T) {
	t.Parallel()

	backend := backup.NewInMemoryBackend("000000000000", "us-east-1")
	h := backup.NewHandler(backend)
	client := newTestBackupClient(t, h)

	_, err := client.CreateRestoreTestingPlan(t.Context(), &backupsdk.CreateRestoreTestingPlanInput{
		RestoreTestingPlan: &types.RestoreTestingPlanForCreate{
			RestoreTestingPlanName: aws.String("rtp-iamrolearn"),
			ScheduleExpression:     aws.String("cron(0 5 ? * * *)"),
			RecoveryPointSelection: &types.RestoreTestingRecoveryPointSelection{
				Algorithm:     types.RestoreTestingRecoveryPointSelectionAlgorithmLatestWithinWindow,
				IncludeVaults: []string{"*"},
			},
		},
	})
	require.NoError(t, err)

	_, err = client.CreateRestoreTestingSelection(t.Context(), &backupsdk.CreateRestoreTestingSelectionInput{
		RestoreTestingPlanName: aws.String("rtp-iamrolearn"),
		RestoreTestingSelection: &types.RestoreTestingSelectionForCreate{
			RestoreTestingSelectionName: aws.String("sel1"),
			ProtectedResourceType:       aws.String("EC2"),
			IamRoleArn:                  aws.String("arn:aws:iam::000000000000:role/restore-test"),
		},
	})
	require.NoError(t, err)

	// Update omitting IamRoleArn -- legal per RestoreTestingSelectionForUpdate,
	// which has no required members beyond identity.
	_, err = client.UpdateRestoreTestingSelection(t.Context(), &backupsdk.UpdateRestoreTestingSelectionInput{
		RestoreTestingPlanName:      aws.String("rtp-iamrolearn"),
		RestoreTestingSelectionName: aws.String("sel1"),
		RestoreTestingSelection: &types.RestoreTestingSelectionForUpdate{
			ValidationWindowHours: 24,
		},
	})
	require.NoError(t, err)

	getOut, err := client.GetRestoreTestingSelection(t.Context(), &backupsdk.GetRestoreTestingSelectionInput{
		RestoreTestingPlanName:      aws.String("rtp-iamrolearn"),
		RestoreTestingSelectionName: aws.String("sel1"),
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.RestoreTestingSelection)
	require.NotNil(t, getOut.RestoreTestingSelection.IamRoleArn,
		"required member IamRoleArn must be present even when empty")

	listOut, err := client.ListRestoreTestingSelections(t.Context(), &backupsdk.ListRestoreTestingSelectionsInput{
		RestoreTestingPlanName: aws.String("rtp-iamrolearn"),
	})
	require.NoError(t, err)
	require.Len(t, listOut.RestoreTestingSelections, 1)
	require.NotNil(t, listOut.RestoreTestingSelections[0].IamRoleArn,
		"required member IamRoleArn must be present even when empty")
}

// Test_SDKRoundTrip_GetRestoreTestingInferredMetadata proves
// GetRestoreTestingInferredMetadataOutput.InferredMetadata -- "This member
// is required." -- is derived from real backend state (the recovery
// point's tracked ResourceType) rather than always returning an empty map
// regardless of input, and that an unknown vault/recovery point is
// rejected rather than silently 200'd.
func Test_SDKRoundTrip_GetRestoreTestingInferredMetadata(t *testing.T) {
	t.Parallel()

	backend := backup.NewInMemoryBackend("000000000000", "us-east-1")
	h := backup.NewHandler(backend)
	client := newTestBackupClient(t, h)

	_, err := client.CreateBackupVault(t.Context(), &backupsdk.CreateBackupVaultInput{
		BackupVaultName: aws.String("vault-inferred"),
	})
	require.NoError(t, err)

	rpArn := "arn:aws:ec2:us-east-1:000000000000:recovery-point:rp-inferred"
	require.NoError(t, backend.AddRecoveryPoint("vault-inferred", &backup.RecoveryPoint{
		RecoveryPointArn: rpArn,
		BackupVaultName:  "vault-inferred",
		ResourceArn:      "arn:aws:ec2:us-east-1:000000000000:instance/i-1",
		ResourceType:     "EC2",
		Status:           "COMPLETED",
		CreationDate:     time.Now().UTC(),
	}))

	out, err := client.GetRestoreTestingInferredMetadata(t.Context(), &backupsdk.GetRestoreTestingInferredMetadataInput{
		BackupVaultName:  aws.String("vault-inferred"),
		RecoveryPointArn: aws.String(rpArn),
	})
	require.NoError(t, err)
	require.NotNil(t, out.InferredMetadata, "required member InferredMetadata must be present")
	require.Equal(t, "EC2", out.InferredMetadata["ResourceType"])

	_, err = client.GetRestoreTestingInferredMetadata(t.Context(), &backupsdk.GetRestoreTestingInferredMetadataInput{
		BackupVaultName:  aws.String("vault-inferred"),
		RecoveryPointArn: aws.String("arn:aws:backup:us-east-1:000000000000:recovery-point:does-not-exist"),
	})
	require.Error(t, err, "an unknown recovery point must not 200")
}
