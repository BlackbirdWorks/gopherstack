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

// Test_SDKRoundTrip_GetRestoreTestingPlan_RecoveryPointSelection proves
// gopherstack-r80d batch 11's finding: RestoreTestingPlanForGet.RecoveryPointSelection
// is marked "This member is required." (types.go:2307-2371, backup@v1.59.4)
// and the real SDK client's own validateRestoreTestingPlanForCreate
// (validators.go:2400-2419) rejects a CreateRestoreTestingPlan call that
// doesn't supply one -- so every real client's created plan has one -- but
// gopherstack had no struct field for RecoveryPointSelection at all before
// this fix, silently discarding it on Create and leaving GetRestoreTestingPlan
// permanently unable to return the required member.
func Test_SDKRoundTrip_GetRestoreTestingPlan_RecoveryPointSelection(t *testing.T) {
	t.Parallel()

	backend := backup.NewInMemoryBackend("000000000000", "us-east-1")
	h := backup.NewHandler(backend)
	client := newTestBackupClient(t, h)

	_, err := client.CreateRestoreTestingPlan(t.Context(), &backupsdk.CreateRestoreTestingPlanInput{
		RestoreTestingPlan: &types.RestoreTestingPlanForCreate{
			RestoreTestingPlanName: aws.String("rtp-r80d"),
			ScheduleExpression:     aws.String("cron(0 5 ? * * *)"),
			RecoveryPointSelection: &types.RestoreTestingRecoveryPointSelection{
				Algorithm: types.RestoreTestingRecoveryPointSelectionAlgorithmLatestWithinWindow,
				RecoveryPointTypes: []types.RestoreTestingRecoveryPointType{
					types.RestoreTestingRecoveryPointTypeSnapshot,
				},
				IncludeVaults: []string{"*"},
			},
		},
	})
	require.NoError(t, err)

	out, err := client.GetRestoreTestingPlan(t.Context(), &backupsdk.GetRestoreTestingPlanInput{
		RestoreTestingPlanName: aws.String("rtp-r80d"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.RestoreTestingPlan)
	require.NotNil(t, out.RestoreTestingPlan.RecoveryPointSelection,
		"required member RecoveryPointSelection must be present")
	require.Equal(
		t,
		types.RestoreTestingRecoveryPointSelectionAlgorithmLatestWithinWindow,
		out.RestoreTestingPlan.RecoveryPointSelection.Algorithm,
	)
	require.Equal(t, []string{"*"}, out.RestoreTestingPlan.RecoveryPointSelection.IncludeVaults)
	require.Equal(
		t,
		[]types.RestoreTestingRecoveryPointType{types.RestoreTestingRecoveryPointTypeSnapshot},
		out.RestoreTestingPlan.RecoveryPointSelection.RecoveryPointTypes,
	)
}

// Test_SDKRoundTrip_GetRestoreTestingPlan_RecoveryPointSelection_EmptyReachable
// proves the reachable-but-empty edge: RestoreTestingRecoveryPointSelection's
// own members (Algorithm/IncludeVaults/RecoveryPointTypes) are documented as
// "required" only in prose, not enforced by the SDK's client-side validator
// (validators.go has no validateRestoreTestingRecoveryPointSelection at
// all) -- so a real client can send an empty selection object, and the
// required RecoveryPointSelection *key* must still come back present (even
// though genuinely empty), not omitted.
func Test_SDKRoundTrip_GetRestoreTestingPlan_RecoveryPointSelection_EmptyReachable(t *testing.T) {
	t.Parallel()

	backend := backup.NewInMemoryBackend("000000000000", "us-east-1")
	h := backup.NewHandler(backend)
	client := newTestBackupClient(t, h)

	_, err := client.CreateRestoreTestingPlan(t.Context(), &backupsdk.CreateRestoreTestingPlanInput{
		RestoreTestingPlan: &types.RestoreTestingPlanForCreate{
			RestoreTestingPlanName: aws.String("rtp-r80d-bare"),
			ScheduleExpression:     aws.String("cron(0 5 ? * * *)"),
			RecoveryPointSelection: &types.RestoreTestingRecoveryPointSelection{},
		},
	})
	require.NoError(t, err)

	out, err := client.GetRestoreTestingPlan(t.Context(), &backupsdk.GetRestoreTestingPlanInput{
		RestoreTestingPlanName: aws.String("rtp-r80d-bare"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.RestoreTestingPlan.RecoveryPointSelection,
		"required member RecoveryPointSelection must be present even when empty")
}

// Test_SDKRoundTrip_DescribeScanJob_RequiredMembers proves
// gopherstack-r80d batch 11's central backup finding: dispatchReportJobOps's
// opDescribeScanJob/opListScanJobs cases (handler_report_plans.go) returned
// only ScanJobId/Status, silently dropping 12+ of DescribeScanJobOutput's 15
// required members (api_op_DescribeScanJob.go:39-146, backup@v1.59.4) even
// though the backend already tracked most of them on ScanJob -- PARITY.md
// had recorded this op as "wire: ok" (only checking an unrelated fabricated-
// 200-status bug), which is exactly the wrong-verdict class this campaign
// exists to catch. AccountId/BackupVaultName/ResourceArn/ResourceType/
// ResourceName had no backing field at all and are now derived honestly
// (AccountId from the backend's own account, BackupVaultName from the
// StartScanJob request, ResourceArn/ResourceType from the recovery point
// input.RecoveryPointArn identifies, ResourceName from that ARN's trailing
// segment) rather than fabricated.
func Test_SDKRoundTrip_DescribeScanJob_RequiredMembers(t *testing.T) {
	t.Parallel()

	backend := backup.NewInMemoryBackend("000000000000", "us-east-1")
	h := backup.NewHandler(backend)
	client := newTestBackupClient(t, h)

	_, err := client.CreateBackupVault(t.Context(), &backupsdk.CreateBackupVaultInput{
		BackupVaultName: aws.String("scan-vault-r80d"),
	})
	require.NoError(t, err)

	rpArn := "arn:aws:ec2:us-east-1:000000000000:recovery-point:rp-r80d"
	require.NoError(t, backend.AddRecoveryPoint("scan-vault-r80d", &backup.RecoveryPoint{
		RecoveryPointArn: rpArn,
		BackupVaultName:  "scan-vault-r80d",
		ResourceArn:      "arn:aws:ec2:us-east-1:000000000000:volume/vol-0abc123",
		ResourceType:     "EBS",
		Status:           "COMPLETED",
		CreationDate:     time.Now().UTC(),
	}))

	startOut, err := client.StartScanJob(t.Context(), &backupsdk.StartScanJobInput{
		BackupVaultName:  aws.String("scan-vault-r80d"),
		IamRoleArn:       aws.String("arn:aws:iam::000000000000:role/ScanRole"),
		MalwareScanner:   types.MalwareScannerGuardduty,
		RecoveryPointArn: aws.String(rpArn),
		ScanMode:         types.ScanModeFullScan,
		ScannerRoleArn:   aws.String("arn:aws:iam::000000000000:role/ScannerRole"),
	})
	require.NoError(t, err)

	out, err := client.DescribeScanJob(t.Context(), &backupsdk.DescribeScanJobInput{
		ScanJobId: startOut.ScanJobId,
	})
	require.NoError(t, err)

	require.Equal(t, "000000000000", aws.ToString(out.AccountId), "required member AccountId")
	require.NotEmpty(t, aws.ToString(out.BackupVaultArn), "required member BackupVaultArn")
	require.Equal(t, "scan-vault-r80d", aws.ToString(out.BackupVaultName), "required member BackupVaultName")
	require.NotNil(t, out.CreationDate, "required member CreationDate")
	require.Equal(
		t,
		"arn:aws:iam::000000000000:role/ScanRole",
		aws.ToString(out.IamRoleArn),
		"required member IamRoleArn",
	)
	require.Equal(t, types.MalwareScannerGuardduty, out.MalwareScanner, "required member MalwareScanner")
	require.Equal(t, rpArn, aws.ToString(out.RecoveryPointArn), "required member RecoveryPointArn")
	require.Equal(
		t,
		"arn:aws:ec2:us-east-1:000000000000:volume/vol-0abc123",
		aws.ToString(out.ResourceArn),
		"required member ResourceArn",
	)
	require.Equal(t, "vol-0abc123", aws.ToString(out.ResourceName), "required member ResourceName")
	require.Equal(t, types.ScanResourceType("EBS"), out.ResourceType, "required member ResourceType")
	require.Equal(t, startOut.ScanJobId, out.ScanJobId, "required member ScanJobId")
	require.Equal(t, types.ScanModeFullScan, out.ScanMode, "required member ScanMode")
	require.Equal(
		t,
		"arn:aws:iam::000000000000:role/ScannerRole",
		aws.ToString(out.ScannerRoleArn),
		"required member ScannerRoleArn",
	)
	require.Equal(t, types.ScanStateCompleted, out.State, "required member State")
}
