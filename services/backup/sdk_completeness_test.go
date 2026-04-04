package backup_test

import (
	"testing"

	backupsdk "github.com/aws/aws-sdk-go-v2/service/backup"
	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/backup"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// backup client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := backup.NewInMemoryBackend("000000000000", "us-east-1")
	h := backup.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &backupsdk.Client{}, h.GetSupportedOperations(), []string{
		"CreateTieringConfiguration",
		"DeleteBackupSelection",
		"DeleteBackupVaultAccessPolicy",
		"DeleteBackupVaultLockConfiguration",
		"DeleteBackupVaultNotifications",
		"DeleteFramework",
		"DeleteRecoveryPoint",
		"DeleteReportPlan",
		"DeleteRestoreTestingPlan",
		"DeleteRestoreTestingSelection",
		"DeleteTieringConfiguration",
		"DescribeCopyJob",
		"DescribeFramework",
		"DescribeGlobalSettings",
		"DescribeProtectedResource",
		"DescribeRecoveryPoint",
		"DescribeRegionSettings",
		"DescribeReportJob",
		"DescribeReportPlan",
		"DescribeRestoreJob",
		"DescribeScanJob",
		"DisassociateBackupVaultMpaApprovalTeam",
		"DisassociateRecoveryPoint",
		"DisassociateRecoveryPointFromParent",
		"ExportBackupPlanTemplate",
		"GetBackupPlanFromJSON",
		"GetBackupPlanFromTemplate",
		"GetBackupSelection",
		"GetBackupVaultAccessPolicy",
		"GetBackupVaultNotifications",
		"GetLegalHold",
		"GetRecoveryPointIndexDetails",
		"GetRecoveryPointRestoreMetadata",
		"GetRestoreJobMetadata",
		"GetRestoreTestingInferredMetadata",
		"GetRestoreTestingPlan",
		"GetRestoreTestingSelection",
		"GetSupportedResourceTypes",
		"GetTieringConfiguration",
		"ListBackupJobSummaries",
		"ListBackupPlanTemplates",
		"ListBackupPlanVersions",
		"ListBackupSelections",
		"ListCopyJobSummaries",
		"ListCopyJobs",
		"ListFrameworks",
		"ListIndexedRecoveryPoints",
		"ListLegalHolds",
		"ListProtectedResources",
		"ListProtectedResourcesByBackupVault",
		"ListRecoveryPointsByBackupVault",
		"ListRecoveryPointsByLegalHold",
		"ListRecoveryPointsByResource",
		"ListReportJobs",
		"ListReportPlans",
		"ListRestoreAccessBackupVaults",
		"ListRestoreJobSummaries",
		"ListRestoreJobs",
		"ListRestoreJobsByProtectedResource",
		"ListRestoreTestingPlans",
		"ListRestoreTestingSelections",
		"ListScanJobSummaries",
		"ListScanJobs",
		"ListTieringConfigurations",
		"PutBackupVaultAccessPolicy",
		"PutBackupVaultLockConfiguration",
		"PutBackupVaultNotifications",
		"PutRestoreValidationResult",
		"RevokeRestoreAccessBackupVault",
		"StartCopyJob",
		"StartReportJob",
		"StartRestoreJob",
		"StartScanJob",
		"StopBackupJob",
		"UpdateFramework",
		"UpdateGlobalSettings",
		"UpdateRecoveryPointIndexSettings",
		"UpdateRecoveryPointLifecycle",
		"UpdateRegionSettings",
		"UpdateReportPlan",
		"UpdateRestoreTestingPlan",
		"UpdateRestoreTestingSelection",
		"UpdateTieringConfiguration",
	})
}
