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
		"DeleteTieringConfiguration",
		"DescribeGlobalSettings",
		"DescribeProtectedResource",
		"DescribeRegionSettings",
		"DescribeReportJob",
		"DescribeRestoreJob",
		"DescribeScanJob",
		"DisassociateBackupVaultMpaApprovalTeam",
		"ExportBackupPlanTemplate",
		"GetBackupPlanFromJSON",
		"GetBackupPlanFromTemplate",
		"GetLegalHold",
		"GetRecoveryPointIndexDetails",
		"GetRestoreJobMetadata",
		"GetRestoreTestingInferredMetadata",
		"GetSupportedResourceTypes",
		"GetTieringConfiguration",
		"ListBackupJobSummaries",
		"ListBackupPlanTemplates",
		"ListBackupPlanVersions",
		"ListCopyJobSummaries",
		"ListIndexedRecoveryPoints",
		"ListLegalHolds",
		"ListProtectedResources",
		"ListProtectedResourcesByBackupVault",
		"ListRecoveryPointsByLegalHold",
		"ListRecoveryPointsByResource",
		"ListReportJobs",
		"ListRestoreAccessBackupVaults",
		"ListRestoreJobSummaries",
		"ListRestoreJobs",
		"ListRestoreJobsByProtectedResource",
		"ListScanJobSummaries",
		"ListScanJobs",
		"ListTieringConfigurations",
		"PutRestoreValidationResult",
		"RevokeRestoreAccessBackupVault",
		"StartCopyJob",
		"StartReportJob",
		"StartRestoreJob",
		"StartScanJob",
		"StopBackupJob",
		"UpdateGlobalSettings",
		"UpdateRecoveryPointIndexSettings",
		"UpdateRecoveryPointLifecycle",
		"UpdateRegionSettings",
		"UpdateTieringConfiguration",
	})
}
