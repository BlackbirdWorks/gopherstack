package backup_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteCases is the authoritative method+path for every real backup
// operation, extracted from backup@v1.59.4 serializers.go: each entry's
// "request.Method" and the string passed to httpbinding.SplitURI in that
// op's awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands in
// for any {Param} URI label -- the router does not validate ID shape, so the
// literal value doesn't matter here, only that the path matches Op.
// DisassociateBackupVaultMpaApprovalTeam's path carries the real bare
// "?delete" query flag AWS uses to distinguish it from
// AssociateBackupVaultMpaApprovalTeam at the same base path.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"AssociateBackupVaultMpaApprovalTeam", "PUT", "/backup-vaults/PLACEHOLDER/mpaApprovalTeam"},
		{"CancelLegalHold", "DELETE", "/legal-holds/PLACEHOLDER"},
		{"CreateBackupPlan", "PUT", "/backup/plans"},
		{"CreateBackupSelection", "PUT", "/backup/plans/PLACEHOLDER/selections"},
		{"CreateBackupVault", "PUT", "/backup-vaults/PLACEHOLDER"},
		{"CreateFramework", "POST", "/audit/frameworks"},
		{"CreateLegalHold", "POST", "/legal-holds"},
		{"CreateLogicallyAirGappedBackupVault", "PUT", "/logically-air-gapped-backup-vaults/PLACEHOLDER"},
		{"CreateReportPlan", "POST", "/audit/report-plans"},
		{"CreateRestoreAccessBackupVault", "PUT", "/restore-access-backup-vaults"},
		{"CreateRestoreTestingPlan", "PUT", "/restore-testing/plans"},
		{"CreateRestoreTestingSelection", "PUT", "/restore-testing/plans/PLACEHOLDER/selections"},
		{"CreateTieringConfiguration", "PUT", "/tiering-configurations"},
		{"DeleteBackupPlan", "DELETE", "/backup/plans/PLACEHOLDER"},
		{"DeleteBackupSelection", "DELETE", "/backup/plans/PLACEHOLDER/selections/PLACEHOLDER"},
		{"DeleteBackupVault", "DELETE", "/backup-vaults/PLACEHOLDER"},
		{"DeleteBackupVaultAccessPolicy", "DELETE", "/backup-vaults/PLACEHOLDER/access-policy"},
		{"DeleteBackupVaultLockConfiguration", "DELETE", "/backup-vaults/PLACEHOLDER/vault-lock"},
		{"DeleteBackupVaultNotifications", "DELETE", "/backup-vaults/PLACEHOLDER/notification-configuration"},
		{"DeleteFramework", "DELETE", "/audit/frameworks/PLACEHOLDER"},
		{"DeleteRecoveryPoint", "DELETE", "/backup-vaults/PLACEHOLDER/recovery-points/PLACEHOLDER"},
		{"DeleteReportPlan", "DELETE", "/audit/report-plans/PLACEHOLDER"},
		{"DeleteRestoreTestingPlan", "DELETE", "/restore-testing/plans/PLACEHOLDER"},
		{"DeleteRestoreTestingSelection", "DELETE", "/restore-testing/plans/PLACEHOLDER/selections/PLACEHOLDER"},
		{"DeleteTieringConfiguration", "DELETE", "/tiering-configurations/PLACEHOLDER"},
		{"DescribeBackupJob", "GET", "/backup-jobs/PLACEHOLDER"},
		{"DescribeBackupVault", "GET", "/backup-vaults/PLACEHOLDER"},
		{"DescribeCopyJob", "GET", "/copy-jobs/PLACEHOLDER"},
		{"DescribeFramework", "GET", "/audit/frameworks/PLACEHOLDER"},
		{"DescribeGlobalSettings", "GET", "/global-settings"},
		{"DescribeProtectedResource", "GET", "/resources/PLACEHOLDER"},
		{"DescribeRecoveryPoint", "GET", "/backup-vaults/PLACEHOLDER/recovery-points/PLACEHOLDER"},
		{"DescribeRegionSettings", "GET", "/account-settings"},
		{"DescribeReportJob", "GET", "/audit/report-jobs/PLACEHOLDER"},
		{"DescribeReportPlan", "GET", "/audit/report-plans/PLACEHOLDER"},
		{"DescribeRestoreJob", "GET", "/restore-jobs/PLACEHOLDER"},
		{"DescribeScanJob", "GET", "/scan/jobs/PLACEHOLDER"},
		{"DisassociateBackupVaultMpaApprovalTeam", "POST", "/backup-vaults/PLACEHOLDER/mpaApprovalTeam?delete"},
		{"DisassociateRecoveryPoint", "POST", "/backup-vaults/PLACEHOLDER/recovery-points/PLACEHOLDER/disassociate"},
		{
			"DisassociateRecoveryPointFromParent", "DELETE",
			"/backup-vaults/PLACEHOLDER/recovery-points/PLACEHOLDER/parentAssociation",
		},
		{"ExportBackupPlanTemplate", "GET", "/backup/plans/PLACEHOLDER/toTemplate"},
		{"GetBackupPlan", "GET", "/backup/plans/PLACEHOLDER"},
		{"GetBackupPlanFromJSON", "POST", "/backup/template/json/toPlan"},
		{"GetBackupPlanFromTemplate", "GET", "/backup/template/plans/PLACEHOLDER/toPlan"},
		{"GetBackupSelection", "GET", "/backup/plans/PLACEHOLDER/selections/PLACEHOLDER"},
		{"GetBackupVaultAccessPolicy", "GET", "/backup-vaults/PLACEHOLDER/access-policy"},
		{"GetBackupVaultNotifications", "GET", "/backup-vaults/PLACEHOLDER/notification-configuration"},
		{"GetLegalHold", "GET", "/legal-holds/PLACEHOLDER"},
		{"GetPITRMalwareScanResults", "GET", "/scan/pitr-malware-scan-results"},
		{"GetRecoveryPointIndexDetails", "GET", "/backup-vaults/PLACEHOLDER/recovery-points/PLACEHOLDER/index"},
		{
			"GetRecoveryPointRestoreMetadata", "GET",
			"/backup-vaults/PLACEHOLDER/recovery-points/PLACEHOLDER/restore-metadata",
		},
		{"GetRestoreJobMetadata", "GET", "/restore-jobs/PLACEHOLDER/metadata"},
		{"GetRestoreTestingInferredMetadata", "GET", "/restore-testing/inferred-metadata"},
		{"GetRestoreTestingPlan", "GET", "/restore-testing/plans/PLACEHOLDER"},
		{"GetRestoreTestingSelection", "GET", "/restore-testing/plans/PLACEHOLDER/selections/PLACEHOLDER"},
		{"GetSupportedResourceTypes", "GET", "/supported-resource-types"},
		{"GetTieringConfiguration", "GET", "/tiering-configurations/PLACEHOLDER"},
		{"ListBackupJobSummaries", "GET", "/audit/backup-job-summaries"},
		{"ListBackupJobs", "GET", "/backup-jobs"},
		{"ListBackupPlanTemplates", "GET", "/backup/template/plans"},
		{"ListBackupPlanVersions", "GET", "/backup/plans/PLACEHOLDER/versions"},
		{"ListBackupPlans", "GET", "/backup/plans"},
		{"ListBackupSelections", "GET", "/backup/plans/PLACEHOLDER/selections"},
		{"ListBackupVaults", "GET", "/backup-vaults"},
		{"ListCopyJobSummaries", "GET", "/audit/copy-job-summaries"},
		{"ListCopyJobs", "GET", "/copy-jobs"},
		{"ListFrameworks", "GET", "/audit/frameworks"},
		{"ListIndexedRecoveryPoints", "GET", "/indexes/recovery-point"},
		{"ListLegalHolds", "GET", "/legal-holds"},
		{"ListProtectedResources", "GET", "/resources"},
		{"ListProtectedResourcesByBackupVault", "GET", "/backup-vaults/PLACEHOLDER/resources"},
		{"ListRecoveryPointsByBackupVault", "GET", "/backup-vaults/PLACEHOLDER/recovery-points"},
		{"ListRecoveryPointsByLegalHold", "GET", "/legal-holds/PLACEHOLDER/recovery-points"},
		{"ListRecoveryPointsByResource", "GET", "/resources/PLACEHOLDER/recovery-points"},
		{"ListReportJobs", "GET", "/audit/report-jobs"},
		{"ListReportPlans", "GET", "/audit/report-plans"},
		{
			"ListRestoreAccessBackupVaults", "GET",
			"/logically-air-gapped-backup-vaults/PLACEHOLDER/restore-access-backup-vaults",
		},
		{"ListRestoreJobSummaries", "GET", "/audit/restore-job-summaries"},
		{"ListRestoreJobs", "GET", "/restore-jobs"},
		{"ListRestoreJobsByProtectedResource", "GET", "/resources/PLACEHOLDER/restore-jobs"},
		{"ListRestoreTestingPlans", "GET", "/restore-testing/plans"},
		{"ListRestoreTestingSelections", "GET", "/restore-testing/plans/PLACEHOLDER/selections"},
		{"ListScanJobSummaries", "GET", "/audit/scan-job-summaries"},
		{"ListScanJobs", "GET", "/scan/jobs"},
		{"ListTags", "GET", "/tags/PLACEHOLDER"},
		{"ListTieringConfigurations", "GET", "/tiering-configurations"},
		{"PutBackupVaultAccessPolicy", "PUT", "/backup-vaults/PLACEHOLDER/access-policy"},
		{"PutBackupVaultLockConfiguration", "PUT", "/backup-vaults/PLACEHOLDER/vault-lock"},
		{"PutBackupVaultNotifications", "PUT", "/backup-vaults/PLACEHOLDER/notification-configuration"},
		{"PutRestoreValidationResult", "PUT", "/restore-jobs/PLACEHOLDER/validations"},
		{
			"RevokeRestoreAccessBackupVault", "DELETE",
			"/logically-air-gapped-backup-vaults/PLACEHOLDER/restore-access-backup-vaults/PLACEHOLDER",
		},
		{"StartBackupJob", "PUT", "/backup-jobs"},
		{"StartCopyJob", "PUT", "/copy-jobs"},
		{"StartReportJob", "POST", "/audit/report-jobs/PLACEHOLDER"},
		{"StartRestoreJob", "PUT", "/restore-jobs"},
		{"StartScanJob", "PUT", "/scan/job"},
		{"StopBackupJob", "POST", "/backup-jobs/PLACEHOLDER"},
		{"TagResource", "POST", "/tags/PLACEHOLDER"},
		{"UntagResource", "POST", "/untag/PLACEHOLDER"},
		{"UpdateBackupPlan", "POST", "/backup/plans/PLACEHOLDER"},
		{"UpdateFramework", "PUT", "/audit/frameworks/PLACEHOLDER"},
		{"UpdateGlobalSettings", "PUT", "/global-settings"},
		{"UpdateRecoveryPointIndexSettings", "POST", "/backup-vaults/PLACEHOLDER/recovery-points/PLACEHOLDER/index"},
		{"UpdateRecoveryPointLifecycle", "POST", "/backup-vaults/PLACEHOLDER/recovery-points/PLACEHOLDER"},
		{"UpdateRegionSettings", "PUT", "/account-settings"},
		{"UpdateReportPlan", "PUT", "/audit/report-plans/PLACEHOLDER"},
		{"UpdateRestoreTestingPlan", "PUT", "/restore-testing/plans/PLACEHOLDER"},
		{"UpdateRestoreTestingSelection", "PUT", "/restore-testing/plans/PLACEHOLDER/selections/PLACEHOLDER"},
		{"UpdateTieringConfiguration", "PUT", "/tiering-configurations/PLACEHOLDER"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real backup op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts the route table resolves it to the right op, then drives the same
// request through the real Handler() and asserts it did not fall through to
// the "unknown operation: " ResourceNotFoundException that dispatch's final
// default case emits (handler_dispatch.go) -- backup shares parseBackupPath
// between ExtractOperation and dispatch, so this mainly guards against an op
// name that resolves correctly but has no matching case in the dispatch
// switch chain (gopherstack-ey26). gopherstack-jqh2.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	h := newTestBackupHandler()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			if got != tc.op {
				t.Errorf("method=%s path=%s: got op %q, want %q", tc.method, tc.path, got, tc.op)
			}

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "unknown operation: ",
				"method=%s path=%s op=%s: dispatched to the unmatched-route handler", tc.method, tc.path, tc.op)
		})
	}
}
