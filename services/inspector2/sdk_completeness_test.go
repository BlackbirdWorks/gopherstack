package inspector2_test

import (
	"testing"

	inspector2sdk "github.com/aws/aws-sdk-go-v2/service/inspector2"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/inspector2"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// inspector2 client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := inspector2.NewInMemoryBackend("000000000000", "us-east-1")
	h := inspector2.NewHandler(backend)

	// Operations not yet implemented — member management, org configuration,
	// CIS scanning, code security, SBOM export, findings reports, coverage,
	// deep inspection, encryption keys, and usage/search operations.
	notImplemented := []string{
		"AssociateMember",
		"BatchAssociateCodeSecurityScanConfiguration",
		"BatchDisassociateCodeSecurityScanConfiguration",
		"BatchGetCodeSnippet",
		"BatchGetFindingDetails",
		"BatchGetFreeTrialInfo",
		"BatchGetMemberEc2DeepInspectionStatus",
		"BatchUpdateMemberEc2DeepInspectionStatus",
		"CancelFindingsReport",
		"CancelSbomExport",
		"CreateCisScanConfiguration",
		"CreateCodeSecurityIntegration",
		"CreateCodeSecurityScanConfiguration",
		"CreateFindingsReport",
		"CreateSbomExport",
		"DeleteCisScanConfiguration",
		"DeleteCodeSecurityIntegration",
		"DeleteCodeSecurityScanConfiguration",
		"DescribeOrganizationConfiguration",
		"DisableDelegatedAdminAccount",
		"DisassociateMember",
		"EnableDelegatedAdminAccount",
		"GetCisScanReport",
		"GetCisScanResultDetails",
		"GetClustersForImage",
		"GetCodeSecurityIntegration",
		"GetCodeSecurityScan",
		"GetCodeSecurityScanConfiguration",
		"GetDelegatedAdminAccount",
		"GetEc2DeepInspectionConfiguration",
		"GetEncryptionKey",
		"GetFindingsReportStatus",
		"GetMember",
		"GetSbomExport",
		"ListAccountPermissions",
		"ListCisScanConfigurations",
		"ListCisScanResultsAggregatedByChecks",
		"ListCisScanResultsAggregatedByTargetResource",
		"ListCisScans",
		"ListCodeSecurityIntegrations",
		"ListCodeSecurityScanConfigurationAssociations",
		"ListCodeSecurityScanConfigurations",
		"ListCoverage",
		"ListCoverageStatistics",
		"ListDelegatedAdminAccounts",
		"ListFindingAggregations",
		"ListMembers",
		"ListUsageTotals",
		"ResetEncryptionKey",
		"SearchVulnerabilities",
		"SendCisSessionHealth",
		"SendCisSessionTelemetry",
		"StartCisSession",
		"StartCodeSecurityScan",
		"StopCisSession",
		"UpdateCisScanConfiguration",
		"UpdateCodeSecurityIntegration",
		"UpdateCodeSecurityScanConfiguration",
		"UpdateEc2DeepInspectionConfiguration",
		"UpdateEncryptionKey",
		"UpdateOrganizationConfiguration",
		"UpdateOrgEc2DeepInspectionConfiguration",
	}

	sdkcheck.CheckCompleteness(t, &inspector2sdk.Client{}, h.GetSupportedOperations(), notImplemented)
}
