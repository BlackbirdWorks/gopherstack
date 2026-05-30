package guardduty_test

import (
	"testing"

	guarddutysdk "github.com/aws/aws-sdk-go-v2/service/guardduty"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/guardduty"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// guardduty client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := guardduty.NewInMemoryBackend("000000000000", "us-east-1")
	h := guardduty.NewHandler(backend)

	// Operations not yet implemented — member management, org configuration,
	// malware scanning, publishing destinations, threat entity sets, and coverage.
	notImplemented := []string{
		"AcceptAdministratorInvitation",
		"AcceptInvitation",
		"CreateMalwareProtectionPlan",
		"CreateMembers",
		"CreatePublishingDestination",
		"CreateThreatEntitySet",
		"CreateTrustedEntitySet",
		"DeclineInvitations",
		"DeleteInvitations",
		"DeleteMalwareProtectionPlan",
		"DeleteMembers",
		"DeletePublishingDestination",
		"DeleteThreatEntitySet",
		"DeleteTrustedEntitySet",
		"DescribeMalwareScans",
		"DescribeOrganizationConfiguration",
		"DescribePublishingDestination",
		"DisableOrganizationAdminAccount",
		"DisassociateFromAdministratorAccount",
		"DisassociateFromMasterAccount",
		"DisassociateMembers",
		"EnableOrganizationAdminAccount",
		"GetAdministratorAccount",
		"GetCoverageStatistics",
		"GetInvitationsCount",
		"GetMalwareProtectionPlan",
		"GetMalwareScan",
		"GetMalwareScanSettings",
		"GetMasterAccount",
		"GetMemberDetectors",
		"GetMembers",
		"GetOrganizationStatistics",
		"GetRemainingFreeTrialDays",
		"GetThreatEntitySet",
		"GetTrustedEntitySet",
		"InviteMembers",
		"ListCoverage",
		"ListInvitations",
		"ListMalwareProtectionPlans",
		"ListMalwareScans",
		"ListMembers",
		"ListOrganizationAdminAccounts",
		"ListPublishingDestinations",
		"ListThreatEntitySets",
		"ListTrustedEntitySets",
		"SendObjectMalwareScan",
		"StartMalwareScan",
		"StartMonitoringMembers",
		"StopMonitoringMembers",
		"GetUsageStatistics",
		"UpdateMalwareProtectionPlan",
		"UpdateMalwareScanSettings",
		"UpdateMemberDetectors",
		"UpdateOrganizationConfiguration",
		"UpdatePublishingDestination",
		"UpdateThreatEntitySet",
		"UpdateTrustedEntitySet",
	}

	sdkcheck.CheckCompleteness(t, &guarddutysdk.Client{}, h.GetSupportedOperations(), notImplemented)
}
