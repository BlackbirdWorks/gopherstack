package macie2_test

import (
	"testing"

	macie2sdk "github.com/aws/aws-sdk-go-v2/service/macie2"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/macie2"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// macie2 client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := macie2.NewInMemoryBackend("000000000000", "us-east-1")
	h := macie2.NewHandler(backend)

	// Operations not yet implemented — multi-account/org, S3 resource scanning,
	// automated discovery, classification jobs, reveal configuration, and usage stats.
	notImplemented := []string{
		"AcceptInvitation",
		"BatchGetCustomDataIdentifiers",
		"BatchUpdateAutomatedDiscoveryAccounts",
		"CreateClassificationJob",
		"CreateInvitations",
		"CreateMember",
		"DeclineInvitations",
		"DeleteInvitations",
		"DeleteMember",
		"DescribeBuckets",
		"DescribeClassificationJob",
		"DescribeOrganizationConfiguration",
		"DisableOrganizationAdminAccount",
		"DisassociateFromAdministratorAccount",
		"DisassociateFromMasterAccount",
		"DisassociateMember",
		"EnableOrganizationAdminAccount",
		"GetAdministratorAccount",
		"GetAutomatedDiscoveryConfiguration",
		"GetBucketStatistics",
		"GetClassificationExportConfiguration",
		"GetClassificationScope",
		"GetInvitationsCount",
		"GetMasterAccount",
		"GetMember",
		"GetResourceProfile",
		"GetRevealConfiguration",
		"GetSensitiveDataOccurrences",
		"GetSensitiveDataOccurrencesAvailability",
		"GetSensitivityInspectionTemplate",
		"GetUsageStatistics",
		"GetUsageTotals",
		"ListAutomatedDiscoveryAccounts",
		"ListClassificationJobs",
		"ListClassificationScopes",
		"ListInvitations",
		"ListManagedDataIdentifiers",
		"ListMembers",
		"ListOrganizationAdminAccounts",
		"ListResourceProfileArtifacts",
		"ListResourceProfileDetections",
		"ListSensitivityInspectionTemplates",
		"GetFindingsPublicationConfiguration",
		"PutClassificationExportConfiguration",
		"PutFindingsPublicationConfiguration",
		"SearchResources",
		"UpdateAutomatedDiscoveryConfiguration",
		"UpdateClassificationJob",
		"UpdateClassificationScope",
		"UpdateMemberSession",
		"UpdateOrganizationConfiguration",
		"UpdateResourceProfile",
		"UpdateResourceProfileDetections",
		"UpdateRevealConfiguration",
		"UpdateSensitivityInspectionTemplate",
	}

	sdkcheck.CheckCompleteness(t, &macie2sdk.Client{}, h.GetSupportedOperations(), notImplemented)
}
