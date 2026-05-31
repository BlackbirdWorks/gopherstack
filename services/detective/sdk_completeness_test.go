package detective_test

import (
	"testing"

	detectivesdk "github.com/aws/aws-sdk-go-v2/service/detective"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/detective"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// detective client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := detective.NewInMemoryBackend("000000000000", "us-east-1")
	h := detective.NewHandler(backend)

	// Operations not yet implemented — invitation management, datasource packages,
	// investigations, organization admin management, and member monitoring.
	notImplemented := []string{
		"AcceptInvitation",
		"BatchGetGraphMemberDatasources",
		"BatchGetMembershipDatasources",
		"DescribeOrganizationConfiguration",
		"DisableOrganizationAdminAccount",
		"DisassociateMembership",
		"EnableOrganizationAdminAccount",
		"GetInvestigation",
		"ListDatasourcePackages",
		"ListIndicators",
		"ListInvestigations",
		"ListInvitations",
		"ListOrganizationAdminAccounts",
		"RejectInvitation",
		"StartInvestigation",
		"StartMonitoringMember",
		"UpdateDatasourcePackages",
		"UpdateInvestigationState",
		"UpdateOrganizationConfiguration",
	}

	sdkcheck.CheckCompleteness(t, &detectivesdk.Client{}, h.GetSupportedOperations(), notImplemented)
}
