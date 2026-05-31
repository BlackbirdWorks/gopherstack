package securityhub_test

import (
	"testing"

	securityhubsdk "github.com/aws/aws-sdk-go-v2/service/securityhub"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/securityhub"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// securityhub client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
	h := securityhub.NewHandler(backend)

	// Operations not yet implemented — organization/member management,
	// finding aggregators, configuration policies, V2 endpoints,
	// invitation management, and advanced hub management.
	notImplemented := []string{
		"AcceptAdministratorInvitation",
		"AcceptInvitation",
		"BatchUpdateFindingsV2",
		"CreateAggregatorV2",
		"CreateAutomationRuleV2",
		"CreateConfigurationPolicy",
		"CreateConnectorV2",
		"CreateFindingAggregator",
		"CreateMembers",
		"CreateTicketV2",
		"DeclineInvitations",
		"DeleteAggregatorV2",
		"DeleteAutomationRuleV2",
		"DeleteConfigurationPolicy",
		"DeleteConnectorV2",
		"DeleteFindingAggregator",
		"DeleteInvitations",
		"DeleteMembers",
		"DescribeOrganizationConfiguration",
		"DescribeProductsV2",
		"DescribeSecurityHubV2",
		"DisableOrganizationAdminAccount",
		"DisableSecurityHubV2",
		"DisassociateFromAdministratorAccount",
		"DisassociateFromMasterAccount",
		"DisassociateMembers",
		"EnableOrganizationAdminAccount",
		"EnableSecurityHubV2",
		"GenerateRecommendedPolicyV2",
		"GetAdministratorAccount",
		"GetAggregatorV2",
		"GetAutomationRuleV2",
		"GetConfigurationPolicy",
		"GetConfigurationPolicyAssociation",
		"GetConnectorV2",
		"GetFindingAggregator",
		"GetFindingStatisticsV2",
		"GetFindingsTrendsV2",
		"GetFindingsV2",
		"GetInvitationsCount",
		"GetMasterAccount",
		"GetMembers",
		"GetRecommendedPolicyV2",
		"GetResourcesStatisticsV2",
		"GetResourcesTrendsV2",
		"GetResourcesV2",
		"InviteMembers",
		"ListAggregatorsV2",
		"ListAutomationRulesV2",
		"ListConfigurationPolicies",
		"ListConfigurationPolicyAssociations",
		"ListConnectorsV2",
		"ListFindingAggregators",
		"ListInvitations",
		"ListMembers",
		"ListOrganizationAdminAccounts",
		"RegisterConnectorV2",
		"StartConfigurationPolicyAssociation",
		"StartConfigurationPolicyDisassociation",
		"UpdateAggregatorV2",
		"UpdateAutomationRuleV2",
		"UpdateConfigurationPolicy",
		"UpdateConnectorV2",
		"UpdateFindingAggregator",
		"UpdateOrganizationConfiguration",
		"BatchGetConfigurationPolicyAssociations",
	}

	sdkcheck.CheckCompleteness(t, &securityhubsdk.Client{}, h.GetSupportedOperations(), notImplemented)
}
