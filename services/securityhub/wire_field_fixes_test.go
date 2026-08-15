package securityhub_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	securityhubsdk "github.com/aws/aws-sdk-go-v2/service/securityhub"
	securityhubtypes "github.com/aws/aws-sdk-go-v2/service/securityhub/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/securityhub"
)

// TestGetAdministratorAndMasterAccount_MemberStatus guards against
// GetAdministratorAccount/GetMasterAccount emitting the fabricated
// "RelationshipStatus" key: the real GetAdministratorAccountOutput.Administrator/
// GetMasterAccountOutput.Master are both *types.Invitation
// (securityhub@v1.75.4 api_op_GetAdministratorAccount.go /
// api_op_GetMasterAccount.go), whose status member is "MemberStatus" -- the
// same real field ListInvitations' Invitation model already names
// correctly, a sibling trap this handler had not yet picked up. Before the
// fix, a real client's typed .MemberStatus field was always empty
// regardless of backend state.
func TestGetAdministratorAndMasterAccount_MemberStatus(t *testing.T) {
	t.Parallel()

	backend := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestSecurityHubClient(t, securityhub.NewHandler(backend))

	_, err := client.AcceptAdministratorInvitation(t.Context(), &securityhubsdk.AcceptAdministratorInvitationInput{
		AdministratorId: aws.String("111111111111"),
		InvitationId:    aws.String("invitation-1"),
	})
	require.NoError(t, err)

	adminOut, err := client.GetAdministratorAccount(t.Context(), &securityhubsdk.GetAdministratorAccountInput{})
	require.NoError(t, err)
	require.NotNil(t, adminOut.Administrator)
	assert.Equal(t, "ENABLED", aws.ToString(adminOut.Administrator.MemberStatus))
	assert.Equal(t, "111111111111", aws.ToString(adminOut.Administrator.AccountId))

	//nolint:staticcheck // GetMasterAccount is deprecated by AWS in favor of GetAdministratorAccount but
	// is still a real, served op (opGetMasterAccount) this handler routes -- in this issue's L+D+G scope.
	masterOut, err := client.GetMasterAccount(t.Context(), &securityhubsdk.GetMasterAccountInput{})
	require.NoError(t, err)
	require.NotNil(t, masterOut.Master)
	assert.Equal(t, "ENABLED", aws.ToString(masterOut.Master.MemberStatus))
}

// TestListOrganizationAdminAccounts_FeatureEcho guards against
// ListOrganizationAdminAccounts dropping the real, always-echoed "Feature"
// response member (securityhub@v1.75.4 api_op_ListOrganizationAdminAccounts.go:
// "Defaults to Security Hub CSPM if not specified"). This backend doesn't
// track admin accounts per-feature, so the echo isn't filtered by it, only
// reflected back -- still a real gap, since a real client's typed .Feature
// field was previously always empty regardless of the request.
func TestListOrganizationAdminAccounts_FeatureEcho(t *testing.T) {
	t.Parallel()

	backend := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestSecurityHubClient(t, securityhub.NewHandler(backend))

	// Default (unset Feature) echoes the real default value.
	out, err := client.ListOrganizationAdminAccounts(
		t.Context(), &securityhubsdk.ListOrganizationAdminAccountsInput{},
	)
	require.NoError(t, err)
	assert.Equal(t, securityhubtypes.SecurityHubFeatureSecurityHub, out.Feature)

	// Explicit Feature echoes back what was sent.
	out, err = client.ListOrganizationAdminAccounts(t.Context(), &securityhubsdk.ListOrganizationAdminAccountsInput{
		Feature: securityhubtypes.SecurityHubFeatureSecurityHubV2,
	})
	require.NoError(t, err)
	assert.Equal(t, securityhubtypes.SecurityHubFeatureSecurityHubV2, out.Feature)
}

// TestListConnectorsV2_ProviderSummaryShape guards against ListConnectorsV2
// emitting a flat "Provider"/"ConnectorStatus" shape where the real
// types.ConnectorSummary (securityhub@v1.75.4 types.go:14833-14871) requires
// a nested, required ProviderSummary{ConnectorStatus,ProviderConfiguration,
// ProviderName} object -- a real client's typed .ProviderSummary field was
// always the zero value regardless of backend state. Mirrors the already-
// correct V1 CspmConnector sibling (ListConnectors' ProviderSummary, see
// connectors_v2_test.go's V1 "list" step).
func TestListConnectorsV2_ProviderSummaryShape(t *testing.T) {
	t.Parallel()

	backend := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestSecurityHubClient(t, securityhub.NewHandler(backend))

	_, err := client.CreateConnectorV2(t.Context(), &securityhubsdk.CreateConnectorV2Input{
		Name: aws.String("test-connector-v2"),
		Provider: &securityhubtypes.ProviderConfigurationMemberJiraCloud{
			Value: securityhubtypes.JiraCloudProviderConfiguration{
				ProjectKey: aws.String("SEC"),
			},
		},
	})
	require.NoError(t, err)

	listOut, err := client.ListConnectorsV2(t.Context(), &securityhubsdk.ListConnectorsV2Input{})
	require.NoError(t, err)
	require.Len(t, listOut.Connectors, 1)

	summary := listOut.Connectors[0]
	require.NotNil(t, summary.ProviderSummary, "ProviderSummary is required on the real ConnectorSummary shape")
	assert.Equal(t, "JIRACLOUD", string(summary.ProviderSummary.ProviderName))
	assert.Equal(t, "ACTIVE", string(summary.ProviderSummary.ConnectorStatus))
	assert.Equal(t, "test-connector-v2", aws.ToString(summary.Name))
}
