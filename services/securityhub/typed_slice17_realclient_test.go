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

// TestSlice17_SecurityHub_RealClient covers securityhub's last two typed-
// client-uncovered ops (gopherstack-n3zi slice 17): the deprecated
// AcceptInvitation and UpdateConnectorV2.
func TestSlice17_SecurityHub_RealClient(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T, client *securityhubsdk.Client)
		name string
	}{
		{testAcceptInvitationRealClient, "accept_invitation"},
		{testUpdateConnectorV2RealClient, "update_connector_v2"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := securityhub.NewHandler(securityhub.NewInMemoryBackend("000000000000", "us-east-1"))
			client := newTestSecurityHubClient(t, h)
			tc.run(t, client)
		})
	}
}

// testAcceptInvitationRealClient drives the deprecated AcceptInvitation op
// (an alias for AcceptAdministratorInvitation, invitations.go) and confirms
// the resulting administrator association is visible through GetMasterAccount.
func testAcceptInvitationRealClient(t *testing.T, client *securityhubsdk.Client) {
	t.Helper()
	ctx := t.Context()

	_, err := client.AcceptInvitation(ctx, &securityhubsdk.AcceptInvitationInput{
		MasterId:     aws.String("111111111111"),
		InvitationId: aws.String("invite-slice17"),
	})
	require.NoError(t, err)

	got, err := client.GetMasterAccount(ctx, &securityhubsdk.GetMasterAccountInput{})
	require.NoError(t, err)
	require.NotNil(t, got.Master)
	assert.Equal(t, "111111111111", aws.ToString(got.Master.AccountId))
	assert.Equal(t, "invite-slice17", aws.ToString(got.Master.InvitationId))
}

// testUpdateConnectorV2RealClient covers UpdateConnectorV2. Fixed a real
// bug found via this test: ConnectorV2 (models.go) had no EnablementStatus
// field at all, so UpdateConnectorV2Output.EnablementStatus (securityhub@
// v1.75.4 api_op_UpdateConnectorV2.go, deserializers.go:17924's case
// "EnablementStatus") always decoded as the zero value regardless of
// backend state -- same for CreateConnectorV2Output.EnablementStatus, fixed
// alongside since both share connectorV2ToResponse.
func testUpdateConnectorV2RealClient(t *testing.T, client *securityhubsdk.Client) {
	t.Helper()
	ctx := t.Context()

	created, err := client.CreateConnectorV2(ctx, &securityhubsdk.CreateConnectorV2Input{
		Name: aws.String("slice17-connector"),
		Provider: &securityhubtypes.ProviderConfigurationMemberJiraCloud{
			Value: securityhubtypes.JiraCloudProviderConfiguration{
				ProjectKey: aws.String("SEC"),
			},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, securityhubtypes.EnablementStatusEnabled, created.EnablementStatus)

	updated, err := client.UpdateConnectorV2(ctx, &securityhubsdk.UpdateConnectorV2Input{
		ConnectorId: created.ConnectorId,
		Description: aws.String("updated description"),
		Provider: &securityhubtypes.ProviderUpdateConfigurationMemberJiraCloud{
			Value: securityhubtypes.JiraCloudUpdateConfiguration{
				ProjectKey: aws.String("SEC2"),
			},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, securityhubtypes.EnablementStatusEnabled, updated.EnablementStatus)
	assert.NotEmpty(t, updated.ConnectorStatus)
}
