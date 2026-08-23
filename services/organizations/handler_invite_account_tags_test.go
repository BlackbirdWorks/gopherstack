package organizations_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	organizationssdk "github.com/aws/aws-sdk-go-v2/service/organizations"
	organizationstypes "github.com/aws/aws-sdk-go-v2/service/organizations/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/organizations"
)

// Test_SDKRoundTrip_InviteAccountToOrganization_Tags proves
// InviteAccountToOrganizationInput.Tags reach the invited account once its
// handshake is accepted: gopherstack's decode struct previously dropped
// Tags entirely, so the account AcceptHandshake creates always had no tags
// no matter what the client sent at invite time.
func Test_SDKRoundTrip_InviteAccountToOrganization_Tags(t *testing.T) {
	t.Parallel()

	b := organizations.NewInMemoryBackend("123456789012", "us-east-1")
	_, _, err := b.CreateOrganization("ALL")
	require.NoError(t, err)

	h := organizations.NewHandler(b)
	client := newTestOrganizationsClient(t, h)
	ctx := t.Context()

	const invitedAccountID = "999999999999"

	inviteOut, err := client.InviteAccountToOrganization(ctx, &organizationssdk.InviteAccountToOrganizationInput{
		Target: &organizationstypes.HandshakeParty{
			Id:   aws.String(invitedAccountID),
			Type: organizationstypes.HandshakePartyTypeAccount,
		},
		Tags: []organizationstypes.Tag{
			{Key: aws.String("env"), Value: aws.String("prod")},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, inviteOut.Handshake)

	_, err = client.AcceptHandshake(ctx, &organizationssdk.AcceptHandshakeInput{
		HandshakeId: inviteOut.Handshake.Id,
	})
	require.NoError(t, err)

	tagsOut, err := client.ListTagsForResource(ctx, &organizationssdk.ListTagsForResourceInput{
		ResourceId: aws.String(invitedAccountID),
	})
	require.NoError(t, err)

	require.Len(t, tagsOut.Tags, 1,
		"InviteAccountToOrganizationInput.Tags must reach the account created on AcceptHandshake")
	require.Equal(t, "env", aws.ToString(tagsOut.Tags[0].Key))
	require.Equal(t, "prod", aws.ToString(tagsOut.Tags[0].Value))
}
