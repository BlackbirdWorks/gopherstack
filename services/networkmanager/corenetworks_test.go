package networkmanager_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	networkmanagersdk "github.com/aws/aws-sdk-go-v2/service/networkmanager"
	"github.com/aws/aws-sdk-go-v2/service/networkmanager/types"
	"github.com/stretchr/testify/require"
)

func createTestCoreNetwork(t *testing.T, client *networkmanagersdk.Client) *networkmanagersdk.CreateCoreNetworkOutput {
	t.Helper()

	gn, err := client.CreateGlobalNetwork(t.Context(), &networkmanagersdk.CreateGlobalNetworkInput{})
	require.NoError(t, err)

	cn, err := client.CreateCoreNetwork(t.Context(), &networkmanagersdk.CreateCoreNetworkInput{
		GlobalNetworkId: gn.GlobalNetwork.GlobalNetworkId,
	})
	require.NoError(t, err)

	return cn
}

// TestRoundTrip_CoreNetworkLifecycle drives family L.
func TestRoundTrip_CoreNetworkLifecycle(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	cn := createTestCoreNetwork(t, client)
	require.Equal(t, types.CoreNetworkStateCreating, cn.CoreNetwork.State)

	id := cn.CoreNetwork.CoreNetworkId

	require.Eventually(t, func() bool {
		g, err := client.GetCoreNetwork(ctx, &networkmanagersdk.GetCoreNetworkInput{CoreNetworkId: id})

		return err == nil && g.CoreNetwork.State == types.CoreNetworkStateAvailable
	}, defaultAsyncWait, defaultAsyncPoll)

	updated, err := client.UpdateCoreNetwork(ctx, &networkmanagersdk.UpdateCoreNetworkInput{
		CoreNetworkId: id, Description: aws.String("renamed"),
	})
	require.NoError(t, err)
	require.Equal(t, "renamed", aws.ToString(updated.CoreNetwork.Description))

	listed, err := client.ListCoreNetworks(ctx, &networkmanagersdk.ListCoreNetworksInput{})
	require.NoError(t, err)
	require.Len(t, listed.CoreNetworks, 1)

	_, err = client.DeleteCoreNetwork(ctx, &networkmanagersdk.DeleteCoreNetworkInput{CoreNetworkId: id})
	require.NoError(t, err)
}

// TestRoundTrip_CoreNetworkPolicyLifecycle drives family M's versioned
// policy + change-set state machine: Put -> ready-to-execute ->
// ExecuteCoreNetworkChangeSet flips LIVE, and GetCoreNetworkPolicy's
// LIVE/LATEST aliases genuinely diverge until execution runs.
func TestRoundTrip_CoreNetworkPolicyLifecycle(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	cn := createTestCoreNetwork(t, client)
	id := cn.CoreNetwork.CoreNetworkId

	put, err := client.PutCoreNetworkPolicy(ctx, &networkmanagersdk.PutCoreNetworkPolicyInput{
		CoreNetworkId: id, PolicyDocument: aws.String(`{"version":"2021.12"}`),
	})
	require.NoError(t, err)
	require.Equal(t, types.CoreNetworkPolicyAliasLatest, put.CoreNetworkPolicy.Alias)
	require.Equal(t, int32(1), aws.ToInt32(put.CoreNetworkPolicy.PolicyVersionId))

	versionID := put.CoreNetworkPolicy.PolicyVersionId

	require.Eventually(t, func() bool {
		g, getErr := client.GetCoreNetworkPolicy(ctx, &networkmanagersdk.GetCoreNetworkPolicyInput{
			CoreNetworkId: id, PolicyVersionId: versionID,
		})

		return getErr == nil && g.CoreNetworkPolicy.ChangeSetState == types.ChangeSetStateReadyToExecute
	}, defaultAsyncWait, defaultAsyncPoll)

	// LATEST and LIVE diverge until ExecuteCoreNetworkChangeSet runs: no
	// LIVE policy exists yet since this is the first version.
	_, err = client.GetCoreNetworkPolicy(ctx, &networkmanagersdk.GetCoreNetworkPolicyInput{
		CoreNetworkId: id, Alias: types.CoreNetworkPolicyAliasLive,
	})
	require.Error(t, err)

	changes, err := client.GetCoreNetworkChangeSet(ctx, &networkmanagersdk.GetCoreNetworkChangeSetInput{
		CoreNetworkId: id, PolicyVersionId: versionID,
	})
	require.NoError(t, err)
	require.Empty(t, changes.CoreNetworkChanges)

	events, err := client.GetCoreNetworkChangeEvents(ctx, &networkmanagersdk.GetCoreNetworkChangeEventsInput{
		CoreNetworkId: id, PolicyVersionId: versionID,
	})
	require.NoError(t, err)
	require.Empty(t, events.CoreNetworkChangeEvents)

	_, err = client.ExecuteCoreNetworkChangeSet(ctx, &networkmanagersdk.ExecuteCoreNetworkChangeSetInput{
		CoreNetworkId: id, PolicyVersionId: versionID,
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		g, getErr := client.GetCoreNetworkPolicy(ctx, &networkmanagersdk.GetCoreNetworkPolicyInput{
			CoreNetworkId: id, Alias: types.CoreNetworkPolicyAliasLive,
		})

		return getErr == nil && aws.ToInt32(g.CoreNetworkPolicy.PolicyVersionId) == aws.ToInt32(versionID) &&
			g.CoreNetworkPolicy.ChangeSetState == types.ChangeSetStateExecutionSucceeded
	}, defaultAsyncWait, defaultAsyncPoll)

	// Can't delete the current LIVE policy version.
	_, err = client.DeleteCoreNetworkPolicyVersion(ctx, &networkmanagersdk.DeleteCoreNetworkPolicyVersionInput{
		CoreNetworkId: id, PolicyVersionId: versionID,
	})
	require.Error(t, err)

	var conflict *types.ConflictException
	require.ErrorAs(t, err, &conflict)

	restored, err := client.RestoreCoreNetworkPolicyVersion(
		ctx,
		&networkmanagersdk.RestoreCoreNetworkPolicyVersionInput{
			CoreNetworkId: id, PolicyVersionId: versionID,
		},
	)
	require.NoError(t, err)
	require.Equal(t, int32(2), aws.ToInt32(restored.CoreNetworkPolicy.PolicyVersionId))
	require.JSONEq(t, `{"version":"2021.12"}`, aws.ToString(restored.CoreNetworkPolicy.PolicyDocument))

	versions, err := client.ListCoreNetworkPolicyVersions(ctx, &networkmanagersdk.ListCoreNetworkPolicyVersionsInput{
		CoreNetworkId: id,
	})
	require.NoError(t, err)
	require.Len(t, versions.CoreNetworkPolicyVersions, 2)
}

// TestRoundTrip_CoreNetworkPrefixListAssociation drives family N.
func TestRoundTrip_CoreNetworkPrefixListAssociation(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	cn := createTestCoreNetwork(t, client)
	id := cn.CoreNetwork.CoreNetworkId

	prefixListArn := "arn:aws:ec2:us-east-1:000000000000:prefix-list/pl-0123456789abcdef0"

	created, err := client.CreateCoreNetworkPrefixListAssociation(
		ctx, &networkmanagersdk.CreateCoreNetworkPrefixListAssociationInput{
			CoreNetworkId: id, PrefixListArn: aws.String(prefixListArn), PrefixListAlias: aws.String("my-alias"),
		},
	)
	require.NoError(t, err)
	require.Equal(t, "my-alias", aws.ToString(created.PrefixListAlias))

	listed, err := client.ListCoreNetworkPrefixListAssociations(
		ctx, &networkmanagersdk.ListCoreNetworkPrefixListAssociationsInput{CoreNetworkId: id},
	)
	require.NoError(t, err)
	require.Len(t, listed.PrefixListAssociations, 1)

	_, err = client.DeleteCoreNetworkPrefixListAssociation(
		ctx, &networkmanagersdk.DeleteCoreNetworkPrefixListAssociationInput{
			CoreNetworkId: id, PrefixListArn: aws.String(prefixListArn),
		},
	)
	require.NoError(t, err)
}

// TestRoundTrip_CoreNetworkRoutingInformation drives family O -- an honest
// empty result, since no BGP route-propagation engine exists.
func TestRoundTrip_CoreNetworkRoutingInformation(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	cn := createTestCoreNetwork(t, client)

	out, err := client.ListCoreNetworkRoutingInformation(ctx, &networkmanagersdk.ListCoreNetworkRoutingInformationInput{
		CoreNetworkId: cn.CoreNetwork.CoreNetworkId,
		EdgeLocation:  aws.String("us-east-1"),
		SegmentName:   aws.String("prod"),
	})
	require.NoError(t, err)
	require.Empty(t, out.CoreNetworkRoutingInformation)
}

// TestRoundTrip_AttachmentRoutingPolicyLabel drives family P.
func TestRoundTrip_AttachmentRoutingPolicyLabel(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	cn := createTestCoreNetwork(t, client)

	att, err := client.CreateVpcAttachment(ctx, &networkmanagersdk.CreateVpcAttachmentInput{
		CoreNetworkId: cn.CoreNetwork.CoreNetworkId,
		VpcArn:        aws.String("arn:aws:ec2:us-east-1:000000000000:vpc/vpc-0123456789abcdef0"),
		SubnetArns:    []string{"arn:aws:ec2:us-east-1:000000000000:subnet/subnet-0123456789abcdef0"},
	})
	require.NoError(t, err)

	labeled, err := client.PutAttachmentRoutingPolicyLabel(ctx, &networkmanagersdk.PutAttachmentRoutingPolicyLabelInput{
		CoreNetworkId: cn.CoreNetwork.CoreNetworkId, AttachmentId: att.VpcAttachment.Attachment.AttachmentId,
		RoutingPolicyLabel: aws.String("gold"),
	})
	require.NoError(t, err)
	require.Equal(t, "gold", aws.ToString(labeled.RoutingPolicyLabel))

	listed, err := client.ListAttachmentRoutingPolicyAssociations(
		ctx,
		&networkmanagersdk.ListAttachmentRoutingPolicyAssociationsInput{CoreNetworkId: cn.CoreNetwork.CoreNetworkId},
	)
	require.NoError(t, err)
	require.Len(t, listed.AttachmentRoutingPolicyAssociations, 1)

	_, err = client.RemoveAttachmentRoutingPolicyLabel(ctx, &networkmanagersdk.RemoveAttachmentRoutingPolicyLabelInput{
		CoreNetworkId: cn.CoreNetwork.CoreNetworkId, AttachmentId: att.VpcAttachment.Attachment.AttachmentId,
	})
	require.NoError(t, err)
}
