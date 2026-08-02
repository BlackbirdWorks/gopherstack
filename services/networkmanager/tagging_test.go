package networkmanager_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	networkmanagersdk "github.com/aws/aws-sdk-go-v2/service/networkmanager"
	"github.com/aws/aws-sdk-go-v2/service/networkmanager/types"
	"github.com/stretchr/testify/require"
)

// TestRoundTrip_Tagging drives family X across two of the 9 taggable
// resource kinds (GlobalNetwork and CoreNetwork), confirming the generic
// /tags/{ResourceArn} path dispatches correctly by ARN resource-kind
// segment -- see tagging.go's resourceKindFromARN.
func TestRoundTrip_Tagging(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	gn, err := client.CreateGlobalNetwork(ctx, &networkmanagersdk.CreateGlobalNetworkInput{})
	require.NoError(t, err)

	gnArn := gn.GlobalNetwork.GlobalNetworkArn

	_, err = client.TagResource(ctx, &networkmanagersdk.TagResourceInput{
		ResourceArn: gnArn, Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
	})
	require.NoError(t, err)

	listed, err := client.ListTagsForResource(ctx, &networkmanagersdk.ListTagsForResourceInput{ResourceArn: gnArn})
	require.NoError(t, err)
	require.Len(t, listed.TagList, 1)
	require.Equal(t, "env", aws.ToString(listed.TagList[0].Key))

	_, err = client.UntagResource(
		ctx,
		&networkmanagersdk.UntagResourceInput{ResourceArn: gnArn, TagKeys: []string{"env"}},
	)
	require.NoError(t, err)

	afterUntag, err := client.ListTagsForResource(ctx, &networkmanagersdk.ListTagsForResourceInput{ResourceArn: gnArn})
	require.NoError(t, err)
	require.Empty(t, afterUntag.TagList)

	cn := createTestCoreNetwork(t, client)
	cnArn := cn.CoreNetwork.CoreNetworkArn

	_, err = client.TagResource(ctx, &networkmanagersdk.TagResourceInput{
		ResourceArn: cnArn, Tags: []types.Tag{{Key: aws.String("team"), Value: aws.String("platform")}},
	})
	require.NoError(t, err)

	cnTags, err := client.ListTagsForResource(ctx, &networkmanagersdk.ListTagsForResourceInput{ResourceArn: cnArn})
	require.NoError(t, err)
	require.Len(t, cnTags.TagList, 1)

	// An unknown resource ARN fails.
	_, err = client.ListTagsForResource(ctx, &networkmanagersdk.ListTagsForResourceInput{
		ResourceArn: aws.String("arn:aws:networkmanager::000000000000:core-network/nonexistent"),
	})
	require.Error(t, err)

	var nf *types.ResourceNotFoundException
	require.ErrorAs(t, err, &nf)
}
