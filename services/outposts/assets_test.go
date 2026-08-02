package outposts_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	outpostssdk "github.com/aws/aws-sdk-go-v2/service/outposts"
	"github.com/aws/aws-sdk-go-v2/service/outposts/types"
	"github.com/stretchr/testify/require"
)

func TestListAssets_SeededOnCreateOutpost(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	siteID := createTestSite(t, client)
	created := createTestOutpost(t, client, siteID)

	out, err := client.ListAssets(t.Context(), &outpostssdk.ListAssetsInput{OutpostIdentifier: created.OutpostId})
	require.NoError(t, err)
	require.Len(t, out.Assets, 1)
	require.Equal(t, types.AssetTypeCompute, out.Assets[0].AssetType)
	require.NotNil(t, out.Assets[0].ComputeAttributes)
	require.Equal(t, types.ComputeAssetStateActive, out.Assets[0].ComputeAttributes.State)
}

func TestListAssets_UnknownOutpost(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)

	_, err := client.ListAssets(t.Context(), &outpostssdk.ListAssetsInput{
		OutpostIdentifier: aws.String("op-does-not-exist"),
	})
	require.Error(t, err)

	var nfe *types.NotFoundException
	require.ErrorAs(t, err, &nfe)
}

func TestListAssetInstances_EmptyButValidated(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	siteID := createTestSite(t, client)
	created := createTestOutpost(t, client, siteID)

	out, err := client.ListAssetInstances(t.Context(), &outpostssdk.ListAssetInstancesInput{
		OutpostIdentifier: created.OutpostId,
	})
	require.NoError(t, err)
	require.Empty(t, out.AssetInstances)

	_, err = client.ListAssetInstances(t.Context(), &outpostssdk.ListAssetInstancesInput{
		OutpostIdentifier: aws.String("op-does-not-exist"),
	})
	require.Error(t, err)

	var nfe *types.NotFoundException
	require.ErrorAs(t, err, &nfe)
}
