package outposts_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	outpostssdk "github.com/aws/aws-sdk-go-v2/service/outposts"
	"github.com/aws/aws-sdk-go-v2/service/outposts/types"
	"github.com/stretchr/testify/require"
)

func TestStartConnection_ThenGetConnection(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	siteID := createTestSite(t, client)
	created := createTestOutpost(t, client, siteID)

	assets, err := client.ListAssets(t.Context(), &outpostssdk.ListAssetsInput{OutpostIdentifier: created.OutpostId})
	require.NoError(t, err)
	require.NotEmpty(t, assets.Assets)

	start, err := client.StartConnection(t.Context(), &outpostssdk.StartConnectionInput{
		AssetId:                     assets.Assets[0].AssetId,
		ClientPublicKey:             aws.String("client-pub-key"),
		NetworkInterfaceDeviceIndex: 0,
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(start.ConnectionId))
	require.NotEmpty(t, aws.ToString(start.UnderlayIpAddress))

	got, err := client.GetConnection(t.Context(), &outpostssdk.GetConnectionInput{
		ConnectionId: start.ConnectionId,
	})
	require.NoError(t, err)
	require.Equal(t, "client-pub-key", aws.ToString(got.ConnectionDetails.ClientPublicKey))
	require.NotEmpty(t, aws.ToString(got.ConnectionDetails.ServerPublicKey))
}

func TestStartConnection_UnknownAsset(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)

	_, err := client.StartConnection(t.Context(), &outpostssdk.StartConnectionInput{
		AssetId:                     aws.String("asset-does-not-exist"),
		ClientPublicKey:             aws.String("key"),
		NetworkInterfaceDeviceIndex: 0,
	})
	require.Error(t, err)

	var nfe *types.NotFoundException
	require.ErrorAs(t, err, &nfe)
}

func TestGetConnection_NotFound(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)

	_, err := client.GetConnection(t.Context(), &outpostssdk.GetConnectionInput{
		ConnectionId: aws.String("conn-does-not-exist"),
	})
	require.Error(t, err)

	var nfe *types.NotFoundException
	require.ErrorAs(t, err, &nfe)
}
