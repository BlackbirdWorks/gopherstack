package managedblockchain_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	managedblockchainsdk "github.com/aws/aws-sdk-go-v2/service/managedblockchain"
	mbctypes "github.com/aws/aws-sdk-go-v2/service/managedblockchain/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const ethereumNetworkID = "n-ethereum-mainnet"

func TestSDKRoundTrip_EthereumMainnetNode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		az      string
		wantErr bool
	}{
		{name: "memberless_node", az: "us-east-1a"},
		{name: "missing_availability_zone", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newSDKTestClient(t, newTestHandler(t))
			ctx := t.Context()

			_, err := client.GetNetwork(
				ctx,
				&managedblockchainsdk.GetNetworkInput{NetworkId: aws.String(ethereumNetworkID)},
			)
			require.Error(t, err, "network is not visible before a node exists")

			cfg := &mbctypes.NodeConfiguration{InstanceType: aws.String("bc.t3.large")}
			if tt.az != "" {
				cfg.AvailabilityZone = aws.String(tt.az)
			}

			created, err := client.CreateNode(ctx, &managedblockchainsdk.CreateNodeInput{
				ClientRequestToken: aws.String("tok"),
				NetworkId:          aws.String(ethereumNetworkID),
				NodeConfiguration:  cfg,
			})
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			net, err := client.GetNetwork(
				ctx,
				&managedblockchainsdk.GetNetworkInput{NetworkId: aws.String(ethereumNetworkID)},
			)
			require.NoError(t, err)
			assert.Equal(t, mbctypes.FrameworkEthereum, net.Network.Framework)
			assert.Equal(t, "1", aws.ToString(net.Network.FrameworkAttributes.Ethereum.ChainId))

			node, err := client.GetNode(ctx, &managedblockchainsdk.GetNodeInput{
				NetworkId: aws.String(ethereumNetworkID), NodeId: created.NodeId,
			})
			require.NoError(t, err)
			assert.Empty(t, aws.ToString(node.Node.MemberId))
			assert.Equal(t, "us-east-1a", aws.ToString(node.Node.AvailabilityZone))
			assert.NotEmpty(t, aws.ToString(node.Node.FrameworkAttributes.Ethereum.HttpEndpoint))

			listed, err := client.ListNodes(
				ctx,
				&managedblockchainsdk.ListNodesInput{NetworkId: aws.String(ethereumNetworkID)},
			)
			require.NoError(t, err)
			require.Len(t, listed.Nodes, 1)

			_, err = client.DeleteNode(ctx, &managedblockchainsdk.DeleteNodeInput{
				NetworkId: aws.String(ethereumNetworkID), NodeId: created.NodeId,
			})
			require.NoError(t, err)

			listed, err = client.ListNodes(
				ctx,
				&managedblockchainsdk.ListNodesInput{NetworkId: aws.String(ethereumNetworkID)},
			)
			require.NoError(t, err)
			assert.Empty(t, listed.Nodes)
		})
	}
}
