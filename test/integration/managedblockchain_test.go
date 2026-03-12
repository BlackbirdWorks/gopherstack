//go:build integration
// +build integration

package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	managedblockchainSDK "github.com/aws/aws-sdk-go-v2/service/managedblockchain"
	managedblockchaintypes "github.com/aws/aws-sdk-go-v2/service/managedblockchain/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createManagedBlockchainClient returns a Managed Blockchain client pointed at the shared test container.
func createManagedBlockchainClient(t *testing.T) *managedblockchainSDK.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return managedblockchainSDK.NewFromConfig(cfg, func(o *managedblockchainSDK.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_ManagedBlockchain_NetworkLifecycle tests network creation and member operations.
func TestIntegration_ManagedBlockchain_NetworkLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		networkName string
		memberName  string
	}{
		{
			name:        "full_lifecycle",
			networkName: "integration-test-network",
			memberName:  "integration-test-member",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createManagedBlockchainClient(t)

			uniqueSuffix := t.Name()
			networkName := tt.networkName + "-" + uniqueSuffix
			memberName := tt.memberName

			// CreateNetwork with initial member.
			createNetOut, err := client.CreateNetwork(ctx, &managedblockchainSDK.CreateNetworkInput{
				ClientRequestToken: aws.String("token-1"),
				Framework:          managedblockchaintypes.FrameworkHyperledgerFabric,
				FrameworkVersion:   aws.String("1.4"),
				Name:               aws.String(networkName),
				VotingPolicy: &managedblockchaintypes.VotingPolicy{
					ApprovalThresholdPolicy: &managedblockchaintypes.ApprovalThresholdPolicy{
						ThresholdPercentage:  aws.Int32(50),
						ProposalDurationInHours: aws.Int32(24),
						ThresholdComparator:  managedblockchaintypes.ThresholdComparatorGreaterThan,
					},
				},
				MemberConfiguration: &managedblockchaintypes.MemberConfiguration{
					Name: aws.String(memberName),
					FrameworkConfiguration: &managedblockchaintypes.MemberFrameworkConfiguration{
						Fabric: &managedblockchaintypes.MemberFabricConfiguration{
							AdminUsername: aws.String("admin"),
							AdminPassword: aws.String("Password123!"),
						},
					},
				},
			})
			require.NoError(t, err, "CreateNetwork should succeed")
			require.NotNil(t, createNetOut.NetworkId)
			require.NotNil(t, createNetOut.MemberId)

			networkID := aws.ToString(createNetOut.NetworkId)
			assert.NotEmpty(t, networkID)

			// GetNetwork.
			getNetOut, err := client.GetNetwork(ctx, &managedblockchainSDK.GetNetworkInput{
				NetworkId: createNetOut.NetworkId,
			})
			require.NoError(t, err, "GetNetwork should succeed")
			require.NotNil(t, getNetOut.Network)
			assert.Equal(t, networkName, aws.ToString(getNetOut.Network.Name))

			// ListNetworks — should contain the created one.
			listNetsOut, err := client.ListNetworks(ctx, &managedblockchainSDK.ListNetworksInput{})
			require.NoError(t, err, "ListNetworks should succeed")

			found := false

			for _, n := range listNetsOut.Networks {
				if aws.ToString(n.Id) == networkID {
					found = true

					break
				}
			}

			assert.True(t, found, "created network should appear in list")

			// CreateMember.
			createMemOut, err := client.CreateMember(ctx, &managedblockchainSDK.CreateMemberInput{
				ClientRequestToken: aws.String("token-2"),
				InvitationId:       aws.String("inv-1"),
				NetworkId:          createNetOut.NetworkId,
				MemberConfiguration: &managedblockchaintypes.MemberConfiguration{
					Name: aws.String("second-member"),
					FrameworkConfiguration: &managedblockchaintypes.MemberFrameworkConfiguration{
						Fabric: &managedblockchaintypes.MemberFabricConfiguration{
							AdminUsername: aws.String("admin"),
							AdminPassword: aws.String("Password123!"),
						},
					},
				},
			})
			require.NoError(t, err, "CreateMember should succeed")
			require.NotNil(t, createMemOut.MemberId)

			// GetMember.
			getMemOut, err := client.GetMember(ctx, &managedblockchainSDK.GetMemberInput{
				NetworkId: createNetOut.NetworkId,
				MemberId:  createMemOut.MemberId,
			})
			require.NoError(t, err, "GetMember should succeed")
			require.NotNil(t, getMemOut.Member)
			assert.Equal(t, "second-member", aws.ToString(getMemOut.Member.Name))

			// ListMembers — should contain initial + second member.
			listMemsOut, err := client.ListMembers(ctx, &managedblockchainSDK.ListMembersInput{
				NetworkId: createNetOut.NetworkId,
			})
			require.NoError(t, err, "ListMembers should succeed")
			assert.GreaterOrEqual(t, len(listMemsOut.Members), 2)

			// DeleteMember.
			_, err = client.DeleteMember(ctx, &managedblockchainSDK.DeleteMemberInput{
				NetworkId: createNetOut.NetworkId,
				MemberId:  createMemOut.MemberId,
			})
			require.NoError(t, err, "DeleteMember should succeed")

			// Verify deletion via GetMember → should return 404.
			_, err = client.GetMember(ctx, &managedblockchainSDK.GetMemberInput{
				NetworkId: createNetOut.NetworkId,
				MemberId:  createMemOut.MemberId,
			})
			require.Error(t, err, "GetMember after deletion should fail")
		})
	}
}
