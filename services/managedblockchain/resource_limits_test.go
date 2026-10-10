package managedblockchain_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	managedblockchainsdk "github.com/aws/aws-sdk-go-v2/service/managedblockchain"
	mbctypes "github.com/aws/aws-sdk-go-v2/service/managedblockchain/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/managedblockchain"
)

func createEditionNetwork(
	t *testing.T, client *managedblockchainsdk.Client, i int, edition mbctypes.Edition,
) error {
	t.Helper()

	_, err := client.CreateNetwork(t.Context(), &managedblockchainsdk.CreateNetworkInput{
		ClientRequestToken: aws.String(fmt.Sprintf("tok-%d", i)),
		Name:               aws.String(fmt.Sprintf("net-%s-%d", edition, i)),
		Framework:          mbctypes.FrameworkHyperledgerFabric,
		FrameworkVersion:   aws.String("1.4"),
		FrameworkConfiguration: &mbctypes.NetworkFrameworkConfiguration{
			Fabric: &mbctypes.NetworkFabricConfiguration{Edition: edition},
		},
		VotingPolicy: &mbctypes.VotingPolicy{
			ApprovalThresholdPolicy: &mbctypes.ApprovalThresholdPolicy{
				ThresholdPercentage:     aws.Int32(50),
				ProposalDurationInHours: aws.Int32(24),
				ThresholdComparator:     mbctypes.ThresholdComparatorGreaterThan,
			},
		},
		MemberConfiguration: &mbctypes.MemberConfiguration{
			Name: aws.String(fmt.Sprintf("m-%s-%d", edition, i)),
			FrameworkConfiguration: &mbctypes.MemberFrameworkConfiguration{
				Fabric: &mbctypes.MemberFabricConfiguration{
					AdminUsername: aws.String("admin"),
					AdminPassword: aws.String("Passw0rd!"),
				},
			},
		},
	})

	return err
}

func TestResourceLimitExceeded(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, client *managedblockchainsdk.Client) error
		name string
	}{
		{
			name: "ethereum_nodes",
			run: func(t *testing.T, client *managedblockchainsdk.Client) error {
				t.Helper()

				var err error

				for i := range 51 {
					_, err = client.CreateNode(t.Context(), &managedblockchainsdk.CreateNodeInput{
						ClientRequestToken: aws.String(fmt.Sprintf("tok-%d", i)),
						NetworkId:          aws.String(ethereumNetworkID),
						NodeConfiguration: &mbctypes.NodeConfiguration{
							InstanceType:     aws.String("bc.t3.large"),
							AvailabilityZone: aws.String("us-east-1a"),
						},
					})
					if i < 50 {
						require.NoError(t, err)
					}
				}

				return err
			},
		},
		{
			name: "starter_networks",
			run: func(t *testing.T, client *managedblockchainsdk.Client) error {
				t.Helper()

				var err error

				for i := range 7 {
					err = createEditionNetwork(t, client, i, mbctypes.EditionStarter)
					if i < 6 {
						require.NoError(t, err)
					}
				}

				require.NoError(t, createEditionNetwork(t, client, 0, mbctypes.EditionStandard))

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newSDKTestClient(t, newTestHandler(t))

			var limit *mbctypes.ResourceLimitExceededException

			require.ErrorAs(t, tt.run(t, client), &limit)
		})
	}
}

func TestResourceLimitExceeded_EditionLimits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		edition     string
		wantMembers int
		wantNodes   int
	}{
		{name: "starter", edition: "STARTER", wantMembers: 5, wantNodes: 2},
		{name: "standard", edition: "STANDARD", wantMembers: 14, wantNodes: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := managedblockchain.NewInMemoryBackend()

			n, m1, err := b.CreateNetwork(testRegion, testAccountID,
				"net", "", "", "", "founder", "", nil, nil, nil, tt.edition, "admin", "")
			require.NoError(t, err)

			for i := 1; i < tt.wantMembers; i++ {
				inv := b.AddInvitationInternal(testRegion, testAccountID, n.ID, "net")
				_, err = b.CreateMember(
					testRegion, testAccountID, n.ID, inv.InvitationID, fmt.Sprintf("m%d", i), "", "admin", "", nil,
				)
				require.NoError(t, err)
			}

			inv := b.AddInvitationInternal(testRegion, testAccountID, n.ID, "net")
			_, err = b.CreateMember(testRegion, testAccountID, n.ID, inv.InvitationID, "extra", "", "admin", "", nil)
			require.ErrorIs(t, err, managedblockchain.ErrResourceLimitExceeded)

			for range tt.wantNodes {
				_, err = b.CreateNode(testRegion, testAccountID, n.ID, m1.ID, "bc.t3.small", "us-east-1a", "", nil)
				require.NoError(t, err)
			}

			_, err = b.CreateNode(testRegion, testAccountID, n.ID, m1.ID, "bc.t3.small", "us-east-1a", "", nil)
			require.ErrorIs(t, err, managedblockchain.ErrResourceLimitExceeded)
		})
	}
}
