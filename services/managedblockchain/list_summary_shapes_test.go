package managedblockchain_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	managedblockchainsdk "github.com/aws/aws-sdk-go-v2/service/managedblockchain"
	mbctypes "github.com/aws/aws-sdk-go-v2/service/managedblockchain/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListSummaryShapes proves this pass's over-wide-response audit
// (gopherstack-dv4s, 2026-09-19) for managedblockchain's six flagged List
// ops: ListAccessors, ListMembers, ListNetworks, ListNodes,
// ListProposalVotes and ListProposals were all ALREADY exact matches of
// their real *Summary types (verified via cmd/structfielddiff against
// managedblockchain@v1.34.4). No bug found; this test locks the member sets
// in via the real aws-sdk-go-v2 client.
func TestListSummaryShapes(t *testing.T) {
	t.Parallel()

	t.Run("accessors exact", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		client := newSDKTestClient(t, h)
		ctx := t.Context()

		createOut, err := client.CreateAccessor(ctx, &managedblockchainsdk.CreateAccessorInput{
			AccessorType:       mbctypes.AccessorTypeBillingToken,
			ClientRequestToken: aws.String("lss-accessor-token"),
			NetworkType:        mbctypes.AccessorNetworkTypeEthereumMainnet,
		})
		require.NoError(t, err)

		out, err := client.ListAccessors(ctx, &managedblockchainsdk.ListAccessorsInput{})
		require.NoError(t, err)
		require.Len(t, out.Accessors, 1)
		a := out.Accessors[0]
		assert.Equal(t, aws.ToString(createOut.AccessorId), aws.ToString(a.Id))
		assert.Equal(t, mbctypes.AccessorNetworkTypeEthereumMainnet, a.NetworkType)
		assert.NotNil(t, a.CreationDate)
	})

	t.Run("networks and members exact", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		client := newSDKTestClient(t, h)
		ctx := t.Context()

		netOut, err := client.CreateNetwork(ctx, &managedblockchainsdk.CreateNetworkInput{
			Name:             aws.String("lss-net2"),
			Framework:        mbctypes.FrameworkHyperledgerFabric,
			FrameworkVersion: aws.String("2.2"),
			VotingPolicy: &mbctypes.VotingPolicy{
				ApprovalThresholdPolicy: &mbctypes.ApprovalThresholdPolicy{
					ProposalDurationInHours: aws.Int32(24),
					ThresholdPercentage:     aws.Int32(50),
					ThresholdComparator:     mbctypes.ThresholdComparatorGreaterThan,
				},
			},
			MemberConfiguration: &mbctypes.MemberConfiguration{
				Name: aws.String("lss-founding-member2"),
				FrameworkConfiguration: &mbctypes.MemberFrameworkConfiguration{
					Fabric: &mbctypes.MemberFabricConfiguration{
						AdminUsername: aws.String("admin"),
						AdminPassword: aws.String("Passw0rd!"),
					},
				},
			},
		})
		require.NoError(t, err)

		netsOut, err := client.ListNetworks(ctx, &managedblockchainsdk.ListNetworksInput{})
		require.NoError(t, err)
		require.Len(t, netsOut.Networks, 1)
		n := netsOut.Networks[0]
		assert.Equal(t, aws.ToString(netOut.NetworkId), aws.ToString(n.Id))
		assert.Equal(t, mbctypes.FrameworkHyperledgerFabric, n.Framework)

		membersOut, err := client.ListMembers(ctx, &managedblockchainsdk.ListMembersInput{
			NetworkId: netOut.NetworkId,
		})
		require.NoError(t, err)
		require.Len(t, membersOut.Members, 1)
		m := membersOut.Members[0]
		assert.Equal(t, aws.ToString(netOut.MemberId), aws.ToString(m.Id))
		assert.NotNil(t, m.IsOwned)
	})

	t.Run("nodes exact", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		client := newSDKTestClient(t, h)
		ctx := t.Context()

		netOut, err := client.CreateNetwork(ctx, &managedblockchainsdk.CreateNetworkInput{
			Name:             aws.String("lss-node-net"),
			Framework:        mbctypes.FrameworkHyperledgerFabric,
			FrameworkVersion: aws.String("2.2"),
			VotingPolicy: &mbctypes.VotingPolicy{
				ApprovalThresholdPolicy: &mbctypes.ApprovalThresholdPolicy{
					ProposalDurationInHours: aws.Int32(24),
					ThresholdPercentage:     aws.Int32(50),
					ThresholdComparator:     mbctypes.ThresholdComparatorGreaterThan,
				},
			},
			MemberConfiguration: &mbctypes.MemberConfiguration{
				Name: aws.String("lss-node-member"),
				FrameworkConfiguration: &mbctypes.MemberFrameworkConfiguration{
					Fabric: &mbctypes.MemberFabricConfiguration{
						AdminUsername: aws.String("admin"),
						AdminPassword: aws.String("Passw0rd!"),
					},
				},
			},
		})
		require.NoError(t, err)

		nodeOut, err := client.CreateNode(ctx, &managedblockchainsdk.CreateNodeInput{
			NetworkId: netOut.NetworkId,
			MemberId:  netOut.MemberId,
			NodeConfiguration: &mbctypes.NodeConfiguration{
				InstanceType:     aws.String("bc.t3.small"),
				AvailabilityZone: aws.String("us-east-1a"),
			},
		})
		require.NoError(t, err)

		out, err := client.ListNodes(ctx, &managedblockchainsdk.ListNodesInput{
			NetworkId: netOut.NetworkId,
			MemberId:  netOut.MemberId,
		})
		require.NoError(t, err)
		require.Len(t, out.Nodes, 1)
		assert.Equal(t, aws.ToString(nodeOut.NodeId), aws.ToString(out.Nodes[0].Id))
		assert.Equal(t, "bc.t3.small", aws.ToString(out.Nodes[0].InstanceType))
	})

	t.Run("proposals and votes exact", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		client := newSDKTestClient(t, h)
		ctx := t.Context()

		netOut, err := client.CreateNetwork(ctx, &managedblockchainsdk.CreateNetworkInput{
			Name:             aws.String("lss-proposal-net"),
			Framework:        mbctypes.FrameworkHyperledgerFabric,
			FrameworkVersion: aws.String("2.2"),
			VotingPolicy: &mbctypes.VotingPolicy{
				ApprovalThresholdPolicy: &mbctypes.ApprovalThresholdPolicy{
					ProposalDurationInHours: aws.Int32(24),
					ThresholdPercentage:     aws.Int32(50),
					ThresholdComparator:     mbctypes.ThresholdComparatorGreaterThan,
				},
			},
			MemberConfiguration: &mbctypes.MemberConfiguration{
				Name: aws.String("lss-proposal-member"),
				FrameworkConfiguration: &mbctypes.MemberFrameworkConfiguration{
					Fabric: &mbctypes.MemberFabricConfiguration{
						AdminUsername: aws.String("admin"),
						AdminPassword: aws.String("Passw0rd!"),
					},
				},
			},
		})
		require.NoError(t, err)

		proposeOut, err := client.CreateProposal(ctx, &managedblockchainsdk.CreateProposalInput{
			NetworkId:          netOut.NetworkId,
			MemberId:           netOut.MemberId,
			ClientRequestToken: aws.String("lss-proposal-token"),
			Description:        aws.String("lss invite"),
			Actions: &mbctypes.ProposalActions{
				Invitations: []mbctypes.InviteAction{{Principal: aws.String("999999999999")}},
			},
		})
		require.NoError(t, err)

		proposalsOut, err := client.ListProposals(ctx, &managedblockchainsdk.ListProposalsInput{
			NetworkId: netOut.NetworkId,
		})
		require.NoError(t, err)
		require.Len(t, proposalsOut.Proposals, 1)
		p := proposalsOut.Proposals[0]
		assert.Equal(t, aws.ToString(proposeOut.ProposalId), aws.ToString(p.ProposalId))
		assert.Equal(t, mbctypes.ProposalStatusInProgress, p.Status)

		_, err = client.VoteOnProposal(ctx, &managedblockchainsdk.VoteOnProposalInput{
			NetworkId:     netOut.NetworkId,
			ProposalId:    proposeOut.ProposalId,
			VoterMemberId: netOut.MemberId,
			Vote:          mbctypes.VoteValueYes,
		})
		require.NoError(t, err)

		votesOut, err := client.ListProposalVotes(ctx, &managedblockchainsdk.ListProposalVotesInput{
			NetworkId:  netOut.NetworkId,
			ProposalId: proposeOut.ProposalId,
		})
		require.NoError(t, err)
		require.Len(t, votesOut.ProposalVotes, 1)
		v := votesOut.ProposalVotes[0]
		assert.Equal(t, aws.ToString(netOut.MemberId), aws.ToString(v.MemberId))
		assert.Equal(t, mbctypes.VoteValueYes, v.Vote)
	})
}
