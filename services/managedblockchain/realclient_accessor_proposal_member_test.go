package managedblockchain_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	managedblockchainsdk "github.com/aws/aws-sdk-go-v2/service/managedblockchain"
	mbctypes "github.com/aws/aws-sdk-go-v2/service/managedblockchain/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRealClient_AccessorProposalMember drives managedblockchain's remaining
// typed-coverage-blind ops (gopherstack-n3zi) through the real
// aws-sdk-go-v2 client: CreateAccessor, CreateProposal, DeleteAccessor,
// DeleteNode, GetAccessor, GetProposal, ListAccessors, ListInvitations,
// ListNodes, ListProposalVotes, ListProposals, RejectInvitation,
// UpdateMember, VoteOnProposal.
func TestRealClient_AccessorProposalMember(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "accessor CRUD",
			run: func(t *testing.T) {
				t.Helper()

				h := newTestHandler(t)
				client := newSDKTestClient(t, h)
				ctx := t.Context()

				createOut, err := client.CreateAccessor(ctx, &managedblockchainsdk.CreateAccessorInput{
					AccessorType:       mbctypes.AccessorTypeBillingToken,
					ClientRequestToken: aws.String("s23-accessor-token-1"),
					NetworkType:        mbctypes.AccessorNetworkTypeEthereumMainnet,
				})
				require.NoError(t, err)
				require.NotEmpty(t, aws.ToString(createOut.AccessorId))
				require.NotEmpty(t, aws.ToString(createOut.BillingToken))
				assert.Equal(t, mbctypes.AccessorNetworkTypeEthereumMainnet, createOut.NetworkType)

				getOut, err := client.GetAccessor(ctx, &managedblockchainsdk.GetAccessorInput{
					AccessorId: createOut.AccessorId,
				})
				require.NoError(t, err)
				require.NotNil(t, getOut.Accessor)
				assert.Equal(t, aws.ToString(createOut.AccessorId), aws.ToString(getOut.Accessor.Id))
				assert.Equal(t, aws.ToString(createOut.BillingToken), aws.ToString(getOut.Accessor.BillingToken))

				listOut, err := client.ListAccessors(ctx, &managedblockchainsdk.ListAccessorsInput{})
				require.NoError(t, err)

				ids := make([]string, 0, len(listOut.Accessors))
				for _, a := range listOut.Accessors {
					ids = append(ids, aws.ToString(a.Id))
				}

				assert.Contains(t, ids, aws.ToString(createOut.AccessorId))

				_, err = client.DeleteAccessor(ctx, &managedblockchainsdk.DeleteAccessorInput{
					AccessorId: createOut.AccessorId,
				})
				require.NoError(t, err)

				getAfterDelete, err := client.GetAccessor(ctx, &managedblockchainsdk.GetAccessorInput{
					AccessorId: createOut.AccessorId,
				})
				require.NoError(
					t,
					err,
					"GetAccessor on a deleted accessor still succeeds, reporting PENDING_DELETION status",
				)
				require.NotNil(t, getAfterDelete.Accessor)
				assert.Equal(t, mbctypes.AccessorStatusPendingDeletion, getAfterDelete.Accessor.Status)
			},
		},
		{
			name: "proposal lifecycle: create, vote, invitation, reject",
			run: func(t *testing.T) {
				t.Helper()

				h := newTestHandler(t)
				client := newSDKTestClient(t, h)
				ctx := t.Context()

				netOut, err := client.CreateNetwork(ctx, &managedblockchainsdk.CreateNetworkInput{
					Name:             aws.String("s23-proposal-net"),
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
						Name: aws.String("s23-founding-member"),
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
					ClientRequestToken: aws.String("s23-proposal-token-1"),
					Description:        aws.String("invite another account"),
					Actions: &mbctypes.ProposalActions{
						Invitations: []mbctypes.InviteAction{{Principal: aws.String("999999999999")}},
					},
				})
				require.NoError(t, err)
				require.NotEmpty(t, aws.ToString(proposeOut.ProposalId))

				getProposalOut, err := client.GetProposal(ctx, &managedblockchainsdk.GetProposalInput{
					NetworkId:  netOut.NetworkId,
					ProposalId: proposeOut.ProposalId,
				})
				require.NoError(t, err)
				require.NotNil(t, getProposalOut.Proposal)
				assert.Equal(t, "invite another account", aws.ToString(getProposalOut.Proposal.Description))
				assert.Equal(t, mbctypes.ProposalStatusInProgress, getProposalOut.Proposal.Status)

				listProposalsOut, err := client.ListProposals(ctx, &managedblockchainsdk.ListProposalsInput{
					NetworkId: netOut.NetworkId,
				})
				require.NoError(t, err)
				require.Len(t, listProposalsOut.Proposals, 1)
				assert.Equal(
					t,
					aws.ToString(proposeOut.ProposalId),
					aws.ToString(listProposalsOut.Proposals[0].ProposalId),
				)

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
				assert.Equal(t, mbctypes.VoteValueYes, votesOut.ProposalVotes[0].Vote)
				assert.Equal(t, aws.ToString(netOut.MemberId), aws.ToString(votesOut.ProposalVotes[0].MemberId))

				getProposalAfterVote, err := client.GetProposal(ctx, &managedblockchainsdk.GetProposalInput{
					NetworkId:  netOut.NetworkId,
					ProposalId: proposeOut.ProposalId,
				})
				require.NoError(t, err)
				assert.Equal(t, mbctypes.ProposalStatusApproved, getProposalAfterVote.Proposal.Status,
					"a single founding member voting YES exceeds a 50%% GREATER_THAN threshold")

				listInvitationsOut, err := client.ListInvitations(ctx, &managedblockchainsdk.ListInvitationsInput{})
				require.NoError(t, err)

				var invitationID *string

				for _, inv := range listInvitationsOut.Invitations {
					if aws.ToString(inv.NetworkSummary.Id) == aws.ToString(netOut.NetworkId) {
						invitationID = inv.InvitationId
					}
				}

				require.NotNil(
					t,
					invitationID,
					"the approved proposal's Invitations action must produce a real invitation",
				)

				_, err = client.RejectInvitation(ctx, &managedblockchainsdk.RejectInvitationInput{
					InvitationId: invitationID,
				})
				require.NoError(t, err)

				listAfterReject, err := client.ListInvitations(ctx, &managedblockchainsdk.ListInvitationsInput{})
				require.NoError(t, err)

				for _, inv := range listAfterReject.Invitations {
					if aws.ToString(inv.InvitationId) == aws.ToString(invitationID) {
						assert.Equal(t, mbctypes.InvitationStatusRejected, inv.Status)
					}
				}
			},
		},
		{
			name: "member update and node lifecycle",
			run: func(t *testing.T) {
				t.Helper()

				h := newTestHandler(t)
				client := newSDKTestClient(t, h)
				ctx := t.Context()

				netOut, err := client.CreateNetwork(ctx, &managedblockchainsdk.CreateNetworkInput{
					Name:             aws.String("s23-node-net"),
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
						Name: aws.String("s23-node-member"),
						FrameworkConfiguration: &mbctypes.MemberFrameworkConfiguration{
							Fabric: &mbctypes.MemberFabricConfiguration{
								AdminUsername: aws.String("admin"),
								AdminPassword: aws.String("Passw0rd!"),
							},
						},
					},
				})
				require.NoError(t, err)

				_, err = client.UpdateMember(ctx, &managedblockchainsdk.UpdateMemberInput{
					NetworkId: netOut.NetworkId,
					MemberId:  netOut.MemberId,
					LogPublishingConfiguration: &mbctypes.MemberLogPublishingConfiguration{
						Fabric: &mbctypes.MemberFabricLogPublishingConfiguration{
							CaLogs: &mbctypes.LogConfigurations{
								Cloudwatch: &mbctypes.LogConfiguration{Enabled: aws.Bool(true)},
							},
						},
					},
				})
				require.NoError(t, err)

				getMemberOut, err := client.GetMember(ctx, &managedblockchainsdk.GetMemberInput{
					NetworkId: netOut.NetworkId,
					MemberId:  netOut.MemberId,
				})
				require.NoError(t, err)
				require.NotNil(t, getMemberOut.Member.LogPublishingConfiguration)
				require.NotNil(t, getMemberOut.Member.LogPublishingConfiguration.Fabric)
				require.NotNil(t, getMemberOut.Member.LogPublishingConfiguration.Fabric.CaLogs)
				require.NotNil(t, getMemberOut.Member.LogPublishingConfiguration.Fabric.CaLogs.Cloudwatch)
				assert.True(
					t,
					aws.ToBool(getMemberOut.Member.LogPublishingConfiguration.Fabric.CaLogs.Cloudwatch.Enabled),
				)

				nodeOut, err := client.CreateNode(ctx, &managedblockchainsdk.CreateNodeInput{
					NetworkId: netOut.NetworkId,
					MemberId:  netOut.MemberId,
					NodeConfiguration: &mbctypes.NodeConfiguration{
						InstanceType:     aws.String("bc.t3.small"),
						AvailabilityZone: aws.String("us-east-1a"),
					},
				})
				require.NoError(t, err)

				listNodesOut, err := client.ListNodes(ctx, &managedblockchainsdk.ListNodesInput{
					NetworkId: netOut.NetworkId,
					MemberId:  netOut.MemberId,
				})
				require.NoError(t, err)
				require.Len(t, listNodesOut.Nodes, 1)
				assert.Equal(t, aws.ToString(nodeOut.NodeId), aws.ToString(listNodesOut.Nodes[0].Id))

				_, err = client.DeleteNode(ctx, &managedblockchainsdk.DeleteNodeInput{
					NetworkId: netOut.NetworkId,
					NodeId:    nodeOut.NodeId,
					MemberId:  netOut.MemberId,
				})
				require.NoError(t, err)

				listNodesAfterDelete, err := client.ListNodes(ctx, &managedblockchainsdk.ListNodesInput{
					NetworkId: netOut.NetworkId,
					MemberId:  netOut.MemberId,
				})
				require.NoError(t, err)
				assert.Empty(t, listNodesAfterDelete.Nodes)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}
