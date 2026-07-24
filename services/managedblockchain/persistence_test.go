package managedblockchain_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/managedblockchain"
)

func TestManagedBlockchain_PersistenceSnapshotRestore(t *testing.T) {
	t.Parallel()

	const (
		region    = "us-east-1"
		accountID = "123456789012"
	)

	tests := []struct {
		setup  func(t *testing.T, b *managedblockchain.InMemoryBackend)
		verify func(t *testing.T, b *managedblockchain.InMemoryBackend)
		name   string
	}{
		{
			name:  "empty_backend",
			setup: func(_ *testing.T, _ *managedblockchain.InMemoryBackend) {},
			verify: func(t *testing.T, b *managedblockchain.InMemoryBackend) {
				t.Helper()

				networks, err := b.ListNetworks(managedblockchain.ListNetworksFilter{})
				require.NoError(t, err)
				assert.Empty(t, networks)
			},
		},
		{
			name: "network_and_member_preserved",
			setup: func(t *testing.T, b *managedblockchain.InMemoryBackend) {
				t.Helper()

				_, _, err := b.CreateNetwork(
					region, accountID,
					"my-network", "test network",
					"HYPERLEDGER_FABRIC", "1.4",
					"founder", "founder member",
					map[string]string{"env": "test"},
					nil,
					"", "admin", "",
				)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *managedblockchain.InMemoryBackend) {
				t.Helper()

				networks, err := b.ListNetworks(managedblockchain.ListNetworksFilter{})
				require.NoError(t, err)
				require.Len(t, networks, 1)

				n := networks[0]
				assert.Equal(t, "my-network", n.Name)
				assert.Equal(t, "AVAILABLE", n.Status)

				// Verify GetNetwork works (ARN index).
				got, err := b.GetNetwork(n.ID)
				require.NoError(t, err)
				assert.Equal(t, "my-network", got.Name)

				// Verify the founding member was restored.
				members, err := b.ListMembers(n.ID, managedblockchain.ListMembersFilter{})
				require.NoError(t, err)
				require.Len(t, members, 1)
				assert.Equal(t, "founder", members[0].Name)
			},
		},
		{
			name: "arn_index_rebuilt_for_tags",
			setup: func(t *testing.T, b *managedblockchain.InMemoryBackend) {
				t.Helper()

				n, _, err := b.CreateNetwork(
					region, accountID,
					"tag-network", "", "", "", "m1", "", nil, nil,
					"", "admin", "",
				)
				require.NoError(t, err)

				require.NoError(t, b.TagResource(n.Arn, map[string]string{"key": "value"}))
			},
			verify: func(t *testing.T, b *managedblockchain.InMemoryBackend) {
				t.Helper()

				networks, err := b.ListNetworks(managedblockchain.ListNetworksFilter{})
				require.NoError(t, err)
				require.Len(t, networks, 1)

				tagMap, err := b.ListTagsForResource(networks[0].Arn)
				require.NoError(t, err)
				assert.Equal(t, "value", tagMap["key"])
			},
		},
		{
			name: "additional_member_preserved",
			setup: func(t *testing.T, b *managedblockchain.InMemoryBackend) {
				t.Helper()

				n, _, err := b.CreateNetwork(
					region, accountID,
					"multi-member", "", "", "", "member1", "", nil, nil,
					"", "admin", "",
				)
				require.NoError(t, err)

				_, err = b.CreateMember(region, accountID, n.ID, "member2", "second member", "admin", "", nil)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *managedblockchain.InMemoryBackend) {
				t.Helper()

				networks, err := b.ListNetworks(managedblockchain.ListNetworksFilter{})
				require.NoError(t, err)
				require.Len(t, networks, 1)

				members, err := b.ListMembers(networks[0].ID, managedblockchain.ListMembersFilter{})
				require.NoError(t, err)
				assert.Len(t, members, 2)
			},
		},
		{
			name: "accessor_preserved",
			setup: func(t *testing.T, b *managedblockchain.InMemoryBackend) {
				t.Helper()

				_, err := b.CreateAccessor(region, accountID, "BILLING_TOKEN", "ETHEREUM_MAINNET", nil)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *managedblockchain.InMemoryBackend) {
				t.Helper()

				accessors, err := b.ListAccessors(managedblockchain.ListAccessorsFilter{})
				require.NoError(t, err)
				require.Len(t, accessors, 1)
				assert.Equal(t, "ETHEREUM_MAINNET", accessors[0].NetworkType)
			},
		},
		{
			name: "proposal_preserved",
			setup: func(t *testing.T, b *managedblockchain.InMemoryBackend) {
				t.Helper()

				n, m, err := b.CreateNetwork(region, accountID,
					"prop-net", "", "", "", "founder", "", nil, nil, "", "admin", "")
				require.NoError(t, err)

				_, err = b.CreateProposal(region, accountID, n.ID, m.ID, "test proposal", nil, nil)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *managedblockchain.InMemoryBackend) {
				t.Helper()

				networks, err := b.ListNetworks(managedblockchain.ListNetworksFilter{})
				require.NoError(t, err)
				require.Len(t, networks, 1)

				proposals, err := b.ListProposals(networks[0].ID, "")
				require.NoError(t, err)
				require.Len(t, proposals, 1)
				assert.Equal(t, "test proposal", proposals[0].Description)
			},
		},
		{
			name: "invitation_preserved",
			setup: func(t *testing.T, b *managedblockchain.InMemoryBackend) {
				t.Helper()

				b.AddInvitationInternal(region, accountID, "net-id", "test-network")
			},
			verify: func(t *testing.T, b *managedblockchain.InMemoryBackend) {
				t.Helper()

				invitations, err := b.ListInvitations()
				require.NoError(t, err)
				require.Len(t, invitations, 1)
				assert.Equal(t, "PENDING", invitations[0].Status)
			},
		},
		{
			name: "node_preserved_and_arn_index_rebuilt",
			setup: func(t *testing.T, b *managedblockchain.InMemoryBackend) {
				t.Helper()

				n, m, err := b.CreateNetwork(region, accountID,
					"node-net", "", "", "", "founder", "", nil, nil, "", "admin", "")
				require.NoError(t, err)

				node, err := b.CreateNode(region, accountID, n.ID, m.ID, "bc.t3.small", "us-east-1a", "", nil)
				require.NoError(t, err)

				require.NoError(t, b.TagResource(node.Arn, map[string]string{"env": "prod"}))
			},
			verify: func(t *testing.T, b *managedblockchain.InMemoryBackend) {
				t.Helper()

				networks, err := b.ListNetworks(managedblockchain.ListNetworksFilter{})
				require.NoError(t, err)
				require.Len(t, networks, 1)

				members, err := b.ListMembers(networks[0].ID, managedblockchain.ListMembersFilter{})
				require.NoError(t, err)
				require.Len(t, members, 1)

				nodes, err := b.ListNodes(networks[0].ID, members[0].ID, managedblockchain.ListNodesFilter{})
				require.NoError(t, err)
				require.Len(t, nodes, 1)
				assert.Equal(t, "bc.t3.small", nodes[0].InstanceType)

				// Verify GetNode works (composite key round-tripped correctly).
				got, err := b.GetNode(networks[0].ID, members[0].ID, nodes[0].ID)
				require.NoError(t, err)
				assert.Equal(t, "us-east-1a", got.AvailabilityZone)

				// Verify the ARN index was rebuilt for the node's tags too.
				tagMap, err := b.ListTagsForResource(nodes[0].Arn)
				require.NoError(t, err)
				assert.Equal(t, "prod", tagMap["env"])
			},
		},
		{
			name: "proposal_votes_preserved",
			setup: func(t *testing.T, b *managedblockchain.InMemoryBackend) {
				t.Helper()

				n, m1, err := b.CreateNetwork(region, accountID,
					"vote-net", "", "", "", "founder", "", nil, nil, "", "admin", "")
				require.NoError(t, err)

				m2, err := b.CreateMember(region, accountID, n.ID, "second", "", "admin", "", nil)
				require.NoError(t, err)

				proposal, err := b.CreateProposal(region, accountID, n.ID, m1.ID, "test proposal", nil, nil)
				require.NoError(t, err)

				require.NoError(t, b.VoteOnProposal(n.ID, proposal.ProposalID, m1.ID, "YES"))
				require.NoError(t, b.VoteOnProposal(n.ID, proposal.ProposalID, m2.ID, "NO"))
			},
			verify: func(t *testing.T, b *managedblockchain.InMemoryBackend) {
				t.Helper()

				networks, err := b.ListNetworks(managedblockchain.ListNetworksFilter{})
				require.NoError(t, err)
				require.Len(t, networks, 1)

				proposals, err := b.ListProposals(networks[0].ID, "")
				require.NoError(t, err)
				require.Len(t, proposals, 1)
				assert.Equal(t, 1, proposals[0].YesVoteCount)
				assert.Equal(t, 1, proposals[0].NoVoteCount)

				votes, err := b.ListProposalVotes(networks[0].ID, proposals[0].ProposalID)
				require.NoError(t, err)
				require.Len(t, votes, 2)

				byMember := make(map[string]string, len(votes))
				for _, v := range votes {
					byMember[v.MemberID] = v.Vote
				}

				var sawYes, sawNo bool

				for _, vote := range byMember {
					switch vote {
					case "YES":
						sawYes = true
					case "NO":
						sawNo = true
					}
				}

				assert.True(t, sawYes, "expected a restored YES vote")
				assert.True(t, sawNo, "expected a restored NO vote")

				// Voting again with either member must still be rejected as a
				// duplicate vote -- proving the restored proposalVotes table's
				// composite key (proposalID|memberID) round-tripped correctly,
				// not just the vote counts.
				members, err := b.ListMembers(networks[0].ID, managedblockchain.ListMembersFilter{})
				require.NoError(t, err)
				require.Len(t, members, 2)

				err = b.VoteOnProposal(networks[0].ID, proposals[0].ProposalID, members[0].ID, "YES")
				assert.ErrorIs(t, err, managedblockchain.ErrValidation)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := managedblockchain.NewInMemoryBackend()
			tt.setup(t, b)

			snap := b.Snapshot(t.Context())
			require.NotNil(t, snap)

			b2 := managedblockchain.NewInMemoryBackend()
			err := b2.Restore(t.Context(), snap)
			require.NoError(t, err)

			tt.verify(t, b2)
		})
	}
}

// TestManagedBlockchain_PersistenceVersionMismatch verifies that Restore
// discards (resets, does not error, does not partially decode) any snapshot
// whose version field does not match the current
// managedblockchainSnapshotVersion -- including a snapshot with no version
// field at all, which decodes as 0.
func TestManagedBlockchain_PersistenceVersionMismatch(t *testing.T) {
	t.Parallel()

	const (
		region    = "us-east-1"
		accountID = "123456789012"
	)

	tests := []struct {
		mutate func(t *testing.T, raw map[string]any)
		name   string
	}{
		{
			name: "future_version",
			mutate: func(t *testing.T, raw map[string]any) {
				t.Helper()

				raw["version"] = 999
			},
		},
		{
			name: "missing_version",
			mutate: func(t *testing.T, raw map[string]any) {
				t.Helper()

				delete(raw, "version")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := managedblockchain.NewInMemoryBackend()

			_, _, err := b.CreateNetwork(
				region, accountID,
				"mismatch-net", "", "", "", "founder", "",
				nil, nil,
				"", "admin", "",
			)
			require.NoError(t, err)

			snap := b.Snapshot(t.Context())
			require.NotNil(t, snap)

			var raw map[string]any
			require.NoError(t, json.Unmarshal(snap, &raw))

			tt.mutate(t, raw)

			mutated, err := json.Marshal(raw)
			require.NoError(t, err)

			// Seed b2 with unrelated pre-existing state so a discarded
			// restore is observable as an actual reset, not merely "nothing
			// changed".
			b2 := managedblockchain.NewInMemoryBackend()
			b2.AddNetworkInternal(region, accountID, "pre-existing")

			err = b2.Restore(t.Context(), mutated)
			require.NoError(t, err)

			networks, err := b2.ListNetworks(managedblockchain.ListNetworksFilter{})
			require.NoError(t, err)
			assert.Empty(t, networks, "incompatible snapshot version must discard and reset, not partially decode")
		})
	}
}

// TestManagedBlockchain_PersistenceRoundTripAllResourceCounts verifies Snapshot/Restore
// preserves every resource collection (including nodes and the ARN index rebuilt from
// them), using the AddXInternal seed helpers and count helpers rather than the
// setup/verify table above.
func TestManagedBlockchain_PersistenceRoundTripAllResourceCounts(t *testing.T) {
	t.Parallel()

	b := managedblockchain.NewInMemoryBackend()
	n := b.AddNetworkInternal(testRegion, testAccountID, "persist-net")
	m := b.AddMemberInternal(testRegion, testAccountID, n.ID, "persist-member")
	node := b.AddNodeInternal(testRegion, testAccountID, n.ID, m.ID, "bc.t3.small.ethereum")
	accessor := b.AddAccessorInternal(testRegion, testAccountID, "BILLING_TOKEN", "ETHEREUM_MAINNET")
	b.AddProposalInternal(testRegion, testAccountID, n.ID, m.ID, "test proposal")
	b.AddInvitationInternal(testRegion, testAccountID, n.ID, "persist-net")

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := managedblockchain.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	assert.Equal(t, 1, managedblockchain.NetworkCount(b2))
	assert.Equal(t, 1, managedblockchain.MemberCount(b2))
	assert.Equal(t, 1, managedblockchain.NodeCount(b2))
	assert.Equal(t, 1, managedblockchain.AccessorCount(b2))
	assert.Equal(t, 1, managedblockchain.ProposalCount(b2))
	assert.Equal(t, 1, managedblockchain.InvitationCount(b2))

	// Network, member, node, accessor all in ARN index = 4
	assert.Equal(t, 4, managedblockchain.ARNIndexSize(b2))

	// Tags on node work after restore
	err := b2.TagResource(node.Arn, map[string]string{"restored": "true"})
	require.NoError(t, err)

	// Tags on accessor work after restore
	err = b2.TagResource(accessor.Arn, map[string]string{"restored": "true"})
	require.NoError(t, err)
}
