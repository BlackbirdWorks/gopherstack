package managedblockchain_test

import (
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
				)
				require.NoError(t, err)

				_, err = b.CreateMember(region, accountID, n.ID, "member2", "second member", nil)
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
					"prop-net", "", "", "", "founder", "", nil, nil)
				require.NoError(t, err)

				_, err = b.CreateProposal(region, accountID, n.ID, m.ID, "test proposal", nil, nil)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *managedblockchain.InMemoryBackend) {
				t.Helper()

				networks, err := b.ListNetworks(managedblockchain.ListNetworksFilter{})
				require.NoError(t, err)
				require.Len(t, networks, 1)

				proposals, err := b.ListProposals(networks[0].ID)
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
