package organizations_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/organizations"
)

// TestAccountPaths verifies the account.Paths format computed at read time:
// "o-<org>/r-<root>/(ou-.../)*<accountID>/", matching the AWS API Reference
// example responses for DescribeAccount.
func TestAccountPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		depth int // number of OUs to nest the account under before reading it
	}{
		{name: "directly_under_root", depth: 0},
		{name: "nested_three_ous_deep", depth: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := organizations.NewInMemoryBackend("000000000000", "us-east-1")
			org, root, err := b.CreateOrganization("ALL")
			require.NoError(t, err)

			status, err := b.CreateAccount("member", "member@example.com", "", "", nil)
			require.NoError(t, err)
			acctID := status.AccountID

			ouIDs := make([]string, 0, tt.depth)
			parentID := root.ID

			for i := range tt.depth {
				ou, ouErr := b.CreateOrganizationalUnit(parentID, fmt.Sprintf("ou-%d", i), nil)
				require.NoError(t, ouErr)
				ouIDs = append(ouIDs, ou.ID)
				parentID = ou.ID
			}

			if tt.depth > 0 {
				require.NoError(t, b.MoveAccount(acctID, root.ID, parentID))
			}

			wantSegments := append([]string{org.ID, root.ID}, ouIDs...)
			wantSegments = append(wantSegments, acctID)
			wantPath := joinSlash(wantSegments) + "/"

			acct, err := b.DescribeAccount(acctID)
			require.NoError(t, err)
			assert.Equal(t, []string{wantPath}, acct.Paths)

			// ListAccounts must carry the same computed Paths through.
			all, err := b.ListAccounts()
			require.NoError(t, err)

			found := false

			for _, a := range all {
				if a.ID == acctID {
					found = true

					assert.Equal(t, []string{wantPath}, a.Paths)
				}
			}

			require.True(t, found, "account must appear in ListAccounts")
		})
	}
}

// TestOUPath verifies the OU.Path format: "o-<org>/r-<root>/(ou-.../)*<ownOUID>/".
func TestOUPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		depth int // OU nesting depth of the OU under test (1 = directly under root)
	}{
		{name: "directly_under_root", depth: 1},
		{name: "nested_three_deep", depth: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := organizations.NewInMemoryBackend("000000000000", "us-east-1")
			org, root, err := b.CreateOrganization("ALL")
			require.NoError(t, err)

			var ouIDs []string

			parentID := root.ID

			var leaf *organizations.OrganizationalUnit

			for i := range tt.depth {
				ou, ouErr := b.CreateOrganizationalUnit(parentID, fmt.Sprintf("ou-%d", i), nil)
				require.NoError(t, ouErr)
				ouIDs = append(ouIDs, ou.ID)
				parentID = ou.ID
				leaf = ou
			}

			wantPath := joinSlash(append([]string{org.ID, root.ID}, ouIDs...)) + "/"

			// CreateOrganizationalUnit's own response must already carry Path.
			assert.Equal(t, wantPath, leaf.Path)

			desc, err := b.DescribeOrganizationalUnit(leaf.ID)
			require.NoError(t, err)
			assert.Equal(t, wantPath, desc.Path)

			updated, err := b.UpdateOrganizationalUnit(leaf.ID, "renamed")
			require.NoError(t, err)
			assert.Equal(t, wantPath, updated.Path)

			siblings, err := b.ListOrganizationalUnitsForParent(root.ID)
			require.NoError(t, err)

			if tt.depth == 1 {
				require.Len(t, siblings, 1)
				assert.Equal(t, wantPath, siblings[0].Path)
			}
		})
	}
}

// TestAccountPathsForParent verifies ListAccountsForParent also carries the
// computed Paths through.
func TestAccountPathsForParent(t *testing.T) {
	t.Parallel()

	b := organizations.NewInMemoryBackend("000000000000", "us-east-1")
	org, root, err := b.CreateOrganization("ALL")
	require.NoError(t, err)

	status, err := b.CreateAccount("member", "member@example.com", "", "", nil)
	require.NoError(t, err)

	accts, err := b.ListAccountsForParent(root.ID)
	require.NoError(t, err)

	found := false

	for _, a := range accts {
		if a.ID == status.AccountID {
			found = true

			assert.Equal(t, []string{joinSlash([]string{org.ID, root.ID, a.ID}) + "/"}, a.Paths)
		}
	}

	require.True(t, found)
}

// TestPathsDetachedOrCyclic verifies deterministic behaviour when the
// persisted parent-chain data is malformed: a dangling parent reference (an
// orphan) or a cycle. Neither can occur through normal API use -- MoveAccount
// only targets an existing root/OU, and DeleteOrganizationalUnit refuses to
// remove a non-empty OU -- so this exercises the only real-world way such
// state can appear: a hand-edited or corrupted snapshot loaded via Restore.
func TestPathsDetachedOrCyclic(t *testing.T) {
	t.Parallel()

	t.Run("orphan_account_parent_yields_nil_paths", func(t *testing.T) {
		t.Parallel()

		b := organizations.NewInMemoryBackend("000000000000", "us-east-1")
		_, _, err := b.CreateOrganization("ALL")
		require.NoError(t, err)

		status, err := b.CreateAccount("member", "member@example.com", "", "", nil)
		require.NoError(t, err)
		acctID := status.AccountID

		data := b.Snapshot(t.Context())
		require.NotNil(t, data)

		snap := organizations.NewBackendSnapshot()
		require.NoError(t, json.Unmarshal(data, snap))
		snap.AccountParent[acctID] = "ou-doesnotexist-00000000"

		corrupted, err := json.Marshal(snap)
		require.NoError(t, err)

		fresh := organizations.NewInMemoryBackend("000000000000", "us-east-1")
		require.NoError(t, fresh.Restore(t.Context(), corrupted))

		acct, err := fresh.DescribeAccount(acctID)
		require.NoError(t, err)
		assert.Nil(t, acct.Paths)
	})

	t.Run("cyclic_ou_parents_yield_empty_path", func(t *testing.T) {
		t.Parallel()

		b := organizations.NewInMemoryBackend("000000000000", "us-east-1")
		_, root, err := b.CreateOrganization("ALL")
		require.NoError(t, err)

		ouA, err := b.CreateOrganizationalUnit(root.ID, "a", nil)
		require.NoError(t, err)
		ouB, err := b.CreateOrganizationalUnit(root.ID, "b", nil)
		require.NoError(t, err)

		data := b.Snapshot(t.Context())
		require.NotNil(t, data)

		snap := organizations.NewBackendSnapshot()
		require.NoError(t, json.Unmarshal(data, snap))

		var ous []organizations.OrganizationalUnit
		require.NoError(t, json.Unmarshal(snap.Tables["ous"], &ous))

		for i := range ous {
			switch ous[i].ID {
			case ouA.ID:
				ous[i].ParentID = ouB.ID
			case ouB.ID:
				ous[i].ParentID = ouA.ID
			}
		}

		rawOUs, err := json.Marshal(ous)
		require.NoError(t, err)
		snap.Tables["ous"] = rawOUs

		snap.OUParent[ouA.ID] = ouB.ID
		snap.OUParent[ouB.ID] = ouA.ID

		corrupted, err := json.Marshal(snap)
		require.NoError(t, err)

		fresh := organizations.NewInMemoryBackend("000000000000", "us-east-1")
		require.NoError(t, fresh.Restore(t.Context(), corrupted))

		descA, err := fresh.DescribeOrganizationalUnit(ouA.ID)
		require.NoError(t, err)
		assert.Empty(t, descA.Path)

		descB, err := fresh.DescribeOrganizationalUnit(ouB.ID)
		require.NoError(t, err)
		assert.Empty(t, descB.Path)
	})
}

func joinSlash(segments []string) string {
	return strings.Join(segments, "/")
}
