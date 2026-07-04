package organizations_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/organizations"
)

// TestOrganizationsChildrenIndexConsistency verifies that the accountChildrenByParent
// and ousByParent indexes stay consistent with accountParent / ouParent across account
// and OU lifecycle operations, and that ListChildren returns the indexed results.
func TestOrganizationsChildrenIndexConsistency(t *testing.T) {
	t.Parallel()

	newBackend := func(t *testing.T) (*organizations.InMemoryBackend, string) {
		t.Helper()

		b := organizations.NewInMemoryBackend("123456789012", "us-east-1")
		_, root, err := b.CreateOrganization("ALL")
		require.NoError(t, err)

		return b, root.ID
	}

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "account_created_appears_in_list_children",
			run: func(t *testing.T) {
				t.Helper()

				b, rootID := newBackend(t)

				// Create an account — should appear in root's ACCOUNT children.
				status, err := b.CreateAccount("alice", "alice@example.com", "OrganizationAccountAccessRole", "", nil)
				require.NoError(t, err)

				children, err := b.ListChildren(rootID, "ACCOUNT")
				require.NoError(t, err)

				ids := make(map[string]bool, len(children))
				for _, c := range children {
					ids[c.ID] = true
				}

				require.True(t, ids[status.AccountID])
			},
		},
		{
			name: "account_moved_updates_list_children",
			run: func(t *testing.T) {
				t.Helper()

				b, rootID := newBackend(t)

				ou, err := b.CreateOrganizationalUnit(rootID, "Engineering", nil)
				require.NoError(t, err)

				status, err := b.CreateAccount("bob", "bob@example.com", "OrganizationAccountAccessRole", "", nil)
				require.NoError(t, err)

				acctID := status.AccountID

				require.NoError(t, b.MoveAccount(acctID, rootID, ou.ID))

				// Should no longer be under root.
				rootChildren, err := b.ListChildren(rootID, "ACCOUNT")
				require.NoError(t, err)

				for _, c := range rootChildren {
					require.NotEqual(t, acctID, c.ID)
				}

				// Should be under the OU now.
				ouChildren, err := b.ListChildren(ou.ID, "ACCOUNT")
				require.NoError(t, err)

				found := false
				for _, c := range ouChildren {
					if c.ID == acctID {
						found = true
					}
				}

				require.True(t, found, "account not found under OU after MoveAccount")
			},
		},
		{
			name: "account_removed_clears_list_children",
			run: func(t *testing.T) {
				t.Helper()

				b, rootID := newBackend(t)

				// Need an OU to move the account into so we can remove it
				// (root accounts can't be directly removed in some paths; move first).
				ou, err := b.CreateOrganizationalUnit(rootID, "Temp", nil)
				require.NoError(t, err)

				status, err := b.CreateAccount("carol", "carol@example.com", "OrganizationAccountAccessRole", "", nil)
				require.NoError(t, err)

				acctID := status.AccountID

				require.NoError(t, b.MoveAccount(acctID, rootID, ou.ID))
				require.NoError(t, b.RemoveAccountFromOrganization(acctID))

				children, err := b.ListChildren(ou.ID, "ACCOUNT")
				require.NoError(t, err)

				for _, c := range children {
					require.NotEqual(t, acctID, c.ID)
				}
			},
		},
		{
			name: "ou_created_appears_in_list_children",
			run: func(t *testing.T) {
				t.Helper()

				b, rootID := newBackend(t)

				ou, err := b.CreateOrganizationalUnit(rootID, "Infra", nil)
				require.NoError(t, err)

				children, err := b.ListChildren(rootID, "ORGANIZATIONAL_UNIT")
				require.NoError(t, err)

				ids := make(map[string]bool, len(children))
				for _, c := range children {
					ids[c.ID] = true
				}

				require.True(t, ids[ou.ID])
			},
		},
		{
			name: "ou_deleted_clears_list_children",
			run: func(t *testing.T) {
				t.Helper()

				b, rootID := newBackend(t)

				ou, err := b.CreateOrganizationalUnit(rootID, "ToDelete", nil)
				require.NoError(t, err)

				require.NoError(t, b.DeleteOrganizationalUnit(ou.ID))

				children, err := b.ListChildren(rootID, "ORGANIZATIONAL_UNIT")
				require.NoError(t, err)

				for _, c := range children {
					require.NotEqual(t, ou.ID, c.ID)
				}
			},
		},
		{
			name: "delete_ou_with_accounts_rejected",
			run: func(t *testing.T) {
				t.Helper()

				b, rootID := newBackend(t)

				ou, err := b.CreateOrganizationalUnit(rootID, "HasAccounts", nil)
				require.NoError(t, err)

				status, err := b.CreateAccount("dave", "dave@example.com", "OrganizationAccountAccessRole", "", nil)
				require.NoError(t, err)

				require.NoError(t, b.MoveAccount(status.AccountID, rootID, ou.ID))

				// Deletion must be rejected — OU still has an account child.
				err = b.DeleteOrganizationalUnit(ou.ID)
				require.Error(t, err)
			},
		},
		{
			name: "delete_ou_with_child_ous_rejected",
			run: func(t *testing.T) {
				t.Helper()

				b, rootID := newBackend(t)

				parent, err := b.CreateOrganizationalUnit(rootID, "Parent", nil)
				require.NoError(t, err)

				_, err = b.CreateOrganizationalUnit(parent.ID, "Child", nil)
				require.NoError(t, err)

				err = b.DeleteOrganizationalUnit(parent.ID)
				require.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

// BenchmarkListChildrenAccounts measures ListChildren for ACCOUNT type under the
// indexed path, showing O(k) retrieval independent of total account count.
func BenchmarkListChildrenAccounts(b *testing.B) {
	backend := organizations.NewInMemoryBackend("123456789012", "us-east-1")
	_, root, err := backend.CreateOrganization("ALL")
	if err != nil {
		b.Fatal(err)
	}

	ou, err := backend.CreateOrganizationalUnit(root.ID, "BenchOU", nil)
	if err != nil {
		b.Fatal(err)
	}

	const n = 200
	for i := range n {
		name := "bench" + string(rune('a'+i/26%26)) + string(rune('a'+i%26))
		email := name + "@example.com"

		acct, createErr := backend.CreateAccount(name, email, "Role", "", nil)
		if createErr != nil {
			b.Fatal(createErr)
		}

		if moveErr := backend.MoveAccount(acct.AccountID, root.ID, ou.ID); moveErr != nil {
			b.Fatal(moveErr)
		}
	}

	b.ResetTimer()

	for range b.N {
		children, listErr := backend.ListChildren(ou.ID, "ACCOUNT")
		if listErr != nil {
			b.Fatal(listErr)
		}

		if len(children) != n {
			b.Fatalf("got %d children, want %d", len(children), n)
		}
	}
}
