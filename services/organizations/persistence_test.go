package organizations_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/organizations"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *organizations.InMemoryBackend) string
		verify func(t *testing.T, b *organizations.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "org_and_account_round_trip",
			setup: func(b *organizations.InMemoryBackend) string {
				_, _, err := b.CreateOrganization("ALL")
				require.NoError(t, err)

				status, err := b.CreateAccount("test-account", "test@example.com", "", "", nil)
				require.NoError(t, err)

				return status.AccountID
			},
			verify: func(t *testing.T, b *organizations.InMemoryBackend, id string) {
				t.Helper()

				acct, err := b.DescribeAccount(id)
				require.NoError(t, err)
				assert.Equal(t, "test-account", acct.Name)
			},
		},
		{
			name: "ou_round_trip",
			setup: func(b *organizations.InMemoryBackend) string {
				_, _, err := b.CreateOrganization("ALL")
				require.NoError(t, err)

				roots, err := b.ListRoots()
				require.NoError(t, err)
				require.NotEmpty(t, roots)

				ou, err := b.CreateOrganizationalUnit(roots[0].ID, "eng-team", nil)
				require.NoError(t, err)

				return ou.ID
			},
			verify: func(t *testing.T, b *organizations.InMemoryBackend, id string) {
				t.Helper()

				ou, err := b.DescribeOrganizationalUnit(id)
				require.NoError(t, err)
				assert.Equal(t, "eng-team", ou.Name)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *organizations.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *organizations.InMemoryBackend, _ string) {
				t.Helper()

				_, err := b.DescribeOrganization()
				require.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := organizations.NewInMemoryBackend("000000000000", "us-east-1")
			id := tt.setup(original)

			snap := original.Snapshot(t.Context())
			require.NotNil(t, snap)

			fresh := organizations.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, fresh.Restore(t.Context(), snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := organizations.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

func TestOrganizationsHandler_Persistence(t *testing.T) {
	t.Parallel()

	backend := organizations.NewInMemoryBackend("000000000000", "us-east-1")
	h := organizations.NewHandler(backend)

	_, _, err := backend.CreateOrganization("ALL")
	require.NoError(t, err)

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := organizations.NewInMemoryBackend("000000000000", "us-east-1")
	freshH := organizations.NewHandler(fresh)
	require.NoError(t, freshH.Restore(t.Context(), snap))

	org, err := fresh.DescribeOrganization()
	require.NoError(t, err)
	assert.NotEmpty(t, org.ID)
}
