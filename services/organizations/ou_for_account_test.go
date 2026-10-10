package organizations_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/organizations"
)

func TestOrganizationalUnitIDsForAccount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		lookup  string
		nested  bool
		wantErr bool
	}{
		{name: "nested", nested: true},
		{name: "at_root"},
		{name: "unknown", lookup: "999999999999", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := organizations.NewInMemoryBackend("000000000000", "us-east-1")
			_, root, err := b.CreateOrganization("ALL")
			require.NoError(t, err)

			outer, err := b.CreateOrganizationalUnit(root.ID, "outer", nil)
			require.NoError(t, err)

			inner, err := b.CreateOrganizationalUnit(outer.ID, "inner", nil)
			require.NoError(t, err)

			status, err := b.CreateAccount("n@example.com", "n", "OrganizationAccountAccessRole", "ALLOW", nil)
			require.NoError(t, err)

			var want []string

			if tt.nested {
				require.NoError(t, b.MoveAccount(status.AccountID, root.ID, inner.ID))

				want = []string{inner.ID, outer.ID}
			}

			account := tt.lookup
			if account == "" {
				account = status.AccountID
			}

			got, gotErr := b.OrganizationalUnitIDsForAccount(account)
			if tt.wantErr {
				require.Error(t, gotErr)

				return
			}

			require.NoError(t, gotErr)
			assert.Equal(t, want, got)
		})
	}
}
