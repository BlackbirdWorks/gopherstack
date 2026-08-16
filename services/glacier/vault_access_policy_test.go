package glacier_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glacier"
)

func TestAccessPolicyStore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		policy string
	}{
		{
			name:   "set_and_get",
			policy: `{"Version":"2012-10-17","Statement":[]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := glacier.NewInMemoryBackend()
			_, err := bk.CreateVault(testAccountID, testRegion, "vault")
			require.NoError(t, err)

			err = bk.SetVaultAccessPolicy(testAccountID, testRegion, "vault", tt.policy)
			require.NoError(t, err)

			policy, err := bk.GetVaultAccessPolicy(testAccountID, testRegion, "vault")
			require.NoError(t, err)
			assert.Equal(t, tt.policy, policy)

			err = bk.DeleteVaultAccessPolicy(testAccountID, testRegion, "vault")
			require.NoError(t, err)

			policy, err = bk.GetVaultAccessPolicy(testAccountID, testRegion, "vault")
			require.NoError(t, err)
			assert.Empty(t, policy)
		})
	}
}

// retrieval job starts InProgress and is only promoted to Succeeded once the simulated
// retrieval window has elapsed, matching AWS's asynchronous retrieval semantics.
