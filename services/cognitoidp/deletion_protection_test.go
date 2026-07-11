package cognitoidp_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

// Test_DeleteUserPool_DeletionProtection proves DeleteUserPool refuses to delete a pool
// whose DeletionProtection is "ACTIVE" (matching real Cognito's InvalidParameterException),
// while a pool left at the default ("INACTIVE") or explicitly deactivated deletes normally.
func Test_DeleteUserPool_DeletionProtection(t *testing.T) {
	t.Parallel()

	cases := []struct {
		wantErr            error
		name               string
		deletionProtection string
	}{
		{
			name:               "default (unset) deletes normally",
			deletionProtection: "",
			wantErr:            nil,
		},
		{
			name:               "INACTIVE deletes normally",
			deletionProtection: "INACTIVE",
			wantErr:            nil,
		},
		{
			name:               "ACTIVE refuses deletion",
			deletionProtection: "ACTIVE",
			wantErr:            cognitoidp.ErrInvalidParameter,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := cognitoidp.NewInMemoryBackend("000000000000", "us-east-1", "http://localhost:8000")

			pool, err := b.CreateUserPoolWithOpts("dp-pool-"+sanitizeTestName(tc.name), cognitoidp.UserPoolOptions{
				DeletionProtection: tc.deletionProtection,
			})
			require.NoError(t, err)

			deleteErr := b.DeleteUserPool(pool.ID)

			if tc.wantErr == nil {
				require.NoError(t, deleteErr)
				assert.Equal(t, 0, b.UserPoolCount())

				return
			}

			require.ErrorIs(t, deleteErr, tc.wantErr)
			assert.Equal(t, 1, b.UserPoolCount(), "pool must survive a refused delete")
		})
	}
}

// Test_DeleteUserPool_DeletionProtection_CanBeDisabledThenDeleted proves the documented
// remediation path: flipping DeletionProtection back to INACTIVE via UpdateUserPool allows
// the subsequent delete to succeed.
func Test_DeleteUserPool_DeletionProtection_CanBeDisabledThenDeleted(t *testing.T) {
	t.Parallel()

	b := cognitoidp.NewInMemoryBackend("000000000000", "us-east-1", "http://localhost:8000")

	pool, err := b.CreateUserPoolWithOpts("dp-toggle-pool", cognitoidp.UserPoolOptions{
		DeletionProtection: "ACTIVE",
	})
	require.NoError(t, err)

	require.ErrorIs(t, b.DeleteUserPool(pool.ID), cognitoidp.ErrInvalidParameter)

	require.NoError(t, b.UpdateUserPoolWithOpts(pool.ID, "", cognitoidp.UserPoolOptions{
		DeletionProtection: "INACTIVE",
	}))

	require.NoError(t, b.DeleteUserPool(pool.ID))
}
