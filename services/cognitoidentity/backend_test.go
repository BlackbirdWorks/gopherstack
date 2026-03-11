package cognitoidentity_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidentity"
)

func newTestBackend() *cognitoidentity.InMemoryBackend {
	return cognitoidentity.NewInMemoryBackend("000000000000", "us-east-1")
}

func TestInMemoryBackend_CreateIdentityPool(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget   error
		name        string
		poolName    string
		wantErr     bool
		allowUnauth bool
	}{
		{
			name:        "success",
			poolName:    "my-pool",
			allowUnauth: true,
		},
		{
			name:      "empty_name",
			poolName:  "",
			wantErr:   true,
			errTarget: cognitoidentity.ErrInvalidParameter,
		},
		{
			name:      "duplicate_name",
			poolName:  "my-pool",
			wantErr:   true,
			errTarget: cognitoidentity.ErrIdentityPoolAlreadyExists,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			if tt.name == "duplicate_name" {
				_, setupErr := b.CreateIdentityPool("my-pool", true, false, nil, nil, nil)
				require.NoError(t, setupErr)
			}

			pool, err := b.CreateIdentityPool(tt.poolName, tt.allowUnauth, false, nil, nil, nil)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, pool.IdentityPoolID)
			assert.Equal(t, tt.poolName, pool.IdentityPoolName)
			assert.Contains(t, pool.IdentityPoolID, "us-east-1:")
		})
	}
}

func TestInMemoryBackend_DeleteIdentityPool(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget error
		name      string
		poolID    string
		wantErr   bool
	}{
		{
			name:   "success",
			poolID: "pool-to-delete",
		},
		{
			name:      "not_found",
			poolID:    "nonexistent-pool",
			wantErr:   true,
			errTarget: cognitoidentity.ErrIdentityPoolNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			var realPoolID string

			if tt.name == "success" {
				pool, setupErr := b.CreateIdentityPool("delete-pool", true, false, nil, nil, nil)
				require.NoError(t, setupErr)
				realPoolID = pool.IdentityPoolID
			} else {
				realPoolID = tt.poolID
			}

			err := b.DeleteIdentityPool(realPoolID)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			require.NoError(t, err)

			_, descErr := b.DescribeIdentityPool(realPoolID)
			require.Error(t, descErr)
		})
	}
}

func TestInMemoryBackend_DescribeIdentityPool(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget error
		name      string
		poolID    string
		wantErr   bool
	}{
		{
			name:   "success",
			poolID: "real",
		},
		{
			name:      "not_found",
			poolID:    "nonexistent",
			wantErr:   true,
			errTarget: cognitoidentity.ErrIdentityPoolNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			var poolID string

			if tt.name == "success" {
				pool, setupErr := b.CreateIdentityPool("describe-pool", true, false, nil, nil, nil)
				require.NoError(t, setupErr)
				poolID = pool.IdentityPoolID
			} else {
				poolID = tt.poolID
			}

			pool, err := b.DescribeIdentityPool(poolID)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, poolID, pool.IdentityPoolID)
			assert.Equal(t, "describe-pool", pool.IdentityPoolName)
		})
	}
}

func TestInMemoryBackend_ListIdentityPools(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	_, err1 := b.CreateIdentityPool("pool-a", true, false, nil, nil, nil)
	require.NoError(t, err1)

	_, err2 := b.CreateIdentityPool("pool-b", false, false, nil, nil, nil)
	require.NoError(t, err2)

	pools := b.ListIdentityPools(0)
	assert.Len(t, pools, 2)

	limited := b.ListIdentityPools(1)
	assert.Len(t, limited, 1)
}

func TestInMemoryBackend_UpdateIdentityPool(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget error
		name      string
		wantErr   bool
	}{
		{name: "success"},
		{
			name:      "not_found",
			wantErr:   true,
			errTarget: cognitoidentity.ErrIdentityPoolNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			var poolID string

			if tt.name == "success" {
				pool, setupErr := b.CreateIdentityPool("update-pool", true, false, nil, nil, nil)
				require.NoError(t, setupErr)
				poolID = pool.IdentityPoolID
			} else {
				poolID = "nonexistent"
			}

			updated, err := b.UpdateIdentityPool(poolID, "update-pool", false, true, nil, nil)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			require.NoError(t, err)
			assert.False(t, updated.AllowUnauthenticatedIdentities)
			assert.True(t, updated.AllowClassicFlow)
		})
	}
}

func TestInMemoryBackend_GetID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget error
		name      string
		wantErr   bool
	}{
		{name: "success_new_identity"},
		{name: "success_existing_identity"},
		{
			name:      "pool_not_found",
			wantErr:   true,
			errTarget: cognitoidentity.ErrIdentityPoolNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			var poolID string

			if tt.name != "pool_not_found" {
				pool, setupErr := b.CreateIdentityPool("get-id-pool", true, false, nil, nil, nil)
				require.NoError(t, setupErr)
				poolID = pool.IdentityPoolID
			} else {
				poolID = "nonexistent"
			}

			logins := map[string]string{"cognito-idp.us-east-1.amazonaws.com/us-east-1_xxx": "token123"}
			identity, err := b.GetID(poolID, "000000000000", logins)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, identity.IdentityID)
			assert.Contains(t, identity.IdentityID, "us-east-1:")

			if tt.name == "success_existing_identity" {
				identity2, err2 := b.GetID(poolID, "000000000000", logins)
				require.NoError(t, err2)
				assert.Equal(t, identity.IdentityID, identity2.IdentityID)
			}
		})
	}
}

func TestInMemoryBackend_GetCredentialsForIdentity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget error
		name      string
		wantErr   bool
	}{
		{name: "success"},
		{
			name:      "identity_not_found",
			wantErr:   true,
			errTarget: cognitoidentity.ErrIdentityPoolNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			var identityID string

			if tt.name == "success" {
				pool, poolErr := b.CreateIdentityPool("creds-pool", true, false, nil, nil, nil)
				require.NoError(t, poolErr)

				identity, idErr := b.GetID(pool.IdentityPoolID, "000000000000", nil)
				require.NoError(t, idErr)
				identityID = identity.IdentityID
			} else {
				identityID = "us-east-1:nonexistent"
			}

			creds, err := b.GetCredentialsForIdentity(identityID, nil)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, creds.AccessKeyID)
			assert.NotEmpty(t, creds.SecretAccessKey)
			assert.NotEmpty(t, creds.SessionToken)
			assert.Equal(t, identityID, creds.IdentityID)
		})
	}
}

func TestInMemoryBackend_GetOpenIDToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget error
		name      string
		wantErr   bool
	}{
		{name: "success"},
		{
			name:      "identity_not_found",
			wantErr:   true,
			errTarget: cognitoidentity.ErrIdentityPoolNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			var identityID string

			if tt.name == "success" {
				pool, poolErr := b.CreateIdentityPool("oidc-pool", true, false, nil, nil, nil)
				require.NoError(t, poolErr)

				identity, idErr := b.GetID(pool.IdentityPoolID, "000000000000", nil)
				require.NoError(t, idErr)
				identityID = identity.IdentityID
			} else {
				identityID = "us-east-1:nonexistent"
			}

			token, err := b.GetOpenIDToken(identityID, nil)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, token.Token)
			assert.Equal(t, identityID, token.IdentityID)
		})
	}
}

func TestInMemoryBackend_SetGetIdentityPoolRoles(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget error
		name      string
		wantErr   bool
	}{
		{name: "success"},
		{
			name:      "pool_not_found",
			wantErr:   true,
			errTarget: cognitoidentity.ErrIdentityPoolNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			var poolID string

			if tt.name == "success" {
				pool, setupErr := b.CreateIdentityPool("roles-pool", true, false, nil, nil, nil)
				require.NoError(t, setupErr)
				poolID = pool.IdentityPoolID
			} else {
				poolID = "nonexistent"
			}

			authRoleARN := "arn:aws:iam::000000000000:role/CognitoAuthRole"
			unauthRoleARN := "arn:aws:iam::000000000000:role/CognitoUnauthRole"

			setErr := b.SetIdentityPoolRoles(poolID, authRoleARN, unauthRoleARN)

			if tt.wantErr {
				require.Error(t, setErr)
				assert.ErrorIs(t, setErr, tt.errTarget)

				return
			}

			require.NoError(t, setErr)

			roles, getErr := b.GetIdentityPoolRoles(poolID)
			require.NoError(t, getErr)
			assert.Equal(t, authRoleARN, roles.AuthenticatedRoleARN)
			assert.Equal(t, unauthRoleARN, roles.UnauthenticatedRoleARN)
		})
	}
}

func TestInMemoryBackend_GetIdentityPoolRoles_NoRoles(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	pool, err := b.CreateIdentityPool("no-roles-pool", true, false, nil, nil, nil)
	require.NoError(t, err)

	roles, err := b.GetIdentityPoolRoles(pool.IdentityPoolID)
	require.NoError(t, err)
	assert.Empty(t, roles.AuthenticatedRoleARN)
	assert.Empty(t, roles.UnauthenticatedRoleARN)
}

func TestInMemoryBackend_Region(t *testing.T) {
	t.Parallel()

	b := cognitoidentity.NewInMemoryBackend("000000000000", "eu-west-1")
	assert.Equal(t, "eu-west-1", b.Region())
}

func TestInMemoryBackend_UpdateIdentityPool_RenameConflict(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	pool1, err := b.CreateIdentityPool("pool-one", true, false, nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateIdentityPool("pool-two", true, false, nil, nil, nil)
	require.NoError(t, err)

	// Attempt to rename pool-one to pool-two (conflict).
	_, err = b.UpdateIdentityPool(pool1.IdentityPoolID, "pool-two", true, false, nil, nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, cognitoidentity.ErrIdentityPoolAlreadyExists)
}

func TestInMemoryBackend_DeleteIdentityPool_CleansIdentities(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	pool, err := b.CreateIdentityPool("clean-pool", true, false, nil, nil, nil)
	require.NoError(t, err)

	// Create an identity inside the pool.
	identity, err := b.GetID(pool.IdentityPoolID, "000000000000", nil)
	require.NoError(t, err)
	require.NotEmpty(t, identity.IdentityID)

	// Delete the pool.
	require.NoError(t, b.DeleteIdentityPool(pool.IdentityPoolID))

	// Pool should be gone.
	_, err = b.DescribeIdentityPool(pool.IdentityPoolID)
	require.Error(t, err)
	assert.ErrorIs(t, err, cognitoidentity.ErrIdentityPoolNotFound)
}

func TestInMemoryBackend_GetIdentityPoolRoles_NotFound(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	_, err := b.GetIdentityPoolRoles("us-east-1:nonexistent")
	require.Error(t, err)
	assert.ErrorIs(t, err, cognitoidentity.ErrIdentityPoolNotFound)
}

func TestInMemoryBackend_SetIdentityPoolRoles_NotFound(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	err := b.SetIdentityPoolRoles("us-east-1:nonexistent", "arn:aws:iam::000000000000:role/Auth", "")
	require.Error(t, err)
	assert.ErrorIs(t, err, cognitoidentity.ErrIdentityPoolNotFound)
}

func TestInMemoryBackend_CreateIdentityPool_WithProviders(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	providers := []cognitoidentity.IdentityProvider{
		{
			ProviderName:         "cognito-idp.us-east-1.amazonaws.com/us-east-1_xxx",
			ClientID:             "client123",
			ServerSideTokenCheck: true,
		},
	}

	pool, err := b.CreateIdentityPool("provider-pool", true, false, providers, map[string]string{
		"graph.facebook.com": "123456789",
	}, map[string]string{"env": "test"})
	require.NoError(t, err)
	assert.Len(t, pool.IdentityProviders, 1)
	assert.Equal(t, "client123", pool.IdentityProviders[0].ClientID)
	assert.Equal(t, "123456789", pool.SupportedLoginProviders["graph.facebook.com"])
	assert.Equal(t, "test", pool.Tags["env"])
}
