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
				_, setupErr := b.CreateIdentityPool("my-pool", true, false, "", nil, nil, nil)
				require.NoError(t, setupErr)
			}

			pool, err := b.CreateIdentityPool(tt.poolName, tt.allowUnauth, false, "", nil, nil, nil)

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
				pool, setupErr := b.CreateIdentityPool("delete-pool", true, false, "", nil, nil, nil)
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
				pool, setupErr := b.CreateIdentityPool("describe-pool", true, false, "", nil, nil, nil)
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

	_, err1 := b.CreateIdentityPool("pool-a", true, false, "", nil, nil, nil)
	require.NoError(t, err1)

	_, err2 := b.CreateIdentityPool("pool-b", false, false, "", nil, nil, nil)
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
				pool, setupErr := b.CreateIdentityPool("update-pool", true, false, "", nil, nil, nil)
				require.NoError(t, setupErr)
				poolID = pool.IdentityPoolID
			} else {
				poolID = "nonexistent"
			}

			updated, err := b.UpdateIdentityPool(poolID, "update-pool", false, true, "", nil, nil, nil)

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
				pool, setupErr := b.CreateIdentityPool("get-id-pool", true, false, "", nil, nil, nil)
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
				pool, poolErr := b.CreateIdentityPool("creds-pool", true, false, "", nil, nil, nil)
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
				pool, poolErr := b.CreateIdentityPool("oidc-pool", true, false, "", nil, nil, nil)
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
				pool, setupErr := b.CreateIdentityPool("roles-pool", true, false, "", nil, nil, nil)
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

	pool, err := b.CreateIdentityPool("no-roles-pool", true, false, "", nil, nil, nil)
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

	pool1, err := b.CreateIdentityPool("pool-one", true, false, "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateIdentityPool("pool-two", true, false, "", nil, nil, nil)
	require.NoError(t, err)

	// Attempt to rename pool-one to pool-two (conflict).
	_, err = b.UpdateIdentityPool(pool1.IdentityPoolID, "pool-two", true, false, "", nil, nil, nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, cognitoidentity.ErrIdentityPoolAlreadyExists)
}

func TestInMemoryBackend_DeleteIdentityPool_CleansIdentities(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	pool, err := b.CreateIdentityPool("clean-pool", true, false, "", nil, nil, nil)
	require.NoError(t, err)

	// Create an identity inside the pool.
	identity, err := b.GetID(pool.IdentityPoolID, "000000000000", nil)
	require.NoError(t, err)
	require.NotEmpty(t, identity.IdentityID)

	// Delete the pool.
	require.NoError(t, b.DeleteIdentityPool(pool.IdentityPoolID))

	// Pool should be gone.
	_, err = b.DescribeIdentityPool(pool.IdentityPoolID)
	require.ErrorIs(t, err, cognitoidentity.ErrIdentityPoolNotFound)

	// Identity from the deleted pool should no longer be usable.
	_, err = b.GetCredentialsForIdentity(identity.IdentityID, nil)
	require.ErrorIs(t, err, cognitoidentity.ErrIdentityPoolNotFound)
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

	pool, err := b.CreateIdentityPool("provider-pool", true, false, "", providers, map[string]string{
		"graph.facebook.com": "123456789",
	}, map[string]string{"env": "test"})
	require.NoError(t, err)
	assert.Len(t, pool.IdentityProviders, 1)
	assert.Equal(t, "client123", pool.IdentityProviders[0].ClientID)
	assert.Equal(t, "123456789", pool.SupportedLoginProviders["graph.facebook.com"])
	assert.Equal(t, "test", pool.Tags["env"])
}

func TestInMemoryBackend_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	pool, err := b.CreateIdentityPool("persist-pool", true, false, "", nil, nil, map[string]string{
		"env": "prod",
	})
	require.NoError(t, err)

	_, err = b.GetID(pool.IdentityPoolID, "000000000000", map[string]string{
		"accounts.google.com": "google-token",
	})
	require.NoError(t, err)

	_, err = b.SetPrincipalTagAttributeMap(
		pool.IdentityPoolID,
		"cognito-idp.us-east-1.amazonaws.com/us-east-1_xxx",
		false,
		map[string]string{"sub": "user_id"},
	)
	require.NoError(t, err)

	snap := b.Snapshot()
	require.NotEmpty(t, snap)

	b2 := cognitoidentity.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(snap))

	restored, err := b2.DescribeIdentityPool(pool.IdentityPoolID)
	require.NoError(t, err)
	assert.Equal(t, "persist-pool", restored.IdentityPoolName)
	assert.Equal(t, "prod", restored.Tags["env"])

	result, err := b2.ListIdentities(pool.IdentityPoolID, 10, false)
	require.NoError(t, err)
	assert.Len(t, result.Identities, 1)

	mapping, err := b2.GetPrincipalTagAttributeMap(
		pool.IdentityPoolID,
		"cognito-idp.us-east-1.amazonaws.com/us-east-1_xxx",
	)
	require.NoError(t, err)
	assert.Equal(t, "user_id", mapping.PrincipalTags["sub"])
}

func TestHandler_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	b := cognitoidentity.NewInMemoryBackend("000000000000", "us-east-1")
	h := cognitoidentity.NewHandler(b, "us-east-1")

	_, err := b.CreateIdentityPool("handler-persist-pool", true, false, "", nil, nil, nil)
	require.NoError(t, err)

	snap := h.Snapshot()
	require.NotEmpty(t, snap)

	b2 := cognitoidentity.NewInMemoryBackend("000000000000", "us-east-1")
	h2 := cognitoidentity.NewHandler(b2, "us-east-1")

	require.NoError(t, h2.Restore(snap))

	pools := b2.ListIdentityPools(0)
	assert.Len(t, pools, 1)
	assert.Equal(t, "handler-persist-pool", pools[0].IdentityPoolName)
}

func TestInMemoryBackend_DeleteIdentities_UnprocessedNil(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	pool, err := b.CreateIdentityPool("del-id-pool", true, false, "", nil, nil, nil)
	require.NoError(t, err)

	identity, err := b.GetID(pool.IdentityPoolID, "000000000000", nil)
	require.NoError(t, err)

	unprocessed, err := b.DeleteIdentities([]string{identity.IdentityID})
	require.NoError(t, err)
	assert.Empty(t, unprocessed)

	_, descErr := b.GetCredentialsForIdentity(identity.IdentityID, nil)
	require.Error(t, descErr)
}

func TestInMemoryBackend_DeveloperLoginsFrom_EmptyProviderName(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	pool, err := b.CreateIdentityPool("dev-logins-pool", true, false, "", nil, nil, nil)
	require.NoError(t, err)

	devRec, err := b.GetOpenIDTokenForDeveloperIdentity(
		pool.IdentityPoolID,
		"",
		map[string]string{"developer.example.com": "user-001"},
		0,
	)
	require.NoError(t, err)
	assert.NotEmpty(t, devRec.IdentityID)

	// LookupDeveloperIdentity with empty provider name returns all dev user IDs.
	result, err := b.LookupDeveloperIdentity(
		pool.IdentityPoolID,
		devRec.IdentityID,
		"",
		"",
	)
	require.NoError(t, err)
	assert.NotEmpty(t, result.DeveloperUserIdentifierList)
}

func TestInMemoryBackend_UnlinkIdentity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget             error
		logins                map[string]string
		name                  string
		identityID            string
		setupIdentityProvider string
		wantProviderRemoved   string
		loginsToRemove        []string
		wantErr               bool
	}{
		{
			name:                  "success",
			identityID:            "use-created-identity",
			logins:                map[string]string{"accounts.google.com": "google-token"},
			loginsToRemove:        []string{"accounts.google.com"},
			setupIdentityProvider: "accounts.google.com",
			wantProviderRemoved:   "accounts.google.com",
		},
		{
			name:       "identity_not_found",
			identityID: "us-east-1:missing",
			logins:     map[string]string{"accounts.google.com": "google-token"},
			loginsToRemove: []string{
				"accounts.google.com",
			},
			wantErr:   true,
			errTarget: cognitoidentity.ErrIdentityPoolNotFound,
		},
		{
			name:   "missing_identity_id",
			logins: map[string]string{"accounts.google.com": "google-token"},
			loginsToRemove: []string{
				"accounts.google.com",
			},
			wantErr:   true,
			errTarget: cognitoidentity.ErrInvalidParameter,
		},
		{
			name:       "missing_logins_to_remove",
			identityID: "use-created-identity",
			logins:     map[string]string{"accounts.google.com": "google-token"},
			wantErr:    true,
			errTarget:  cognitoidentity.ErrInvalidParameter,
		},
		{
			name:       "token_mismatch",
			identityID: "use-created-identity",
			logins:     map[string]string{"accounts.google.com": "wrong-token"},
			loginsToRemove: []string{
				"accounts.google.com",
			},
			wantErr:   true,
			errTarget: cognitoidentity.ErrNotAuthorized,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			pool, err := b.CreateIdentityPool("unlink-identity-pool-"+tt.name, true, false, "", nil, nil, nil)
			require.NoError(t, err)

			identity, err := b.GetID(pool.IdentityPoolID, "000000000000", map[string]string{
				"accounts.google.com": "google-token",
				"graph.facebook.com":  "facebook-token",
			})
			require.NoError(t, err)

			identityID := tt.identityID
			if identityID == "use-created-identity" {
				identityID = identity.IdentityID
			}

			err = b.UnlinkIdentity(identityID, tt.logins, tt.loginsToRemove)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			require.NoError(t, err)

			desc, err := b.DescribeIdentity(identity.IdentityID)
			require.NoError(t, err)
			assert.NotContains(t, desc.Logins, tt.wantProviderRemoved)
		})
	}
}

func TestInMemoryBackend_UnlinkDeveloperIdentity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget               error
		name                    string
		identityID              string
		poolID                  string
		developerProviderName   string
		developerUserIdentifier string
		wantErr                 bool
	}{
		{
			name:                    "success",
			identityID:              "use-created-identity",
			poolID:                  "use-created-pool",
			developerProviderName:   "developer.example.com",
			developerUserIdentifier: "user-001",
		},
		{
			name:                    "pool_not_found",
			identityID:              "use-created-identity",
			poolID:                  "us-east-1:missing-pool",
			developerProviderName:   "developer.example.com",
			developerUserIdentifier: "user-001",
			wantErr:                 true,
			errTarget:               cognitoidentity.ErrIdentityPoolNotFound,
		},
		{
			name:                    "identity_not_found",
			identityID:              "us-east-1:missing-identity",
			poolID:                  "use-created-pool",
			developerProviderName:   "developer.example.com",
			developerUserIdentifier: "user-001",
			wantErr:                 true,
			errTarget:               cognitoidentity.ErrIdentityPoolNotFound,
		},
		{
			name:                    "user_identifier_not_found",
			identityID:              "use-created-identity",
			poolID:                  "use-created-pool",
			developerProviderName:   "developer.example.com",
			developerUserIdentifier: "missing-user",
			wantErr:                 true,
			errTarget:               cognitoidentity.ErrNotAuthorized,
		},
		{
			name:                    "missing_identity_id",
			poolID:                  "use-created-pool",
			developerProviderName:   "developer.example.com",
			developerUserIdentifier: "user-001",
			wantErr:                 true,
			errTarget:               cognitoidentity.ErrInvalidParameter,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			pool, err := b.CreateIdentityPool("unlink-dev-pool-"+tt.name, true, false, "", nil, nil, nil)
			require.NoError(t, err)

			devToken, err := b.GetOpenIDTokenForDeveloperIdentity(
				pool.IdentityPoolID,
				"",
				map[string]string{"developer.example.com": "user-001"},
				0,
			)
			require.NoError(t, err)

			identityID := tt.identityID
			if identityID == "use-created-identity" {
				identityID = devToken.IdentityID
			}

			poolID := tt.poolID
			if poolID == "use-created-pool" {
				poolID = pool.IdentityPoolID
			}

			err = b.UnlinkDeveloperIdentity(
				identityID,
				poolID,
				tt.developerProviderName,
				tt.developerUserIdentifier,
			)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			require.NoError(t, err)

			result, err := b.LookupDeveloperIdentity(
				pool.IdentityPoolID,
				devToken.IdentityID,
				"",
				"developer.example.com",
			)
			require.NoError(t, err)
			assert.Empty(t, result.DeveloperUserIdentifierList)
		})
	}
}

func TestInMemoryBackend_Refinement1_GetCredentialsForIdentity_EmptyID(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	_, err := b.GetCredentialsForIdentity("", nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, cognitoidentity.ErrInvalidParameter)
}

func TestInMemoryBackend_Refinement1_GetOpenIDToken_EmptyID(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	_, err := b.GetOpenIDToken("", nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, cognitoidentity.ErrInvalidParameter)
}

func TestInMemoryBackend_Refinement1_LookupDeveloperIdentity_EmptyPoolID(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	_, err := b.LookupDeveloperIdentity("", "", "user-001", "developer.example.com")
	require.Error(t, err)
	assert.ErrorIs(t, err, cognitoidentity.ErrInvalidParameter)
}

func TestInMemoryBackend_Refinement1_MergeDeveloperIdentities_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget             error
		name                  string
		poolID                string
		developerProviderName string
		sourceUserID          string
		destUserID            string
		wantErr               bool
	}{
		{
			name:                  "missing_pool_id",
			poolID:                "",
			developerProviderName: "developer.example.com",
			sourceUserID:          "src",
			destUserID:            "dst",
			wantErr:               true,
			errTarget:             cognitoidentity.ErrInvalidParameter,
		},
		{
			name:                  "missing_developer_provider_name",
			poolID:                "us-east-1:pool",
			developerProviderName: "",
			sourceUserID:          "src",
			destUserID:            "dst",
			wantErr:               true,
			errTarget:             cognitoidentity.ErrInvalidParameter,
		},
		{
			name:                  "missing_source_user_id",
			poolID:                "us-east-1:pool",
			developerProviderName: "developer.example.com",
			sourceUserID:          "",
			destUserID:            "dst",
			wantErr:               true,
			errTarget:             cognitoidentity.ErrInvalidParameter,
		},
		{
			name:                  "missing_dest_user_id",
			poolID:                "us-east-1:pool",
			developerProviderName: "developer.example.com",
			sourceUserID:          "src",
			destUserID:            "",
			wantErr:               true,
			errTarget:             cognitoidentity.ErrInvalidParameter,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			_, err := b.MergeDeveloperIdentities(tt.sourceUserID, tt.destUserID, tt.developerProviderName, tt.poolID)
			require.Error(t, err)
			assert.ErrorIs(t, err, tt.errTarget)
		})
	}
}

func TestInMemoryBackend_Refinement1_TagResource_Validation(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	err := b.TagResource("", map[string]string{"k": "v"})
	require.Error(t, err)
	assert.ErrorIs(t, err, cognitoidentity.ErrInvalidParameter)
}

func TestInMemoryBackend_Refinement1_UntagResource_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget   error
		name        string
		resourceARN string
		tagKeys     []string
	}{
		{
			name:        "empty_arn",
			resourceARN: "",
			tagKeys:     []string{"k"},
			errTarget:   cognitoidentity.ErrInvalidParameter,
		},
		{
			name:        "empty_tag_keys",
			resourceARN: "arn:aws:cognito-identity:us-east-1:000000000000:identitypool/pool",
			tagKeys:     []string{},
			errTarget:   cognitoidentity.ErrInvalidParameter,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			err := b.UntagResource(tt.resourceARN, tt.tagKeys)
			require.Error(t, err)
			assert.ErrorIs(t, err, tt.errTarget)
		})
	}
}

func TestInMemoryBackend_Refinement1_GetPrincipalTagAttributeMap_EmptyProvider(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateIdentityPool("ptag-pool", true, false, "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.GetPrincipalTagAttributeMap(pool.IdentityPoolID, "")
	require.Error(t, err)
	assert.ErrorIs(t, err, cognitoidentity.ErrInvalidParameter)
}

func TestInMemoryBackend_Refinement1_SetPrincipalTagAttributeMap_EmptyProvider(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateIdentityPool("ptag-pool2", true, false, "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.SetPrincipalTagAttributeMap(pool.IdentityPoolID, "", false, nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, cognitoidentity.ErrInvalidParameter)
}

func TestInMemoryBackend_Refinement1_ListIdentities_EmptyPoolID(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	_, err := b.ListIdentities("", 10, false)
	require.Error(t, err)
	assert.ErrorIs(t, err, cognitoidentity.ErrInvalidParameter)
}

func TestInMemoryBackend_Refinement1_GetOpenIDTokenForDeveloperIdentity_InvalidDuration(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateIdentityPool("dur-pool", true, false, "", nil, nil, nil)
	require.NoError(t, err)

	tests := []struct {
		name     string
		duration int64
		wantErr  bool
	}{
		{name: "zero_valid", duration: 0, wantErr: false},
		{name: "max_valid", duration: 86400, wantErr: false},
		{name: "negative", duration: -1, wantErr: true},
		{name: "too_large", duration: 86401, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, tokenErr := b.GetOpenIDTokenForDeveloperIdentity(
				pool.IdentityPoolID,
				"",
				map[string]string{"developer.example.com": "user-" + tt.name},
				tt.duration,
			)

			if tt.wantErr {
				require.Error(t, tokenErr)
				assert.ErrorIs(t, tokenErr, cognitoidentity.ErrInvalidParameter)
			} else {
				require.NoError(t, tokenErr)
			}
		})
	}
}

func TestInMemoryBackend_Refinement1_UnlinkIdentity_ProviderNotLinked_Returns_NotAuthorized(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateIdentityPool("unlink-pool", true, false, "", nil, nil, nil)
	require.NoError(t, err)

	identity, err := b.GetID(pool.IdentityPoolID, "000000000000", map[string]string{
		"graph.facebook.com": "fb-token",
	})
	require.NoError(t, err)

	// Try to unlink a provider that isn't linked (google was never linked).
	err = b.UnlinkIdentity(
		identity.IdentityID,
		map[string]string{"accounts.google.com": "some-token"},
		[]string{"accounts.google.com"},
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, cognitoidentity.ErrNotAuthorized)
}

func TestInMemoryBackend_Refinement1_DeveloperProviderName_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateIdentityPool("dev-pool", true, false, "developer.myapp.com", nil, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "developer.myapp.com", pool.DeveloperProviderName)

	described, err := b.DescribeIdentityPool(pool.IdentityPoolID)
	require.NoError(t, err)
	assert.Equal(t, "developer.myapp.com", described.DeveloperProviderName)
}

func TestInMemoryBackend_Refinement1_LastModifiedDate_UpdatedOnUnlink(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateIdentityPool("lmd-pool", true, false, "", nil, nil, nil)
	require.NoError(t, err)

	identity, err := b.GetID(pool.IdentityPoolID, "000000000000", map[string]string{
		"accounts.google.com": "google-token",
	})
	require.NoError(t, err)

	// Describe before unlink.
	desc1, err := b.DescribeIdentity(identity.IdentityID)
	require.NoError(t, err)
	createdAt := desc1.CreationDate

	// Unlink the google login.
	err = b.UnlinkIdentity(
		identity.IdentityID,
		map[string]string{"accounts.google.com": "google-token"},
		[]string{"accounts.google.com"},
	)
	require.NoError(t, err)

	// Describe after unlink.
	desc2, err := b.DescribeIdentity(identity.IdentityID)
	require.NoError(t, err)
	assert.False(t, desc2.LastModifiedDate.Before(createdAt), "LastModifiedDate should not be before CreatedAt")
}

func TestInMemoryBackend_Refinement1_UpdateIdentityPool_WithTags(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateIdentityPool("tag-update-pool", true, false, "", nil, nil, map[string]string{"env": "dev"})
	require.NoError(t, err)
	assert.Equal(t, "dev", pool.Tags["env"])

	updated, err := b.UpdateIdentityPool(
		pool.IdentityPoolID,
		"tag-update-pool",
		true,
		false,
		"",
		nil,
		nil,
		map[string]string{"env": "prod"},
	)
	require.NoError(t, err)
	assert.Equal(t, "prod", updated.Tags["env"])
}
