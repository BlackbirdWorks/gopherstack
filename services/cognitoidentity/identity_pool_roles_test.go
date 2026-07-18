package cognitoidentity_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidentity"
)

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
				pool, setupErr := b.CreateIdentityPool(
					context.Background(),
					"roles-pool",
					true,
					false,
					"",
					nil,
					nil,
					nil,
				)
				require.NoError(t, setupErr)
				poolID = pool.IdentityPoolID
			} else {
				poolID = "nonexistent"
			}

			authRoleARN := "arn:aws:iam::000000000000:role/CognitoAuthRole"
			unauthRoleARN := "arn:aws:iam::000000000000:role/CognitoUnauthRole"

			setErr := b.SetIdentityPoolRoles(
				context.Background(),
				poolID,
				authRoleARN,
				unauthRoleARN,
				nil,
			)

			if tt.wantErr {
				require.Error(t, setErr)
				assert.ErrorIs(t, setErr, tt.errTarget)

				return
			}

			require.NoError(t, setErr)

			roles, getErr := b.GetIdentityPoolRoles(context.Background(), poolID)
			require.NoError(t, getErr)
			assert.Equal(t, authRoleARN, roles.AuthenticatedRoleARN)
			assert.Equal(t, unauthRoleARN, roles.UnauthenticatedRoleARN)
		})
	}
}

func TestInMemoryBackend_GetIdentityPoolRoles_NoRoles(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	pool, err := b.CreateIdentityPool(
		context.Background(),
		"no-roles-pool",
		true,
		false,
		"",
		nil,
		nil,
		nil,
	)
	require.NoError(t, err)

	roles, err := b.GetIdentityPoolRoles(context.Background(), pool.IdentityPoolID)
	require.NoError(t, err)
	assert.Empty(t, roles.AuthenticatedRoleARN)
	assert.Empty(t, roles.UnauthenticatedRoleARN)
}

func TestInMemoryBackend_GetIdentityPoolRoles_NotFound(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	_, err := b.GetIdentityPoolRoles(context.Background(), "us-east-1:nonexistent")
	require.Error(t, err)
	assert.ErrorIs(t, err, cognitoidentity.ErrIdentityPoolNotFound)
}

func TestInMemoryBackend_SetIdentityPoolRoles_NotFound(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	err := b.SetIdentityPoolRoles(
		context.Background(),
		"us-east-1:nonexistent",
		"arn:aws:iam::000000000000:role/Auth",
		"",
		nil,
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, cognitoidentity.ErrIdentityPoolNotFound)
}

func TestInMemoryBackend_GetPrincipalTagAttributeMap_EmptyProvider(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateIdentityPool(
		context.Background(),
		"ptag-pool",
		true,
		false,
		"",
		nil,
		nil,
		nil,
	)
	require.NoError(t, err)

	_, err = b.GetPrincipalTagAttributeMap(context.Background(), pool.IdentityPoolID, "")
	require.Error(t, err)
	assert.ErrorIs(t, err, cognitoidentity.ErrInvalidParameter)
}

func TestInMemoryBackend_SetPrincipalTagAttributeMap_EmptyProvider(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateIdentityPool(
		context.Background(),
		"ptag-pool2",
		true,
		false,
		"",
		nil,
		nil,
		nil,
	)
	require.NoError(t, err)

	_, err = b.SetPrincipalTagAttributeMap(
		context.Background(),
		pool.IdentityPoolID,
		"",
		false,
		nil,
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, cognitoidentity.ErrInvalidParameter)
}

func TestInMemoryBackend_SetIdentityPoolRoles_MergePreservesExistingRole(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateIdentityPool(
		context.Background(),
		"role-merge-pool",
		true,
		false,
		"",
		nil,
		nil,
		nil,
	)
	require.NoError(t, err)

	// Set both roles initially.
	require.NoError(
		t,
		b.SetIdentityPoolRoles(context.Background(),
			pool.IdentityPoolID,
			"arn:aws:iam::000000000000:role/Auth",
			"arn:aws:iam::000000000000:role/Unauth",
			nil,
		),
	)

	// Update only the authenticated role – the unauthenticated role must be preserved.
	require.NoError(
		t,
		b.SetIdentityPoolRoles(
			context.Background(),
			pool.IdentityPoolID,
			"arn:aws:iam::000000000000:role/AuthV2",
			"",
			nil,
		),
	)

	roles, err := b.GetIdentityPoolRoles(context.Background(), pool.IdentityPoolID)
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:iam::000000000000:role/AuthV2", roles.AuthenticatedRoleARN)
	assert.Equal(
		t,
		"arn:aws:iam::000000000000:role/Unauth",
		roles.UnauthenticatedRoleARN,
		"unauthenticated role must not be wiped",
	)
}

func TestInMemoryBackend_GetIdentityPoolRoles_EmptyID(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	_, err := b.GetIdentityPoolRoles(context.Background(), "")
	require.Error(t, err)
	assert.ErrorIs(t, err, cognitoidentity.ErrInvalidParameter)
}

func TestSetGetIdentityPoolRoles_WithRoleMappings(t *testing.T) {
	t.Parallel()

	b := cognitoidentity.NewInMemoryBackend("000000000000", "us-east-1")

	pool, err := b.CreateIdentityPool(context.Background(), "role-mappings-pool", true, false, "", nil, nil, nil)
	require.NoError(t, err)

	roleMappings := map[string]cognitoidentity.RoleMapping{
		"accounts.google.com": {
			Type:                    "Rules",
			AmbiguousRoleResolution: "Deny",
			RulesConfiguration: &cognitoidentity.RulesConfiguration{
				Rules: []cognitoidentity.MappingRule{
					{
						Claim:     "sub",
						MatchType: "Equals",
						Value:     "user123",
						RoleARN:   "arn:aws:iam::000000000000:role/GoogleUser",
					},
				},
			},
		},
	}

	err = b.SetIdentityPoolRoles(context.Background(),
		pool.IdentityPoolID,
		"arn:aws:iam::000000000000:role/Auth",
		"arn:aws:iam::000000000000:role/Unauth",
		roleMappings,
	)
	require.NoError(t, err)

	roles, err := b.GetIdentityPoolRoles(context.Background(), pool.IdentityPoolID)
	require.NoError(t, err)

	assert.Equal(t, "arn:aws:iam::000000000000:role/Auth", roles.AuthenticatedRoleARN)
	assert.Equal(t, "arn:aws:iam::000000000000:role/Unauth", roles.UnauthenticatedRoleARN)
	require.Contains(t, roles.RoleMappings, "accounts.google.com")

	mapping := roles.RoleMappings["accounts.google.com"]
	assert.Equal(t, "Rules", mapping.Type)
	assert.Equal(t, "Deny", mapping.AmbiguousRoleResolution)
	require.NotNil(t, mapping.RulesConfiguration)
	require.Len(t, mapping.RulesConfiguration.Rules, 1)

	rule := mapping.RulesConfiguration.Rules[0]
	assert.Equal(t, "sub", rule.Claim)
	assert.Equal(t, "Equals", rule.MatchType)
	assert.Equal(t, "user123", rule.Value)
	assert.Equal(t, "arn:aws:iam::000000000000:role/GoogleUser", rule.RoleARN)
}

func TestSetIdentityPoolRoles_RoleMappingsNilPreservesExisting(t *testing.T) {
	t.Parallel()

	b := cognitoidentity.NewInMemoryBackend("000000000000", "us-east-1")

	pool, err := b.CreateIdentityPool(context.Background(), "role-preserve-pool", true, false, "", nil, nil, nil)
	require.NoError(t, err)

	roleMappings := map[string]cognitoidentity.RoleMapping{
		"accounts.google.com": {Type: "Token", AmbiguousRoleResolution: "AuthenticatedRole"},
	}

	err = b.SetIdentityPoolRoles(context.Background(),
		pool.IdentityPoolID,
		"arn:aws:iam::000000000000:role/Auth",
		"",
		roleMappings,
	)
	require.NoError(t, err)

	// Update with nil roleMappings — existing mappings must be preserved.
	err = b.SetIdentityPoolRoles(context.Background(),
		pool.IdentityPoolID,
		"arn:aws:iam::000000000000:role/AuthV2",
		"",
		nil,
	)
	require.NoError(t, err)

	roles, err := b.GetIdentityPoolRoles(context.Background(), pool.IdentityPoolID)
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:iam::000000000000:role/AuthV2", roles.AuthenticatedRoleARN)
	assert.Contains(
		t,
		roles.RoleMappings,
		"accounts.google.com",
		"nil roleMappings must not clear existing mappings",
	)
}
