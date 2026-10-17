package cognitoidentity_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidentity"
)

func TestGetCredentialsForIdentity_CustomRoleSelection(t *testing.T) {
	t.Parallel()

	future := time.Now().Add(time.Hour).Unix()
	roles := []string{rsAdmin, rsReader}

	tests := []struct {
		wantErr  error
		mapping  cognitoidentity.RoleMapping
		name     string
		custom   string
		wantRole string
		unauth   bool
	}{
		{name: "token roles picks custom", mapping: rsToken("AuthenticatedRole"), custom: rsReader, wantRole: rsReader},
		{name: "token roles rejects unlisted", mapping: rsToken("AuthenticatedRole"), custom: rsUnauth,
			wantErr: cognitoidentity.ErrNotAuthorized},
		{name: "authenticated role allowed", mapping: rsToken("AuthenticatedRole"), custom: rsAuthRole,
			wantRole: rsAuthRole},
		{
			name: "rules role allowed",
			mapping: rsRules("AuthenticatedRole", cognitoidentity.MappingRule{
				Claim: "custom:team", MatchType: "Equals", Value: "ops", RoleARN: rsAdmin,
			}),
			custom:   rsAdmin,
			wantRole: rsAdmin,
		},
		{name: "unauthenticated own role", unauth: true, custom: rsUnauth, wantRole: rsUnauth},
		{name: "unauthenticated rejects other", unauth: true, custom: rsAdmin,
			wantErr: cognitoidentity.ErrNotAuthorized},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			b := newTestBackend()
			assumer := &recordingAssumer{}
			b.SetRoleAssumer(assumer)

			pool, err := b.CreateIdentityPool(ctx, "p", true, false, "",
				[]cognitoidentity.IdentityProvider{{ProviderName: rsProvider, ClientID: "client-1"}}, nil, nil)
			require.NoError(t, err)
			require.NoError(t, b.SetIdentityPoolRoles(ctx, pool.IdentityPoolID, rsAuthRole, rsUnauth,
				map[string]cognitoidentity.RoleMapping{rsMapKey: tt.mapping}))

			var logins map[string]string
			if !tt.unauth {
				logins = map[string]string{rsProvider: loginJWT(t, map[string]any{
					"sub": "u1", "iss": "iss", "aud": "client-1", "exp": future,
					"cognito:roles": roles, "custom:team": "ops",
				})}
			}

			identity, err := b.GetID(ctx, pool.IdentityPoolID, "", logins)
			require.NoError(t, err)

			_, err = b.GetCredentialsForIdentity(ctx, identity.IdentityID, logins, tt.custom)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				assert.Empty(t, assumer.roleARN)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantRole, assumer.roleARN)
		})
	}
}

func TestGetCredentialsForIdentity_PrincipalTags(t *testing.T) {
	t.Parallel()

	future := time.Now().Add(time.Hour).Unix()

	tests := []struct {
		mapped      map[string]string
		wantTags    map[string]any
		name        string
		useDefaults bool
		setMapping  bool
	}{
		{name: "no mapping no tags"},
		{
			name: "mapped claims", setMapping: true,
			mapped:   map[string]string{"team": "custom:team"},
			wantTags: map[string]any{"team": "ops"},
		},
		{
			name: "defaults", setMapping: true, useDefaults: true,
			wantTags: map[string]any{"username": "u1", "clientID": "client-1"},
		},
		{
			name: "defaults plus mapped", setMapping: true, useDefaults: true,
			mapped:   map[string]string{"team": "custom:team"},
			wantTags: map[string]any{"username": "u1", "clientID": "client-1", "team": "ops"},
		},
		{
			name: "missing claim skipped", setMapping: true,
			mapped: map[string]string{"dept": "custom:dept"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			b := newTestBackend()
			assumer := &recordingAssumer{}
			b.SetRoleAssumer(assumer)

			pool, err := b.CreateIdentityPool(ctx, "p", true, false, "",
				[]cognitoidentity.IdentityProvider{{ProviderName: rsProvider, ClientID: "client-1"}}, nil, nil)
			require.NoError(t, err)
			require.NoError(t, b.SetIdentityPoolRoles(ctx, pool.IdentityPoolID, rsAuthRole, rsUnauth, nil))

			if tt.setMapping {
				_, err = b.SetPrincipalTagAttributeMap(ctx, pool.IdentityPoolID, rsProvider, tt.useDefaults, tt.mapped)
				require.NoError(t, err)
			}

			logins := map[string]string{rsProvider: loginJWT(t, map[string]any{
				"sub": "u1", "iss": "iss", "aud": "client-1", "exp": future, "custom:team": "ops",
			})}

			identity, err := b.GetID(ctx, pool.IdentityPoolID, "", logins)
			require.NoError(t, err)

			_, err = b.GetCredentialsForIdentity(ctx, identity.IdentityID, logins, "")
			require.NoError(t, err)

			got := tokenClaims(t, assumer.token)["https://aws.amazon.com/tags"]
			if tt.wantTags == nil {
				assert.Nil(t, got)

				return
			}

			assert.Equal(t, tt.wantTags, got)
		})
	}
}

func TestGetOpenIDTokenForDeveloperIdentity_RequiresDeveloperProviderLogin(t *testing.T) {
	t.Parallel()

	tests := []struct {
		logins  map[string]string
		name    string
		wantErr bool
	}{
		{name: "developer provider", logins: map[string]string{"login.app": "u1"}},
		{name: "other provider", logins: map[string]string{"accounts.google.com": "g1"}, wantErr: true},
		{
			name:    "developer plus other",
			logins:  map[string]string{"login.app": "u1", "accounts.google.com": "g1"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			b := newTestBackend()

			pool, err := b.CreateIdentityPool(ctx, "p", false, false, "login.app", nil, nil, nil)
			require.NoError(t, err)

			_, err = b.GetOpenIDTokenForDeveloperIdentity(ctx, pool.IdentityPoolID, "", tt.logins, 0)
			if tt.wantErr {
				require.ErrorIs(t, err, cognitoidentity.ErrInvalidParameter)

				return
			}

			require.NoError(t, err)
		})
	}
}
