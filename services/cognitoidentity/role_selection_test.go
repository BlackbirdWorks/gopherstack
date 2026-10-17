package cognitoidentity_test

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"maps"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidentity"
)

var errAssumeDenied = errors.New("trust policy denied")

const (
	rsProvider = "cognito-idp.us-east-1.amazonaws.com/us-east-1_Pool1"
	rsMapKey   = rsProvider + ":client-1"
	rsAuthRole = "arn:aws:iam::000000000000:role/Auth"
	rsUnauth   = "arn:aws:iam::000000000000:role/Unauth"
	rsAdmin    = "arn:aws:iam::000000000000:role/Admin"
	rsReader   = "arn:aws:iam::000000000000:role/Reader"
)

type recordingAssumer struct {
	err     error
	roleARN string
	token   string
	session string
}

func (r *recordingAssumer) AssumeRoleWithWebIdentity(
	roleARN, sessionName, token string,
) (*cognitoidentity.Credentials, error) {
	r.roleARN, r.session, r.token = roleARN, sessionName, token
	if r.err != nil {
		return nil, r.err
	}

	return &cognitoidentity.Credentials{
		AccessKeyID:     "ASIAFROMSTS",
		SecretAccessKey: "secret-from-sts",
		SessionToken:    "token-from-sts",
		Expiration:      time.Now().Add(time.Hour),
	}, nil
}

func rsRules(resolution string, rules ...cognitoidentity.MappingRule) cognitoidentity.RoleMapping {
	return cognitoidentity.RoleMapping{
		Type:                    "Rules",
		AmbiguousRoleResolution: resolution,
		RulesConfiguration:      &cognitoidentity.RulesConfiguration{Rules: rules},
	}
}

func rsToken(resolution string) cognitoidentity.RoleMapping {
	return cognitoidentity.RoleMapping{Type: "Token", AmbiguousRoleResolution: resolution}
}

func TestGetCredentialsForIdentity_RoleSelection(t *testing.T) {
	t.Parallel()

	future := time.Now().Add(time.Hour).Unix()
	idToken := func(extra map[string]any) string {
		claims := map[string]any{"sub": "u1", "iss": "iss", "aud": "client-1", "exp": future}
		maps.Copy(claims, extra)

		return loginJWT(t, claims)
	}

	tests := []struct {
		wantErr  error
		mapping  *cognitoidentity.RoleMapping
		claims   map[string]any
		name     string
		wantRole string
		unauth   bool
	}{
		{name: "unauthenticated role", unauth: true, wantRole: rsUnauth},
		{name: "no mapping uses authenticated role", wantRole: rsAuthRole},
		{
			name:     "token preferred role",
			mapping:  new(rsToken("AuthenticatedRole")),
			claims:   map[string]any{"cognito:preferred_role": rsAdmin, "cognito:roles": []string{rsAdmin, rsReader}},
			wantRole: rsAdmin,
		},
		{
			name:     "token single role",
			mapping:  new(rsToken("AuthenticatedRole")),
			claims:   map[string]any{"cognito:roles": []string{rsReader}},
			wantRole: rsReader,
		},
		{
			name:     "token ambiguous falls back",
			mapping:  new(rsToken("AuthenticatedRole")),
			claims:   map[string]any{"cognito:roles": []string{rsAdmin, rsReader}},
			wantRole: rsAuthRole,
		},
		{
			name:    "token ambiguous denied",
			mapping: new(rsToken("Deny")),
			claims:  map[string]any{"cognito:roles": []string{rsAdmin, rsReader}},
			wantErr: cognitoidentity.ErrInvalidIdentityPoolConfiguration,
		},
		{
			name:    "token no claims denied",
			mapping: new(rsToken("Deny")),
			wantErr: cognitoidentity.ErrInvalidIdentityPoolConfiguration,
		},
		{
			name: "rules equals",
			mapping: new(rsRules(
				"AuthenticatedRole",
				cognitoidentity.MappingRule{
					Claim:     "custom:team",
					MatchType: "Equals",
					Value:     "ops",
					RoleARN:   rsAdmin,
				},
			)),
			claims:   map[string]any{"custom:team": "ops"},
			wantRole: rsAdmin,
		},
		{
			name: "rules contains",
			mapping: new(rsRules("AuthenticatedRole",
				cognitoidentity.MappingRule{Claim: "email", MatchType: "Contains", Value: "corp", RoleARN: rsAdmin})),
			claims:   map[string]any{"email": "a@corp.example.com"},
			wantRole: rsAdmin,
		},
		{
			name: "rules starts with",
			mapping: new(rsRules(
				"AuthenticatedRole",
				cognitoidentity.MappingRule{
					Claim:     "email",
					MatchType: "StartsWith",
					Value:     "admin",
					RoleARN:   rsAdmin,
				},
			)),
			claims:   map[string]any{"email": "admin@example.com"},
			wantRole: rsAdmin,
		},
		{
			name: "rules not equal",
			mapping: new(rsRules(
				"AuthenticatedRole",
				cognitoidentity.MappingRule{
					Claim:     "custom:team",
					MatchType: "NotEqual",
					Value:     "ops",
					RoleARN:   rsReader,
				},
			)),
			claims:   map[string]any{"custom:team": "dev"},
			wantRole: rsReader,
		},
		{
			name: "rules first match wins",
			mapping: new(rsRules(
				"AuthenticatedRole",
				cognitoidentity.MappingRule{Claim: "custom:team", MatchType: "Equals", Value: "x", RoleARN: rsAdmin},
				cognitoidentity.MappingRule{
					Claim:     "custom:team",
					MatchType: "Contains",
					Value:     "o",
					RoleARN:   rsReader,
				},
			)),
			claims:   map[string]any{"custom:team": "ops"},
			wantRole: rsReader,
		},
		{
			name: "rules group array claim",
			mapping: new(rsRules(
				"AuthenticatedRole",
				cognitoidentity.MappingRule{
					Claim:     "cognito:groups",
					MatchType: "Equals",
					Value:     "admins",
					RoleARN:   rsAdmin,
				},
			)),
			claims:   map[string]any{"cognito:groups": []string{"users", "admins"}},
			wantRole: rsAdmin,
		},
		{
			name: "rules no match falls back",
			mapping: new(rsRules(
				"AuthenticatedRole",
				cognitoidentity.MappingRule{
					Claim:     "custom:team",
					MatchType: "Equals",
					Value:     "ops",
					RoleARN:   rsAdmin,
				},
			)),
			claims:   map[string]any{"custom:team": "dev"},
			wantRole: rsAuthRole,
		},
		{
			name: "rules no match denied",
			mapping: new(rsRules(
				"Deny",
				cognitoidentity.MappingRule{
					Claim:     "custom:team",
					MatchType: "Equals",
					Value:     "ops",
					RoleARN:   rsAdmin,
				},
			)),
			claims:  map[string]any{"custom:team": "dev"},
			wantErr: cognitoidentity.ErrInvalidIdentityPoolConfiguration,
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

			mappings := map[string]cognitoidentity.RoleMapping(nil)
			if tt.mapping != nil {
				mappings = map[string]cognitoidentity.RoleMapping{rsMapKey: *tt.mapping}
			}

			require.NoError(t, b.SetIdentityPoolRoles(ctx, pool.IdentityPoolID, rsAuthRole, rsUnauth, mappings))

			var logins map[string]string
			if !tt.unauth {
				logins = map[string]string{rsProvider: idToken(tt.claims)}
			}

			identity, err := b.GetID(ctx, pool.IdentityPoolID, "", logins)
			require.NoError(t, err)

			creds, err := b.GetCredentialsForIdentity(ctx, identity.IdentityID, logins, "")
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantRole, assumer.roleARN)
			assert.Equal(t, "ASIAFROMSTS", creds.AccessKeyID)
			assert.Equal(t, "CognitoIdentityCredentials", assumer.session)
			assert.NotEmpty(t, assumer.token)
		})
	}
}

func TestGetCredentialsForIdentity_AssumeFailure(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newTestBackend()
	b.SetRoleAssumer(&recordingAssumer{err: errAssumeDenied})

	pool, err := b.CreateIdentityPool(ctx, "p", true, false, "", nil, nil, nil)
	require.NoError(t, err)
	require.NoError(t, b.SetIdentityPoolRoles(ctx, pool.IdentityPoolID, "", rsUnauth, nil))

	identity, err := b.GetID(ctx, pool.IdentityPoolID, "", nil)
	require.NoError(t, err)

	_, err = b.GetCredentialsForIdentity(ctx, identity.IdentityID, nil, "")
	require.ErrorIs(t, err, cognitoidentity.ErrInvalidIdentityPoolConfiguration)
}

type fakeKeyResolver struct {
	key    *rsa.PublicKey
	issuer string
}

func (f fakeKeyResolver) ResolveTokenKey(issuer, _ string) (*rsa.PublicKey, bool, error) {
	if issuer != f.issuer {
		return nil, false, nil
	}

	return f.key, true, nil
}

func signedLogin(t *testing.T, key *rsa.PrivateKey, claims map[string]any) string {
	t.Helper()

	enc := base64.RawURLEncoding
	hdr, err := json.Marshal(map[string]string{"alg": "RS256", "kid": "k1"})
	require.NoError(t, err)

	body, err := json.Marshal(claims)
	require.NoError(t, err)

	signing := enc.EncodeToString(hdr) + "." + enc.EncodeToString(body)
	digest := sha256.Sum256([]byte(signing))

	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	require.NoError(t, err)

	return signing + "." + enc.EncodeToString(sig)
}

func TestLoginTokenSignatureVerification(t *testing.T) {
	t.Parallel()

	good, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	other, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	claims := map[string]any{
		"sub": "u1", "iss": "http://idp/pool", "aud": "client-1", "exp": time.Now().Add(time.Hour).Unix(),
	}
	foreign := map[string]any{
		"sub": "u1", "iss": "http://elsewhere", "aud": "client-1", "exp": time.Now().Add(time.Hour).Unix(),
	}

	tests := []struct {
		wantErr error
		token   func() string
		name    string
	}{
		{name: "valid", token: func() string { return signedLogin(t, good, claims) }},
		{
			name:    "foreign key",
			token:   func() string { return signedLogin(t, other, claims) },
			wantErr: cognitoidentity.ErrNotAuthorized,
		},
		{
			name:    "bad signature",
			token:   func() string { return loginJWT(t, claims) },
			wantErr: cognitoidentity.ErrNotAuthorized,
		},
		{name: "unhosted issuer skipped", token: func() string { return loginJWT(t, foreign) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			b := newTestBackend()
			b.SetTokenKeyResolver(fakeKeyResolver{issuer: "http://idp/pool", key: &good.PublicKey})

			pool, poolErr := b.CreateIdentityPool(ctx, "p", false, false, "",
				[]cognitoidentity.IdentityProvider{{ProviderName: rsProvider, ClientID: "client-1"}}, nil, nil)
			require.NoError(t, poolErr)

			_, idErr := b.GetID(ctx, pool.IdentityPoolID, "", map[string]string{rsProvider: tt.token()})
			if tt.wantErr != nil {
				require.ErrorIs(t, idErr, tt.wantErr)

				return
			}

			require.NoError(t, idErr)
		})
	}
}
