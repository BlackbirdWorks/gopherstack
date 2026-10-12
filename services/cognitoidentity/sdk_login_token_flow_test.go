package cognitoidentity_test

import (
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cognitoidentitysdk "github.com/aws/aws-sdk-go-v2/service/cognitoidentity"
	sdktypes "github.com/aws/aws-sdk-go-v2/service/cognitoidentity/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func loginJWT(t *testing.T, claims map[string]any) string {
	t.Helper()

	enc := base64.RawURLEncoding
	hdr, err := json.Marshal(map[string]string{"alg": "RS256"})
	require.NoError(t, err)

	body, err := json.Marshal(claims)
	require.NoError(t, err)

	return enc.EncodeToString(hdr) + "." + enc.EncodeToString(body) + ".sig"
}

func TestLoginTokenFlows_RealClient(t *testing.T) {
	t.Parallel()

	const (
		provider = "cognito-idp.us-east-1.amazonaws.com/us-east-1_Pool1"
		clientID = "client-1"
	)

	future := time.Now().Add(time.Hour).Unix()
	past := time.Now().Add(-time.Hour).Unix()

	tests := []struct {
		second       map[string]any
		name         string
		wantSameID   bool
		wantAuthFail bool
	}{
		{
			name:       "relogin_new_token_same_user",
			second:     map[string]any{"sub": "u1", "iss": "iss", "aud": clientID, "exp": future, "jti": "second"},
			wantSameID: true,
		},
		{
			name:       "other_user_new_identity",
			second:     map[string]any{"sub": "u2", "iss": "iss", "aud": clientID, "exp": future},
			wantSameID: false,
		},
		{
			name:         "expired_token",
			second:       map[string]any{"sub": "u1", "iss": "iss", "aud": clientID, "exp": past},
			wantAuthFail: true,
		},
		{
			name:         "wrong_client",
			second:       map[string]any{"sub": "u1", "iss": "iss", "aud": "other-client", "exp": future},
			wantAuthFail: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)
			ctx := t.Context()

			pool, err := client.CreateIdentityPool(ctx, &cognitoidentitysdk.CreateIdentityPoolInput{
				IdentityPoolName:               aws.String("rotate-pool"),
				AllowUnauthenticatedIdentities: false,
				CognitoIdentityProviders: []sdktypes.CognitoIdentityProvider{
					{ProviderName: aws.String(provider), ClientId: aws.String(clientID)},
				},
			})
			require.NoError(t, err)

			_, err = client.SetIdentityPoolRoles(ctx, &cognitoidentitysdk.SetIdentityPoolRolesInput{
				IdentityPoolId: pool.IdentityPoolId,
				Roles:          map[string]string{"authenticated": "arn:aws:iam::000000000000:role/auth"},
			})
			require.NoError(t, err)

			first := loginJWT(
				t,
				map[string]any{"sub": "u1", "iss": "iss", "aud": clientID, "exp": future, "jti": "first"},
			)
			id1, err := client.GetId(ctx, &cognitoidentitysdk.GetIdInput{
				IdentityPoolId: pool.IdentityPoolId,
				Logins:         map[string]string{provider: first},
			})
			require.NoError(t, err)

			second := loginJWT(t, tt.second)
			id2, err := client.GetId(ctx, &cognitoidentitysdk.GetIdInput{
				IdentityPoolId: pool.IdentityPoolId,
				Logins:         map[string]string{provider: second},
			})

			if tt.wantAuthFail {
				var na *sdktypes.NotAuthorizedException
				require.ErrorAs(t, err, &na)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantSameID, aws.ToString(id1.IdentityId) == aws.ToString(id2.IdentityId))

			if tt.wantSameID {
				_, err = client.GetCredentialsForIdentity(ctx, &cognitoidentitysdk.GetCredentialsForIdentityInput{
					IdentityId: id1.IdentityId,
					Logins:     map[string]string{provider: second},
				})
				require.NoError(t, err)
			}
		})
	}
}

func TestCredentialsAccessKeyShape_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "unauthenticated"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)
			ctx := t.Context()

			pool, err := client.CreateIdentityPool(ctx, &cognitoidentitysdk.CreateIdentityPoolInput{
				IdentityPoolName:               aws.String("shape-pool"),
				AllowUnauthenticatedIdentities: true,
			})
			require.NoError(t, err)

			_, err = client.SetIdentityPoolRoles(ctx, &cognitoidentitysdk.SetIdentityPoolRolesInput{
				IdentityPoolId: pool.IdentityPoolId,
				Roles:          map[string]string{"unauthenticated": "arn:aws:iam::000000000000:role/unauth"},
			})
			require.NoError(t, err)

			id, err := client.GetId(ctx, &cognitoidentitysdk.GetIdInput{IdentityPoolId: pool.IdentityPoolId})
			require.NoError(t, err)

			out, err := client.GetCredentialsForIdentity(ctx, &cognitoidentitysdk.GetCredentialsForIdentityInput{
				IdentityId: id.IdentityId,
			})
			require.NoError(t, err)
			assert.Regexp(t, `^ASIA[A-Z0-9]{16}$`, aws.ToString(out.Credentials.AccessKeyId))
		})
	}
}

func TestSetIdentityPoolRoles_Validation_RealClient(t *testing.T) {
	t.Parallel()

	const roleArn = "arn:aws:iam::000000000000:role/a"

	rule := sdktypes.MappingRule{
		Claim: aws.String("cognito:username"), MatchType: sdktypes.MappingRuleMatchTypeEquals,
		Value: aws.String("bob"), RoleARN: aws.String(roleArn),
	}

	tests := []struct {
		roles    map[string]string
		mappings map[string]sdktypes.RoleMapping
		name     string
		wantErr  bool
	}{
		{name: "plain", roles: map[string]string{"authenticated": roleArn}},
		{name: "unknown_role_key", roles: map[string]string{"admins": roleArn}, wantErr: true},
		{
			name: "token_mapping", roles: map[string]string{"authenticated": roleArn},
			mappings: map[string]sdktypes.RoleMapping{
				"cognito-idp.us-east-1.amazonaws.com/p:c": {
					Type:                    sdktypes.RoleMappingTypeToken,
					AmbiguousRoleResolution: sdktypes.AmbiguousRoleResolutionTypeDeny,
				},
			},
		},
		{
			name: "token_without_resolution", roles: map[string]string{"authenticated": roleArn}, wantErr: true,
			mappings: map[string]sdktypes.RoleMapping{
				"cognito-idp.us-east-1.amazonaws.com/p:c": {Type: sdktypes.RoleMappingTypeToken},
			},
		},
		{
			name: "rules_without_configuration", roles: map[string]string{"authenticated": roleArn}, wantErr: true,
			mappings: map[string]sdktypes.RoleMapping{
				"cognito-idp.us-east-1.amazonaws.com/p:c": {
					Type:                    sdktypes.RoleMappingTypeRules,
					AmbiguousRoleResolution: sdktypes.AmbiguousRoleResolutionTypeDeny,
				},
			},
		},
		{
			name: "rules_with_configuration", roles: map[string]string{"authenticated": roleArn},
			mappings: map[string]sdktypes.RoleMapping{
				"cognito-idp.us-east-1.amazonaws.com/p:c": {
					Type:                    sdktypes.RoleMappingTypeRules,
					AmbiguousRoleResolution: sdktypes.AmbiguousRoleResolutionTypeAuthenticatedRole,
					RulesConfiguration:      &sdktypes.RulesConfigurationType{Rules: []sdktypes.MappingRule{rule}},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)
			ctx := t.Context()

			pool, err := client.CreateIdentityPool(ctx, &cognitoidentitysdk.CreateIdentityPoolInput{
				IdentityPoolName: aws.String("roles-pool"), AllowUnauthenticatedIdentities: true,
			})
			require.NoError(t, err)

			_, err = client.SetIdentityPoolRoles(ctx, &cognitoidentitysdk.SetIdentityPoolRolesInput{
				IdentityPoolId: pool.IdentityPoolId, Roles: tt.roles, RoleMappings: tt.mappings,
			})

			if tt.wantErr {
				var ip *sdktypes.InvalidParameterException
				require.ErrorAs(t, err, &ip)

				return
			}

			require.NoError(t, err)
		})
	}
}
