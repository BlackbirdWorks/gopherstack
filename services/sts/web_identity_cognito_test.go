package sts_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sts"
)

const cognitoIdentityTrust = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
	`"Principal":{"Federated":"cognito-identity.amazonaws.com"},` +
	`"Action":"sts:AssumeRoleWithWebIdentity","Condition":{` +
	`"StringEquals":{"cognito-identity.amazonaws.com:aud":"us-east-1:pool-1"},` +
	`"ForAnyValue:StringLike":{"cognito-identity.amazonaws.com:amr":"authenticated"}}}]}`

func TestAssumeRoleWithWebIdentity_CognitoIdentityIssuer(t *testing.T) {
	t.Parallel()

	claims := func(aud, amr string) map[string]any {
		return map[string]any{
			"iss": "cognito-identity.amazonaws.com",
			"aud": aud,
			"sub": "us-east-1:identity-1",
			"amr": []string{amr},
			"exp": time.Now().Add(10 * time.Minute).Unix(),
		}
	}

	tests := []struct {
		wantErr error
		claims  map[string]any
		name    string
	}{
		{name: "authenticated", claims: claims("us-east-1:pool-1", "authenticated")},
		{
			name:    "unauthenticated amr denied",
			claims:  claims("us-east-1:pool-1", "unauthenticated"),
			wantErr: sts.ErrAccessDenied,
		},
		{
			name:    "other pool denied",
			claims:  claims("us-east-1:pool-2", "authenticated"),
			wantErr: sts.ErrAccessDenied,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sts.NewInMemoryBackend()
			b.SetRoleLookup(&stubRoleLookup{meta: &sts.RoleMeta{TrustPolicy: cognitoIdentityTrust}})
			b.SetOIDCLookup(irsaOIDCLookup{providers: map[string][]string{}})

			out, err := b.AssumeRoleWithWebIdentity(&sts.AssumeRoleWithWebIdentityInput{
				RoleArn:          "arn:aws:iam::123456789012:role/cognito-auth",
				RoleSessionName:  "CognitoIdentityCredentials",
				WebIdentityToken: buildJWT(t, tt.claims),
			})

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, "arn:aws:sts::123456789012:assumed-role/cognito-auth/CognitoIdentityCredentials",
				out.AssumeRoleWithWebIdentityResult.AssumedRoleUser.Arn)
			assert.Equal(t, "cognito-identity.amazonaws.com", out.AssumeRoleWithWebIdentityResult.Provider)
		})
	}
}
