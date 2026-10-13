package cognitoidentity_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidentity"
)

func tokenClaims(t *testing.T, token string) map[string]any {
	t.Helper()

	parts := strings.Split(token, ".")
	require.Len(t, parts, 3)

	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	require.NoError(t, err)

	var claims map[string]any
	require.NoError(t, json.Unmarshal(raw, &claims))

	return claims
}

func TestDeveloperOpenIDTokenClaims(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags         map[string]string
		name         string
		duration     int64
		wantLifetime float64
	}{
		{name: "default_fifteen_minutes", wantLifetime: 900},
		{name: "custom_duration", duration: 3600, wantLifetime: 3600},
		{name: "principal_tags", duration: 60, wantLifetime: 60, tags: map[string]string{"team": "core"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			pool, err := b.CreateIdentityPool(context.Background(), "claims-pool", true, false, "", nil, nil, nil)
			require.NoError(t, err)

			out, err := b.GetOpenIDTokenForDeveloperIdentity(context.Background(),
				pool.IdentityPoolID, "", map[string]string{"dev.example.com": "u1"}, tt.duration,
				cognitoidentity.DeveloperTokenOptions{PrincipalTags: tt.tags})
			require.NoError(t, err)

			claims := tokenClaims(t, out.Token)
			assert.InDelta(t, tt.wantLifetime, claims["exp"].(float64)-claims["iat"].(float64), 0)
			assert.Equal(t, pool.IdentityPoolID, claims["aud"])
			assert.Equal(t, out.IdentityID, claims["sub"])

			tagClaim, hasTags := claims["https://aws.amazon.com/tags"].(map[string]any)
			assert.Equal(t, len(tt.tags) > 0, hasTags)

			for k, v := range tt.tags {
				assert.Equal(t, v, tagClaim[k])
			}
		})
	}
}

func TestOpenIDTokenTenMinutes(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateIdentityPool(context.Background(), "ten-pool", true, false, "", nil, nil, nil)
	require.NoError(t, err)

	id, err := b.GetID(context.Background(), pool.IdentityPoolID, "", nil)
	require.NoError(t, err)

	tok, err := b.GetOpenIDToken(context.Background(), id.IdentityID, nil)
	require.NoError(t, err)

	claims := tokenClaims(t, tok.Token)
	assert.InDelta(t, 600, claims["exp"].(float64)-claims["iat"].(float64), 0)
}
