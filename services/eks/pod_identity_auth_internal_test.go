package eks

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestVerifyPodIdentityTokenClaims(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend(context.Background(), "123456789012", "us-east-1")
	c, cerr := b.CreateCluster("c1", "1.32", "", nil, nil, nil)
	require.NoError(t, cerr)

	key, kerr := b.signingKey(c.OIDCIssuer)
	require.NoError(t, kerr)

	const sub = "system:serviceaccount:d:a"

	now := time.Now()

	claims := func(iss string, aud any, sub string, exp, nbf int64) map[string]any {
		m := map[string]any{"iss": iss, "aud": aud, "sub": sub}
		if exp != 0 {
			m["exp"] = exp
		}

		if nbf != 0 {
			m["nbf"] = nbf
		}

		return m
	}

	tests := []struct {
		wantErr error
		claims  map[string]any
		name    string
	}{
		{
			name: "expired", wantErr: ErrExpiredToken,
			claims: claims(c.OIDCIssuer, []string{PodIdentityAudience}, sub, now.Add(-time.Minute).Unix(), 0),
		},
		{
			name: "not yet valid", wantErr: ErrInvalidToken,
			claims: claims(c.OIDCIssuer, PodIdentityAudience, sub, 0, now.Add(time.Hour).Unix()),
		},
		{
			name: "foreign issuer", wantErr: ErrInvalidToken,
			claims: claims("https://elsewhere", PodIdentityAudience, sub, 0, 0),
		},
		{
			name: "not a service account", wantErr: ErrInvalidToken,
			claims: claims(c.OIDCIssuer, PodIdentityAudience, "user:bob", 0, 0),
		},
		{
			name: "string audience", wantErr: nil,
			claims: claims(c.OIDCIssuer, PodIdentityAudience, sub, 0, 0),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tok, serr := signServiceAccountJWT(key, tt.claims)
			require.NoError(t, serr)

			_, err := verifyPodIdentityToken(tok, c.OIDCIssuer, &key.PublicKey, now)
			if tt.wantErr == nil {
				require.NoError(t, err)

				return
			}

			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}
