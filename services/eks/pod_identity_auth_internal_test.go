package eks

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestVerifyPodIdentityTokenClaims(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend(context.Background(), "123456789012", "us-east-1")
	c, err := b.CreateCluster("c1", "1.32", "", nil, nil, nil)
	require.NoError(t, err)

	key, err := b.signingKey(c.OIDCIssuer)
	require.NoError(t, err)

	now := time.Now()

	tests := []struct {
		claims  map[string]any
		wantErr error
		name    string
	}{
		{
			name:    "expired",
			claims:  map[string]any{"iss": c.OIDCIssuer, "aud": []string{PodIdentityAudience}, "exp": now.Add(-time.Minute).Unix(), "sub": "system:serviceaccount:d:a"},
			wantErr: ErrExpiredToken,
		},
		{
			name:    "not yet valid",
			claims:  map[string]any{"iss": c.OIDCIssuer, "aud": PodIdentityAudience, "nbf": now.Add(time.Hour).Unix(), "sub": "system:serviceaccount:d:a"},
			wantErr: ErrInvalidToken,
		},
		{
			name:    "foreign issuer",
			claims:  map[string]any{"iss": "https://elsewhere", "aud": PodIdentityAudience, "sub": "system:serviceaccount:d:a"},
			wantErr: ErrInvalidToken,
		},
		{
			name:    "not a service account",
			claims:  map[string]any{"iss": c.OIDCIssuer, "aud": PodIdentityAudience, "sub": "user:bob"},
			wantErr: ErrInvalidToken,
		},
		{
			name:   "string audience",
			claims: map[string]any{"iss": c.OIDCIssuer, "aud": PodIdentityAudience, "sub": "system:serviceaccount:d:a"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tok, err := signServiceAccountJWT(key, tt.claims)
			require.NoError(t, err)

			_, err = verifyPodIdentityToken(tok, c.OIDCIssuer, &key.PublicKey, now)
			if tt.wantErr == nil {
				require.NoError(t, err)

				return
			}

			require.True(t, errors.Is(err, tt.wantErr), "got %v", err)
		})
	}
}
