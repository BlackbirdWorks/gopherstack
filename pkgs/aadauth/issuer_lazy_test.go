package aadauth_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/aadauth"
)

func TestIssuer_KeyGeneratedLazily(t *testing.T) {
	t.Parallel()

	tests := []struct {
		use  func(iss *aadauth.Issuer)
		name string
		want bool
	}{
		{name: "construct_only", use: func(*aadauth.Issuer) {}, want: false},
		{name: "key_id_only", use: func(iss *aadauth.Issuer) { _ = iss.KeyID() }, want: false},
		{name: "jwks", use: func(iss *aadauth.Issuer) { _ = iss.JWKS() }, want: true},
		{
			name: "issue_token",
			use: func(iss *aadauth.Issuer) {
				_, err := iss.IssueClientCredentialsToken(aadauth.ClientCredentialsClaims{ClientID: "c"}, 0)
				assert.NoError(t, err)
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			iss, err := aadauth.NewIssuer()
			require.NoError(t, err)
			tt.use(iss)
			assert.Equal(t, tt.want, iss.KeyGenerated())
		})
	}
}
