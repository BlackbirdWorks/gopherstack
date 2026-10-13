package iam_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

func TestCreateOpenIDConnectProvider_ArnKeepsIssuerPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		url     string
		wantArn string
	}{
		{
			name: "path",
			url:  "https://oidc.eks.us-east-1.amazonaws.com/id/0123456789abcdef0123456789abcdef",
			wantArn: "arn:aws:iam::000000000000:oidc-provider/" +
				"oidc.eks.us-east-1.amazonaws.com/id/0123456789abcdef0123456789abcdef",
		},
		{
			name:    "trailing slash",
			url:     "https://example.com/tenant/",
			wantArn: "arn:aws:iam::000000000000:oidc-provider/example.com/tenant",
		},
		{
			name:    "host only",
			url:     "https://token.actions.githubusercontent.com",
			wantArn: "arn:aws:iam::000000000000:oidc-provider/token.actions.githubusercontent.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			p, err := b.CreateOpenIDConnectProvider(tt.url, []string{"sts.amazonaws.com"}, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.wantArn, p.Arn)

			ids, ok := b.OIDCProviderClientIDs(tt.url)
			require.True(t, ok)
			assert.Equal(t, []string{"sts.amazonaws.com"}, ids)
		})
	}
}
