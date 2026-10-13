package ssooidc_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ssooidc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRegisterClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode string
		in       ssooidc.RegisterClientInput
	}{
		{
			name: "device_and_refresh",
			in: ssooidc.RegisterClientInput{
				ClientName: aws.String("c"), ClientType: aws.String("public"),
				GrantTypes: []string{"urn:ietf:params:oauth:grant-type:device_code", "refresh_token"},
				Scopes:     []string{"sso:account:access"},
			},
		},
		{
			name: "legacy_no_grants",
			in:   ssooidc.RegisterClientInput{ClientName: aws.String("c"), ClientType: aws.String("public")},
		},
		{
			name: "authorization_code",
			in: ssooidc.RegisterClientInput{
				ClientName: aws.String("c"), ClientType: aws.String("public"),
				GrantTypes: []string{"authorization_code"}, RedirectUris: []string{"https://example.com/cb"},
			},
		},
		{
			name:     "confidential_type",
			in:       ssooidc.RegisterClientInput{ClientName: aws.String("c"), ClientType: aws.String("confidential")},
			wantCode: "InvalidClientMetadataException",
		},
		{
			name: "unknown_grant",
			in: ssooidc.RegisterClientInput{
				ClientName: aws.String("c"), ClientType: aws.String("public"), GrantTypes: []string{"password"},
			},
			wantCode: "UnsupportedGrantTypeException",
		},
		{
			name: "blank_scope",
			in: ssooidc.RegisterClientInput{
				ClientName: aws.String("c"), ClientType: aws.String("public"), Scopes: []string{" "},
			},
			wantCode: "InvalidScopeException",
		},
		{
			name: "plain_http_redirect",
			in: ssooidc.RegisterClientInput{
				ClientName: aws.String("c"), ClientType: aws.String("public"),
				GrantTypes: []string{"authorization_code"}, RedirectUris: []string{"http://example.com/cb"},
			},
			wantCode: "InvalidRedirectUriException",
		},
		{
			name: "auth_code_without_redirect",
			in: ssooidc.RegisterClientInput{
				ClientName: aws.String(
					"c",
				),
				ClientType: aws.String("public"),
				GrantTypes: []string{"authorization_code"},
			},
			wantCode: "InvalidRedirectUriException",
		},
		{
			name:     "blank_name",
			in:       ssooidc.RegisterClientInput{ClientName: aws.String(" "), ClientType: aws.String("public")},
			wantCode: "InvalidRequestException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			out, err := f.client.RegisterClient(t.Context(), &tt.in)

			if tt.wantCode != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, apiCode(err))

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, aws.ToString(out.ClientId))
			assert.NotEmpty(t, aws.ToString(out.ClientSecret))
			assert.Positive(t, out.ClientIdIssuedAt)
			assert.Greater(t, out.ClientSecretExpiresAt, out.ClientIdIssuedAt)
		})
	}
}

func TestRegisterClientEndpoints(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		grants       []string
		wantEndpoint bool
	}{
		{name: "device_only", grants: []string{"urn:ietf:params:oauth:grant-type:device_code"}},
		{name: "authorization_code", grants: []string{"authorization_code"}, wantEndpoint: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			out, err := f.client.RegisterClient(t.Context(), &ssooidc.RegisterClientInput{
				ClientName: aws.String("c"), ClientType: aws.String("public"),
				GrantTypes: tt.grants, RedirectUris: redirectsFor(tt.grants),
			})
			require.NoError(t, err)

			if tt.wantEndpoint {
				assert.Equal(t, f.srv.URL+"/_gopherstack/sso/authorize", aws.ToString(out.AuthorizationEndpoint))
				assert.Equal(t, f.srv.URL+"/token", aws.ToString(out.TokenEndpoint))

				return
			}

			assert.Nil(t, out.AuthorizationEndpoint)
		})
	}
}
