package ssooidc_test

import (
	"crypto/sha256"
	"encoding/base64"
	"net/http"
	"net/url"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ssooidc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	verifier    = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
	callbackURL = "http://127.0.0.1:51234/oauth/callback"
)

func s256(v string) string {
	sum := sha256.Sum256([]byte(v))

	return base64.RawURLEncoding.EncodeToString(sum[:])
}

func noRedirectClient() *http.Client {
	return &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

func (f fixture) authorize(t *testing.T, q url.Values) *http.Response {
	t.Helper()

	req, err := http.NewRequestWithContext(
		t.Context(),
		http.MethodGet,
		f.srv.URL+"/_gopherstack/sso/authorize?"+q.Encode(),
		http.NoBody,
	)
	require.NoError(t, err)

	resp, err := noRedirectClient().Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })

	return resp
}

func authQuery(clientID string) url.Values {
	return url.Values{
		"response_type": {"code"}, "client_id": {clientID}, "redirect_uri": {callbackURL},
		"state": {"st-1"}, "code_challenge": {s256(verifier)}, "code_challenge_method": {"S256"}, "user": {"bob"},
	}
}

func TestAuthorizationCodePKCE(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate   func(in *ssooidc.CreateTokenInput)
		name     string
		wantCode string
	}{
		{name: "valid"},
		{
			name: "wrong_verifier", wantCode: "InvalidGrantException",
			mutate: func(in *ssooidc.CreateTokenInput) { in.CodeVerifier = aws.String("other-verifier") },
		},
		{
			name: "wrong_redirect", wantCode: "InvalidGrantException",
			mutate: func(in *ssooidc.CreateTokenInput) { in.RedirectUri = aws.String("http://127.0.0.1:1/other") },
		},
		{
			name: "missing_verifier", wantCode: "InvalidRequestException",
			mutate: func(in *ssooidc.CreateTokenInput) { in.CodeVerifier = nil },
		},
		{
			name: "unknown_code", wantCode: "InvalidGrantException",
			mutate: func(in *ssooidc.CreateTokenInput) { in.Code = aws.String("nope") },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			id, secret := f.register(t, "authorization_code", grantRefresh)

			resp := f.authorize(t, authQuery(id))
			require.Equal(t, http.StatusFound, resp.StatusCode)

			loc, err := url.Parse(resp.Header.Get("Location"))
			require.NoError(t, err)
			assert.Equal(t, "st-1", loc.Query().Get("state"))
			assert.Equal(t, "/oauth/callback", loc.Path)

			in := &ssooidc.CreateTokenInput{
				ClientId: aws.String(id), ClientSecret: aws.String(secret), GrantType: aws.String("authorization_code"),
				Code: aws.String(loc.Query().Get("code")), CodeVerifier: aws.String(verifier),
				RedirectUri: aws.String(callbackURL),
			}
			if tt.mutate != nil {
				tt.mutate(in)
			}

			out, err := f.client.CreateToken(t.Context(), in)
			if tt.wantCode != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, apiCode(err))

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, aws.ToString(out.RefreshToken))

			sess, serr := f.handler.Backend.SessionForAccessToken(aws.ToString(out.AccessToken))
			require.NoError(t, serr)
			assert.Equal(t, "bob", sess.Subject.UserName)

			_, err = f.client.CreateToken(t.Context(), in)
			assert.Equal(t, "InvalidGrantException", apiCode(err), "an authorization code is single use")
		})
	}
}

func TestAuthorizeRejections(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate     func(q url.Values)
		name       string
		wantStatus int
	}{
		{
			name:       "no_challenge",
			mutate:     func(q url.Values) { q.Del("code_challenge") },
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "plain_method", mutate: func(q url.Values) { q.Set("code_challenge_method", "plain") },
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "unregistered_redirect",
			mutate:     func(q url.Values) { q.Set("redirect_uri", "http://127.0.0.1/elsewhere") },
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "unknown_client",
			mutate:     func(q url.Values) { q.Set("client_id", "nope") },
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "unknown_user",
			mutate:     func(q url.Values) { q.Set("user", "mallory") },
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "wrong_response_type",
			mutate:     func(q url.Values) { q.Set("response_type", "token") },
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			id, _ := f.register(t, "authorization_code")
			q := authQuery(id)
			tt.mutate(q)

			assert.Equal(t, tt.wantStatus, f.authorize(t, q).StatusCode)
		})
	}
}

func TestAuthorizationCodeNeedsRegisteredGrant(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	id, _ := f.register(t, grantDevice)

	assert.Equal(t, http.StatusBadRequest, f.authorize(t, authQuery(id)).StatusCode)
}

func TestCreateTokenWithIAM(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	id, secret := f.register(t, "authorization_code", grantRefresh)

	loc, err := url.Parse(f.authorize(t, authQuery(id)).Header.Get("Location"))
	require.NoError(t, err)

	out, err := f.client.CreateTokenWithIAM(t.Context(), &ssooidc.CreateTokenWithIAMInput{
		ClientId: aws.String(id), GrantType: aws.String("authorization_code"),
		Code: aws.String(loc.Query().Get("code")), CodeVerifier: aws.String(verifier),
		RedirectUri: aws.String(callbackURL),
	})
	require.NoError(t, err)
	assert.Equal(t, "Bearer", aws.ToString(out.TokenType))
	assert.Equal(t, []string{"sso:account:access"}, out.Scope)
	assert.Equal(t, "urn:ietf:params:oauth:token-type:access_token", aws.ToString(out.IssuedTokenType))
	assert.NotEmpty(t, aws.ToString(out.RefreshToken))

	refreshed, err := f.client.CreateTokenWithIAM(t.Context(), &ssooidc.CreateTokenWithIAMInput{
		ClientId: aws.String(id), GrantType: aws.String(grantRefresh), RefreshToken: out.RefreshToken,
	})
	require.NoError(t, err)
	assert.NotEqual(t, aws.ToString(out.AccessToken), aws.ToString(refreshed.AccessToken))

	_, err = f.client.CreateTokenWithIAM(t.Context(), &ssooidc.CreateTokenWithIAMInput{
		ClientId: aws.String(id), GrantType: aws.String(grantDevice), Code: aws.String("x"),
	})
	assert.Equal(t, "UnsupportedGrantTypeException", apiCode(err))

	_, err = f.client.CreateTokenWithIAM(t.Context(), &ssooidc.CreateTokenWithIAMInput{
		ClientId: aws.String("nope"), GrantType: aws.String(grantRefresh), RefreshToken: out.RefreshToken,
	})
	assert.Equal(t, "InvalidClientException", apiCode(err))

	_ = secret
}
