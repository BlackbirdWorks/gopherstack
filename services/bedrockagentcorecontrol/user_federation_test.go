package bedrockagentcorecontrol_test

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json/v2"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	dataplane "github.com/blackbirdworks/gopherstack/services/bedrockagentcore"
	"github.com/blackbirdworks/gopherstack/services/bedrockagentcorecontrol"
)

type jwksIDP struct {
	*httptest.Server
	key  *rsa.PrivateKey
	hits atomic.Int32
}

func newJWKSIDP(t *testing.T) *jwksIDP {
	t.Helper()

	return newJWKSIDPWith(t, 2048, "")
}

func newJWKSIDPWith(t *testing.T, bits int, docIssuer string) *jwksIDP {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, bits)
	require.NoError(t, err)

	idp := &jwksIDP{key: key}
	idp.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body any

		switch r.URL.Path {
		case "/.well-known/openid-configuration":
			iss := idp.URL
			if docIssuer != "" {
				iss = docIssuer
			}

			body = map[string]string{"issuer": iss, "jwks_uri": idp.URL + "/jwks"}
		case "/jwks":
			idp.hits.Add(1)

			body = map[string]any{"keys": []map[string]string{{
				"kty": "RSA", "kid": testKID, "alg": "RS256",
				"n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
				"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes()),
			}}}
		default:
			http.NotFound(w, r)

			return
		}

		raw, _ := json.Marshal(body, jsonx.V1())
		_, _ = w.Write(raw)
	}))
	t.Cleanup(idp.Close)

	return idp
}

func (i *jwksIDP) sign(t *testing.T, key *rsa.PrivateKey, claims jwt.MapClaims) string {
	t.Helper()

	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = testKID

	s, err := tok.SignedString(key)
	require.NoError(t, err)

	return s
}

func TestVerifyUserJWT(t *testing.T) {
	t.Parallel()

	other, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	tests := []struct {
		name    string
		signer  func(i *jwksIDP) *rsa.PrivateKey
		claims  func(i *jwksIDP) jwt.MapClaims
		wantSub string
		wantErr bool
	}{
		{
			name: "valid", signer: func(i *jwksIDP) *rsa.PrivateKey { return i.key }, wantSub: "alice",
			claims: func(i *jwksIDP) jwt.MapClaims {
				return jwt.MapClaims{"iss": i.URL, "sub": "alice", "exp": time.Now().Add(time.Hour).Unix()}
			},
		},
		{
			name: "forged signature", signer: func(*jwksIDP) *rsa.PrivateKey { return other }, wantErr: true,
			claims: func(i *jwksIDP) jwt.MapClaims {
				return jwt.MapClaims{"iss": i.URL, "sub": "alice", "exp": time.Now().Add(time.Hour).Unix()}
			},
		},
		{
			name: "expired", signer: func(i *jwksIDP) *rsa.PrivateKey { return i.key }, wantErr: true,
			claims: func(i *jwksIDP) jwt.MapClaims {
				return jwt.MapClaims{"iss": i.URL, "sub": "alice", "exp": time.Now().Add(-time.Hour).Unix()}
			},
		},
		{
			name: "no subject", signer: func(i *jwksIDP) *rsa.PrivateKey { return i.key }, wantErr: true,
			claims: func(i *jwksIDP) jwt.MapClaims {
				return jwt.MapClaims{"iss": i.URL, "exp": time.Now().Add(time.Hour).Unix()}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			idp := newJWKSIDP(t)
			f := newFixture(t)
			token := idp.sign(t, tt.signer(idp), tt.claims(idp))

			sub, verr := f.backend.DataPlane().VerifyUserJWT(t.Context(), token)
			if tt.wantErr {
				require.Error(t, verr)

				return
			}

			require.NoError(t, verr)
			assert.Equal(t, tt.wantSub, sub)

			_, verr = f.backend.DataPlane().VerifyUserJWT(t.Context(), token)
			require.NoError(t, verr)
			assert.Equal(t, int32(1), idp.hits.Load(), "the JWKS is cached")
		})
	}
}

func TestUserFederationFlow(t *testing.T) {
	t.Parallel()

	idp := newCodeIDP(t)
	m := newMCPFixture(t)
	_, callback := m.codeProvider(t, idp)
	ctx := awsmeta.Set(t.Context(), &awsmeta.Metadata{Account: testAccount, Region: "us-east-1"})
	seam := m.backend.DataPlane()
	sc := dataplane.Scope{Account: testAccount, Region: "us-east-1"}
	req := dataplane.FederationRequest{
		Provider: "idp", UserID: "alice", Scopes: []string{"a"},
		ReturnURL: "https://app.example/cb", CustomState: "xyz",
	}

	start, err := seam.FederatedToken(ctx, sc, req)
	require.NoError(t, err)
	require.True(t, start.InProgress)
	assert.Contains(t, start.AuthorizationURL, idp.URL+"/authorize?")

	authURL, err := url.Parse(start.AuthorizationURL)
	require.NoError(t, err)

	resume := req
	resume.SessionURI = start.SessionURI

	cb := m.url + callbackPath(callback) + "?code=c1&state=" + authURL.Query().Get("state")
	hreq, err := http.NewRequestWithContext(ctx, http.MethodGet, cb, http.NoBody)
	require.NoError(t, err)

	resp, err := noRedirect(t).Do(hreq)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, "https://app.example/cb?state=xyz", resp.Header.Get("Location"))

	pending, err := seam.FederatedToken(ctx, sc, resume)
	require.NoError(t, err)
	assert.True(t, pending.InProgress, "the user is not bound until CompleteResourceTokenAuth")

	require.ErrorIs(t, seam.CompleteFederation(ctx, sc, start.SessionURI,
		dataplane.UserIdentifier{UserID: "mallory"}), bedrockagentcorecontrol.ErrSessionMismatch)
	require.NoError(t, seam.CompleteFederation(ctx, sc, start.SessionURI, dataplane.UserIdentifier{UserID: "alice"}))

	done, err := seam.FederatedToken(ctx, sc, resume)
	require.NoError(t, err)
	assert.Equal(t, "at-c1", done.AccessToken)

	cached, err := seam.FederatedToken(ctx, sc, req)
	require.NoError(t, err)
	assert.Equal(t, "at-c1", cached.AccessToken, "a bound session is reused")

	req.Force = true
	forced, err := seam.FederatedToken(ctx, sc, req)
	require.NoError(t, err)
	assert.True(t, forced.InProgress)

	unknown := dataplane.FederationRequest{Provider: "idp", UserID: "alice", SessionURI: "nope"}
	_, err = seam.FederatedToken(ctx, sc, unknown)
	require.ErrorIs(t, err, dataplane.ErrNotFound)
}
