package cognitoidp_test

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"maps"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

const (
	fedProvider = "Corp"
	fedClientID = "idp-client"
	fedSecret   = "idp-secret"
	fedKID      = "idp-key-1"
	fedSubject  = "idp-user-42"
)

// fakeIDP is an in-test OIDC provider with its own RSA key, discovery, authorize, token and jwks endpoints.
type fakeIDP struct {
	srv       *httptest.Server
	key       *rsa.PrivateKey
	signKey   *rsa.PrivateKey
	claims    map[string]any
	nonces    map[string]string
	audience  string
	mu        sync.Mutex
	expiresIn time.Duration
	tokenCode int
}

func newFakeIDP(t *testing.T) *fakeIDP {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	idp := &fakeIDP{
		key: key, signKey: key, nonces: map[string]string{}, audience: fedClientID, expiresIn: time.Hour,
		claims: map[string]any{"email": "bob@corp.example", "name": "Bob", "email_verified": true},
	}
	idp.srv = httptest.NewServer(http.HandlerFunc(idp.serve))
	t.Cleanup(idp.srv.Close)

	return idp
}

func (f *fakeIDP) serve(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/.well-known/openid-configuration":
		_ = json.NewEncoder(w).Encode(map[string]string{
			"issuer": f.srv.URL, "authorization_endpoint": f.srv.URL + "/authorize",
			"token_endpoint": f.srv.URL + "/token", "jwks_uri": f.srv.URL + "/jwks",
		})
	case "/authorize":
		f.authorize(w, r)
	case "/token":
		f.token(w, r)
	case "/jwks":
		pub := f.key.PublicKey
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]string{{
			"kty": "RSA", "kid": fedKID, "alg": "RS256", "use": "sig",
			"n": base64.RawURLEncoding.EncodeToString(pub.N.Bytes()),
			"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(pub.E)).Bytes()),
		}}})
	default:
		http.NotFound(w, r)
	}
}

func (f *fakeIDP) authorize(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	if q.Get("client_id") != fedClientID || q.Get("response_type") != "code" {
		http.Error(w, "bad request", http.StatusBadRequest)

		return
	}

	code := "idp-code-" + q.Get("state")[:6]

	f.mu.Lock()
	f.nonces[code] = q.Get("nonce")
	f.mu.Unlock()

	back, _ := url.Parse(q.Get("redirect_uri"))
	bq := back.Query()
	bq.Set("code", code)
	bq.Set("state", q.Get("state"))
	back.RawQuery = bq.Encode()
	http.Redirect(w, r, back.String(), http.StatusFound)
}

func (f *fakeIDP) token(w http.ResponseWriter, r *http.Request) {
	if f.tokenCode != 0 {
		http.Error(w, `{"error":"invalid_grant"}`, f.tokenCode)

		return
	}

	if err := r.ParseForm(); err != nil || r.PostForm.Get("client_secret") != fedSecret ||
		r.PostForm.Get("grant_type") != "authorization_code" {
		http.Error(w, `{"error":"invalid_client"}`, http.StatusUnauthorized)

		return
	}

	f.mu.Lock()
	nonce, ok := f.nonces[r.PostForm.Get("code")]
	delete(f.nonces, r.PostForm.Get("code"))
	f.mu.Unlock()

	if !ok {
		http.Error(w, `{"error":"invalid_grant"}`, http.StatusBadRequest)

		return
	}

	claims := jwt.MapClaims{
		"iss": f.srv.URL, "aud": f.audience, "sub": fedSubject, "nonce": nonce,
		"iat": time.Now().Unix(), "exp": time.Now().Add(f.expiresIn).Unix(),
	}
	maps.Copy(claims, f.claims)

	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = fedKID

	signed, err := tok.SignedString(f.signKey)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)

		return
	}

	_ = json.NewEncoder(w).Encode(map[string]string{
		"id_token": signed, "access_token": "idp-access", "token_type": "Bearer",
	})
}

// fedEnv is an oauthEnv with a federated app client and an OIDC provider pointing at a fakeIDP.
type fedEnv struct {
	*oauthEnv

	idp      *fakeIDP
	clientID string
}

func newFedEnv(t *testing.T, idp *fakeIDP, supported []string, explicit bool) *fedEnv {
	t.Helper()

	env := newOAuthEnv(t)

	details := map[string]string{
		"client_id": fedClientID, "client_secret": fedSecret, "oidc_issuer": idp.srv.URL,
		"authorize_scopes": "openid email profile", "attributes_request_method": "GET",
	}
	if explicit {
		details["authorize_url"] = idp.srv.URL + "/authorize"
		details["token_url"] = idp.srv.URL + "/token"
		details["jwks_uri"] = idp.srv.URL + "/jwks"
	}

	_, err := env.backend.CreateIdentityProviderFull(env.poolID, fedProvider, "OIDC", details,
		map[string]string{"email": "email", "name": "name", "email_verified": "email_verified", "username": "sub"},
		[]string{"corp-sso"})
	require.NoError(t, err)

	client, err := env.backend.CreateUserPoolClientWithOpts(env.poolID, "fed", cognitoidp.UserPoolClientOptions{
		AllowedOAuthFlows: []string{"code"}, AllowedOAuthFlowsUserPoolClient: true,
		AllowedOAuthScopes: []string{"openid", "email", "profile"}, CallbackURLs: []string{oauthRedirect},
		SupportedIdentityProviders: supported,
	})
	require.NoError(t, err)

	return &fedEnv{oauthEnv: env, idp: idp, clientID: client.ClientID}
}

// authorizeToIDP starts federation and returns the redirect Location pointing at the fake IdP.
func (e *fedEnv) authorizeToIDP(t *testing.T, extra url.Values) *http.Response {
	t.Helper()

	q := authorizeQuery(e.clientID, "")
	maps.Copy(q, extra)

	resp, _ := e.get(t, "/oauth2/authorize?"+q.Encode(), nil)

	return resp
}

// viaIDP follows the authorize redirect to the IdP and returns the IdP's callback request path.
func (e *fedEnv) viaIDP(t *testing.T, resp *http.Response) string {
	t.Helper()

	require.Equal(t, http.StatusFound, resp.StatusCode)

	idpResp, err := e.client.Get(resp.Header.Get("Location"))
	require.NoError(t, err)

	defer idpResp.Body.Close()

	require.Equal(t, http.StatusFound, idpResp.StatusCode)

	back, err := url.Parse(idpResp.Header.Get("Location"))
	require.NoError(t, err)
	require.Equal(t, "/oauth2/idpresponse", back.Path)

	return back.RequestURI()
}

// signIn runs authorize -> IdP -> idpresponse and returns the final response from the callback.
func (e *fedEnv) signIn(t *testing.T) *http.Response {
	t.Helper()

	cb := e.viaIDP(t, e.authorizeToIDP(t, url.Values{"identity_provider": {fedProvider}}))
	resp, _ := e.get(t, cb, nil)

	return resp
}

func (e *fedEnv) redeem(t *testing.T, code string) map[string]any {
	t.Helper()

	resp, body := e.postForm(t, "/oauth2/token", url.Values{
		"grant_type": {"authorization_code"}, "code": {code}, "redirect_uri": {oauthRedirect},
		"client_id": {e.clientID},
	}, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))

	return jsonMap(t, body)
}

func (e *fedEnv) federatedUser(t *testing.T) *cognitoidp.User {
	t.Helper()

	users, err := e.backend.ListUsers(e.poolID)
	require.NoError(t, err)

	for _, u := range users {
		if u.Username == fedProvider+"_"+fedSubject {
			return u
		}
	}

	return nil
}

func locationQuery(t *testing.T, resp *http.Response) url.Values {
	t.Helper()

	require.Equal(t, http.StatusFound, resp.StatusCode)

	loc, err := url.Parse(resp.Header.Get("Location"))
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(loc.String(), oauthRedirect), loc.String())

	return loc.Query()
}
