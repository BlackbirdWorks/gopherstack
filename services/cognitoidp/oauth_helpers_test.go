package cognitoidp_test

import (
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"math/big"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/golang-jwt/jwt/v5"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

const (
	keySub        = "sub"
	keyName       = "name"
	keyEmail      = "email"
	oauthRedirect = "https://app.example.com/cb"
	oauthLogout   = "https://app.example.com/bye"
	oauthPassword = "Pass1234!"
	oauthUser     = "alice"
)

type oauthEnv struct {
	backend  *cognitoidp.InMemoryBackend
	handler  *cognitoidp.Handler
	srv      *httptest.Server
	client   *http.Client
	poolID   string
	webID    string
	webSec   string
	pubID    string
	m2mID    string
	m2mSec   string
	noRevoke string
}

func newOAuthEnv(t *testing.T) *oauthEnv {
	t.Helper()

	b := newTestBackend()
	h := cognitoidp.NewHandler(b, "us-east-1")
	e := echo.New()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = h.Handler()(e.NewContext(r, w))
	}))
	t.Cleanup(srv.Close)

	jar, err := cookiejar.New(nil)
	require.NoError(t, err)

	env := &oauthEnv{backend: b, handler: h, srv: srv, client: &http.Client{
		Jar: jar, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}

	pool, err := b.CreateUserPool("oauth-pool")
	require.NoError(t, err)

	env.poolID = pool.ID

	_, err = b.CreateResourceServer(pool.ID, "api", "api", []cognitoidp.ResourceServerScope{
		{ScopeName: "read"}, {ScopeName: "write"},
	})
	require.NoError(t, err)

	user, err := b.SignUp(env.mkClient(t, "pub", false, []string{"code"}, true).ClientID, oauthUser, oauthPassword,
		map[string]string{keyEmail: "alice@example.com", keyName: "Alice", "phone_number": "+15555550100"})
	require.NoError(t, err)
	require.NoError(t, b.AdminConfirmSignUp(pool.ID, user.Username))

	web := env.mkClient(t, "web", true, []string{"code", "implicit"}, true)
	env.webID, env.webSec = web.ClientID, web.ClientSecret
	env.pubID = env.lookupClient(t, "pub")
	m2m := env.mkClient(t, "m2m", true, []string{"client_credentials"}, true)
	env.m2mID, env.m2mSec = m2m.ClientID, m2m.ClientSecret
	env.noRevoke = env.mkClient(t, "norevoke", false, []string{"code"}, false).ClientID

	return env
}

func (e *oauthEnv) lookupClient(t *testing.T, name string) string {
	t.Helper()

	clients, err := e.backend.ListUserPoolClients(e.poolID)
	require.NoError(t, err)

	for _, c := range clients {
		if c.ClientName == name {
			return c.ClientID
		}
	}

	require.FailNow(t, "client not found", name)

	return ""
}

func (e *oauthEnv) mkClient(
	t *testing.T, name string, secret bool, flows []string, revocation bool,
) *cognitoidp.UserPoolClient {
	t.Helper()

	scopes := []string{"openid", "email", "profile", "phone", "aws.cognito.signin.user.admin"}
	if flows[0] == "client_credentials" {
		scopes = []string{"api/read", "api/write"}
	}

	flowsExplicit := []string{"ALLOW_REFRESH_TOKEN_AUTH"}
	if name == "pub" {
		flowsExplicit = append(flowsExplicit, "ALLOW_USER_PASSWORD_AUTH")
	}

	c, err := e.backend.CreateUserPoolClientWithOpts(e.poolID, name, cognitoidp.UserPoolClientOptions{
		GenerateSecret: secret, AllowedOAuthFlows: flows, AllowedOAuthFlowsUserPoolClient: true,
		AllowedOAuthScopes: scopes, CallbackURLs: []string{oauthRedirect}, LogoutURLs: []string{oauthLogout},
		EnableTokenRevocation: revocation,
		ExplicitAuthFlows:     flowsExplicit,
	})
	require.NoError(t, err)

	return c
}

func pkcePair() (string, string) {
	verifier := strings.Repeat("v", 50)
	sum := sha256.Sum256([]byte(verifier))

	return verifier, base64.RawURLEncoding.EncodeToString(sum[:])
}

func (e *oauthEnv) do(t *testing.T, req *http.Request) (*http.Response, []byte) {
	t.Helper()

	resp, err := e.client.Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp, body
}

func (e *oauthEnv) get(t *testing.T, path string, hdr map[string]string) (*http.Response, []byte) {
	t.Helper()

	req, err := http.NewRequest(http.MethodGet, e.srv.URL+path, nil)
	require.NoError(t, err)

	for k, v := range hdr {
		req.Header.Set(k, v)
	}

	return e.do(t, req)
}

func (e *oauthEnv) postForm(
	t *testing.T,
	path string,
	form url.Values,
	hdr map[string]string,
) (*http.Response, []byte) {
	t.Helper()

	req, err := http.NewRequest(http.MethodPost, e.srv.URL+path, strings.NewReader(form.Encode()))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	for k, v := range hdr {
		req.Header.Set(k, v)
	}

	return e.do(t, req)
}

func basic(id, secret string) map[string]string {
	return map[string]string{"Authorization": "Basic " + base64.StdEncoding.EncodeToString([]byte(id+":"+secret))}
}

func authorizeQuery(clientID, challenge string) url.Values {
	q := url.Values{
		"response_type": {"code"}, "client_id": {clientID}, "redirect_uri": {oauthRedirect},
		"scope": {"openid email profile"}, "state": {"st8"},
	}
	if challenge != "" {
		q.Set("code_challenge", challenge)
		q.Set("code_challenge_method", "S256")
	}

	return q
}

// login drives authorize -> GET /login -> POST /login and returns the final redirect response.
func (e *oauthEnv) login(t *testing.T, q url.Values) *http.Response {
	t.Helper()

	resp, _ := e.get(t, "/oauth2/authorize?"+q.Encode(), nil)
	require.Equal(t, http.StatusFound, resp.StatusCode)

	loginURL := resp.Header.Get("Location")
	require.True(t, strings.HasPrefix(loginURL, "/login?"))

	resp, body := e.get(t, loginURL, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Contains(t, string(body), `name="_csrf"`)

	csrf := between(string(body), `name="_csrf" value="`, `"`)
	resp, _ = e.postForm(
		t,
		loginURL,
		url.Values{"username": {oauthUser}, "password": {oauthPassword}, "_csrf": {csrf}},
		nil,
	)

	return resp
}

func between(s, start, end string) string {
	_, rest, _ := strings.Cut(s, start)
	out, _, _ := strings.Cut(rest, end)

	return out
}

func codeFrom(t *testing.T, resp *http.Response) string {
	t.Helper()

	require.Equal(t, http.StatusFound, resp.StatusCode)

	loc, err := url.Parse(resp.Header.Get("Location"))
	require.NoError(t, err)
	require.Equal(t, "st8", loc.Query().Get("state"))

	return loc.Query().Get("code")
}

func jsonMap(t *testing.T, body []byte) map[string]any {
	t.Helper()

	var m map[string]any
	require.NoError(t, json.Unmarshal(body, &m), string(body))

	return m
}

// verifyAgainstJWKS validates tok against the served JWKS document and returns its claims.
func (e *oauthEnv) verifyAgainstJWKS(t *testing.T, tok string) jwt.MapClaims {
	t.Helper()

	_, body := e.get(t, "/"+e.poolID+"/.well-known/jwks.json", nil)

	var set struct {
		Keys []struct {
			Kid string `json:"kid"`
			N   string `json:"n"`
			E   string `json:"e"`
		} `json:"keys"`
	}
	require.NoError(t, json.Unmarshal(body, &set))
	require.NotEmpty(t, set.Keys)

	parsed, err := jwt.Parse(tok, func(tk *jwt.Token) (any, error) {
		for _, k := range set.Keys {
			if k.Kid != tk.Header["kid"] {
				continue
			}

			n, _ := base64.RawURLEncoding.DecodeString(k.N)
			ex, _ := base64.RawURLEncoding.DecodeString(k.E)

			return &rsa.PublicKey{N: new(big.Int).SetBytes(n), E: int(new(big.Int).SetBytes(ex).Int64())}, nil
		}

		return nil, jwt.ErrTokenUnverifiable
	}, jwt.WithValidMethods([]string{"RS256"}))
	require.NoError(t, err)

	claims, ok := parsed.Claims.(jwt.MapClaims)
	require.True(t, ok)

	return claims
}
