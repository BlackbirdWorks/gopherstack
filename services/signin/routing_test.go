package signin_test

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/signin"
)

func TestRouteMatcherAndExtraction(t *testing.T) {
	t.Parallel()

	const signed = "AWS4-HMAC-SHA256 Credential=AKIA/20260101/us-east-1/signin/aws4_request, " +
		"SignedHeaders=host, Signature=00"

	tests := []struct {
		name      string
		method    string
		path      string
		auth      string
		host      string
		ua        string
		wantOp    string
		wantMatch bool
	}{
		{
			name:      "token ua",
			method:    http.MethodPost,
			path:      "/v1/token",
			ua:        "aws-sdk-go-v2 api/signin#1.10.3",
			wantOp:    "CreateOAuth2Token",
			wantMatch: true,
		},
		{
			name:      "token iam",
			method:    http.MethodPost,
			path:      "/v1/token?x-amz-client-auth-method=iam",
			auth:      signed,
			wantOp:    "CreateOAuth2TokenWithIAM",
			wantMatch: true,
		},
		{
			name:      "token host",
			method:    http.MethodPost,
			path:      "/v1/token",
			host:      "us-east-1.oauth.signin.aws",
			wantOp:    "CreateOAuth2Token",
			wantMatch: true,
		},
		{name: "token no ident", method: http.MethodPost, path: "/v1/token", wantOp: "CreateOAuth2Token"},
		{
			name:   "token s3 signed",
			method: http.MethodPost,
			path:   "/v1/token",
			auth:   "AWS4-HMAC-SHA256 Credential=AKIA/20260101/us-east-1/s3/aws4_request",
			wantOp: "CreateOAuth2Token",
		},
		{
			name:      "introspect",
			method:    http.MethodPost,
			path:      "/v1/introspect",
			auth:      signed,
			wantOp:    "IntrospectOAuth2TokenWithIAM",
			wantMatch: true,
		},
		{
			name:      "revoke",
			method:    http.MethodPost,
			path:      "/v1/revoke",
			auth:      signed,
			wantOp:    "RevokeOAuth2TokenWithIAM",
			wantMatch: true,
		},
		{
			name:      "get policy",
			method:    http.MethodPost,
			path:      "/get-resource-policy",
			auth:      signed,
			wantOp:    "GetResourcePolicy",
			wantMatch: true,
		},
		{
			name:      "list",
			method:    http.MethodPost,
			path:      "/list-resource-permission-statements",
			host:      "signin.us-east-1.api.aws",
			wantOp:    "ListResourcePermissionStatements",
			wantMatch: true,
		},
		{name: "get method", method: http.MethodGet, path: "/get-resource-policy", auth: signed, wantOp: "Unknown"},
		{name: "unrelated", method: http.MethodPost, path: "/other", auth: signed, wantOp: "Unknown"},
		{
			name:      "authorize",
			method:    http.MethodGet,
			path:      "/_gopherstack/signin/authorize",
			wantOp:    "Authorize",
			wantMatch: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := signin.NewHandler(signin.NewInMemoryBackend(testAccount, testRegion))
			req := httptest.NewRequest(tt.method, tt.path, nil)

			if tt.auth != "" {
				req.Header.Set("Authorization", tt.auth)
			}

			if tt.ua != "" {
				req.Header.Set("User-Agent", tt.ua)
			}

			if tt.host != "" {
				req.Host = tt.host
			}

			c := echo.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.wantMatch, h.RouteMatcher()(c))
			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
		})
	}
}

func TestAuthorizeEndpointIssuesCode(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	hc := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	q := "?client_id=" + testClient + "&redirect_uri=http://127.0.0.1:8080/callback&state=xyz" +
		"&code_challenge_method=S256&code_challenge=" + challenge()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, f.url+"/_gopherstack/signin/authorize"+q, nil)
	require.NoError(t, err)

	resp, err := hc.Do(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusFound, resp.StatusCode)

	loc, err := resp.Location()
	require.NoError(t, err)
	assert.Equal(t, "xyz", loc.Query().Get("state"))
	assert.NotEmpty(t, loc.Query().Get("code"))

	req, err = http.NewRequestWithContext(
		t.Context(),
		http.MethodGet,
		f.url+"/_gopherstack/signin/authorize?client_id=x",
		nil,
	)
	require.NoError(t, err)

	resp, err = hc.Do(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestAuthorizeRedirectTargets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		redirect string
		wantHost string
		wantPath string
		wantCode int
	}{
		{
			name:     "ipv4",
			redirect: "http://127.0.0.1:8080/callback",
			wantHost: "127.0.0.1:8080",
			wantPath: "/callback",
			wantCode: http.StatusFound,
		},
		{
			name:     "localhost no port",
			redirect: "http://localhost/cb",
			wantHost: "localhost",
			wantPath: "/cb",
			wantCode: http.StatusFound,
		},
		{
			name:     "ipv6",
			redirect: "http://[::1]:9/x",
			wantHost: "[::1]:9",
			wantPath: "/x",
			wantCode: http.StatusFound,
		},
		{name: "external host", redirect: "http://evil.example.com/cb", wantCode: http.StatusBadRequest},
		{name: "https loopback", redirect: "https://127.0.0.1/cb", wantCode: http.StatusBadRequest},
		{name: "userinfo trick", redirect: "http://127.0.0.1@evil.example.com/cb", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			hc := &http.Client{
				CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
			}
			q := "?client_id=" + testClient + "&redirect_uri=" + url.QueryEscape(tt.redirect) +
				"&state=s&code_challenge_method=S256&code_challenge=" + challenge()

			target := f.url + "/_gopherstack/signin/authorize" + q

			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, target, nil)
			require.NoError(t, err)

			resp, err := hc.Do(req)
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())
			require.Equal(t, tt.wantCode, resp.StatusCode)

			if tt.wantCode != http.StatusFound {
				return
			}

			loc, err := resp.Location()
			require.NoError(t, err)
			assert.Equal(t, tt.wantHost, loc.Host)
			assert.Equal(t, tt.wantPath, loc.Path)
			assert.Equal(t, "s", loc.Query().Get("state"))
			assert.NotEmpty(t, loc.Query().Get("code"))
		})
	}
}
