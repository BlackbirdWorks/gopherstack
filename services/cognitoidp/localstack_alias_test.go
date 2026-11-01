package cognitoidp_test

import (
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	cognitoidpsdk "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

const aliasPrefix = "/_aws/cognito-idp"

func newAliasServer(t *testing.T) (*httptest.Server, *cognitoidpsdk.Client) {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(newTestHandler(t)))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	return srv, cognitoidpsdk.NewFromConfig(cfg, func(o *cognitoidpsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

func TestLocalStackAlias_AuthCodeFlowWithPinnedIDs(t *testing.T) {
	t.Parallel()

	srv, sdk := newAliasServer(t)
	ctx := t.Context()

	pool, err := sdk.CreateUserPool(ctx, &cognitoidpsdk.CreateUserPoolInput{
		PoolName:     aws.String("alias-pool"),
		UserPoolTags: map[string]string{"_custom_id_": "us-east-1_aliaspool"},
	})
	require.NoError(t, err)
	assert.Equal(t, "us-east-1_aliaspool", aws.ToString(pool.UserPool.Id))

	cl, err := sdk.CreateUserPoolClient(ctx, &cognitoidpsdk.CreateUserPoolClientInput{
		UserPoolId:                      pool.UserPool.Id,
		ClientName:                      aws.String("_custom_id_:myclient123"),
		AllowedOAuthFlows:               []types.OAuthFlowType{types.OAuthFlowTypeCode},
		AllowedOAuthFlowsUserPoolClient: true,
		AllowedOAuthScopes:              []string{"openid", "email"},
		CallbackURLs:                    []string{oauthRedirect},
		SupportedIdentityProviders:      []string{"COGNITO"},
	})
	require.NoError(t, err)
	assert.Equal(t, "myclient123", aws.ToString(cl.UserPoolClient.ClientId))

	_, err = sdk.AdminCreateUser(ctx, &cognitoidpsdk.AdminCreateUserInput{
		UserPoolId: pool.UserPool.Id, Username: aws.String(oauthUser), MessageAction: types.MessageActionTypeSuppress,
	})
	require.NoError(t, err)
	_, err = sdk.AdminSetUserPassword(ctx, &cognitoidpsdk.AdminSetUserPasswordInput{
		UserPoolId: pool.UserPool.Id, Username: aws.String(oauthUser),
		Password: aws.String(oauthPassword), Permanent: true,
	})
	require.NoError(t, err)

	jar, err := cookiejar.New(nil)
	require.NoError(t, err)

	hc := &http.Client{Jar: jar, CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}

	q := url.Values{
		"response_type": {"code"}, "client_id": {"myclient123"}, "redirect_uri": {oauthRedirect}, "state": {"st8"},
	}
	loginURL := aliasPrefix + "/login?" + q.Encode()

	resp, err := hc.Get(srv.URL + loginURL)
	require.NoError(t, err)

	page, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode, string(page))
	assert.Contains(t, string(page), `action="`+aliasPrefix+`/login?`)

	csrf := between(string(page), `name="_csrf" value="`, `"`)
	require.NotEmpty(t, csrf)

	resp, err = hc.PostForm(srv.URL+loginURL, url.Values{
		"username": {oauthUser}, "password": {oauthPassword}, "_csrf": {csrf},
	})
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusFound, resp.StatusCode)

	code := codeFrom(t, resp)
	require.NotEmpty(t, code)

	resp, err = hc.PostForm(srv.URL+aliasPrefix+"/oauth2/token", url.Values{
		"grant_type": {"authorization_code"}, "code": {code},
		"redirect_uri": {oauthRedirect}, "client_id": {"myclient123"},
	})
	require.NoError(t, err)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))

	tok := jsonMap(t, body)
	assert.Equal(t, "Bearer", tok["token_type"])
	assert.NotEmpty(t, tok["access_token"])

	req, err := http.NewRequest(http.MethodGet, srv.URL+aliasPrefix+"/oauth2/userInfo", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+tok["access_token"].(string))

	resp, err = hc.Do(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestLocalStackAlias_AuthorizeRedirectsToAliasedLogin(t *testing.T) {
	t.Parallel()

	env := newOAuthEnv(t)
	q := authorizeQuery(env.pubID, "")

	resp, _ := env.get(t, aliasPrefix+"/oauth2/authorize?"+q.Encode(), nil)
	require.Equal(t, http.StatusFound, resp.StatusCode)
	assert.True(t, strings.HasPrefix(resp.Header.Get("Location"), aliasPrefix+"/login?"), resp.Header.Get("Location"))
}

func TestLocalStackAlias_RouteMatcher(t *testing.T) {
	t.Parallel()

	env := newOAuthEnv(t)

	tests := []struct {
		name   string
		method string
		target string
		want   bool
	}{
		{"login", "GET", aliasPrefix + "/login?client_id=x", true},
		{"login post", "POST", aliasPrefix + "/login?client_id=x", true},
		{"login without client", "GET", aliasPrefix + "/login", false},
		{"authorize", "GET", aliasPrefix + "/oauth2/authorize?client_id=x", true},
		{"token", "POST", aliasPrefix + "/oauth2/token", true},
		{"token get", "GET", aliasPrefix + "/oauth2/token", false},
		{"userinfo", "GET", aliasPrefix + "/oauth2/userInfo", true},
		{"logout", "GET", aliasPrefix + "/logout?client_id=x", true},
		{"unknown subpath", "GET", aliasPrefix + "/other", false},
		{"bare prefix", "GET", aliasPrefix, false},
		{"sqs inspect", "GET", "/_aws/sqs/messages", false},
		{"ses retro", "GET", "/_aws/ses", false},
		{"health", "GET", "/_aws/health", false},
		{"other service login alias", "GET", "/_aws/other/login?client_id=x", false},
	}

	e := echo.New()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tt.method, tt.target, nil)
			assert.Equal(t, tt.want, env.handler.RouteMatcher()(e.NewContext(req, httptest.NewRecorder())))
		})
	}
}

func TestPinnedClientID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		first   string
		second  string
		wantID  string
		wantErr bool
		useOpts bool
	}{
		{name: "pinned", first: "_custom_id_:abc123", wantID: "abc123"},
		{name: "pinned via opts", first: "_custom_id_:abc123", wantID: "abc123", useOpts: true},
		{name: "duplicate", first: "_custom_id_:abc123", second: "_custom_id_:abc123", wantErr: true},
		{name: "duplicate via opts", first: "_custom_id_:abc123", second: "_custom_id_:abc123", wantErr: true, useOpts: true},
		{name: "empty id", first: "_custom_id_:", wantErr: true},
		{name: "bad chars", first: "_custom_id_:a b/c", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			pool, err := b.CreateUserPoolWithOpts("p", cognitoidp.UserPoolOptions{})
			require.NoError(t, err)

			create := func(name string) (*cognitoidp.UserPoolClient, error) {
				if tt.useOpts {
					return b.CreateUserPoolClientWithOpts(pool.ID, name, cognitoidp.UserPoolClientOptions{})
				}

				return b.CreateUserPoolClient(pool.ID, name)
			}

			c, err := create(tt.first)
			if tt.second == "" {
				if tt.wantErr {
					require.ErrorIs(t, err, cognitoidp.ErrInvalidParameter)

					return
				}

				require.NoError(t, err)
				assert.Equal(t, tt.wantID, c.ClientID)

				return
			}

			require.NoError(t, err)
			_, err = create(tt.second)
			require.ErrorIs(t, err, cognitoidp.ErrInvalidParameter)
		})
	}
}

func TestPinnedClientID_SDKDuplicateIsInvalidParameter(t *testing.T) {
	t.Parallel()

	_, sdk := newAliasServer(t)
	ctx := t.Context()

	pool, err := sdk.CreateUserPool(ctx, &cognitoidpsdk.CreateUserPoolInput{PoolName: aws.String("p")})
	require.NoError(t, err)

	in := &cognitoidpsdk.CreateUserPoolClientInput{
		UserPoolId: pool.UserPool.Id, ClientName: aws.String("_custom_id_:dup1"),
	}
	_, err = sdk.CreateUserPoolClient(ctx, in)
	require.NoError(t, err)

	_, err = sdk.CreateUserPoolClient(ctx, in)
	var ipe *types.InvalidParameterException
	require.ErrorAs(t, err, &ipe)
}
