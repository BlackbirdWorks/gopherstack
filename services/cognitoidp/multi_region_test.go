package cognitoidp_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

const (
	mrHome    = "us-east-1"
	mrPeer    = "eu-west-1"
	mrAccount = "000000000000"
)

func newRegionHandler() *cognitoidp.Handler {
	h := cognitoidp.NewHandler(cognitoidp.NewInMemoryBackend(mrAccount, mrHome, "http://localhost:8000"), mrHome)
	h.EnableRegions(context.Background())

	return h
}

func serve(t *testing.T, h *cognitoidp.Handler, req *http.Request, region string) *httptest.ResponseRecorder {
	t.Helper()

	if region != "" {
		req = req.WithContext(awsmeta.Set(req.Context(), &awsmeta.Metadata{Region: region, Account: mrAccount}))
	}

	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(echo.New().NewContext(req, rec)))

	return rec
}

func idpCall(t *testing.T, h *cognitoidp.Handler, region, op, body string) map[string]any {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("X-Amz-Target", "AWSCognitoIdentityProviderService."+op)
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")

	rec := serve(t, h, req, region)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	out := map[string]any{}
	if rec.Body.Len() > 0 {
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	}

	return out
}

func createPool(t *testing.T, h *cognitoidp.Handler, region, name string) string {
	t.Helper()

	pool, _ := idpCall(t, h, region, "CreateUserPool", `{"PoolName":"`+name+`"}`)["UserPool"].(map[string]any)
	id, _ := pool["Id"].(string)
	require.NotEmpty(t, id)

	return id
}

func poolNames(t *testing.T, h *cognitoidp.Handler, region string) []string {
	t.Helper()

	list, _ := idpCall(t, h, region, "ListUserPools", `{"MaxResults":60}`)["UserPools"].([]any)

	names := make([]string, 0, len(list))

	for _, v := range list {
		m, _ := v.(map[string]any)
		n, _ := m["Name"].(string)
		names = append(names, n)
	}

	return names
}

func TestHandler_MultiRegionIsolation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		regions []string
	}{
		{name: "two-regions", regions: []string{"us-east-1", "eu-west-1"}},
		{name: "three-regions", regions: []string{"us-east-1", "eu-west-1", "ap-south-1"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newRegionHandler()

			for _, r := range tc.regions {
				assert.True(t, strings.HasPrefix(createPool(t, h, r, "shared"), r+"_"))
				createPool(t, h, r, "only-"+r)
			}

			for _, r := range tc.regions {
				assert.ElementsMatch(t, []string{"shared", "only-" + r}, poolNames(t, h, r))
			}
		})
	}
}

func TestHandler_RoutesByOwningResource(t *testing.T) {
	t.Parallel()

	h := newRegionHandler()
	poolID := createPool(t, h, mrPeer, "eu")

	client, _ := idpCall(t, h, "", "CreateUserPoolClient",
		`{"UserPoolId":"`+poolID+`","ClientName":"c",`+
			`"ExplicitAuthFlows":["ALLOW_USER_PASSWORD_AUTH","ALLOW_REFRESH_TOKEN_AUTH"]}`,
	)["UserPoolClient"].(map[string]any)
	clientID, _ := client["ClientId"].(string)
	require.NotEmpty(t, clientID)

	idpCall(t, h, "", "SignUp", `{"ClientId":"`+clientID+`","Username":"u1","Password":"Passw0rd!Aa1"}`)
	idpCall(t, h, "", "AdminConfirmSignUp", `{"UserPoolId":"`+poolID+`","Username":"u1"}`)

	auth, _ := idpCall(t, h, "", "InitiateAuth",
		`{"AuthFlow":"USER_PASSWORD_AUTH","ClientId":"`+clientID+
			`","AuthParameters":{"USERNAME":"u1","PASSWORD":"Passw0rd!Aa1"}}`,
	)["AuthenticationResult"].(map[string]any)
	token, _ := auth["AccessToken"].(string)
	require.NotEmpty(t, token)

	got := idpCall(t, h, "", "GetUser", `{"AccessToken":"`+token+`"}`)
	assert.Equal(t, "u1", got["Username"])

	assert.Empty(t, poolNames(t, h, mrHome))
	assert.Equal(t, []string{"eu"}, poolNames(t, h, mrPeer))
}

func TestHandler_OAuthDiscoveryAndJWKSRouteToOwningPool(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		region string
		suffix string
		want   string
	}{
		{name: "peer-jwks", region: mrPeer, suffix: "/.well-known/jwks.json", want: `"keys"`},
		{name: "peer-discovery", region: mrPeer, suffix: "/.well-known/openid-configuration", want: "issuer"},
		{name: "home-jwks", region: mrHome, suffix: "/.well-known/jwks.json", want: `"keys"`},
		{name: "home-discovery", region: mrHome, suffix: "/.well-known/openid-configuration", want: "issuer"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newRegionHandler()
			poolID := createPool(t, h, tc.region, "p")

			rec := serve(t, h, httptest.NewRequest(http.MethodGet, "/"+poolID+tc.suffix, nil), mrHome)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			assert.Contains(t, rec.Body.String(), tc.want)

			missing := serve(t, h, httptest.NewRequest(http.MethodGet, "/"+mrPeer+"_nosuchpool"+tc.suffix, nil), mrHome)
			assert.NotEqual(t, http.StatusOK, missing.Code)
		})
	}
}

func TestHandler_GetJWTPublicKeySearchesEveryRegion(t *testing.T) {
	t.Parallel()

	h := newRegionHandler()
	createPool(t, h, mrPeer, "eu")

	_, err := h.GetJWTPublicKey("http://localhost:8000/unknown", "kid")
	require.ErrorIs(t, err, cognitoidp.ErrJWTIssuerUnknown)
}

func TestHandler_MultiRegionPersistence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		remote bool
	}{
		{name: "home-only-snapshot-unchanged"},
		{name: "peer-round-trips", remote: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			src := newRegionHandler()
			createPool(t, src, mrHome, "home")

			if tc.remote {
				createPool(t, src, mrPeer, "eu")
			}

			snap := src.Snapshot(context.Background())
			require.NotNil(t, snap)
			assert.Equal(t, tc.remote, strings.Contains(string(snap), `"regions"`))

			dst := newRegionHandler()
			require.NoError(t, dst.Restore(context.Background(), snap))
			assert.Equal(t, []string{"home"}, poolNames(t, dst, mrHome))
			assert.Equal(t, tc.remote, len(poolNames(t, dst, mrPeer)) == 1)

			old := cognitoidp.NewInMemoryBackend(mrAccount, mrHome, "http://localhost:8000")
			require.NoError(t, old.Restore(context.Background(), snap))
		})
	}
}

func TestHandler_HostedAuthorizeRoutesByClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		clientID func(actual string) string
		name     string
		region   string
		wantOK   bool
	}{
		{name: "peer-client", region: mrPeer, clientID: func(r string) string { return r }, wantOK: true},
		{name: "home-client", region: mrHome, clientID: func(r string) string { return r }, wantOK: true},
		{name: "unknown-client", region: mrPeer, clientID: func(string) string { return "nosuchclient" }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newRegionHandler()
			poolID := createPool(t, h, tc.region, "p")

			client, _ := idpCall(t, h, "", "CreateUserPoolClient",
				`{"UserPoolId":"`+poolID+`","ClientName":"c","CallbackURLs":["https://app.example.com/cb"],`+
					`"AllowedOAuthFlows":["code"],"AllowedOAuthFlowsUserPoolClient":true,`+
					`"AllowedOAuthScopes":["openid"],"SupportedIdentityProviders":["COGNITO"]}`,
			)["UserPoolClient"].(map[string]any)
			clientID, _ := client["ClientId"].(string)

			target := "/oauth2/authorize?response_type=code&redirect_uri=https%3A%2F%2Fapp.example.com%2Fcb" +
				"&scope=openid&client_id=" + tc.clientID(clientID)
			rec := serve(t, h, httptest.NewRequest(http.MethodGet, target, nil), mrHome)

			if tc.wantOK {
				assert.Equal(t, http.StatusFound, rec.Code, rec.Body.String())
				assert.Contains(t, rec.Header().Get("Location"), clientID)

				return
			}

			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}
