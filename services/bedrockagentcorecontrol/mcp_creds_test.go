package bedrockagentcorecontrol_test

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/bedrockagentcorecontrol"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagentcorecontrol/types"
	"github.com/golang-jwt/jwt/v5"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

func openAPISpecTarget(server string) types.TargetConfiguration {
	return &types.TargetConfigurationMemberMcp{Value: &types.McpTargetConfigurationMemberOpenApiSchema{
		Value: &types.ApiSchemaConfigurationMemberInlinePayload{
			Value: `{"openapi":"3.0.1","servers":[{"url":"` + server + `"}],"paths":{"/ping":{"get":{"operationId":"ping"}}}}`,
		},
	}}
}

func TestGatewayMCPOutboundCredentials(t *testing.T) {
	t.Parallel()

	type seenAuth struct{ header, query, bearer string }

	var (
		mu   sync.Mutex
		seen = map[string]seenAuth{}
	)

	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen[r.URL.Path] = seenAuth{r.Header.Get("X-Api-Key"), r.URL.Query().Get("key"), r.Header.Get("Authorization")}
		mu.Unlock()
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(api.Close)

	idp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		assert.Contains(t, string(raw), "grant_type=client_credentials")
		assert.Contains(t, string(raw), "client_secret=csecret")
		assert.Contains(t, string(raw), "scope=a+b")
		_, _ = w.Write([]byte(`{"access_token":"tok-123","token_type":"Bearer"}`))
	}))
	t.Cleanup(idp.Close)

	m := newMCPFixture(t)
	store := &memorySecrets{
		functions: map[string]bool{lambdaARN: true},
		values:    map[string]string{},
	}
	m.backend.SetReferences(store)

	ctx := t.Context()
	apiKey, err := m.client.CreateApiKeyCredentialProvider(ctx, &sdk.CreateApiKeyCredentialProviderInput{
		Name: aws.String("vendor"), ApiKey: aws.String("sk-1"),
	})
	require.NoError(t, err)

	oauth, err := m.client.CreateOauth2CredentialProvider(ctx, &sdk.CreateOauth2CredentialProviderInput{
		Name: aws.String("idp"), CredentialProviderVendor: types.CredentialProviderVendorTypeCustomOauth2,
		Oauth2ProviderConfigInput: &types.Oauth2ProviderConfigInputMemberCustomOauth2ProviderConfig{
			Value: types.CustomOauth2ProviderConfigInput{
				ClientId: aws.String("cid"), ClientSecret: aws.String("csecret"),
				OauthDiscovery: &types.Oauth2DiscoveryMemberAuthorizationServerMetadata{
					Value: types.Oauth2AuthorizationServerMetadata{
						Issuer: aws.String(idp.URL), AuthorizationEndpoint: aws.String(idp.URL + "/authorize"),
						TokenEndpoint: aws.String(idp.URL + "/token"),
					},
				},
			},
		},
	})
	require.NoError(t, err)

	gw := createGateway(t, m.client, "Cred-Gw")
	gid := aws.ToString(gw.GatewayId)

	withCred := func(name string, cfg types.CredentialProviderConfiguration) {
		_, cerr := m.client.CreateGatewayTarget(ctx, &sdk.CreateGatewayTargetInput{
			GatewayIdentifier: aws.String(gid), Name: aws.String(name),
			TargetConfiguration:              openAPISpecTarget(api.URL + "/" + name),
			CredentialProviderConfigurations: []types.CredentialProviderConfiguration{cfg},
		})
		require.NoError(t, cerr)
	}

	withCred("hdr", types.CredentialProviderConfiguration{
		CredentialProviderType: types.CredentialProviderTypeApiKey,
		CredentialProvider: &types.CredentialProviderMemberApiKeyCredentialProvider{
			Value: types.GatewayApiKeyCredentialProvider{
				ProviderArn: apiKey.CredentialProviderArn, CredentialParameterName: aws.String("X-Api-Key"),
				CredentialLocation: types.ApiKeyCredentialLocationHeader,
			},
		},
	})
	withCred("qry", types.CredentialProviderConfiguration{
		CredentialProviderType: types.CredentialProviderTypeApiKey,
		CredentialProvider: &types.CredentialProviderMemberApiKeyCredentialProvider{
			Value: types.GatewayApiKeyCredentialProvider{
				ProviderArn: apiKey.CredentialProviderArn, CredentialParameterName: aws.String("key"),
				CredentialLocation: types.ApiKeyCredentialLocationQueryParameter,
			},
		},
	})
	withCred("oa", types.CredentialProviderConfiguration{
		CredentialProviderType: types.CredentialProviderTypeOauth,
		CredentialProvider: &types.CredentialProviderMemberOauthCredentialProvider{Value: types.OAuthCredentialProvider{
			ProviderArn: oauth.CredentialProviderArn, Scopes: []string{"a", "b"},
			GrantType: types.OAuthGrantTypeClientCredentials,
		}},
	})
	withCred("code", types.CredentialProviderConfiguration{
		CredentialProviderType: types.CredentialProviderTypeOauth,
		CredentialProvider: &types.CredentialProviderMemberOauthCredentialProvider{Value: types.OAuthCredentialProvider{
			ProviderArn: oauth.CredentialProviderArn, Scopes: []string{"a"},
			GrantType: types.OAuthGrantTypeAuthorizationCode, DefaultReturnUrl: aws.String("https://app.example/cb"),
		}},
	})
	m.clock.advance()

	cs, err := connectMCP(t, m.mcpURL(gid), m.token(t, nil))
	require.NoError(t, err)

	t.Cleanup(func() { _ = cs.Close() })

	tests := []struct {
		name    string
		tool    string
		want    seenAuth
		isError bool
	}{
		{name: "api key header", tool: "hdr___ping", want: seenAuth{header: "sk-1"}},
		{name: "api key query", tool: "qry___ping", want: seenAuth{query: "sk-1"}},
		{name: "oauth client credentials", tool: "oa___ping", want: seenAuth{bearer: "Bearer tok-123"}},
		{name: "authorization code needs consent", tool: "code___ping", isError: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			res, callErr := cs.CallTool(t.Context(), &mcp.CallToolParams{Name: tt.tool})
			require.NoError(t, callErr)
			assert.Equal(t, tt.isError, res.IsError)

			if !tt.isError {
				mu.Lock()
				got := seen["/"+strings.SplitN(tt.tool, "___", 2)[0]+"/ping"]
				mu.Unlock()

				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestGatewayMCPLambdaToolSchemaFromS3(t *testing.T) {
	t.Parallel()

	m := newMCPFixture(t)
	schema, _ := json.Marshal([]map[string]any{{
		"name": "s3tool", "description": "from s3", "inputSchema": map[string]any{"type": "object"},
	}}, jsonx.V1())

	m.backend.SetReferences(fakeInvoker{
		functions: map[string]bool{lambdaARN: true},
		objects:   map[string][]byte{"bkt/schemas/tools.json": schema},
		calls:     m.calls,
	})

	gw := createGateway(t, m.client, "S3-Gw")
	gid := aws.ToString(gw.GatewayId)

	m.createTarget(
		t,
		gid,
		"fromS3",
		&types.TargetConfigurationMemberMcp{Value: &types.McpTargetConfigurationMemberLambda{
			Value: types.McpLambdaTargetConfiguration{
				LambdaArn: aws.String(lambdaARN),
				ToolSchema: &types.ToolSchemaMemberS3{
					Value: types.S3Configuration{Uri: aws.String("s3://bkt/schemas/tools.json")},
				},
			},
		}},
	)

	cs, err := connectMCP(t, m.mcpURL(gid), m.token(t, nil))
	require.NoError(t, err)

	t.Cleanup(func() { _ = cs.Close() })

	list, err := cs.ListTools(t.Context(), nil)
	require.NoError(t, err)
	require.Len(t, list.Tools, 1)
	assert.Equal(t, "fromS3___s3tool", list.Tools[0].Name)
	assert.Equal(t, "from s3", list.Tools[0].Description)
}

func TestGatewayMCPJWTDiscoveryOverHTTP(t *testing.T) {
	t.Parallel()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	var idp *httptest.Server

	idp = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/.well-known/openid-configuration":
			_ = json.MarshalEncode(
				jsontext.NewEncoder(w, jsonx.V1()),
				map[string]string{"issuer": idp.URL, "jwks_uri": idp.URL + "/jwks"},
			)
		case "/jwks":
			_ = json.MarshalEncode(jsontext.NewEncoder(w, jsonx.V1()), map[string]any{"keys": []map[string]string{{
				"kty": "RSA", "kid": "k1", "alg": "RS256", "use": "sig",
				"n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
				"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes()),
			}}})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(idp.Close)

	m := newMCPFixture(t)
	pool := x509.NewCertPool()
	pool.AddCert(idp.Certificate())
	m.backend.SetTrustedRoots(pool)
	gw, err := m.client.CreateGateway(t.Context(), &sdk.CreateGatewayInput{
		Name: aws.String("Disc-Gw"), RoleArn: aws.String(testRole), AuthorizerType: types.AuthorizerTypeCustomJwt,
		AuthorizerConfiguration: &types.AuthorizerConfigurationMemberCustomJWTAuthorizer{
			Value: types.CustomJWTAuthorizerConfiguration{
				DiscoveryUrl:    aws.String(idp.URL + "/.well-known/openid-configuration"),
				AllowedAudience: []string{"aud"},
			},
		},
	})
	require.NoError(t, err)

	sign := func(k *rsa.PrivateKey) string {
		tok := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
			"iss": idp.URL, "aud": "aud", "exp": time.Now().Add(time.Hour).Unix(),
		})
		tok.Header["kid"] = "k1"
		s, serr := tok.SignedString(k)
		require.NoError(t, serr)

		return s
	}

	foreign, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	tests := []struct {
		token      string
		name       string
		wantStatus int
	}{
		{name: "key from jwks", token: sign(key), wantStatus: http.StatusOK},
		{name: "foreign key", token: sign(foreign), wantStatus: http.StatusUnauthorized},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			status, _ := postRPC(t, m.mcpURL(aws.ToString(gw.GatewayId)), "Bearer "+tt.token,
				`{"jsonrpc":"2.0","id":1,"method":"ping"}`)
			assert.Equal(t, tt.wantStatus, status)
		})
	}
}

func TestGatewayMCPServerCertificateVerified(t *testing.T) {
	t.Parallel()

	upstream := httptest.NewTLSServer(http.NotFoundHandler())
	t.Cleanup(upstream.Close)

	m := newMCPFixture(t)
	gw := createGateway(t, m.client, "Tls-Gw")
	gid := aws.ToString(gw.GatewayId)
	tid := m.createTarget(t, gid, "up", &types.TargetConfigurationMemberMcp{
		Value: &types.McpTargetConfigurationMemberMcpServer{
			Value: types.McpServerTargetConfiguration{Endpoint: aws.String(upstream.URL)},
		},
	})

	_, err := m.client.SynchronizeGatewayTargets(t.Context(), &sdk.SynchronizeGatewayTargetsInput{
		GatewayIdentifier: aws.String(gid), TargetIdList: []string{tid},
	})
	require.NoError(t, err)
	m.clock.advance()

	got, err := m.client.GetGatewayTarget(t.Context(),
		&sdk.GetGatewayTargetInput{GatewayIdentifier: aws.String(gid), TargetId: aws.String(tid)})
	require.NoError(t, err)
	assert.Equal(t, types.TargetStatusSynchronizeUnsuccessful, got.Status)
	require.NotEmpty(t, got.StatusReasons)
	assert.Contains(t, got.StatusReasons[0], "certificate")
}
