package appsync_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appsync"
)

const (
	identityVTL = `{"version":"2018-05-29","payload":{"identity":$util.toJson($ctx.identity),` +
		`"authType":$util.toJson($util.authType()),"domainName":$util.toJson($ctx.request.domainName)}}`
	identityJS = `import { util } from '@aws-appsync/utils';
export function request(ctx) {
  return { payload: { identity: ctx.identity, authType: util.authType(), domainName: ctx.request.domainName } };
}
export function response(ctx) { return ctx.result; }`
	identityLambdaARN = "arn:aws:lambda:us-east-1:000000000000:function:authorizer"
)

type identityCase struct {
	setup    func(t *testing.T, b *appsync.InMemoryBackend) (*appsync.GraphqlAPIConfig, appsync.GraphQLAuth)
	check    func(t *testing.T, who map[string]any)
	authType appsync.AuthenticationType
	name     string
}

func identityCases() []identityCase {
	return []identityCase{
		{
			name: "api_key", authType: appsync.AuthTypeAPIKey,
			setup: func(*testing.T, *appsync.InMemoryBackend) (*appsync.GraphqlAPIConfig, appsync.GraphQLAuth) {
				return nil, appsync.GraphQLAuth{APIKey: "placeholder"}
			},
			check: func(t *testing.T, who map[string]any) {
				t.Helper()
				assert.Nil(t, who["identity"])
				assert.Equal(t, "API Key Authorization", who["authType"])
				assert.Nil(t, who["domainName"])
			},
		},
		{
			name: "iam", authType: appsync.AuthTypeIAM,
			setup: func(t *testing.T, _ *appsync.InMemoryBackend) (*appsync.GraphqlAPIConfig, appsync.GraphQLAuth) {
				t.Helper()

				return nil, appsync.GraphQLAuth{Request: signedIAMRequest(t, "test")}
			},
			check: func(t *testing.T, who map[string]any) {
				t.Helper()
				id, _ := who["identity"].(map[string]any)
				assert.Equal(t, "IAM Authorization", who["authType"])
				assert.Equal(t, "000000000000", id["accountId"])
				assert.Equal(t, "AKIDEXAMPLE", id["username"])
				assert.Contains(t, id["userArn"], ":user/AKIDEXAMPLE")
				assert.Equal(t, []any{"192.0.2.1"}, id["sourceIp"])
				assert.Contains(t, id, "cognitoIdentityPoolId")
				assert.Contains(t, id, "cognitoIdentityId")
				assert.Contains(t, id, "cognitoIdentityAuthType")
				assert.Contains(t, id, "cognitoIdentityAuthProvider")
			},
		},
		{
			name: "cognito", authType: appsync.AuthTypeCognito,
			setup: func(t *testing.T, b *appsync.InMemoryBackend) (*appsync.GraphqlAPIConfig, appsync.GraphQLAuth) {
				t.Helper()
				fx := cognitoTestFixture(t)
				b.SetJWKSProvider(fx.Backend)

				cfg := &appsync.GraphqlAPIConfig{UserPoolConfig: &appsync.UserPoolConfig{
					UserPoolID: fx.PoolID, AWSRegion: "us-east-1", DefaultAction: "ALLOW",
					AppIDClientRegex: fx.ClientID,
				}}
				auth := appsync.GraphQLAuth{AuthToken: "Bearer " + fx.IDToken, Request: signedIAMRequest(t, "x")}

				return cfg, auth
			},
			check: func(t *testing.T, who map[string]any) {
				t.Helper()
				id, _ := who["identity"].(map[string]any)
				claims, _ := id["claims"].(map[string]any)
				assert.Equal(t, "User Pool Authorization", who["authType"])
				assert.NotEmpty(t, id["sub"])
				assert.Equal(t, id["sub"], claims["sub"])
				assert.Contains(t, id["issuer"], "http://localhost:8000/")
				assert.Equal(t, "ALLOW", id["defaultAuthStrategy"])
				assert.Equal(t, []any{"192.0.2.1"}, id["sourceIp"])
				assert.Contains(t, id, "groups")
				assert.Contains(t, id, "username")
			},
		},
		{
			name: "oidc", authType: appsync.AuthTypeOIDC,
			setup: func(t *testing.T, b *appsync.InMemoryBackend) (*appsync.GraphqlAPIConfig, appsync.GraphQLAuth) {
				t.Helper()
				fx := cognitoTestFixture(t)
				b.SetJWKSProvider(fx.Backend)

				return &appsync.GraphqlAPIConfig{OpenIDConnectConfig: &appsync.OpenIDConnectConfig{
					Issuer: fx.IssuerURL, ClientID: fx.ClientID,
				}}, appsync.GraphQLAuth{AuthToken: "Bearer " + fx.IDToken}
			},
			check: func(t *testing.T, who map[string]any) {
				t.Helper()
				id, _ := who["identity"].(map[string]any)
				claims, _ := id["claims"].(map[string]any)
				assert.Equal(t, "Open ID Connect Authorization", who["authType"])
				assert.NotEmpty(t, id["sub"])
				assert.Equal(t, id["issuer"], claims["iss"])
				assert.NotContains(t, id, "groups")
			},
		},
		{
			name: "lambda", authType: appsync.AuthTypeLambda,
			setup: func(_ *testing.T, b *appsync.InMemoryBackend) (*appsync.GraphqlAPIConfig, appsync.GraphQLAuth) {
				b.SetLambdaInvoker(&mockLambdaInvoker{
					payload: []byte(`{"isAuthorized":true,"resolverContext":{"tenant":"acme","n":3}}`),
				})

				return &appsync.GraphqlAPIConfig{LambdaAuthorizerConfig: &appsync.LambdaAuthorizerConfig{
					AuthorizerURI: identityLambdaARN,
				}}, appsync.GraphQLAuth{AuthToken: "tok"}
			},
			check: func(t *testing.T, who map[string]any) {
				t.Helper()
				id, _ := who["identity"].(map[string]any)
				assert.Equal(t, "Lambda Authorization", who["authType"])
				assert.Equal(t, map[string]any{"tenant": "acme", "n": float64(3)}, id["resolverContext"])
			},
		},
	}
}

func TestExecuteGraphQL_Identity(t *testing.T) {
	t.Parallel()

	for _, tt := range identityCases() {
		for _, js := range []bool{false, true} {
			name := tt.name + "_vtl"
			if js {
				name = tt.name + "_js"
			}

			t.Run(name, func(t *testing.T) {
				t.Parallel()

				b := newTestBackend()
				cfg, auth := tt.setup(t, b)

				api, err := b.CreateGraphqlAPI("IdAPI", tt.authType, false, "", "", nil, nil, cfg)
				require.NoError(t, err)

				if tt.authType == appsync.AuthTypeAPIKey {
					key, kerr := b.CreateAPIKey(api.APIID, "", 0)
					require.NoError(t, kerr)

					auth.APIKey = key.ID
				}

				_, err = b.StartSchemaCreation(api.APIID, `type Query { who: AWSJSON }`)
				require.NoError(t, err)
				none := &appsync.DataSource{Name: "NoneDS", Type: appsync.DataSourceTypeNone}
				_, err = b.CreateDataSource(api.APIID, none)
				require.NoError(t, err)

				res := &appsync.Resolver{FieldName: "who", DataSourceName: "NoneDS"}
				if js {
					res.Code = identityJS
					res.Runtime = &appsync.Runtime{Name: "APPSYNC_JS", RuntimeVersion: "1.0.0"}
				} else {
					res.RequestMappingTemplate = identityVTL
				}

				_, err = b.CreateResolver(api.APIID, "Query", res)
				require.NoError(t, err)

				out, err := b.ExecuteGraphQL(t.Context(), api.APIID, `query { who }`, "", nil, auth)
				require.NoError(t, err)

				tt.check(t, whoMap(t, out["who"]))
			})
		}
	}
}

func whoMap(t *testing.T, v any) map[string]any {
	t.Helper()

	if m, ok := v.(map[string]any); ok {
		return m
	}

	s, ok := v.(string)
	require.True(t, ok, "unexpected who: %#v", v)

	var m map[string]any
	require.NoError(t, json.Unmarshal([]byte(s), &m))

	return m
}

func TestExecuteGraphQL_RequestDomainName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want       any
		name, host string
	}{
		{name: "custom_domain", host: "api.example.com:8443", want: "api.example.com"},
		{name: "default_endpoint", host: "localhost:8000", want: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			_, err := b.CreateDomainName("api.example.com", "arn:aws:acm:us-east-1:000000000000:certificate/c", "", nil)
			require.NoError(t, err)

			api, err := b.CreateGraphqlAPI("D", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
			require.NoError(t, err)
			key, err := b.CreateAPIKey(api.APIID, "", 0)
			require.NoError(t, err)
			_, err = b.StartSchemaCreation(api.APIID, `type Query { who: AWSJSON }`)
			require.NoError(t, err)
			_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{Name: "N", Type: appsync.DataSourceTypeNone})
			require.NoError(t, err)
			_, err = b.CreateResolver(api.APIID, "Query", &appsync.Resolver{
				FieldName: "who", DataSourceName: "N", RequestMappingTemplate: identityVTL,
			})
			require.NoError(t, err)

			req := httptest.NewRequest(http.MethodPost, "/graphql", nil)
			req.Host = tt.host

			out, err := b.ExecuteGraphQL(t.Context(), api.APIID, `query { who }`, "", nil,
				appsync.GraphQLAuth{APIKey: key.ID, Request: req})
			require.NoError(t, err)
			assert.Equal(t, tt.want, whoMap(t, out["who"])["domainName"])
		})
	}
}
