package appsync_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appsync"
)

func TestCreateGraphqlAPI_VisibilityGlobal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		visibility string
		wantVis    string
	}{
		{
			name:       "explicit_global",
			visibility: "GLOBAL",
			wantVis:    "GLOBAL",
		},
		{
			name:       "default_empty_becomes_global",
			visibility: "",
			wantVis:    "GLOBAL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", tt.visibility, nil, nil, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.wantVis, api.Visibility)
		})
	}
}

func TestCreateGraphqlAPI_VisibilityPrivate(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("PrivateAPI", appsync.AuthTypeAPIKey, false, "", "PRIVATE", nil, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "PRIVATE", api.Visibility)

	got, err := b.GetGraphqlAPI(api.APIID)
	require.NoError(t, err)
	assert.Equal(t, "PRIVATE", got.Visibility)
}

func TestCreateGraphqlAPI_VisibilityInvalid(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	_, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "INTERNAL", nil, nil, nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, appsync.ErrValidation)
}

func TestUpdateGraphqlAPI_VisibilityRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		visibility string
		wantVis    string
		wantErr    bool
	}{
		{
			name:       "switch_to_private",
			visibility: "PRIVATE",
			wantVis:    "PRIVATE",
		},
		{
			name:       "switch_to_global",
			visibility: "GLOBAL",
			wantVis:    "GLOBAL",
		},
		{
			name:       "empty_no_change",
			visibility: "",
			wantVis:    "GLOBAL",
		},
		{
			name:       "invalid_visibility",
			visibility: "RESTRICTED",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "GLOBAL", nil, nil, nil)
			require.NoError(t, err)

			updated, err := b.UpdateGraphqlAPI(api.APIID, "", "", nil, tt.visibility, nil, nil)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, appsync.ErrValidation)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantVis, updated.Visibility)
		})
	}
}

func TestCreateGraphqlAPI_AdditionalAuthProviders_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		providers []appsync.AdditionalAuthenticationProvider
	}{
		{
			name: "iam_provider",
			providers: []appsync.AdditionalAuthenticationProvider{
				{AuthenticationType: appsync.AuthTypeIAM},
			},
		},
		{
			name: "oidc_provider",
			providers: []appsync.AdditionalAuthenticationProvider{
				{
					AuthenticationType: appsync.AuthTypeOIDC,
					OpenIDConnectConfig: &appsync.OpenIDConnectConfig{
						Issuer:  "https://example.com",
						AuthTTL: 3600,
						IatTTL:  300,
					},
				},
			},
		},
		{
			name: "cognito_provider",
			providers: []appsync.AdditionalAuthenticationProvider{
				{
					AuthenticationType: appsync.AuthTypeCognito,
					UserPoolConfig: &appsync.CognitoUserPoolConfig{
						UserPoolID: "us-east-1_abc123",
						AWSRegion:  "us-east-1",
					},
				},
			},
		},
		{
			name: "lambda_provider",
			providers: []appsync.AdditionalAuthenticationProvider{
				{
					AuthenticationType: appsync.AuthTypeLambda,
					LambdaAuthorizerConfig: &appsync.LambdaAuthorizerConfig{
						AuthorizerURI:                "arn:aws:lambda:us-east-1:000000000000:function:auth",
						AuthorizerResultTTLInSeconds: 300,
					},
				},
			},
		},
		{
			name: "multiple_providers",
			providers: []appsync.AdditionalAuthenticationProvider{
				{AuthenticationType: appsync.AuthTypeIAM},
				{
					AuthenticationType: appsync.AuthTypeOIDC,
					OpenIDConnectConfig: &appsync.OpenIDConnectConfig{
						Issuer: "https://example.com",
					},
				},
			},
		},
		{
			name:      "no_additional_providers",
			providers: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, err := b.CreateGraphqlAPI(
				"TestAPI", appsync.AuthTypeAPIKey, false, "", "GLOBAL", tt.providers, nil, nil,
			)
			require.NoError(t, err)

			got, err := b.GetGraphqlAPI(api.APIID)
			require.NoError(t, err)
			assert.Len(t, got.AdditionalAuthenticationProviders, len(tt.providers))

			if len(tt.providers) > 0 {
				wantAuthType := tt.providers[0].AuthenticationType
				gotAuthType := got.AdditionalAuthenticationProviders[0].AuthenticationType
				assert.Equal(t, wantAuthType, gotAuthType)
			}
		})
	}
}

func TestUpdateGraphqlAPI_AdditionalAuthProviders(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		initialProvs []appsync.AdditionalAuthenticationProvider
		updateProvs  []appsync.AdditionalAuthenticationProvider
		wantCount    int
	}{
		{
			name:         "add_iam_provider",
			initialProvs: nil,
			updateProvs: []appsync.AdditionalAuthenticationProvider{
				{AuthenticationType: appsync.AuthTypeIAM},
			},
			wantCount: 1,
		},
		{
			name: "replace_providers",
			initialProvs: []appsync.AdditionalAuthenticationProvider{
				{AuthenticationType: appsync.AuthTypeIAM},
			},
			updateProvs: []appsync.AdditionalAuthenticationProvider{
				{
					AuthenticationType:  appsync.AuthTypeOIDC,
					OpenIDConnectConfig: &appsync.OpenIDConnectConfig{Issuer: "https://example.com"},
				},
				{AuthenticationType: appsync.AuthTypeIAM},
			},
			wantCount: 2,
		},
		{
			name: "clear_providers_not_called_when_nil",
			initialProvs: []appsync.AdditionalAuthenticationProvider{
				{AuthenticationType: appsync.AuthTypeIAM},
			},
			updateProvs: nil,
			wantCount:   1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", tt.initialProvs, nil, nil)
			require.NoError(t, err)

			updated, err := b.UpdateGraphqlAPI(api.APIID, "", "", nil, "", tt.updateProvs, nil)
			require.NoError(t, err)
			assert.Len(t, updated.AdditionalAuthenticationProviders, tt.wantCount)
		})
	}
}

func TestListGraphqlAPIs_VisibilityPreserved(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	_, err := b.CreateGraphqlAPI("GlobalAPI", appsync.AuthTypeAPIKey, false, "", "GLOBAL", nil, nil, nil)
	require.NoError(t, err)
	_, err = b.CreateGraphqlAPI("PrivateAPI", appsync.AuthTypeIAM, false, "", "PRIVATE", nil, nil, nil)
	require.NoError(t, err)

	apis, err := b.ListGraphqlAPIs("")
	require.NoError(t, err)
	require.Len(t, apis, 2)

	visByName := make(map[string]string)
	for _, a := range apis {
		visByName[a.Name] = a.Visibility
	}

	assert.Equal(t, "GLOBAL", visByName["GlobalAPI"])
	assert.Equal(t, "PRIVATE", visByName["PrivateAPI"])
}

func TestUpdateGraphqlAPI_PreservesAdditionalAuthProviders(t *testing.T) {
	t.Parallel()

	providers := []appsync.AdditionalAuthenticationProvider{
		{AuthenticationType: appsync.AuthTypeIAM},
		{
			AuthenticationType: appsync.AuthTypeOIDC,
			OpenIDConnectConfig: &appsync.OpenIDConnectConfig{
				Issuer: "https://example.com",
			},
		},
	}

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "GLOBAL", providers, nil, nil)
	require.NoError(t, err)
	require.Len(t, api.AdditionalAuthenticationProviders, 2)

	// Update only the name — providers should remain unchanged.
	updated, err := b.UpdateGraphqlAPI(api.APIID, "NewName", "", nil, "", nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "NewName", updated.Name)
	assert.Len(t, updated.AdditionalAuthenticationProviders, 2)
	assert.Equal(t, appsync.AuthTypeIAM, updated.AdditionalAuthenticationProviders[0].AuthenticationType)
}

func TestCreateGraphqlAPI_CognitoUserPoolConfig_RoundTrip(t *testing.T) {
	t.Parallel()

	providers := []appsync.AdditionalAuthenticationProvider{
		{
			AuthenticationType: appsync.AuthTypeCognito,
			UserPoolConfig: &appsync.CognitoUserPoolConfig{
				UserPoolID:  "us-east-1_TestPool123",
				AWSRegion:   "us-east-1",
				AppIDClient: "my-app-client-regex",
			},
		},
	}

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", providers, nil, nil)
	require.NoError(t, err)

	got, err := b.GetGraphqlAPI(api.APIID)
	require.NoError(t, err)
	require.Len(t, got.AdditionalAuthenticationProviders, 1)
	require.NotNil(t, got.AdditionalAuthenticationProviders[0].UserPoolConfig)
	assert.Equal(t, "us-east-1_TestPool123", got.AdditionalAuthenticationProviders[0].UserPoolConfig.UserPoolID)
	assert.Equal(t, "my-app-client-regex", got.AdditionalAuthenticationProviders[0].UserPoolConfig.AppIDClient)
}

func TestCreateGraphqlAPI_LambdaAuthorizerConfig_RoundTrip(t *testing.T) {
	t.Parallel()

	authURI := "arn:aws:lambda:us-east-1:000000000000:function:my-authorizer"
	providers := []appsync.AdditionalAuthenticationProvider{
		{
			AuthenticationType: appsync.AuthTypeLambda,
			LambdaAuthorizerConfig: &appsync.LambdaAuthorizerConfig{
				AuthorizerURI:                authURI,
				AuthorizerResultTTLInSeconds: 300,
				IdentityValidationExpression: "^Bearer [-0-9a-zA-z\\.]*$",
			},
		},
	}

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", providers, nil, nil)
	require.NoError(t, err)

	got, err := b.GetGraphqlAPI(api.APIID)
	require.NoError(t, err)
	require.Len(t, got.AdditionalAuthenticationProviders, 1)
	require.NotNil(t, got.AdditionalAuthenticationProviders[0].LambdaAuthorizerConfig)
	gotLambdaCfg := got.AdditionalAuthenticationProviders[0].LambdaAuthorizerConfig
	assert.Equal(t, authURI, gotLambdaCfg.AuthorizerURI)
	assert.Equal(t, int32(300), gotLambdaCfg.AuthorizerResultTTLInSeconds)
}

func TestCreateGraphqlAPI_OIDCConfig_RoundTrip(t *testing.T) {
	t.Parallel()

	providers := []appsync.AdditionalAuthenticationProvider{
		{
			AuthenticationType: appsync.AuthTypeOIDC,
			OpenIDConnectConfig: &appsync.OpenIDConnectConfig{
				Issuer:   "https://idp.example.com",
				ClientID: "my-client",
				IatTTL:   600,
				AuthTTL:  3600,
			},
		},
	}

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", providers, nil, nil)
	require.NoError(t, err)

	got, err := b.GetGraphqlAPI(api.APIID)
	require.NoError(t, err)
	require.Len(t, got.AdditionalAuthenticationProviders, 1)
	require.NotNil(t, got.AdditionalAuthenticationProviders[0].OpenIDConnectConfig)
	oidc := got.AdditionalAuthenticationProviders[0].OpenIDConnectConfig
	assert.Equal(t, "https://idp.example.com", oidc.Issuer)
	assert.Equal(t, "my-client", oidc.ClientID)
	assert.Equal(t, int64(600), oidc.IatTTL)
	assert.Equal(t, int64(3600), oidc.AuthTTL)
}

func TestGetPutGraphqlAPIEnvironmentVariables_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	envVars := map[string]string{
		"DB_HOST":   "localhost",
		"LOG_LEVEL": "debug",
	}

	_, err = b.PutGraphqlAPIEnvironmentVariables(api.APIID, envVars)
	require.NoError(t, err)

	got, err := b.GetGraphqlAPIEnvironmentVariables(api.APIID)
	require.NoError(t, err)
	assert.Equal(t, envVars, got)
}

func TestCreateGraphqlAPI_UserPoolConfig_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	cfg := &appsync.GraphqlAPIConfig{
		UserPoolConfig: &appsync.UserPoolConfig{
			UserPoolID:       "us-east-1_abc123",
			AWSRegion:        "us-east-1",
			DefaultAction:    "ALLOW",
			AppIDClientRegex: "^my-client-",
		},
	}

	api, err := b.CreateGraphqlAPI("CognitoAPI", appsync.AuthTypeCognito, false, "", "", nil, nil, cfg)
	require.NoError(t, err)
	require.NotNil(t, api.UserPoolConfig)
	assert.Equal(t, "us-east-1_abc123", api.UserPoolConfig.UserPoolID)
	assert.Equal(t, "us-east-1", api.UserPoolConfig.AWSRegion)
	assert.Equal(t, "ALLOW", api.UserPoolConfig.DefaultAction)
	assert.Equal(t, "^my-client-", api.UserPoolConfig.AppIDClientRegex)

	got, err := b.GetGraphqlAPI(api.APIID)
	require.NoError(t, err)
	require.NotNil(t, got.UserPoolConfig)
	assert.Equal(t, "us-east-1_abc123", got.UserPoolConfig.UserPoolID)
	assert.Equal(t, "ALLOW", got.UserPoolConfig.DefaultAction)
}

func TestCreateGraphqlAPI_OpenIDConnectConfig_Primary_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	cfg := &appsync.GraphqlAPIConfig{
		OpenIDConnectConfig: &appsync.OpenIDConnectConfig{
			Issuer:   "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_abc",
			ClientID: "my-client-id",
			IatTTL:   3600,
			AuthTTL:  7200,
		},
	}

	api, err := b.CreateGraphqlAPI("OIDCPrimaryAPI", appsync.AuthTypeOIDC, false, "", "", nil, nil, cfg)
	require.NoError(t, err)
	require.NotNil(t, api.OpenIDConnectConfig)
	assert.Equal(t, "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_abc", api.OpenIDConnectConfig.Issuer)
	assert.Equal(t, "my-client-id", api.OpenIDConnectConfig.ClientID)
	assert.Equal(t, int64(3600), api.OpenIDConnectConfig.IatTTL)
	assert.Equal(t, int64(7200), api.OpenIDConnectConfig.AuthTTL)

	got, err := b.GetGraphqlAPI(api.APIID)
	require.NoError(t, err)
	require.NotNil(t, got.OpenIDConnectConfig)
	assert.Equal(t, "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_abc", got.OpenIDConnectConfig.Issuer)
}

func TestCreateGraphqlAPI_LambdaAuthorizerConfig_Primary_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	cfg := &appsync.GraphqlAPIConfig{
		LambdaAuthorizerConfig: &appsync.LambdaAuthorizerConfig{
			AuthorizerURI:                "arn:aws:lambda:us-east-1:123456789012:function:auth",
			IdentityValidationExpression: "^Bearer .+",
			AuthorizerResultTTLInSeconds: 300,
		},
	}

	api, err := b.CreateGraphqlAPI("LambdaAPI", appsync.AuthTypeLambda, false, "", "", nil, nil, cfg)
	require.NoError(t, err)
	require.NotNil(t, api.LambdaAuthorizerConfig)
	assert.Equal(t, "arn:aws:lambda:us-east-1:123456789012:function:auth", api.LambdaAuthorizerConfig.AuthorizerURI)
	assert.Equal(t, "^Bearer .+", api.LambdaAuthorizerConfig.IdentityValidationExpression)
	assert.Equal(t, int32(300), api.LambdaAuthorizerConfig.AuthorizerResultTTLInSeconds)

	got, err := b.GetGraphqlAPI(api.APIID)
	require.NoError(t, err)
	require.NotNil(t, got.LambdaAuthorizerConfig)
	assert.Equal(t, "arn:aws:lambda:us-east-1:123456789012:function:auth", got.LambdaAuthorizerConfig.AuthorizerURI)
}

func TestCreateGraphqlAPI_LogConfig_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	cfg := &appsync.GraphqlAPIConfig{
		LogConfig: &appsync.LogConfig{
			CloudWatchLogsRoleARN: "arn:aws:iam::123456789012:role/appsync-logs",
			FieldLogLevel:         "ERROR",
			ExcludeVerboseContent: true,
		},
	}

	api, err := b.CreateGraphqlAPI("LoggedAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, cfg)
	require.NoError(t, err)
	require.NotNil(t, api.LogConfig)
	assert.Equal(t, "arn:aws:iam::123456789012:role/appsync-logs", api.LogConfig.CloudWatchLogsRoleARN)
	assert.Equal(t, "ERROR", api.LogConfig.FieldLogLevel)
	assert.True(t, api.LogConfig.ExcludeVerboseContent)

	got, err := b.GetGraphqlAPI(api.APIID)
	require.NoError(t, err)
	require.NotNil(t, got.LogConfig)
	assert.Equal(t, "ERROR", got.LogConfig.FieldLogLevel)
}

func TestCreateGraphqlAPI_IntrospectionConfig_DefaultEnabled(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("API", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, appsync.IntrospectionConfigEnabled, api.IntrospectionConfig)
}

func TestCreateGraphqlAPI_IntrospectionConfig_Disabled(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	cfg := &appsync.GraphqlAPIConfig{IntrospectionConfig: appsync.IntrospectionConfigDisabled}

	api, err := b.CreateGraphqlAPI("API", appsync.AuthTypeAPIKey, false, "", "", nil, nil, cfg)
	require.NoError(t, err)
	assert.Equal(t, appsync.IntrospectionConfigDisabled, api.IntrospectionConfig)

	got, err := b.GetGraphqlAPI(api.APIID)
	require.NoError(t, err)
	assert.Equal(t, appsync.IntrospectionConfigDisabled, got.IntrospectionConfig)
}

func TestCreateGraphqlAPI_QueryDepthLimit_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	cfg := &appsync.GraphqlAPIConfig{QueryDepthLimit: 10}

	api, err := b.CreateGraphqlAPI("API", appsync.AuthTypeAPIKey, false, "", "", nil, nil, cfg)
	require.NoError(t, err)
	assert.Equal(t, int32(10), api.QueryDepthLimit)

	got, err := b.GetGraphqlAPI(api.APIID)
	require.NoError(t, err)
	assert.Equal(t, int32(10), got.QueryDepthLimit)
}

func TestCreateGraphqlAPI_ResolverCountLimit_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	cfg := &appsync.GraphqlAPIConfig{ResolverCountLimit: 500}

	api, err := b.CreateGraphqlAPI("API", appsync.AuthTypeAPIKey, false, "", "", nil, nil, cfg)
	require.NoError(t, err)
	assert.Equal(t, int32(500), api.ResolverCountLimit)
}

func TestUpdateGraphqlAPI_UserPoolConfig_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("CognitoAPI", appsync.AuthTypeCognito, false, "", "", nil, nil, nil)
	require.NoError(t, err)
	assert.Nil(t, api.UserPoolConfig)

	cfg := &appsync.GraphqlAPIConfig{
		UserPoolConfig: &appsync.UserPoolConfig{
			UserPoolID:    "us-west-2_xyz",
			AWSRegion:     "us-west-2",
			DefaultAction: "DENY",
		},
	}

	updated, err := b.UpdateGraphqlAPI(api.APIID, "", "", nil, "", nil, cfg)
	require.NoError(t, err)
	require.NotNil(t, updated.UserPoolConfig)
	assert.Equal(t, "us-west-2_xyz", updated.UserPoolConfig.UserPoolID)
	assert.Equal(t, "DENY", updated.UserPoolConfig.DefaultAction)
}

func TestUpdateGraphqlAPI_LogConfig_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("API", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	cfg := &appsync.GraphqlAPIConfig{
		LogConfig: &appsync.LogConfig{
			CloudWatchLogsRoleARN: "arn:aws:iam::123:role/logs",
			FieldLogLevel:         "ALL",
		},
	}

	updated, err := b.UpdateGraphqlAPI(api.APIID, "", "", nil, "", nil, cfg)
	require.NoError(t, err)
	require.NotNil(t, updated.LogConfig)
	assert.Equal(t, "ALL", updated.LogConfig.FieldLogLevel)
}

// TestListGraphqlAPIs_Pagination verifies maxResults/nextToken on ListGraphqlApis.
func TestListGraphqlAPIs_Pagination(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

	for i := range 5 {
		rec := doRequest(t, h, http.MethodPost, "/v1/apis", map[string]any{
			"name":               fmt.Sprintf("api-%d", i),
			"authenticationType": "API_KEY",
		})
		require.Equal(t, http.StatusCreated, rec.Code)
	}

	tests := []struct {
		name          string
		query         string
		wantLen       int
		wantNextToken bool
	}{
		{
			name:          "no_limit_returns_all",
			query:         "/v1/apis",
			wantLen:       5,
			wantNextToken: false,
		},
		{
			name:          "page1_two_items",
			query:         "/v1/apis?maxResults=2",
			wantLen:       2,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := doRequest(t, h, http.MethodGet, tt.query, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				NextToken   string           `json:"nextToken"`
				GraphqlAPIs []map[string]any `json:"graphqlApis"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Len(t, out.GraphqlAPIs, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, out.NextToken)
			} else {
				assert.Empty(t, out.NextToken)
			}
		})
	}
}

// TestListGraphqlAPIs_FullPagination walks all pages and collects all apis.
func TestListGraphqlAPIs_FullPagination(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

	const total = 5
	names := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	for i := range total {
		rec := doRequest(t, h, http.MethodPost, "/v1/apis", map[string]any{
			"name":               names[i],
			"authenticationType": "API_KEY",
		})
		require.Equal(t, http.StatusCreated, rec.Code)
	}

	seen := map[string]bool{}
	token := ""
	pages := 0

	for {
		path := "/v1/apis?maxResults=2"
		if token != "" {
			path += "&nextToken=" + token
		}

		rec := doRequest(t, h, http.MethodGet, path, nil)
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			NextToken   string           `json:"nextToken"`
			GraphqlAPIs []map[string]any `json:"graphqlApis"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		assert.LessOrEqual(t, len(out.GraphqlAPIs), 2)

		for _, api := range out.GraphqlAPIs {
			name := api["name"].(string)
			assert.False(t, seen[name], "api %s seen twice", name)
			seen[name] = true
		}

		pages++
		require.Less(t, pages, 10)

		token = out.NextToken
		if token == "" {
			break
		}
	}

	assert.Len(t, seen, total)
	assert.GreaterOrEqual(t, pages, 3)
}
