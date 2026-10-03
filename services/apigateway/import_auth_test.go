package apigateway_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigateway"
)

func TestImportRestAPI_Authorizers(t *testing.T) {
	t.Parallel()

	const doc = `{
  "swagger": "2.0",
  "info": {"title": "auth", "version": "1"},
  "paths": {
    "/c": {"get": {"security": [{"Cog": []}], "x-amazon-apigateway-integration": {"type": "mock"}}},
    "/t": {"x-amazon-apigateway-any-method": {"security": [{"Tok": []}],
      "x-amazon-apigateway-integration": {"type": "mock"}}},
    "/r": {"post": {"security": [{"Req": []}], "x-amazon-apigateway-integration": {"type": "mock"}}},
    "/open": {"get": {"x-amazon-apigateway-integration": {"type": "mock"}}}
  },
  "securityDefinitions": {
    "Cog": {"type": "apiKey", "name": "Authorization", "in": "header",
      "x-amazon-apigateway-authtype": "cognito_user_pools",
      "x-amazon-apigateway-authorizer": {"type": "cognito_user_pools",
        "providerARNs": ["arn:aws:cognito-idp:us-east-1:000000000000:userpool/p"]}},
    "Tok": {"type": "apiKey", "name": "X-Token", "in": "header",
      "x-amazon-apigateway-authorizer": {"type": "token", "authorizerUri": "arn:fn",
        "authorizerResultTtlInSeconds": 20}},
    "Req": {"type": "apiKey", "name": "Unused", "in": "header",
      "x-amazon-apigateway-authorizer": {"type": "request", "authorizerUri": "arn:fn",
        "identitySource": "method.request.header.Auth"}}
  }
}`

	tests := []struct {
		name     string
		path     string
		method   string
		wantType string
		wantAuth string
	}{
		{name: "cognito", path: "/c", method: "GET", wantType: "COGNITO_USER_POOLS", wantAuth: "Cog"},
		{name: "token_any_method", path: "/t", method: "ANY", wantType: "CUSTOM", wantAuth: "Tok"},
		{name: "request", path: "/r", method: "POST", wantType: "CUSTOM", wantAuth: "Req"},
		{name: "unsecured", path: "/open", method: "GET", wantType: "NONE"},
	}

	b := apigateway.NewInMemoryBackend()
	api, err := b.ImportRestAPI(apigateway.ImportRestAPIInput{Body: []byte(doc)})
	require.NoError(t, err)
	auths, err := b.GetAuthorizers(api.ID)
	require.NoError(t, err)
	byName := map[string]apigateway.Authorizer{}
	for _, a := range auths {
		byName[a.Name] = a
	}
	require.Len(t, byName, 3)
	assert.Equal(t, "COGNITO_USER_POOLS", byName["Cog"].Type)
	assert.Equal(t, "method.request.header.Authorization", byName["Cog"].IdentitySource)
	assert.Equal(t, "TOKEN", byName["Tok"].Type)
	assert.Equal(t, "method.request.header.X-Token", byName["Tok"].IdentitySource)
	assert.Equal(t, 20, byName["Tok"].AuthorizerResultTTLInSeconds)
	assert.Equal(t, "REQUEST", byName["Req"].Type)
	assert.Equal(t, "method.request.header.Auth", byName["Req"].IdentitySource)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			res := findResourceByPath(t, b, api.ID, tc.path)
			require.NotNil(t, res)
			m, mErr := b.GetMethod(api.ID, res.ID, tc.method)
			require.NoError(t, mErr)
			assert.Equal(t, tc.wantType, m.AuthorizationType)
			if tc.wantAuth == "" {
				assert.Empty(t, m.AuthorizerID)
			} else {
				assert.Equal(t, byName[tc.wantAuth].ID, m.AuthorizerID)
			}
		})
	}
}
