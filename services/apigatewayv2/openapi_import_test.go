package apigatewayv2_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
)

const openAPIAuthSpec = `{
  "openapi": "3.0.1",
  "info": {"title": "Pets", "description": "pet store", "version": "7"},
  "x-amazon-apigateway-cors": {"allowOrigins": ["https://a.example"], "maxAge": 60},
  "tags": [{"name": "team", "x-amazon-apigateway-tag-value": "core"}],
  "servers": [{"x-amazon-apigateway-endpoint-configuration": {"disableExecuteApiEndpoint": true}}],
  "components": {"securitySchemes": {
    "Jwt": {"type": "oauth2", "x-amazon-apigateway-authorizer": {"type": "jwt",
      "identitySource": "$request.header.Authorization",
      "jwtConfiguration": {"issuer": "https://iss.example/", "audience": ["aud"]}}},
    "Req": {"type": "apiKey", "name": "Unused", "in": "header", "x-amazon-apigateway-authorizer": {
      "type": "request",
      "authorizerUri": "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn/invocations",
      "authorizerPayloadFormatVersion": 2.0, "enableSimpleResponses": true, "authorizerResultTtlInSeconds": 30,
      "identitySource": ["$request.header.X-Token", "$request.querystring.q"]}},
    "Iam": {"type": "apiKey", "name": "Authorization", "in": "header",
      "x-amazon-apigateway-authtype": "awsSigv4"}
  }},
  "paths": {
    "/jwt": {"get": {"security": [{"Jwt": ["read", "write"]}], "x-amazon-apigateway-integration": {
      "type": "http_proxy", "httpMethod": "GET", "uri": "https://example.com/jwt",
      "payloadFormatVersion": "1.0", "timeoutInMillis": 5000}}},
    "/req": {"get": {"security": [{"Req": []}], "x-amazon-apigateway-integration": {
      "type": "http_proxy", "httpMethod": "GET", "uri": "https://example.com/req"}}},
    "/iam": {"post": {"security": [{"Iam": []}], "x-amazon-apigateway-integration": {
      "type": "http_proxy", "httpMethod": "POST", "uri": "https://example.com/iam"}}},
    "/open": {"get": {"security": [{"NONE": []}], "x-amazon-apigateway-integration": {
      "type": "http_proxy", "httpMethod": "GET", "uri": "https://example.com/open"}}},
    "$default": {"x-amazon-apigateway-any-method": {"isDefaultRoute": true, "x-amazon-apigateway-integration": {
      "type": "http_proxy", "httpMethod": "ANY", "uri": "https://example.com/"}}},
    "/any": {"x-amazon-apigateway-any-method": {"x-amazon-apigateway-integration": {
      "type": "http_proxy", "httpMethod": "ANY", "uri": "https://example.com/any"}}, "parameters": []}
  }
}`

func TestApplyOpenAPI_AuthCorsTagsAndRoutes(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()
	api, err := b.CreateAPI(t.Context(), apigatewayv2.CreateAPIInput{Name: "x", ProtocolType: "HTTP"})
	require.NoError(t, err)

	require.NoError(t, apigatewayv2.ApplyOpenAPI(b, api.APIID, openAPIAuthSpec, ""))

	got, err := b.GetAPI(api.APIID)
	require.NoError(t, err)
	assert.Equal(t, "Pets", got.Name)
	assert.Equal(t, "pet store", got.Description)
	assert.Equal(t, "7", got.Version)
	assert.True(t, got.DisableExecuteAPIEndpoint)
	assert.Equal(t, map[string]string{"team": "core"}, got.Tags)
	require.NotNil(t, got.CorsConfiguration)
	assert.Equal(t, []string{"https://a.example"}, got.CorsConfiguration.AllowOrigins)
	assert.EqualValues(t, 60, got.CorsConfiguration.MaxAge)

	auths, err := b.GetAuthorizers(api.APIID)
	require.NoError(t, err)
	byName := map[string]apigatewayv2.Authorizer{}
	for _, a := range auths {
		byName[a.Name] = a
	}
	require.Len(t, byName, 2, "the awsSigv4 scheme is not an authorizer")
	assert.Equal(t, "JWT", byName["Jwt"].AuthorizerType)
	assert.Equal(t, []string{"$request.header.Authorization"}, byName["Jwt"].IdentitySource)
	assert.Equal(t, "REQUEST", byName["Req"].AuthorizerType)
	assert.Equal(t, "2.0", byName["Req"].AuthorizerPayloadFormatVersion)
	assert.True(t, byName["Req"].EnableSimpleResponses)
	assert.EqualValues(t, 30, byName["Req"].AuthorizerResultTTLInSeconds)
	assert.Equal(t, []string{"$request.header.X-Token", "$request.querystring.q"}, byName["Req"].IdentitySource)

	routes, err := b.GetRoutes(api.APIID)
	require.NoError(t, err)
	byKey := map[string]apigatewayv2.Route{}
	for _, r := range routes {
		byKey[r.RouteKey] = r
	}
	require.Len(t, byKey, 6)

	jwt := byKey["GET /jwt"]
	assert.Equal(t, "JWT", jwt.AuthorizationType)
	assert.Equal(t, byName["Jwt"].AuthorizerID, jwt.AuthorizerID)
	assert.Equal(t, []string{"read", "write"}, jwt.AuthorizationScopes)
	assert.Equal(t, "CUSTOM", byKey["GET /req"].AuthorizationType)
	assert.Equal(t, "AWS_IAM", byKey["POST /iam"].AuthorizationType)
	assert.Equal(t, "NONE", byKey["GET /open"].AuthorizationType)
	assert.Contains(t, byKey, "$default")
	assert.Contains(t, byKey, "ANY /any")

	integ, err := b.GetIntegration(api.APIID, jwt.Target[len("integrations/"):])
	require.NoError(t, err)
	assert.Equal(t, "HTTP_PROXY", integ.IntegrationType)
	assert.Equal(t, "1.0", integ.PayloadFormatVersion)
	assert.EqualValues(t, 5000, integ.TimeoutInMillis)
}

func TestApplyOpenAPI_YAMLBody(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()
	api, err := b.CreateAPI(t.Context(), apigatewayv2.CreateAPIInput{Name: "x", ProtocolType: "HTTP"})
	require.NoError(t, err)

	spec := "openapi: 3.0.1\ninfo:\n  title: FromYAML\npaths:\n  /a:\n    get:\n      x-amazon-apigateway-integration:\n" +
		"        type: http_proxy\n        httpMethod: GET\n        uri: https://example.com/a\n"
	require.NoError(t, apigatewayv2.ApplyOpenAPI(b, api.APIID, spec, ""))

	got, err := b.GetAPI(api.APIID)
	require.NoError(t, err)
	assert.Equal(t, "FromYAML", got.Name)
	routes, err := b.GetRoutes(api.APIID)
	require.NoError(t, err)
	require.Len(t, routes, 1)
	assert.Equal(t, "GET /a", routes[0].RouteKey)
}

func TestApplyOpenAPI_InvalidDocument(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()
	api, err := b.CreateAPI(t.Context(), apigatewayv2.CreateAPIInput{Name: "x", ProtocolType: "HTTP"})
	require.NoError(t, err)

	err = apigatewayv2.ApplyOpenAPI(b, api.APIID, "{not json", "")
	require.ErrorIs(t, err, apigatewayv2.ErrOpenAPIInvalid)
}
