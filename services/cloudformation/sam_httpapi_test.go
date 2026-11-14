package cloudformation_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	apigatewayv2backend "github.com/blackbirdworks/gopherstack/services/apigatewayv2"
)

const samHTTPAPIAuthDomainTemplate = samHeader + `
Resources:
  AuthFn:
    Type: AWS::Serverless::Function
    Properties:
` + samFnHeader + `
  Hello:
    Type: AWS::Serverless::Function
    Properties:
` + samFnHeader + `
      Events:
        Secure:
          Type: HttpApi
          Properties:
            ApiId: !Ref Api
            Path: /secure
            Method: GET
            Auth:
              Authorizer: Jwt
              AuthorizationScopes: [read]
        Custom:
          Type: HttpApi
          Properties:
            ApiId: !Ref Api
            Path: /custom
            Method: GET
            Auth:
              Authorizer: LambdaAuth
        Open:
          Type: HttpApi
          Properties:
            ApiId: !Ref Api
            Path: /open
            Method: GET
            Auth:
              Authorizer: NONE
  Api:
    Type: AWS::Serverless::HttpApi
    Properties:
      StageName: live
      Auth:
        DefaultAuthorizer: Jwt
        Authorizers:
          Jwt:
            AuthorizationScopes: [openid]
            JwtConfiguration:
              issuer: https://issuer.example.com/
              audience: [my-audience]
            IdentitySource: $request.header.Authorization
          LambdaAuth:
            FunctionArn: !GetAtt AuthFn.Arn
            AuthorizerPayloadFormatVersion: '2.0'
            EnableFunctionDefaultPermissions: true
            Identity:
              Headers: [X-Token]
              ReauthorizeEvery: 30
      Domain:
        DomainName: api.example.com
        CertificateArn: arn:aws:acm:us-east-1:000000000000:certificate/abc
        BasePath: [v1, /v2/]
        Route53:
          HostedZoneId: !Ref Zone
  Zone:
    Type: AWS::Route53::HostedZone
    Properties:
      Name: example.com
`

func TestSAMTransform_HttpApiAuthAndDomain(t *testing.T) {
	t.Parallel()

	env := newSAMEventEnv(t)
	deploySAM(t, env.client, samHTTPAPIAuthDomainTemplate)

	wantIDs := map[string]string{
		"Api":                               "AWS::ApiGatewayV2::Api",
		"ApiliveStage":                      "AWS::ApiGatewayV2::Stage",
		"ApiLambdaAuthAuthorizerPermission": "AWS::Lambda::Permission",
		"HelloSecurePermission":             "AWS::Lambda::Permission",
		"HelloCustomPermission":             "AWS::Lambda::Permission",
		"HelloOpenPermission":               "AWS::Lambda::Permission",
		"HelloRole":                         "AWS::IAM::Role",
		"ApiGatewayDomainNameV21bc412bb08":  "AWS::ApiGatewayV2::DomainName",
		"Apiv1ApiMapping":                   "AWS::ApiGatewayV2::ApiMapping",
		"Apiv2ApiMapping":                   "AWS::ApiGatewayV2::ApiMapping",
		"RecordSetGroup563e605946":          "AWS::Route53::RecordSetGroup",
	}
	ids := logicalIDs(t, env.client)
	for id, typ := range wantIDs {
		assert.Equal(t, typ, ids[id], id)
	}
	for id := range ids {
		assert.NotContains(
			t,
			id,
			"Integration",
			"SAM HttpApi expands to an OpenAPI body, not Integration/Route resources",
		)
	}

	bk, ok := env.backends.APIGatewayV2.Backend.(*apigatewayv2backend.InMemoryBackend)
	require.True(t, ok)
	apiID := env.physicalID(t, "Api")

	auths, err := bk.GetAuthorizers(apiID)
	require.NoError(t, err)
	byName := map[string]apigatewayv2backend.Authorizer{}
	for _, a := range auths {
		byName[a.Name] = a
	}
	require.Len(t, byName, 2)
	assert.Equal(t, "JWT", byName["Jwt"].AuthorizerType)
	require.NotNil(t, byName["Jwt"].JwtConfiguration)
	assert.Equal(t, "https://issuer.example.com/", byName["Jwt"].JwtConfiguration.Issuer)
	assert.Equal(t, []string{"my-audience"}, byName["Jwt"].JwtConfiguration.Audience)
	assert.Equal(t, "REQUEST", byName["LambdaAuth"].AuthorizerType)
	assert.Equal(t, []string{"$request.header.X-Token"}, byName["LambdaAuth"].IdentitySource)
	assert.EqualValues(t, 30, byName["LambdaAuth"].AuthorizerResultTTLInSeconds)
	assert.Equal(t, "2.0", byName["LambdaAuth"].AuthorizerPayloadFormatVersion)

	routes, err := bk.GetRoutes(apiID)
	require.NoError(t, err)
	byKey := map[string]apigatewayv2backend.Route{}
	for _, r := range routes {
		byKey[r.RouteKey] = r
	}
	require.Len(t, byKey, 3)
	assert.Equal(t, "JWT", byKey["GET /secure"].AuthorizationType)
	assert.Equal(t, byName["Jwt"].AuthorizerID, byKey["GET /secure"].AuthorizerID)
	assert.Equal(t, []string{"read"}, byKey["GET /secure"].AuthorizationScopes)
	assert.Equal(t, "CUSTOM", byKey["GET /custom"].AuthorizationType)
	assert.Equal(t, byName["LambdaAuth"].AuthorizerID, byKey["GET /custom"].AuthorizerID)
	assert.Equal(t, "NONE", byKey["GET /open"].AuthorizationType)

	domain, err := bk.GetDomainName("api.example.com")
	require.NoError(t, err)
	assert.Equal(t, "api.example.com", domain.DomainNameValue)
	mappings, err := bk.GetAPIMappings("api.example.com")
	require.NoError(t, err)
	keys := map[string]string{}
	for _, m := range mappings {
		keys[m.APIMappingKey] = m.Stage
	}
	assert.Equal(t, map[string]string{"v1": "live", "v2": "live"}, keys)

	// The JWT default authorizer rejects an unauthenticated call before reaching Lambda.
	rr := env.proxy(t, http.MethodGet, "/v2proxy/"+apiID+"/live/secure", "")
	assert.Equal(t, http.StatusUnauthorized, rr.Code)
	open := env.proxy(t, http.MethodGet, "/v2proxy/"+apiID+"/live/open", "")
	assert.Equal(t, http.StatusOK, open.Code)

	assertDeleteClean(t, env.client)
}
