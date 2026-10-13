package apigateway_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	apigwsdk "github.com/aws/aws-sdk-go-v2/service/apigateway"
	apigwtypes "github.com/aws/aws-sdk-go-v2/service/apigateway/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigateway"
)

const (
	allowAll = `{"principalId":"p","policyDocument":{"Statement":[` +
		`{"Action":"execute-api:Invoke","Effect":"Allow","Resource":"*"}]}}`
	denyAll = `{"principalId":"p","policyDocument":{"Statement":[` +
		`{"Action":"execute-api:Invoke","Effect":"Deny","Resource":"*"}]}}`
)

func TestTestInvokeAuthorizer_Decisions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		headers    map[string]string
		name       string
		lambdaResp string
		wantStatus int32
		wantPolicy bool
	}{
		{name: "allow", lambdaResp: allowAll, headers: map[string]string{"Authorization": "t"}, wantPolicy: true},
		{
			name: "deny", lambdaResp: denyAll, headers: map[string]string{"Authorization": "t"},
			wantStatus: 403, wantPolicy: true,
		},
		{name: "missing_token", lambdaResp: allowAll, wantStatus: 401},
		{
			name:       "unparseable",
			lambdaResp: `not json`,
			headers:    map[string]string{"Authorization": "t"},
			wantStatus: 401,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := apigateway.NewHandler(apigateway.NewInMemoryBackend())
			h.SetLambdaInvoker(&captureAuthInvoker{response: []byte(tt.lambdaResp)})
			client := newTestAPIGatewayClient(t, h)

			api, err := client.CreateRestApi(t.Context(), &apigwsdk.CreateRestApiInput{Name: aws.String("a")})
			require.NoError(t, err)

			authz, err := client.CreateAuthorizer(t.Context(), &apigwsdk.CreateAuthorizerInput{
				RestApiId: api.Id, Name: aws.String("authz"), Type: apigwtypes.AuthorizerTypeToken,
				AuthorizerUri: aws.String("arn:aws:lambda:us-east-1:1:function:authFn"),
			})
			require.NoError(t, err)

			out, err := client.TestInvokeAuthorizer(t.Context(), &apigwsdk.TestInvokeAuthorizerInput{
				RestApiId: api.Id, AuthorizerId: authz.Id, Headers: tt.headers,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, out.ClientStatus)
			assert.Equal(t, tt.wantPolicy, aws.ToString(out.Policy) != "")
			assert.NotEmpty(t, aws.ToString(out.Log))
		})
	}
}

func TestTestInvokeAuthorizer_NoLambda(t *testing.T) {
	t.Parallel()

	client := newTestAPIGatewayClient(t, apigateway.NewHandler(apigateway.NewInMemoryBackend()))

	api, err := client.CreateRestApi(t.Context(), &apigwsdk.CreateRestApiInput{Name: aws.String("a")})
	require.NoError(t, err)

	authz, err := client.CreateAuthorizer(t.Context(), &apigwsdk.CreateAuthorizerInput{
		RestApiId: api.Id, Name: aws.String("authz"), Type: apigwtypes.AuthorizerTypeToken,
	})
	require.NoError(t, err)

	_, err = client.TestInvokeAuthorizer(t.Context(), &apigwsdk.TestInvokeAuthorizerInput{
		RestApiId: api.Id, AuthorizerId: authz.Id, Headers: map[string]string{"Authorization": "t"},
	})
	require.Error(t, err, "a Lambda authorizer cannot be test-invoked without a Lambda backend")
}

func TestTestInvokeMethod_ClientCertificate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		useReal bool
		wantErr bool
	}{
		{name: "known", useReal: true},
		{name: "unknown", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, apiID, rootID := setupSDKMethod(t, nil)

			certID := "nope"

			if tt.useReal {
				cert, err := client.GenerateClientCertificate(t.Context(), &apigwsdk.GenerateClientCertificateInput{})
				require.NoError(t, err)

				certID = aws.ToString(cert.ClientCertificateId)
			}

			_, err := client.TestInvokeMethod(t.Context(), &apigwsdk.TestInvokeMethodInput{
				RestApiId: aws.String(apiID), ResourceId: aws.String(rootID), HttpMethod: aws.String("GET"),
				ClientCertificateId: aws.String(certID),
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			assert.NoError(t, err)
		})
	}
}

type payloadInvoker struct {
	fn      string
	payload []byte
}

func (p *payloadInvoker) InvokeFunction(_ context.Context, fn, _ string, payload []byte) ([]byte, int, error) {
	p.fn, p.payload = fn, payload

	return []byte(`{"statusCode":200,"body":"ok"}`), 200, nil
}

func TestTestInvokeMethod_StageVariablesAndPathParameters(t *testing.T) {
	t.Parallel()

	h := apigateway.NewHandler(apigateway.NewInMemoryBackend())
	inv := &payloadInvoker{}
	h.SetLambdaInvoker(inv)
	client := newTestAPIGatewayClient(t, h)
	ctx := t.Context()

	api, err := client.CreateRestApi(ctx, &apigwsdk.CreateRestApiInput{Name: aws.String("a")})
	require.NoError(t, err)

	roots, err := client.GetResources(ctx, &apigwsdk.GetResourcesInput{RestApiId: api.Id})
	require.NoError(t, err)

	res, err := client.CreateResource(ctx, &apigwsdk.CreateResourceInput{
		RestApiId: api.Id, ParentId: roots.Items[0].Id, PathPart: aws.String("{id}"),
	})
	require.NoError(t, err)

	_, err = client.PutMethod(ctx, &apigwsdk.PutMethodInput{
		RestApiId: api.Id, ResourceId: res.Id, HttpMethod: aws.String("GET"), AuthorizationType: aws.String("NONE"),
	})
	require.NoError(t, err)

	_, err = client.PutIntegration(ctx, &apigwsdk.PutIntegrationInput{
		RestApiId: api.Id, ResourceId: res.Id, HttpMethod: aws.String("GET"), Type: apigwtypes.IntegrationTypeAwsProxy,
		IntegrationHttpMethod: aws.String("POST"),
		Uri: aws.String("arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/" +
			"arn:aws:lambda:us-east-1:1:function:${stageVariables.fn}/invocations"),
	})
	require.NoError(t, err)

	_, err = client.TestInvokeMethod(ctx, &apigwsdk.TestInvokeMethodInput{
		RestApiId: api.Id, ResourceId: res.Id, HttpMethod: aws.String("GET"),
		PathWithQueryString: aws.String("/42"), StageVariables: map[string]string{"fn": "real-fn"},
	})
	require.NoError(t, err)

	assert.Equal(t, "real-fn", inv.fn, "stage variable substituted into the integration URI")

	var event map[string]any
	require.NoError(t, json.Unmarshal(inv.payload, &event))
	assert.Equal(t, map[string]any{"id": "42"}, event["pathParameters"])
	assert.Equal(t, map[string]any{"fn": "real-fn"}, event["stageVariables"])
}
