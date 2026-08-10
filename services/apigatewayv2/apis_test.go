package apigatewayv2_test

import (
	"context"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryBackend_CreateGetAPI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantName string
		wantProt string
		input    apigatewayv2.CreateAPIInput
	}{
		{
			name:     "http_api",
			input:    apigatewayv2.CreateAPIInput{Name: "my-http-api", ProtocolType: "HTTP"},
			wantName: "my-http-api",
			wantProt: "HTTP",
		},
		{
			name:     "websocket_api",
			input:    apigatewayv2.CreateAPIInput{Name: "my-ws-api", ProtocolType: "WEBSOCKET"},
			wantName: "my-ws-api",
			wantProt: "WEBSOCKET",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigatewayv2.NewInMemoryBackend()

			api, err := b.CreateAPI(context.Background(), tt.input)
			require.NoError(t, err)
			assert.Equal(t, tt.wantName, api.Name)
			assert.Equal(t, tt.wantProt, api.ProtocolType)
			assert.NotEmpty(t, api.APIID)
			assert.NotEmpty(t, api.APIEndpoint)

			got, err := b.GetAPI(api.APIID)
			require.NoError(t, err)
			assert.Equal(t, api.APIID, got.APIID)
			assert.Equal(t, tt.wantName, got.Name)
		})
	}
}

func TestInMemoryBackend_GetAPI_NotFound(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()

	_, err := b.GetAPI("nonexistent")
	require.Error(t, err)
	assert.ErrorIs(t, err, apigatewayv2.ErrAPINotFound)
}

func TestInMemoryBackend_GetAPIs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		apiNames  []string
		wantCount int
	}{
		{
			name:      "empty",
			apiNames:  nil,
			wantCount: 0,
		},
		{
			name:      "multiple",
			apiNames:  []string{"api-a", "api-b", "api-c"},
			wantCount: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigatewayv2.NewInMemoryBackend()

			for _, n := range tt.apiNames {
				_, err := b.CreateAPI(context.Background(), apigatewayv2.CreateAPIInput{Name: n, ProtocolType: "HTTP"})
				require.NoError(t, err)
			}

			apis, err := b.GetAPIs()
			require.NoError(t, err)
			assert.Len(t, apis, tt.wantCount)
		})
	}
}

func TestInMemoryBackend_DeleteAPI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		name      string
		apiID     string
		createAPI bool
	}{
		{
			name:      "success",
			createAPI: true,
		},
		{
			name:    "not_found",
			apiID:   "nonexistent",
			wantErr: apigatewayv2.ErrAPINotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigatewayv2.NewInMemoryBackend()

			apiID := tt.apiID
			if tt.createAPI {
				api, err := b.CreateAPI(
					context.Background(),
					apigatewayv2.CreateAPIInput{Name: "test", ProtocolType: "HTTP"},
				)
				require.NoError(t, err)
				apiID = api.APIID
			}

			err := b.DeleteAPI(apiID)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			_, err = b.GetAPI(apiID)
			require.ErrorIs(t, err, apigatewayv2.ErrAPINotFound)
		})
	}
}

func TestInMemoryBackend_UpdateAPI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		update    apigatewayv2.UpdateAPIInput
		name      string
		wantName  string
		apiExists bool
	}{
		{
			name:      "update_name",
			update:    apigatewayv2.UpdateAPIInput{Name: "new-name"},
			apiExists: true,
			wantName:  "new-name",
		},
		{
			name:    "not_found",
			update:  apigatewayv2.UpdateAPIInput{Name: "x"},
			wantErr: apigatewayv2.ErrAPINotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigatewayv2.NewInMemoryBackend()

			apiID := "nonexistent"
			if tt.apiExists {
				api, err := b.CreateAPI(
					context.Background(),
					apigatewayv2.CreateAPIInput{Name: "original", ProtocolType: "HTTP"},
				)
				require.NoError(t, err)
				apiID = api.APIID
			}

			updated, err := b.UpdateAPI(apiID, tt.update)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantName, updated.Name)
		})
	}
}

func TestInMemoryBackend_UpdateAPI_AllFields(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()

	api, err := b.CreateAPI(context.Background(), apigatewayv2.CreateAPIInput{Name: "test", ProtocolType: "HTTP"})
	require.NoError(t, err)

	updated, err := b.UpdateAPI(api.APIID, apigatewayv2.UpdateAPIInput{
		Name:                     "updated",
		Description:              "new desc",
		RouteSelectionExpression: "${request.method} ${request.path}",
		Version:                  "2",
		Tags:                     map[string]string{"env": "prod"},
	})
	require.NoError(t, err)
	assert.Equal(t, "updated", updated.Name)
	assert.Equal(t, "new desc", updated.Description)
	assert.Equal(t, "2", updated.Version)
}

func TestInMemoryBackend_ExportAPI(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()

	api, err := b.CreateAPI(context.Background(), apigatewayv2.CreateAPIInput{
		Name:         "export-test",
		ProtocolType: "HTTP",
		Description:  "Test API",
		Version:      "1.0",
	})
	require.NoError(t, err)

	_, err = b.CreateRoute(api.APIID, apigatewayv2.CreateRouteInput{
		RouteKey:      "GET /items",
		OperationName: "ListItems",
	})
	require.NoError(t, err)

	_, err = b.CreateRoute(api.APIID, apigatewayv2.CreateRouteInput{
		RouteKey:          "POST /items",
		AuthorizationType: "JWT",
		AuthorizerID:      "some-id",
	})
	require.NoError(t, err)

	spec, err := b.ExportAPI(api.APIID)
	require.NoError(t, err)
	assert.Equal(t, "3.0.1", spec["openapi"])

	info, ok := spec["info"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "export-test", info["title"])
	assert.Equal(t, "1.0", info["version"])

	paths, ok := spec["paths"].(map[string]any)
	require.True(t, ok)
	assert.Contains(t, paths, "/items")

	// ExportAPI returns error for unknown API.
	_, err = b.ExportAPI("nonexistent")
	require.ErrorIs(t, err, apigatewayv2.ErrAPINotFound)
}

func TestCreateAPIEndpointAndARNsUseCtxbagRegion(t *testing.T) {
	t.Parallel()

	ctx := awsmeta.Set(context.Background(), &awsmeta.Metadata{
		Account:   "555566667777",
		Region:    "ap-southeast-2",
		Partition: "aws",
	})

	b := apigatewayv2.NewInMemoryBackend()

	api, err := b.CreateAPI(ctx, apigatewayv2.CreateAPIInput{Name: "regional", ProtocolType: "HTTP"})
	require.NoError(t, err)
	assert.Contains(t, api.APIEndpoint, ".execute-api.ap-southeast-2.amazonaws.com")

	_, err = b.CreateDomainName(ctx, apigatewayv2.CreateDomainNameInput{
		DomainNameValue: "api.example.com",
		DomainNameConfigurations: []apigatewayv2.DomainNameConfiguration{{
			EndpointType: "REGIONAL",
		}},
	})
	require.NoError(t, err)

	rule, err := b.CreateRoutingRule(ctx, "api.example.com", apigatewayv2.CreateRoutingRuleInput{Priority: 1})
	require.NoError(t, err)

	wantARN := "arn:aws:apigateway:ap-southeast-2:555566667777:/domainnames/api.example.com/routingrules/" +
		rule.RoutingRuleID
	assert.Equal(t, wantARN, rule.RoutingRuleARN)
}

// TestRoutingRuleARNIncludesAccountID pins the RoutingRule ARN shape against
// the documented AWS format, not gopherstack's own output: RoutingRule (and
// DomainNameAccessAssociation) carry an account ID even though the DomainName
// they nest under does not.
// arn:{partition}:apigateway:{region}:{account-id}:/domainnames/{domain-name}/routingrules/{routing-rule-id}
// https://docs.aws.amazon.com/apigateway/latest/developerguide/arn-format-reference.html#apigateway-domain-name-arns
func TestRoutingRuleARNIncludesAccountID(t *testing.T) {
	t.Parallel()

	ctx := awsmeta.Set(context.Background(), &awsmeta.Metadata{
		Account:   "555566667777",
		Region:    "ap-southeast-2",
		Partition: "aws",
	})

	b := apigatewayv2.NewInMemoryBackend()

	_, err := b.CreateDomainName(ctx, apigatewayv2.CreateDomainNameInput{DomainNameValue: "api.example.com"})
	require.NoError(t, err)

	rule, err := b.CreateRoutingRule(ctx, "api.example.com", apigatewayv2.CreateRoutingRuleInput{Priority: 1})
	require.NoError(t, err)

	want := "arn:aws:apigateway:ap-southeast-2:555566667777:/domainnames/api.example.com/routingrules/" +
		rule.RoutingRuleID
	assert.Equal(t, want, rule.RoutingRuleARN)
}

// TestCreateDomainNameARNHasNoAccountID pins DomainName's own ARN, which real
// AWS documents WITHOUT an account ID (a domain name is a shared resource
// across REST and HTTP/WebSocket APIs), unlike RoutingRule underneath it.
// arn:{partition}:apigateway:{region}::/domainnames/{domain-name}
func TestCreateDomainNameARNHasNoAccountID(t *testing.T) {
	t.Parallel()

	ctx := awsmeta.Set(context.Background(), &awsmeta.Metadata{
		Account:   "555566667777",
		Region:    "ap-southeast-2",
		Partition: "aws",
	})

	b := apigatewayv2.NewInMemoryBackend()

	dn, err := b.CreateDomainName(ctx, apigatewayv2.CreateDomainNameInput{DomainNameValue: "api.example.com"})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:apigateway:ap-southeast-2::/domainnames/api.example.com", dn.DomainNameArn)
}

func TestCreateAPIEndpointFallsBackToDefaultRegion(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()

	api, err := b.CreateAPI(context.Background(), apigatewayv2.CreateAPIInput{Name: "x", ProtocolType: "HTTP"})
	require.NoError(t, err)
	assert.Contains(t, api.APIEndpoint, ".execute-api.us-east-1.amazonaws.com")
}

// Test_CreateAPI_QuickCreate covers CreateApi's routeKey+target shortcut,
// which was previously entirely unimplemented: CreateAPIInput had no
// RouteKey/Target fields, so real quick-create requests (e.g. `aws
// apigatewayv2 create-api --target ...`) silently succeeded but produced an
// API with no route, integration, or stage at all. AWS instead auto-creates
// a $default route, a matching integration (HTTP_PROXY for a URL target,
// AWS_PROXY for a Lambda ARN target), and an auto-deployed $default stage,
// all marked apiGatewayManaged.
func Test_CreateAPI_QuickCreate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                string
		target              string
		wantIntegrationType string
	}{
		{
			name:                "http_url_target_is_http_proxy",
			target:              "https://example.com/backend",
			wantIntegrationType: "HTTP_PROXY",
		},
		{
			name:                "lambda_arn_target_is_aws_proxy",
			target:              "arn:aws:lambda:us-east-1:123456789012:function:my-func",
			wantIntegrationType: "AWS_PROXY",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigatewayv2.NewInMemoryBackend()

			api, err := b.CreateAPI(context.Background(), apigatewayv2.CreateAPIInput{
				Name:         "quick-create-api",
				ProtocolType: "HTTP",
				RouteKey:     "GET /",
				Target:       tt.target,
			})
			require.NoError(t, err)

			routes, err := b.GetRoutes(api.APIID)
			require.NoError(t, err)
			require.Len(t, routes, 1)
			assert.Equal(t, "GET /", routes[0].RouteKey)
			assert.True(t, routes[0].APIGatewayManaged)
			assert.True(t, strings.HasPrefix(routes[0].Target, "integrations/"))

			integrations, err := b.GetIntegrations(api.APIID)
			require.NoError(t, err)
			require.Len(t, integrations, 1)
			assert.Equal(t, tt.wantIntegrationType, integrations[0].IntegrationType)
			assert.Equal(t, tt.target, integrations[0].IntegrationURI)
			assert.True(t, integrations[0].APIGatewayManaged)
			assert.Equal(t, "integrations/"+integrations[0].IntegrationID, routes[0].Target)

			stages, err := b.GetStages(api.APIID)
			require.NoError(t, err)
			require.Len(t, stages, 1)
			assert.Equal(t, "$default", stages[0].StageName)
			assert.True(t, stages[0].AutoDeploy)
			assert.True(t, stages[0].APIGatewayManaged)
			assert.NotEmpty(t, stages[0].DeploymentID, "quick create should auto-deploy the $default stage")

			deployments, err := b.GetDeployments(api.APIID)
			require.NoError(t, err)
			require.Len(t, deployments, 1)
			assert.Equal(t, "DEPLOYED", deployments[0].DeploymentStatus)
		})
	}
}

// Test_CreateAPI_QuickCreate_Validation covers the input-validation edge
// cases for the routeKey+target shortcut: AWS requires both fields together
// and only supports them for HTTP APIs.
func Test_CreateAPI_QuickCreate_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		protocolType string
		routeKey     string
		target       string
	}{
		{name: "route_key_without_target", protocolType: "HTTP", routeKey: "GET /", target: ""},
		{name: "target_without_route_key", protocolType: "HTTP", routeKey: "", target: "https://example.com"},
		{
			name: "websocket_rejects_quick_create", protocolType: "WEBSOCKET",
			routeKey: "$default", target: "https://example.com",
		},
		{
			name: "invalid_http_route_key_format", protocolType: "HTTP",
			routeKey: "not-a-route-key", target: "https://example.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigatewayv2.NewInMemoryBackend()

			_, err := b.CreateAPI(context.Background(), apigatewayv2.CreateAPIInput{
				Name:         "api",
				ProtocolType: tt.protocolType,
				RouteKey:     tt.routeKey,
				Target:       tt.target,
			})
			require.Error(t, err)
			assert.ErrorIs(t, err, apigatewayv2.ErrBadRequest)
		})
	}
}

// Test_CreateAPI_NoQuickCreate_NotManaged confirms a normal CreateApi call
// (no routeKey/target) does not create any child resources and that
// directly-created routes/integrations/stages are not apiGatewayManaged --
// the flag must stay false for the overwhelming majority of resources that
// weren't produced by the quick-create shortcut.
func Test_CreateAPI_NoQuickCreate_NotManaged(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()

	api, err := b.CreateAPI(context.Background(), apigatewayv2.CreateAPIInput{
		Name:         "plain-api",
		ProtocolType: "HTTP",
	})
	require.NoError(t, err)

	routes, err := b.GetRoutes(api.APIID)
	require.NoError(t, err)
	assert.Empty(t, routes)

	integrations, err := b.GetIntegrations(api.APIID)
	require.NoError(t, err)
	assert.Empty(t, integrations)

	stages, err := b.GetStages(api.APIID)
	require.NoError(t, err)
	assert.Empty(t, stages)

	route, err := b.CreateRoute(api.APIID, apigatewayv2.CreateRouteInput{RouteKey: "GET /foo"})
	require.NoError(t, err)
	assert.False(t, route.APIGatewayManaged)

	integration, err := b.CreateIntegration(api.APIID, apigatewayv2.CreateIntegrationInput{
		IntegrationType: "HTTP_PROXY",
		IntegrationURI:  "https://example.com",
	})
	require.NoError(t, err)
	assert.False(t, integration.APIGatewayManaged)

	stage, err := b.CreateStage(api.APIID, apigatewayv2.CreateStageInput{StageName: "prod"})
	require.NoError(t, err)
	assert.False(t, stage.APIGatewayManaged)
}

// Test_UpdateAPI_QuickCreate covers UpdateApiInput's routeKey/target fields,
// which the SDK docs mark "part of quick create": each independently
// replaces the route key / integration target+type of the API's existing
// quick-create route/integration. Before this fix UpdateAPIInput had no
// such fields, so these updates silently no-opped.
func Test_UpdateAPI_QuickCreate(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()

	api, err := b.CreateAPI(context.Background(), apigatewayv2.CreateAPIInput{
		Name:         "quick-create-api",
		ProtocolType: "HTTP",
		RouteKey:     "GET /",
		Target:       "https://example.com/backend",
	})
	require.NoError(t, err)

	_, err = b.UpdateAPI(api.APIID, apigatewayv2.UpdateAPIInput{
		RouteKey: "POST /submit",
		Target:   "arn:aws:lambda:us-east-1:123456789012:function:my-func",
	})
	require.NoError(t, err)

	routes, err := b.GetRoutes(api.APIID)
	require.NoError(t, err)
	require.Len(t, routes, 1)
	assert.Equal(t, "POST /submit", routes[0].RouteKey)
	assert.True(t, routes[0].APIGatewayManaged)

	integrations, err := b.GetIntegrations(api.APIID)
	require.NoError(t, err)
	require.Len(t, integrations, 1)
	assert.Equal(t, "arn:aws:lambda:us-east-1:123456789012:function:my-func", integrations[0].IntegrationURI)
	assert.Equal(t, "AWS_PROXY", integrations[0].IntegrationType)
	assert.True(t, integrations[0].APIGatewayManaged)
}

// Test_UpdateAPI_QuickCreate_NoExistingQuickCreate confirms routeKey/target
// on UpdateApi is rejected (not silently ignored) when the API has no
// quick-create route/integration to update.
func Test_UpdateAPI_QuickCreate_NoExistingQuickCreate(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()

	api, err := b.CreateAPI(context.Background(), apigatewayv2.CreateAPIInput{
		Name:         "plain-api",
		ProtocolType: "HTTP",
	})
	require.NoError(t, err)

	_, err = b.UpdateAPI(api.APIID, apigatewayv2.UpdateAPIInput{RouteKey: "GET /"})
	require.ErrorIs(t, err, apigatewayv2.ErrBadRequest)

	_, err = b.UpdateAPI(api.APIID, apigatewayv2.UpdateAPIInput{Target: "https://example.com"})
	require.ErrorIs(t, err, apigatewayv2.ErrBadRequest)
}

// Test_CreateAPI_IPAddressType covers API.IPAddressType, which the real AWS
// SDK carries on CreateApiInput/UpdateApiInput/Api but was entirely absent
// from gopherstack's shapes, so a caller-supplied ipAddressType was silently
// dropped on decode and GetApi always returned "" instead of the AWS
// default ("ipv4").
func Test_CreateAPI_IPAddressType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{name: "defaults_to_ipv4", input: "", want: "ipv4"},
		{name: "explicit_ipv4", input: "ipv4", want: "ipv4"},
		{name: "explicit_dualstack", input: "dualstack", want: "dualstack"},
		{name: "rejects_invalid_value", input: "ipv6", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigatewayv2.NewInMemoryBackend()

			api, err := b.CreateAPI(context.Background(), apigatewayv2.CreateAPIInput{
				Name:          "api",
				ProtocolType:  "HTTP",
				IPAddressType: tt.input,
			})

			if tt.wantErr {
				require.ErrorIs(t, err, apigatewayv2.ErrBadRequest)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, api.IPAddressType)

			got, err := b.GetAPI(api.APIID)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got.IPAddressType)
		})
	}
}

// Test_UpdateAPI_IPAddressType covers updating an existing API's
// IPAddressType, and that an invalid value is rejected rather than silently
// applied.
func Test_UpdateAPI_IPAddressType(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()

	api, err := b.CreateAPI(context.Background(), apigatewayv2.CreateAPIInput{Name: "api", ProtocolType: "HTTP"})
	require.NoError(t, err)
	require.Equal(t, "ipv4", api.IPAddressType)

	updated, err := b.UpdateAPI(api.APIID, apigatewayv2.UpdateAPIInput{IPAddressType: "dualstack"})
	require.NoError(t, err)
	assert.Equal(t, "dualstack", updated.IPAddressType)

	_, err = b.UpdateAPI(api.APIID, apigatewayv2.UpdateAPIInput{IPAddressType: "not-a-type"})
	require.ErrorIs(t, err, apigatewayv2.ErrBadRequest)
}

// Test_CreateAPI_QuickCreate_CredentialsArn covers CreateApiInput's
// quick-create-only CredentialsArn, which the real AWS SDK threads through
// to the auto-provisioned integration but was entirely absent from
// gopherstack's CreateAPIInput.
func Test_CreateAPI_QuickCreate_CredentialsArn(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()

	const roleARN = "arn:aws:iam::123456789012:role/apigw-role"

	api, err := b.CreateAPI(context.Background(), apigatewayv2.CreateAPIInput{
		Name:           "quick-create-api",
		ProtocolType:   "HTTP",
		RouteKey:       "GET /",
		Target:         "https://example.com/backend",
		CredentialsArn: roleARN,
	})
	require.NoError(t, err)

	integrations, err := b.GetIntegrations(api.APIID)
	require.NoError(t, err)
	require.Len(t, integrations, 1)
	assert.Equal(t, roleARN, integrations[0].CredentialsArn)
}

// Test_UpdateAPI_QuickCreate_CredentialsArn covers UpdateApiInput's
// quick-create-only CredentialsArn, which independently replaces the
// managed integration's credentials, and that it is rejected (not silently
// ignored) when the API has no quick-create integration.
func Test_UpdateAPI_QuickCreate_CredentialsArn(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()

	api, err := b.CreateAPI(context.Background(), apigatewayv2.CreateAPIInput{
		Name:         "quick-create-api",
		ProtocolType: "HTTP",
		RouteKey:     "GET /",
		Target:       "https://example.com/backend",
	})
	require.NoError(t, err)

	const roleARN = "arn:aws:iam::123456789012:role/apigw-role"

	_, err = b.UpdateAPI(api.APIID, apigatewayv2.UpdateAPIInput{CredentialsArn: roleARN})
	require.NoError(t, err)

	integrations, err := b.GetIntegrations(api.APIID)
	require.NoError(t, err)
	require.Len(t, integrations, 1)
	assert.Equal(t, roleARN, integrations[0].CredentialsArn)

	plainAPI, err := b.CreateAPI(context.Background(), apigatewayv2.CreateAPIInput{Name: "plain", ProtocolType: "HTTP"})
	require.NoError(t, err)

	_, err = b.UpdateAPI(plainAPI.APIID, apigatewayv2.UpdateAPIInput{CredentialsArn: roleARN})
	require.ErrorIs(t, err, apigatewayv2.ErrBadRequest)
}
