package apigatewayv2_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
)

func TestInMemoryBackend_CreateGetAPI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    apigatewayv2.CreateAPIInput
		wantName string
		wantProt string
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

			api, err := b.CreateAPI(tt.input)
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
				_, err := b.CreateAPI(apigatewayv2.CreateAPIInput{Name: n, ProtocolType: "HTTP"})
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
				api, err := b.CreateAPI(apigatewayv2.CreateAPIInput{Name: "test", ProtocolType: "HTTP"})
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
				api, err := b.CreateAPI(apigatewayv2.CreateAPIInput{Name: "original", ProtocolType: "HTTP"})
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

func TestInMemoryBackend_Stages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		name      string
		stageName string
	}{
		{
			name:      "create_and_get",
			stageName: "prod",
		},
		{
			name:      "get_not_found",
			stageName: "nonexistent",
			wantErr:   apigatewayv2.ErrStageNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigatewayv2.NewInMemoryBackend()

			api, err := b.CreateAPI(apigatewayv2.CreateAPIInput{Name: "test", ProtocolType: "HTTP"})
			require.NoError(t, err)

			if tt.wantErr == nil {
				stage, createErr := b.CreateStage(api.APIID, apigatewayv2.CreateStageInput{StageName: tt.stageName})
				require.NoError(t, createErr)
				assert.Equal(t, tt.stageName, stage.StageName)

				got, getErr := b.GetStage(api.APIID, tt.stageName)
				require.NoError(t, getErr)
				assert.Equal(t, tt.stageName, got.StageName)
			} else {
				_, getErr := b.GetStage(api.APIID, tt.stageName)
				require.ErrorIs(t, getErr, tt.wantErr)
			}
		})
	}
}

func TestInMemoryBackend_Routes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		routeKey string
	}{
		{
			name:     "get_items_route",
			routeKey: "GET /items",
		},
		{
			name:     "post_items_route",
			routeKey: "POST /items",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigatewayv2.NewInMemoryBackend()

			api, err := b.CreateAPI(apigatewayv2.CreateAPIInput{Name: "test", ProtocolType: "HTTP"})
			require.NoError(t, err)

			route, err := b.CreateRoute(api.APIID, apigatewayv2.CreateRouteInput{RouteKey: tt.routeKey})
			require.NoError(t, err)
			assert.Equal(t, tt.routeKey, route.RouteKey)
			assert.NotEmpty(t, route.RouteID)

			got, err := b.GetRoute(api.APIID, route.RouteID)
			require.NoError(t, err)
			assert.Equal(t, route.RouteID, got.RouteID)

			routes, err := b.GetRoutes(api.APIID)
			require.NoError(t, err)
			assert.Len(t, routes, 1)

			err = b.DeleteRoute(api.APIID, route.RouteID)
			require.NoError(t, err)

			_, err = b.GetRoute(api.APIID, route.RouteID)
			require.ErrorIs(t, err, apigatewayv2.ErrRouteNotFound)
		})
	}
}

func TestInMemoryBackend_Integrations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		integrationType string
	}{
		{
			name:            "aws_proxy",
			integrationType: "AWS_PROXY",
		},
		{
			name:            "http_proxy",
			integrationType: "HTTP_PROXY",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigatewayv2.NewInMemoryBackend()

			api, err := b.CreateAPI(apigatewayv2.CreateAPIInput{Name: "test", ProtocolType: "HTTP"})
			require.NoError(t, err)

			integration, err := b.CreateIntegration(api.APIID, apigatewayv2.CreateIntegrationInput{
				IntegrationType: tt.integrationType,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.integrationType, integration.IntegrationType)
			assert.NotEmpty(t, integration.IntegrationID)

			got, err := b.GetIntegration(api.APIID, integration.IntegrationID)
			require.NoError(t, err)
			assert.Equal(t, integration.IntegrationID, got.IntegrationID)

			integrations, err := b.GetIntegrations(api.APIID)
			require.NoError(t, err)
			assert.Len(t, integrations, 1)

			err = b.DeleteIntegration(api.APIID, integration.IntegrationID)
			require.NoError(t, err)

			_, err = b.GetIntegration(api.APIID, integration.IntegrationID)
			require.ErrorIs(t, err, apigatewayv2.ErrIntegrationNotFound)
		})
	}
}

func TestInMemoryBackend_Deployments(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		desc string
	}{
		{
			name: "basic_deployment",
			desc: "initial",
		},
		{
			name: "empty_desc",
			desc: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigatewayv2.NewInMemoryBackend()

			api, err := b.CreateAPI(apigatewayv2.CreateAPIInput{Name: "test", ProtocolType: "HTTP"})
			require.NoError(t, err)

			deployment, err := b.CreateDeployment(api.APIID, apigatewayv2.CreateDeploymentInput{Description: tt.desc})
			require.NoError(t, err)
			assert.NotEmpty(t, deployment.DeploymentID)
			assert.Equal(t, "DEPLOYED", deployment.DeploymentStatus)

			got, err := b.GetDeployment(api.APIID, deployment.DeploymentID)
			require.NoError(t, err)
			assert.Equal(t, deployment.DeploymentID, got.DeploymentID)

			deployments, err := b.GetDeployments(api.APIID)
			require.NoError(t, err)
			assert.Len(t, deployments, 1)

			err = b.DeleteDeployment(api.APIID, deployment.DeploymentID)
			require.NoError(t, err)

			_, err = b.GetDeployment(api.APIID, deployment.DeploymentID)
			require.ErrorIs(t, err, apigatewayv2.ErrDeploymentNotFound)
		})
	}
}

func TestInMemoryBackend_Authorizers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		authorizerName string
		authType       string
	}{
		{
			name:           "jwt_authorizer",
			authorizerName: "my-jwt-auth",
			authType:       "JWT",
		},
		{
			name:           "request_authorizer",
			authorizerName: "my-req-auth",
			authType:       "REQUEST",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigatewayv2.NewInMemoryBackend()

			api, err := b.CreateAPI(apigatewayv2.CreateAPIInput{Name: "test", ProtocolType: "HTTP"})
			require.NoError(t, err)

			authorizer, err := b.CreateAuthorizer(api.APIID, apigatewayv2.CreateAuthorizerInput{
				Name:           tt.authorizerName,
				AuthorizerType: tt.authType,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.authorizerName, authorizer.Name)
			assert.Equal(t, tt.authType, authorizer.AuthorizerType)
			assert.NotEmpty(t, authorizer.AuthorizerID)

			got, err := b.GetAuthorizer(api.APIID, authorizer.AuthorizerID)
			require.NoError(t, err)
			assert.Equal(t, authorizer.AuthorizerID, got.AuthorizerID)

			authorizers, err := b.GetAuthorizers(api.APIID)
			require.NoError(t, err)
			assert.Len(t, authorizers, 1)

			updated, err := b.UpdateAuthorizer(api.APIID, authorizer.AuthorizerID, apigatewayv2.UpdateAuthorizerInput{
				Name: "updated-name",
			})
			require.NoError(t, err)
			assert.Equal(t, "updated-name", updated.Name)

			err = b.DeleteAuthorizer(api.APIID, authorizer.AuthorizerID)
			require.NoError(t, err)

			_, err = b.GetAuthorizer(api.APIID, authorizer.AuthorizerID)
			require.ErrorIs(t, err, apigatewayv2.ErrAuthorizerNotFound)
		})
	}
}

func TestInMemoryBackend_Persistence(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()

	api, err := b.CreateAPI(apigatewayv2.CreateAPIInput{Name: "test-api", ProtocolType: "HTTP"})
	require.NoError(t, err)

	_, err = b.CreateStage(api.APIID, apigatewayv2.CreateStageInput{StageName: "prod"})
	require.NoError(t, err)

	snap := b.Snapshot()
	require.NotNil(t, snap)

	b2 := apigatewayv2.NewInMemoryBackend()
	require.NoError(t, b2.Restore(snap))

	got, err := b2.GetAPI(api.APIID)
	require.NoError(t, err)
	assert.Equal(t, api.Name, got.Name)

	stage, err := b2.GetStage(api.APIID, "prod")
	require.NoError(t, err)
	assert.Equal(t, "prod", stage.StageName)
}

func TestProvider(t *testing.T) {
	t.Parallel()

	p := &apigatewayv2.Provider{}
	assert.Equal(t, "APIGatewayV2", p.Name())

	registerable, err := p.Init(nil)
	require.NoError(t, err)
	require.NotNil(t, registerable)
}

func TestInMemoryBackend_UpdateStage_AllFields(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()

	api, err := b.CreateAPI(apigatewayv2.CreateAPIInput{Name: "test", ProtocolType: "HTTP"})
	require.NoError(t, err)

	_, err = b.CreateStage(api.APIID, apigatewayv2.CreateStageInput{StageName: "dev"})
	require.NoError(t, err)

	autoDeploy := true
	updated, err := b.UpdateStage(api.APIID, "dev", apigatewayv2.UpdateStageInput{
		DeploymentID:   "deploy-1",
		Description:    "new desc",
		AutoDeploy:     &autoDeploy,
		StageVariables: map[string]string{"key": "val"},
	})
	require.NoError(t, err)
	assert.Equal(t, "deploy-1", updated.DeploymentID)
	assert.Equal(t, "new desc", updated.Description)
	assert.True(t, updated.AutoDeploy)
}

func TestInMemoryBackend_UpdateRoute_AllFields(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()

	api, err := b.CreateAPI(apigatewayv2.CreateAPIInput{Name: "test", ProtocolType: "HTTP"})
	require.NoError(t, err)

	route, err := b.CreateRoute(api.APIID, apigatewayv2.CreateRouteInput{RouteKey: "GET /test"})
	require.NoError(t, err)

	updated, err := b.UpdateRoute(api.APIID, route.RouteID, apigatewayv2.UpdateRouteInput{
		RouteKey:          "POST /test",
		Target:            "integrations/abc",
		AuthorizationType: "JWT",
		AuthorizerID:      "auth-1",
		OperationName:     "DoSomething",
	})
	require.NoError(t, err)
	assert.Equal(t, "POST /test", updated.RouteKey)
	assert.Equal(t, "integrations/abc", updated.Target)
}

func TestInMemoryBackend_UpdateIntegration_AllFields(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()

	api, err := b.CreateAPI(apigatewayv2.CreateAPIInput{Name: "test", ProtocolType: "HTTP"})
	require.NoError(t, err)

	integration, err := b.CreateIntegration(api.APIID, apigatewayv2.CreateIntegrationInput{
		IntegrationType: "AWS_PROXY",
	})
	require.NoError(t, err)

	updated, err := b.UpdateIntegration(api.APIID, integration.IntegrationID, apigatewayv2.UpdateIntegrationInput{
		IntegrationType:      "HTTP_PROXY",
		IntegrationMethod:    "POST",
		IntegrationURI:       "https://example.com",
		Description:          "updated",
		PayloadFormatVersion: "2.0",
		ConnectionType:       "INTERNET",
		ConnectionID:         "conn-1",
		TimeoutInMillis:      5000,
	})
	require.NoError(t, err)
	assert.Equal(t, "HTTP_PROXY", updated.IntegrationType)
	assert.Equal(t, "POST", updated.IntegrationMethod)
	assert.Equal(t, int32(5000), updated.TimeoutInMillis)
}

func TestInMemoryBackend_UpdateAuthorizer_AllFields(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()

	api, err := b.CreateAPI(apigatewayv2.CreateAPIInput{Name: "test", ProtocolType: "HTTP"})
	require.NoError(t, err)

	auth, err := b.CreateAuthorizer(api.APIID, apigatewayv2.CreateAuthorizerInput{
		Name:           "auth",
		AuthorizerType: "JWT",
	})
	require.NoError(t, err)

	updated, err := b.UpdateAuthorizer(api.APIID, auth.AuthorizerID, apigatewayv2.UpdateAuthorizerInput{
		Name:                         "new-auth",
		AuthorizerType:               "REQUEST",
		AuthorizerURI:                "https://auth.example.com",
		IdentitySource:               "$request.header.Authorization",
		AuthorizerCredentialsArn:     "arn:aws:iam::123:role/role",
		AuthorizerResultTTLInSeconds: 300,
	})
	require.NoError(t, err)
	assert.Equal(t, "new-auth", updated.Name)
	assert.Equal(t, "REQUEST", updated.AuthorizerType)
	assert.Equal(t, int32(300), updated.AuthorizerResultTTLInSeconds)
}

func TestInMemoryBackend_UpdateAPI_AllFields(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()

	api, err := b.CreateAPI(apigatewayv2.CreateAPIInput{Name: "test", ProtocolType: "HTTP"})
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

func TestInMemoryBackend_CreateStage_ApiNotFound(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()

	_, err := b.CreateStage("bad-api", apigatewayv2.CreateStageInput{StageName: "prod"})
	require.ErrorIs(t, err, apigatewayv2.ErrAPINotFound)
}

func TestInMemoryBackend_CreateRoute_ApiNotFound(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()

	_, err := b.CreateRoute("bad-api", apigatewayv2.CreateRouteInput{RouteKey: "GET /test"})
	require.ErrorIs(t, err, apigatewayv2.ErrAPINotFound)
}

func TestInMemoryBackend_CreateIntegration_ApiNotFound(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()

	_, err := b.CreateIntegration("bad-api", apigatewayv2.CreateIntegrationInput{IntegrationType: "AWS_PROXY"})
	require.ErrorIs(t, err, apigatewayv2.ErrAPINotFound)
}

func TestInMemoryBackend_CreateDeployment_ApiNotFound(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()

	_, err := b.CreateDeployment("bad-api", apigatewayv2.CreateDeploymentInput{})
	require.ErrorIs(t, err, apigatewayv2.ErrAPINotFound)
}

func TestInMemoryBackend_CreateAuthorizer_ApiNotFound(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()

	_, err := b.CreateAuthorizer("bad-api", apigatewayv2.CreateAuthorizerInput{Name: "auth", AuthorizerType: "JWT"})
	require.ErrorIs(t, err, apigatewayv2.ErrAPINotFound)
}
