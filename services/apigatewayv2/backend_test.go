package apigatewayv2_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"

	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
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

			api, err := b.CreateAPI(
				context.Background(),
				apigatewayv2.CreateAPIInput{Name: "test", ProtocolType: "HTTP"},
			)
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

			api, err := b.CreateAPI(
				context.Background(),
				apigatewayv2.CreateAPIInput{Name: "test", ProtocolType: "HTTP"},
			)
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

			api, err := b.CreateAPI(
				context.Background(),
				apigatewayv2.CreateAPIInput{Name: "test", ProtocolType: "HTTP"},
			)
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

			api, err := b.CreateAPI(
				context.Background(),
				apigatewayv2.CreateAPIInput{Name: "test", ProtocolType: "HTTP"},
			)
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

	jwtConfig := &apigatewayv2.JwtConfiguration{Issuer: "https://issuer.example.com", Audience: []string{"client-id"}}

	tests := []struct {
		jwtConfig      *apigatewayv2.JwtConfiguration
		name           string
		authorizerName string
		authType       string
	}{
		{
			name:           "jwt_authorizer",
			authorizerName: "my-jwt-auth",
			authType:       "JWT",
			jwtConfig:      jwtConfig,
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

			api, err := b.CreateAPI(
				context.Background(),
				apigatewayv2.CreateAPIInput{Name: "test", ProtocolType: "HTTP"},
			)
			require.NoError(t, err)

			authorizer, err := b.CreateAuthorizer(api.APIID, apigatewayv2.CreateAuthorizerInput{
				Name:             tt.authorizerName,
				AuthorizerType:   tt.authType,
				JwtConfiguration: tt.jwtConfig,
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

	api, err := b.CreateAPI(context.Background(), apigatewayv2.CreateAPIInput{Name: "test-api", ProtocolType: "HTTP"})
	require.NoError(t, err)

	_, err = b.CreateStage(api.APIID, apigatewayv2.CreateStageInput{StageName: "prod"})
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := apigatewayv2.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

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

	api, err := b.CreateAPI(context.Background(), apigatewayv2.CreateAPIInput{Name: "test", ProtocolType: "HTTP"})
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

	api, err := b.CreateAPI(context.Background(), apigatewayv2.CreateAPIInput{Name: "test", ProtocolType: "HTTP"})
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

	api, err := b.CreateAPI(context.Background(), apigatewayv2.CreateAPIInput{Name: "test", ProtocolType: "HTTP"})
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

	api, err := b.CreateAPI(context.Background(), apigatewayv2.CreateAPIInput{Name: "test", ProtocolType: "HTTP"})
	require.NoError(t, err)

	auth, err := b.CreateAuthorizer(api.APIID, apigatewayv2.CreateAuthorizerInput{
		Name:           "auth",
		AuthorizerType: "JWT",
		JwtConfiguration: &apigatewayv2.JwtConfiguration{
			Issuer:   "https://issuer.example.com",
			Audience: []string{"client-id"},
		},
	})
	require.NoError(t, err)

	updated, err := b.UpdateAuthorizer(api.APIID, auth.AuthorizerID, apigatewayv2.UpdateAuthorizerInput{
		Name:                         "new-auth",
		AuthorizerType:               "REQUEST",
		AuthorizerURI:                "https://auth.example.com",
		IdentitySource:               []string{"$request.header.Authorization"},
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

	_, err := b.CreateAuthorizer("bad-api", apigatewayv2.CreateAuthorizerInput{
		Name:           "auth",
		AuthorizerType: "JWT",
		JwtConfiguration: &apigatewayv2.JwtConfiguration{
			Issuer:   "https://issuer.example.com",
			Audience: []string{"client-id"},
		},
	})
	require.ErrorIs(t, err, apigatewayv2.ErrAPINotFound)
}

func TestInMemoryBackend_JWTAuthorizer_StoresConfig(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()

	api, err := b.CreateAPI(context.Background(), apigatewayv2.CreateAPIInput{Name: "test", ProtocolType: "HTTP"})
	require.NoError(t, err)

	jwtCfg := &apigatewayv2.JwtConfiguration{
		Issuer:   "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_abc",
		Audience: []string{"client-id-1", "client-id-2"},
	}

	authorizer, err := b.CreateAuthorizer(api.APIID, apigatewayv2.CreateAuthorizerInput{
		Name:             "jwt-auth",
		AuthorizerType:   "JWT",
		JwtConfiguration: jwtCfg,
		IdentitySource:   []string{"$request.header.Authorization"},
	})
	require.NoError(t, err)
	require.NotNil(t, authorizer.JwtConfiguration)
	assert.Equal(t, jwtCfg.Issuer, authorizer.JwtConfiguration.Issuer)
	assert.Equal(t, jwtCfg.Audience, authorizer.JwtConfiguration.Audience)

	// GetAuthorizer returns JwtConfiguration.
	got, err := b.GetAuthorizer(api.APIID, authorizer.AuthorizerID)
	require.NoError(t, err)
	require.NotNil(t, got.JwtConfiguration)
	assert.Equal(t, jwtCfg.Issuer, got.JwtConfiguration.Issuer)

	// CreateRoute with JWT authorizationType references the authorizer.
	route, err := b.CreateRoute(api.APIID, apigatewayv2.CreateRouteInput{
		RouteKey:          "GET /protected",
		AuthorizationType: "JWT",
		AuthorizerID:      authorizer.AuthorizerID,
	})
	require.NoError(t, err)
	assert.Equal(t, "JWT", route.AuthorizationType)
	assert.Equal(t, authorizer.AuthorizerID, route.AuthorizerID)

	gotRoute, err := b.GetRoute(api.APIID, route.RouteID)
	require.NoError(t, err)
	assert.Equal(t, authorizer.AuthorizerID, gotRoute.AuthorizerID)
}

func TestInMemoryBackend_RouteSettings(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()

	api, err := b.CreateAPI(context.Background(), apigatewayv2.CreateAPIInput{Name: "test", ProtocolType: "HTTP"})
	require.NoError(t, err)

	stage, err := b.CreateStage(api.APIID, apigatewayv2.CreateStageInput{StageName: "prod"})
	require.NoError(t, err)
	assert.Nil(t, stage.RouteSettings)

	// UpdateStage with RouteSettings persists them.
	routeKey := "GET /items"
	updated, err := b.UpdateStage(api.APIID, stage.StageName, apigatewayv2.UpdateStageInput{
		RouteSettings: map[string]apigatewayv2.RouteSettings{
			routeKey: {ThrottlingRateLimit: 100, ThrottlingBurstLimit: 50},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, updated.RouteSettings)
	assert.InDelta(t, float64(100), updated.RouteSettings[routeKey].ThrottlingRateLimit, 0)

	// GetStage returns RouteSettings.
	got, err := b.GetStage(api.APIID, stage.StageName)
	require.NoError(t, err)
	require.NotNil(t, got.RouteSettings)
	assert.Equal(t, int32(50), got.RouteSettings[routeKey].ThrottlingBurstLimit)

	// DeleteRouteSettings removes the key.
	err = b.DeleteRouteSettings(api.APIID, stage.StageName, routeKey)
	require.NoError(t, err)

	got2, err := b.GetStage(api.APIID, stage.StageName)
	require.NoError(t, err)
	_, exists := got2.RouteSettings[routeKey]
	assert.False(t, exists)
}

func TestInMemoryBackend_StageAccessLogSettings(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()

	api, err := b.CreateAPI(context.Background(), apigatewayv2.CreateAPIInput{Name: "test", ProtocolType: "HTTP"})
	require.NoError(t, err)

	_, err = b.CreateStage(api.APIID, apigatewayv2.CreateStageInput{StageName: "prod"})
	require.NoError(t, err)

	destARN := "arn:aws:logs:us-east-1:123456789012:log-group:/aws/apigateway/prod"
	updated, err := b.UpdateStage(api.APIID, "prod", apigatewayv2.UpdateStageInput{
		AccessLogSettings: &apigatewayv2.AccessLogSettings{
			DestinationArn: destARN,
			Format:         "$context.requestId",
		},
	})
	require.NoError(t, err)
	require.NotNil(t, updated.AccessLogSettings)
	assert.Equal(t, destARN, updated.AccessLogSettings.DestinationArn)

	got, err := b.GetStage(api.APIID, "prod")
	require.NoError(t, err)
	require.NotNil(t, got.AccessLogSettings)
	assert.Equal(t, destARN, got.AccessLogSettings.DestinationArn)

	// DeleteAccessLogSettings clears it.
	err = b.DeleteAccessLogSettings(api.APIID, "prod")
	require.NoError(t, err)

	got2, err := b.GetStage(api.APIID, "prod")
	require.NoError(t, err)
	assert.Nil(t, got2.AccessLogSettings)
}

func TestInMemoryBackend_DomainNames(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()

	// CreateDomainName.
	dn, err := b.CreateDomainName(context.Background(), apigatewayv2.CreateDomainNameInput{
		DomainNameValue: "api.example.com",
		DomainNameConfigurations: []apigatewayv2.DomainNameConfiguration{
			{CertificateArn: "arn:aws:acm:us-east-1:123:certificate/abc", EndpointType: "REGIONAL"},
		},
		Tags: map[string]string{"env": "prod"},
	})
	require.NoError(t, err)
	assert.Equal(t, "api.example.com", dn.DomainNameValue)
	require.Len(t, dn.DomainNameConfigurations, 1)
	assert.Equal(t, "REGIONAL", dn.DomainNameConfigurations[0].EndpointType)

	// GetDomainName.
	got, err := b.GetDomainName("api.example.com")
	require.NoError(t, err)
	assert.Equal(t, "api.example.com", got.DomainNameValue)

	// GetDomainNames.
	all, err := b.GetDomainNames()
	require.NoError(t, err)
	assert.Len(t, all, 1)

	// UpdateDomainName.
	upd, err := b.UpdateDomainName("api.example.com", apigatewayv2.UpdateDomainNameInput{
		DomainNameConfigurations: []apigatewayv2.DomainNameConfiguration{
			{CertificateArn: "arn:aws:acm:us-east-1:123:certificate/xyz", EndpointType: "EDGE"},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "EDGE", upd.DomainNameConfigurations[0].EndpointType)

	// DeleteDomainName.
	err = b.DeleteDomainName("api.example.com")
	require.NoError(t, err)

	_, err = b.GetDomainName("api.example.com")
	require.ErrorIs(t, err, apigatewayv2.ErrDomainNameNotFound)
}

func TestInMemoryBackend_APIMappings(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()

	api, err := b.CreateAPI(context.Background(), apigatewayv2.CreateAPIInput{Name: "test", ProtocolType: "HTTP"})
	require.NoError(t, err)

	_, err = b.CreateStage(api.APIID, apigatewayv2.CreateStageInput{StageName: "prod"})
	require.NoError(t, err)

	_, err = b.CreateDomainName(
		context.Background(),
		apigatewayv2.CreateDomainNameInput{DomainNameValue: "api.example.com"},
	)
	require.NoError(t, err)

	// CreateAPIMapping.
	m, err := b.CreateAPIMapping("api.example.com", apigatewayv2.CreateAPIMappingInput{
		APIID:         api.APIID,
		Stage:         "prod",
		APIMappingKey: "v1",
	})
	require.NoError(t, err)
	assert.Equal(t, api.APIID, m.APIID)
	assert.Equal(t, "prod", m.Stage)
	assert.Equal(t, "v1", m.APIMappingKey)
	assert.NotEmpty(t, m.APIMappingID)

	// GetAPIMapping.
	got, err := b.GetAPIMapping("api.example.com", m.APIMappingID)
	require.NoError(t, err)
	assert.Equal(t, m.APIMappingID, got.APIMappingID)

	// GetAPIMappings.
	all, err := b.GetAPIMappings("api.example.com")
	require.NoError(t, err)
	assert.Len(t, all, 1)

	// UpdateAPIMapping.
	upd, err := b.UpdateAPIMapping("api.example.com", m.APIMappingID, apigatewayv2.UpdateAPIMappingInput{
		APIMappingKey: "v2",
	})
	require.NoError(t, err)
	assert.Equal(t, "v2", upd.APIMappingKey)

	// DeleteAPIMapping.
	err = b.DeleteAPIMapping("api.example.com", m.APIMappingID)
	require.NoError(t, err)

	_, err = b.GetAPIMapping("api.example.com", m.APIMappingID)
	require.ErrorIs(t, err, apigatewayv2.ErrAPIMappingNotFound)
}

func TestInMemoryBackend_VpcLinks(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()

	// CreateVpcLink.
	vl, err := b.CreateVpcLink(apigatewayv2.CreateVpcLinkInput{
		Name:             "my-vpc-link",
		SubnetIDs:        []string{"subnet-aaa", "subnet-bbb"},
		SecurityGroupIDs: []string{"sg-111"},
		Tags:             map[string]string{"env": "prod"},
	})
	require.NoError(t, err)
	assert.Equal(t, "my-vpc-link", vl.Name)
	assert.Equal(t, "AVAILABLE", vl.VpcLinkStatus)
	assert.NotEmpty(t, vl.VpcLinkID)
	assert.Equal(t, []string{"subnet-aaa", "subnet-bbb"}, vl.SubnetIDs)
	assert.Equal(t, []string{"sg-111"}, vl.SecurityGroupIDs)

	// GetVpcLink.
	got, err := b.GetVpcLink(vl.VpcLinkID)
	require.NoError(t, err)
	assert.Equal(t, vl.VpcLinkID, got.VpcLinkID)

	// GetVpcLinks.
	all, err := b.GetVpcLinks()
	require.NoError(t, err)
	assert.Len(t, all, 1)

	// UpdateVpcLink.
	upd, err := b.UpdateVpcLink(vl.VpcLinkID, apigatewayv2.UpdateVpcLinkInput{Name: "updated-link"})
	require.NoError(t, err)
	assert.Equal(t, "updated-link", upd.Name)

	// DeleteVpcLink.
	err = b.DeleteVpcLink(vl.VpcLinkID)
	require.NoError(t, err)

	_, err = b.GetVpcLink(vl.VpcLinkID)
	require.ErrorIs(t, err, apigatewayv2.ErrVpcLinkNotFound)
}

func TestInMemoryBackend_Model_Schema(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()

	api, err := b.CreateAPI(context.Background(), apigatewayv2.CreateAPIInput{Name: "test", ProtocolType: "HTTP"})
	require.NoError(t, err)

	schema := `{"type":"object","properties":{"id":{"type":"string"}}}`
	model, err := b.CreateModel(api.APIID, apigatewayv2.CreateModelInput{
		Name:        "MyModel",
		Schema:      schema,
		ContentType: "application/json",
	})
	require.NoError(t, err)
	assert.Equal(t, schema, model.Schema)
	assert.Equal(t, "application/json", model.ContentType)

	got, err := b.GetModel(api.APIID, model.ModelID)
	require.NoError(t, err)
	assert.Equal(t, schema, got.Schema)
}

func TestInMemoryBackend_WebSocket_RouteSelectionExpression(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()

	// WEBSOCKET api stores RouteSelectionExpression.
	api, err := b.CreateAPI(context.Background(), apigatewayv2.CreateAPIInput{
		Name:                     "ws-api",
		ProtocolType:             "WEBSOCKET",
		RouteSelectionExpression: "$request.body.action",
	})
	require.NoError(t, err)
	assert.Equal(t, "$request.body.action", api.RouteSelectionExpression)

	// WebSocket routes like $connect, $disconnect, $default can be stored.
	for _, routeKey := range []string{"$connect", "$disconnect", "$default"} {
		var route *apigatewayv2.Route
		route, err = b.CreateRoute(api.APIID, apigatewayv2.CreateRouteInput{RouteKey: routeKey})
		require.NoError(t, err)
		assert.Equal(t, routeKey, route.RouteKey)
	}

	routes, err := b.GetRoutes(api.APIID)
	require.NoError(t, err)
	assert.Len(t, routes, 3)
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

func TestDomainNameConfiguration_SecurityPolicy_Defaults(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()
	dn, err := b.CreateDomainName(context.Background(), apigatewayv2.CreateDomainNameInput{
		DomainNameValue: "tls.example.com",
		DomainNameConfigurations: []apigatewayv2.DomainNameConfiguration{
			{
				CertificateArn: "arn:aws:acm:us-east-1:123456789012:certificate/abc",
				EndpointType:   "REGIONAL",
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, dn.DomainNameConfigurations, 1)

	cfg := dn.DomainNameConfigurations[0]
	assert.Equal(t, "TLS_1_2", cfg.SecurityPolicy)
	assert.Equal(t, "AVAILABLE", cfg.DomainNameStatus)
	assert.NotEmpty(t, cfg.APIGatewayDomainName)
	assert.NotEmpty(t, cfg.HostedZoneID)
}

func TestDomainNameConfiguration_CustomSecurityPolicy(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()
	dn, err := b.CreateDomainName(context.Background(), apigatewayv2.CreateDomainNameInput{
		DomainNameValue: "custom-tls.example.com",
		DomainNameConfigurations: []apigatewayv2.DomainNameConfiguration{
			{
				CertificateArn: "arn:aws:acm:us-east-1:123456789012:certificate/xyz",
				EndpointType:   "REGIONAL",
				SecurityPolicy: "TLS_1_0",
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, dn.DomainNameConfigurations, 1)
	assert.Equal(t, "TLS_1_0", dn.DomainNameConfigurations[0].SecurityPolicy)
}

func TestDomainNameConfiguration_ApiGatewayDomainName_Contains_DomainName(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()
	dn, err := b.CreateDomainName(context.Background(), apigatewayv2.CreateDomainNameInput{
		DomainNameValue: "api.mycompany.com",
		DomainNameConfigurations: []apigatewayv2.DomainNameConfiguration{
			{CertificateArn: "arn:aws:acm:us-east-1:123456789012:certificate/abc", EndpointType: "REGIONAL"},
		},
	})
	require.NoError(t, err)
	require.Len(t, dn.DomainNameConfigurations, 1)
	assert.Contains(t, dn.DomainNameConfigurations[0].APIGatewayDomainName, "api.mycompany.com")
}

func TestDomainNameConfiguration_CustomApiGatewayDomainName_Preserved(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()
	customDomain := "d-abc123.execute-api.us-east-1.amazonaws.com"
	dn, err := b.CreateDomainName(context.Background(), apigatewayv2.CreateDomainNameInput{
		DomainNameValue: "custom.example.com",
		DomainNameConfigurations: []apigatewayv2.DomainNameConfiguration{
			{
				CertificateArn:       "arn:aws:acm:us-east-1:123456789012:certificate/abc",
				EndpointType:         "REGIONAL",
				APIGatewayDomainName: customDomain,
				HostedZoneID:         "Z1HUB23UULQXV",
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, dn.DomainNameConfigurations, 1)
	assert.Equal(t, customDomain, dn.DomainNameConfigurations[0].APIGatewayDomainName)
	assert.Equal(t, "Z1HUB23UULQXV", dn.DomainNameConfigurations[0].HostedZoneID)
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
	assert.Equal(t, "arn:aws:apigateway:ap-southeast-2::/domainnames/api.example.com/routingrules/"+rule.RoutingRuleID,
		rule.RoutingRuleARN)
}

func TestCreateAPIEndpointFallsBackToDefaultRegion(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()

	api, err := b.CreateAPI(context.Background(), apigatewayv2.CreateAPIInput{Name: "x", ProtocolType: "HTTP"})
	require.NoError(t, err)
	assert.Contains(t, api.APIEndpoint, ".execute-api.us-east-1.amazonaws.com")
}

func TestInMemoryBackend_CreateRoute_HTTPRouteKeyValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		routeKey string
		wantErr  bool
	}{
		{name: "valid_get", routeKey: "GET /items", wantErr: false},
		{name: "valid_post", routeKey: "POST /orders", wantErr: false},
		{name: "valid_put", routeKey: "PUT /items/123", wantErr: false},
		{name: "valid_delete", routeKey: "DELETE /items/123", wantErr: false},
		{name: "valid_patch", routeKey: "PATCH /items/123", wantErr: false},
		{name: "valid_head", routeKey: "HEAD /items", wantErr: false},
		{name: "valid_options", routeKey: "OPTIONS /items", wantErr: false},
		{name: "valid_any", routeKey: "ANY /items", wantErr: false},
		{name: "valid_default", routeKey: "$default", wantErr: false},
		{name: "lowercase_method", routeKey: "get /items", wantErr: true},
		{name: "invalid_method", routeKey: "CONNECT /items", wantErr: true},
		{name: "missing_path", routeKey: "GET", wantErr: true},
		{name: "path_no_slash", routeKey: "GET items", wantErr: true},
		{name: "just_path", routeKey: "/items", wantErr: true},
		{name: "empty_path", routeKey: "GET ", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigatewayv2.NewInMemoryBackend()

			api, err := b.CreateAPI(context.Background(), apigatewayv2.CreateAPIInput{
				Name:         "http-api",
				ProtocolType: "HTTP",
			})
			require.NoError(t, err)

			_, err = b.CreateRoute(api.APIID, apigatewayv2.CreateRouteInput{RouteKey: tt.routeKey})
			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, apigatewayv2.ErrBadRequest)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestInMemoryBackend_WebSocketRouteKey_NoFormatValidation(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()

	api, err := b.CreateAPI(context.Background(), apigatewayv2.CreateAPIInput{
		Name:                     "ws-api",
		ProtocolType:             "WEBSOCKET",
		RouteSelectionExpression: "$request.body.action",
	})
	require.NoError(t, err)

	tests := []struct {
		name     string
		routeKey string
	}{
		{name: "connect", routeKey: "$connect"},
		{name: "disconnect", routeKey: "$disconnect"},
		{name: "message", routeKey: "$message"},
		{name: "default", routeKey: "$default"},
		{name: "custom", routeKey: "chat"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, routeErr := b.CreateRoute(api.APIID, apigatewayv2.CreateRouteInput{RouteKey: tt.routeKey})
			require.NoError(t, routeErr)
		})
	}
}

// TestInMemoryBackend_CreateIntegration_TimeoutValidation proves the
// per-protocol timeout ceiling AWS documents for CreateIntegration: HTTP APIs
// allow up to 30,000ms (default 30,000ms when unset) while WebSocket APIs
// allow up to 29,000ms (default 29,000ms when unset). Before this fix the
// backend hardcoded a 29,000ms ceiling/default for both protocols, incorrectly
// rejecting valid HTTP API timeouts in [29001, 30000] and under-defaulting
// unset HTTP API timeouts to 29000 instead of 30000.
func TestInMemoryBackend_CreateIntegration_TimeoutValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		protocolType string
		timeoutMs    int32
		wantErr      bool
		wantTimeout  int32
	}{
		{name: "http_zero_defaults_to_30000", protocolType: "HTTP", timeoutMs: 0, wantErr: false, wantTimeout: 30000},
		{name: "http_min_boundary_50", protocolType: "HTTP", timeoutMs: 50, wantErr: false, wantTimeout: 50},
		{name: "http_max_boundary_30000", protocolType: "HTTP", timeoutMs: 30000, wantErr: false, wantTimeout: 30000},
		{name: "http_mid_range_5000", protocolType: "HTTP", timeoutMs: 5000, wantErr: false, wantTimeout: 5000},
		{name: "http_29001_now_valid", protocolType: "HTTP", timeoutMs: 29001, wantErr: false, wantTimeout: 29001},
		{name: "http_too_low_49", protocolType: "HTTP", timeoutMs: 49, wantErr: true},
		{name: "http_too_low_1", protocolType: "HTTP", timeoutMs: 1, wantErr: true},
		{name: "http_too_high_30001", protocolType: "HTTP", timeoutMs: 30001, wantErr: true},
		{name: "http_too_high_60000", protocolType: "HTTP", timeoutMs: 60000, wantErr: true},
		{
			name: "ws_zero_defaults_to_29000", protocolType: "WEBSOCKET",
			timeoutMs: 0, wantErr: false, wantTimeout: 29000,
		},
		{
			name: "ws_max_boundary_29000", protocolType: "WEBSOCKET",
			timeoutMs: 29000, wantErr: false, wantTimeout: 29000,
		},
		{name: "ws_too_high_29001", protocolType: "WEBSOCKET", timeoutMs: 29001, wantErr: true},
		{name: "ws_too_high_30000", protocolType: "WEBSOCKET", timeoutMs: 30000, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigatewayv2.NewInMemoryBackend()

			api, err := b.CreateAPI(context.Background(), apigatewayv2.CreateAPIInput{
				Name:         "api",
				ProtocolType: tt.protocolType,
			})
			require.NoError(t, err)

			integrationType := "HTTP_PROXY"
			if tt.protocolType == "WEBSOCKET" {
				integrationType = "AWS"
			}

			intg, err := b.CreateIntegration(api.APIID, apigatewayv2.CreateIntegrationInput{
				IntegrationType: integrationType,
				IntegrationURI:  "https://example.com",
				TimeoutInMillis: tt.timeoutMs,
			})

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, apigatewayv2.ErrBadRequest)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantTimeout, intg.TimeoutInMillis)
			}
		})
	}
}

func TestInMemoryBackend_UpdateIntegration_TimeoutValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		protocolType string
		timeoutMs    int32
		wantErr      bool
	}{
		{name: "http_valid_50", protocolType: "HTTP", timeoutMs: 50, wantErr: false},
		{name: "http_valid_30000", protocolType: "HTTP", timeoutMs: 30000, wantErr: false},
		{name: "http_valid_1000", protocolType: "HTTP", timeoutMs: 1000, wantErr: false},
		{name: "http_zero_skips_update", protocolType: "HTTP", timeoutMs: 0, wantErr: false},
		{name: "http_too_low_49", protocolType: "HTTP", timeoutMs: 49, wantErr: true},
		{name: "http_too_high_30001", protocolType: "HTTP", timeoutMs: 30001, wantErr: true},
		{name: "ws_valid_29000", protocolType: "WEBSOCKET", timeoutMs: 29000, wantErr: false},
		{name: "ws_too_high_29001", protocolType: "WEBSOCKET", timeoutMs: 29001, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigatewayv2.NewInMemoryBackend()

			api, err := b.CreateAPI(context.Background(), apigatewayv2.CreateAPIInput{
				Name:         "api",
				ProtocolType: tt.protocolType,
			})
			require.NoError(t, err)

			integrationType := "HTTP_PROXY"
			if tt.protocolType == "WEBSOCKET" {
				integrationType = "AWS"
			}

			intg, err := b.CreateIntegration(api.APIID, apigatewayv2.CreateIntegrationInput{
				IntegrationType: integrationType,
				IntegrationURI:  "https://example.com",
			})
			require.NoError(t, err)

			_, err = b.UpdateIntegration(api.APIID, intg.IntegrationID, apigatewayv2.UpdateIntegrationInput{
				TimeoutInMillis: tt.timeoutMs,
			})

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, apigatewayv2.ErrBadRequest)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// Test_CreateIntegration_ConnectionType proves ConnectionType defaults to
// INTERNET when unset (matching the AWS-documented default), is validated
// against the modeled enum (INTERNET, VPC_LINK), and requires a connectionId
// when VPC_LINK is specified. Before this fix ConnectionType had no default
// and no validation, so GetIntegration returned an empty string instead of
// "INTERNET" for the common case of an internet-routed integration.
func Test_CreateIntegration_ConnectionType(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name           string
		connectionType string
		connectionID   string
		wantConnType   string
		wantErr        bool
	}{
		{name: "unset_defaults_to_internet", connectionType: "", wantErr: false, wantConnType: "INTERNET"},
		{name: "explicit_internet", connectionType: "INTERNET", wantErr: false, wantConnType: "INTERNET"},
		{
			name: "vpc_link_with_connection_id", connectionType: "VPC_LINK", connectionID: "vpc-link-1",
			wantErr: false, wantConnType: "VPC_LINK",
		},
		{name: "vpc_link_missing_connection_id", connectionType: "VPC_LINK", wantErr: true},
		{name: "invalid_enum_value", connectionType: "BOGUS", wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := apigatewayv2.NewInMemoryBackend()

			api, err := b.CreateAPI(context.Background(), apigatewayv2.CreateAPIInput{
				Name:         "api",
				ProtocolType: "HTTP",
			})
			require.NoError(t, err)

			intg, err := b.CreateIntegration(api.APIID, apigatewayv2.CreateIntegrationInput{
				IntegrationType: "HTTP_PROXY",
				IntegrationURI:  "https://example.com",
				ConnectionType:  tc.connectionType,
				ConnectionID:    tc.connectionID,
			})

			if tc.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, apigatewayv2.ErrBadRequest)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.wantConnType, intg.ConnectionType)

			got, err := b.GetIntegration(api.APIID, intg.IntegrationID)
			require.NoError(t, err)
			assert.Equal(t, tc.wantConnType, got.ConnectionType)
		})
	}
}

// Test_Integration_TlsConfig proves the tlsConfig field (AWS-modeled for
// private integrations) round-trips through Create, Get, and Update. Before
// this fix the Integration model had no TLSConfig field at all, so any client
// setting it silently lost the value.
func Test_Integration_TlsConfig(t *testing.T) {
	t.Parallel()

	cases := []struct {
		createTLS       *apigatewayv2.IntegrationTLSConfig
		updateTLS       *apigatewayv2.IntegrationTLSConfig
		wantAfterUpdate *apigatewayv2.IntegrationTLSConfig
		name            string
	}{
		{
			name:            "create_with_tls_no_update",
			createTLS:       &apigatewayv2.IntegrationTLSConfig{ServerNameToVerify: "backend.example.com"},
			wantAfterUpdate: &apigatewayv2.IntegrationTLSConfig{ServerNameToVerify: "backend.example.com"},
		},
		{
			name:      "create_without_tls_then_add_via_update",
			createTLS: nil,
			updateTLS: &apigatewayv2.IntegrationTLSConfig{ServerNameToVerify: "updated.example.com"},
			wantAfterUpdate: &apigatewayv2.IntegrationTLSConfig{
				ServerNameToVerify: "updated.example.com",
			},
		},
		{
			name:            "create_without_tls_stays_nil",
			createTLS:       nil,
			wantAfterUpdate: nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := apigatewayv2.NewInMemoryBackend()

			api, err := b.CreateAPI(context.Background(), apigatewayv2.CreateAPIInput{
				Name:         "api",
				ProtocolType: "HTTP",
			})
			require.NoError(t, err)

			intg, err := b.CreateIntegration(api.APIID, apigatewayv2.CreateIntegrationInput{
				IntegrationType: "HTTP_PROXY",
				IntegrationURI:  "https://example.com",
				ConnectionType:  "VPC_LINK",
				ConnectionID:    "vpc-link-1",
				TLSConfig:       tc.createTLS,
			})
			require.NoError(t, err)
			assert.Equal(t, tc.createTLS, intg.TLSConfig)

			if tc.updateTLS != nil {
				_, err = b.UpdateIntegration(api.APIID, intg.IntegrationID, apigatewayv2.UpdateIntegrationInput{
					TLSConfig: tc.updateTLS,
				})
				require.NoError(t, err)
			}

			got, err := b.GetIntegration(api.APIID, intg.IntegrationID)
			require.NoError(t, err)
			assert.Equal(t, tc.wantAfterUpdate, got.TLSConfig)

			// Mutating the caller's struct after Create/Update must not alias
			// backend state (deep-copy proof).
			if tc.createTLS != nil {
				tc.createTLS.ServerNameToVerify = "mutated"

				afterMutate, getErr := b.GetIntegration(api.APIID, intg.IntegrationID)
				require.NoError(t, getErr)
				assert.NotEqual(t, "mutated", func() string {
					if afterMutate.TLSConfig == nil {
						return ""
					}

					return afterMutate.TLSConfig.ServerNameToVerify
				}())
			}
		})
	}
}

// Test_Stage_ClientCertificateID proves the clientCertificateId field
// (AWS-modeled for WebSocket API stages) round-trips through CreateStage,
// GetStage, and UpdateStage. Before this fix the Stage model had no
// ClientCertificateID field, so it was silently dropped.
func Test_Stage_ClientCertificateID(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name            string
		createCertID    string
		updateCertID    string
		wantAfterUpdate string
	}{
		{name: "set_on_create_no_update", createCertID: "cert-abc", wantAfterUpdate: "cert-abc"},
		{
			name: "set_on_create_then_replaced", createCertID: "cert-abc",
			updateCertID: "cert-xyz", wantAfterUpdate: "cert-xyz",
		},
		{name: "unset_stays_empty", createCertID: "", wantAfterUpdate: ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := apigatewayv2.NewInMemoryBackend()

			api, err := b.CreateAPI(context.Background(), apigatewayv2.CreateAPIInput{
				Name:         "api",
				ProtocolType: "WEBSOCKET",
			})
			require.NoError(t, err)

			stage, err := b.CreateStage(api.APIID, apigatewayv2.CreateStageInput{
				StageName:           "prod",
				ClientCertificateID: tc.createCertID,
			})
			require.NoError(t, err)
			assert.Equal(t, tc.createCertID, stage.ClientCertificateID)

			if tc.updateCertID != "" {
				_, err = b.UpdateStage(api.APIID, "prod", apigatewayv2.UpdateStageInput{
					ClientCertificateID: tc.updateCertID,
				})
				require.NoError(t, err)
			}

			got, err := b.GetStage(api.APIID, "prod")
			require.NoError(t, err)
			assert.Equal(t, tc.wantAfterUpdate, got.ClientCertificateID)
		})
	}
}

// Test_DomainName_ArnAndMutualTLS proves CreateDomainName populates a
// well-formed domainNameArn and round-trips mutualTlsAuthentication, and that
// UpdateDomainName can replace the mTLS configuration. Before this fix
// DomainName had neither field, so both were silently dropped.
func Test_DomainName_ArnAndMutualTLS(t *testing.T) {
	t.Parallel()

	ctx := awsmeta.Set(context.Background(), &awsmeta.Metadata{
		Account:   "555566667777",
		Region:    "eu-west-1",
		Partition: "aws",
	})

	b := apigatewayv2.NewInMemoryBackend()

	dn, err := b.CreateDomainName(ctx, apigatewayv2.CreateDomainNameInput{
		DomainNameValue: "api.example.com",
		MutualTLSAuthentication: &apigatewayv2.MutualTLSAuthentication{
			TruststoreURI:     "s3://bucket/truststore.pem",
			TruststoreVersion: "v1",
		},
	})
	require.NoError(t, err)

	assert.Equal(t, "arn:aws:apigateway:eu-west-1::/domainnames/api.example.com", dn.DomainNameArn)
	require.NotNil(t, dn.MutualTLSAuthentication)
	assert.Equal(t, "s3://bucket/truststore.pem", dn.MutualTLSAuthentication.TruststoreURI)
	assert.Equal(t, "v1", dn.MutualTLSAuthentication.TruststoreVersion)
	assert.Empty(t, dn.MutualTLSAuthentication.TruststoreWarnings)

	updated, err := b.UpdateDomainName("api.example.com", apigatewayv2.UpdateDomainNameInput{
		MutualTLSAuthentication: &apigatewayv2.MutualTLSAuthentication{
			TruststoreURI:     "s3://bucket/truststore-v2.pem",
			TruststoreVersion: "v2",
		},
	})
	require.NoError(t, err)
	require.NotNil(t, updated.MutualTLSAuthentication)
	assert.Equal(t, "s3://bucket/truststore-v2.pem", updated.MutualTLSAuthentication.TruststoreURI)

	// DomainNameArn is stable across updates.
	assert.Equal(t, dn.DomainNameArn, updated.DomainNameArn)
}

// Test_StageTags proves Stage resources can be tagged/untagged via their own
// nested ARN ("arn:.../apis/{apiId}/stages/{stageName}"), matching real API
// Gateway v2 (Stage responses carry a Tags field). Before this fix
// TagResource/UntagResource/GetTags only recognised single-segment resources
// (apis, vpclinks, domainnames) so a stage ARN silently resolved to the wrong
// resource type and 500'd instead of tagging the stage.
func Test_StageTags(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()

	api, err := b.CreateAPI(context.Background(), apigatewayv2.CreateAPIInput{
		Name:         "api",
		ProtocolType: "HTTP",
	})
	require.NoError(t, err)

	_, err = b.CreateStage(api.APIID, apigatewayv2.CreateStageInput{StageName: "prod"})
	require.NoError(t, err)

	stageARN := "arn:aws:apigateway:us-east-1::/apis/" + api.APIID + "/stages/prod"

	require.NoError(t, b.TagResource(stageARN, map[string]string{"env": "prod", "team": "core"}))

	tags, err := b.GetTags(stageARN)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"env": "prod", "team": "core"}, tags)

	stage, err := b.GetStage(api.APIID, "prod")
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"env": "prod", "team": "core"}, stage.Tags)

	require.NoError(t, b.UntagResource(stageARN, []string{"team"}))

	tags, err = b.GetTags(stageARN)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"env": "prod"}, tags)

	// Not-found cases surface the correct AWS error, not a 500.
	_, err = b.GetTags("arn:aws:apigateway:us-east-1::/apis/" + api.APIID + "/stages/missing")
	require.ErrorIs(t, err, apigatewayv2.ErrStageNotFound)

	_, err = b.GetTags("arn:aws:apigateway:us-east-1::/apis/nonexistent/stages/prod")
	require.ErrorIs(t, err, apigatewayv2.ErrAPINotFound)
}
