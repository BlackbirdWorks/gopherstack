package apigatewayv2_test

import (
	"context"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

func TestInMemoryBackend_EdgeCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "update_stage_api_not_found",
			run: func(t *testing.T) {
				t.Helper()
				b := apigatewayv2.NewInMemoryBackend()
				_, err := b.UpdateStage("bad-api", "prod", apigatewayv2.UpdateStageInput{})
				require.ErrorIs(t, err, apigatewayv2.ErrAPINotFound)
			},
		},
		{
			name: "update_route_api_not_found",
			run: func(t *testing.T) {
				t.Helper()
				b := apigatewayv2.NewInMemoryBackend()
				_, err := b.UpdateRoute("bad-api", "r1", apigatewayv2.UpdateRouteInput{})
				require.ErrorIs(t, err, apigatewayv2.ErrAPINotFound)
			},
		},
		{
			name: "update_integration_api_not_found",
			run: func(t *testing.T) {
				t.Helper()
				b := apigatewayv2.NewInMemoryBackend()
				_, err := b.UpdateIntegration("bad-api", "i1", apigatewayv2.UpdateIntegrationInput{})
				require.ErrorIs(t, err, apigatewayv2.ErrAPINotFound)
			},
		},
		{
			name: "delete_deployment_api_not_found",
			run: func(t *testing.T) {
				t.Helper()
				b := apigatewayv2.NewInMemoryBackend()
				err := b.DeleteDeployment("bad-api", "d1")
				require.ErrorIs(t, err, apigatewayv2.ErrAPINotFound)
			},
		},
		{
			name: "delete_authorizer_api_not_found",
			run: func(t *testing.T) {
				t.Helper()
				b := apigatewayv2.NewInMemoryBackend()
				err := b.DeleteAuthorizer("bad-api", "a1")
				require.ErrorIs(t, err, apigatewayv2.ErrAPINotFound)
			},
		},
		{
			name: "update_authorizer_api_not_found",
			run: func(t *testing.T) {
				t.Helper()
				b := apigatewayv2.NewInMemoryBackend()
				_, err := b.UpdateAuthorizer("bad-api", "a1", apigatewayv2.UpdateAuthorizerInput{})
				require.ErrorIs(t, err, apigatewayv2.ErrAPINotFound)
			},
		},
		{
			name: "get_stages_api_not_found",
			run: func(t *testing.T) {
				t.Helper()
				b := apigatewayv2.NewInMemoryBackend()
				_, err := b.GetStages("bad-api")
				require.ErrorIs(t, err, apigatewayv2.ErrAPINotFound)
			},
		},
		{
			name: "get_routes_api_not_found",
			run: func(t *testing.T) {
				t.Helper()
				b := apigatewayv2.NewInMemoryBackend()
				_, err := b.GetRoutes("bad-api")
				require.ErrorIs(t, err, apigatewayv2.ErrAPINotFound)
			},
		},
		{
			name: "get_integrations_api_not_found",
			run: func(t *testing.T) {
				t.Helper()
				b := apigatewayv2.NewInMemoryBackend()
				_, err := b.GetIntegrations("bad-api")
				require.ErrorIs(t, err, apigatewayv2.ErrAPINotFound)
			},
		},
		{
			name: "get_deployments_api_not_found",
			run: func(t *testing.T) {
				t.Helper()
				b := apigatewayv2.NewInMemoryBackend()
				_, err := b.GetDeployments("bad-api")
				require.ErrorIs(t, err, apigatewayv2.ErrAPINotFound)
			},
		},
		{
			name: "get_authorizers_api_not_found",
			run: func(t *testing.T) {
				t.Helper()
				b := apigatewayv2.NewInMemoryBackend()
				_, err := b.GetAuthorizers("bad-api")
				require.ErrorIs(t, err, apigatewayv2.ErrAPINotFound)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

func TestBackend_Reset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *apigatewayv2.InMemoryBackend)
		verify func(t *testing.T, b *apigatewayv2.InMemoryBackend)
		name   string
	}{
		{
			name: "clears_apis",
			setup: func(b *apigatewayv2.InMemoryBackend) {
				_, err := b.CreateAPI(
					context.Background(),
					apigatewayv2.CreateAPIInput{Name: "test", ProtocolType: "HTTP"},
				)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *apigatewayv2.InMemoryBackend) {
				t.Helper()
				apis, err := b.GetAPIs()
				require.NoError(t, err)
				assert.Empty(t, apis)
			},
		},
		{
			name: "clears_domain_names",
			setup: func(b *apigatewayv2.InMemoryBackend) {
				_, err := b.CreateDomainName(
					context.Background(),
					apigatewayv2.CreateDomainNameInput{DomainNameValue: "example.com"},
				)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *apigatewayv2.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateDomainName(
					context.Background(),
					apigatewayv2.CreateDomainNameInput{DomainNameValue: "example.com"},
				)
				require.NoError(t, err)
			},
		},
		{
			name:  "empty_backend_reset_is_safe",
			setup: func(_ *apigatewayv2.InMemoryBackend) {},
			verify: func(t *testing.T, b *apigatewayv2.InMemoryBackend) {
				t.Helper()
				apis, err := b.GetAPIs()
				require.NoError(t, err)
				assert.Empty(t, apis)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigatewayv2.NewInMemoryBackend()
			tt.setup(b)
			b.Reset()
			tt.verify(t, b)
		})
	}
}
