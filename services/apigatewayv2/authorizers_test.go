package apigatewayv2_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
)

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
				Name: aws.String("updated-name"),
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
		Name:                         aws.String("new-auth"),
		AuthorizerType:               "REQUEST",
		AuthorizerURI:                aws.String("https://auth.example.com"),
		IdentitySource:               []string{"$request.header.Authorization"},
		AuthorizerCredentialsArn:     aws.String("arn:aws:iam::123:role/role"),
		AuthorizerResultTTLInSeconds: aws.Int32(300),
	})
	require.NoError(t, err)
	assert.Equal(t, "new-auth", updated.Name)
	assert.Equal(t, "REQUEST", updated.AuthorizerType)
	assert.Equal(t, int32(300), updated.AuthorizerResultTTLInSeconds)
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
