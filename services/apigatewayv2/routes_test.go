package apigatewayv2_test

import (
	"context"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

func TestInMemoryBackend_CreateRoute_ApiNotFound(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()

	_, err := b.CreateRoute("bad-api", apigatewayv2.CreateRouteInput{RouteKey: "GET /test"})
	require.ErrorIs(t, err, apigatewayv2.ErrAPINotFound)
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
