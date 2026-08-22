package apigateway_test

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	apigatewaysdk "github.com/aws/aws-sdk-go-v2/service/apigateway"
	smithy "github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/apigateway"
)

// newTestAPIGatewayClientWithMiddleware is newTestAPIGatewayClient plus an
// extra middleware installed on the outgoing request, so tests can force
// gopherstack down a path no legitimately-constructed SDK input can reach.
// The real apigatewaysdk client never sends X-Amz-Target at all (its wire
// protocol is REST-JSON1, not the JSON-RPC-style dispatch
// Handler.handleJSONProtocol implements for the "APIGateway." target
// prefix) -- setting that header is itself the injection.
func newTestAPIGatewayClientWithMiddleware(
	t *testing.T,
	h *apigateway.Handler,
	inject func(*middleware.Stack) error,
) *apigatewaysdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(tagsRTRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return apigatewaysdk.NewFromConfig(cfg, func(o *apigatewaysdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
		o.APIOptions = append(o.APIOptions, inject)
	})
}

// setXAmzTarget injects an X-Amz-Target header after signing. A value
// prefixed "APIGateway." satisfies Handler.RouteMatcher's header check
// (services/apigateway/handler.go:261) regardless of the request's real
// REST path, and satisfies Handler()'s "!hasPrefix" guard so it dispatches
// into handleJSONProtocol instead of handleRESTAPI.
func setXAmzTarget(value string) func(*middleware.Stack) error {
	return func(stack *middleware.Stack) error {
		return stack.Finalize.Add(
			middleware.FinalizeMiddlewareFunc("SetXAmzTarget", func(
				ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler,
			) (middleware.FinalizeOutput, middleware.Metadata, error) {
				if req, ok := in.Request.(*smithyhttp.Request); ok {
					req.Header.Set("X-Amz-Target", value)
				}

				return next.HandleFinalize(ctx, in)
			}),
			middleware.Before,
		)
	}
}

// TestHandleJSONProtocol_MalformedXAmzTargetSurfacesUnknownOperationException
// drives a real apigateway client with an injected X-Amz-Target that keeps
// the "APIGateway." prefix RouteMatcher and Handler() require to select
// handleJSONProtocol, but splits into 3 dot-separated parts rather than the
// 2 that branch expects. Before this fix, that branch wrote a bare
// "Invalid X-Amz-Target" text/plain body -- the JSON-RPC family's shared
// error decoder (aws-sdk-go-v2@v1.43.4 aws/protocol/restjson.GetErrorInfo)
// reads __type/code/message from a JSON body, so the client saw
// smithy.GenericAPIError{Code:"UnknownError"} instead of the real code, even
// though handleError's own unknown-operation branch (handler.go:806) was
// already correctly typed -- the same asymmetry as
// iot/vpclattice/medialive/mediatailor/ssm/dynamodb (gopherstack-wlo1).
func TestHandleJSONProtocol_MalformedXAmzTargetSurfacesUnknownOperationException(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	client := newTestAPIGatewayClientWithMiddleware(t, apigateway.NewHandler(backend),
		setXAmzTarget("APIGateway.Foo.Bar"))

	// CreateRestApi (unlike GetRestApis) genuinely sends POST, so this
	// isolates the len(parts)!=2 branch from the "non-POST" branch below.
	_, err := client.CreateRestApi(t.Context(), &apigatewaysdk.CreateRestApiInput{Name: aws.String("x")})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "UnknownOperationException", apiErr.ErrorCode())
	assert.NotEqual(t, "UnknownError", apiErr.ErrorCode())
}

// TestHandleJSONProtocol_NonPOSTSurfacesUnknownOperationException drives a
// real apigateway client (GetRestApis, which genuinely sends GET) with an
// injected valid two-part "APIGateway." target, reaching
// handleJSONProtocol's "non-POST" branch, which had the same bare-text bug.
func TestHandleJSONProtocol_NonPOSTSurfacesUnknownOperationException(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	client := newTestAPIGatewayClientWithMiddleware(t, apigateway.NewHandler(backend),
		setXAmzTarget("APIGateway.GetRestApis"))

	_, err := client.GetRestApis(t.Context(), nil)
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "UnknownOperationException", apiErr.ErrorCode())
	assert.NotEqual(t, "UnknownError", apiErr.ErrorCode())
}
