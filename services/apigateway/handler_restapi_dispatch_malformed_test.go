package apigateway_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	apigatewaysdk "github.com/aws/aws-sdk-go-v2/service/apigateway"
	smithy "github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigateway"
)

// corruptCreateRestAPIPath rewrites CreateRestApi's outgoing request path
// from "/restapis" down to "/restapis/does-not-exist/nowhere", after
// signing. That still starts with "/restapis" (so
// isAPIGWTopLevelRESTPath keeps RouteMatcher routing the request to this
// package's Handler), but matches none of parseAPIGWRESTPath's method/path
// cases, landing in handleRESTAPI's own dispatch-miss fallback -- the same
// white-box category as securityhub's analogous fix (a98561767).
func corruptCreateRestAPIPath(stack *middleware.Stack) error {
	return stack.Finalize.Add(
		middleware.FinalizeMiddlewareFunc("CorruptCreateRestApiPath", func(
			ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler,
		) (middleware.FinalizeOutput, middleware.Metadata, error) {
			if req, ok := in.Request.(*smithyhttp.Request); ok {
				req.URL.Path = "/restapis/does-not-exist/nowhere"
			}

			return next.HandleFinalize(ctx, in)
		}),
		middleware.Before,
	)
}

// TestHandleRESTAPI_UnrecognisedPathSurfacesUnknownOperationException drives
// a real apigateway client's CreateRestApi through a rewritten, unrecognised
// request path. Before this fix, handleRESTAPI's dispatch-miss fallback
// (handler.go) wrote a bare "not found" text/plain body -- apigateway is
// restjson1 (services/_PROTOCOLS.md), whose deserializer
// (aws-sdk-go-v2@v1.42.4 aws/protocol/restjson.GetErrorInfo) parses __type/
// code/message from a JSON body; plain text doesn't decode, so a real
// client saw smithy.GenericAPIError{Code:"UnknownError"} instead of a typed
// error (gopherstack-wlo1).
func TestHandleRESTAPI_UnrecognisedPathSurfacesUnknownOperationException(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	client := newTestAPIGatewayClientWithMiddleware(t, apigateway.NewHandler(backend), corruptCreateRestAPIPath)

	_, err := client.CreateRestApi(t.Context(), &apigatewaysdk.CreateRestApiInput{
		Name: aws.String("unrecognised-path-api"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "UnknownOperationException", apiErr.ErrorCode())
	assert.NotEqual(t, "UnknownError", apiErr.ErrorCode())
}
