package cleanrooms_test

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	cleanroomssdk "github.com/aws/aws-sdk-go-v2/service/cleanrooms"
	smithy "github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/cleanrooms"
)

// corruptListCollaborationsPath rewrites ListCollaborations' outgoing
// request path from "/collaborations" to "/collaborations/does-not-exist/
// nowhere", after signing. That still starts with "/collaborations" (so
// Handler.RouteMatcher's HasPrefix check keeps routing the request to this
// package's Handler), but matches none of classifyPath's method/path
// cases, landing in Handler()'s own dispatch-miss fallback -- the same
// white-box category as securityhub's analogous fix (a98561767).
func corruptListCollaborationsPath(stack *middleware.Stack) error {
	return stack.Finalize.Add(
		middleware.FinalizeMiddlewareFunc("CorruptListCollaborationsPath", func(
			ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler,
		) (middleware.FinalizeOutput, middleware.Metadata, error) {
			if req, ok := in.Request.(*smithyhttp.Request); ok {
				req.URL.Path = "/collaborations/does-not-exist/nowhere"
			}

			return next.HandleFinalize(ctx, in)
		}),
		middleware.Before,
	)
}

// TestHandler_UnrecognisedPathSurfacesResourceNotFoundException drives a
// real Clean Rooms client's ListCollaborations through a rewritten,
// unrecognised request path. Before this fix, Handler()'s dispatch-miss
// fallback (handler.go) wrote a bare "not found" text/plain body --
// cleanrooms is restjson1 (services/_PROTOCOLS.md), whose deserializer
// (aws-sdk-go-v2@v1.49.4 aws/protocol/restjson.GetErrorInfo) parses __type/
// code/message from a JSON body; plain text doesn't decode, so a real
// client saw smithy.GenericAPIError{Code:"UnknownError"} instead of a typed
// error (gopherstack-wlo1).
func TestHandler_UnrecognisedPathSurfacesResourceNotFoundException(t *testing.T) {
	t.Parallel()

	h := cleanrooms.NewHandler(cleanrooms.NewInMemoryBackend("123456789012", "us-east-1"))
	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	client := cleanroomssdk.NewFromConfig(cfg, func(o *cleanroomssdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
		o.APIOptions = append(o.APIOptions, corruptListCollaborationsPath)
	})

	_, err = client.ListCollaborations(t.Context(), &cleanroomssdk.ListCollaborationsInput{},
		func(o *cleanroomssdk.Options) { o.RetryMaxAttempts = 1 })
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "ResourceNotFoundException", apiErr.ErrorCode())
	assert.NotEqual(t, "UnknownError", apiErr.ErrorCode())
}
