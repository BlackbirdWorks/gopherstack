package mediatailor_test

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	mediatailorsdk "github.com/aws/aws-sdk-go-v2/service/mediatailor"
	smithy "github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/mediatailor"
)

// newErrorInjectingMediaTailorClient is newTestMediaTailorClient plus an
// extra middleware installed on the outgoing request, so tests can force
// gopherstack down a path no legitimately-constructed SDK input can reach
// (a malformed body, or a request no route recognises).
func newErrorInjectingMediaTailorClient(
	t *testing.T,
	h *mediatailor.Handler,
	inject func(*middleware.Stack) error,
) *mediatailorsdk.Client {
	t.Helper()

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

	return mediatailorsdk.NewFromConfig(cfg, func(o *mediatailorsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
		o.APIOptions = append(o.APIOptions, inject)
	})
}

// corruptRequestBody replaces the outgoing request body with invalid JSON
// after normal serialization. No real operation input can reach
// handleREST's "invalid JSON body" path -- the SDK always marshals valid
// JSON -- so this is the only way to drive it through a genuine client.
func corruptRequestBody(stack *middleware.Stack) error {
	return stack.Serialize.Add(
		middleware.SerializeMiddlewareFunc("CorruptJSONBody", func(
			ctx context.Context, in middleware.SerializeInput, next middleware.SerializeHandler,
		) (middleware.SerializeOutput, middleware.Metadata, error) {
			if req, ok := in.Request.(*smithyhttp.Request); ok {
				corrupted, err := req.SetStream(strings.NewReader("{not valid json"))
				if err != nil {
					return middleware.SerializeOutput{}, middleware.Metadata{}, err
				}

				corrupted.ContentLength = int64(len("{not valid json"))
				in.Request = corrupted
			}

			return next.HandleSerialize(ctx, in)
		}),
		middleware.After,
	)
}

// corruptRequestMethod rewrites the outgoing request to
// PATCH /playbackConfiguration/{name}, a combination isMediaTailorPath
// accepts (so RouteMatcher still claims the request) but classifyPath does
// not recognise (only GET and DELETE are modeled there), so a genuine
// client call reaches handleREST's "unknown operation" dispatch-failure
// path.
func corruptRequestMethod(stack *middleware.Stack) error {
	return stack.Finalize.Add(
		middleware.FinalizeMiddlewareFunc("CorruptRequestMethod", func(
			ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler,
		) (middleware.FinalizeOutput, middleware.Metadata, error) {
			if req, ok := in.Request.(*smithyhttp.Request); ok {
				req.Method = "PATCH"
				req.URL.Path = "/playbackConfiguration/gopherstack-test-config"
			}

			return next.HandleFinalize(ctx, in)
		}),
		middleware.Before,
	)
}

// TestPutPlaybackConfiguration_MalformedBodySurfacesBadRequestException
// drives PutPlaybackConfiguration with a corrupted request body through a
// real SDK client. Before this fix, handleREST's "invalid JSON body"
// response never set X-Amzn-Errortype, so it deserialized client-side as
// UnknownError (gopherstack-wlo1; same bug class as the sibling medialive
// and mediapackage services).
func TestPutPlaybackConfiguration_MalformedBodySurfacesBadRequestException(t *testing.T) {
	t.Parallel()

	backend := mediatailor.NewInMemoryBackend("000000000000", "us-east-1")
	client := newErrorInjectingMediaTailorClient(t, mediatailor.NewHandler(backend), corruptRequestBody)

	_, err := client.PutPlaybackConfiguration(t.Context(), &mediatailorsdk.PutPlaybackConfigurationInput{
		Name: aws.String("test-config"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "BadRequestException", apiErr.ErrorCode())
}

// TestPutPlaybackConfiguration_UnrecognisedRouteSurfacesNotFoundException
// drives a rewritten, unrecognised (method, path) pair through a real SDK
// client. Before this fix, handleREST's "unknown operation" response never
// set X-Amzn-Errortype, so it deserialized client-side as UnknownError
// (gopherstack-wlo1).
func TestPutPlaybackConfiguration_UnrecognisedRouteSurfacesNotFoundException(t *testing.T) {
	t.Parallel()

	backend := mediatailor.NewInMemoryBackend("000000000000", "us-east-1")
	client := newErrorInjectingMediaTailorClient(t, mediatailor.NewHandler(backend), corruptRequestMethod)

	_, err := client.PutPlaybackConfiguration(t.Context(), &mediatailorsdk.PutPlaybackConfigurationInput{
		Name: aws.String("test-config"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "NotFoundException", apiErr.ErrorCode())
}
