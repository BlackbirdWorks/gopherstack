package medialive_test

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	medialivesdk "github.com/aws/aws-sdk-go-v2/service/medialive"
	smithy "github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/medialive"
)

// TestDescribeChannel_UnknownChannelSurfacesNotFoundException drives
// DescribeChannel for a channel that doesn't exist through a real SDK
// client. Before this fix, respondErr never set X-Amzn-Errortype nor a body
// code/__type field -- the exact bug fixed for the sibling mediatailor
// service in f41d5b42f -- so restjson.GetErrorInfo had nothing to read and
// every error, including this one, deserialized client-side as a generic
// UnknownError instead of the modeled NotFoundException (gopherstack-ifni).
func TestDescribeChannel_UnknownChannelSurfacesNotFoundException(t *testing.T) {
	t.Parallel()

	backend := medialive.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestMediaLiveClient(t, medialive.NewHandler(backend))

	_, err := client.DescribeChannel(t.Context(), &medialivesdk.DescribeChannelInput{
		ChannelId: aws.String("no-such-channel"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "NotFoundException", apiErr.ErrorCode())
}

// newErrorInjectingMediaLiveClient is newTestMediaLiveClient plus an extra
// middleware installed on the outgoing request, so tests can force gopherstack
// down a path no legitimately-constructed SDK input can reach (a malformed
// body, or a request path no route recognises).
func newErrorInjectingMediaLiveClient(
	t *testing.T,
	h *medialive.Handler,
	inject func(*middleware.Stack) error,
) *medialivesdk.Client {
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

	return medialivesdk.NewFromConfig(cfg, func(o *medialivesdk.Options) {
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

// corruptRequestPath rewrites the outgoing request path to one classifyPath
// doesn't recognise, so a genuine client call reaches handleREST's "unknown
// operation" dispatch-failure path.
func corruptRequestPath(stack *middleware.Stack) error {
	return stack.Finalize.Add(
		middleware.FinalizeMiddlewareFunc("CorruptRequestPath", func(
			ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler,
		) (middleware.FinalizeOutput, middleware.Metadata, error) {
			if req, ok := in.Request.(*smithyhttp.Request); ok {
				req.URL.Path = "/prod/gopherstack-test-unrecognised-route"
			}

			return next.HandleFinalize(ctx, in)
		}),
		middleware.Before,
	)
}

// TestCreateChannel_MalformedBodySurfacesBadRequestException drives
// CreateChannel with a corrupted request body through a real SDK client.
// Before this fix, handleREST's "invalid JSON body" response never set
// X-Amzn-Errortype, so it deserialized client-side as UnknownError even
// though respondErr's genuine-error path (proven by the sibling
// DescribeChannel test above) was already correct (gopherstack-wlo1).
func TestCreateChannel_MalformedBodySurfacesBadRequestException(t *testing.T) {
	t.Parallel()

	backend := medialive.NewInMemoryBackend("000000000000", "us-east-1")
	client := newErrorInjectingMediaLiveClient(t, medialive.NewHandler(backend), corruptRequestBody)

	_, err := client.CreateChannel(t.Context(), &medialivesdk.CreateChannelInput{})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "BadRequestException", apiErr.ErrorCode())
}

// TestCreateChannel_UnrecognisedRouteSurfacesNotFoundException drives
// CreateChannel against a rewritten, unrecognised request path through a
// real SDK client. Before this fix, handleREST's "unknown operation"
// response never set X-Amzn-Errortype, so it deserialized client-side as
// UnknownError (gopherstack-wlo1).
func TestCreateChannel_UnrecognisedRouteSurfacesNotFoundException(t *testing.T) {
	t.Parallel()

	backend := medialive.NewInMemoryBackend("000000000000", "us-east-1")
	client := newErrorInjectingMediaLiveClient(t, medialive.NewHandler(backend), corruptRequestPath)

	_, err := client.CreateChannel(t.Context(), &medialivesdk.CreateChannelInput{
		Name: aws.String("test-channel"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "NotFoundException", apiErr.ErrorCode())
}
