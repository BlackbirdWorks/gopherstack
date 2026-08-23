package mediapackage_test

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	mediapackagesdk "github.com/aws/aws-sdk-go-v2/service/mediapackage"
	smithy "github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/mediapackage"
)

// newErrorInjectingMediaPackageClient is newTestMediaPackageClient plus an
// extra middleware installed on the outgoing request, so tests can force
// gopherstack down a path no legitimately-constructed SDK input can reach
// (a malformed body, or a request path no route recognises).
func newErrorInjectingMediaPackageClient(
	t *testing.T,
	h *mediapackage.Handler,
	inject func(*middleware.Stack) error,
) *mediapackagesdk.Client {
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

	return mediapackagesdk.NewFromConfig(cfg, func(o *mediapackagesdk.Options) {
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

// corruptRequestPath rewrites the outgoing request to
// GET /channels/{id}/gopherstack-test-bogus-subresource, a path RouteMatcher
// still claims (it's under the SigV4-signed /channels/ prefix) but
// classifyChannelSubOp doesn't recognise, so a genuine client call reaches
// handleREST's "unknown operation" dispatch-failure path.
func corruptRequestPath(stack *middleware.Stack) error {
	return stack.Finalize.Add(
		middleware.FinalizeMiddlewareFunc("CorruptRequestPath", func(
			ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler,
		) (middleware.FinalizeOutput, middleware.Metadata, error) {
			if req, ok := in.Request.(*smithyhttp.Request); ok {
				req.URL.Path = "/channels/test-id/gopherstack-test-bogus-subresource"
			}

			return next.HandleFinalize(ctx, in)
		}),
		middleware.Before,
	)
}

// TestCreateChannel_DuplicateIDSurfacesUnprocessableEntityException drives
// CreateChannel twice with the same Id through a real SDK client. Before
// this fix, mapError's AlreadyExists branch called the untyped jsonError
// helper, which never set __type, so restjson.GetErrorInfo had nothing to
// read and the conflict deserialized client-side as UnknownError even
// though the NotFound branch right next to it was already correct
// (gopherstack-wlo1).
func TestCreateChannel_DuplicateIDSurfacesUnprocessableEntityException(t *testing.T) {
	t.Parallel()

	backend := mediapackage.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestMediaPackageClient(t, mediapackage.NewHandler(backend))

	_, err := client.CreateChannel(t.Context(), &mediapackagesdk.CreateChannelInput{
		Id: aws.String("dup-channel"),
	})
	require.NoError(t, err)

	_, err = client.CreateChannel(t.Context(), &mediapackagesdk.CreateChannelInput{
		Id: aws.String("dup-channel"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "UnprocessableEntityException", apiErr.ErrorCode())
}

// TestCreateChannel_EmptyIDSurfacesUnprocessableEntityException drives
// CreateChannel with an explicit empty (not nil) Id -- validateOpCreateChannelInput
// (validators.go:679-692) only rejects a nil Id client-side, so the empty
// string reaches the server and exercises mapError's InvalidParameter
// branch, which had the same untyped-jsonError bug as AlreadyExists.
func TestCreateChannel_EmptyIDSurfacesUnprocessableEntityException(t *testing.T) {
	t.Parallel()

	backend := mediapackage.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestMediaPackageClient(t, mediapackage.NewHandler(backend))

	_, err := client.CreateChannel(t.Context(), &mediapackagesdk.CreateChannelInput{
		Id: aws.String(""),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "UnprocessableEntityException", apiErr.ErrorCode())
}

// TestCreateChannel_MalformedBodySurfacesUnprocessableEntityException
// drives CreateChannel with a corrupted request body through a real SDK
// client, exercising handleREST's "invalid JSON body" path.
func TestCreateChannel_MalformedBodySurfacesUnprocessableEntityException(t *testing.T) {
	t.Parallel()

	backend := mediapackage.NewInMemoryBackend("000000000000", "us-east-1")
	client := newErrorInjectingMediaPackageClient(t, mediapackage.NewHandler(backend), corruptRequestBody)

	_, err := client.CreateChannel(t.Context(), &mediapackagesdk.CreateChannelInput{
		Id: aws.String("test-channel"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "UnprocessableEntityException", apiErr.ErrorCode())
}

// TestCreateChannel_UnrecognisedRouteSurfacesNotFoundException drives a
// rewritten, unrecognised request path through a real SDK client,
// exercising handleREST's "unknown operation" path.
func TestCreateChannel_UnrecognisedRouteSurfacesNotFoundException(t *testing.T) {
	t.Parallel()

	backend := mediapackage.NewInMemoryBackend("000000000000", "us-east-1")
	client := newErrorInjectingMediaPackageClient(t, mediapackage.NewHandler(backend), corruptRequestPath)

	_, err := client.CreateChannel(t.Context(), &mediapackagesdk.CreateChannelInput{
		Id: aws.String("test-channel"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "NotFoundException", apiErr.ErrorCode())
}
