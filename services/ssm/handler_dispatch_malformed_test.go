package ssm_test

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	ssmsdk "github.com/aws/aws-sdk-go-v2/service/ssm"
	smithy "github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/ssm"
)

// newTestSSMClientWithMiddleware is newTestSSMClient plus an extra
// middleware installed on the outgoing request, so tests can force
// gopherstack down a path no legitimately-constructed SDK input can reach.
func newTestSSMClientWithMiddleware(
	t *testing.T,
	h *ssm.Handler,
	inject func(*middleware.Stack) error,
) *ssmsdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(ssmTagsRTRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return ssmsdk.NewFromConfig(cfg, func(o *ssmsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
		o.APIOptions = append(o.APIOptions, inject)
	})
}

// mangleXAmzTarget rewrites the outgoing X-Amz-Target header to targetValue
// after signing. Kept prefixed with "AmazonSSM" so ssm.Handler.RouteMatcher
// (services/ssm/handler.go, a bare strings.HasPrefix check) still routes the
// request to ssm -- only service.HandleTarget's own len(parts)==2 validation
// is meant to reject it. No legitimately-constructed SDK call ever sends a
// target without exactly one dot, so this is the only way a real client
// reaches that branch.
func mangleXAmzTarget(targetValue string) func(*middleware.Stack) error {
	return func(stack *middleware.Stack) error {
		return stack.Finalize.Add(
			middleware.FinalizeMiddlewareFunc("MangleXAmzTarget", func(
				ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler,
			) (middleware.FinalizeOutput, middleware.Metadata, error) {
				if req, ok := in.Request.(*smithyhttp.Request); ok {
					req.Header.Set("X-Amz-Target", targetValue)
				}

				return next.HandleFinalize(ctx, in)
			}),
			middleware.Before,
		)
	}
}

// forceMethod rewrites the outgoing request method after signing. The real
// SDK always sends POST for JSON-RPC operations, so this is the only way a
// real client reaches HandleTarget's "non-POST" branch. ssm.Handler's
// RouteMatcher only inspects X-Amz-Target (not method), so the mangled
// request still routes to ssm before HandleTarget rejects it.
func forceMethod(method string) func(*middleware.Stack) error {
	return func(stack *middleware.Stack) error {
		return stack.Finalize.Add(
			middleware.FinalizeMiddlewareFunc("ForceMethod", func(
				ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler,
			) (middleware.FinalizeOutput, middleware.Metadata, error) {
				if req, ok := in.Request.(*smithyhttp.Request); ok {
					req.Method = method
				}

				return next.HandleFinalize(ctx, in)
			}),
			middleware.Before,
		)
	}
}

// TestHandleTarget_MalformedXAmzTargetSurfacesUnknownOperationException drives
// a real ssm client with X-Amz-Target rewritten to a value that keeps the
// "AmazonSSM" prefix ssm's RouteMatcher requires but has no dot, so it
// reaches service.HandleTarget's own len(parts)==2 check. Before
// gopherstack-wlo1's pkgs/service fix, that branch wrote a bare
// "Invalid X-Amz-Target" text/plain body -- the JSON-RPC family's shared
// error decoder (aws-sdk-go-v2@v1.43.4 aws/protocol/restjson.GetErrorInfo,
// which every awsjson1.0/1.1 deserializeOpError function calls) reads
// __type/code/message from a JSON body, so the client saw
// smithy.GenericAPIError{Code:"UnknownError"} instead of the real code. This
// branch is shared by all ~54 services that call service.HandleTarget, not
// just ssm.
func TestHandleTarget_MalformedXAmzTargetSurfacesUnknownOperationException(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClientWithMiddleware(t, ssm.NewHandler(backend),
		mangleXAmzTarget("AmazonSSMNoDotHere"))

	_, err := client.DescribeInstanceInformation(t.Context(), nil)
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "UnknownOperationException", apiErr.ErrorCode())
	assert.NotEqual(t, "UnknownError", apiErr.ErrorCode())
}

// TestHandleTarget_NonPOSTSurfacesUnknownOperationException drives a real
// ssm client whose request method is rewritten to PUT after signing.
// ssm.Handler.RouteMatcher only inspects X-Amz-Target, so the request still
// routes to ssm and reaches service.HandleTarget's "non-POST" branch, which
// had the same bare-text bug as the malformed-target branch above.
func TestHandleTarget_NonPOSTSurfacesUnknownOperationException(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClientWithMiddleware(t, ssm.NewHandler(backend),
		forceMethod("PUT"))

	_, err := client.DescribeInstanceInformation(t.Context(), nil)
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "UnknownOperationException", apiErr.ErrorCode())
	assert.NotEqual(t, "UnknownError", apiErr.ErrorCode())
}
