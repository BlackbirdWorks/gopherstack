package dynamodb_test

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	dynamodbsdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	smithy "github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

// newTestDynamoDBClientWithMiddleware is newTestDynamoDBClient plus an extra
// middleware installed on the outgoing request, so tests can force
// gopherstack down a path no legitimately-constructed SDK input can reach.
func newTestDynamoDBClientWithMiddleware(
	t *testing.T,
	h *dynamodb.DynamoDBHandler,
	inject func(*middleware.Stack) error,
) *dynamodbsdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(ddbTagsRTRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return dynamodbsdk.NewFromConfig(cfg, func(o *dynamodbsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
		o.APIOptions = append(o.APIOptions, inject)
	})
}

// mangleDynamoDBTarget rewrites the outgoing X-Amz-Target header to
// targetValue after signing. Kept prefixed with "DynamoDB_" so
// DynamoDBHandler.RouteMatcher (services/dynamodb/handler.go, a bare
// strings.HasPrefix check) still routes the request to dynamodb -- only
// Handler()'s own len(parts)==2 validation is meant to reject it. No
// legitimately-constructed SDK call ever sends a target without exactly one
// dot, so this is the only way a real client reaches that branch.
func mangleDynamoDBTarget(targetValue string) func(*middleware.Stack) error {
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

// forceDynamoDBMethod rewrites the outgoing request method after signing.
// The real SDK always sends POST for JSON-RPC operations, so this is the
// only way a real client reaches Handler()'s "non-POST" branch.
// DynamoDBHandler's RouteMatcher only inspects X-Amz-Target (not method), so
// the mangled request still routes to dynamodb before it's rejected.
func forceDynamoDBMethod(method string) func(*middleware.Stack) error {
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

// TestHandler_MalformedXAmzTargetSurfacesUnknownOperationException drives a
// real dynamodb client with X-Amz-Target rewritten to a value that keeps the
// "DynamoDB_" prefix RouteMatcher requires but has no dot. Before this fix,
// Handler()'s own len(parts)==2 check wrote a bare "Invalid X-Amz-Target"
// text/plain body -- the JSON-RPC 1.0 error decoder (aws-sdk-go-v2@v1.43.4
// aws/protocol/restjson.GetErrorInfo) reads __type/message from a JSON body,
// so the client saw smithy.GenericAPIError{Code:"UnknownError"} instead of
// the real code, even though dynamodb's genuine unknown-operation path
// (handleError, handler.go:861) was already correctly typed -- the same
// asymmetry as iot/vpclattice/medialive/mediatailor (gopherstack-wlo1).
func TestHandler_MalformedXAmzTargetSurfacesUnknownOperationException(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	client := newTestDynamoDBClientWithMiddleware(t, dynamodb.NewHandler(backend),
		mangleDynamoDBTarget("DynamoDB_NoDotHere"))

	_, err := client.ListTables(t.Context(), nil)
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "UnknownOperationException", apiErr.ErrorCode())
	assert.NotEqual(t, "UnknownError", apiErr.ErrorCode())
}

// TestHandler_NonPOSTSurfacesUnknownOperationException drives a real
// dynamodb client whose request method is rewritten to PUT after signing,
// reaching Handler()'s "non-POST" branch, which had the same bare-text bug.
func TestHandler_NonPOSTSurfacesUnknownOperationException(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	client := newTestDynamoDBClientWithMiddleware(t, dynamodb.NewHandler(backend),
		forceDynamoDBMethod("PUT"))

	_, err := client.ListTables(t.Context(), nil)
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "UnknownOperationException", apiErr.ErrorCode())
	assert.NotEqual(t, "UnknownError", apiErr.ErrorCode())
}
