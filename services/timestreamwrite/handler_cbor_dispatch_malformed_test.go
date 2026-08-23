package timestreamwrite_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	timestreamwritesdk "github.com/aws/aws-sdk-go-v2/service/timestreamwrite"
	twtypes "github.com/aws/aws-sdk-go-v2/service/timestreamwrite/types"
	smithy "github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/timestreamwrite"
)

// newTestTimestreamWriteClientWithMiddleware is
// newTestTimestreamWriteSDKClient plus an extra middleware installed on the
// outgoing request, so tests can force gopherstack down a path no
// legitimately-constructed SDK input can otherwise reach.
func newTestTimestreamWriteClientWithMiddleware(
	t *testing.T,
	h *timestreamwrite.Handler,
	inject func(*middleware.Stack) error,
) *timestreamwritesdk.Client {
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

	return timestreamwritesdk.NewFromConfig(cfg, func(o *timestreamwritesdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
		o.APIOptions = append(o.APIOptions, inject)
	})
}

// setCBORContentTypeAndMangleMethod rewrites the outgoing WriteRecords
// request post-signing to the CBOR content type with its HTTP method changed
// to GET. Handler.RouteMatcher (services/timestreamwrite/handler.go) routes
// purely on the X-Amz-Target header -- it never inspects the HTTP method --
// so the request still reaches this package's Handler; handleCBOR's own
// c.Request().Method != http.MethodPost check is what's meant to reject it.
// (A malformed-target request, by contrast, can't reach handleCBOR's own
// target-parsing branch at all: RouteMatcher already requires
// strings.TrimPrefix(target, targetPrefix) to be a non-empty, known
// supportedOps member before routing here, which guarantees handleCBOR's own
// identical SplitN(target, ".", 2) parse always succeeds too -- that branch
// is genuinely unreachable, not merely untested.) aws-sdk-go-v2/service/
// timestreamwrite never sends application/x-amz-cbor-1.1 itself (that wire
// option is used by other language SDKs), so this is the injection that
// forces gopherstack down Handler.handleCBOR -- the sanctioned "smithy
// middleware corrupting the request after signing" proof technique for a
// path no legitimately-constructed aws-sdk-go-v2 request can otherwise reach
// (gopherstack-wlo1). The DescribeEndpoints call the client issues first
// (endpoint discovery) is left alone by matching only on the WriteRecords
// target.
func setCBORContentTypeAndMangleMethod() func(*middleware.Stack) error {
	return func(stack *middleware.Stack) error {
		return stack.Finalize.Add(
			middleware.FinalizeMiddlewareFunc("MangleCBORMethod", func(
				ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler,
			) (middleware.FinalizeOutput, middleware.Metadata, error) {
				if req, ok := in.Request.(*smithyhttp.Request); ok &&
					req.Header.Get("X-Amz-Target") == "Timestream_20181101.WriteRecords" {
					req.Header.Set("Content-Type", service.ContentTypeCBOR)
					req.Method = http.MethodGet
				}

				return next.HandleFinalize(ctx, in)
			}),
			middleware.Before,
		)
	}
}

// TestHandleCBOR_WrongMethodSurfacesUnknownOperationException drives a real
// TimestreamWrite client's WriteRecords, with its Content-Type rewritten to
// the CBOR wire type and its HTTP method changed to GET post-signing,
// forcing gopherstack's handleCBOR into its own method-not-allowed branch
// (handler.go). Before this fix that branch wrote a bare "Method not
// allowed" text/plain body -- the real SDK's JSON-RPC 1.0 error decoder
// (aws-sdk-go-v2@v1.43.4 aws/protocol/restjson.GetErrorInfo) cannot parse
// plain text, so the client saw smithy.GenericAPIError{Code:"UnknownError"}
// instead of a typed error (gopherstack-wlo1).
func TestHandleCBOR_WrongMethodSurfacesUnknownOperationException(t *testing.T) {
	t.Parallel()

	backend := timestreamwrite.NewInMemoryBackend()
	client := newTestTimestreamWriteClientWithMiddleware(
		t, timestreamwrite.NewHandler(backend), setCBORContentTypeAndMangleMethod(),
	)

	_, err := client.WriteRecords(t.Context(), &timestreamwritesdk.WriteRecordsInput{
		DatabaseName: aws.String("malformed-target-db"),
		TableName:    aws.String("malformed-target-table"),
		Records: []twtypes.Record{
			{
				MeasureName:      aws.String("cpu"),
				MeasureValue:     aws.String("1"),
				MeasureValueType: twtypes.MeasureValueTypeDouble,
				Time:             aws.String("1"),
			},
		},
	}, func(o *timestreamwritesdk.Options) { o.RetryMaxAttempts = 1 })
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "UnknownOperationException", apiErr.ErrorCode())
	assert.NotEqual(t, "UnknownError", apiErr.ErrorCode())
}
