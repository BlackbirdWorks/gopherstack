package kinesis_test

import (
	"bytes"
	"context"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	kinesissdk "github.com/aws/aws-sdk-go-v2/service/kinesis"
	kinesissdktypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	smithy "github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/kinesis"
)

// setCBORContentType injects an application/x-amz-cbor-1.1 Content-Type
// header after signing. aws-sdk-go-v2/service/kinesis never sends this
// content type itself (its deserializers.go has no CBOR handling at all --
// the CBOR wire option is used by other language SDKs, e.g. Node.js), so
// this header rewrite is the injection that forces gopherstack down
// Handler.handleCBORRequest, mirroring the sanctioned "smithy middleware
// corrupting the request after signing" proof technique for a path no
// legitimately-constructed aws-sdk-go-v2 request can otherwise reach
// (gopherstack-o7gx).
func setCBORContentType() func(*middleware.Stack) error {
	return func(stack *middleware.Stack) error {
		return stack.Finalize.Add(
			middleware.FinalizeMiddlewareFunc("SetCBORContentType", func(
				ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler,
			) (middleware.FinalizeOutput, middleware.Metadata, error) {
				if req, ok := in.Request.(*smithyhttp.Request); ok {
					req.Header.Set("Content-Type", service.ContentTypeCBOR)
				}

				return next.HandleFinalize(ctx, in)
			}),
			middleware.Before,
		)
	}
}

// TestHandleCBORRequest_OversizedBodySurfacesInternalFailure drives a real
// Kinesis client's PutRecord, with its Content-Type rewritten to the CBOR
// wire type post-signing (see setCBORContentType), and a Data blob large
// enough to push the request body past httputils.MaxRequestBodyBytes.
// Before this fix, handleCBORRequest's ReadBody-failure branch (cbor.go)
// wrote a bare "internal server error" text/plain body -- inconsistent with
// its own sibling branch three lines below (the CBORToJSON decode failure),
// which was already correctly typed via service.JSONErrorResponse -- so the
// real SDK's JSON error decoder
// (aws-sdk-go-v2@v1.43.4 aws/protocol/restjson.GetErrorInfo) saw plain text
// it cannot parse and surfaced smithy.GenericAPIError{Code:"UnknownError"}
// instead (gopherstack-o7gx).
func TestHandleCBORRequest_OversizedBodySurfacesInternalFailure(t *testing.T) {
	t.Parallel()

	backend := kinesis.NewInMemoryBackend()
	h := kinesis.NewHandler(backend)

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	client := kinesissdk.NewFromConfig(cfg, func(o *kinesissdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
		o.APIOptions = append(o.APIOptions, setCBORContentType())
	})

	huge := bytes.Repeat([]byte("x"), int(httputils.MaxRequestBodyBytes+1))

	_, err = client.PutRecord(t.Context(), &kinesissdk.PutRecordInput{
		StreamName:   aws.String("oversized-body-stream"),
		Data:         huge,
		PartitionKey: aws.String("pk"),
	}, func(o *kinesissdk.Options) { o.RetryMaxAttempts = 1 })
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "InternalFailure", apiErr.ErrorCode())
	assert.NotEqual(t, "UnknownError", apiErr.ErrorCode())
}

// TestSubscribeToShard_OversizedBodySurfacesInternalFailureException drives
// a real Kinesis client's SubscribeToShard with a ConsumerARN large enough
// to push the request body past httputils.MaxRequestBodyBytes (a real
// client can legitimately send this; aws-sdk-go-v2 imposes no client-side
// cap). Before this fix, handleSubscribeToShardHTTP's ReadBody-failure
// branch (handler_consumers.go) wrote a bare "internal server error"
// text/plain body -- the JSON error decoder
// (aws-sdk-go-v2@v1.43.4 aws/protocol/restjson.GetErrorInfo) cannot parse
// plain text, so the client saw
// smithy.GenericAPIError{Code:"UnknownError"} instead of the typed
// InternalFailureException h.handleError's default branch already produces
// for every unmatched backend error (gopherstack-o7gx).
func TestSubscribeToShard_OversizedBodySurfacesInternalFailureException(t *testing.T) {
	t.Parallel()

	client := newTestKinesisClient(t, kinesis.NewHandler(kinesis.NewInMemoryBackend()))

	huge := aws.String("arn:aws:kinesis:us-east-1:123456789012:stream/x/consumer/" +
		string(bytes.Repeat([]byte("x"), int(httputils.MaxRequestBodyBytes+1))))

	_, err := client.SubscribeToShard(t.Context(), &kinesissdk.SubscribeToShardInput{
		ConsumerARN: huge,
		ShardId:     aws.String("shardId-000000000000"),
		StartingPosition: &kinesissdktypes.StartingPosition{
			Type: kinesissdktypes.ShardIteratorTypeLatest,
		},
	}, func(o *kinesissdk.Options) { o.RetryMaxAttempts = 1 })
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "InternalFailureException", apiErr.ErrorCode())
	assert.NotEqual(t, "UnknownError", apiErr.ErrorCode())
}
