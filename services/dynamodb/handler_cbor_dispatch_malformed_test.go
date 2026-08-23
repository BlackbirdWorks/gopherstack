package dynamodb_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	dynamodbsdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	smithy "github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

// setDynamoDBCBORContentType injects an application/x-amz-cbor-1.1
// Content-Type header after signing. aws-sdk-go-v2/service/dynamodb never
// sends this content type itself (the CBOR wire option is used by other
// language SDKs, e.g. Node.js), so this header rewrite is the injection that
// forces gopherstack down DynamoDBHandler.handleCBORRequest -- the sanctioned
// "smithy middleware corrupting the request after signing" proof technique
// for a path no legitimately-constructed aws-sdk-go-v2 request can otherwise
// reach (gopherstack-wlo1), mirroring kinesis's setCBORContentType
// (services/kinesis/handler_oversized_body_test.go).
func setDynamoDBCBORContentType() func(*middleware.Stack) error {
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
// DynamoDB client's PutItem, with its Content-Type rewritten to the CBOR
// wire type post-signing, and an item attribute large enough to push the
// request body past httputils.MaxRequestBodyBytes. Before this fix,
// handleCBORRequest's ReadBody-failure branch (cbor.go) wrote a bare
// "internal server error" text/plain body, unlike the main dispatch path's
// equivalent failure (handler.go's writeDynamoDBDispatchError, fixed by
// c6554e9f8) -- so the real SDK's JSON-RPC 1.0 error decoder
// (aws-sdk-go-v2@v1.63.1 deserializers.go, which always JSON-decodes the
// error body regardless of the request's content type) saw plain text it
// cannot parse and surfaced smithy.GenericAPIError{Code:"UnknownError"}
// instead of InternalFailure (gopherstack-wlo1).
func TestHandleCBORRequest_OversizedBodySurfacesInternalFailure(t *testing.T) {
	t.Parallel()

	client := newTestDynamoDBClientWithMiddleware(t, dynamodb.NewHandler(dynamodb.NewInMemoryDB()),
		setDynamoDBCBORContentType())

	huge := string(bytes.Repeat([]byte("x"), int(httputils.MaxRequestBodyBytes+1)))

	_, err := client.PutItem(t.Context(), &dynamodbsdk.PutItemInput{
		TableName: aws.String("oversized-body-table"),
		Item: map[string]ddbtypes.AttributeValue{
			"pk":   &ddbtypes.AttributeValueMemberS{Value: "oversized-body-key"},
			"data": &ddbtypes.AttributeValueMemberS{Value: huge},
		},
	}, func(o *dynamodbsdk.Options) { o.RetryMaxAttempts = 1 })
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "InternalFailure", apiErr.ErrorCode())
	assert.NotEqual(t, "UnknownError", apiErr.ErrorCode())
}
