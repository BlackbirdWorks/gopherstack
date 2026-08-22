package dynamodbstreams_test

import (
	"bytes"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	dynamodbstreamssdk "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	smithy "github.com/aws/smithy-go"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/dynamodb"
	"github.com/blackbirdworks/gopherstack/services/dynamodbstreams"
)

// newTestStreamsClient stands up the real aws-sdk-go-v2 DynamoDB Streams
// client against an httptest server running this package's Handler, wired
// through the same pkgs/service registry/router used in production.
func newTestStreamsClient(t *testing.T) *dynamodbstreamssdk.Client {
	t.Helper()

	backend := dynamodb.NewInMemoryDB()
	h := dynamodbstreams.NewHandler(backend)

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

	return dynamodbstreamssdk.NewFromConfig(cfg, func(o *dynamodbstreamssdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestHandler_OversizedBodySurfacesInternalServerError drives a real DynamoDB
// Streams client's GetRecords with a ShardIterator large enough to push the
// request body past httputils.MaxRequestBodyBytes (a real client can
// legitimately send this; aws-sdk-go-v2 imposes no client-side cap). Before
// this fix, Handler()'s ReadBody-failure branch wrote a bare
// "internal server error" text/plain body -- the awsjson1.0 error decoder
// (aws-sdk-go-v2@v1.43.4 dynamodbstreams@v1.36.4 deserializers.go:92-123,
// shared restjson.GetErrorInfo machinery) cannot parse plain text, so the
// client saw smithy.GenericAPIError{Code:"UnknownError"} instead of the
// typed InternalServerError handleError already produces for genuine backend
// errors of that type (gopherstack-o7gx).
func TestHandler_OversizedBodySurfacesInternalServerError(t *testing.T) {
	t.Parallel()

	client := newTestStreamsClient(t)

	huge := aws.String(string(bytes.Repeat([]byte("x"), int(httputils.MaxRequestBodyBytes+1))))

	_, err := client.GetRecords(t.Context(), &dynamodbstreamssdk.GetRecordsInput{
		ShardIterator: huge,
	}, func(o *dynamodbstreamssdk.Options) { o.RetryMaxAttempts = 1 })
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "InternalServerError", apiErr.ErrorCode())
	assert.NotEqual(t, "UnknownError", apiErr.ErrorCode())
}
