package apigatewaymanagementapi_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	apigwmgmtsdk "github.com/aws/aws-sdk-go-v2/service/apigatewaymanagementapi"
	apigwmgmttypes "github.com/aws/aws-sdk-go-v2/service/apigatewaymanagementapi/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/apigatewaymanagementapi"
)

// newTestAPIGwMgmtSDKClient stands up the real aws-sdk-go-v2
// apigatewaymanagementapi client against an httptest server running this
// package's Handler, wired through the same pkgs/service registry/router
// used in production -- so a shape is verified by the real client's own
// deserializer, not gopherstack's own JSON tags.
func newTestAPIGwMgmtSDKClient(
	t *testing.T,
	h *apigatewaymanagementapi.Handler,
) *apigwmgmtsdk.Client {
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

	return apigwmgmtsdk.NewFromConfig(cfg, func(o *apigwmgmtsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestAPIGwMgmt_SDKRoundTrip drives all three real operations through the
// real SDK client, proving the standing checks from parity-principles.md:
// every member on GetConnectionOutput (including the nested Identity shape
// and the ISO 8601 timestamp encoding) round-trips through the real
// deserializer, and PostToConnection/DeleteConnection's void responses and
// path-parameter binding survive a real client round trip.
func TestAPIGwMgmt_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := apigatewaymanagementapi.NewInMemoryBackend()
	h := apigatewaymanagementapi.NewHandler(backend)
	client := newTestAPIGwMgmtSDKClient(t, h)

	const connID = "roundtrip-conn-1"

	created, err := backend.CreateConnection(connID, "203.0.113.5", "test-agent/1.0", nil)
	require.NoError(t, err)

	got, err := client.GetConnection(t.Context(), &apigwmgmtsdk.GetConnectionInput{
		ConnectionId: aws.String(connID),
	})
	require.NoError(t, err)
	require.NotNil(t, got.Identity)
	assert.Equal(t, created.SourceIP, aws.ToString(got.Identity.SourceIp))
	assert.Equal(t, created.UserAgent, aws.ToString(got.Identity.UserAgent))
	require.NotNil(t, got.ConnectedAt)
	require.NotNil(t, got.LastActiveAt)
	assert.WithinDuration(t, created.ConnectedAt, *got.ConnectedAt, 0)
	assert.WithinDuration(t, created.LastActiveAt, *got.LastActiveAt, 0)

	payload := []byte("hello over the wire")
	_, err = client.PostToConnection(t.Context(), &apigwmgmtsdk.PostToConnectionInput{
		ConnectionId: aws.String(connID),
		Data:         payload,
	})
	require.NoError(t, err)

	msgs := backend.GetMessages(connID)
	require.Len(t, msgs, 1)
	assert.Equal(t, payload, msgs[0].Data)

	_, err = client.DeleteConnection(t.Context(), &apigwmgmtsdk.DeleteConnectionInput{
		ConnectionId: aws.String(connID),
	})
	require.NoError(t, err)

	_, err = backend.GetConnection(connID)
	require.Error(t, err)
}

// TestAPIGwMgmt_SDKRoundTrip_GoneException proves an unknown connection id
// resolves to the real *types.GoneException on all three ops, not a generic
// error -- the client can only get the typed exception if the response sets
// X-Amzn-Errortype (or a matching body __type/code), since
// deserializeErrorGoneException never reads Message from the body.
func TestAPIGwMgmt_SDKRoundTrip_GoneException(t *testing.T) {
	t.Parallel()

	backend := apigatewaymanagementapi.NewInMemoryBackend()
	h := apigatewaymanagementapi.NewHandler(backend)
	client := newTestAPIGwMgmtSDKClient(t, h)

	const missingID = "does-not-exist"

	tests := []struct {
		call func() error
		name string
	}{
		{
			name: "GetConnection",
			call: func() error {
				_, err := client.GetConnection(t.Context(), &apigwmgmtsdk.GetConnectionInput{
					ConnectionId: aws.String(missingID),
				})

				return err
			},
		},
		{
			name: "PostToConnection",
			call: func() error {
				_, err := client.PostToConnection(t.Context(), &apigwmgmtsdk.PostToConnectionInput{
					ConnectionId: aws.String(missingID),
					Data:         []byte("x"),
				})

				return err
			},
		},
		{
			name: "DeleteConnection",
			call: func() error {
				_, err := client.DeleteConnection(t.Context(), &apigwmgmtsdk.DeleteConnectionInput{
					ConnectionId: aws.String(missingID),
				})

				return err
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := tc.call()
			require.Error(t, err)

			var goneErr *apigwmgmttypes.GoneException
			require.ErrorAs(t, err, &goneErr, "expected a real GoneException from the SDK deserializer")
		})
	}
}

// TestAPIGwMgmt_SDKRoundTrip_PayloadTooLarge proves an over-limit payload
// resolves to the real *types.PayloadTooLargeException -- the one error type
// among this op's set whose body the SDK actually decodes (see
// awsRestjson1_deserializeErrorPayloadTooLargeException).
func TestAPIGwMgmt_SDKRoundTrip_PayloadTooLarge(t *testing.T) {
	t.Parallel()

	backend := apigatewaymanagementapi.NewInMemoryBackend()
	h := apigatewaymanagementapi.NewHandler(backend)
	client := newTestAPIGwMgmtSDKClient(t, h)

	const connID = "roundtrip-conn-oversize"

	_, err := backend.CreateConnection(connID, "203.0.113.6", "test-agent/1.0", nil)
	require.NoError(t, err)

	_, err = client.PostToConnection(t.Context(), &apigwmgmtsdk.PostToConnectionInput{
		ConnectionId: aws.String(connID),
		Data:         make([]byte, 128*1024+1),
	})
	require.Error(t, err)

	var tooLargeErr *apigwmgmttypes.PayloadTooLargeException
	require.ErrorAs(t, err, &tooLargeErr, "expected a real PayloadTooLargeException from the SDK deserializer")
}

// TestAPIGwMgmt_SDKRoundTrip_LimitExceeded proves a connection whose real
// downstream transport buffer is full resolves to *types.LimitExceededException,
// not a silently-accepted 200.
func TestAPIGwMgmt_SDKRoundTrip_LimitExceeded(t *testing.T) {
	t.Parallel()

	backend := apigatewaymanagementapi.NewInMemoryBackend()
	h := apigatewaymanagementapi.NewHandler(backend)
	client := newTestAPIGwMgmtSDKClient(t, h)

	const connID = "roundtrip-conn-full-buffer"

	downstream := make(chan []byte)
	_, err := backend.CreateConnection(connID, "203.0.113.7", "test-agent/1.0", downstream)
	require.NoError(t, err)

	// LimitExceededException is in the SDK's default retryable-error-code
	// list, so the standard retryer would otherwise retry this call several
	// times with backoff; cap attempts at 1 to keep the test fast and
	// deterministic.
	_, err = client.PostToConnection(t.Context(), &apigwmgmtsdk.PostToConnectionInput{
		ConnectionId: aws.String(connID),
		Data:         []byte("no reader for this channel"),
	}, func(o *apigwmgmtsdk.Options) {
		o.RetryMaxAttempts = 1
	})
	require.Error(t, err)

	var limitErr *apigwmgmttypes.LimitExceededException
	require.ErrorAs(t, err, &limitErr, "expected a real LimitExceededException from the SDK deserializer")

	var generic *apigwmgmttypes.GoneException
	assert.NotErrorAs(t, err, &generic)
}
