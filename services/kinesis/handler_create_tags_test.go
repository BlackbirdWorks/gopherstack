package kinesis_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	kinesissdk "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/kinesis"
)

const kinesisTagsRTRegion = "us-east-1"

// newTestKinesisClient stands up the real aws-sdk-go-v2 kinesis client
// against an httptest server running this package's Handler, wired through
// the same pkgs/service registry/router used in production.
//
// Keep-alives are disabled: SubscribeToShard tests issue several ordinary
// calls (CreateStream, DescribeStream, ...) on this client before opening
// the event stream, and under heavy scheduler contention (-race, shuffled
// t.Parallel load) net/http's Transport can reuse the keep-alive
// connection from one of those calls for the SubscribeToShard request; a
// stale-idle-connection race in that reuse path
// (net/http.(*persistConn).writeLoop tearing down the conn while the
// SDK's event-stream reader is still blocked reading it) surfaces to the
// caller as "use of closed network connection" instead of the clean EOF a
// fresh connection gets (gopherstack-i8q7). Forcing a new connection per
// request removes the reuse window entirely; it does not change what any
// test observes over the wire.
func newTestKinesisClient(t *testing.T, h *kinesis.Handler) *kinesissdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(kinesisTagsRTRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return kinesissdk.NewFromConfig(cfg, func(o *kinesissdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
		o.HTTPClient = awshttp.NewBuildableClient().WithTransportOptions(func(tr *http.Transport) {
			tr.DisableKeepAlives = true
		})
	})
}

// TestCreateStream_TagsRoundTrip drives CreateStream, whose real Input
// struct accepts Tags (kinesis@v1.46.4 api_op_CreateStream.go:117), through
// the real SDK client and asserts ListTagsForResource sees what was
// supplied at creation (gopherstack-2mwl).
func TestCreateStream_TagsRoundTrip(t *testing.T) {
	t.Parallel()

	backend := kinesis.NewInMemoryBackend()
	client := newTestKinesisClient(t, kinesis.NewHandler(backend))

	streamName := "tagged-stream"

	_, err := client.CreateStream(t.Context(), &kinesissdk.CreateStreamInput{
		StreamName: aws.String(streamName),
		ShardCount: aws.Int32(1),
		Tags:       map[string]string{"env": "test"},
	})
	require.NoError(t, err)

	desc, err := client.DescribeStream(t.Context(), &kinesissdk.DescribeStreamInput{
		StreamName: aws.String(streamName),
	})
	require.NoError(t, err)

	got, err := client.ListTagsForResource(t.Context(), &kinesissdk.ListTagsForResourceInput{
		ResourceARN: desc.StreamDescription.StreamARN,
	})
	require.NoError(t, err)
	require.Len(t, got.Tags, 1)
	assert.Equal(t, "env", aws.ToString(got.Tags[0].Key))
	assert.Equal(t, "test", aws.ToString(got.Tags[0].Value))
}
