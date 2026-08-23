package firehose_test

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	firehosesdk "github.com/aws/aws-sdk-go-v2/service/firehose"
	firehosetypes "github.com/aws/aws-sdk-go-v2/service/firehose/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/firehose"
)

// newTestFirehoseClient stands up the real aws-sdk-go-v2 firehose client
// against an httptest server running this package's Handler, wired through
// the same pkgs/service registry/router used in production.
func newTestFirehoseClient(t *testing.T, h *firehose.Handler) *firehosesdk.Client {
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

	return firehosesdk.NewFromConfig(cfg, func(o *firehosesdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreateDeliveryStream_MSKReadFromTimestampRoundTrip drives
// CreateDeliveryStream and DescribeDeliveryStream through the real
// aws-sdk-go-v2 firehose client. MSKSourceConfiguration/MSKSourceDescription
// .ReadFromTimestamp is a JSON number (epoch seconds) on the real shape --
// both serializers.go (request) and deserializers.go (response) encode/
// decode it via FormatEpochSeconds/ParseEpochSeconds. gopherstack previously
// modeled it as a plain string on both sides, so a real client setting this
// MSK field failed CreateDeliveryStream's request decode outright.
func TestCreateDeliveryStream_MSKReadFromTimestampRoundTrip(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("123456789012", "us-east-1")
	h := firehose.NewHandler(b)
	client := newTestFirehoseClient(t, h)

	readFrom := time.Unix(1_700_000_000, 0).UTC()

	_, err := client.CreateDeliveryStream(t.Context(), &firehosesdk.CreateDeliveryStreamInput{
		DeliveryStreamName: aws.String("msk-stream"),
		DeliveryStreamType: firehosetypes.DeliveryStreamTypeMSKAsSource,
		MSKSourceConfiguration: &firehosetypes.MSKSourceConfiguration{
			MSKClusterARN: aws.String("arn:aws:kafka:us-east-1:123456789012:cluster/c/1"),
			TopicName:     aws.String("my-topic"),
			AuthenticationConfiguration: &firehosetypes.AuthenticationConfiguration{
				Connectivity: firehosetypes.ConnectivityPublic,
				RoleARN:      aws.String("arn:aws:iam::123456789012:role/r"),
			},
			ReadFromTimestamp: aws.Time(readFrom),
		},
	})
	require.NoError(
		t,
		err,
		"real SDK client's CreateDeliveryStream request must decode without error",
	)

	out, err := client.DescribeDeliveryStream(t.Context(), &firehosesdk.DescribeDeliveryStreamInput{
		DeliveryStreamName: aws.String("msk-stream"),
	})
	require.NoError(
		t,
		err,
		"real SDK client must decode DescribeDeliveryStream response without error",
	)
	require.NotNil(t, out.DeliveryStreamDescription)
	require.NotNil(t, out.DeliveryStreamDescription.Source)
	require.NotNil(t, out.DeliveryStreamDescription.Source.MSKSourceDescription)
	require.NotNil(t, out.DeliveryStreamDescription.Source.MSKSourceDescription.ReadFromTimestamp)
	assert.WithinDuration(
		t,
		readFrom,
		*out.DeliveryStreamDescription.Source.MSKSourceDescription.ReadFromTimestamp,
		time.Second,
	)
}
