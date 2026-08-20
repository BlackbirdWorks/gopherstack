package firehose_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	firehosesdk "github.com/aws/aws-sdk-go-v2/service/firehose"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/firehose"
)

const rtTestRegion = "us-east-1"

// newRoundTripClient stands up the real aws-sdk-go-v2 firehose client against
// an httptest server running this package's Handler, wired through the same
// pkgs/service registry/router used in production, so a shape is verified by
// the real client's own serializer/deserializer -- not gopherstack's own JSON
// tags. This is what proved that several nested S3 destination fields used
// the Create-only wire key "S3BackupConfiguration"/"S3Configuration" even
// when parsing an Update request, where the real SDK actually serializes
// "S3BackupUpdate"/"S3Update".
func newRoundTripClient(t *testing.T, h *firehose.Handler) *firehosesdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(rtTestRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return firehosesdk.NewFromConfig(cfg, func(o *firehosesdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// newTestClient is a convenience wrapper combining a fresh in-memory
// backend/handler pair with a round-trip SDK client against it.
func newTestClient(t *testing.T) *firehosesdk.Client {
	t.Helper()

	backend := firehose.NewInMemoryBackend("000000000000", rtTestRegion)
	h := firehose.NewHandler(backend)

	return newRoundTripClient(t, h)
}
