package rekognition_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	rekognitionsdk "github.com/aws/aws-sdk-go-v2/service/rekognition"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/rekognition"
)

const rtTestRegion = "us-east-1"

const rtTestAccountID = "000000000000"

// newRoundTripClient stands up the real aws-sdk-go-v2 rekognition client
// against an httptest server running this package's Handler through the
// same pkgs/service registry/router used in production, proving wire
// compatibility rather than just calling Go methods directly.
func newRoundTripClient(t *testing.T, h *rekognition.Handler) *rekognitionsdk.Client {
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

	return rekognitionsdk.NewFromConfig(cfg, func(o *rekognitionsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// newTestHandlerAndClient combines a fresh in-memory backend/handler pair
// with a round-trip SDK client against it.
func newTestHandlerAndClient(t *testing.T) *rekognitionsdk.Client {
	t.Helper()

	backend := rekognition.NewInMemoryBackend(rtTestAccountID, rtTestRegion)
	h := rekognition.NewHandler(backend)

	return newRoundTripClient(t, h)
}
