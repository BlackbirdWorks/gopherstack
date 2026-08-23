package opsworks_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	opsworkssdk "github.com/aws/aws-sdk-go-v2/service/opsworks"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/opsworks"
)

const rtTestRegion = "us-east-1"

// newRoundTripClient stands up the real aws-sdk-go-v2 opsworks client
// against an httptest server running this package's Handler through the
// same pkgs/service registry/router used in production. gopherstack-n3zi:
// opsworks had zero operations ever exercised by a typed client -- every
// existing test drove h.Handler() directly, which proves the JSON body
// looks right but never proves the real SDK deserializer can read it.
func newRoundTripClient(t *testing.T, h *opsworks.Handler) *opsworkssdk.Client {
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

	return opsworkssdk.NewFromConfig(cfg, func(o *opsworkssdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

func newTestClient(t *testing.T) *opsworkssdk.Client {
	t.Helper()

	backend := opsworks.NewInMemoryBackend("000000000000", rtTestRegion)
	h := opsworks.NewHandler(backend)

	return newRoundTripClient(t, h)
}
