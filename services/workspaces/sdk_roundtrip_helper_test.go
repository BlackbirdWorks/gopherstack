package workspaces_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	wssdk "github.com/aws/aws-sdk-go-v2/service/workspaces"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/workspaces"
)

const (
	rtTestRegion    = "us-east-1"
	rtTestAccountID = "000000000000"
)

// newTestHandlerAndClient stands up a fresh in-memory workspaces backend and
// a real aws-sdk-go-v2 client against an httptest server running its
// Handler, wired through the same pkgs/service registry/router used in
// production. Round-tripping through the genuine SDK serializer/deserializer
// is what proves wire-compatibility that a direct handler call would miss.
func newTestHandlerAndClient(t *testing.T) *wssdk.Client {
	t.Helper()

	backend := workspaces.NewInMemoryBackend(rtTestAccountID, rtTestRegion)
	h := workspaces.NewHandler(backend)

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

	return wssdk.NewFromConfig(cfg, func(o *wssdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}
