package cognitoidentity_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	cognitoidentitysdk "github.com/aws/aws-sdk-go-v2/service/cognitoidentity"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/cognitoidentity"
)

const rtTestRegion = "us-east-1"

// newTestHandlerAndClient stands up a fresh in-memory cognitoidentity backend
// and a real aws-sdk-go-v2 client against an httptest server running its
// Handler, wired through the same pkgs/service registry/router used in
// production.
func newTestHandlerAndClient(t *testing.T) *cognitoidentitysdk.Client {
	t.Helper()

	backend := cognitoidentity.NewInMemoryBackend("000000000000", rtTestRegion)
	h := cognitoidentity.NewHandler(backend, rtTestRegion)

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

	return cognitoidentitysdk.NewFromConfig(cfg, func(o *cognitoidentitysdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}
