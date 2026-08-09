package cloudwatch_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	cwsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

const rtTestRegion = "us-east-1"

// newTestHandlerAndClient stands up a fresh in-memory cloudwatch backend and
// a real aws-sdk-go-v2 client against an httptest server running its
// Handler, wired through the same pkgs/service registry/router used in
// production. cloudwatch@v1.66.3 speaks smithy rpc-v2 CBOR exclusively (no
// awsQuery serializer exists in this SDK version), so this round trip
// exercises rpcv2cbor.go, not the legacy XML/form handler.go path.
func newTestHandlerAndClient(t *testing.T) *cwsdk.Client {
	t.Helper()

	backend := cloudwatch.NewInMemoryBackend()
	h := cloudwatch.NewHandler(backend)

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

	return cwsdk.NewFromConfig(cfg, func(o *cwsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}
