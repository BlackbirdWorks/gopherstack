package kafkaconnect_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	kafkaconnectsdk "github.com/aws/aws-sdk-go-v2/service/kafkaconnect"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/kafkaconnect"
)

const testRegion = "us-east-1"
const testAccountID = "123456789012"

// newTestClient stands up the real aws-sdk-go-v2 kafkaconnect client against
// an httptest server running this package's Handler, wired through the same
// pkgs/service registry/router used in production.
func newTestClient(t *testing.T, h *kafkaconnect.Handler) *kafkaconnectsdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(testRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return kafkaconnectsdk.NewFromConfig(cfg, func(o *kafkaconnectsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

func newTestHandler() *kafkaconnect.Handler {
	backend := kafkaconnect.NewInMemoryBackend()
	h := kafkaconnect.NewHandler(backend)
	h.AccountID = testAccountID
	h.DefaultRegion = testRegion

	return h
}
