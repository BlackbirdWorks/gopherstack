package batch_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	batchsdk "github.com/aws/aws-sdk-go-v2/service/batch"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/batch"
	"github.com/blackbirdworks/gopherstack/services/polly"
)

// newTestBatchPollyRegistryServer wires up Batch's real Handler alongside
// Polly, which registers earlier in cli.go's chain and whose RouteMatcher
// claims a bare "/v1/" prefix (pollyPathPrefix, services/polly/handler.go).
// Polly's matcher additionally requires parseRoute(method, path).operation
// != opUnknown -- a closed whitelist of 5 routes, none of which is
// "/v1/describejobqueues" (Batch's real DescribeJobQueues path,
// batch@v1.68.4 serializers.go) -- so Polly never actually claims Batch's
// traffic. cmd/routecollisions flags this as UNGUARDED-WINNER because it
// only sees Polly's bare prefix literal, not the whitelist check
// (gopherstack-op3e census sweep 2026-09-19).
func newTestBatchPollyRegistryServer(t *testing.T) *httptest.Server {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()

	require.NoError(t, registry.Register(polly.NewHandler(polly.NewInMemoryBackend())))
	require.NoError(t, registry.Register(
		batch.NewHandler(batch.NewInMemoryBackend("000000000000", "us-east-1")),
	))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	return srv
}

// TestV1Routing_BatchNotShadowedByPolly proves Batch's own
// DescribeJobQueues (POST /v1/describejobqueues) still reaches Batch's
// handler when Polly is registered ahead of it in the same router.
func TestV1Routing_BatchNotShadowedByPolly(t *testing.T) {
	t.Parallel()

	srv := newTestBatchPollyRegistryServer(t)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	client := batchsdk.NewFromConfig(cfg, func(o *batchsdk.Options) { o.BaseEndpoint = aws.String(srv.URL) })

	out, batchErr := client.DescribeJobQueues(t.Context(), &batchsdk.DescribeJobQueuesInput{})
	require.NoError(
		t, batchErr,
		"Batch's DescribeJobQueues must reach Batch's handler, not Polly's unknown-route 404",
	)
	require.NotNil(t, out.JobQueues)
}
