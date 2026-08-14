package athena_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	athenasdk "github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/athena"
)

// newTestAthenaClient stands up the real aws-sdk-go-v2 athena client against
// an httptest server running this package's Handler, wired through the same
// pkgs/service registry/router used in production.
func newTestAthenaClient(t *testing.T, h *athena.Handler) *athenasdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(config.DefaultRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return athenasdk.NewFromConfig(cfg, func(o *athenasdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestGetQueryExecution_ReusedPreviousResult_Nesting_RealClient covers a
// layer-2 bug (gopherstack-21my): the backend genuinely tracks whether a
// query execution reused a previous result (query_executions.go's
// hasReusableResult/newQueryExecution), but emitted it as a flat
// Statistics.ReusedPreviousResult field. Real AWS nests this one level
// deeper under Statistics.ResultReuseInformation.ReusedPreviousResult
// (athena@v1.60.4 deserializers.go's
// awsAwsjson11_deserializeDocumentResultReuseInformation, referenced from
// awsAwsjson11_deserializeDocumentQueryExecutionStatistics's "ResultReuseInformation"
// case -- there is no "ReusedPreviousResult" case directly on Statistics).
// Pre-fix, a real client's Statistics.ResultReuseInformation was always nil
// regardless of whether the query actually reused a prior result, since the
// deserializer has no case for a flat "ReusedPreviousResult" key on
// Statistics and silently drops it.
func TestGetQueryExecution_ReusedPreviousResult_Nesting_RealClient(t *testing.T) {
	t.Parallel()

	backend := athena.NewInMemoryBackend("123456789012", config.DefaultRegion)
	client := newTestAthenaClient(t, athena.NewHandler(backend))
	ctx := t.Context()

	reuseCfg := &types.ResultReuseConfiguration{
		ResultReuseByAgeConfiguration: &types.ResultReuseByAgeConfiguration{
			Enabled:         true,
			MaxAgeInMinutes: aws.Int32(60),
		},
	}

	start1, err := client.StartQueryExecution(ctx, &athenasdk.StartQueryExecutionInput{
		QueryString:              aws.String("SELECT 42"),
		WorkGroup:                aws.String("primary"),
		ResultReuseConfiguration: reuseCfg,
	})
	require.NoError(t, err)

	get1, err := client.GetQueryExecution(ctx, &athenasdk.GetQueryExecutionInput{
		QueryExecutionId: start1.QueryExecutionId,
	})
	require.NoError(t, err)
	require.NotNil(t, get1.QueryExecution.Statistics)
	if get1.QueryExecution.Statistics.ResultReuseInformation != nil {
		assert.False(t, get1.QueryExecution.Statistics.ResultReuseInformation.ReusedPreviousResult,
			"first execution must not be marked reused")
	}

	start2, err := client.StartQueryExecution(ctx, &athenasdk.StartQueryExecutionInput{
		QueryString:              aws.String("SELECT 42"),
		WorkGroup:                aws.String("primary"),
		ResultReuseConfiguration: reuseCfg,
	})
	require.NoError(t, err)

	get2, err := client.GetQueryExecution(ctx, &athenasdk.GetQueryExecutionInput{
		QueryExecutionId: start2.QueryExecutionId,
	})
	require.NoError(t, err)
	require.NotNil(t, get2.QueryExecution.Statistics)
	require.NotNil(t, get2.QueryExecution.Statistics.ResultReuseInformation,
		"ResultReuseInformation must round-trip through the real client; pre-fix it was always nil")
	assert.True(t, get2.QueryExecution.Statistics.ResultReuseInformation.ReusedPreviousResult,
		"second identical execution should be marked as having reused the previous result")
}
