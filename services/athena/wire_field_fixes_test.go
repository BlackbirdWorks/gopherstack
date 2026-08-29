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

// TestCreateWorkGroup_EngineAndMonitoringConfiguration_RealClient covers
// gopherstack-6flj-athena-1: real types.WorkGroupConfiguration
// (athena@v1.60.4/types/types.go) has EngineConfiguration and
// MonitoringConfiguration members (serializers.go's
// awsAwsjson11_serializeDocumentWorkGroupConfiguration "EngineConfiguration"/
// "MonitoringConfiguration" cases), but gopherstack's WorkGroupConfiguration
// model had neither field -- both were silently dropped on CreateWorkGroup
// regardless of what a real client set. Also covers
// EngineConfiguration.Classifications (types.Classification{Name,
// Properties}), a real member missing from gopherstack's EngineConfiguration
// model entirely -- affecting both this workgroup-level use and the
// pre-existing StartSession path, since real AWS reuses the identical
// EngineConfiguration type for both.
func TestCreateWorkGroup_EngineAndMonitoringConfiguration_RealClient(t *testing.T) {
	t.Parallel()

	backend := athena.NewInMemoryBackend(config.DefaultRegion, "123456789012")
	client := newTestAthenaClient(t, athena.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateWorkGroup(ctx, &athenasdk.CreateWorkGroupInput{
		Name: aws.String("spark-workgroup"),
		Configuration: &types.WorkGroupConfiguration{
			ResultConfiguration: &types.ResultConfiguration{
				OutputLocation: aws.String("s3://my-bucket/results/"),
			},
			EngineConfiguration: &types.EngineConfiguration{
				CoordinatorDpuSize:     aws.Int32(1),
				DefaultExecutorDpuSize: aws.Int32(2),
				MaxConcurrentDpus:      aws.Int32(5),
				Classifications: []types.Classification{
					{Name: aws.String("spark"), Properties: map[string]string{"key": "value"}},
				},
			},
			MonitoringConfiguration: &types.MonitoringConfiguration{
				CloudWatchLoggingConfiguration: &types.CloudWatchLoggingConfiguration{
					Enabled:  aws.Bool(true),
					LogGroup: aws.String("/aws/athena/spark"),
				},
			},
		},
	})
	require.NoError(t, err)

	got, err := client.GetWorkGroup(ctx, &athenasdk.GetWorkGroupInput{WorkGroup: aws.String("spark-workgroup")})
	require.NoError(t, err)

	cfg := got.WorkGroup.Configuration
	require.NotNil(t, cfg.EngineConfiguration,
		"WorkGroupConfiguration.EngineConfiguration must round-trip; pre-fix it was always nil")
	assert.Equal(t, int32(5), aws.ToInt32(cfg.EngineConfiguration.MaxConcurrentDpus))
	require.Len(t, cfg.EngineConfiguration.Classifications, 1,
		"EngineConfiguration.Classifications must round-trip; pre-fix the field did not exist")
	assert.Equal(t, "spark", aws.ToString(cfg.EngineConfiguration.Classifications[0].Name))

	require.NotNil(t, cfg.MonitoringConfiguration,
		"WorkGroupConfiguration.MonitoringConfiguration must round-trip; pre-fix it was always nil")
	require.NotNil(t, cfg.MonitoringConfiguration.CloudWatchLoggingConfiguration)
	assert.True(t, aws.ToBool(cfg.MonitoringConfiguration.CloudWatchLoggingConfiguration.Enabled))

	_, err = client.UpdateWorkGroup(ctx, &athenasdk.UpdateWorkGroupInput{
		WorkGroup: aws.String("spark-workgroup"),
		ConfigurationUpdates: &types.WorkGroupConfigurationUpdates{
			EngineConfiguration: &types.EngineConfiguration{MaxConcurrentDpus: aws.Int32(10)},
		},
	})
	require.NoError(t, err)

	got2, err := client.GetWorkGroup(ctx, &athenasdk.GetWorkGroupInput{WorkGroup: aws.String("spark-workgroup")})
	require.NoError(t, err)
	require.NotNil(t, got2.WorkGroup.Configuration.EngineConfiguration)
	assert.Equal(t, int32(10), aws.ToInt32(got2.WorkGroup.Configuration.EngineConfiguration.MaxConcurrentDpus))
	outputLoc := aws.ToString(got2.WorkGroup.Configuration.ResultConfiguration.OutputLocation)
	assert.Equal(t, "s3://my-bucket/results/", outputLoc,
		"UpdateWorkGroup's ConfigurationUpdates must merge, not wholesale-replace the stored configuration")
}
