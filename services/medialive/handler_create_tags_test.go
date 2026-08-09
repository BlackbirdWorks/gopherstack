package medialive_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	medialivesdk "github.com/aws/aws-sdk-go-v2/service/medialive"
	"github.com/aws/aws-sdk-go-v2/service/medialive/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/medialive"
)

// newTestMediaLiveClient stands up the real aws-sdk-go-v2 MediaLive client
// against an httptest server running this package's Handler, wired through
// the same pkgs/service registry/router used in production.
func newTestMediaLiveClient(t *testing.T, h *medialive.Handler) *medialivesdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return medialivesdk.NewFromConfig(cfg, func(o *medialivesdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreateOps_TagsRoundTrip drives the nine MediaLive Create ops whose tag
// wiring gopherstack-2mwl found broken through a real SDK client and asserts
// ListTagsForResource sees what was supplied at creation. All nine
// (ChannelPlacementGroup, Cluster, Node, Network, SignalMap, SdiSource,
// CloudWatchAlarmTemplate(Group), EventBridgeRuleTemplate(Group)) wrote tags
// only onto their own resource struct -- never reaching taggableResourceTags'
// lookup table (a missing-case gap, memorydb/neptune-shaped) or, for
// SdiSource, dropping tags outright (backend signature discarded the
// parameter with `_ map[string]string`). Channel/Input/InputSecurityGroup/
// Multiplex/InputDevice were already fixed and covered before this pass.
func TestCreateOps_TagsRoundTrip(t *testing.T) {
	t.Parallel()

	tags := map[string]string{"env": "prod"}

	requireTags := func(t *testing.T, client *medialivesdk.Client, resourceARN string) {
		t.Helper()
		out, err := client.ListTagsForResource(t.Context(), &medialivesdk.ListTagsForResourceInput{
			ResourceArn: aws.String(resourceARN),
		})
		require.NoError(t, err)
		assert.Equal(t, tags, out.Tags)
	}

	t.Run("createcluster", func(t *testing.T) {
		t.Parallel()

		h := medialive.NewHandler(medialive.NewInMemoryBackend("123456789012", "us-east-1"))
		client := newTestMediaLiveClient(t, h)

		out, err := client.CreateCluster(t.Context(), &medialivesdk.CreateClusterInput{
			Name: aws.String("tagged-cluster"),
			Tags: tags,
		})
		require.NoError(t, err)
		requireTags(t, client, aws.ToString(out.Arn))
	})

	t.Run("createchannelplacementgroup", func(t *testing.T) {
		t.Parallel()

		h := medialive.NewHandler(medialive.NewInMemoryBackend("123456789012", "us-east-1"))
		client := newTestMediaLiveClient(t, h)

		cluster, err := client.CreateCluster(t.Context(), &medialivesdk.CreateClusterInput{Name: aws.String("c")})
		require.NoError(t, err)

		out, err := client.CreateChannelPlacementGroup(t.Context(), &medialivesdk.CreateChannelPlacementGroupInput{
			ClusterId: cluster.Id,
			Name:      aws.String("tagged-cpg"),
			Tags:      tags,
		})
		require.NoError(t, err)
		requireTags(t, client, aws.ToString(out.Arn))
	})

	t.Run("createnode", func(t *testing.T) {
		t.Parallel()

		h := medialive.NewHandler(medialive.NewInMemoryBackend("123456789012", "us-east-1"))
		client := newTestMediaLiveClient(t, h)

		cluster, err := client.CreateCluster(t.Context(), &medialivesdk.CreateClusterInput{Name: aws.String("c")})
		require.NoError(t, err)

		out, err := client.CreateNode(t.Context(), &medialivesdk.CreateNodeInput{
			ClusterId: cluster.Id,
			Name:      aws.String("tagged-node"),
			Tags:      tags,
		})
		require.NoError(t, err)
		requireTags(t, client, aws.ToString(out.Arn))
	})

	t.Run("createnetwork", func(t *testing.T) {
		t.Parallel()

		h := medialive.NewHandler(medialive.NewInMemoryBackend("123456789012", "us-east-1"))
		client := newTestMediaLiveClient(t, h)

		out, err := client.CreateNetwork(t.Context(), &medialivesdk.CreateNetworkInput{
			Name: aws.String("tagged-network"),
			Tags: tags,
		})
		require.NoError(t, err)
		requireTags(t, client, aws.ToString(out.Arn))
	})

	t.Run("createsignalmap", func(t *testing.T) {
		t.Parallel()

		h := medialive.NewHandler(medialive.NewInMemoryBackend("123456789012", "us-east-1"))
		client := newTestMediaLiveClient(t, h)

		out, err := client.CreateSignalMap(t.Context(), &medialivesdk.CreateSignalMapInput{
			Name:                   aws.String("tagged-signalmap"),
			DiscoveryEntryPointArn: aws.String("arn:aws:mediaconnect:us-east-1:123456789012:flow:1:f"),
			Tags:                   tags,
		})
		require.NoError(t, err)
		requireTags(t, client, aws.ToString(out.Arn))
	})

	t.Run("createsdisource", func(t *testing.T) {
		t.Parallel()

		h := medialive.NewHandler(medialive.NewInMemoryBackend("123456789012", "us-east-1"))
		client := newTestMediaLiveClient(t, h)

		out, err := client.CreateSdiSource(t.Context(), &medialivesdk.CreateSdiSourceInput{
			Name: aws.String("tagged-sdisource"),
			Tags: tags,
		})
		require.NoError(t, err)
		requireTags(t, client, aws.ToString(out.SdiSource.Arn))
	})

	t.Run("createcloudwatchalarmtemplategroup", func(t *testing.T) {
		t.Parallel()

		h := medialive.NewHandler(medialive.NewInMemoryBackend("123456789012", "us-east-1"))
		client := newTestMediaLiveClient(t, h)

		out, err := client.CreateCloudWatchAlarmTemplateGroup(
			t.Context(), &medialivesdk.CreateCloudWatchAlarmTemplateGroupInput{
				Name: aws.String("tagged-cw-group"),
				Tags: tags,
			},
		)
		require.NoError(t, err)
		requireTags(t, client, aws.ToString(out.Arn))
	})

	t.Run("createcloudwatchalarmtemplate", func(t *testing.T) {
		t.Parallel()

		h := medialive.NewHandler(medialive.NewInMemoryBackend("123456789012", "us-east-1"))
		client := newTestMediaLiveClient(t, h)

		out, err := client.CreateCloudWatchAlarmTemplate(t.Context(), &medialivesdk.CreateCloudWatchAlarmTemplateInput{
			Name:               aws.String("tagged-cw-template"),
			GroupIdentifier:    aws.String("some-group"),
			MetricName:         aws.String("4xxErrors"),
			ComparisonOperator: types.CloudWatchAlarmTemplateComparisonOperatorGreaterThanThreshold,
			EvaluationPeriods:  aws.Int32(1),
			Period:             aws.Int32(60),
			Statistic:          types.CloudWatchAlarmTemplateStatisticAverage,
			TargetResourceType: types.CloudWatchAlarmTemplateTargetResourceTypeCloudfrontDistribution,
			Threshold:          aws.Float64(1),
			TreatMissingData:   types.CloudWatchAlarmTemplateTreatMissingDataMissing,
			Tags:               tags,
		})
		require.NoError(t, err)
		requireTags(t, client, aws.ToString(out.Arn))
	})

	t.Run("createeventbridgeruletemplategroup", func(t *testing.T) {
		t.Parallel()

		h := medialive.NewHandler(medialive.NewInMemoryBackend("123456789012", "us-east-1"))
		client := newTestMediaLiveClient(t, h)

		out, err := client.CreateEventBridgeRuleTemplateGroup(
			t.Context(), &medialivesdk.CreateEventBridgeRuleTemplateGroupInput{
				Name: aws.String("tagged-eb-group"),
				Tags: tags,
			},
		)
		require.NoError(t, err)
		requireTags(t, client, aws.ToString(out.Arn))
	})

	t.Run("createeventbridgeruletemplate", func(t *testing.T) {
		t.Parallel()

		h := medialive.NewHandler(medialive.NewInMemoryBackend("123456789012", "us-east-1"))
		client := newTestMediaLiveClient(t, h)

		out, err := client.CreateEventBridgeRuleTemplate(t.Context(), &medialivesdk.CreateEventBridgeRuleTemplateInput{
			Name:            aws.String("tagged-eb-template"),
			GroupIdentifier: aws.String("some-group"),
			EventType:       types.EventBridgeRuleTemplateEventTypeMedialiveChannelAlert,
			Tags:            tags,
		})
		require.NoError(t, err)
		requireTags(t, client, aws.ToString(out.Arn))
	})
}
