package kafka_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	kafkasdk "github.com/aws/aws-sdk-go-v2/service/kafka"
	"github.com/aws/aws-sdk-go-v2/service/kafka/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/kafka"
)

// newTestKafkaClient stands up the real aws-sdk-go-v2 Kafka (MSK) client
// against an httptest server running this package's Handler, wired through
// the same pkgs/service registry/router used in production.
func newTestKafkaClient(t *testing.T, h *kafka.Handler) *kafkasdk.Client {
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

	return kafkasdk.NewFromConfig(cfg, func(o *kafkasdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreateOps_TagsRoundTrip drives a representative sample of MSK Create
// ops through a real SDK client and asserts ListTagsForResource sees what
// was supplied at creation (gopherstack-2mwl). All 5 tag-accepting Create
// ops (kafka@v1.57.2: CreateCluster, CreateClusterV2, CreateChannel,
// CreateReplicator, CreateVpcConnection) were read against the pinned SDK
// and found uniformly wired: each handler decodes a "tags" field (confirmed
// against serializers.go's object.Key("tags")) and each backend Create
// stores nonNilTagsCopy(tags) directly onto the resource struct that
// TagResource/GetTags key off by ARN. This test exercises that wiring for
// CreateCluster (independent) and CreateVpcConnection (depends on an
// existing cluster, testing the cross-resource ARN lookup) as real-client
// spot checks; the other three follow the identical pattern.
func TestCreateOps_TagsRoundTrip(t *testing.T) {
	t.Parallel()

	tags := map[string]string{"env": "prod"}

	requireTags := func(t *testing.T, client *kafkasdk.Client, resourceARN *string) {
		t.Helper()
		out, err := client.ListTagsForResource(t.Context(), &kafkasdk.ListTagsForResourceInput{
			ResourceArn: resourceARN,
		})
		require.NoError(t, err)
		assert.Equal(t, tags, out.Tags)
	}

	newClient := func(t *testing.T) *kafkasdk.Client {
		t.Helper()

		h := kafka.NewHandler(kafka.NewInMemoryBackend("123456789012", "us-east-1"))

		return newTestKafkaClient(t, h)
	}

	t.Run("createcluster", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)

		out, err := client.CreateCluster(t.Context(), &kafkasdk.CreateClusterInput{
			ClusterName:         aws.String("tagged-cluster"),
			KafkaVersion:        aws.String("3.5.1"),
			NumberOfBrokerNodes: aws.Int32(3),
			BrokerNodeGroupInfo: &types.BrokerNodeGroupInfo{
				ClientSubnets: []string{"subnet-1", "subnet-2"},
				InstanceType:  aws.String("kafka.m5.large"),
			},
			Tags: tags,
		})
		require.NoError(t, err)
		requireTags(t, client, out.ClusterArn)
	})

	t.Run("createvpcconnection", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)

		cluster, err := client.CreateCluster(t.Context(), &kafkasdk.CreateClusterInput{
			ClusterName:         aws.String("cluster-for-vpc-conn"),
			KafkaVersion:        aws.String("3.5.1"),
			NumberOfBrokerNodes: aws.Int32(3),
			BrokerNodeGroupInfo: &types.BrokerNodeGroupInfo{
				ClientSubnets: []string{"subnet-1", "subnet-2"},
				InstanceType:  aws.String("kafka.m5.large"),
			},
		})
		require.NoError(t, err)

		out, err := client.CreateVpcConnection(t.Context(), &kafkasdk.CreateVpcConnectionInput{
			TargetClusterArn: cluster.ClusterArn,
			Authentication:   aws.String("SASL_IAM"),
			VpcId:            aws.String("vpc-1"),
			ClientSubnets:    []string{"subnet-1"},
			SecurityGroups:   []string{"sg-1"},
			Tags:             tags,
		})
		require.NoError(t, err)
		requireTags(t, client, out.VpcConnectionArn)
	})
}
