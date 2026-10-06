package kafka_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kafkasdk "github.com/aws/aws-sdk-go-v2/service/kafka"
	"github.com/aws/aws-sdk-go-v2/service/kafka/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateCluster_OmittedMembersTakeDefaults(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name           string
		monitoring     types.EnhancedMonitoring
		wantMonitoring types.EnhancedMonitoring
	}{
		{name: "omitted", wantMonitoring: types.EnhancedMonitoringDefault},
		{
			name:           "explicit",
			monitoring:     types.EnhancedMonitoringPerBroker,
			wantMonitoring: types.EnhancedMonitoringPerBroker,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newTestKafkaClient(t, newTestHandler(t))
			ctx := t.Context()

			created, err := c.CreateCluster(ctx, &kafkasdk.CreateClusterInput{
				ClusterName: aws.String("c"), KafkaVersion: aws.String("3.5.1"), NumberOfBrokerNodes: aws.Int32(3),
				EnhancedMonitoring: tc.monitoring,
				BrokerNodeGroupInfo: &types.BrokerNodeGroupInfo{
					InstanceType: aws.String("kafka.m5.large"), ClientSubnets: []string{"a", "b", "c"},
				},
			})
			require.NoError(t, err)

			got, err := c.DescribeCluster(ctx, &kafkasdk.DescribeClusterInput{ClusterArn: created.ClusterArn})
			require.NoError(t, err)
			assert.Equal(t, tc.wantMonitoring, got.ClusterInfo.EnhancedMonitoring)
			assert.Equal(t, types.BrokerAZDistributionDefault, got.ClusterInfo.BrokerNodeGroupInfo.BrokerAZDistribution)
		})
	}
}

func TestUpdateConfiguration_CreatesNewRevision(t *testing.T) {
	t.Parallel()

	c := newTestKafkaClient(t, newTestHandler(t))
	ctx := t.Context()

	cfg, err := c.CreateConfiguration(ctx, &kafkasdk.CreateConfigurationInput{
		Name: aws.String("cfg"), ServerProperties: []byte("a=b"), KafkaVersions: []string{"3.5.1"},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), aws.ToInt64(cfg.LatestRevision.Revision))

	upd, err := c.UpdateConfiguration(
		ctx,
		&kafkasdk.UpdateConfigurationInput{Arn: cfg.Arn, ServerProperties: []byte("a=c")},
	)
	require.NoError(t, err)
	assert.Equal(t, int64(2), aws.ToInt64(upd.LatestRevision.Revision))

	revs, err := c.ListConfigurationRevisions(ctx, &kafkasdk.ListConfigurationRevisionsInput{Arn: cfg.Arn})
	require.NoError(t, err)
	assert.Len(t, revs.Revisions, 2)

	first, err := c.DescribeConfigurationRevision(
		ctx,
		&kafkasdk.DescribeConfigurationRevisionInput{Arn: cfg.Arn, Revision: aws.Int64(1)},
	)
	require.NoError(t, err)
	assert.Equal(t, "a=b", string(first.ServerProperties))

	latest, err := c.DescribeConfigurationRevision(
		ctx,
		&kafkasdk.DescribeConfigurationRevisionInput{Arn: cfg.Arn, Revision: aws.Int64(2)},
	)
	require.NoError(t, err)
	assert.Equal(t, "a=c", string(latest.ServerProperties))
}
