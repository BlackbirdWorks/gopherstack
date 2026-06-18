package kafka_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kafka"
)

// TestBackend_UpdateSettings verifies that the per-setting Update* operations
// persist their payloads onto the cluster and record a ClusterOperation whose
// SourceClusterInfo/TargetClusterInfo round-trip via DescribeClusterOperation.
func TestBackend_UpdateSettings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		// apply runs the update against the cluster, returning the operation ARN.
		apply func(t *testing.T, b *kafka.InMemoryBackend, arn string) string
		// verify asserts both the persisted cluster state and the recorded operation.
		verify func(t *testing.T, c *kafka.Cluster, op *kafka.ClusterOperation)
		name   string
		opType string
	}{
		{
			name:   "connectivity",
			opType: "UPDATE_CONNECTIVITY",
			apply: func(t *testing.T, b *kafka.InMemoryBackend, arn string) string {
				t.Helper()
				op, err := b.UpdateConnectivity(context.Background(), arn, kafka.UpdateConnectivitySettings{
					ConnectivityInfo: &kafka.ConnectivityInfo{
						PublicAccess: &kafka.PublicAccess{Type: "SERVICE_PROVIDED_EIPS"},
					},
				})
				require.NoError(t, err)

				return op.ClusterOperationArn
			},
			verify: func(t *testing.T, c *kafka.Cluster, op *kafka.ClusterOperation) {
				t.Helper()
				require.NotNil(t, c.BrokerNodeGroupInfo.ConnectivityInfo)
				require.NotNil(t, c.BrokerNodeGroupInfo.ConnectivityInfo.PublicAccess)
				assert.Equal(t, "SERVICE_PROVIDED_EIPS", c.BrokerNodeGroupInfo.ConnectivityInfo.PublicAccess.Type)
				require.NotNil(t, op.TargetClusterInfo)
				require.NotNil(t, op.TargetClusterInfo.ConnectivityInfo)
				assert.Equal(t, "SERVICE_PROVIDED_EIPS",
					op.TargetClusterInfo.ConnectivityInfo.PublicAccess.Type)
			},
		},
		{
			name:   "monitoring",
			opType: "UPDATE_MONITORING",
			apply: func(t *testing.T, b *kafka.InMemoryBackend, arn string) string {
				t.Helper()
				op, err := b.UpdateMonitoring(context.Background(), arn, kafka.UpdateMonitoringSettings{
					EnhancedMonitoring: "PER_TOPIC_PER_BROKER",
					OpenMonitoring: &kafka.OpenMonitoring{
						Prometheus: &kafka.PrometheusInfo{
							JmxExporter: &kafka.JmxExporter{EnabledInBroker: true},
						},
					},
				})
				require.NoError(t, err)

				return op.ClusterOperationArn
			},
			verify: func(t *testing.T, c *kafka.Cluster, op *kafka.ClusterOperation) {
				t.Helper()
				assert.Equal(t, "PER_TOPIC_PER_BROKER", c.EnhancedMonitoring)
				require.NotNil(t, c.OpenMonitoring)
				require.NotNil(t, c.OpenMonitoring.Prometheus)
				require.NotNil(t, c.OpenMonitoring.Prometheus.JmxExporter)
				assert.True(t, c.OpenMonitoring.Prometheus.JmxExporter.EnabledInBroker)
				require.NotNil(t, op.TargetClusterInfo)
				assert.Equal(t, "PER_TOPIC_PER_BROKER", op.TargetClusterInfo.EnhancedMonitoring)
				// Source reflects pre-update state (default monitoring level).
				require.NotNil(t, op.SourceClusterInfo)
			},
		},
		{
			name:   "security",
			opType: "UPDATE_SECURITY",
			apply: func(t *testing.T, b *kafka.InMemoryBackend, arn string) string {
				t.Helper()
				op, err := b.UpdateSecurity(context.Background(), arn, kafka.UpdateSecuritySettings{
					ClientAuthentication: &kafka.ClientAuthentication{
						Sasl: &kafka.SaslSettings{Iam: &kafka.SaslIam{Enabled: true}},
					},
					EncryptionInfo: &kafka.EncryptionInfo{
						EncryptionInTransit: &kafka.EncryptionInTransit{ClientBroker: "TLS", InCluster: true},
					},
				})
				require.NoError(t, err)

				return op.ClusterOperationArn
			},
			verify: func(t *testing.T, c *kafka.Cluster, op *kafka.ClusterOperation) {
				t.Helper()
				require.NotNil(t, c.ClientAuthentication)
				require.NotNil(t, c.ClientAuthentication.Sasl)
				require.NotNil(t, c.ClientAuthentication.Sasl.Iam)
				assert.True(t, c.ClientAuthentication.Sasl.Iam.Enabled)
				require.NotNil(t, c.EncryptionInfo)
				require.NotNil(t, c.EncryptionInfo.EncryptionInTransit)
				assert.Equal(t, "TLS", c.EncryptionInfo.EncryptionInTransit.ClientBroker)
				require.NotNil(t, op.TargetClusterInfo)
				require.NotNil(t, op.TargetClusterInfo.ClientAuthentication)
			},
		},
		{
			name:   "storage",
			opType: "UPDATE_STORAGE",
			apply: func(t *testing.T, b *kafka.InMemoryBackend, arn string) string {
				t.Helper()
				op, err := b.UpdateStorage(context.Background(), arn, kafka.UpdateStorageSettings{
					StorageMode:  "TIERED",
					VolumeSizeGB: 2048,
					ProvisionedThroughput: &kafka.ProvisionedThroughput{
						Enabled:          true,
						VolumeThroughput: 250,
					},
				})
				require.NoError(t, err)

				return op.ClusterOperationArn
			},
			verify: func(t *testing.T, c *kafka.Cluster, op *kafka.ClusterOperation) {
				t.Helper()
				assert.Equal(t, "TIERED", c.StorageMode)
				require.NotNil(t, c.BrokerNodeGroupInfo.StorageInfo)
				require.NotNil(t, c.BrokerNodeGroupInfo.StorageInfo.EbsStorageInfo)
				ebs := c.BrokerNodeGroupInfo.StorageInfo.EbsStorageInfo
				assert.Equal(t, int32(2048), ebs.VolumeSize)
				require.NotNil(t, ebs.ProvisionedThroughput)
				assert.True(t, ebs.ProvisionedThroughput.Enabled)
				assert.Equal(t, int32(250), ebs.ProvisionedThroughput.VolumeThroughput)
				require.NotNil(t, op.TargetClusterInfo)
				assert.Equal(t, "TIERED", op.TargetClusterInfo.StorageMode)
				require.Len(t, op.TargetClusterInfo.BrokerEBSVolumeInfo, 1)
				assert.Equal(t, int32(2048), op.TargetClusterInfo.BrokerEBSVolumeInfo[0].VolumeSizeGB)
			},
		},
		{
			name:   "rebalancing_is_action_only",
			opType: "UPDATE_REBALANCING",
			apply: func(t *testing.T, b *kafka.InMemoryBackend, arn string) string {
				t.Helper()
				op, err := b.UpdateRebalancing(context.Background(), arn)
				require.NoError(t, err)

				return op.ClusterOperationArn
			},
			verify: func(t *testing.T, _ *kafka.Cluster, op *kafka.ClusterOperation) {
				t.Helper()
				// Rebalancing has no persisted setting; the operation is still recorded.
				assert.Nil(t, op.TargetClusterInfo)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			cluster, err := b.CreateCluster(context.Background(), "c1", "3.5.1", 3, kafka.BrokerNodeGroupInfo{
				InstanceType:  "kafka.m5.large",
				ClientSubnets: []string{"subnet-1"},
			}, nil, nil)
			require.NoError(t, err)

			opArn := tt.apply(t, b, cluster.ClusterArn)
			assert.NotEmpty(t, opArn)

			op, descErr := b.DescribeClusterOperation(context.Background(), opArn)
			require.NoError(t, descErr)
			assert.Equal(t, tt.opType, op.OperationType)
			assert.Equal(t, cluster.ClusterArn, op.ClusterArn)

			updated, getErr := b.DescribeCluster(context.Background(), cluster.ClusterArn)
			require.NoError(t, getErr)

			tt.verify(t, updated, op)
		})
	}
}

// TestBackend_UpdateSettings_NotFound verifies missing clusters error.
func TestBackend_UpdateSettings_NotFound(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	missing := "arn:aws:kafka:us-east-1:000000000000:cluster/missing/abc"

	_, err := b.UpdateConnectivity(context.Background(), missing, kafka.UpdateConnectivitySettings{})
	require.Error(t, err)
	_, err = b.UpdateMonitoring(context.Background(), missing, kafka.UpdateMonitoringSettings{})
	require.Error(t, err)
	_, err = b.UpdateSecurity(context.Background(), missing, kafka.UpdateSecuritySettings{})
	require.Error(t, err)
	_, err = b.UpdateStorage(context.Background(), missing, kafka.UpdateStorageSettings{})
	require.Error(t, err)
	_, err = b.UpdateRebalancing(context.Background(), missing)
	require.Error(t, err)
}
