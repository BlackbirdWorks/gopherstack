package kafka_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kafka"
)

func newPersistenceBrokerInfo() kafka.BrokerNodeGroupInfo {
	return kafka.BrokerNodeGroupInfo{
		InstanceType:  "kafka.m5.large",
		ClientSubnets: []string{"subnet-1"},
	}
}

// TestBackend_SnapshotRestore covers basic Snapshot->Restore round trips: a
// backend with a single cluster, and an empty backend.
func TestBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(t *testing.T, b *kafka.InMemoryBackend) string
		verify func(t *testing.T, b *kafka.InMemoryBackend, clusterArn string)
		name   string
	}{
		{
			name: "round_trip_preserves_cluster",
			setup: func(t *testing.T, b *kafka.InMemoryBackend) string {
				t.Helper()

				cl, err := b.CreateCluster(
					context.Background(), "c1", "3.5.1", 3, newPersistenceBrokerInfo(), nil, nil,
				)
				require.NoError(t, err)

				return cl.ClusterArn
			},
			verify: func(t *testing.T, b *kafka.InMemoryBackend, clusterArn string) {
				t.Helper()

				got, err := b.DescribeCluster(context.Background(), clusterArn)
				require.NoError(t, err)
				assert.Equal(t, "c1", got.ClusterName)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(*testing.T, *kafka.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *kafka.InMemoryBackend, _ string) {
				t.Helper()

				assert.Empty(t, b.ListClusters(context.Background()))
				assert.Equal(t, 0, kafka.ClusterCount(b))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := kafka.NewInMemoryBackend(testAccountID, testRegion)
			clusterArn := tt.setup(t, original)

			snap := original.Snapshot(t.Context())
			require.NotNil(t, snap)

			fresh := kafka.NewInMemoryBackend(testAccountID, testRegion)
			require.NoError(t, fresh.Restore(t.Context(), snap))

			tt.verify(t, fresh, clusterArn)
		})
	}
}

// TestBackend_RestoreInvalidData verifies a malformed snapshot is rejected
// with an error rather than partially decoded.
func TestBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestBackend_RestoreIncompatibleVersionResetsToEmpty verifies that a
// snapshot whose version does not match the backend's current
// kafkaSnapshotVersion is discarded (ResetAll, no error) rather than
// partially decoded as the current shape.
func TestBackend_RestoreIncompatibleVersionResetsToEmpty(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b.CreateCluster(context.Background(), "stale", "3.5.1", 3, newPersistenceBrokerInfo(), nil, nil)
	require.NoError(t, err)
	require.Equal(t, 1, kafka.ClusterCount(b))

	staleSnapshot := []byte(
		`{"version":0,"tables":{},"scramSecrets":{},"clusterPolicies":{},"accountID":"","region":""}`,
	)
	require.NoError(t, b.Restore(t.Context(), staleSnapshot))

	assert.Equal(t, 0, kafka.ClusterCount(b))
}

// TestHandler_SnapshotRestoreDelegate verifies Handler.Snapshot/Restore
// delegate to the underlying backend.
func TestHandler_SnapshotRestoreDelegate(t *testing.T) {
	t.Parallel()

	h := kafka.NewHandler(kafka.NewInMemoryBackend(testAccountID, testRegion))
	_, err := h.Backend.CreateCluster(
		context.Background(), "delegate", "3.5.1", 3, newPersistenceBrokerInfo(), nil, nil,
	)
	require.NoError(t, err)

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	h2 := kafka.NewHandler(kafka.NewInMemoryBackend(testAccountID, testRegion))
	require.NoError(t, h2.Restore(t.Context(), snap))

	assert.Len(t, h2.Backend.ListClusters(context.Background()), 1)
}

// TestBackend_SnapshotRestoreFullState exercises a Snapshot->Restore round
// trip across every resource family the Phase 3.3 pkgs/store conversion
// touched: clusters (+ tags + the clustersByName/clustersByRegion indexes),
// configurations, replicators, topics (+ topicsByCluster index), VPC
// connections, cluster operations, and the two raw (non-Table) maps --
// scramSecrets and clusterPolicies. It also proves the indexes rebuilt by
// Restore still function afterward: name-uniqueness rejection on the
// restored cluster's name, and a cascade delete removing its topic.
func TestBackend_SnapshotRestoreFullState(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	original := kafka.NewInMemoryBackend("999999999999", "us-west-2")

	cluster, err := original.CreateCluster(
		ctx, "full-state", "3.5.1", 3, newPersistenceBrokerInfo(), nil, map[string]string{"env": "prod"},
	)
	require.NoError(t, err)

	config, err := original.CreateConfiguration(ctx, "cfg1", "desc", []string{"3.5.1"}, "auto.create=true")
	require.NoError(t, err)
	require.NoError(t, original.TagResource(ctx, config.Arn, map[string]string{"team": "data"}))

	replicator, err := original.CreateReplicator(
		ctx, "repl1", "desc", "arn:aws:iam::999999999999:role/r", nil, nil, nil, nil,
	)
	require.NoError(t, err)

	topic, err := original.CreateTopic(
		ctx, cluster.ClusterArn, "orders", 3, 6, "cmV0ZW50aW9uLm1zPTEwMDA=",
	)
	require.NoError(t, err)

	vpcConn, err := original.CreateVpcConnection(ctx, cluster.ClusterArn, "vpc-1", "SASL_IAM", nil, nil, nil)
	require.NoError(t, err)

	op, err := original.UpdateBrokerCount(ctx, cluster.ClusterArn, 5)
	require.NoError(t, err)

	require.NoError(t, original.PutClusterPolicy(ctx, cluster.ClusterArn, `{"policy":true}`))

	_, err = original.BatchAssociateScramSecret(
		ctx, cluster.ClusterArn, []string{"arn:aws:secretsmanager:us-west-2:999999999999:secret:s1"},
	)
	require.NoError(t, err)

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := kafka.NewInMemoryBackend("other", "eu-west-1")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	assert.Equal(t, "us-west-2", fresh.Region())
	assert.Equal(t, "999999999999", fresh.AccountID())

	gotCluster, err := fresh.DescribeCluster(ctx, cluster.ClusterArn)
	require.NoError(t, err)
	assert.Equal(t, "full-state", gotCluster.ClusterName)

	// Tags are carried on Cluster/Configuration/Replicator/VpcConnection with
	// a json:"-" tag (the AWS wire response never embeds tags in the resource
	// body -- they are fetched separately via ListTagsForResource/GetTags), so
	// they are excluded from the JSON snapshot and do NOT survive a
	// Restore -- this is pre-existing behavior from before the Phase 3.3
	// conversion, preserved unchanged (a mechanical map->store swap, not a fix
	// for this separately-tracked persistence gap).
	gotTags, err := fresh.GetTags(ctx, cluster.ClusterArn)
	require.NoError(t, err)
	assert.Empty(t, gotTags)

	gotConfig, err := fresh.DescribeConfiguration(ctx, config.Arn)
	require.NoError(t, err)
	assert.Equal(t, "cfg1", gotConfig.Name)

	configTags, err := fresh.GetTags(ctx, config.Arn)
	require.NoError(t, err)
	assert.Empty(t, configTags)

	gotReplicator, err := fresh.DescribeReplicator(ctx, replicator.ReplicatorArn)
	require.NoError(t, err)
	assert.Equal(t, "repl1", gotReplicator.ReplicatorName)

	gotTopic, err := fresh.DescribeTopic(ctx, cluster.ClusterArn, topic.TopicName)
	require.NoError(t, err)
	assert.Equal(t, int32(6), gotTopic.PartitionCount)

	gotVpcConn, err := fresh.DescribeVpcConnection(ctx, vpcConn.VpcConnectionArn)
	require.NoError(t, err)
	assert.Equal(t, "vpc-1", gotVpcConn.VpcID)

	gotOp, err := fresh.DescribeClusterOperation(ctx, op.ClusterOperationArn)
	require.NoError(t, err)
	assert.Equal(t, "UPDATE_BROKER_COUNT", gotOp.OperationType)

	policy, err := fresh.GetClusterPolicy(ctx, cluster.ClusterArn)
	require.NoError(t, err)
	assert.JSONEq(t, `{"policy":true}`, policy)

	secrets, err := fresh.ListScramSecrets(ctx, cluster.ClusterArn)
	require.NoError(t, err)
	assert.Equal(t, []string{"arn:aws:secretsmanager:us-west-2:999999999999:secret:s1"}, secrets)

	// The clustersByName index must be rebuilt by Restore: creating another
	// cluster with the same name in the same region must still be rejected.
	_, err = fresh.CreateCluster(ctx, "full-state", "3.5.1", 3, newPersistenceBrokerInfo(), nil, nil)
	require.ErrorIs(t, err, kafka.ErrAlreadyExists)

	// The topicsByCluster index must be rebuilt by Restore: deleting the
	// cluster must cascade-delete its restored topic.
	require.NoError(t, fresh.DeleteCluster(ctx, cluster.ClusterArn))
	_, err = fresh.DescribeTopic(ctx, cluster.ClusterArn, topic.TopicName)
	require.ErrorIs(t, err, kafka.ErrNotFound)
}

func TestPersistenceRoundTripFromSeedHelpers(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	cl := b.AddClusterInternal("c1", "2.8.0")
	b.AddConfigurationInternal("cfg1")
	b.AddReplicatorInternal("rep1")
	b.AddTopicInternal(cl.ClusterArn, "t1")
	b.AddClusterOperationInternal(cl.ClusterArn, "UPDATE")

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := kafka.NewInMemoryBackend("other", "eu-west-1")
	err := b2.Restore(t.Context(), snap)
	require.NoError(t, err)

	assert.Equal(t, 1, kafka.ClusterCount(b2))
	assert.Equal(t, 1, kafka.ConfigurationCount(b2))
	assert.Equal(t, 1, kafka.ReplicatorCount(b2))
	assert.Equal(t, 1, kafka.TopicCount(b2))
	assert.Equal(t, 1, kafka.ClusterOperationCount(b2))
	assert.Equal(t, testAccountID, b2.AccountID())
	assert.Equal(t, testRegion, b2.Region())
}

func TestPersistenceEmptyBackend(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := kafka.NewInMemoryBackend("other", "eu-west-1")
	err := b2.Restore(t.Context(), snap)
	require.NoError(t, err)

	assert.Equal(t, 0, kafka.ClusterCount(b2))
}

func TestPersistence_ServerlessCluster(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	srv := &kafka.ServerlessClusterInfo{
		VpcConfigs: []kafka.ServerlessVpcConfig{
			{SubnetIDs: []string{"subnet-1"}, SecurityGroupIDs: []string{"sg-1"}},
		},
	}
	cl, err := b.CreateServerlessCluster(context.Background(), "srv-persist", srv, map[string]string{"env": "test"})
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := kafka.NewInMemoryBackend("other", "eu-west-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	described, err := b2.DescribeCluster(context.Background(), cl.ClusterArn)
	require.NoError(t, err)
	assert.Equal(t, kafka.ClusterTypeServerless, described.ClusterType)
	require.NotNil(t, described.Serverless)
	require.Len(t, described.Serverless.VpcConfigs, 1)
	assert.Equal(t, []string{"subnet-1"}, described.Serverless.VpcConfigs[0].SubnetIDs)
}

// TestRefinement2_Persistence_EncryptionInfo verifies EncryptionInfo survives snapshot/restore.

func TestPersistence_EncryptionInfo(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	cl := b.AddClusterInternal("enc-persist", "3.5.1")

	stored := kafka.GetStoredCluster(b, cl.ClusterArn)
	stored.EncryptionInfo = &kafka.EncryptionInfo{
		EncryptionAtRest: &kafka.EncryptionAtRest{
			DataVolumeKMSKeyID: "arn:aws:kms:us-east-1:123:key/persist",
		},
		EncryptionInTransit: &kafka.EncryptionInTransit{
			ClientBroker: kafka.EncryptionInTransitTLS,
			InCluster:    true,
		},
	}

	snap := b.Snapshot(t.Context())
	b2 := kafka.NewInMemoryBackend("other", "eu-west-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	described, err := b2.DescribeCluster(context.Background(), cl.ClusterArn)
	require.NoError(t, err)
	require.NotNil(t, described.EncryptionInfo)
	require.NotNil(t, described.EncryptionInfo.EncryptionAtRest)
	assert.Equal(t, "arn:aws:kms:us-east-1:123:key/persist",
		described.EncryptionInfo.EncryptionAtRest.DataVolumeKMSKeyID)
}

// TestRefinement2_Persistence_ConfigurationInfo verifies ConfigurationInfo survives snapshot/restore.

func TestPersistence_ConfigurationInfo(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	cl := b.AddClusterInternal("cfginfo-persist", "3.5.1")

	_, err := b.UpdateClusterConfiguration(context.Background(), cl.ClusterArn,
		"arn:aws:kafka:us-east-1:123:configuration/my-cfg/xyz",
		3)
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	b2 := kafka.NewInMemoryBackend("other", "eu-west-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	described, err := b2.DescribeCluster(context.Background(), cl.ClusterArn)
	require.NoError(t, err)
	require.NotNil(t, described.ConfigurationInfo)
	assert.Equal(t, "arn:aws:kafka:us-east-1:123:configuration/my-cfg/xyz",
		described.ConfigurationInfo.Arn)
	assert.Equal(t, int64(3), described.ConfigurationInfo.Revision)
}

// TestRefinement2_DeepCopy_ProvisionedThroughput verifies no aliasing in ProvisionedThroughput.

func TestHandlerSnapshotFromSeedHelper(t *testing.T) {
	t.Parallel()

	h, backend := newTestHandlerWithBackend(t)
	backend.AddClusterInternal("c1", "2.8.0")

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	h2, backend2 := newTestHandlerWithBackend(t)
	err := h2.Restore(t.Context(), snap)
	require.NoError(t, err)
	assert.Equal(t, 1, kafka.ClusterCount(backend2))
}
