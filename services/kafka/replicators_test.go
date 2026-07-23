package kafka_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kafka"
)

func TestCreateReplicator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*kafka.InMemoryBackend)
		name    string
		repName string
		wantErr bool
	}{
		{
			name:    "success",
			repName: "my-replicator",
			setup:   func(_ *kafka.InMemoryBackend) {},
		},
		{
			name:    "duplicate_name",
			repName: "my-replicator",
			setup: func(b *kafka.InMemoryBackend) {
				_, _ = b.CreateReplicator(
					context.Background(),
					"my-replicator",
					"",
					"arn:aws:iam::000000000000:role/my-role",
					nil,
					nil,
					nil,
				)
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			tt.setup(b)

			replicator, err := b.CreateReplicator(context.Background(),
				tt.repName,
				"test replicator",
				"arn:aws:iam::000000000000:role/my-role",
				nil,
				nil,
				nil,
			)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.repName, replicator.ReplicatorName)
			assert.NotEmpty(t, replicator.ReplicatorArn)
			assert.NotEmpty(t, replicator.CurrentVersion)
			assert.NotEmpty(t, replicator.CreationTime)
			assert.Equal(t, kafka.ReplicatorStateRunning, replicator.ReplicatorState)
		})
	}
}

// TestCreateReplicator_TopologyAndAliasResolution locks in the CreateReplicator
// gap fix: kafkaClusters/replicationInfoList are accepted, persisted, and
// DescribeReplicator/ListReplicators resolve KafkaClusterAlias /
// SourceKafkaClusterAlias / TargetKafkaClusterAlias from the real name of the
// referenced MSK cluster (falling back to the ARN's last path segment when
// the cluster doesn't exist in this backend).
func TestCreateReplicator_TopologyAndAliasResolution(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	source := b.AddClusterInternal("source-cluster", "3.5.1")
	target := b.AddClusterInternal("target-cluster", "3.5.1")

	kafkaClusters := []kafka.ClusterConfig{
		{MskClusterArn: source.ClusterArn, SubnetIDs: []string{"subnet-1"}},
		{MskClusterArn: target.ClusterArn, SubnetIDs: []string{"subnet-2"}},
	}
	replicationInfoList := []kafka.ReplicationInfoConfig{
		{
			SourceKafkaClusterArn: source.ClusterArn,
			TargetKafkaClusterArn: target.ClusterArn,
			TargetCompressionType: "GZIP",
			TopicReplication: kafka.TopicReplicationConfig{
				TopicsToReplicate: []string{".*"},
			},
			ConsumerGroupReplication: kafka.ConsumerGroupReplicationConfig{
				ConsumerGroupsToReplicate: []string{".*"},
			},
		},
	}

	replicator, err := b.CreateReplicator(
		context.Background(), "topo-replicator", "", "arn:aws:iam::000000000000:role/r",
		kafkaClusters, replicationInfoList, nil,
	)
	require.NoError(t, err)
	require.Len(t, replicator.KafkaClusters, 2)
	assert.Equal(t, "source-cluster", replicator.KafkaClusters[0].Alias)
	assert.Equal(t, "target-cluster", replicator.KafkaClusters[1].Alias)
	require.Len(t, replicator.ReplicationInfoList, 1)
	assert.Equal(t, "source-cluster", replicator.ReplicationInfoList[0].SourceAlias)
	assert.Equal(t, "target-cluster", replicator.ReplicationInfoList[0].TargetAlias)

	// DescribeReplicator resolves the same aliases on a fresh read.
	described, err := b.DescribeReplicator(context.Background(), replicator.ReplicatorArn)
	require.NoError(t, err)
	assert.Equal(t, "source-cluster", described.KafkaClusters[0].Alias)

	// A referenced cluster that doesn't exist in this backend falls back to
	// the ARN's trailing resource segment instead of an empty alias.
	unknownArn := "arn:aws:kafka:us-east-1:000000000000:cluster/ghost-cluster/uuid-1"
	ghost, err := b.CreateReplicator(
		context.Background(), "ghost-replicator", "", "arn:aws:iam::000000000000:role/r",
		[]kafka.ClusterConfig{{MskClusterArn: unknownArn}}, nil, nil,
	)
	require.NoError(t, err)
	assert.Equal(t, "uuid-1", ghost.KafkaClusters[0].Alias)
}

func TestDeleteReplicator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*kafka.InMemoryBackend) string
		name    string
		wantErr bool
	}{
		{
			name: "success",
			setup: func(b *kafka.InMemoryBackend) string {
				r, _ := b.CreateReplicator(
					context.Background(),
					"my-replicator",
					"",
					"arn:aws:iam::000000000000:role/my-role",
					nil,
					nil,
					nil,
				)

				return r.ReplicatorArn
			},
		},
		{
			name: "not_found",
			setup: func(_ *kafka.InMemoryBackend) string {
				return "arn:aws:kafka:us-east-1:000000000000:replicator/nonexistent/uuid"
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			replicatorArn := tt.setup(b)

			err := b.DeleteReplicator(context.Background(), replicatorArn)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestCreateReplicator_RequiresName(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b.CreateReplicator(context.Background(), "", "", "", nil, nil, nil)

	require.Error(t, err)
	require.ErrorIs(t, err, kafka.ErrValidation)
}

func TestNonNilTags_Replicator(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	rep := b.AddReplicatorInternal("rep1")

	assert.NotNil(t, rep.Tags)
}

// replicationInfoFixture builds a fresh backend with one replicator whose
// single replication flow runs source -> target, for UpdateReplicationInfo tests.
func replicationInfoFixture(
	t *testing.T,
) (*kafka.InMemoryBackend, *kafka.Replicator, string, string) {
	t.Helper()

	b := newTestBackend(t)
	source := b.AddClusterInternal("upd-source", "3.5.1")
	target := b.AddClusterInternal("upd-target", "3.5.1")

	replicator, err := b.CreateReplicator(
		context.Background(), "upd-replicator", "", "arn:aws:iam::000000000000:role/r",
		nil,
		[]kafka.ReplicationInfoConfig{{
			SourceKafkaClusterArn: source.ClusterArn,
			TargetKafkaClusterArn: target.ClusterArn,
		}},
		nil,
	)
	require.NoError(t, err)

	return b, replicator, source.ClusterArn, target.ClusterArn
}

// TestUpdateReplicationInfo_Backend locks in UpdateReplicationInfo's real
// wire contract: it requires the caller's currentVersion to match, and it
// mutates the specific (source, target) replication flow, not a free-form
// description field.
func TestUpdateReplicationInfo_Backend(t *testing.T) {
	t.Parallel()

	t.Run("wrong_version_rejected", func(t *testing.T) {
		t.Parallel()

		b, replicator, sourceArn, targetArn := replicationInfoFixture(t)

		_, err := b.UpdateReplicationInfo(
			context.Background(), replicator.ReplicatorArn, "WRONG", sourceArn, targetArn, nil, nil,
		)
		require.ErrorIs(t, err, kafka.ErrValidation)
	})

	t.Run("unknown_flow_not_found", func(t *testing.T) {
		t.Parallel()

		b, replicator, _, targetArn := replicationInfoFixture(t)

		_, err := b.UpdateReplicationInfo(
			context.Background(), replicator.ReplicatorArn, replicator.CurrentVersion,
			"arn:aws:kafka:us-east-1:000000000000:cluster/no-such/uuid", targetArn, nil, nil,
		)
		require.ErrorIs(t, err, kafka.ErrNotFound)
	})

	t.Run("success_bumps_version_and_persists_topic_replication", func(t *testing.T) {
		t.Parallel()

		b, replicator, sourceArn, targetArn := replicationInfoFixture(t)
		newTopicReplication := &kafka.TopicReplicationConfig{TopicsToReplicate: []string{"orders.*"}}

		updated, err := b.UpdateReplicationInfo(
			context.Background(), replicator.ReplicatorArn, replicator.CurrentVersion,
			sourceArn, targetArn, newTopicReplication, nil,
		)
		require.NoError(t, err)
		assert.NotEqual(t, replicator.CurrentVersion, updated.CurrentVersion)
		require.Len(t, updated.ReplicationInfoList, 1)
		assert.Equal(t, []string{"orders.*"}, updated.ReplicationInfoList[0].TopicReplication.TopicsToReplicate)
	})

	t.Run("replicator_not_found", func(t *testing.T) {
		t.Parallel()

		b, _, sourceArn, targetArn := replicationInfoFixture(t)

		_, err := b.UpdateReplicationInfo(
			context.Background(), "arn:aws:kafka:us-east-1:000000000000:replicator/missing/uuid",
			"v1", sourceArn, targetArn, nil, nil,
		)
		require.ErrorIs(t, err, kafka.ErrNotFound)
	})
}
