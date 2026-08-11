package kafka_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kafka"
)

// s3ChannelFixtures returns a valid S3 destination config plus the single
// required topic configuration entry, for use across CreateChannel tests.
func s3ChannelFixtures() (*kafka.S3DestinationConfiguration, []kafka.TopicConfiguration) {
	s3Dest := &kafka.S3DestinationConfiguration{
		DeadLetterQueueS3:       &kafka.DeadLetterQueueS3{BucketArn: "arn:aws:s3:::dlq-bucket"},
		ServiceExecutionRoleArn: "arn:aws:iam::000000000000:role/channel-role",
		Storage: &kafka.S3Storage{
			BucketArn:       "arn:aws:s3:::dest-bucket",
			CompressionType: "GZIP",
			StorageClass:    "STANDARD",
		},
	}

	topics := []kafka.TopicConfiguration{
		{
			RecordConverter: &kafka.RecordConverter{ValueConverter: "JSON"},
			TopicArn:        "arn:aws:kafka:us-east-1:000000000000:topic/my-cluster/uuid/my-topic",
		},
	}

	return s3Dest, topics
}

func icebergChannelFixtures() (*kafka.IcebergDestinationConfiguration, []kafka.TopicConfiguration) {
	appendOnly := true
	icebergDest := &kafka.IcebergDestinationConfiguration{
		AppendOnly:              appendOnly,
		DeadLetterQueueS3:       &kafka.DeadLetterQueueS3{BucketArn: "arn:aws:s3:::dlq-bucket"},
		DestinationTableList:    []kafka.DestinationTable{{DestinationTableName: "t1"}},
		SchemaEvolution:         &kafka.SchemaEvolution{},
		ServiceExecutionRoleArn: "arn:aws:iam::000000000000:role/channel-role",
		TableCreation:           &kafka.TableCreation{EnableTableCreation: true},
	}

	topics := []kafka.TopicConfiguration{
		{
			RecordConverter: &kafka.RecordConverter{ValueConverter: "JSON"},
			TopicArn:        "arn:aws:kafka:us-east-1:000000000000:topic/my-cluster/uuid/my-topic",
		},
	}

	return icebergDest, topics
}

func TestCreateChannel(t *testing.T) {
	t.Parallel()

	t.Run("success_s3", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend(t)
		cl := b.AddClusterInternal("my-cluster", "3.6.0")
		s3Dest, topics := s3ChannelFixtures()

		ch, err := b.CreateChannel(
			context.Background(), cl.ClusterArn, "my-channel", topics, nil, nil, s3Dest, nil, nil,
		)

		require.NoError(t, err)
		assert.Equal(t, "my-channel", ch.ChannelName)
		assert.Equal(t, kafka.ChannelDestinationTypeS3, ch.DestinationType)
		assert.Equal(t, kafka.ChannelStatusActive, ch.Status)
		assert.NotEmpty(t, ch.ChannelArn)
		assert.NotEmpty(t, ch.ClusterOperationArn)
	})

	t.Run("success_iceberg", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend(t)
		cl := b.AddClusterInternal("my-cluster", "3.6.0")
		icebergDest, topics := icebergChannelFixtures()

		ch, err := b.CreateChannel(
			context.Background(), cl.ClusterArn, "my-channel", topics, nil, icebergDest, nil, nil, nil,
		)

		require.NoError(t, err)
		assert.Equal(t, kafka.ChannelDestinationTypeIceberg, ch.DestinationType)
	})

	t.Run("cluster_not_found", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend(t)
		s3Dest, topics := s3ChannelFixtures()

		_, err := b.CreateChannel(
			context.Background(),
			"arn:aws:kafka:us-east-1:000000000000:cluster/nonexistent/uuid",
			"my-channel", topics, nil, nil, s3Dest, nil, nil,
		)

		require.ErrorIs(t, err, kafka.ErrNotFound)
	})

	t.Run("duplicate_channel_name", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend(t)
		cl := b.AddClusterInternal("my-cluster", "3.6.0")
		s3Dest, topics := s3ChannelFixtures()

		_, err := b.CreateChannel(
			context.Background(), cl.ClusterArn, "my-channel", topics, nil, nil, s3Dest, nil, nil,
		)
		require.NoError(t, err)

		_, err = b.CreateChannel(
			context.Background(), cl.ClusterArn, "my-channel", topics, nil, nil, s3Dest, nil, nil,
		)
		require.ErrorIs(t, err, kafka.ErrAlreadyExists)
	})

	t.Run("missing_channel_name", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend(t)
		cl := b.AddClusterInternal("my-cluster", "3.6.0")
		s3Dest, topics := s3ChannelFixtures()

		_, err := b.CreateChannel(context.Background(), cl.ClusterArn, "", topics, nil, nil, s3Dest, nil, nil)
		require.ErrorIs(t, err, kafka.ErrValidation)
	})

	t.Run("wrong_topic_configuration_count", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend(t)
		cl := b.AddClusterInternal("my-cluster", "3.6.0")
		s3Dest, _ := s3ChannelFixtures()

		_, err := b.CreateChannel(
			context.Background(), cl.ClusterArn, "my-channel", nil, nil, nil, s3Dest, nil, nil,
		)
		require.ErrorIs(t, err, kafka.ErrValidation)
	})

	t.Run("neither_destination_specified", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend(t)
		cl := b.AddClusterInternal("my-cluster", "3.6.0")
		_, topics := s3ChannelFixtures()

		_, err := b.CreateChannel(
			context.Background(), cl.ClusterArn, "my-channel", topics, nil, nil, nil, nil, nil,
		)
		require.ErrorIs(t, err, kafka.ErrValidation)
	})

	t.Run("both_destinations_specified", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend(t)
		cl := b.AddClusterInternal("my-cluster", "3.6.0")
		s3Dest, topics := s3ChannelFixtures()
		icebergDest, _ := icebergChannelFixtures()

		_, err := b.CreateChannel(
			context.Background(), cl.ClusterArn, "my-channel", topics, nil, icebergDest, s3Dest, nil, nil,
		)
		require.ErrorIs(t, err, kafka.ErrValidation)
	})

	t.Run("s3_destination_missing_storage", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend(t)
		cl := b.AddClusterInternal("my-cluster", "3.6.0")
		_, topics := s3ChannelFixtures()

		invalid := &kafka.S3DestinationConfiguration{
			DeadLetterQueueS3:       &kafka.DeadLetterQueueS3{BucketArn: "arn:aws:s3:::dlq-bucket"},
			ServiceExecutionRoleArn: "arn:aws:iam::000000000000:role/channel-role",
		}

		_, err := b.CreateChannel(
			context.Background(), cl.ClusterArn, "my-channel", topics, nil, nil, invalid, nil, nil,
		)
		require.ErrorIs(t, err, kafka.ErrValidation)
	})

	t.Run("iceberg_destination_missing_table_creation", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend(t)
		cl := b.AddClusterInternal("my-cluster", "3.6.0")
		_, topics := s3ChannelFixtures()

		invalid := &kafka.IcebergDestinationConfiguration{
			AppendOnly:              true,
			DeadLetterQueueS3:       &kafka.DeadLetterQueueS3{BucketArn: "arn:aws:s3:::dlq-bucket"},
			DestinationTableList:    []kafka.DestinationTable{{DestinationTableName: "t1"}},
			SchemaEvolution:         &kafka.SchemaEvolution{},
			ServiceExecutionRoleArn: "arn:aws:iam::000000000000:role/channel-role",
		}

		_, err := b.CreateChannel(
			context.Background(), cl.ClusterArn, "my-channel", topics, nil, invalid, nil, nil, nil,
		)
		require.ErrorIs(t, err, kafka.ErrValidation)
	})

	t.Run("topic_configuration_missing_record_converter", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend(t)
		cl := b.AddClusterInternal("my-cluster", "3.6.0")
		s3Dest, _ := s3ChannelFixtures()

		badTopics := []kafka.TopicConfiguration{
			{TopicArn: "arn:aws:kafka:us-east-1:000000000000:topic/my-cluster/uuid/my-topic"},
		}

		_, err := b.CreateChannel(
			context.Background(), cl.ClusterArn, "my-channel", badTopics, nil, nil, s3Dest, nil, nil,
		)
		require.ErrorIs(t, err, kafka.ErrValidation)
	})
}

func TestChannelLifecycle(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newTestBackend(t)
	cl := b.AddClusterInternal("my-cluster", "3.6.0")
	s3Dest, topics := s3ChannelFixtures()

	created, err := b.CreateChannel(ctx, cl.ClusterArn, "my-channel", topics, nil, nil, s3Dest, nil, nil)
	require.NoError(t, err)

	// DescribeChannel finds it.
	described, err := b.DescribeChannel(ctx, cl.ClusterArn, created.ChannelArn)
	require.NoError(t, err)
	assert.Equal(t, "my-channel", described.ChannelName)
	assert.Empty(t, described.ClusterOperationArn, "clusterOperationArn only appears on the mutating response")

	// ListChannels finds it.
	list, err := b.ListChannels(ctx, cl.ClusterArn, "")
	require.NoError(t, err)
	require.Len(t, list, 1)
	assert.Equal(t, created.ChannelArn, list[0].ChannelArn)

	// ListChannels topicNameFilter matches by exact topic name.
	filtered, err := b.ListChannels(ctx, cl.ClusterArn, "my-topic")
	require.NoError(t, err)
	assert.Len(t, filtered, 1)

	notFiltered, err := b.ListChannels(ctx, cl.ClusterArn, "other-topic")
	require.NoError(t, err)
	assert.Empty(t, notFiltered)

	// UpdateChannel mutates the S3 destination's DataFreshnessInSeconds.
	update := &kafka.S3DestinationUpdate{DataFreshnessInSeconds: 600}
	updated, err := b.UpdateChannel(ctx, cl.ClusterArn, created.ChannelArn, nil, update)
	require.NoError(t, err)
	assert.NotEmpty(t, updated.ClusterOperationArn)

	describedAfterUpdate, err := b.DescribeChannel(ctx, cl.ClusterArn, created.ChannelArn)
	require.NoError(t, err)
	require.NotNil(t, describedAfterUpdate.S3DestinationConfiguration)
	assert.Equal(t, int32(600), describedAfterUpdate.S3DestinationConfiguration.DataFreshnessInSeconds)

	// DeleteChannel removes it.
	deleted, err := b.DeleteChannel(ctx, cl.ClusterArn, created.ChannelArn)
	require.NoError(t, err)
	assert.Equal(t, created.ChannelArn, deleted.ChannelArn)
	assert.NotEmpty(t, deleted.ClusterOperationArn)

	_, err = b.DescribeChannel(ctx, cl.ClusterArn, created.ChannelArn)
	require.ErrorIs(t, err, kafka.ErrNotFound)

	list, err = b.ListChannels(ctx, cl.ClusterArn, "")
	require.NoError(t, err)
	assert.Empty(t, list)
}

func TestDescribeChannel_NotFound(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newTestBackend(t)
	cl := b.AddClusterInternal("my-cluster", "3.6.0")

	_, err := b.DescribeChannel(ctx, cl.ClusterArn, "arn:aws:kafka:us-east-1:000000000000:channel/x/y/nonexistent")
	require.ErrorIs(t, err, kafka.ErrNotFound)
}

func TestDescribeChannel_WrongClusterScope(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newTestBackend(t)
	cl1 := b.AddClusterInternal("cluster-1", "3.6.0")
	cl2 := b.AddClusterInternal("cluster-2", "3.6.0")
	s3Dest, topics := s3ChannelFixtures()

	ch, err := b.CreateChannel(ctx, cl1.ClusterArn, "my-channel", topics, nil, nil, s3Dest, nil, nil)
	require.NoError(t, err)

	// Describing the channel under the wrong cluster ARN must 404, matching
	// real MSK's cluster-scoped Channel resource model.
	_, err = b.DescribeChannel(ctx, cl2.ClusterArn, ch.ChannelArn)
	require.ErrorIs(t, err, kafka.ErrNotFound)

	_, err = b.DeleteChannel(ctx, cl2.ClusterArn, ch.ChannelArn)
	require.ErrorIs(t, err, kafka.ErrNotFound)

	_, err = b.UpdateChannel(
		ctx, cl2.ClusterArn, ch.ChannelArn, nil, &kafka.S3DestinationUpdate{DataFreshnessInSeconds: 300},
	)
	require.ErrorIs(t, err, kafka.ErrNotFound)
}

func TestListChannels_ClusterNotFound(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)

	_, err := b.ListChannels(
		context.Background(), "arn:aws:kafka:us-east-1:000000000000:cluster/nonexistent/uuid", "",
	)
	require.ErrorIs(t, err, kafka.ErrNotFound)
}

func TestUpdateChannel_DestinationTypeMismatch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newTestBackend(t)
	cl := b.AddClusterInternal("my-cluster", "3.6.0")
	s3Dest, topics := s3ChannelFixtures()

	ch, err := b.CreateChannel(ctx, cl.ClusterArn, "my-channel", topics, nil, nil, s3Dest, nil, nil)
	require.NoError(t, err)

	// Channel was created with an S3 destination; applying an Iceberg update
	// must be rejected (real MSK: "the destination type cannot be changed").
	_, err = b.UpdateChannel(
		ctx, cl.ClusterArn, ch.ChannelArn, &kafka.IcebergDestinationUpdate{DataFreshnessInSeconds: 300}, nil,
	)
	require.ErrorIs(t, err, kafka.ErrValidation)
}

func TestUpdateChannel_RequiresExactlyOneDestinationUpdate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newTestBackend(t)
	cl := b.AddClusterInternal("my-cluster", "3.6.0")
	s3Dest, topics := s3ChannelFixtures()

	ch, err := b.CreateChannel(ctx, cl.ClusterArn, "my-channel", topics, nil, nil, s3Dest, nil, nil)
	require.NoError(t, err)

	_, err = b.UpdateChannel(ctx, cl.ClusterArn, ch.ChannelArn, nil, nil)
	require.ErrorIs(t, err, kafka.ErrValidation)

	_, err = b.UpdateChannel(
		ctx, cl.ClusterArn, ch.ChannelArn,
		&kafka.IcebergDestinationUpdate{DataFreshnessInSeconds: 300},
		&kafka.S3DestinationUpdate{DataFreshnessInSeconds: 300},
	)
	require.ErrorIs(t, err, kafka.ErrValidation)
}

func TestDeleteChannel_NotFound(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newTestBackend(t)
	cl := b.AddClusterInternal("my-cluster", "3.6.0")

	_, err := b.DeleteChannel(ctx, cl.ClusterArn, "arn:aws:kafka:us-east-1:000000000000:channel/x/y/nonexistent")
	require.ErrorIs(t, err, kafka.ErrNotFound)
}

func TestChannel_TagResourceLifecycle(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newTestBackend(t)
	cl := b.AddClusterInternal("my-cluster", "3.6.0")
	s3Dest, topics := s3ChannelFixtures()

	ch, err := b.CreateChannel(
		ctx, cl.ClusterArn, "my-channel", topics, nil, nil, s3Dest, nil, map[string]string{"env": "prod"},
	)
	require.NoError(t, err)

	tags, err := b.GetTags(ctx, ch.ChannelArn)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"env": "prod"}, tags)

	require.NoError(t, b.TagResource(ctx, ch.ChannelArn, map[string]string{"team": "data"}))

	tags, err = b.GetTags(ctx, ch.ChannelArn)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"env": "prod", "team": "data"}, tags)

	require.NoError(t, b.UntagResource(ctx, ch.ChannelArn, []string{"env"}))

	tags, err = b.GetTags(ctx, ch.ChannelArn)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"team": "data"}, tags)

	// DescribeChannel echoes the same tags directly (unlike Cluster/
	// Configuration/Replicator/VpcConnection, whose wire response omits
	// tags entirely).
	described, err := b.DescribeChannel(ctx, cl.ClusterArn, ch.ChannelArn)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"team": "data"}, described.Tags)
}

func TestChannel_SnapshotRestoreRoundTrip(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	original := kafka.NewInMemoryBackend(testAccountID, testRegion)
	cl, err := original.CreateCluster(
		ctx, "chan-persist", "3.6.0", 3, kafka.BrokerNodeGroupInfo{}, nil, nil,
	)
	require.NoError(t, err)

	s3Dest, topics := s3ChannelFixtures()
	created, err := original.CreateChannel(
		ctx, cl.ClusterArn, "persist-channel", topics, nil, nil, s3Dest, nil, map[string]string{"env": "prod"},
	)
	require.NoError(t, err)

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := kafka.NewInMemoryBackend("other", "eu-west-1")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	restored, err := fresh.DescribeChannel(ctx, cl.ClusterArn, created.ChannelArn)
	require.NoError(t, err)
	assert.Equal(t, "persist-channel", restored.ChannelName)
	assert.Equal(t, kafka.ChannelDestinationTypeS3, restored.DestinationType)
	require.NotNil(t, restored.S3DestinationConfiguration)
	assert.Equal(t, "arn:aws:s3:::dest-bucket", restored.S3DestinationConfiguration.Storage.BucketArn)
	// Unlike Cluster/Configuration/Replicator/VpcConnection tags, Channel
	// tags carry a normal JSON tag and survive the round trip.
	assert.Equal(t, map[string]string{"env": "prod"}, restored.Tags)

	// The channelsByCluster index must be rebuilt by Restore too.
	list, err := fresh.ListChannels(ctx, cl.ClusterArn, "")
	require.NoError(t, err)
	require.Len(t, list, 1)
	assert.Equal(t, created.ChannelArn, list[0].ChannelArn)
}
