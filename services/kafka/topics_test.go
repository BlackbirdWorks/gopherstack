package kafka_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kafka"
)

func TestCreateTopic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(*kafka.InMemoryBackend) string
		name      string
		topicName string
		wantErr   bool
	}{
		{
			name: "success",
			setup: func(b *kafka.InMemoryBackend) string {
				c, _ := b.CreateCluster(
					context.Background(),
					"my-cluster",
					"2.8.0",
					3,
					kafka.BrokerNodeGroupInfo{},
					nil,
					nil,
				)

				return c.ClusterArn
			},
			topicName: "my-topic",
		},
		{
			name: "duplicate_topic",
			setup: func(b *kafka.InMemoryBackend) string {
				c, _ := b.CreateCluster(
					context.Background(),
					"my-cluster",
					"2.8.0",
					3,
					kafka.BrokerNodeGroupInfo{},
					nil,
					nil,
				)
				_, _ = b.CreateTopic(context.Background(), c.ClusterArn, "my-topic", 1, 3, nil)

				return c.ClusterArn
			},
			topicName: "my-topic",
			wantErr:   true,
		},
		{
			name: "cluster_not_found",
			setup: func(_ *kafka.InMemoryBackend) string {
				return "arn:aws:kafka:us-east-1:000000000000:cluster/nonexistent/uuid"
			},
			topicName: "my-topic",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			clusterArn := tt.setup(b)

			topic, err := b.CreateTopic(context.Background(), clusterArn, tt.topicName, 1, 3, nil)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.topicName, topic.TopicName)
			assert.Equal(t, clusterArn, topic.ClusterArn)
		})
	}
}

func TestDeleteTopic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*kafka.InMemoryBackend) (string, string)
		name    string
		wantErr bool
	}{
		{
			name: "success",
			setup: func(b *kafka.InMemoryBackend) (string, string) {
				c, _ := b.CreateCluster(
					context.Background(),
					"my-cluster",
					"2.8.0",
					3,
					kafka.BrokerNodeGroupInfo{},
					nil,
					nil,
				)
				_, _ = b.CreateTopic(context.Background(), c.ClusterArn, "my-topic", 1, 3, nil)

				return c.ClusterArn, "my-topic"
			},
		},
		{
			name: "topic_not_found",
			setup: func(b *kafka.InMemoryBackend) (string, string) {
				c, _ := b.CreateCluster(
					context.Background(),
					"my-cluster",
					"2.8.0",
					3,
					kafka.BrokerNodeGroupInfo{},
					nil,
					nil,
				)

				return c.ClusterArn, "nonexistent-topic"
			},
			wantErr: true,
		},
		{
			name: "cluster_not_found",
			setup: func(_ *kafka.InMemoryBackend) (string, string) {
				return "arn:aws:kafka:us-east-1:000000000000:cluster/nonexistent/uuid", "my-topic"
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			clusterArn, topicName := tt.setup(b)

			err := b.DeleteTopic(context.Background(), clusterArn, topicName)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestCreateTopic_RequiresName(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	cl := b.AddClusterInternal("c1", "2.8.0")
	_, err := b.CreateTopic(context.Background(), cl.ClusterArn, "", 3, 1, nil)

	require.Error(t, err)
	require.ErrorIs(t, err, kafka.ErrValidation)
}
