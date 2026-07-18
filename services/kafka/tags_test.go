package kafka_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kafka"
)

func TestTagOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(*kafka.InMemoryBackend) string
		tags      map[string]string
		wantTags  map[string]string
		name      string
		removKeys []string
		wantErr   bool
	}{
		{
			name: "tag_and_untag_cluster",
			setup: func(b *kafka.InMemoryBackend) string {
				c, _ := b.CreateCluster(
					context.Background(),
					"tagged-cluster",
					"2.8.0",
					3,
					kafka.BrokerNodeGroupInfo{},
					nil,
					nil,
				)

				return c.ClusterArn
			},
			tags:      map[string]string{"env": "prod", "team": "platform"},
			removKeys: []string{"team"},
			wantTags:  map[string]string{"env": "prod"},
		},
		{
			name: "tag_not_found",
			setup: func(_ *kafka.InMemoryBackend) string {
				return "arn:aws:kafka:us-east-1:000000000000:cluster/nonexistent/uuid"
			},
			tags:    map[string]string{"env": "prod"},
			wantErr: true,
		},
		{
			name: "get_tags_not_found",
			setup: func(_ *kafka.InMemoryBackend) string {
				return "arn:aws:kafka:us-east-1:000000000000:cluster/nonexistent/uuid"
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			arn := tt.setup(b)

			if tt.tags != nil {
				err := b.TagResource(context.Background(), arn, tt.tags)

				if tt.wantErr {
					require.Error(t, err)

					return
				}

				require.NoError(t, err)
			}

			if tt.removKeys != nil {
				err := b.UntagResource(context.Background(), arn, tt.removKeys)
				require.NoError(t, err)
			}

			if !tt.wantErr && tt.wantTags != nil {
				got, err := b.GetTags(context.Background(), arn)
				require.NoError(t, err)
				assert.Equal(t, tt.wantTags, got)
			}

			if tt.wantErr && tt.tags == nil {
				_, err := b.GetTags(context.Background(), arn)
				require.Error(t, err)
			}
		})
	}
}

func TestTagResource_Replicator(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	rep := b.AddReplicatorInternal("rep1")

	err := b.TagResource(context.Background(), rep.ReplicatorArn, map[string]string{"env": "prod"})
	require.NoError(t, err)

	tags, err := b.GetTags(context.Background(), rep.ReplicatorArn)
	require.NoError(t, err)
	assert.Equal(t, "prod", tags["env"])
}

func TestTagResource_VpcConnection(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	cl := b.AddClusterInternal("c1", "2.8.0")
	vpc := b.AddVpcConnectionInternal(cl.ClusterArn, "vpc-1")

	err := b.TagResource(context.Background(), vpc.VpcConnectionArn, map[string]string{"team": "infra"})
	require.NoError(t, err)

	tags, err := b.GetTags(context.Background(), vpc.VpcConnectionArn)
	require.NoError(t, err)
	assert.Equal(t, "infra", tags["team"])
}
