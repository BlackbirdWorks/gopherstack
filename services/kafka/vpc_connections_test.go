package kafka_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kafka"
)

func TestCreateVpcConnection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*kafka.InMemoryBackend) string
		name    string
		wantErr bool
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
		},
		{
			name: "cluster_not_found",
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
			clusterArn := tt.setup(b)

			conn, err := b.CreateVpcConnection(context.Background(), clusterArn, "vpc-12345", "SASL_IAM", nil, nil, nil)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, conn.VpcConnectionArn)
			assert.Equal(t, clusterArn, conn.TargetClusterArn)
			assert.Equal(t, kafka.VpcConnectionStateAvailable, conn.State)
		})
	}
}

func TestDeleteVpcConnection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*kafka.InMemoryBackend) string
		name    string
		wantErr bool
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
				conn, _ := b.CreateVpcConnection(
					context.Background(), c.ClusterArn, "vpc-12345", "SASL_IAM", nil, nil, nil,
				)

				return conn.VpcConnectionArn
			},
		},
		{
			name: "not_found",
			setup: func(_ *kafka.InMemoryBackend) string {
				return "arn:aws:kafka:us-east-1:000000000000:vpc-connection/nonexistent"
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			vpcConnectionArn := tt.setup(b)

			err := b.DeleteVpcConnection(context.Background(), vpcConnectionArn)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestNonNilTags_VpcConnection(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	cl := b.AddClusterInternal("c1", "2.8.0")
	vpc := b.AddVpcConnectionInternal(cl.ClusterArn, "vpc-1")

	assert.NotNil(t, vpc.Tags)
}
