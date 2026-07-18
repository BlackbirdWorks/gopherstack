package kafka_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kafka"
)

func TestBatchAssociateScramSecret(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*kafka.InMemoryBackend) string
		name       string
		secretArns []string
		wantErr    bool
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
			secretArns: []string{"arn:aws:secretsmanager:us-east-1:000000000000:secret/my-secret"},
		},
		{
			name: "cluster_not_found",
			setup: func(_ *kafka.InMemoryBackend) string {
				return "arn:aws:kafka:us-east-1:000000000000:cluster/nonexistent/uuid"
			},
			secretArns: []string{"arn:aws:secretsmanager:us-east-1:000000000000:secret/my-secret"},
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			clusterArn := tt.setup(b)

			errs, err := b.BatchAssociateScramSecret(context.Background(), clusterArn, tt.secretArns)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Empty(t, errs)
		})
	}
}

func TestBatchDisassociateScramSecret(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*kafka.InMemoryBackend) string
		name       string
		secretArns []string
		wantErr    bool
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
				_, _ = b.BatchAssociateScramSecret(context.Background(),
					c.ClusterArn,
					[]string{"arn:aws:secretsmanager:us-east-1:000000000000:secret/my-secret"},
				)

				return c.ClusterArn
			},
			secretArns: []string{"arn:aws:secretsmanager:us-east-1:000000000000:secret/my-secret"},
		},
		{
			name: "cluster_not_found",
			setup: func(_ *kafka.InMemoryBackend) string {
				return "arn:aws:kafka:us-east-1:000000000000:cluster/nonexistent/uuid"
			},
			secretArns: []string{"arn:aws:secretsmanager:us-east-1:000000000000:secret/my-secret"},
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			clusterArn := tt.setup(b)

			errs, err := b.BatchDisassociateScramSecret(context.Background(), clusterArn, tt.secretArns)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Empty(t, errs)
		})
	}
}

func TestScramSecretCount(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	cl := b.AddClusterInternal("c1", "2.8.0")

	secrets := []string{
		"arn:aws:secretsmanager:us-east-1:000000000000:secret:s1",
		"arn:aws:secretsmanager:us-east-1:000000000000:secret:s2",
	}
	_, err := b.BatchAssociateScramSecret(context.Background(), cl.ClusterArn, secrets)
	require.NoError(t, err)
	assert.Equal(t, 2, kafka.ScramSecretCount(b))

	_, err = b.BatchDisassociateScramSecret(context.Background(), cl.ClusterArn, secrets[:1])
	require.NoError(t, err)
	assert.Equal(t, 1, kafka.ScramSecretCount(b))
}
