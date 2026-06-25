package kafka_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kafka"
)

const (
	testAccountID = "000000000000"
	testRegion    = "us-east-1"
)

func newTestBackend(t *testing.T) *kafka.InMemoryBackend {
	t.Helper()

	return kafka.NewInMemoryBackend(testAccountID, testRegion)
}

func TestBackend_Region(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	assert.Equal(t, testRegion, b.Region())
}

func TestBackend_AccountID(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	assert.Equal(t, testAccountID, b.AccountID())
}

func TestBackend_CreateCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		clusterName string
		wantErr     bool
	}{
		{
			name:        "success",
			clusterName: "my-cluster",
		},
		{
			name:        "duplicate_name",
			clusterName: "my-cluster",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)

			// Pre-create if testing duplicate
			if tt.wantErr {
				_, err := b.CreateCluster(
					context.Background(),
					"my-cluster",
					"2.8.0",
					3,
					kafka.BrokerNodeGroupInfo{},
					nil,
					nil,
				)
				require.NoError(t, err)
			}

			cluster, err := b.CreateCluster(context.Background(),
				tt.clusterName,
				"2.8.0",
				3,
				kafka.BrokerNodeGroupInfo{},
				nil,
				map[string]string{"env": "test"},
			)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.clusterName, cluster.ClusterName)
			assert.Equal(t, kafka.ClusterStateCreating, cluster.State)
			assert.NotEmpty(t, cluster.ClusterArn)
			assert.Contains(t, cluster.ClusterArn, "cluster/"+tt.clusterName+"/")
		})
	}
}

func TestBackend_DescribeCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*kafka.InMemoryBackend) string
		name    string
		wantErr bool
	}{
		{
			name: "existing_cluster",
			setup: func(b *kafka.InMemoryBackend) string {
				c, err := b.CreateCluster(
					context.Background(),
					"my-cluster",
					"2.8.0",
					3,
					kafka.BrokerNodeGroupInfo{},
					nil,
					nil,
				)
				if err != nil {
					return ""
				}

				return c.ClusterArn
			},
		},
		{
			name: "not_found",
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

			cluster, err := b.DescribeCluster(context.Background(), arn)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, arn, cluster.ClusterArn)
		})
	}
}

func TestBackend_ListClusters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(*kafka.InMemoryBackend)
		name      string
		wantCount int
	}{
		{
			name:      "empty",
			setup:     func(_ *kafka.InMemoryBackend) {},
			wantCount: 0,
		},
		{
			name: "multiple",
			setup: func(b *kafka.InMemoryBackend) {
				_, _ = b.CreateCluster(
					context.Background(),
					"cluster-a",
					"2.8.0",
					3,
					kafka.BrokerNodeGroupInfo{},
					nil,
					nil,
				)
				_, _ = b.CreateCluster(
					context.Background(),
					"cluster-b",
					"2.8.0",
					3,
					kafka.BrokerNodeGroupInfo{},
					nil,
					nil,
				)
			},
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			tt.setup(b)

			clusters := b.ListClusters(context.Background())
			assert.Len(t, clusters, tt.wantCount)
		})
	}
}

func TestBackend_DeleteCluster(t *testing.T) {
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
			name: "not_found",
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

			err := b.DeleteCluster(context.Background(), arn)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			_, err = b.DescribeCluster(context.Background(), arn)
			require.Error(t, err)
		})
	}
}

func TestBackend_CreateConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*kafka.InMemoryBackend)
		name     string
		confName string
		wantErr  bool
	}{
		{
			name:     "success",
			confName: "my-config",
			setup:    func(_ *kafka.InMemoryBackend) {},
		},
		{
			name:     "duplicate_name",
			confName: "my-config",
			setup: func(b *kafka.InMemoryBackend) {
				_, _ = b.CreateConfiguration(
					context.Background(),
					"my-config",
					"",
					[]string{"2.8.0"},
					"auto.create.topics.enable=false",
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

			config, err := b.CreateConfiguration(context.Background(),
				tt.confName,
				"test config",
				[]string{"2.8.0"},
				"auto.create.topics.enable=false",
			)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.confName, config.Name)
			assert.NotEmpty(t, config.Arn)
			assert.Contains(t, config.Arn, "configuration/"+tt.confName+"/")
		})
	}
}

func TestBackend_DescribeConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*kafka.InMemoryBackend) string
		name    string
		wantErr bool
	}{
		{
			name: "existing_config",
			setup: func(b *kafka.InMemoryBackend) string {
				c, _ := b.CreateConfiguration(context.Background(), "my-config", "", []string{"2.8.0"}, "")

				return c.Arn
			},
		},
		{
			name: "not_found",
			setup: func(_ *kafka.InMemoryBackend) string {
				return "arn:aws:kafka:us-east-1:000000000000:configuration/nonexistent/uuid"
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			arn := tt.setup(b)

			config, err := b.DescribeConfiguration(context.Background(), arn)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, arn, config.Arn)
		})
	}
}

func TestBackend_TagOperations(t *testing.T) {
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

func TestBackend_BatchAssociateScramSecret(t *testing.T) {
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

func TestBackend_BatchDisassociateScramSecret(t *testing.T) {
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

func TestBackend_CreateReplicator(t *testing.T) {
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
			)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.repName, replicator.ReplicatorName)
			assert.NotEmpty(t, replicator.ReplicatorArn)
			assert.Equal(t, kafka.ReplicatorStateRunning, replicator.ReplicatorState)
		})
	}
}

func TestBackend_DeleteReplicator(t *testing.T) {
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

func TestBackend_CreateTopic(t *testing.T) {
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

func TestBackend_DeleteTopic(t *testing.T) {
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

func TestBackend_CreateVpcConnection(t *testing.T) {
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

			conn, err := b.CreateVpcConnection(context.Background(), clusterArn, "vpc-12345", "SASL_IAM", nil)

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

func TestBackend_DeleteVpcConnection(t *testing.T) {
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
				conn, _ := b.CreateVpcConnection(context.Background(), c.ClusterArn, "vpc-12345", "SASL_IAM", nil)

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

func TestBackend_DeleteClusterPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*kafka.InMemoryBackend) string
		name    string
		wantErr bool
	}{
		{
			name: "success_no_policy",
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

			err := b.DeleteClusterPolicy(context.Background(), clusterArn)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestBackend_DescribeClusterOperation(t *testing.T) {
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
				op := b.AddClusterOperationInternal(c.ClusterArn, "UPDATE_BROKER_COUNT")

				return op.ClusterOperationArn
			},
		},
		{
			name: "not_found",
			setup: func(_ *kafka.InMemoryBackend) string {
				return "arn:aws:kafka:us-east-1:000000000000:cluster-operation/nonexistent/uuid"
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			clusterOperationArn := tt.setup(b)

			op, err := b.DescribeClusterOperation(context.Background(), clusterOperationArn)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, clusterOperationArn, op.ClusterOperationArn)
			assert.Equal(t, kafka.ClusterOperationStateUpdateComplete, op.OperationState)
		})
	}
}

func TestParseKafkaPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		method       string
		path         string
		wantOp       string
		wantResource string
	}{
		{
			name:   "list_clusters_v1",
			method: http.MethodGet,
			path:   "/v1/clusters",
			wantOp: "ListClusters",
		},
		{
			name:   "create_cluster_v1",
			method: http.MethodPost,
			path:   "/v1/clusters",
			wantOp: "CreateCluster",
		},
		{
			name:         "describe_cluster_v1",
			method:       http.MethodGet,
			path:         "/v1/clusters/arn:aws:kafka:us-east-1:000000000000:cluster/test/uuid-1",
			wantOp:       "DescribeCluster",
			wantResource: "arn:aws:kafka:us-east-1:000000000000:cluster/test/uuid-1",
		},
		{
			name:         "delete_cluster_v1",
			method:       http.MethodDelete,
			path:         "/v1/clusters/arn:aws:kafka:us-east-1:000000000000:cluster/test/uuid-1",
			wantOp:       "DeleteCluster",
			wantResource: "arn:aws:kafka:us-east-1:000000000000:cluster/test/uuid-1",
		},
		{
			name:         "bootstrap_brokers",
			method:       http.MethodGet,
			path:         "/v1/clusters/arn:aws:kafka:us-east-1:000000000000:cluster/test/uuid-1/bootstrap-brokers",
			wantOp:       "GetBootstrapBrokers",
			wantResource: "arn:aws:kafka:us-east-1:000000000000:cluster/test/uuid-1",
		},
		{
			name:   "list_clusters_v2",
			method: http.MethodGet,
			path:   "/api/v2/clusters",
			wantOp: "ListClustersV2",
		},
		{
			name:   "create_cluster_v2",
			method: http.MethodPost,
			path:   "/api/v2/clusters",
			wantOp: "CreateClusterV2",
		},
		{
			name:   "list_configurations",
			method: http.MethodGet,
			path:   "/v1/configurations",
			wantOp: "ListConfigurations",
		},
		{
			name:   "create_configuration",
			method: http.MethodPost,
			path:   "/v1/configurations",
			wantOp: "CreateConfiguration",
		},
		{
			name:         "describe_configuration",
			method:       http.MethodGet,
			path:         "/v1/configurations/arn:aws:kafka:us-east-1:000000000000:configuration/my-config/uuid-1",
			wantOp:       "DescribeConfiguration",
			wantResource: "arn:aws:kafka:us-east-1:000000000000:configuration/my-config/uuid-1",
		},
		{
			name:         "list_tags",
			method:       http.MethodGet,
			path:         "/v1/tags/arn:aws:kafka:us-east-1:000000000000:cluster/test/uuid-1",
			wantOp:       "ListTagsForResource",
			wantResource: "arn:aws:kafka:us-east-1:000000000000:cluster/test/uuid-1",
		},
		{
			name:         "tag_resource",
			method:       http.MethodPost,
			path:         "/v1/tags/arn:aws:kafka:us-east-1:000000000000:cluster/test/uuid-1",
			wantOp:       "TagResource",
			wantResource: "arn:aws:kafka:us-east-1:000000000000:cluster/test/uuid-1",
		},
		{
			name:         "untag_resource",
			method:       http.MethodDelete,
			path:         "/v1/tags/arn:aws:kafka:us-east-1:000000000000:cluster/test/uuid-1",
			wantOp:       "UntagResource",
			wantResource: "arn:aws:kafka:us-east-1:000000000000:cluster/test/uuid-1",
		},
		{
			name:   "unknown_path",
			method: http.MethodGet,
			path:   "/unknown/path",
			wantOp: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			op, resource := kafka.ParseKafkaPathForTest(tt.method, tt.path)
			assert.Equal(t, tt.wantOp, op)
			assert.Equal(t, tt.wantResource, resource)
		})
	}
}
