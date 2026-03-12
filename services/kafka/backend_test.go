package kafka_test

import (
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
				_, err := b.CreateCluster("my-cluster", "2.8.0", 3, kafka.BrokerNodeGroupInfo{}, nil)
				require.NoError(t, err)
			}

			cluster, err := b.CreateCluster(
				tt.clusterName,
				"2.8.0",
				3,
				kafka.BrokerNodeGroupInfo{},
				map[string]string{"env": "test"},
			)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.clusterName, cluster.ClusterName)
			assert.Equal(t, kafka.ClusterStateActive, cluster.State)
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
				c, err := b.CreateCluster("my-cluster", "2.8.0", 3, kafka.BrokerNodeGroupInfo{}, nil)
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

			cluster, err := b.DescribeCluster(arn)

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
				_, _ = b.CreateCluster("cluster-a", "2.8.0", 3, kafka.BrokerNodeGroupInfo{}, nil)
				_, _ = b.CreateCluster("cluster-b", "2.8.0", 3, kafka.BrokerNodeGroupInfo{}, nil)
			},
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			tt.setup(b)

			clusters := b.ListClusters()
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
				c, _ := b.CreateCluster("my-cluster", "2.8.0", 3, kafka.BrokerNodeGroupInfo{}, nil)

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

			err := b.DeleteCluster(arn)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			_, err = b.DescribeCluster(arn)
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
				_, _ = b.CreateConfiguration("my-config", "", []string{"2.8.0"}, "auto.create.topics.enable=false")
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			tt.setup(b)

			config, err := b.CreateConfiguration(
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
				c, _ := b.CreateConfiguration("my-config", "", []string{"2.8.0"}, "")

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

			config, err := b.DescribeConfiguration(arn)

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
				c, _ := b.CreateCluster("tagged-cluster", "2.8.0", 3, kafka.BrokerNodeGroupInfo{}, nil)

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
				err := b.TagResource(arn, tt.tags)

				if tt.wantErr {
					require.Error(t, err)

					return
				}

				require.NoError(t, err)
			}

			if tt.removKeys != nil {
				err := b.UntagResource(arn, tt.removKeys)
				require.NoError(t, err)
			}

			if !tt.wantErr && tt.wantTags != nil {
				got, err := b.GetTags(arn)
				require.NoError(t, err)
				assert.Equal(t, tt.wantTags, got)
			}

			if tt.wantErr && tt.tags == nil {
				_, err := b.GetTags(arn)
				require.Error(t, err)
			}
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
