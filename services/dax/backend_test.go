package dax_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dax"
)

func newTestBackend() *dax.InMemoryBackend {
	return dax.NewInMemoryBackend("123456789012", "us-east-1")
}

func validCreateInput(name string) dax.CreateClusterInput {
	return dax.CreateClusterInput{
		ClusterName:       name,
		NodeType:          "dax.r5.large",
		IamRoleArn:        "arn:aws:iam::123456789012:role/DAXRole",
		ReplicationFactor: 1,
	}
}

func strPtr(s string) *string { return &s }

// ---- CreateCluster ----

func TestCreateCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   dax.CreateClusterInput
		wantErr bool
		check   func(t *testing.T, c *dax.Cluster)
	}{
		{
			name:  "valid minimal input",
			input: validCreateInput("my-cluster"),
			check: func(t *testing.T, c *dax.Cluster) {
				t.Helper()
				assert.Equal(t, "my-cluster", c.ClusterName)
				assert.Equal(t, "dax.r5.large", c.NodeType)
				assert.Equal(t, dax.StatusAvailable, c.Status)
				assert.Equal(t, 1, c.TotalNodes)
				assert.Equal(t, 1, c.ActiveNodes)
				assert.Len(t, c.Nodes, 1)
				assert.NotEmpty(t, c.ClusterArn)
				assert.NotNil(t, c.Endpoint)
				assert.NotEmpty(t, c.Endpoint.Address)
				assert.Equal(t, 8111, c.Endpoint.Port)
				assert.Equal(t, dax.EncryptionTypeNone, c.ClusterEndpointEncryptionType)
			},
		},
		{
			name: "multiple nodes",
			input: func() dax.CreateClusterInput {
				in := validCreateInput("ha-cluster")
				in.ReplicationFactor = 3
				return in
			}(),
			check: func(t *testing.T, c *dax.Cluster) {
				t.Helper()
				assert.Equal(t, 3, c.TotalNodes)
				assert.Len(t, c.Nodes, 3)
			},
		},
		{
			name: "SSE enabled",
			input: func() dax.CreateClusterInput {
				in := validCreateInput("sse-cluster")
				in.SSESpecificationEnabled = true
				return in
			}(),
			check: func(t *testing.T, c *dax.Cluster) {
				t.Helper()
				assert.Equal(t, "ENABLED", c.SSEDescription.Status)
			},
		},
		{
			name: "with tags",
			input: func() dax.CreateClusterInput {
				in := validCreateInput("tagged")
				in.Tags = map[string]string{"env": "test", "team": "platform"}
				return in
			}(),
			check: func(t *testing.T, c *dax.Cluster) {
				t.Helper()
				assert.Equal(t, "test", c.Tags["env"])
				assert.Equal(t, "platform", c.Tags["team"])
			},
		},
		{
			name: "TLS encryption type",
			input: func() dax.CreateClusterInput {
				in := validCreateInput("tls-cluster")
				in.ClusterEndpointEncryptionType = dax.EncryptionTypeTLS
				return in
			}(),
			check: func(t *testing.T, c *dax.Cluster) {
				t.Helper()
				assert.Equal(t, dax.EncryptionTypeTLS, c.ClusterEndpointEncryptionType)
			},
		},
		{
			name: "with notification topic",
			input: func() dax.CreateClusterInput {
				in := validCreateInput("notif-cluster")
				in.NotificationTopicArn = "arn:aws:sns:us-east-1:123456789012:my-topic"
				return in
			}(),
			check: func(t *testing.T, c *dax.Cluster) {
				t.Helper()
				require.NotNil(t, c.NotificationConfiguration)
				assert.Equal(t, "arn:aws:sns:us-east-1:123456789012:my-topic", c.NotificationConfiguration.TopicArn)
				assert.Equal(t, "active", c.NotificationConfiguration.TopicStatus)
			},
		},
		{
			name:    "missing cluster name",
			input:   dax.CreateClusterInput{NodeType: "dax.r5.large", IamRoleArn: "arn:aws:iam::123456789012:role/r"},
			wantErr: true,
		},
		{
			name:    "missing node type",
			input:   dax.CreateClusterInput{ClusterName: "x", IamRoleArn: "arn:aws:iam::123456789012:role/r"},
			wantErr: true,
		},
		{
			name:    "invalid node type",
			input:   dax.CreateClusterInput{ClusterName: "x", NodeType: "invalid.type", IamRoleArn: "arn:aws:iam::123456789012:role/r"},
			wantErr: true,
		},
		{
			name:    "missing IAM role",
			input:   dax.CreateClusterInput{ClusterName: "x", NodeType: "dax.r5.large"},
			wantErr: true,
		},
		{
			name: "replication factor exceeds max",
			input: func() dax.CreateClusterInput {
				in := validCreateInput("big-cluster")
				in.ReplicationFactor = 11
				return in
			}(),
			wantErr: true,
		},
		{
			name: "unknown subnet group",
			input: func() dax.CreateClusterInput {
				in := validCreateInput("x")
				in.SubnetGroupName = "no-such-group"
				return in
			}(),
			wantErr: true,
		},
		{
			name: "unknown parameter group",
			input: func() dax.CreateClusterInput {
				in := validCreateInput("x")
				in.ParameterGroupName = "no-such-pg"
				return in
			}(),
			wantErr: true,
		},
		{
			name: "invalid encryption type",
			input: func() dax.CreateClusterInput {
				in := validCreateInput("x")
				in.ClusterEndpointEncryptionType = "INVALID"
				return in
			}(),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend()
			c, err := b.CreateCluster(tt.input)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)

			if tt.check != nil {
				tt.check(t, c)
			}
		})
	}
}

func TestCreateCluster_DuplicateName(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateCluster(validCreateInput("dup-cluster"))
	require.NoError(t, err)
	_, err = b.CreateCluster(validCreateInput("dup-cluster"))
	require.Error(t, err)
}

// ---- DescribeClusters ----

func TestDescribeClusters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setup     func(b *dax.InMemoryBackend)
		names     []string
		wantCount int
		wantErr   bool
	}{
		{
			name: "all clusters",
			setup: func(b *dax.InMemoryBackend) {
				_, _ = b.CreateCluster(validCreateInput("a"))
				_, _ = b.CreateCluster(validCreateInput("b"))
			},
			wantCount: 2,
		},
		{
			name: "filtered by name",
			setup: func(b *dax.InMemoryBackend) {
				_, _ = b.CreateCluster(validCreateInput("alpha"))
				_, _ = b.CreateCluster(validCreateInput("beta"))
			},
			names:     []string{"alpha"},
			wantCount: 1,
		},
		{
			name:    "not found",
			setup:   func(_ *dax.InMemoryBackend) {},
			names:   []string{"nonexistent"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend()
			tt.setup(b)

			clusters, _, err := b.DescribeClusters(tt.names, 0, "")

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Len(t, clusters, tt.wantCount)
		})
	}
}

func TestDescribeClusters_Pagination(t *testing.T) {
	t.Parallel()
	b := newTestBackend()

	for _, name := range []string{"c1", "c2", "c3"} {
		_, err := b.CreateCluster(validCreateInput(name))
		require.NoError(t, err)
	}

	page1, nextToken, err := b.DescribeClusters(nil, 2, "")
	require.NoError(t, err)
	assert.Len(t, page1, 2)
	assert.NotEmpty(t, nextToken)

	page2, nextToken2, err := b.DescribeClusters(nil, 2, nextToken)
	require.NoError(t, err)
	assert.Len(t, page2, 1)
	assert.Empty(t, nextToken2)
}

// ---- UpdateCluster ----

func TestUpdateCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		setup   func(b *dax.InMemoryBackend) string
		input   func(clusterName string) dax.UpdateClusterInput
		wantErr bool
		check   func(t *testing.T, c *dax.Cluster)
	}{
		{
			name: "update description",
			setup: func(b *dax.InMemoryBackend) string {
				_, _ = b.CreateCluster(validCreateInput("upd"))
				return "upd"
			},
			input: func(name string) dax.UpdateClusterInput {
				return dax.UpdateClusterInput{ClusterName: name, Description: strPtr("new description")}
			},
			check: func(t *testing.T, c *dax.Cluster) {
				t.Helper()
				assert.Equal(t, "new description", c.Description)
			},
		},
		{
			name: "clear description with empty string",
			setup: func(b *dax.InMemoryBackend) string {
				in := validCreateInput("desc-cluster")
				in.Description = "initial"
				_, _ = b.CreateCluster(in)
				return "desc-cluster"
			},
			input: func(name string) dax.UpdateClusterInput {
				return dax.UpdateClusterInput{ClusterName: name, Description: strPtr("")}
			},
			check: func(t *testing.T, c *dax.Cluster) {
				t.Helper()
				assert.Equal(t, "", c.Description)
			},
		},
		{
			name: "nil description does not clear",
			setup: func(b *dax.InMemoryBackend) string {
				in := validCreateInput("keep-desc")
				in.Description = "kept"
				_, _ = b.CreateCluster(in)
				return "keep-desc"
			},
			input: func(name string) dax.UpdateClusterInput {
				return dax.UpdateClusterInput{ClusterName: name}
			},
			check: func(t *testing.T, c *dax.Cluster) {
				t.Helper()
				assert.Equal(t, "kept", c.Description)
			},
		},
		{
			name: "update notification topic",
			setup: func(b *dax.InMemoryBackend) string {
				_, _ = b.CreateCluster(validCreateInput("notif"))
				return "notif"
			},
			input: func(name string) dax.UpdateClusterInput {
				return dax.UpdateClusterInput{
					ClusterName:          name,
					NotificationTopicArn: "arn:aws:sns:us-east-1:123456789012:topic",
				}
			},
			check: func(t *testing.T, c *dax.Cluster) {
				t.Helper()
				require.NotNil(t, c.NotificationConfiguration)
				assert.Equal(t, "arn:aws:sns:us-east-1:123456789012:topic", c.NotificationConfiguration.TopicArn)
				assert.Equal(t, "active", c.NotificationConfiguration.TopicStatus)
			},
		},
		{
			name: "update notification topic status",
			setup: func(b *dax.InMemoryBackend) string {
				in := validCreateInput("topic-status")
				in.NotificationTopicArn = "arn:aws:sns:us-east-1:123456789012:topic"
				_, _ = b.CreateCluster(in)
				return "topic-status"
			},
			input: func(name string) dax.UpdateClusterInput {
				return dax.UpdateClusterInput{
					ClusterName:             name,
					NotificationTopicStatus: "inactive",
				}
			},
			check: func(t *testing.T, c *dax.Cluster) {
				t.Helper()
				require.NotNil(t, c.NotificationConfiguration)
				assert.Equal(t, "inactive", c.NotificationConfiguration.TopicStatus)
			},
		},
		{
			name:  "not found",
			setup: func(_ *dax.InMemoryBackend) string { return "no-such" },
			input: func(name string) dax.UpdateClusterInput {
				return dax.UpdateClusterInput{ClusterName: name}
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend()
			clusterName := tt.setup(b)

			c, err := b.UpdateCluster(tt.input(clusterName))

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)

			if tt.check != nil {
				tt.check(t, c)
			}
		})
	}
}

// ---- DeleteCluster ----

func TestDeleteCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		setup       func(b *dax.InMemoryBackend)
		clusterName string
		wantErr     bool
	}{
		{
			name: "success",
			setup: func(b *dax.InMemoryBackend) {
				_, _ = b.CreateCluster(validCreateInput("del"))
			},
			clusterName: "del",
		},
		{
			name:        "not found",
			setup:       func(_ *dax.InMemoryBackend) {},
			clusterName: "no-such",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend()
			tt.setup(b)

			deleted, err := b.DeleteCluster(tt.clusterName)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.clusterName, deleted.ClusterName)
			assert.Equal(t, dax.StatusDeleting, deleted.Status)

			_, _, err = b.DescribeClusters([]string{tt.clusterName}, 0, "")
			require.Error(t, err)
		})
	}
}

// ---- IncreaseReplicationFactor ----

func TestIncreaseReplicationFactor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		setup   func(b *dax.InMemoryBackend)
		input   dax.IncreaseReplicationFactorInput
		wantErr bool
		check   func(t *testing.T, c *dax.Cluster)
	}{
		{
			name: "increase from 1 to 3",
			setup: func(b *dax.InMemoryBackend) {
				_, _ = b.CreateCluster(validCreateInput("grow"))
			},
			input: dax.IncreaseReplicationFactorInput{
				ClusterName:          "grow",
				NewReplicationFactor: 3,
			},
			check: func(t *testing.T, c *dax.Cluster) {
				t.Helper()
				assert.Equal(t, 3, c.TotalNodes)
				assert.Len(t, c.Nodes, 3)
			},
		},
		{
			name:    "cluster not found",
			setup:   func(_ *dax.InMemoryBackend) {},
			input:   dax.IncreaseReplicationFactorInput{ClusterName: "no-such", NewReplicationFactor: 2},
			wantErr: true,
		},
		{
			name: "factor exceeds max",
			setup: func(b *dax.InMemoryBackend) {
				_, _ = b.CreateCluster(validCreateInput("x"))
			},
			input:   dax.IncreaseReplicationFactorInput{ClusterName: "x", NewReplicationFactor: 11},
			wantErr: true,
		},
		{
			name: "factor not greater than current",
			setup: func(b *dax.InMemoryBackend) {
				in := validCreateInput("x")
				in.ReplicationFactor = 3
				_, _ = b.CreateCluster(in)
			},
			input:   dax.IncreaseReplicationFactorInput{ClusterName: "x", NewReplicationFactor: 3},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend()
			tt.setup(b)

			c, err := b.IncreaseReplicationFactor(tt.input)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)

			if tt.check != nil {
				tt.check(t, c)
			}
		})
	}
}

// ---- DecreaseReplicationFactor ----

func TestDecreaseReplicationFactor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		setup   func(b *dax.InMemoryBackend)
		input   dax.DecreaseReplicationFactorInput
		wantErr bool
		check   func(t *testing.T, c *dax.Cluster)
	}{
		{
			name: "decrease from 3 to 1",
			setup: func(b *dax.InMemoryBackend) {
				in := validCreateInput("shrink")
				in.ReplicationFactor = 3
				_, _ = b.CreateCluster(in)
			},
			input: dax.DecreaseReplicationFactorInput{
				ClusterName:          "shrink",
				NewReplicationFactor: 1,
			},
			check: func(t *testing.T, c *dax.Cluster) {
				t.Helper()
				assert.Equal(t, 1, c.TotalNodes)
				assert.Len(t, c.Nodes, 1)
			},
		},
		{
			name: "decrease with specific node IDs",
			setup: func(b *dax.InMemoryBackend) {
				in := validCreateInput("specific")
				in.ReplicationFactor = 3
				_, _ = b.CreateCluster(in)
			},
			input: dax.DecreaseReplicationFactorInput{
				ClusterName:          "specific",
				NewReplicationFactor: 1,
				NodeIDsToRemove:      []string{"specific-0001", "specific-0002"},
			},
			check: func(t *testing.T, c *dax.Cluster) {
				t.Helper()
				assert.Len(t, c.Nodes, 1)
				assert.Equal(t, "specific-0000", c.Nodes[0].NodeID)
			},
		},
		{
			name:    "cluster not found",
			setup:   func(_ *dax.InMemoryBackend) {},
			input:   dax.DecreaseReplicationFactorInput{ClusterName: "no-such", NewReplicationFactor: 1},
			wantErr: true,
		},
		{
			name: "factor not less than current",
			setup: func(b *dax.InMemoryBackend) {
				_, _ = b.CreateCluster(validCreateInput("x"))
			},
			input:   dax.DecreaseReplicationFactorInput{ClusterName: "x", NewReplicationFactor: 1},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend()
			tt.setup(b)

			c, err := b.DecreaseReplicationFactor(tt.input)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)

			if tt.check != nil {
				tt.check(t, c)
			}
		})
	}
}

// ---- RebootNode ----

func TestRebootNode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		setup       func(b *dax.InMemoryBackend)
		clusterName string
		nodeID      string
		wantErr     bool
		check       func(t *testing.T, c *dax.Cluster)
	}{
		{
			name: "success",
			setup: func(b *dax.InMemoryBackend) {
				_, _ = b.CreateCluster(validCreateInput("reboot-me"))
			},
			clusterName: "reboot-me",
			nodeID:      "reboot-me-0000",
			check: func(t *testing.T, c *dax.Cluster) {
				t.Helper()
				require.Len(t, c.Nodes, 1)
				assert.Equal(t, dax.StatusRebooting, c.Nodes[0].NodeStatus)
			},
		},
		{
			name:        "cluster not found",
			setup:       func(_ *dax.InMemoryBackend) {},
			clusterName: "no-such",
			nodeID:      "node-0000",
			wantErr:     true,
		},
		{
			name: "node not found",
			setup: func(b *dax.InMemoryBackend) {
				_, _ = b.CreateCluster(validCreateInput("exist"))
			},
			clusterName: "exist",
			nodeID:      "no-such-node",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend()
			tt.setup(b)

			c, err := b.RebootNode(tt.clusterName, tt.nodeID)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)

			if tt.check != nil {
				tt.check(t, c)
			}
		})
	}
}

// ---- Tags ----

func TestTagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		setup   func(b *dax.InMemoryBackend) string
		tags    map[string]string
		wantErr bool
		check   func(t *testing.T, tags map[string]string)
	}{
		{
			name: "tag cluster ARN",
			setup: func(b *dax.InMemoryBackend) string {
				c, _ := b.CreateCluster(validCreateInput("tagged2"))
				return c.ClusterArn
			},
			tags: map[string]string{"k1": "v1", "k2": "v2"},
			check: func(t *testing.T, tags map[string]string) {
				t.Helper()
				assert.Equal(t, "v1", tags["k1"])
				assert.Equal(t, "v2", tags["k2"])
			},
		},
		{
			name: "tag parameter group ARN",
			setup: func(b *dax.InMemoryBackend) string {
				_, _ = b.CreateParameterGroup("my-pg", "")
				return "arn:aws:dax:us-east-1:123456789012:parametergroup/my-pg"
			},
			tags: map[string]string{"k": "v"},
			check: func(t *testing.T, tags map[string]string) {
				t.Helper()
				assert.Equal(t, "v", tags["k"])
			},
		},
		{
			name: "tag subnet group ARN",
			setup: func(b *dax.InMemoryBackend) string {
				_, _ = b.CreateSubnetGroup("my-sg", "", []string{"subnet-1"})
				return "arn:aws:dax:us-east-1:123456789012:subnetgroup/my-sg"
			},
			tags: map[string]string{"k": "v"},
			check: func(t *testing.T, tags map[string]string) {
				t.Helper()
				assert.Equal(t, "v", tags["k"])
			},
		},
		{
			name: "not found",
			setup: func(_ *dax.InMemoryBackend) string {
				return "arn:aws:dax:us-east-1:123456789012:cache/no-such"
			},
			tags:    map[string]string{"k": "v"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend()
			arn := tt.setup(b)

			err := b.TagResource(arn, tt.tags)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			tags, _, err := b.ListTags(arn, "")
			require.NoError(t, err)
			tt.check(t, tags)
		})
	}
}

func TestUntagResource(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	cluster, err := b.CreateCluster(validCreateInput("untag-test"))
	require.NoError(t, err)

	err = b.TagResource(cluster.ClusterArn, map[string]string{"k1": "v1", "k2": "v2"})
	require.NoError(t, err)

	err = b.UntagResource(cluster.ClusterArn, []string{"k1"})
	require.NoError(t, err)

	tags, _, err := b.ListTags(cluster.ClusterArn, "")
	require.NoError(t, err)
	_, hasK1 := tags["k1"]
	assert.False(t, hasK1)
	assert.Equal(t, "v2", tags["k2"])
}

// ---- Parameter Groups ----

func TestCreateParameterGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		pgName      string
		description string
		wantErr     bool
		check       func(t *testing.T, pg *dax.ParameterGroup)
	}{
		{
			name:        "success",
			pgName:      "my-pg",
			description: "test group",
			check: func(t *testing.T, pg *dax.ParameterGroup) {
				t.Helper()
				assert.Equal(t, "my-pg", pg.ParameterGroupName)
				assert.Equal(t, "test group", pg.Description)
				assert.Equal(t, "300000", pg.Parameters["query-ttl-millis"])
				assert.Equal(t, "300000", pg.Parameters["record-ttl-millis"])
			},
		},
		{
			name:    "empty name",
			pgName:  "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend()

			pg, err := b.CreateParameterGroup(tt.pgName, tt.description)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)

			if tt.check != nil {
				tt.check(t, pg)
			}
		})
	}
}

func TestCreateParameterGroup_Duplicate(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateParameterGroup("pg", "")
	require.NoError(t, err)
	_, err = b.CreateParameterGroup("pg", "")
	require.Error(t, err)
}

func TestDescribeParameterGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setup     func(b *dax.InMemoryBackend)
		names     []string
		wantCount int
		wantErr   bool
	}{
		{
			name:      "default group exists",
			setup:     func(_ *dax.InMemoryBackend) {},
			wantCount: 1,
		},
		{
			name: "with custom group",
			setup: func(b *dax.InMemoryBackend) {
				_, _ = b.CreateParameterGroup("custom", "")
			},
			wantCount: 2,
		},
		{
			name: "filter by name found",
			setup: func(b *dax.InMemoryBackend) {
				_, _ = b.CreateParameterGroup("target", "")
			},
			names:     []string{"target"},
			wantCount: 1,
		},
		{
			name:    "filter by name not found",
			setup:   func(_ *dax.InMemoryBackend) {},
			names:   []string{"nonexistent"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend()
			tt.setup(b)

			groups, _, err := b.DescribeParameterGroups(tt.names, 0, "")

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Len(t, groups, tt.wantCount)
		})
	}
}

func TestUpdateParameterGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		setup   func(b *dax.InMemoryBackend)
		input   dax.UpdateParameterGroupInput
		wantErr bool
		check   func(t *testing.T, pg *dax.ParameterGroup)
	}{
		{
			name: "update query-ttl",
			setup: func(b *dax.InMemoryBackend) {
				_, _ = b.CreateParameterGroup("my-pg", "")
			},
			input: dax.UpdateParameterGroupInput{
				ParameterGroupName:  "my-pg",
				ParameterNameValues: []dax.ParameterNameValue{{ParameterName: "query-ttl-millis", ParameterValue: "60000"}},
			},
			check: func(t *testing.T, pg *dax.ParameterGroup) {
				t.Helper()
				assert.Equal(t, "60000", pg.Parameters["query-ttl-millis"])
			},
		},
		{
			name:    "unknown parameter name",
			setup:   func(b *dax.InMemoryBackend) { _, _ = b.CreateParameterGroup("pg", "") },
			input:   dax.UpdateParameterGroupInput{ParameterGroupName: "pg", ParameterNameValues: []dax.ParameterNameValue{{ParameterName: "unknown-param", ParameterValue: "1"}}},
			wantErr: true,
		},
		{
			name:    "group not found",
			setup:   func(_ *dax.InMemoryBackend) {},
			input:   dax.UpdateParameterGroupInput{ParameterGroupName: "no-such"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend()
			tt.setup(b)

			pg, err := b.UpdateParameterGroup(tt.input)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)

			if tt.check != nil {
				tt.check(t, pg)
			}
		})
	}
}

func TestDeleteParameterGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		setup   func(b *dax.InMemoryBackend)
		pgName  string
		wantErr bool
	}{
		{
			name: "success",
			setup: func(b *dax.InMemoryBackend) {
				_, _ = b.CreateParameterGroup("pg-del", "")
			},
			pgName: "pg-del",
		},
		{
			name:    "not found",
			setup:   func(_ *dax.InMemoryBackend) {},
			pgName:  "no-such",
			wantErr: true,
		},
		{
			name: "in use",
			setup: func(b *dax.InMemoryBackend) {
				input := validCreateInput("cluster-with-pg")
				input.ParameterGroupName = dax.DefaultParameterGroupName
				_, _ = b.CreateCluster(input)
			},
			pgName:  dax.DefaultParameterGroupName,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend()
			tt.setup(b)

			err := b.DeleteParameterGroup(tt.pgName)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestDescribeParameters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		pgName     string
		wantErr    bool
		wantParams []string
	}{
		{
			name:       "default group has params",
			pgName:     dax.DefaultParameterGroupName,
			wantParams: []string{"query-ttl-millis", "record-ttl-millis"},
		},
		{
			name:    "group not found",
			pgName:  "no-such",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend()

			params, _, err := b.DescribeParameters(tt.pgName, 0, "")

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)

			names := make([]string, 0, len(params))
			for _, p := range params {
				names = append(names, p.ParameterName)
			}

			for _, expected := range tt.wantParams {
				assert.Contains(t, names, expected)
			}
		})
	}
}

func TestDescribeDefaultParameters(t *testing.T) {
	t.Parallel()
	b := newTestBackend()

	params, _, err := b.DescribeDefaultParameters(0, "")
	require.NoError(t, err)
	assert.Len(t, params, 2)

	names := make([]string, 0, len(params))
	for _, p := range params {
		names = append(names, p.ParameterName)
	}

	assert.Contains(t, names, "query-ttl-millis")
	assert.Contains(t, names, "record-ttl-millis")
}

func TestResetParameterGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		setup          func(b *dax.InMemoryBackend)
		pgName         string
		parameterNames []string
		wantErr        bool
		check          func(t *testing.T, pg *dax.ParameterGroup)
	}{
		{
			name: "reset all to defaults",
			setup: func(b *dax.InMemoryBackend) {
				_, _ = b.CreateParameterGroup("pg", "")
				_, _ = b.UpdateParameterGroup(dax.UpdateParameterGroupInput{
					ParameterGroupName:  "pg",
					ParameterNameValues: []dax.ParameterNameValue{{ParameterName: "query-ttl-millis", ParameterValue: "99999"}},
				})
			},
			pgName: "pg",
			check: func(t *testing.T, pg *dax.ParameterGroup) {
				t.Helper()
				assert.Equal(t, "300000", pg.Parameters["query-ttl-millis"])
			},
		},
		{
			name: "reset specific parameter",
			setup: func(b *dax.InMemoryBackend) {
				_, _ = b.CreateParameterGroup("pg2", "")
				_, _ = b.UpdateParameterGroup(dax.UpdateParameterGroupInput{
					ParameterGroupName:  "pg2",
					ParameterNameValues: []dax.ParameterNameValue{{ParameterName: "query-ttl-millis", ParameterValue: "99999"}},
				})
			},
			pgName:         "pg2",
			parameterNames: []string{"query-ttl-millis"},
			check: func(t *testing.T, pg *dax.ParameterGroup) {
				t.Helper()
				assert.Equal(t, "300000", pg.Parameters["query-ttl-millis"])
			},
		},
		{
			name:    "not found",
			setup:   func(_ *dax.InMemoryBackend) {},
			pgName:  "no-such",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend()
			tt.setup(b)

			pg, err := b.ResetParameterGroup(tt.pgName, tt.parameterNames)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)

			if tt.check != nil {
				tt.check(t, pg)
			}
		})
	}
}

// ---- Subnet Groups ----

func TestCreateSubnetGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		sgName    string
		desc      string
		subnetIDs []string
		wantErr   bool
		check     func(t *testing.T, sg *dax.SubnetGroup)
	}{
		{
			name:      "success",
			sgName:    "my-sg",
			desc:      "test subnet group",
			subnetIDs: []string{"subnet-1", "subnet-2"},
			check: func(t *testing.T, sg *dax.SubnetGroup) {
				t.Helper()
				assert.Equal(t, "my-sg", sg.SubnetGroupName)
				assert.Len(t, sg.Subnets, 2)
				assert.Equal(t, "subnet-1", sg.Subnets[0].SubnetID)
				assert.Equal(t, "us-east-1a", sg.Subnets[0].AvailabilityZone)
			},
		},
		{
			name:    "empty name",
			sgName:  "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend()

			sg, err := b.CreateSubnetGroup(tt.sgName, tt.desc, tt.subnetIDs)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)

			if tt.check != nil {
				tt.check(t, sg)
			}
		})
	}
}

func TestCreateSubnetGroup_Duplicate(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateSubnetGroup("sg", "", nil)
	require.NoError(t, err)
	_, err = b.CreateSubnetGroup("sg", "", nil)
	require.Error(t, err)
}

func TestUpdateSubnetGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		setup   func(b *dax.InMemoryBackend)
		input   dax.UpdateSubnetGroupInput
		wantErr bool
		check   func(t *testing.T, sg *dax.SubnetGroup)
	}{
		{
			name: "update description",
			setup: func(b *dax.InMemoryBackend) {
				_, _ = b.CreateSubnetGroup("upd-sg", "old desc", []string{"subnet-1"})
			},
			input: dax.UpdateSubnetGroupInput{SubnetGroupName: "upd-sg", Description: "new desc"},
			check: func(t *testing.T, sg *dax.SubnetGroup) {
				t.Helper()
				assert.Equal(t, "new desc", sg.Description)
			},
		},
		{
			name: "update subnets",
			setup: func(b *dax.InMemoryBackend) {
				_, _ = b.CreateSubnetGroup("sub-sg", "", []string{"subnet-1"})
			},
			input: dax.UpdateSubnetGroupInput{SubnetGroupName: "sub-sg", SubnetIDs: []string{"subnet-2", "subnet-3"}},
			check: func(t *testing.T, sg *dax.SubnetGroup) {
				t.Helper()
				assert.Len(t, sg.Subnets, 2)
				assert.Equal(t, "subnet-2", sg.Subnets[0].SubnetID)
			},
		},
		{
			name:    "not found",
			setup:   func(_ *dax.InMemoryBackend) {},
			input:   dax.UpdateSubnetGroupInput{SubnetGroupName: "no-such"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend()
			tt.setup(b)

			sg, err := b.UpdateSubnetGroup(tt.input)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)

			if tt.check != nil {
				tt.check(t, sg)
			}
		})
	}
}

func TestDeleteSubnetGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		setup   func(b *dax.InMemoryBackend)
		sgName  string
		wantErr bool
	}{
		{
			name: "success",
			setup: func(b *dax.InMemoryBackend) {
				_, _ = b.CreateSubnetGroup("sg-del", "", nil)
			},
			sgName: "sg-del",
		},
		{
			name:    "not found",
			setup:   func(_ *dax.InMemoryBackend) {},
			sgName:  "no-such",
			wantErr: true,
		},
		{
			name: "in use",
			setup: func(b *dax.InMemoryBackend) {
				_, _ = b.CreateCluster(validCreateInput("cluster-with-sg"))
			},
			sgName:  dax.DefaultSubnetGroupName,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend()
			tt.setup(b)

			err := b.DeleteSubnetGroup(tt.sgName)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestDescribeSubnetGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setup     func(b *dax.InMemoryBackend)
		names     []string
		wantCount int
		wantErr   bool
	}{
		{
			name:      "default group exists",
			setup:     func(_ *dax.InMemoryBackend) {},
			wantCount: 1,
		},
		{
			name: "with custom group",
			setup: func(b *dax.InMemoryBackend) {
				_, _ = b.CreateSubnetGroup("custom", "", nil)
			},
			wantCount: 2,
		},
		{
			name:    "not found",
			setup:   func(_ *dax.InMemoryBackend) {},
			names:   []string{"nonexistent"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend()
			tt.setup(b)

			groups, _, err := b.DescribeSubnetGroups(tt.names, 0, "")

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Len(t, groups, tt.wantCount)
		})
	}
}

// ---- DescribeEvents ----

func TestDescribeEvents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(b *dax.InMemoryBackend)
		sourceName string
		sourceType string
		wantMin    int
	}{
		{
			name: "events emitted on create",
			setup: func(b *dax.InMemoryBackend) {
				_, _ = b.CreateCluster(validCreateInput("evt-cluster"))
			},
			sourceType: dax.EventSourceTypeCluster,
			wantMin:    1,
		},
		{
			name: "filter by source name",
			setup: func(b *dax.InMemoryBackend) {
				_, _ = b.CreateCluster(validCreateInput("target-cluster"))
				_, _ = b.CreateCluster(validCreateInput("other-cluster"))
			},
			sourceName: "target-cluster",
			wantMin:    1,
		},
		{
			name:    "no events on fresh backend",
			setup:   func(_ *dax.InMemoryBackend) {},
			wantMin: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend()
			tt.setup(b)

			events, _, err := b.DescribeEvents(tt.sourceName, tt.sourceType, nil, nil, 0, "")
			require.NoError(t, err)
			assert.GreaterOrEqual(t, len(events), tt.wantMin)

			if tt.sourceName != "" {
				for _, ev := range events {
					assert.Equal(t, tt.sourceName, ev.SourceName)
				}
			}

			if tt.sourceType != "" {
				for _, ev := range events {
					assert.Equal(t, tt.sourceType, ev.SourceType)
				}
			}
		})
	}
}

// ---- Reset ----

func TestReset(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateCluster(validCreateInput("temp"))
	require.NoError(t, err)

	b.Reset()

	clusters, _, err := b.DescribeClusters(nil, 0, "")
	require.NoError(t, err)
	assert.Empty(t, clusters)

	groups, _, err := b.DescribeParameterGroups(nil, 0, "")
	require.NoError(t, err)
	assert.NotEmpty(t, groups)
}

// ---- Snapshot/Restore ----

func TestSnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		setup func(b *dax.InMemoryBackend)
		check func(t *testing.T, b2 *dax.InMemoryBackend)
	}{
		{
			name: "round-trip cluster",
			setup: func(b *dax.InMemoryBackend) {
				_, _ = b.CreateCluster(validCreateInput("snap-cluster"))
			},
			check: func(t *testing.T, b2 *dax.InMemoryBackend) {
				t.Helper()
				clusters, _, err := b2.DescribeClusters(nil, 0, "")
				require.NoError(t, err)
				assert.Len(t, clusters, 1)
				assert.Equal(t, "snap-cluster", clusters[0].ClusterName)
			},
		},
		{
			name: "tag index rebuilt on restore",
			setup: func(b *dax.InMemoryBackend) {
				in := validCreateInput("tagged")
				in.Tags = map[string]string{"k": "v"}
				_, _ = b.CreateCluster(in)
			},
			check: func(t *testing.T, b2 *dax.InMemoryBackend) {
				t.Helper()
				clusters, _, err := b2.DescribeClusters([]string{"tagged"}, 0, "")
				require.NoError(t, err)
				require.Len(t, clusters, 1)
				tags, _, err := b2.ListTags(clusters[0].ClusterArn, "")
				require.NoError(t, err)
				assert.Equal(t, "v", tags["k"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend()
			tt.setup(b)

			data := b.Snapshot()
			require.NotEmpty(t, data)

			b2 := newTestBackend()
			err := b2.Restore(data)
			require.NoError(t, err)

			tt.check(t, b2)
		})
	}
}

func TestRestore_InvalidJSON(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	err := b.Restore([]byte("not-json"))
	require.Error(t, err)
}

func TestRestore_ReferentialIntegrityFailure(t *testing.T) {
	t.Parallel()

	// Craft a snapshot where a cluster references a missing parameter group.
	badSnap := `{"clusters":{"bad":{"clusterName":"bad","parameterGroup":{"parameterGroupName":"missing-pg"},"subnetGroupName":"default","tags":{},"nodes":[],"securityGroupIds":[]}},"paramGroups":{},"subnetGroups":{"default":{"subnetGroupName":"default","subnets":[]}},"tags":{}}`

	b := newTestBackend()
	err := b.Restore([]byte(badSnap))
	require.Error(t, err)
}
