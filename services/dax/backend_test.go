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

// ---- CreateCluster ----

func TestCreateCluster_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	cluster, err := b.CreateCluster(validCreateInput("my-cluster"))
	require.NoError(t, err)
	assert.Equal(t, "my-cluster", cluster.ClusterName)
	assert.Equal(t, "dax.r5.large", cluster.NodeType)
	assert.Equal(t, dax.StatusAvailable, cluster.Status)
	assert.Equal(t, 1, cluster.TotalNodes)
	assert.Equal(t, 1, cluster.ActiveNodes)
	assert.Len(t, cluster.Nodes, 1)
	assert.NotEmpty(t, cluster.ClusterArn)
	assert.NotNil(t, cluster.Endpoint)
	assert.NotEmpty(t, cluster.Endpoint.Address)
	assert.Equal(t, 8111, cluster.Endpoint.Port)
}

func TestCreateCluster_MultipleNodes(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	input := validCreateInput("ha-cluster")
	input.ReplicationFactor = 3
	cluster, err := b.CreateCluster(input)
	require.NoError(t, err)
	assert.Equal(t, 3, cluster.TotalNodes)
	assert.Len(t, cluster.Nodes, 3)
}

func TestCreateCluster_DuplicateName(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateCluster(validCreateInput("dup-cluster"))
	require.NoError(t, err)
	_, err = b.CreateCluster(validCreateInput("dup-cluster"))
	require.Error(t, err)
}

func TestCreateCluster_MissingName(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	input := validCreateInput("")
	_, err := b.CreateCluster(input)
	require.Error(t, err)
}

func TestCreateCluster_MissingNodeType(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	input := validCreateInput("x")
	input.NodeType = ""
	_, err := b.CreateCluster(input)
	require.Error(t, err)
}

func TestCreateCluster_MissingIamRole(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	input := validCreateInput("x")
	input.IamRoleArn = ""
	_, err := b.CreateCluster(input)
	require.Error(t, err)
}

func TestCreateCluster_UnknownSubnetGroup(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	input := validCreateInput("x")
	input.SubnetGroupName = "no-such-group"
	_, err := b.CreateCluster(input)
	require.Error(t, err)
}

func TestCreateCluster_SSEEnabled(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	input := validCreateInput("sse-cluster")
	input.SSESpecificationEnabled = true
	cluster, err := b.CreateCluster(input)
	require.NoError(t, err)
	assert.Equal(t, "ENABLED", cluster.SSEDescription.Status)
}

func TestCreateCluster_WithTags(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	input := validCreateInput("tagged")
	input.Tags = map[string]string{"env": "test", "team": "platform"}
	cluster, err := b.CreateCluster(input)
	require.NoError(t, err)
	assert.Equal(t, "test", cluster.Tags["env"])
	assert.Equal(t, "platform", cluster.Tags["team"])
}

// ---- DescribeClusters ----

func TestDescribeClusters_All(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateCluster(validCreateInput("a"))
	require.NoError(t, err)
	_, err = b.CreateCluster(validCreateInput("b"))
	require.NoError(t, err)

	clusters, _, err := b.DescribeClusters(nil, 0, "")
	require.NoError(t, err)
	assert.Len(t, clusters, 2)
}

func TestDescribeClusters_Filtered(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateCluster(validCreateInput("alpha"))
	require.NoError(t, err)
	_, err = b.CreateCluster(validCreateInput("beta"))
	require.NoError(t, err)

	clusters, _, err := b.DescribeClusters([]string{"alpha"}, 0, "")
	require.NoError(t, err)
	assert.Len(t, clusters, 1)
	assert.Equal(t, "alpha", clusters[0].ClusterName)
}

func TestDescribeClusters_NotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, _, err := b.DescribeClusters([]string{"nonexistent"}, 0, "")
	require.Error(t, err)
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

func TestUpdateCluster_Description(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateCluster(validCreateInput("upd"))
	require.NoError(t, err)

	updated, err := b.UpdateCluster(dax.UpdateClusterInput{
		ClusterName: "upd",
		Description: "Updated description",
	})
	require.NoError(t, err)
	assert.Equal(t, "Updated description", updated.Description)
}

func TestUpdateCluster_NotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.UpdateCluster(dax.UpdateClusterInput{ClusterName: "no-such"})
	require.Error(t, err)
}

// ---- DeleteCluster ----

func TestDeleteCluster_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateCluster(validCreateInput("del"))
	require.NoError(t, err)

	deleted, err := b.DeleteCluster("del")
	require.NoError(t, err)
	assert.Equal(t, "del", deleted.ClusterName)
	assert.Equal(t, dax.StatusDeleting, deleted.Status)

	// Should be gone now.
	_, _, err = b.DescribeClusters([]string{"del"}, 0, "")
	require.Error(t, err)
}

func TestDeleteCluster_NotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.DeleteCluster("no-such")
	require.Error(t, err)
}

// ---- Tags ----

func TestTagResource_ListTags(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	cluster, err := b.CreateCluster(validCreateInput("tagged2"))
	require.NoError(t, err)

	err = b.TagResource(cluster.ClusterArn, map[string]string{"k1": "v1", "k2": "v2"})
	require.NoError(t, err)

	tags, _, err := b.ListTags(cluster.ClusterArn, "")
	require.NoError(t, err)
	assert.Equal(t, "v1", tags["k1"])
	assert.Equal(t, "v2", tags["k2"])
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

func TestTagResource_NotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	err := b.TagResource(
		"arn:aws:dax:us-east-1:123456789012:cache/no-such",
		map[string]string{"k": "v"},
	)
	require.Error(t, err)
}

// ---- Parameter Groups ----

func TestCreateParameterGroup(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	pg, err := b.CreateParameterGroup("my-pg", "test group")
	require.NoError(t, err)
	assert.Equal(t, "my-pg", pg.ParameterGroupName)
}

func TestCreateParameterGroup_Duplicate(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateParameterGroup("pg", "")
	require.NoError(t, err)
	_, err = b.CreateParameterGroup("pg", "")
	require.Error(t, err)
}

func TestDescribeParameterGroups_Default(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	groups, _, err := b.DescribeParameterGroups(nil, 0, "")
	require.NoError(t, err)
	// Should include the default group.
	assert.NotEmpty(t, groups)
}

func TestDeleteParameterGroup(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateParameterGroup("pg-del", "")
	require.NoError(t, err)

	err = b.DeleteParameterGroup("pg-del")
	require.NoError(t, err)
}

func TestDeleteParameterGroup_InUse(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	input := validCreateInput("cluster-with-pg")
	input.ParameterGroupName = dax.DefaultParameterGroupName
	_, err := b.CreateCluster(input)
	require.NoError(t, err)

	// Default parameter group is in use.
	err = b.DeleteParameterGroup(dax.DefaultParameterGroupName)
	require.Error(t, err)
}

// ---- Subnet Groups ----

func TestCreateSubnetGroup(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	sg, err := b.CreateSubnetGroup("my-sg", "test subnet group", []string{"subnet-1", "subnet-2"})
	require.NoError(t, err)
	assert.Equal(t, "my-sg", sg.SubnetGroupName)
	assert.Len(t, sg.SubnetIDs, 2)
}

func TestCreateSubnetGroup_Duplicate(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateSubnetGroup("sg", "", nil)
	require.NoError(t, err)
	_, err = b.CreateSubnetGroup("sg", "", nil)
	require.Error(t, err)
}

func TestDescribeSubnetGroups_Default(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	groups, _, err := b.DescribeSubnetGroups(nil, 0, "")
	require.NoError(t, err)
	// Should include the default group.
	assert.NotEmpty(t, groups)
}

func TestDeleteSubnetGroup_InUse(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	input := validCreateInput("cluster-with-sg")
	// Uses default subnet group.
	_, err := b.CreateCluster(input)
	require.NoError(t, err)

	err = b.DeleteSubnetGroup(dax.DefaultSubnetGroupName)
	require.Error(t, err)
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
}

// ---- Snapshot/Restore ----

func TestSnapshotRestore(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateCluster(validCreateInput("snap-cluster"))
	require.NoError(t, err)

	data := b.Snapshot()
	require.NotEmpty(t, data)

	b2 := newTestBackend()
	err = b2.Restore(data)
	require.NoError(t, err)

	clusters, _, err := b2.DescribeClusters(nil, 0, "")
	require.NoError(t, err)
	assert.Len(t, clusters, 1)
	assert.Equal(t, "snap-cluster", clusters[0].ClusterName)
}
