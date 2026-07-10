package dax_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dax"
)

// newFullPersistenceTestBackend creates a backend with one populated entry in
// every store.Table (clusters, paramGroups, subnetGroups -- see
// services/dax/store_setup.go) plus every raw field left un-converted (tags,
// events), so a Snapshot from it exercises the entire persisted surface of
// the backend.
func newFullPersistenceTestBackend(t *testing.T) (*dax.InMemoryBackend, *dax.Cluster) {
	t.Helper()

	b := newTestBackend()

	_, err := b.CreateSubnetGroup("full-subnets", "custom subnet group", []string{"subnet-0123abcd"})
	require.NoError(t, err)

	_, err = b.CreateParameterGroup("full-params", "custom parameter group")
	require.NoError(t, err)

	_, err = b.UpdateParameterGroup(dax.UpdateParameterGroupInput{
		ParameterGroupName: "full-params",
		ParameterNameValues: []dax.ParameterNameValue{
			{ParameterName: "record-ttl-millis", ParameterValue: "60000"},
		},
	})
	require.NoError(t, err)

	in := validCreateInput("full-cluster")
	in.SubnetGroupName = "full-subnets"
	in.ParameterGroupName = "full-params"
	in.Tags = map[string]string{"env": "test"}

	cluster, err := b.CreateCluster(in)
	require.NoError(t, err)

	// DescribeEvents (raw events ring buffer) has at least the "created" event
	// emitted by CreateCluster above.
	events, _, err := b.DescribeEvents("", "", nil, nil, 0, "")
	require.NoError(t, err)
	require.NotEmpty(t, events)

	return b, cluster
}

// TestInMemoryBackend_SnapshotRestore_FullState round-trips every
// store.Table (clusters, paramGroups, subnetGroups) and every raw field left
// un-converted (tags, events) through Snapshot -> Restore into a fresh
// backend.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	original, cluster := newFullPersistenceTestBackend(t)

	snap := original.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	fresh := newTestBackend()
	require.NoError(t, fresh.Restore(t.Context(), snap))

	// clusters table.
	clusters, _, err := fresh.DescribeClusters([]string{"full-cluster"}, 0, "")
	require.NoError(t, err)
	require.Len(t, clusters, 1)
	assert.Equal(t, "full-subnets", clusters[0].SubnetGroupName)
	assert.Equal(t, "full-params", clusters[0].ParameterGroup.ParameterGroupName)

	// paramGroups table.
	paramGroups, _, err := fresh.DescribeParameterGroups([]string{"full-params"}, 0, "")
	require.NoError(t, err)
	require.Len(t, paramGroups, 1)

	params, _, err := fresh.DescribeParameters("full-params", 0, "")
	require.NoError(t, err)

	var found bool

	for _, p := range params {
		if p.ParameterName == "record-ttl-millis" {
			assert.Equal(t, "60000", p.ParameterValue)

			found = true
		}
	}

	assert.True(t, found, "expected record-ttl-millis parameter to survive restore")

	// subnetGroups table.
	subnetGroups, _, err := fresh.DescribeSubnetGroups([]string{"full-subnets"}, 0, "")
	require.NoError(t, err)
	require.Len(t, subnetGroups, 1)

	// tags (raw field), including the tag index rebuild from cluster.Tags.
	tags, _, err := fresh.ListTags(cluster.ClusterArn, "")
	require.NoError(t, err)
	assert.Equal(t, "test", tags["env"])

	// events (raw field).
	events, _, err := fresh.DescribeEvents("", "", nil, nil, 0, "")
	require.NoError(t, err)
	assert.NotEmpty(t, events)
}

// TestInMemoryBackend_RestoreVersionMismatch verifies that a snapshot whose
// version doesn't match the current backend (including the pre-Phase-3.3
// format, which decodes with Version == 0) is discarded cleanly rather than
// partially decoded: the backend resets to empty state and Restore returns
// no error.
func TestInMemoryBackend_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	b, cluster := newFullPersistenceTestBackend(t)

	// A syntactically valid but version-mismatched snapshot.
	err := b.Restore(t.Context(), []byte(`{"version":999,"tables":{}}`))
	require.NoError(t, err)

	clusters, _, err := b.DescribeClusters(nil, 0, "")
	require.NoError(t, err)
	assert.Empty(t, clusters)

	groups, _, err := b.DescribeParameterGroups(nil, 0, "")
	require.NoError(t, err)
	assert.Empty(t, groups)

	subnetGroups, _, err := b.DescribeSubnetGroups(nil, 0, "")
	require.NoError(t, err)
	assert.Empty(t, subnetGroups)

	_, _, err = b.ListTags(cluster.ClusterArn, "")
	require.Error(t, err)

	events, _, err := b.DescribeEvents("", "", nil, nil, 0, "")
	require.NoError(t, err)
	assert.Empty(t, events)
}

// TestInMemoryBackend_RestoreOldFormatDecodesAsVersionZero verifies that a
// pre-Phase-3.3 snapshot (no "version"/"tables" fields at all) decodes with
// Version == 0, which mismatches daxSnapshotVersion and is discarded the same
// way any other incompatible version is -- never partially decoded under the
// old flat-map shape.
func TestInMemoryBackend_RestoreOldFormatDecodesAsVersionZero(t *testing.T) {
	t.Parallel()

	oldFormatSnap := `{` +
		`"clusters":{"legacy":{"clusterName":"legacy",` +
		`"parameterGroup":{"parameterGroupName":"default.dax1.0"},` +
		`"subnetGroupName":"default","tags":{},"nodes":[],"securityGroupIds":[]}},` +
		`"paramGroups":{},` +
		`"subnetGroups":{"default":{"subnetGroupName":"default","subnets":[]}},` +
		`"tags":{}}`

	b := newTestBackend()
	err := b.Restore(t.Context(), []byte(oldFormatSnap))
	require.NoError(t, err)

	clusters, _, err := b.DescribeClusters(nil, 0, "")
	require.NoError(t, err)
	assert.Empty(t, clusters, "old-format snapshot must not be partially decoded")
}

// TestHandler_SnapshotRestoreDelegate verifies Handler.Snapshot/Restore
// delegate to the backend (the wiring cli.go's generic setupPersistence
// relies on).
func TestHandler_SnapshotRestoreDelegate(t *testing.T) {
	t.Parallel()

	backend, cluster := newFullPersistenceTestBackend(t)
	h := dax.NewHandler(backend)

	snap := h.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	h2 := dax.NewHandler(newTestBackend())
	require.NoError(t, h2.Restore(t.Context(), snap))

	clusters, _, err := h2.Backend.DescribeClusters([]string{"full-cluster"}, 0, "")
	require.NoError(t, err)
	require.Len(t, clusters, 1)
	assert.Equal(t, cluster.ClusterArn, clusters[0].ClusterArn)
}
