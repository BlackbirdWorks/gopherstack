//nolint:staticcheck // the API still serves fields the proto marks deprecated
package gcpcontainer_test

import (
	"testing"

	"cloud.google.com/go/container/apiv1/containerpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	"github.com/blackbirdworks/gopherstack/services/gcpcontainer"
)

func TestSnapshotRestore(t *testing.T) {
	t.Parallel()

	src := newEnv(t)
	src.ready(t, zone, &containerpb.Cluster{
		Name: "keep", InitialNodeCount: 2, InitialClusterVersion: "1.34.2-gke.1008000",
		ResourceLabels: map[string]string{"k": "v"},
	})
	src.ready(t, region, &containerpb.Cluster{Name: "ap", Autopilot: &containerpb.Autopilot{Enabled: true}})

	_, err := src.gc.UpdateCluster(t.Context(), &containerpb.UpdateClusterRequest{
		Name: clusterPath(zone, "keep"), Update: &containerpb.ClusterUpdate{DesiredMasterVersion: "1.35.1-gke.1000000"},
	})
	require.NoError(t, err)
	src.settle()

	_, err = src.gc.UpdateNodePool(t.Context(), &containerpb.UpdateNodePoolRequest{
		Name: poolPath(zone, "keep", "default-pool"), NodeVersion: "latest",
	})
	require.NoError(t, err)
	src.settle()

	_, err = src.gc.StartIPRotation(
		t.Context(),
		&containerpb.StartIPRotationRequest{Name: clusterPath(zone, "keep"), RotateCredentials: true},
	)
	require.NoError(t, err)

	inflight, err := src.gc.DeleteCluster(
		t.Context(),
		&containerpb.DeleteClusterRequest{Name: clusterPath(region, "ap")},
	)
	require.NoError(t, err)

	data := src.h.Snapshot(t.Context())
	require.NotEmpty(t, data)

	dst := newEnv(t)
	dst.clock.Advance(src.clock.Now().Sub(dst.clock.Now()))
	require.NoError(t, dst.h.Restore(t.Context(), data))

	want := src.getCluster(t, zone, "keep")
	got := dst.getCluster(t, zone, "keep")
	assert.True(t, proto.Equal(want, got), "cluster survives a restore")
	assert.Equal(t, containerpb.Cluster_RECONCILING, got.GetStatus(), "in-flight operations stay in flight")

	ops, err := dst.gc.GetOperation(
		t.Context(),
		&containerpb.GetOperationRequest{Name: regionLc + "/operations/" + inflight.GetName()},
	)
	require.NoError(t, err)
	assert.Equal(t, containerpb.Operation_DELETE_CLUSTER, ops.GetOperationType())

	assert.Equal(t, containerpb.Cluster_STOPPING, dst.getCluster(t, region, "ap").GetStatus())

	dst.settle()
	assert.Equal(t, containerpb.Cluster_RUNNING, dst.getCluster(t, zone, "keep").GetStatus())

	_, err = dst.gc.CompleteIPRotation(
		t.Context(),
		&containerpb.CompleteIPRotationRequest{Name: clusterPath(zone, "keep")},
	)
	require.NoError(t, err, "rotation state is restored")

	_, err = dst.gc.RollbackNodePoolUpgrade(t.Context(), &containerpb.RollbackNodePoolUpgradeRequest{
		Name: poolPath(zone, "keep", "default-pool"),
	})
	assertCode(t, codes.FailedPrecondition, err, "")

	dst.settle()

	_, err = dst.gc.RollbackNodePoolUpgrade(t.Context(), &containerpb.RollbackNodePoolUpgradeRequest{
		Name: poolPath(zone, "keep", "default-pool"),
	})
	require.NoError(t, err, "previous node version is restored")

	dst.settle()

	_, err = dst.gc.GetCluster(t.Context(), &containerpb.GetClusterRequest{Name: clusterPath(region, "ap")})
	assertCode(t, codes.NotFound, err, "")
}

func TestRestoreRejectsGarbage(t *testing.T) {
	t.Parallel()

	b := gcpcontainer.NewInMemoryBackend()
	require.Error(t, b.Restore(t.Context(), []byte("not json")))
}

func TestReset(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.ready(t, zone, stdCluster("gone"))
	e.h.Reset()

	list, err := e.gc.ListClusters(t.Context(), &containerpb.ListClustersRequest{Parent: zoneLoc})
	require.NoError(t, err)
	assert.Empty(t, list.GetClusters())

	ops, err := e.gc.ListOperations(t.Context(), &containerpb.ListOperationsRequest{Parent: zoneLoc})
	require.NoError(t, err)
	assert.Empty(t, ops.GetOperations())
}
