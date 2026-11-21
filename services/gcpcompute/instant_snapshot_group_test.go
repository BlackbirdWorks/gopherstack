package gcpcompute_test

import (
	"net/http"
	"testing"

	compute "cloud.google.com/go/compute/apiv1"
	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
)

func (e *env) isgInsert(t *testing.T, group string) error {
	t.Helper()

	op, err := client(t, e, compute.NewInstantSnapshotGroupsRESTClient).Insert(t.Context(),
		&computepb.InsertInstantSnapshotGroupRequest{
			Project: project, Zone: zone, SourceConsistencyGroup: s(group),
			InstantSnapshotGroupResource: &computepb.InstantSnapshotGroup{Name: s("g")},
		})

	return e.finish(t, op, err)
}

func (e *env) instantSnapshotNames(t *testing.T) []string {
	t.Helper()

	var out []string

	it := client(t, e, compute.NewInstantSnapshotsRESTClient).List(t.Context(),
		&computepb.ListInstantSnapshotsRequest{Project: project, Zone: zone})

	for {
		is, err := it.Next()
		if err != nil {
			require.ErrorIs(t, err, iterator.Done)

			return out
		}

		out = append(out, is.GetName())
	}
}

func TestInstantSnapshotGroupLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	cg := &computepb.ResourcePolicy{
		Name: s("cg"), DiskConsistencyGroupPolicy: &computepb.ResourcePolicyDiskConsistencyGroupPolicy{},
	}
	require.NoError(t, e.resPolicy(t, cg))
	require.NoError(t, e.resPolicy(t, named(daily("00:00"), "snap")))
	e.disk(t, "d1")
	e.disk(t, "d2")
	e.disk(t, "other")

	assertAPIError(t, e.isgInsert(t, rpRef("cg")), http.StatusBadRequest, "invalid")
	assertAPIError(t, e.isgInsert(t, ""), http.StatusBadRequest, "required")
	assertAPIError(t, e.isgInsert(t, rpRef("ghost")), http.StatusNotFound, "notFound")
	assertAPIError(t, e.isgInsert(t, rpRef("snap")), http.StatusBadRequest, "invalid")

	require.NoError(t, e.changeDisk(t, "d1", true, rpRef("cg")))
	require.NoError(t, e.changeDisk(t, "d2", true, rpRef("cg")))
	require.NoError(t, e.isgInsert(t, rpRef("cg")))
	assertAPIError(t, e.isgInsert(t, rpRef("cg")), http.StatusConflict, "alreadyExists")

	c := client(t, e, compute.NewInstantSnapshotGroupsRESTClient)

	g, err := c.Get(
		t.Context(),
		&computepb.GetInstantSnapshotGroupRequest{Project: project, Zone: zone, InstantSnapshotGroup: "g"},
	)
	require.NoError(t, err)
	assert.Equal(t, "READY", g.GetStatus())
	assert.Contains(t, g.GetSourceConsistencyGroup(), "/resourcePolicies/cg")
	assert.Contains(t, g.GetResourceStatus().GetSourceInfo().GetConsistencyGroup(), "/resourcePolicies/cg")
	assert.Contains(t, g.GetZone(), "/zones/"+zone)
	assert.ElementsMatch(t, []string{"g-d1", "g-d2"}, e.instantSnapshotNames(t))

	is, err := client(t, e, compute.NewInstantSnapshotsRESTClient).Get(t.Context(),
		&computepb.GetInstantSnapshotRequest{Project: project, Zone: zone, InstantSnapshot: "g-d1"})
	require.NoError(t, err)
	assert.Contains(t, is.GetSourceInstantSnapshotGroup(), "/instantSnapshotGroups/g")
	assert.Contains(t, is.GetSourceDisk(), "/disks/d1")

	_, err = e.resPolicies(t).Delete(t.Context(), &computepb.DeleteResourcePolicyRequest{
		Project: project, Region: region, ResourcePolicy: "cg",
	})
	assertAPIError(t, err, http.StatusBadRequest, "resourceInUseByAnotherResource")

	it := c.List(t.Context(), &computepb.ListInstantSnapshotGroupsRequest{Project: project, Zone: zone})
	l, err := it.Next()
	require.NoError(t, err)
	assert.Equal(t, "g", l.GetName())

	op, err := c.Delete(t.Context(), &computepb.DeleteInstantSnapshotGroupRequest{
		Project: project, Zone: zone, InstantSnapshotGroup: "g",
	})
	require.NoError(t, e.finish(t, op, err))
	assert.Empty(t, e.instantSnapshotNames(t))

	_, err = c.Get(
		t.Context(),
		&computepb.GetInstantSnapshotGroupRequest{Project: project, Zone: zone, InstantSnapshotGroup: "g"},
	)
	assertAPIError(t, err, http.StatusNotFound, "notFound")
}

func TestInstantSnapshotGroupRestore(t *testing.T) {
	t.Parallel()

	src := newEnv(t)
	require.NoError(t, src.resPolicy(t, &computepb.ResourcePolicy{
		Name: s("cg"), DiskConsistencyGroupPolicy: &computepb.ResourcePolicyDiskConsistencyGroupPolicy{},
	}))
	src.disk(t, "d1")
	require.NoError(t, src.changeDisk(t, "d1", true, rpRef("cg")))
	require.NoError(t, src.isgInsert(t, rpRef("cg")))

	dst := newEnv(t)
	require.NoError(t, dst.backend.Restore(t.Context(), src.backend.Snapshot(t.Context())))

	g, err := client(t, dst, compute.NewInstantSnapshotGroupsRESTClient).Get(t.Context(),
		&computepb.GetInstantSnapshotGroupRequest{Project: project, Zone: zone, InstantSnapshotGroup: "g"})
	require.NoError(t, err)
	assert.Equal(t, "READY", g.GetStatus())
	assert.Equal(t, []string{"g-d1"}, dst.instantSnapshotNames(t))
}
