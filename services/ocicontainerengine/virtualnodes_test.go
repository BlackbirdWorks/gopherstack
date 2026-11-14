package ocicontainerengine_test

import (
	"net/http"
	"testing"

	ke "github.com/oracle/oci-go-sdk/v65/containerengine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func vnpDetails(clusterID *string, name string, size int) ke.CreateVirtualNodePoolDetails {
	return ke.CreateVirtualNodePoolDetails{
		CompartmentId: new(testCompartment), ClusterId: clusterID, DisplayName: new(name), Size: new(size),
		PlacementConfigurations: []ke.PlacementConfiguration{
			{AvailabilityDomain: new(ad1), SubnetId: new(subAD1)},
			{AvailabilityDomain: new(ad2), SubnetId: new(subAD2)},
		},
		PodConfiguration: &ke.PodConfiguration{SubnetId: new(subRegional), Shape: new("Pod.Standard.E4.Flex")},
	}
}

func (e *env) mkVNP(t *testing.T, clusterID *string, name string, size int) ke.VirtualNodePool {
	t.Helper()

	resp, err := e.ce.CreateVirtualNodePool(t.Context(), ke.CreateVirtualNodePoolRequest{
		CreateVirtualNodePoolDetails: vnpDetails(clusterID, name, size),
	})
	require.NoError(t, err)
	require.NotNil(t, resp.OpcWorkRequestId)
	e.settle()

	list, err := e.ce.ListVirtualNodePools(t.Context(), ke.ListVirtualNodePoolsRequest{
		CompartmentId: new(testCompartment), Name: new(name),
	})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)

	got, err := e.ce.GetVirtualNodePool(t.Context(), ke.GetVirtualNodePoolRequest{VirtualNodePoolId: list.Items[0].Id})
	require.NoError(t, err)

	return got.VirtualNodePool
}

func TestVirtualNodePoolLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	c := e.mkCluster(t, "vnp", nil)

	created, err := e.ce.CreateVirtualNodePool(ctx, ke.CreateVirtualNodePoolRequest{
		CreateVirtualNodePoolDetails: vnpDetails(c.Id, "serverless", 3),
	})
	require.NoError(t, err)

	wr, err := e.ce.GetWorkRequest(ctx, ke.GetWorkRequestRequest{WorkRequestId: created.OpcWorkRequestId})
	require.NoError(t, err)
	assert.Equal(t, ke.WorkRequestOperationTypeVirtualnodepoolCreate, wr.OperationType)
	assert.Equal(t, ke.WorkRequestStatusInProgress, wr.Status)

	list, err := e.ce.ListVirtualNodePools(ctx, ke.ListVirtualNodePoolsRequest{
		CompartmentId: new(testCompartment), ClusterId: c.Id,
	})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)
	assert.Equal(t, ke.VirtualNodePoolLifecycleStateCreating, list.Items[0].LifecycleState)

	id := list.Items[0].Id
	e.settle()

	got, err := e.ce.GetVirtualNodePool(ctx, ke.GetVirtualNodePoolRequest{VirtualNodePoolId: id})
	require.NoError(t, err)
	assert.NotEmpty(t, *got.Etag)
	assert.Equal(t, ke.VirtualNodePoolLifecycleStateActive, got.LifecycleState)
	assert.Equal(t, testVersion, *got.KubernetesVersion)
	assert.Equal(t, 3, *got.Size)
	assert.Equal(t, "Pod.Standard.E4.Flex", *got.PodConfiguration.Shape)

	nodes, err := e.ce.ListVirtualNodes(ctx, ke.ListVirtualNodesRequest{VirtualNodePoolId: id})
	require.NoError(t, err)
	require.Len(t, nodes.Items, 3)

	perAD := map[string]int{}
	ips := map[string]bool{}

	for _, n := range nodes.Items {
		assert.Equal(t, ke.VirtualNodeLifecycleStateActive, n.LifecycleState)
		assert.Equal(t, *id, *n.VirtualNodePoolId)
		perAD[*n.AvailabilityDomain]++
		ips[*n.PrivateIp] = true
	}

	assert.Equal(t, map[string]int{ad1: 2, ad2: 1}, perAD)
	assert.Len(t, ips, 3, "private IPs are unique")

	node, err := e.ce.GetVirtualNode(
		ctx,
		ke.GetVirtualNodeRequest{VirtualNodePoolId: id, VirtualNodeId: nodes.Items[0].Id},
	)
	require.NoError(t, err)
	assert.Equal(t, *nodes.Items[0].Id, *node.Id)

	upd, err := e.ce.UpdateVirtualNodePool(ctx, ke.UpdateVirtualNodePoolRequest{
		VirtualNodePoolId: id, IfMatch: got.Etag,
		DisplayName: new("renamed"), Size: new(1), FreeformTags: map[string]string{"k": "v"},
		Taints: []ke.Taint{{Key: new("dedicated"), Value: new("x"), Effect: new("NoSchedule")}},
	})
	require.NoError(t, err)

	wr, err = e.ce.GetWorkRequest(ctx, ke.GetWorkRequestRequest{WorkRequestId: upd.OpcWorkRequestId})
	require.NoError(t, err)
	assert.Equal(t, ke.WorkRequestOperationTypeVirtualnodepoolUpdate, wr.OperationType)

	mid, err := e.ce.GetVirtualNodePool(ctx, ke.GetVirtualNodePoolRequest{VirtualNodePoolId: id})
	require.NoError(t, err)
	assert.Equal(t, ke.VirtualNodePoolLifecycleStateUpdating, mid.LifecycleState)

	e.settle()

	after, err := e.ce.GetVirtualNodePool(ctx, ke.GetVirtualNodePoolRequest{VirtualNodePoolId: id})
	require.NoError(t, err)
	assert.Equal(t, "renamed", *after.DisplayName)
	assert.Equal(t, 1, *after.Size)
	assert.Equal(t, "v", after.FreeformTags["k"])
	require.Len(t, after.Taints, 1)

	nodes, err = e.ce.ListVirtualNodes(ctx, ke.ListVirtualNodesRequest{VirtualNodePoolId: id})
	require.NoError(t, err)
	assert.Len(t, nodes.Items, 1)

	snap := e.backend.Snapshot(ctx)
	fresh := newEnv(t, envOpts{})
	require.NoError(t, fresh.backend.Restore(ctx, snap))

	restored, err := fresh.ce.GetVirtualNodePool(ctx, ke.GetVirtualNodePoolRequest{VirtualNodePoolId: id})
	require.NoError(t, err)
	assert.Equal(t, "renamed", *restored.DisplayName)

	del, err := e.ce.DeleteVirtualNodePool(ctx, ke.DeleteVirtualNodePoolRequest{VirtualNodePoolId: id})
	require.NoError(t, err)

	wr, err = e.ce.GetWorkRequest(ctx, ke.GetWorkRequestRequest{WorkRequestId: del.OpcWorkRequestId})
	require.NoError(t, err)
	assert.Equal(t, ke.WorkRequestOperationTypeVirtualnodepoolDelete, wr.OperationType)

	e.settle()

	gone, err := e.ce.GetVirtualNodePool(ctx, ke.GetVirtualNodePoolRequest{VirtualNodePoolId: id})
	require.NoError(t, err)
	assert.Equal(t, ke.VirtualNodePoolLifecycleStateDeleted, gone.LifecycleState)

	_, err = e.ce.UpdateVirtualNodePool(ctx, ke.UpdateVirtualNodePoolRequest{
		VirtualNodePoolId: id, Size: new(2),
	})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())
}

func TestVirtualNodePoolErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mod    func(d *ke.CreateVirtualNodePoolDetails)
		name   string
		code   string
		status int
	}{
		{name: "no placement", status: 400, code: "MissingParameter", mod: func(d *ke.CreateVirtualNodePoolDetails) {
			d.PlacementConfigurations = []ke.PlacementConfiguration{}
		}},
		{name: "no pod config", status: 400, code: "MissingParameter", mod: func(d *ke.CreateVirtualNodePoolDetails) {
			d.PodConfiguration = &ke.PodConfiguration{SubnetId: new(subRegional), Shape: new("")}
		}},
		{name: "negative size", status: 400, code: "InvalidParameter", mod: func(d *ke.CreateVirtualNodePoolDetails) {
			d.Size = new(-1)
		}},
		{name: "huge size", status: 400, code: "InvalidParameter", mod: func(d *ke.CreateVirtualNodePoolDetails) {
			d.Size = new(1001)
		}},
		{name: "bad subnet", status: 400, code: "InvalidParameter", mod: func(d *ke.CreateVirtualNodePoolDetails) {
			d.PlacementConfigurations[0].SubnetId = new("ocid1.subnet.oc1.iad.nope")
		}},
		{name: "ad mismatch", status: 400, code: "InvalidParameter", mod: func(d *ke.CreateVirtualNodePoolDetails) {
			d.PlacementConfigurations[0].AvailabilityDomain = new(ad2)
		}},
		{name: "bad taint", status: 400, code: "InvalidParameter", mod: func(d *ke.CreateVirtualNodePoolDetails) {
			d.Taints = []ke.Taint{{Key: new("k"), Effect: new("Bogus")}}
		}},
		{
			name:   "unknown cluster",
			status: 404,
			code:   "NotAuthorizedOrNotFound",
			mod: func(d *ke.CreateVirtualNodePoolDetails) {
				d.ClusterId = new("ocid1.cluster.oc1.iad.nope")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			c := e.mkCluster(t, "vnperr", nil)
			d := vnpDetails(c.Id, "p", 1)
			tt.mod(&d)

			_, err := e.ce.CreateVirtualNodePool(
				t.Context(),
				ke.CreateVirtualNodePoolRequest{CreateVirtualNodePoolDetails: d},
			)
			se := svcErr(t, err)
			assert.Equal(t, tt.status, se.GetHTTPStatusCode())
			assert.Equal(t, tt.code, se.GetCode())
		})
	}
}

func TestVirtualNodePoolConflicts(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	c := e.mkCluster(t, "vnpdup", nil)
	p := e.mkVNP(t, c.Id, "dup", 1)

	_, err := e.ce.CreateVirtualNodePool(ctx, ke.CreateVirtualNodePoolRequest{
		CreateVirtualNodePoolDetails: vnpDetails(c.Id, "dup", 1),
	})
	se := svcErr(t, err)
	assert.Equal(t, http.StatusConflict, se.GetHTTPStatusCode())
	assert.Equal(t, "NotAuthorizedOrResourceAlreadyExists", se.GetCode())

	_, err = e.ce.UpdateVirtualNodePool(ctx, ke.UpdateVirtualNodePoolRequest{
		VirtualNodePoolId: p.Id, IfMatch: new("stale"),
		Size: new(2),
	})
	assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.ce.DeleteVirtualNodePool(ctx, ke.DeleteVirtualNodePoolRequest{
		VirtualNodePoolId: p.Id, OverrideEvictionGraceDurationVnp: new("PT90M"),
	})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.ce.GetVirtualNode(ctx, ke.GetVirtualNodeRequest{VirtualNodePoolId: p.Id, VirtualNodeId: new("nope")})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.ce.GetVirtualNodePool(ctx, ke.GetVirtualNodePoolRequest{VirtualNodePoolId: new("nope")})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	retry := func() string {
		r, rerr := e.ce.CreateVirtualNodePool(ctx, ke.CreateVirtualNodePoolRequest{
			OpcRetryToken: new("tok"), CreateVirtualNodePoolDetails: vnpDetails(c.Id, "tokened", 1),
		})
		require.NoError(t, rerr)

		return *r.OpcWorkRequestId
	}

	first, second := retry(), retry()
	assert.Equal(t, first, second)

	del, err := e.ce.DeleteCluster(ctx, ke.DeleteClusterRequest{ClusterId: c.Id})
	require.NoError(t, err)
	assert.NotEmpty(t, *del.OpcWorkRequestId)
	e.settle()

	cascaded, err := e.ce.GetVirtualNodePool(ctx, ke.GetVirtualNodePoolRequest{VirtualNodePoolId: p.Id})
	require.NoError(t, err)
	assert.Equal(t, ke.VirtualNodePoolLifecycleStateDeleted, cascaded.LifecycleState)
}
