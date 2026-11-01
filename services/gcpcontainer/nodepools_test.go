//nolint:staticcheck // the API still serves fields the proto marks deprecated
package gcpcontainer_test

import (
	"testing"

	"cloud.google.com/go/container/apiv1/containerpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

func TestNodePoolLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.ready(t, zone, stdCluster("np"))
	parentName := clusterPath(zone, "np")

	op, err := e.gc.CreateNodePool(t.Context(), &containerpb.CreateNodePoolRequest{
		Parent: parentName,
		NodePool: &containerpb.NodePool{
			Name: "workers", InitialNodeCount: 3,
			Config: &containerpb.NodeConfig{
				MachineType: "e2-standard-4",
				Labels:      map[string]string{"role": "worker"},
				Taints: []*containerpb.NodeTaint{
					{Key: "dedicated", Value: "gpu", Effect: containerpb.NodeTaint_NO_SCHEDULE},
				},
				Tags: []string{"web"},
				Spot: true,
			},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, containerpb.Operation_CREATE_NODE_POOL, op.GetOperationType())
	assert.Equal(t, "https://container.googleapis.com/v1/"+poolPath(zone, "np", "workers"), op.GetTargetLink())

	assert.Equal(t, containerpb.NodePool_PROVISIONING, e.getPool(t, zone, "np", "workers").GetStatus())
	assert.Equal(t, containerpb.NodePool_RUNNING, e.getPool(t, zone, "np", "default-pool").GetStatus())
	assert.Equal(t, containerpb.Cluster_RECONCILING, e.getCluster(t, zone, "np").GetStatus())

	_, err = e.gc.CreateNodePool(t.Context(), &containerpb.CreateNodePoolRequest{
		Parent: parentName, NodePool: &containerpb.NodePool{Name: "other", InitialNodeCount: 1},
	})
	require.NoError(t, err, "pool operations on different pools run side by side")

	e.settle()

	p := e.getPool(t, zone, "np", "workers")
	assert.Equal(t, containerpb.NodePool_RUNNING, p.GetStatus())
	assert.Equal(t, "e2-standard-4", p.GetConfig().GetMachineType())
	assert.Equal(t, map[string]string{"role": "worker"}, p.GetConfig().GetLabels())
	assert.True(t, p.GetConfig().GetSpot())

	list, err := e.gc.ListNodePools(t.Context(), &containerpb.ListNodePoolsRequest{Parent: parentName})
	require.NoError(t, err)
	assert.Len(t, list.GetNodePools(), 3)

	c := e.getCluster(t, zone, "np")
	assert.Equal(t, int32(2+3+1), c.GetCurrentNodeCount())

	_, err = e.gc.CreateNodePool(t.Context(), &containerpb.CreateNodePoolRequest{
		Parent: parentName, NodePool: &containerpb.NodePool{Name: "workers", InitialNodeCount: 1},
	})
	assertCode(t, codes.AlreadyExists, err, poolPath(zone, "np", "workers"))

	del, err := e.gc.DeleteNodePool(
		t.Context(),
		&containerpb.DeleteNodePoolRequest{Name: poolPath(zone, "np", "workers")},
	)
	require.NoError(t, err)
	assert.Equal(t, containerpb.Operation_DELETE_NODE_POOL, del.GetOperationType())
	assert.Equal(t, containerpb.NodePool_STOPPING, e.getPool(t, zone, "np", "workers").GetStatus())

	e.settle()

	_, err = e.gc.GetNodePool(t.Context(), &containerpb.GetNodePoolRequest{Name: poolPath(zone, "np", "workers")})
	assertCode(t, codes.NotFound, err, "")

	list, err = e.gc.ListNodePools(
		t.Context(),
		&containerpb.ListNodePoolsRequest{ProjectId: project, Zone: zone, ClusterId: "np"},
	)
	require.NoError(t, err)
	assert.Len(t, list.GetNodePools(), 2)

	_, err = e.gc.GetNodePool(t.Context(), &containerpb.GetNodePoolRequest{
		ProjectId: project, Zone: zone, ClusterId: "np", NodePoolId: "default-pool",
	})
	require.NoError(t, err)
}

func TestNodePoolValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		pool *containerpb.NodePool
		name string
		want string
	}{
		{&containerpb.NodePool{Name: "Bad", InitialNodeCount: 1}, "bad name", "Bad"},
		{&containerpb.NodePool{Name: "p"}, "zero nodes", "greater than 0"},
		{&containerpb.NodePool{Name: "p", InitialNodeCount: 5000}, "too many nodes", "between 0 and"},
		{
			&containerpb.NodePool{Name: "p", InitialNodeCount: 1, Locations: []string{"us-central1-b"}},
			"foreign location", "not one of the cluster locations",
		},
		{
			&containerpb.NodePool{Name: "p", InitialNodeCount: 1, Version: "1.10.0"},
			"unknown version", "unsupported",
		},
		{
			&containerpb.NodePool{Name: "p", InitialNodeCount: 1, Version: "1.29.15-gke.1039000"},
			"version skew", "at most 2 minor",
		},
		{
			&containerpb.NodePool{
				Name:             "p",
				InitialNodeCount: 1,
				Autoscaling:      &containerpb.NodePoolAutoscaling{Enabled: true, MinNodeCount: 3, MaxNodeCount: 1},
			},
			"min above max",
			"Maximum node count must not be less",
		},
		{
			&containerpb.NodePool{
				Name:              "p",
				InitialNodeCount:  1,
				MaxPodsConstraint: &containerpb.MaxPodsConstraint{MaxPodsPerNode: 4},
			},
			"too few pods",
			"Max pods per node",
		},
		{
			&containerpb.NodePool{Name: "p", InitialNodeCount: 1, Config: &containerpb.NodeConfig{
				Taints: []*containerpb.NodeTaint{{Key: "k", Value: "v"}},
			}}, "taint without effect", "must have an effect",
		},
		{
			&containerpb.NodePool{
				Name:             "p",
				InitialNodeCount: 1,
				Config:           &containerpb.NodeConfig{ImageType: "WINDOZE"},
			},
			"image type",
			"not supported",
		},
		{
			&containerpb.NodePool{Name: "p", InitialNodeCount: 1, Config: &containerpb.NodeConfig{DiskType: "tape"}},
			"disk type", "Invalid disk type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.ready(
				t,
				zone,
				&containerpb.Cluster{Name: "v", InitialNodeCount: 1, InitialClusterVersion: "1.34.2-gke.1008000"},
			)

			_, err := e.gc.CreateNodePool(t.Context(), &containerpb.CreateNodePoolRequest{
				Parent: clusterPath(zone, "v"), NodePool: tt.pool,
			})
			assertCode(t, codes.InvalidArgument, err, tt.want)
		})
	}
}

func TestSetNodePoolSizeAndAutoscaling(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.ready(
		t,
		region,
		&containerpb.Cluster{Name: "sz", NodePools: []*containerpb.NodePool{{Name: "p", InitialNodeCount: 1}}},
	)
	pool := poolPath(region, "sz", "p")

	op, err := e.gc.SetNodePoolSize(t.Context(), &containerpb.SetNodePoolSizeRequest{Name: pool, NodeCount: 4})
	require.NoError(t, err)
	assert.Equal(t, containerpb.Operation_SET_NODE_POOL_SIZE, op.GetOperationType())
	assert.Equal(t, containerpb.NodePool_RECONCILING, e.getPool(t, region, "sz", "p").GetStatus())

	_, err = e.gc.SetNodePoolSize(t.Context(), &containerpb.SetNodePoolSizeRequest{Name: pool, NodeCount: 5})
	assertCode(t, codes.FailedPrecondition, err, "incompatible operation "+op.GetName())

	e.settle()
	assert.Equal(t, int32(12), e.getCluster(t, region, "sz").GetCurrentNodeCount())

	_, err = e.gc.SetNodePoolSize(t.Context(), &containerpb.SetNodePoolSizeRequest{Name: pool, NodeCount: -1})
	assertCode(t, codes.InvalidArgument, err, "Node count")

	bad := []*containerpb.NodePoolAutoscaling{
		{Enabled: false, MaxNodeCount: 3},
		{Enabled: true},
		{Enabled: true, MinNodeCount: -1, MaxNodeCount: 3},
		{Enabled: true, MinNodeCount: 5, MaxNodeCount: 3},
		{Enabled: true, MaxNodeCount: 5000},
		{Enabled: true, MaxNodeCount: 3, TotalMaxNodeCount: 6},
	}
	for _, a := range bad {
		_, err = e.gc.SetNodePoolAutoscaling(
			t.Context(),
			&containerpb.SetNodePoolAutoscalingRequest{Name: pool, Autoscaling: a},
		)
		assertCode(t, codes.InvalidArgument, err, "")
	}

	_, err = e.gc.SetNodePoolAutoscaling(t.Context(), &containerpb.SetNodePoolAutoscalingRequest{Name: pool})
	assertCode(t, codes.InvalidArgument, err, "must be specified")

	_, err = e.gc.SetNodePoolAutoscaling(t.Context(), &containerpb.SetNodePoolAutoscalingRequest{
		Name: pool, Autoscaling: &containerpb.NodePoolAutoscaling{Enabled: true, MinNodeCount: 5, MaxNodeCount: 9},
	})
	require.NoError(t, err)
	e.settle()

	p := e.getPool(t, region, "sz", "p")
	assert.True(t, p.GetAutoscaling().GetEnabled())
	assert.Equal(t, int32(9), p.GetAutoscaling().GetMaxNodeCount())
	assert.Equal(
		t,
		int32(15),
		e.getCluster(t, region, "sz").GetCurrentNodeCount(),
		"size clamps up to the autoscaler minimum in every zone",
	)

	_, err = e.gc.SetNodePoolSize(t.Context(), &containerpb.SetNodePoolSizeRequest{Name: pool, NodeCount: 2})
	assertCode(t, codes.FailedPrecondition, err, "autoscaling enabled")

	_, err = e.gc.UpdateCluster(t.Context(), &containerpb.UpdateClusterRequest{
		Name: clusterPath(region, "sz"),
		Update: &containerpb.ClusterUpdate{
			DesiredNodePoolId: "p",
			DesiredNodePoolAutoscaling: &containerpb.NodePoolAutoscaling{
				Enabled:      true,
				MinNodeCount: 1,
				MaxNodeCount: 2,
			},
		},
	})
	require.NoError(t, err)
	e.settle()
	assert.Equal(t, int32(2), e.getPool(t, region, "sz", "p").GetAutoscaling().GetMaxNodeCount())
}

func TestSetNodePoolManagement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mgmt       *containerpb.NodeManagement
		name       string
		wantMsg    string
		channel    containerpb.ReleaseChannel_Channel
		want       codes.Code
		wantRepair bool
	}{
		{name: "no channel", mgmt: &containerpb.NodeManagement{AutoRepair: true}, wantRepair: true},
		{name: "off", mgmt: &containerpb.NodeManagement{}},
		{
			name:    "channel needs upgrade",
			channel: containerpb.ReleaseChannel_REGULAR,
			mgmt: &containerpb.NodeManagement{
				AutoRepair: true,
			},
			want:    codes.InvalidArgument,
			wantMsg: "release channel",
		},
		{
			name: "channel ok", channel: containerpb.ReleaseChannel_REGULAR, wantRepair: true,
			mgmt: &containerpb.NodeManagement{AutoUpgrade: true, AutoRepair: true},
		},
		{name: "nil", want: codes.InvalidArgument, wantMsg: "must be specified"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			cl := stdCluster("mg")
			if tt.channel != containerpb.ReleaseChannel_UNSPECIFIED {
				cl.ReleaseChannel = &containerpb.ReleaseChannel{Channel: tt.channel}
			}

			e.ready(t, zone, cl)

			op, err := e.gc.SetNodePoolManagement(t.Context(), &containerpb.SetNodePoolManagementRequest{
				Name: poolPath(zone, "mg", "default-pool"), Management: tt.mgmt,
			})
			if tt.want != codes.OK {
				assertCode(t, tt.want, err, tt.wantMsg)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, containerpb.Operation_SET_NODE_POOL_MANAGEMENT, op.GetOperationType())
			e.settle()
			assert.Equal(t, tt.wantRepair, e.getPool(t, zone, "mg", "default-pool").GetManagement().GetAutoRepair())
		})
	}
}

func TestNodeUpgradeAndRollback(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.ready(t, zone, &containerpb.Cluster{Name: "up", InitialNodeCount: 1, InitialClusterVersion: "1.34.2-gke.1008000"})
	pool := poolPath(zone, "up", "default-pool")

	_, err := e.gc.RollbackNodePoolUpgrade(t.Context(), &containerpb.RollbackNodePoolUpgradeRequest{Name: pool})
	assertCode(t, codes.FailedPrecondition, err, "no upgrade to roll back")

	_, err = e.gc.UpdateNodePool(
		t.Context(),
		&containerpb.UpdateNodePoolRequest{Name: pool, NodeVersion: "1.35.0-gke.1233000"},
	)
	assertCode(t, codes.InvalidArgument, err, "at most 2 minor")

	_, err = e.gc.UpdateCluster(t.Context(), &containerpb.UpdateClusterRequest{
		Name: clusterPath(zone, "up"), Update: &containerpb.ClusterUpdate{DesiredMasterVersion: "1.35.1-gke.1000000"},
	})
	require.NoError(t, err)
	e.settle()

	op, err := e.gc.UpdateNodePool(t.Context(), &containerpb.UpdateNodePoolRequest{Name: pool, NodeVersion: "latest"})
	require.NoError(t, err)
	assert.Equal(t, containerpb.Operation_UPGRADE_NODES, op.GetOperationType())
	assert.Equal(t, containerpb.NodePool_RECONCILING, e.getPool(t, zone, "up", "default-pool").GetStatus())
	e.settle()

	assert.Equal(t, "1.35.1-gke.1000000", e.getPool(t, zone, "up", "default-pool").GetVersion())
	assert.Equal(t, "1.35.1-gke.1000000", e.getCluster(t, zone, "up").GetCurrentNodeVersion())

	_, err = e.gc.UpdateNodePool(
		t.Context(),
		&containerpb.UpdateNodePoolRequest{Name: pool, NodeVersion: "1.34.2-gke.1008000"},
	)
	assertCode(t, codes.InvalidArgument, err, "older than")

	rb, err := e.gc.RollbackNodePoolUpgrade(t.Context(), &containerpb.RollbackNodePoolUpgradeRequest{Name: pool})
	require.NoError(t, err)
	assert.Equal(t, containerpb.Operation_UPGRADE_NODES, rb.GetOperationType())
	e.settle()
	assert.Equal(t, "1.34.2-gke.1008000", e.getPool(t, zone, "up", "default-pool").GetVersion())

	_, err = e.gc.RollbackNodePoolUpgrade(t.Context(), &containerpb.RollbackNodePoolUpgradeRequest{Name: pool})
	assertCode(t, codes.FailedPrecondition, err, "no upgrade to roll back")

	_, err = e.gc.UpdateCluster(t.Context(), &containerpb.UpdateClusterRequest{
		Name: clusterPath(zone, "up"), Update: &containerpb.ClusterUpdate{DesiredNodeVersion: "1.35.1-gke.1000000"},
	})
	require.NoError(t, err)
	e.settle()
	assert.Equal(t, "1.35.1-gke.1000000", e.getPool(t, zone, "up", "default-pool").GetVersion())
}

func TestUpdateNodePool(t *testing.T) {
	t.Parallel()

	tests := []struct {
		req     *containerpb.UpdateNodePoolRequest
		check   func(t *testing.T, p *containerpb.NodePool)
		name    string
		wantMsg string
		want    codes.Code
	}{
		{
			name: "labels taints tags", req: &containerpb.UpdateNodePoolRequest{
				Labels: &containerpb.NodeLabels{Labels: map[string]string{"a": "b"}},
				Taints: &containerpb.NodeTaints{
					Taints: []*containerpb.NodeTaint{{Key: "k", Value: "v", Effect: containerpb.NodeTaint_NO_EXECUTE}},
				},
				Tags: &containerpb.NetworkTags{Tags: []string{"t1", "t2"}},
			},
			check: func(t *testing.T, p *containerpb.NodePool) {
				t.Helper()
				assert.Equal(t, map[string]string{"a": "b"}, p.GetConfig().GetLabels())
				assert.Equal(t, containerpb.NodeTaint_NO_EXECUTE, p.GetConfig().GetTaints()[0].GetEffect())
				assert.Equal(t, []string{"t1", "t2"}, p.GetConfig().GetTags())
			},
		},
		{
			name: "machine type and disk",
			req:  &containerpb.UpdateNodePoolRequest{MachineType: "n2-standard-8", DiskSizeGb: 200, DiskType: "pd-ssd"},
			check: func(t *testing.T, p *containerpb.NodePool) {
				t.Helper()
				assert.Equal(t, "n2-standard-8", p.GetConfig().GetMachineType())
				assert.Equal(t, int32(200), p.GetConfig().GetDiskSizeGb())
				assert.Equal(t, "pd-ssd", p.GetConfig().GetDiskType())
			},
		},
		{
			name: "upgrade settings and kubelet", req: &containerpb.UpdateNodePoolRequest{
				UpgradeSettings: &containerpb.NodePool_UpgradeSettings{MaxSurge: 3},
				KubeletConfig:   &containerpb.NodeKubeletConfig{CpuManagerPolicy: "static"},
			},
			check: func(t *testing.T, p *containerpb.NodePool) {
				t.Helper()
				assert.Equal(t, int32(3), p.GetUpgradeSettings().GetMaxSurge())
				assert.Equal(t, "static", p.GetConfig().GetKubeletConfig().GetCpuManagerPolicy())
			},
		},
		{
			name: "image type", req: &containerpb.UpdateNodePoolRequest{ImageType: "UBUNTU_CONTAINERD"},
			check: func(t *testing.T, p *containerpb.NodePool) {
				t.Helper()
				assert.Equal(t, "UBUNTU_CONTAINERD", p.GetConfig().GetImageType())
			},
		},
		{
			name:    "bad image type",
			req:     &containerpb.UpdateNodePoolRequest{ImageType: "x"},
			want:    codes.InvalidArgument,
			wantMsg: "not supported",
		},
		{
			name:    "bad machine type",
			req:     &containerpb.UpdateNodePoolRequest{MachineType: "Not A Type"},
			want:    codes.InvalidArgument,
			wantMsg: "machine type",
		},
		{
			name: "bad label",
			req: &containerpb.UpdateNodePoolRequest{
				Labels: &containerpb.NodeLabels{Labels: map[string]string{"-x": "y"}},
			},
			want:    codes.InvalidArgument,
			wantMsg: "label",
		},
		{
			name:    "unsupported",
			req:     &containerpb.UpdateNodePoolRequest{Image: "custom"},
			want:    codes.InvalidArgument,
			wantMsg: "not supported by the emulator",
		},
		{
			name:    "stale etag",
			req:     &containerpb.UpdateNodePoolRequest{Etag: "stale", MachineType: "n2-standard-2"},
			want:    codes.FailedPrecondition,
			wantMsg: "etag",
		},
		{name: "empty", req: &containerpb.UpdateNodePoolRequest{}, want: codes.InvalidArgument, wantMsg: "No update"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.ready(t, zone, stdCluster("un"))
			tt.req.Name = poolPath(zone, "un", "default-pool")

			_, err := e.gc.UpdateNodePool(t.Context(), tt.req)
			if tt.want != codes.OK {
				assertCode(t, tt.want, err, tt.wantMsg)
				assert.Equal(t, containerpb.NodePool_RUNNING, e.getPool(t, zone, "un", "default-pool").GetStatus())

				return
			}

			require.NoError(t, err)
			e.settle()
			tt.check(t, e.getPool(t, zone, "un", "default-pool"))
		})
	}
}

func TestNodePoolEtag(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.ready(t, zone, stdCluster("et"))

	p := e.getPool(t, zone, "et", "default-pool")
	_, err := e.gc.UpdateNodePool(t.Context(), &containerpb.UpdateNodePoolRequest{
		Name: poolPath(zone, "et", "default-pool"), Etag: p.GetEtag(), MachineType: "n2-standard-2",
	})
	require.NoError(t, err)
	e.settle()
	assert.NotEqual(t, p.GetEtag(), e.getPool(t, zone, "et", "default-pool").GetEtag())
}

func TestNodePoolNotFound(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.ready(t, zone, stdCluster("nf"))
	missing := poolPath(zone, "nf", "ghost")

	_, err := e.gc.GetNodePool(t.Context(), &containerpb.GetNodePoolRequest{Name: missing})
	assertCode(t, codes.NotFound, err, missing)

	_, err = e.gc.DeleteNodePool(t.Context(), &containerpb.DeleteNodePoolRequest{Name: missing})
	assertCode(t, codes.NotFound, err, "")

	_, err = e.gc.SetNodePoolSize(t.Context(), &containerpb.SetNodePoolSizeRequest{Name: missing, NodeCount: 1})
	assertCode(t, codes.NotFound, err, "")

	_, err = e.gc.ListNodePools(t.Context(), &containerpb.ListNodePoolsRequest{Parent: clusterPath(zone, "ghost")})
	assertCode(t, codes.NotFound, err, "")

	_, err = e.gc.CreateNodePool(t.Context(), &containerpb.CreateNodePoolRequest{Parent: clusterPath(zone, "nf")})
	assertCode(t, codes.InvalidArgument, err, "Node pool must be specified")
}
