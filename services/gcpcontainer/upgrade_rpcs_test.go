package gcpcontainer_test

import (
	"testing"
	"time"

	"cloud.google.com/go/container/apiv1/containerpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (e *env) startMasterUpgrade(t *testing.T, name string) *containerpb.Operation {
	t.Helper()

	op, err := e.gc.UpdateMaster(t.Context(), &containerpb.UpdateMasterRequest{
		Name: clusterPath(zone, name), MasterVersion: "1.34.3-gke.1051000",
	})
	require.NoError(t, err)

	return op
}

func TestCancelOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run     func(t *testing.T, e *env) string
		name    string
		wantMsg string
		want    codes.Code
	}{
		{
			name: "running upgrade",
			run: func(t *testing.T, e *env) string {
				t.Helper()

				return e.startMasterUpgrade(t, "c").GetName()
			},
		},
		{
			name: "finished upgrade", want: codes.FailedPrecondition, wantMsg: "already done",
			run: func(t *testing.T, e *env) string {
				t.Helper()

				op := e.startMasterUpgrade(t, "c")
				e.settle()

				return op.GetName()
			},
		},
		{
			name: "not cancellable", want: codes.FailedPrecondition, wantMsg: "cannot be cancelled",
			run: func(t *testing.T, e *env) string {
				t.Helper()

				op, err := e.gc.SetLabels(t.Context(), &containerpb.SetLabelsRequest{
					Name: clusterPath(zone, "c"), ResourceLabels: map[string]string{"a": "b"},
					LabelFingerprint: e.getCluster(t, zone, "c").GetLabelFingerprint(),
				})
				require.NoError(t, err)

				return op.GetName()
			},
		},
		{
			name: "unknown", want: codes.NotFound,
			run: func(*testing.T, *env) string { return "operation-0" },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.ready(t, zone, stdCluster("c"))

			name := tt.run(t, e)
			err := e.gc.CancelOperation(
				t.Context(),
				&containerpb.CancelOperationRequest{Name: zoneLoc + "/operations/" + name},
			)

			if tt.want != codes.OK {
				assertCode(t, tt.want, err, tt.wantMsg)

				return
			}

			require.NoError(t, err)

			got := e.getOp(t, &containerpb.Operation{Name: name})
			assert.Equal(t, containerpb.Operation_DONE, got.GetStatus())
			assert.Equal(t, int32(codes.Canceled), got.GetError().GetCode())
		})
	}
}

func TestCompleteControlPlaneUpgrade(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		version string
		wantMsg string
		idle    bool
		want    codes.Code
	}{
		{name: "running upgrade"},
		{name: "wrong version", version: "1.1.1-gke.1", want: codes.InvalidArgument, wantMsg: "version"},
		{name: "no upgrade", idle: true, want: codes.FailedPrecondition, wantMsg: "no control plane upgrade"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.ready(t, zone, stdCluster("c"))

			var started *containerpb.Operation
			if !tt.idle {
				started = e.startMasterUpgrade(t, "c")
			}

			op, err := e.gc.CompleteControlPlaneUpgrade(t.Context(), &containerpb.CompleteControlPlaneUpgradeRequest{
				Name: clusterPath(zone, "c"), Version: tt.version,
			})
			if tt.want != codes.OK {
				assertCode(t, tt.want, err, tt.wantMsg)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, started.GetName(), op.GetName())
			assert.Equal(t, containerpb.Operation_DONE, op.GetStatus())
			assert.Equal(t, containerpb.Operation_DONE, e.getOp(t, started).GetStatus())
		})
	}
}

func TestCompleteNodePoolUpgrade(t *testing.T) {
	t.Parallel()

	pool := poolPath(zone, "c", "default-pool")

	tests := []struct {
		name    string
		path    string
		wantMsg string
		idle    bool
		want    codes.Code
	}{
		{name: "running upgrade", path: pool},
		{name: "no upgrade", path: pool, idle: true, want: codes.FailedPrecondition, wantMsg: "no upgrade in progress"},
		{name: "unknown pool", path: poolPath(zone, "c", "ghost"), want: codes.NotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.ready(t, zone, stdCluster("c"))

			var started *containerpb.Operation

			if !tt.idle {
				var err error

				started, err = e.gc.UpdateNodePool(t.Context(), &containerpb.UpdateNodePoolRequest{
					Name: pool, NodeVersion: "1.34.2-gke.1008000",
				})
				require.NoError(t, err)
			}

			err := e.gc.CompleteNodePoolUpgrade(t.Context(), &containerpb.CompleteNodePoolUpgradeRequest{Name: tt.path})
			if tt.want != codes.OK {
				assertCode(t, tt.want, err, tt.wantMsg)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, containerpb.Operation_DONE, e.getOp(t, started).GetStatus())
		})
	}
}

func TestFetchUpgradeInfo(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.ready(t, zone, stdCluster("c"))

	info, err := e.gc.FetchClusterUpgradeInfo(t.Context(), &containerpb.FetchClusterUpgradeInfoRequest{
		Name: clusterPath(zone, "c"),
	})
	require.NoError(t, err)
	assert.Equal(t, "1.34.3-gke.1051000", info.GetPatchTargetVersion())
	assert.Equal(t, "1.35.1-gke.1000000", info.GetMinorTargetVersion())
	assert.Equal(
		t,
		[]containerpb.ClusterUpgradeInfo_AutoUpgradeStatus{containerpb.ClusterUpgradeInfo_ACTIVE},
		info.GetAutoUpgradeStatus(),
	)

	now := e.clock.Now()
	_, err = e.gc.SetMaintenancePolicy(t.Context(), &containerpb.SetMaintenancePolicyRequest{
		Name: clusterPath(zone, "c"),
		MaintenancePolicy: &containerpb.MaintenancePolicy{
			ResourceVersion: e.getCluster(t, zone, "c").GetMaintenancePolicy().GetResourceVersion(),
			Window: &containerpb.MaintenanceWindow{
				Policy: &containerpb.MaintenanceWindow_DailyMaintenanceWindow{
					DailyMaintenanceWindow: &containerpb.DailyMaintenanceWindow{StartTime: "03:00"},
				},
				MaintenanceExclusions: map[string]*containerpb.TimeWindow{"freeze": {
					StartTime: timestamppb.New(now.Add(-time.Hour)), EndTime: timestamppb.New(now.Add(time.Hour)),
					Options: &containerpb.TimeWindow_MaintenanceExclusionOptions{
						MaintenanceExclusionOptions: &containerpb.MaintenanceExclusionOptions{
							Scope: containerpb.MaintenanceExclusionOptions_NO_UPGRADES,
						},
					},
				}},
			}},
	})
	require.NoError(t, err)
	e.settle()

	info, err = e.gc.FetchClusterUpgradeInfo(t.Context(), &containerpb.FetchClusterUpgradeInfoRequest{
		Name: clusterPath(zone, "c"),
	})
	require.NoError(t, err)
	assert.Equal(
		t,
		[]containerpb.ClusterUpgradeInfo_AutoUpgradeStatus{containerpb.ClusterUpgradeInfo_UPGRADE_PAUSED},
		info.GetAutoUpgradeStatus(),
	)
	assert.Equal(t, []containerpb.ClusterUpgradeInfo_AutoUpgradePausedReason{
		containerpb.ClusterUpgradeInfo_MAINTENANCE_EXCLUSION_NO_UPGRADES,
	}, info.GetPausedReason())

	pool, err := e.gc.FetchNodePoolUpgradeInfo(t.Context(), &containerpb.FetchNodePoolUpgradeInfoRequest{
		Name: poolPath(zone, "c", "default-pool"),
	})
	require.NoError(t, err)
	assert.Equal(t, "1.34.3-gke.1051000", pool.GetPatchTargetVersion())
	assert.Equal(
		t,
		[]containerpb.NodePoolUpgradeInfo_AutoUpgradeStatus{containerpb.NodePoolUpgradeInfo_UPGRADE_PAUSED},
		pool.GetAutoUpgradeStatus(),
	)

	_, err = e.gc.FetchNodePoolUpgradeInfo(t.Context(), &containerpb.FetchNodePoolUpgradeInfoRequest{
		Name: poolPath(zone, "c", "ghost"),
	})
	assertCode(t, codes.NotFound, err, "")

	_, err = e.gc.FetchClusterUpgradeInfo(t.Context(), &containerpb.FetchClusterUpgradeInfoRequest{
		Name: clusterPath(zone, "ghost"),
	})
	assertCode(t, codes.NotFound, err, "")
}
