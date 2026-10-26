package gcpcompute_test

import (
	"net/http"
	"testing"
	"time"

	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
)

func TestOperationStatusFollowsClock(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	timing := gcpcomputeTiming()

	op, err := e.networks(t).Insert(t.Context(), &computepb.InsertNetworkRequest{
		Project: project, NetworkResource: &computepb.Network{Name: s("n1")},
	})
	require.NoError(t, err)

	ops := e.globalOps(t)

	tests := []struct {
		name    string
		at      time.Duration
		want    computepb.Operation_Status
		started bool
		ended   bool
	}{
		{"pending", 0, computepb.Operation_PENDING, false, false},
		{"still pending", timing.Pending - time.Millisecond, computepb.Operation_PENDING, false, false},
		{"running", timing.Pending, computepb.Operation_RUNNING, true, false},
		{"still running", timing.Done - time.Millisecond, computepb.Operation_RUNNING, true, false},
		{"done", timing.Done, computepb.Operation_DONE, true, true},
	}

	var elapsed time.Duration

	for _, tt := range tests {
		e.clock.Advance(tt.at - elapsed)
		elapsed = tt.at

		got, gerr := ops.Get(t.Context(), &computepb.GetGlobalOperationRequest{Project: project, Operation: op.Name()})
		require.NoError(t, gerr, tt.name)
		assert.Equal(t, tt.want, got.GetStatus(), tt.name)
		assert.Equal(t, tt.started, got.GetStartTime() != "", tt.name)
		assert.Equal(t, tt.ended, got.GetEndTime() != "", tt.name)
		assert.Equal(t, "compute#operation", got.GetKind())
		assert.Equal(t, "insert", got.GetOperationType())
	}
}

func TestWaitEndpointBlocksOnInjectedClock(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	insts := e.instances(t)
	e.baseNet(t)

	op, err := insts.Insert(
		t.Context(),
		&computepb.InsertInstanceRequest{Project: project, Zone: zone, InstanceResource: vmSpec("vm1")},
	)
	require.NoError(t, err)
	require.Equal(t, computepb.Operation_PENDING, op.Proto().GetStatus())

	start := e.clock.Now()
	waited, err := e.zoneOps(t).
		Wait(t.Context(), &computepb.WaitZoneOperationRequest{Project: project, Zone: zone, Operation: op.Name()})
	require.NoError(t, err)
	assert.Equal(t, computepb.Operation_DONE, waited.GetStatus())
	assert.Equal(
		t,
		gcpcomputeTiming().Stage,
		e.clock.Now().Sub(start),
		"wait sleeps exactly until the operation finishes",
	)
	assert.Equal(t, stRunning, e.getVM(t, "vm1").GetStatus())

	again, err := e.zoneOps(t).
		Wait(t.Context(), &computepb.WaitZoneOperationRequest{Project: project, Zone: zone, Operation: op.Name()})
	require.NoError(t, err)
	assert.Equal(t, computepb.Operation_DONE, again.GetStatus())
	assert.Equal(
		t,
		start.Add(gcpcomputeTiming().Stage),
		e.clock.Now(),
		"waiting on a finished operation does not sleep",
	)

	_, err = e.zoneOps(t).
		Wait(
			t.Context(),
			&computepb.WaitZoneOperationRequest{Project: project, Zone: zone, Operation: "operation-ghost"},
		)
	assertAPIError(t, err, http.StatusNotFound, "notFound")
}

func TestOperationScopes(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.baseNet(t)

	netOps, err := e.networks(t).
		Insert(
			t.Context(),
			&computepb.InsertNetworkRequest{Project: project, NetworkResource: &computepb.Network{Name: s("n2")}},
		)
	require.NoError(t, err)
	assert.Empty(t, netOps.Proto().GetZone())
	assert.Empty(t, netOps.Proto().GetRegion())

	subOp, err := e.subnets(t).Insert(t.Context(), &computepb.InsertSubnetworkRequest{
		Project: project,
		Region:  region,
		SubnetworkResource: &computepb.Subnetwork{
			Name:        s("s3"),
			Network:     s("global/networks/vpc"),
			IpCidrRange: s("10.77.0.0/24"),
		},
	})
	require.NoError(t, err)
	assert.Contains(t, subOp.Proto().GetRegion(), "/regions/"+region)

	vmOp, err := e.instances(t).
		Insert(
			t.Context(),
			&computepb.InsertInstanceRequest{Project: project, Zone: zone, InstanceResource: vmSpec("vm1")},
		)
	require.NoError(t, err)

	e.settle()

	tests := []struct {
		name string
		get  func() (string, error)
		want string
	}{
		{"global", func() (string, error) {
			o, gerr := e.globalOps(t).
				Get(t.Context(), &computepb.GetGlobalOperationRequest{Project: project, Operation: netOps.Name()})

			return o.GetTargetLink(), gerr
		}, "/global/networks/n2"},
		{"region", func() (string, error) {
			o, gerr := e.regionOps(t).
				Get(
					t.Context(),
					&computepb.GetRegionOperationRequest{Project: project, Region: region, Operation: subOp.Name()},
				)

			return o.GetTargetLink(), gerr
		}, "/subnetworks/s3"},
		{"zone", func() (string, error) {
			o, gerr := e.zoneOps(t).
				Get(
					t.Context(),
					&computepb.GetZoneOperationRequest{Project: project, Zone: zone, Operation: vmOp.Name()},
				)

			return o.GetTargetLink(), gerr
		}, "/instances/vm1"},
	}

	for _, tt := range tests {
		link, gerr := tt.get()
		require.NoError(t, gerr, tt.name)
		assert.Contains(t, link, tt.want, tt.name)
	}

	_, err = e.zoneOps(t).
		Get(
			t.Context(),
			&computepb.GetZoneOperationRequest{Project: project, Zone: zone, Operation: netOps.Name()},
		)
	assertAPIError(t, err, http.StatusNotFound, "notFound")

	_, err = e.zoneOps(t).
		Get(
			t.Context(),
			&computepb.GetZoneOperationRequest{Project: project, Zone: "mars-central1-a", Operation: vmOp.Name()},
		)
	assertAPIError(t, err, http.StatusNotFound, "notFound")

	listed := 0
	it := e.zoneOps(t).List(t.Context(), &computepb.ListZoneOperationsRequest{Project: project, Zone: zone})

	for {
		_, nerr := it.Next()
		if nerr != nil {
			require.ErrorIs(t, nerr, iterator.Done)

			break
		}

		listed++
	}

	assert.Equal(t, 1, listed)

	_, err = e.globalOps(t).
		Delete(t.Context(), &computepb.DeleteGlobalOperationRequest{Project: project, Operation: netOps.Name()})
	require.NoError(t, err)
	_, err = e.globalOps(t).
		Get(t.Context(), &computepb.GetGlobalOperationRequest{Project: project, Operation: netOps.Name()})
	assertAPIError(t, err, http.StatusNotFound, "notFound")
}
