package ocicore_test

import (
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func reservationCfg(shape string, count int64) core.InstanceReservationConfigDetails {
	d := core.InstanceReservationConfigDetails{InstanceShape: new(shape), ReservedCount: new(count)}
	if shape == "VM.Standard.E4.Flex" {
		d.InstanceShapeConfig = &core.InstanceReservationShapeConfigDetails{
			Ocpus: new(float32(2)), MemoryInGBs: new(float32(32)),
		}
	}

	return d
}

func TestCapacityReservationValidation(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)

	tests := []struct {
		name     string
		cfgs     []core.InstanceReservationConfigDetails
		wantCode int
	}{
		{"unknown shape", []core.InstanceReservationConfigDetails{reservationCfg("VM.Nope", 1)}, http.StatusBadRequest},
		{
			"zero count",
			[]core.InstanceReservationConfigDetails{reservationCfg("VM.Standard2.1", 0)},
			http.StatusBadRequest,
		},
		{"flex without config", []core.InstanceReservationConfigDetails{
			{InstanceShape: new("VM.Standard.E4.Flex"), ReservedCount: new(int64(1))},
		}, http.StatusBadRequest},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := e.compute.CreateComputeCapacityReservation(
				t.Context(),
				core.CreateComputeCapacityReservationRequest{
					CompartmentId: new(testComp), AvailabilityDomain: new(testAD), InstanceReservationConfigs: tc.cfgs,
				},
			)
			assert.Equal(t, tc.wantCode, svcErr(t, err).GetHTTPStatusCode())
		})
	}
}

func TestCapacityReservation(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	pub := e.fixture(t).pub

	res, err := e.compute.CreateComputeCapacityReservation(ctx, core.CreateComputeCapacityReservationRequest{
		CompartmentId: new(testComp), AvailabilityDomain: new(testAD), DisplayName: new("rsv"),
		InstanceReservationConfigs: []core.InstanceReservationConfigDetails{reservationCfg("VM.Standard.E4.Flex", 2)},
	})
	require.NoError(t, err)
	assert.EqualValues(t, 2, *res.ReservedInstanceCount)
	assert.EqualValues(t, 0, *res.UsedInstanceCount)
	assert.NotEmpty(t, *res.OpcWorkRequestId)
	e.settle()

	launch := func(shape string, ocpus float32) (core.Instance, error) {
		return e.launch(t, pub, func(d *core.LaunchInstanceDetails) {
			d.CapacityReservationId, d.Shape = res.Id, new(shape)
			d.ShapeConfig = &core.LaunchInstanceShapeConfigDetails{Ocpus: new(ocpus), MemoryInGBs: new(float32(32))}
		})
	}

	_, err = launch("VM.Standard.E4.Flex", 4)
	assert.Equal(t, "LimitExceeded", svcErr(t, err).GetCode(), "no reservation for 4 OCPUs")

	i1, err := launch("VM.Standard.E4.Flex", 2)
	require.NoError(t, err)

	_, err = launch("VM.Standard.E4.Flex", 2)
	require.NoError(t, err)

	_, err = launch("VM.Standard.E4.Flex", 2)
	assert.Equal(t, "LimitExceeded", svcErr(t, err).GetCode(), "reservation exhausted")

	got, err := e.compute.GetComputeCapacityReservation(ctx, core.GetComputeCapacityReservationRequest{
		CapacityReservationId: res.Id,
	})
	require.NoError(t, err)
	assert.EqualValues(t, 2, *got.UsedInstanceCount)
	assert.EqualValues(t, 2, *got.InstanceReservationConfigs[0].UsedCount)

	li, err := e.compute.ListComputeCapacityReservationInstances(
		ctx,
		core.ListComputeCapacityReservationInstancesRequest{
			CapacityReservationId: res.Id,
		},
	)
	require.NoError(t, err)
	assert.Len(t, li.Items, 2)

	_, err = e.compute.DeleteComputeCapacityReservation(ctx, core.DeleteComputeCapacityReservationRequest{
		CapacityReservationId: res.Id,
	})
	assert.Equal(t, "Conflict", svcErr(t, err).GetCode(), "instances use it")

	_, err = e.compute.UpdateComputeCapacityReservation(ctx, core.UpdateComputeCapacityReservationRequest{
		CapacityReservationId:      res.Id,
		InstanceReservationConfigs: []core.InstanceReservationConfigDetails{reservationCfg("VM.Standard.E4.Flex", 1)},
	})
	assert.Equal(t, "Conflict", svcErr(t, err).GetCode(), "two instances use the reservation")

	_, err = e.compute.TerminateInstance(ctx, core.TerminateInstanceRequest{InstanceId: i1.Id})
	require.NoError(t, err)
	e.settle()

	got, err = e.compute.GetComputeCapacityReservation(ctx, core.GetComputeCapacityReservationRequest{
		CapacityReservationId: res.Id,
	})
	require.NoError(t, err)
	assert.EqualValues(t, 1, *got.UsedInstanceCount, "terminating releases the reservation")
}

func TestDedicatedVmHostPlacement(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	pub := e.fixture(t).pub

	_, err := e.compute.CreateDedicatedVmHost(ctx, core.CreateDedicatedVmHostRequest{
		CompartmentId: new(testComp), AvailabilityDomain: new(testAD), DedicatedVmHostShape: new("DVH.Nope"),
	})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())

	host, err := e.compute.CreateDedicatedVmHost(ctx, core.CreateDedicatedVmHostRequest{
		CompartmentId: new(
			testComp,
		),
		AvailabilityDomain:   new(testAD),
		DedicatedVmHostShape: new("DVH.Standard2.52"),
		DisplayName:          new("dvh"),
	})
	require.NoError(t, err)
	assert.InDelta(t, 48, *host.TotalOcpus, 0)
	assert.InDelta(t, 736, *host.TotalMemoryInGBs, 0)
	assert.NotEmpty(t, *host.OpcWorkRequestId)
	e.settle()

	launch := func(shape string) (core.Instance, error) {
		return e.launch(t, pub, func(d *core.LaunchInstanceDetails) {
			d.DedicatedVmHostId, d.Shape, d.ShapeConfig = host.Id, new(shape), nil
		})
	}

	_, err = e.launch(t, pub, func(d *core.LaunchInstanceDetails) { d.DedicatedVmHostId = host.Id })
	assert.Equal(
		t,
		http.StatusBadRequest,
		svcErr(t, err).GetHTTPStatusCode(),
		"an E4 flex shape does not run on a Standard2 host",
	)

	for range 2 {
		_, err = launch("VM.Standard2.16")
		require.NoError(t, err)
	}

	_, err = launch("VM.Standard2.16")
	assert.Equal(
		t,
		"LimitExceeded",
		svcErr(t, err).GetCode(),
		"each instance sits on one 24-OCPU socket; 16 OCPUs remain split",
	)

	_, err = launch("VM.Standard2.8")
	require.NoError(t, err, "a smaller shape still fits next to a 16-OCPU instance")

	got, err := e.compute.GetDedicatedVmHost(ctx, core.GetDedicatedVmHostRequest{DedicatedVmHostId: host.Id})
	require.NoError(t, err)
	assert.InDelta(t, 8, *got.RemainingOcpus, 0)

	li, err := e.compute.ListDedicatedVmHostInstances(ctx, core.ListDedicatedVmHostInstancesRequest{
		CompartmentId: new(testComp), DedicatedVmHostId: host.Id,
	})
	require.NoError(t, err)
	assert.Len(t, li.Items, 3)

	_, err = e.compute.DeleteDedicatedVmHost(ctx, core.DeleteDedicatedVmHostRequest{DedicatedVmHostId: host.Id})
	assert.Equal(t, "Conflict", svcErr(t, err).GetCode())

	shapes, err := e.compute.ListDedicatedVmHostInstanceShapes(ctx, core.ListDedicatedVmHostInstanceShapesRequest{
		CompartmentId: new(testComp), DedicatedVmHostShape: new("DVH.Standard2.52"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, shapes.Items)
}

func TestComputeCluster(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	pub := e.fixture(t).pub

	c, err := e.compute.CreateComputeCluster(ctx, core.CreateComputeClusterRequest{
		CompartmentId: new(testComp), AvailabilityDomain: new(testAD), DisplayName: new("hpc"),
	})
	require.NoError(t, err)
	assert.Equal(t, core.ComputeClusterLifecycleStateActive, c.LifecycleState)

	_, err = e.launch(t, pub, func(d *core.LaunchInstanceDetails) { d.ComputeClusterId = c.Id })
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode(), "a flex VM has no RDMA network")

	inst, err := e.launch(t, pub, func(d *core.LaunchInstanceDetails) {
		d.ComputeClusterId, d.Shape, d.ShapeConfig = c.Id, new("BM.HPC2.36"), nil
	})
	require.NoError(t, err)

	_, err = e.compute.DeleteComputeCluster(ctx, core.DeleteComputeClusterRequest{ComputeClusterId: c.Id})
	assert.Equal(t, "Conflict", svcErr(t, err).GetCode())

	_, err = e.compute.TerminateInstance(ctx, core.TerminateInstanceRequest{InstanceId: inst.Id})
	require.NoError(t, err)
	e.settle()

	_, err = e.compute.DeleteComputeCluster(ctx, core.DeleteComputeClusterRequest{ComputeClusterId: c.Id})
	require.NoError(t, err)

	got, err := e.compute.GetComputeCluster(ctx, core.GetComputeClusterRequest{ComputeClusterId: c.Id})
	require.NoError(t, err)
	assert.Equal(t, core.ComputeClusterLifecycleStateDeleted, got.LifecycleState)
}
