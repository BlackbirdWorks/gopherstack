package gcpcompute_test

import (
	"errors"
	"net/http"
	"testing"

	compute "cloud.google.com/go/compute/apiv1"
	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/proto"
)

func (e *env) regionDisks(t *testing.T) *compute.RegionDisksClient {
	t.Helper()

	return client(t, e, compute.NewRegionDisksRESTClient)
}

func (e *env) instantSnaps(t *testing.T) *compute.InstantSnapshotsClient {
	t.Helper()

	return client(t, e, compute.NewInstantSnapshotsRESTClient)
}

func (e *env) regionInstantSnaps(t *testing.T) *compute.RegionInstantSnapshotsClient {
	t.Helper()

	return client(t, e, compute.NewRegionInstantSnapshotsRESTClient)
}

func (e *env) machineImages(t *testing.T) *compute.MachineImagesClient {
	t.Helper()

	return client(t, e, compute.NewMachineImagesRESTClient)
}

func (e *env) reservations(t *testing.T) *compute.ReservationsClient {
	t.Helper()

	return client(t, e, compute.NewReservationsRESTClient)
}

func replicas() []string {
	return []string{"zones/" + region + "-a", "zones/" + region + "-b"}
}

func TestRegionalDisks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		disk   *computepb.Disk
		name   string
		status int
	}{
		{name: "ok", disk: &computepb.Disk{Name: s("rd"), SizeGb: proto.Int64(200), ReplicaZones: replicas()}},
		{name: "ssd small", disk: &computepb.Disk{
			Name: s("rd"), SizeGb: proto.Int64(20), ReplicaZones: replicas(),
			Type: s("regions/" + region + "/diskTypes/pd-ssd"),
		}},
		{
			name:   "needs replicas",
			disk:   &computepb.Disk{Name: s("rd"), SizeGb: proto.Int64(200)},
			status: http.StatusBadRequest,
		},
		{name: "same replica twice", disk: &computepb.Disk{
			Name: s(
				"rd",
			),
			SizeGb:       proto.Int64(200),
			ReplicaZones: []string{"zones/" + region + "-a", "zones/" + region + "-a"},
		}, status: http.StatusBadRequest},
		{name: "foreign replica", disk: &computepb.Disk{
			Name: s(
				"rd",
			),
			SizeGb:       proto.Int64(200),
			ReplicaZones: []string{"zones/" + region + "-a", "zones/" + otherZone},
		}, status: http.StatusBadRequest},
		{name: "standard minimum", disk: &computepb.Disk{
			Name: s("rd"), SizeGb: proto.Int64(20), ReplicaZones: replicas(),
		}, status: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.baseNet(t)

			op, err := e.regionDisks(t).Insert(t.Context(), &computepb.InsertRegionDiskRequest{
				Project: project, Region: region, DiskResource: tt.disk,
			})
			if tt.status != 0 {
				assertAPIError(t, err, tt.status, "invalid")

				return
			}

			e.done(t, op, err)

			d, err := e.regionDisks(t).
				Get(t.Context(), &computepb.GetRegionDiskRequest{Project: project, Region: region, Disk: "rd"})
			require.NoError(t, err)
			assert.Contains(t, d.GetRegion(), "/regions/"+region)
			assert.Empty(t, d.GetZone())
			assert.Len(t, d.GetReplicaZones(), 2)
			assert.Contains(t, d.GetSelfLink(), "/regions/"+region+"/disks/rd")

			_, err = e.disks(t).Get(t.Context(), &computepb.GetDiskRequest{Project: project, Zone: zone, Disk: "rd"})
			assertAPIError(t, err, http.StatusNotFound, "notFound")
		})
	}
}

func TestRegionalDiskLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.baseNet(t)

	op, err := e.regionDisks(t).Insert(t.Context(), &computepb.InsertRegionDiskRequest{
		Project: project, Region: region,
		DiskResource: &computepb.Disk{Name: s("rd"), SizeGb: proto.Int64(200), ReplicaZones: replicas()},
	})
	e.done(t, op, err)

	op, err = e.regionDisks(t).Resize(t.Context(), &computepb.ResizeRegionDiskRequest{
		Project: project, Region: region, Disk: "rd",
		RegionDisksResizeRequestResource: &computepb.RegionDisksResizeRequest{SizeGb: proto.Int64(300)},
	})
	e.done(t, op, err)

	op, err = e.regionDisks(t).CreateSnapshot(t.Context(), &computepb.CreateSnapshotRegionDiskRequest{
		Project: project, Region: region, Disk: "rd", SnapshotResource: &computepb.Snapshot{Name: s("rsnap")},
	})
	e.done(t, op, err)

	snap, err := e.snapshots(t).Get(t.Context(), &computepb.GetSnapshotRequest{Project: project, Snapshot: "rsnap"})
	require.NoError(t, err)
	assert.Equal(t, int64(300), snap.GetDiskSizeGb())

	spec := vmSpec("vm1")
	spec.Disks = append(spec.Disks, &computepb.AttachedDisk{
		Source: s("regions/" + region + "/disks/rd"), DeviceName: s("data"), AutoDelete: new(false),
	})
	e.vm(t, spec)

	d, err := e.regionDisks(t).
		Get(t.Context(), &computepb.GetRegionDiskRequest{Project: project, Region: region, Disk: "rd"})
	require.NoError(t, err)
	require.Len(t, d.GetUsers(), 1)
	assert.Contains(t, d.GetUsers()[0], "/instances/vm1")

	_, err = e.regionDisks(t).
		Delete(t.Context(), &computepb.DeleteRegionDiskRequest{Project: project, Region: region, Disk: "rd"})
	assertAPIError(t, err, http.StatusBadRequest, "resourceInUseByAnotherResource")

	bad := vmSpec("vm2")
	bad.Disks = append(
		bad.Disks,
		&computepb.AttachedDisk{Source: s("regions/" + region + "/disks/rd"), Mode: s("READ_ONLY")},
	)
	_, err = e.instances(t).
		Insert(t.Context(), &computepb.InsertInstanceRequest{Project: project, Zone: "us-central1-c", InstanceResource: bad})
	assertAPIError(t, err, http.StatusBadRequest, "")

	op, err = e.instances(t).
		Delete(t.Context(), &computepb.DeleteInstanceRequest{Project: project, Zone: zone, Instance: "vm1"})
	e.done(t, op, err)

	op, err = e.regionDisks(t).
		Delete(t.Context(), &computepb.DeleteRegionDiskRequest{Project: project, Region: region, Disk: "rd"})
	e.done(t, op, err)

	it := e.disks(t).AggregatedList(t.Context(), &computepb.AggregatedListDisksRequest{Project: project})
	for {
		pair, nerr := it.Next()
		if errors.Is(nerr, iterator.Done) {
			break
		}

		require.NoError(t, nerr)
		assert.Empty(t, pair.Value.GetDisks())
	}
}

func TestInstantSnapshots(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.baseNet(t)
	e.vm(t, vmSpec("vm1"))

	op, err := e.disks(t).Insert(t.Context(), &computepb.InsertDiskRequest{
		Project: project, Zone: zone, DiskResource: &computepb.Disk{Name: s("zd"), SizeGb: proto.Int64(30)},
	})
	e.done(t, op, err)

	_, err = e.instantSnaps(t).Insert(t.Context(), &computepb.InsertInstantSnapshotRequest{
		Project: project,
		Zone:    zone,
		InstantSnapshotResource: &computepb.InstantSnapshot{
			Name:       s("is"),
			SourceDisk: s("zones/" + zone + "/disks/nope"),
		},
	})
	assertAPIError(t, err, http.StatusNotFound, "notFound")

	op, err = e.instantSnaps(t).Insert(t.Context(), &computepb.InsertInstantSnapshotRequest{
		Project: project, Zone: zone, InstantSnapshotResource: &computepb.InstantSnapshot{
			Name: s("is"), SourceDisk: s("zones/" + zone + "/disks/zd"), Labels: map[string]string{"k": "v"},
		},
	})
	e.done(t, op, err)

	got, err := e.instantSnaps(t).
		Get(t.Context(), &computepb.GetInstantSnapshotRequest{Project: project, Zone: zone, InstantSnapshot: "is"})
	require.NoError(t, err)
	assert.Equal(t, "READY", got.GetStatus())
	assert.Equal(t, int64(30), got.GetDiskSizeGb())
	assert.Contains(t, got.GetSourceDisk(), "/disks/zd")

	op, err = e.disks(t).Insert(t.Context(), &computepb.InsertDiskRequest{
		Project: project, Zone: zone, DiskResource: &computepb.Disk{
			Name: s("restored"), SourceInstantSnapshot: s("zones/" + zone + "/instantSnapshots/is"),
		},
	})
	e.done(t, op, err)

	restored, err := e.disks(t).
		Get(t.Context(), &computepb.GetDiskRequest{Project: project, Zone: zone, Disk: "restored"})
	require.NoError(t, err)
	assert.Equal(t, int64(30), restored.GetSizeGb(), "size follows the instant snapshot")
	assert.Contains(t, restored.GetSourceInstantSnapshot(), "/instantSnapshots/is")

	it := e.instantSnaps(t).List(t.Context(), &computepb.ListInstantSnapshotsRequest{Project: project, Zone: zone})
	n := 0

	for {
		_, nerr := it.Next()
		if errors.Is(nerr, iterator.Done) {
			break
		}

		require.NoError(t, nerr)

		n++
	}

	assert.Equal(t, 1, n)

	op, err = e.instantSnaps(t).
		Delete(t.Context(), &computepb.DeleteInstantSnapshotRequest{Project: project, Zone: zone, InstantSnapshot: "is"})
	e.done(t, op, err)

	_, err = e.instantSnaps(t).
		Get(t.Context(), &computepb.GetInstantSnapshotRequest{Project: project, Zone: zone, InstantSnapshot: "is"})
	assertAPIError(t, err, http.StatusNotFound, "notFound")
}

func TestRegionalInstantSnapshot(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.baseNet(t)

	op, err := e.regionDisks(t).Insert(t.Context(), &computepb.InsertRegionDiskRequest{
		Project: project, Region: region,
		DiskResource: &computepb.Disk{Name: s("rd"), SizeGb: proto.Int64(200), ReplicaZones: replicas()},
	})
	e.done(t, op, err)

	op, err = e.regionInstantSnaps(t).Insert(t.Context(), &computepb.InsertRegionInstantSnapshotRequest{
		Project: project, Region: region, InstantSnapshotResource: &computepb.InstantSnapshot{
			Name: s("ris"), SourceDisk: s("regions/" + region + "/disks/rd"),
		},
	})
	e.done(t, op, err)

	got, err := e.regionInstantSnaps(t).Get(t.Context(), &computepb.GetRegionInstantSnapshotRequest{
		Project: project, Region: region, InstantSnapshot: "ris",
	})
	require.NoError(t, err)
	assert.Contains(t, got.GetRegion(), "/regions/"+region)
	assert.Equal(t, int64(200), got.GetDiskSizeGb())

	_, err = e.instantSnaps(t).
		Get(t.Context(), &computepb.GetInstantSnapshotRequest{Project: project, Zone: zone, InstantSnapshot: "ris"})
	assertAPIError(t, err, http.StatusNotFound, "notFound")
}

func TestMachineImages(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.baseNet(t)

	spec := vmSpec("vm1")
	spec.Labels = map[string]string{"role": "web"}
	spec.Tags = &computepb.Tags{Items: []string{"http"}}
	e.vm(t, spec)

	_, err := e.machineImages(t).Insert(t.Context(), &computepb.InsertMachineImageRequest{
		Project: project, MachineImageResource: &computepb.MachineImage{Name: s("mi")},
	})
	assertAPIError(t, err, http.StatusBadRequest, "required")

	_, err = e.machineImages(t).Insert(t.Context(), &computepb.InsertMachineImageRequest{
		Project: project, MachineImageResource: &computepb.MachineImage{Name: s("mi")},
		SourceInstance: s("zones/" + zone + "/instances/none"),
	})
	assertAPIError(t, err, http.StatusNotFound, "notFound")

	op, err := e.machineImages(t).Insert(t.Context(), &computepb.InsertMachineImageRequest{
		Project: project, MachineImageResource: &computepb.MachineImage{Name: s("mi")},
		SourceInstance: s("zones/" + zone + "/instances/vm1"),
	})
	e.done(t, op, err)

	mi, err := e.machineImages(t).
		Get(t.Context(), &computepb.GetMachineImageRequest{Project: project, MachineImage: "mi"})
	require.NoError(t, err)
	assert.Equal(t, "READY", mi.GetStatus())
	assert.Contains(t, mi.GetSourceInstance(), "/instances/vm1")
	assert.Equal(t, "e2-medium", mi.GetSourceInstanceProperties().GetMachineType())
	assert.Equal(t, "web", mi.GetSourceInstanceProperties().GetLabels()["role"])
	require.Len(t, mi.GetSavedDisks(), 1)
	assert.Positive(t, mi.GetTotalStorageBytes())

	op, err = e.instances(t).Insert(t.Context(), &computepb.InsertInstanceRequest{
		Project: project, Zone: zone, SourceMachineImage: s("global/machineImages/mi"),
		InstanceResource: &computepb.Instance{Name: s("clone")},
	})
	e.done(t, op, err)

	clone := e.getVM(t, "clone")
	assert.Equal(t, "web", clone.GetLabels()["role"])
	assert.Equal(t, []string{"http"}, clone.GetTags().GetItems())
	assert.Contains(t, clone.GetMachineType(), e2Medium)
	require.Len(t, clone.GetDisks(), 1)
	assert.Contains(t, clone.GetDisks()[0].GetSource(), "/disks/clone")

	orig := e.getVM(t, "vm1")
	assert.NotEqual(t, orig.GetNetworkInterfaces()[0].GetNetworkIP(), clone.GetNetworkInterfaces()[0].GetNetworkIP())

	_, err = e.instances(t).Insert(t.Context(), &computepb.InsertInstanceRequest{
		Project: project, Zone: zone, SourceMachineImage: s("global/machineImages/none"),
		InstanceResource: &computepb.Instance{Name: s("bad")},
	})
	assertAPIError(t, err, http.StatusNotFound, "notFound")

	op, err = e.machineImages(t).
		Delete(t.Context(), &computepb.DeleteMachineImageRequest{Project: project, MachineImage: "mi"})
	e.done(t, op, err)

	_, err = e.machineImages(t).
		Get(t.Context(), &computepb.GetMachineImageRequest{Project: project, MachineImage: "mi"})
	assertAPIError(t, err, http.StatusNotFound, "notFound")
}

func reservationSpec(name string, count int64, required bool) *computepb.Reservation {
	return &computepb.Reservation{
		Name: s(name), SpecificReservationRequired: new(required),
		SpecificReservation: &computepb.AllocationSpecificSKUReservation{
			Count: new(count),
			InstanceProperties: &computepb.AllocationSpecificSKUAllocationReservedInstanceProperties{
				MachineType: s(e2Medium),
			},
		},
	}
}

func (e *env) reserve(t *testing.T, r *computepb.Reservation) {
	t.Helper()

	op, err := e.reservations(t).Insert(t.Context(), &computepb.InsertReservationRequest{
		Project: project, Zone: zone, ReservationResource: r,
	})
	e.done(t, op, err)
}

func (e *env) rsvInUse(t *testing.T) int64 {
	t.Helper()

	r, err := e.reservations(t).
		Get(t.Context(), &computepb.GetReservationRequest{Project: project, Zone: zone, Reservation: "rsv"})
	require.NoError(t, err)

	return r.GetSpecificReservation().GetInUseCount()
}

func TestReservationConsumption(t *testing.T) {
	t.Parallel()

	tests := []struct {
		affinity *computepb.ReservationAffinity
		name     string
		wantUse  int64
		status   int
		required bool
	}{
		{name: "default consumes any", wantUse: 1},
		{
			name:     "no reservation",
			affinity: &computepb.ReservationAffinity{ConsumeReservationType: s("NO_RESERVATION")},
			wantUse:  0,
		},
		{
			name: "specific",
			affinity: &computepb.ReservationAffinity{
				ConsumeReservationType: s("SPECIFIC_RESERVATION"),
				Key:                    s("compute.googleapis.com/reservation-name"),
				Values:                 []string{"rsv"},
			},
			required: true,
			wantUse:  1,
		},
		{name: "any skips specific-only", required: true, wantUse: 0},
		{
			name: "specific missing reservation",
			affinity: &computepb.ReservationAffinity{
				ConsumeReservationType: s("SPECIFIC_RESERVATION"),
				Key:                    s("compute.googleapis.com/reservation-name"),
				Values:                 []string{"nope"},
			},
			status: http.StatusNotFound,
		},
		{
			name: "specific wrong key",
			affinity: &computepb.ReservationAffinity{
				ConsumeReservationType: s("SPECIFIC_RESERVATION"),
				Key:                    s("other"),
				Values:                 []string{"rsv"},
			},
			status: http.StatusBadRequest,
		},
		{
			name:     "key without specific",
			affinity: &computepb.ReservationAffinity{ConsumeReservationType: s("ANY_RESERVATION"), Key: s("x")},
			status:   http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.baseNet(t)
			e.reserve(t, reservationSpec("rsv", 2, tt.required))

			spec := vmSpec("vm1")
			spec.ReservationAffinity = tt.affinity

			op, err := e.instances(t).
				Insert(t.Context(), &computepb.InsertInstanceRequest{Project: project, Zone: zone, InstanceResource: spec})
			if tt.status != 0 {
				assertAPIError(t, err, tt.status, "")

				return
			}

			e.done(t, op, err)
			assert.Equal(t, tt.wantUse, e.rsvInUse(t))

			if tt.wantUse > 0 {
				e.stopVM1(t)
				assert.Zero(t, e.rsvInUse(t), "stopped instances release the slot")
			}
		})
	}
}

func TestReservationCapacityAndResize(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.baseNet(t)
	e.reserve(t, reservationSpec("rsv", 1, true))

	specific := func(name string) *computepb.Instance {
		spec := vmSpec(name)
		spec.ReservationAffinity = &computepb.ReservationAffinity{
			ConsumeReservationType: s(
				"SPECIFIC_RESERVATION",
			),
			Key:    s("compute.googleapis.com/reservation-name"),
			Values: []string{"rsv"},
		}

		return spec
	}

	e.vm(t, specific("vm1"))

	_, err := e.instances(t).
		Insert(t.Context(), &computepb.InsertInstanceRequest{Project: project, Zone: zone, InstanceResource: specific("vm2")})
	assertAPIError(t, err, http.StatusBadRequest, "")

	_, err = e.reservations(t).
		Delete(t.Context(), &computepb.DeleteReservationRequest{Project: project, Zone: zone, Reservation: "rsv"})
	assertAPIError(t, err, http.StatusBadRequest, "resourceInUseByAnotherResource")

	op, err := e.reservations(t).Resize(t.Context(), &computepb.ResizeReservationRequest{
		Project: project, Zone: zone, Reservation: "rsv",
		ReservationsResizeRequestResource: &computepb.ReservationsResizeRequest{SpecificSkuCount: proto.Int64(2)},
	})
	e.done(t, op, err)

	e.vm(t, specific("vm2"))
	assert.Equal(t, int64(2), e.rsvInUse(t))

	_, err = e.reservations(t).Resize(t.Context(), &computepb.ResizeReservationRequest{
		Project: project, Zone: zone, Reservation: "rsv",
		ReservationsResizeRequestResource: &computepb.ReservationsResizeRequest{SpecificSkuCount: proto.Int64(1)},
	})
	assertAPIError(t, err, http.StatusBadRequest, "invalid")

	op, err = e.reservations(t).Update(t.Context(), &computepb.UpdateReservationRequest{
		Project:     project,
		Zone:        zone,
		Reservation: "rsv",
		Paths:       s("specificReservation.count"),
		ReservationResource: &computepb.Reservation{
			SpecificReservation: &computepb.AllocationSpecificSKUReservation{Count: proto.Int64(3)},
		},
	})
	e.done(t, op, err)

	r, err := e.reservations(t).
		Get(t.Context(), &computepb.GetReservationRequest{Project: project, Zone: zone, Reservation: "rsv"})
	require.NoError(t, err)
	assert.Equal(t, int64(3), r.GetSpecificReservation().GetCount())
	assert.Equal(t, "READY", r.GetStatus())
}

func TestReservationValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		res  *computepb.Reservation
		name string
	}{
		{name: "no specific", res: &computepb.Reservation{Name: s("r")}},
		{name: "zero count", res: reservationSpec("r", 0, false)},
		{name: "bad name", res: reservationSpec("Bad_Name", 1, false)},
		{name: "unknown machine", res: &computepb.Reservation{
			Name: s("r"), SpecificReservation: &computepb.AllocationSpecificSKUReservation{
				Count: proto.Int64(
					1,
				),
				InstanceProperties: &computepb.AllocationSpecificSKUAllocationReservedInstanceProperties{
					MachineType: s("nope-1"),
				},
			},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			_, err := e.reservations(t).
				Insert(t.Context(), &computepb.InsertReservationRequest{Project: project, Zone: zone, ReservationResource: tt.res})
			require.Error(t, err)
		})
	}
}
