package gcpcompute_test

import (
	"testing"

	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestSnapshotRestoreNewResources(t *testing.T) {
	t.Parallel()

	src := newEnv(t)
	src.baseNet(t)
	src.vm(t, vmSpec("vm1"))
	src.reserve(t, reservationSpec("rsv", 2, false))

	op, err := src.regionDisks(t).Insert(t.Context(), &computepb.InsertRegionDiskRequest{
		Project: project, Region: region,
		DiskResource: &computepb.Disk{Name: s("rd"), SizeGb: proto.Int64(200), ReplicaZones: replicas()},
	})
	src.done(t, op, err)

	op, err = src.regionInstantSnaps(t).Insert(t.Context(), &computepb.InsertRegionInstantSnapshotRequest{
		Project: project, Region: region, InstantSnapshotResource: &computepb.InstantSnapshot{
			Name: s("ris"), SourceDisk: s("regions/" + region + "/disks/rd"),
		},
	})
	src.done(t, op, err)

	op, err = src.machineImages(t).Insert(t.Context(), &computepb.InsertMachineImageRequest{
		Project: project, MachineImageResource: &computepb.MachineImage{Name: s("mi")},
		SourceInstance: s("zones/" + zone + "/instances/vm1"),
	})
	src.done(t, op, err)

	dst := newEnv(t)
	require.NoError(t, dst.backend.Restore(t.Context(), src.backend.Snapshot(t.Context())))

	rd, err := dst.regionDisks(t).Get(t.Context(), &computepb.GetRegionDiskRequest{Project: project, Region: region, Disk: "rd"})
	require.NoError(t, err)
	assert.Len(t, rd.GetReplicaZones(), 2)

	is, err := dst.regionInstantSnaps(t).Get(t.Context(), &computepb.GetRegionInstantSnapshotRequest{
		Project: project, Region: region, InstantSnapshot: "ris",
	})
	require.NoError(t, err)
	assert.Equal(t, int64(200), is.GetDiskSizeGb())

	mi, err := dst.machineImages(t).Get(t.Context(), &computepb.GetMachineImageRequest{Project: project, MachineImage: "mi"})
	require.NoError(t, err)
	assert.Len(t, mi.GetSavedDisks(), 1)

	assert.Equal(t, int64(1), dst.inUse(t, "rsv"))
}
