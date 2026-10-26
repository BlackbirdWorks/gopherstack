package gcpcompute_test

import (
	"net/http"
	"testing"

	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/proto"
)

func TestImagesCatalog(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		project string
		family  string
		prefix  string
		reason  string
		status  int
	}{
		{"debian 12", "debian-cloud", "debian-12", "debian-12-bookworm-v", "", 200},
		{"ubuntu 2204", "ubuntu-os-cloud", "ubuntu-2204-lts", "ubuntu-2204-jammy-v", "", 200},
		{"ubuntu 2404", "ubuntu-os-cloud", "ubuntu-2404-lts-amd64", "ubuntu-2404-noble-amd64-v", "", 200},
		{"rocky 9", "rocky-linux-cloud", "rocky-linux-9", "rocky-linux-9-v", "", 200},
		{"unknown family", "debian-cloud", "debian-99", "", "notFound", 404},
		{"custom project has none", project, "debian-12", "", "notFound", 404},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			imgs := e.images(t)

			img, err := imgs.GetFromFamily(t.Context(), &computepb.GetFromFamilyImageRequest{Project: tt.project, Family: tt.family})
			if tt.status != http.StatusOK {
				assertAPIError(t, err, tt.status, tt.reason)

				return
			}

			require.NoError(t, err)
			assert.Contains(t, img.GetName(), tt.prefix)
			assert.Equal(t, tt.family, img.GetFamily())
			assert.Equal(t, "READY", img.GetStatus())
			assert.Equal(t, "https://www.googleapis.com/compute/v1/projects/"+tt.project+"/global/images/"+img.GetName(), img.GetSelfLink())
			assert.NotZero(t, img.GetDiskSizeGb())
			assert.NotEmpty(t, img.GetLicenses())

			byName, err := imgs.Get(t.Context(), &computepb.GetImageRequest{Project: tt.project, Image: img.GetName()})
			require.NoError(t, err)
			assert.Equal(t, img.GetId(), byName.GetId())
		})
	}
}

func TestImagesListAndWrites(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	imgs := e.images(t)

	it := imgs.List(t.Context(), &computepb.ListImagesRequest{Project: "debian-cloud"})

	var families []string

	for {
		img, err := it.Next()
		if err != nil {
			require.ErrorIs(t, err, iterator.Done)

			break
		}

		families = append(families, img.GetFamily())
	}

	assert.ElementsMatch(t, []string{"debian-11", "debian-12", "debian-13"}, families)

	_, err := imgs.Delete(t.Context(), &computepb.DeleteImageRequest{Project: "debian-cloud", Image: "debian-12-bookworm-v20260115"})
	assertAPIError(t, err, http.StatusForbidden, "forbidden")

	_, err = imgs.Insert(t.Context(), &computepb.InsertImageRequest{Project: "debian-cloud", ImageResource: &computepb.Image{Name: s("mine"), SourceDisk: s("zones/" + zone + "/disks/d")}})
	assertAPIError(t, err, http.StatusForbidden, "forbidden")
}

func TestDiskCreateFromImage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		disk   *computepb.Disk
		check  func(t *testing.T, d *computepb.Disk)
		name   string
		reason string
		status int
	}{
		{
			disk: &computepb.Disk{Name: s("blank"), SizeGb: proto.Int64(20), Type: s("zones/" + zone + "/diskTypes/pd-ssd")}, name: "blank pd-ssd", status: 200,
			check: func(t *testing.T, d *computepb.Disk) {
				assert.Equal(t, int64(20), d.GetSizeGb())
				assert.Contains(t, d.GetType(), "/zones/"+zone+"/diskTypes/pd-ssd")
				assert.Equal(t, "READY", d.GetStatus())
				assert.Equal(t, int64(4096), d.GetPhysicalBlockSizeBytes())
			},
		},
		{
			disk: &computepb.Disk{Name: s("fromfam"), SourceImage: s("projects/debian-cloud/global/images/family/debian-12")}, name: "image family", status: 200,
			check: func(t *testing.T, d *computepb.Disk) {
				assert.Equal(t, int64(10), d.GetSizeGb())
				assert.Contains(t, d.GetSourceImage(), "/projects/debian-cloud/global/images/debian-12-bookworm-v")
				assert.Contains(t, d.GetType(), "pd-standard")
				assert.NotEmpty(t, d.GetSourceImageId())
			},
		},
		{
			disk: &computepb.Disk{Name: s("bigger"), SourceImage: s("projects/rocky-linux-cloud/global/images/family/rocky-linux-9"), SizeGb: proto.Int64(50)}, name: "bigger than image", status: 200,
			check: func(t *testing.T, d *computepb.Disk) { assert.Equal(t, int64(50), d.GetSizeGb()) },
		},
		{disk: &computepb.Disk{Name: s("small"), SourceImage: s("projects/rocky-linux-cloud/global/images/family/rocky-linux-9"), SizeGb: proto.Int64(10)}, name: "smaller than image", status: 400, reason: "invalid"},
		{disk: &computepb.Disk{Name: s("tiny"), SizeGb: proto.Int64(5)}, name: "below minimum", status: 400, reason: "invalid"},
		{disk: &computepb.Disk{Name: s("huge"), SizeGb: proto.Int64(70000)}, name: "above maximum", status: 400, reason: "invalid"},
		{disk: &computepb.Disk{Name: s("badtype"), SizeGb: proto.Int64(10), Type: s("zones/" + zone + "/diskTypes/floppy")}, name: "unknown type", status: 404, reason: "notFound"},
		{disk: &computepb.Disk{Name: s("noimg"), SourceImage: s("projects/debian-cloud/global/images/family/nope")}, name: "unknown image", status: 404, reason: "notFound"},
		{disk: &computepb.Disk{Name: s("nosnap"), SourceSnapshot: s("global/snapshots/ghost")}, name: "unknown snapshot", status: 404, reason: "notFound"},
		{disk: &computepb.Disk{Name: s("dup"), SizeGb: proto.Int64(10)}, name: "duplicate", status: 409, reason: "alreadyExists"},
		{disk: &computepb.Disk{SizeGb: proto.Int64(10)}, name: "no name", status: 400, reason: "required"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			disks := e.disks(t)

			op, err := disks.Insert(t.Context(), &computepb.InsertDiskRequest{Project: project, Zone: zone, DiskResource: &computepb.Disk{Name: s("dup"), SizeGb: proto.Int64(10)}})
			e.done(t, op, err)

			op, err = disks.Insert(t.Context(), &computepb.InsertDiskRequest{Project: project, Zone: zone, DiskResource: tt.disk})
			if tt.status != http.StatusOK {
				assertAPIError(t, err, tt.status, tt.reason)

				return
			}

			e.done(t, op, err)

			got, err := disks.Get(t.Context(), &computepb.GetDiskRequest{Project: project, Zone: zone, Disk: tt.disk.GetName()})
			require.NoError(t, err)
			assert.Equal(t, "compute#disk", got.GetKind())
			assert.Contains(t, got.GetZone(), "/zones/"+zone)
			tt.check(t, got)
		})
	}
}

func TestDiskResizeSnapshotRestore(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	disks := e.disks(t)

	op, err := disks.Insert(t.Context(), &computepb.InsertDiskRequest{Project: project, Zone: zone, DiskResource: &computepb.Disk{
		Name: s("data"), SizeGb: proto.Int64(20), Labels: map[string]string{"app": "db"},
	}})
	e.done(t, op, err)

	for _, tc := range []struct {
		name   string
		size   int64
		status int
	}{{"same", 20, http.StatusBadRequest}, {"shrink", 10, http.StatusBadRequest}, {"grow", 40, http.StatusOK}} {
		op, err = disks.Resize(t.Context(), &computepb.ResizeDiskRequest{
			Project: project, Zone: zone, Disk: "data", DisksResizeRequestResource: &computepb.DisksResizeRequest{SizeGb: proto.Int64(tc.size)},
		})
		if tc.status == http.StatusOK {
			e.done(t, op, err)

			continue
		}

		assertAPIError(t, err, tc.status, "invalid")
	}

	snaps := e.snapshots(t)
	op, err = disks.CreateSnapshot(t.Context(), &computepb.CreateSnapshotDiskRequest{
		Project: project, Zone: zone, Disk: "data", SnapshotResource: &computepb.Snapshot{Name: s("snap1"), Labels: map[string]string{"k": "v"}},
	})
	e.done(t, op, err)
	assert.Contains(t, op.Proto().GetTargetLink(), "/zones/"+zone+"/disks/data")

	snap, err := snaps.Get(t.Context(), &computepb.GetSnapshotRequest{Project: project, Snapshot: "snap1"})
	require.NoError(t, err)
	assert.Equal(t, int64(40), snap.GetDiskSizeGb())
	assert.Equal(t, "READY", snap.GetStatus())
	assert.Equal(t, []string{"us"}, snap.GetStorageLocations())
	assert.Contains(t, snap.GetSourceDisk(), "/zones/"+zone+"/disks/data")

	_, err = disks.CreateSnapshot(t.Context(), &computepb.CreateSnapshotDiskRequest{
		Project: project, Zone: zone, Disk: "data", SnapshotResource: &computepb.Snapshot{Name: s("snap1")},
	})
	assertAPIError(t, err, http.StatusConflict, "alreadyExists")

	op, err = disks.Insert(t.Context(), &computepb.InsertDiskRequest{Project: project, Zone: zone, DiskResource: &computepb.Disk{
		Name: s("restored"), SourceSnapshot: s("global/snapshots/snap1"),
	}})
	e.done(t, op, err)

	rest, err := disks.Get(t.Context(), &computepb.GetDiskRequest{Project: project, Zone: zone, Disk: "restored"})
	require.NoError(t, err)
	assert.Equal(t, int64(40), rest.GetSizeGb())
	assert.Contains(t, rest.GetSourceSnapshot(), "/global/snapshots/snap1")

	op, err = snaps.SetLabels(t.Context(), &computepb.SetLabelsSnapshotRequest{
		Project: project, Resource: "snap1",
		GlobalSetLabelsRequestResource: &computepb.GlobalSetLabelsRequest{Labels: map[string]string{"k": "v2"}, LabelFingerprint: snap.LabelFingerprint},
	})
	e.done(t, op, err)

	snap, err = snaps.Get(t.Context(), &computepb.GetSnapshotRequest{Project: project, Snapshot: "snap1"})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"k": "v2"}, snap.GetLabels())

	it := snaps.List(t.Context(), &computepb.ListSnapshotsRequest{Project: project})
	got, err := it.Next()
	require.NoError(t, err)
	assert.Equal(t, "snap1", got.GetName())

	op, err = snaps.Delete(t.Context(), &computepb.DeleteSnapshotRequest{Project: project, Snapshot: "snap1"})
	e.done(t, op, err)

	_, err = snaps.Get(t.Context(), &computepb.GetSnapshotRequest{Project: project, Snapshot: "snap1"})
	assertAPIError(t, err, http.StatusNotFound, "notFound")

	op, err = disks.Delete(t.Context(), &computepb.DeleteDiskRequest{Project: project, Zone: zone, Disk: "data"})
	e.done(t, op, err)
}

func TestCustomImages(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	disks, imgs := e.disks(t), e.images(t)

	op, err := disks.Insert(t.Context(), &computepb.InsertDiskRequest{Project: project, Zone: zone, DiskResource: &computepb.Disk{
		Name: s("golden"), SourceImage: s("projects/ubuntu-os-cloud/global/images/family/ubuntu-2204-lts"), SizeGb: proto.Int64(30),
	}})
	e.done(t, op, err)

	op, err = imgs.Insert(t.Context(), &computepb.InsertImageRequest{Project: project, ImageResource: &computepb.Image{
		Name: s("golden-v1"), Family: s("golden"), SourceDisk: s("zones/" + zone + "/disks/golden"), Labels: map[string]string{"stage": "prod"},
	}})
	e.done(t, op, err)

	got, err := imgs.GetFromFamily(t.Context(), &computepb.GetFromFamilyImageRequest{Project: project, Family: "golden"})
	require.NoError(t, err)
	assert.Equal(t, "golden-v1", got.GetName())
	assert.Equal(t, int64(30), got.GetDiskSizeGb())
	assert.NotEmpty(t, got.GetLicenses())

	op, err = disks.Insert(t.Context(), &computepb.InsertDiskRequest{Project: project, Zone: zone, DiskResource: &computepb.Disk{
		Name: s("clone"), SourceImage: s("global/images/family/golden"),
	}})
	e.done(t, op, err)

	clone, err := disks.Get(t.Context(), &computepb.GetDiskRequest{Project: project, Zone: zone, Disk: "clone"})
	require.NoError(t, err)
	assert.Equal(t, int64(30), clone.GetSizeGb())

	for _, bad := range []*computepb.Image{
		{Name: s("nosrc")},
		{Name: s("twosrc"), SourceDisk: s("zones/" + zone + "/disks/golden"), SourceImage: s("global/images/golden-v1")},
		{Name: s("ghost"), SourceDisk: s("zones/" + zone + "/disks/ghost")},
		{Name: s("raw"), RawDisk: &computepb.RawDisk{Source: s("gs://b/o")}, SourceDisk: s("zones/" + zone + "/disks/golden")},
	} {
		_, err = imgs.Insert(t.Context(), &computepb.InsertImageRequest{Project: project, ImageResource: bad})
		require.Error(t, err, bad.GetName())
	}

	op, err = imgs.Delete(t.Context(), &computepb.DeleteImageRequest{Project: project, Image: "golden-v1"})
	e.done(t, op, err)

	_, err = imgs.Get(t.Context(), &computepb.GetImageRequest{Project: project, Image: "golden-v1"})
	assertAPIError(t, err, http.StatusNotFound, "notFound")
}
