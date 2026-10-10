package fsx_test

import (
	"encoding/json"
	"maps"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/fsx"
)

type zfsFixture struct {
	h      *fsx.Handler
	fsID   string
	rootID string
}

func newZFSFixture(t *testing.T) zfsFixture {
	t.Helper()

	h := newTestHandler(t)
	fsID := createFS(t, h, "OPENZFS")

	return zfsFixture{h: h, fsID: fsID, rootID: openZFSRootVolumeID(t, h, fsID)}
}

func (f zfsFixture) createVolume(t *testing.T, name string, cfg map[string]any) *httptest.ResponseRecorder {
	t.Helper()

	zfs := map[string]any{"ParentVolumeId": f.rootID}
	maps.Copy(zfs, cfg)

	return doFSxRequest(t, f.h, "CreateVolume", map[string]any{
		"VolumeType": "OPENZFS", "Name": name, "OpenZFSConfiguration": zfs,
	})
}

func (f zfsFixture) mustCreateVolume(t *testing.T, name string, cfg map[string]any) string {
	t.Helper()

	rec := f.createVolume(t, name, cfg)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	return responseID(t, rec.Body.Bytes(), "Volume", "VolumeId")
}

func (f zfsFixture) snapshot(t *testing.T, volID, name string) (string, string) {
	t.Helper()

	rec := doFSxRequest(t, f.h, "CreateSnapshot", map[string]any{"VolumeId": volID, "Name": name})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	return responseID(t, rec.Body.Bytes(), "Snapshot", "SnapshotId"),
		responseID(t, rec.Body.Bytes(), "Snapshot", "ResourceARN")
}

func (f zfsFixture) describe(t *testing.T, volID string) map[string]any {
	t.Helper()

	rec := doFSxRequest(t, f.h, "DescribeVolumes", map[string]any{"VolumeIds": []string{volID}})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var out struct {
		Volumes []struct {
			OpenZFSConfiguration map[string]any `json:"OpenZFSConfiguration"`
		} `json:"Volumes"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.Volumes, 1)

	return out.Volumes[0].OpenZFSConfiguration
}

func TestOpenZFSVolume_ConfigurationRoundTrip(t *testing.T) {
	t.Parallel()

	f := newZFSFixture(t)
	exports := []any{map[string]any{"ClientConfigurations": []any{
		map[string]any{"Clients": "10.0.0.0/16", "Options": []any{"rw", "crossmnt"}},
	}}}
	quotas := []any{map[string]any{"Id": float64(1001), "StorageCapacityQuotaGiB": float64(5), "Type": "USER"}}

	id := f.mustCreateVolume(t, "cfg", map[string]any{
		"CopyTagsToSnapshots": true, "DataCompressionType": "ZSTD", "ReadOnly": true,
		"RecordSizeKiB": 64, "StorageCapacityQuotaGiB": 100, "StorageCapacityReservationGiB": 10,
		"NfsExports": exports, "UserAndGroupQuotas": quotas,
	})

	cfg := f.describe(t, id)
	assert.Equal(t, f.rootID, cfg["ParentVolumeId"])
	assert.Equal(t, "ZSTD", cfg["DataCompressionType"])
	assert.Equal(t, true, cfg["ReadOnly"])
	assert.Equal(t, true, cfg["CopyTagsToSnapshots"])
	assert.InDelta(t, 64, cfg["RecordSizeKiB"], 0)
	assert.InDelta(t, 100, cfg["StorageCapacityQuotaGiB"], 0)
	assert.InDelta(t, 10, cfg["StorageCapacityReservationGiB"], 0)
	assert.Equal(t, exports, cfg["NfsExports"])
	assert.Equal(t, quotas, cfg["UserAndGroupQuotas"])
}

func TestOpenZFSVolume_CreateValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		cfg  map[string]any
		name string
	}{
		{name: "compression", cfg: map[string]any{"DataCompressionType": "GZIP"}},
		{name: "record size", cfg: map[string]any{"RecordSizeKiB": 3}},
		{name: "record size too large", cfg: map[string]any{"RecordSizeKiB": 2048}},
		{
			name: "empty export",
			cfg:  map[string]any{"NfsExports": []any{map[string]any{"ClientConfigurations": []any{}}}},
		},
		{name: "quota type", cfg: map[string]any{"UserAndGroupQuotas": []any{
			map[string]any{"Id": 1, "StorageCapacityQuotaGiB": 1, "Type": "ROLE"},
		}}},
		{name: "origin strategy", cfg: map[string]any{"OriginSnapshot": map[string]any{
			"CopyStrategy": "INCREMENTAL_COPY", "SnapshotARN": "arn:aws:fsx:us-east-1:000000000000:snapshot/x",
		}}},
		{name: "origin missing snapshot", cfg: map[string]any{"OriginSnapshot": map[string]any{
			"CopyStrategy": "CLONE", "SnapshotARN": "arn:aws:fsx:us-east-1:000000000000:snapshot/x",
		}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := newZFSFixture(t).createVolume(t, "v", tt.cfg)
			assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
		})
	}
}

func TestOpenZFSVolume_UpdateConfiguration(t *testing.T) {
	t.Parallel()

	f := newZFSFixture(t)
	id := f.mustCreateVolume(t, "v", map[string]any{"StorageCapacityQuotaGiB": 50})

	rec := doFSxRequest(t, f.h, "UpdateVolume", map[string]any{
		"VolumeId": id,
		"OpenZFSConfiguration": map[string]any{
			"DataCompressionType": "LZ4", "RecordSizeKiB": 16, "StorageCapacityQuotaGiB": -1,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	cfg := f.describe(t, id)
	assert.Equal(t, "LZ4", cfg["DataCompressionType"])
	assert.InDelta(t, 16, cfg["RecordSizeKiB"], 0)
	assert.NotContains(t, cfg, "StorageCapacityQuotaGiB")

	bad := doFSxRequest(t, f.h, "UpdateVolume", map[string]any{
		"VolumeId": id, "OpenZFSConfiguration": map[string]any{"RecordSizeKiB": 5},
	})
	assert.Equal(t, http.StatusBadRequest, bad.Code)
}

func TestOpenZFSVolume_CopyTagsToSnapshots(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		copy bool
		want int
	}{
		{name: "copies", copy: true, want: 1},
		{name: "off", copy: false, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newZFSFixture(t)
			rec := doFSxRequest(t, f.h, "CreateVolume", map[string]any{
				"VolumeType": "OPENZFS", "Name": "v",
				"Tags": []map[string]string{{"Key": "env", "Value": "prod"}},
				"OpenZFSConfiguration": map[string]any{
					"ParentVolumeId": f.rootID, "CopyTagsToSnapshots": tt.copy,
				},
			})
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			snap := doFSxRequest(t, f.h, "CreateSnapshot", map[string]any{
				"VolumeId": responseID(t, rec.Body.Bytes(), "Volume", "VolumeId"), "Name": "s",
			})
			require.Equal(t, http.StatusOK, snap.Code, snap.Body.String())

			var out struct {
				Snapshot struct {
					Tags []any `json:"Tags"`
				} `json:"Snapshot"`
			}

			require.NoError(t, json.Unmarshal(snap.Body.Bytes(), &out))
			assert.Len(t, out.Snapshot.Tags, tt.want)
		})
	}
}

func TestOpenZFSVolume_DeleteDependents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		options    []string
		wantStatus int
		wantChild  bool
	}{
		{name: "blocked without option", wantStatus: http.StatusBadRequest, wantChild: true},
		{
			name:       "option deletes child",
			options:    []string{"DELETE_CHILD_VOLUMES_AND_SNAPSHOTS"},
			wantStatus: http.StatusOK,
		},
		{name: "unknown option", options: []string{"BOGUS"}, wantStatus: http.StatusBadRequest, wantChild: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newZFSFixture(t)
			parent := f.mustCreateVolume(t, "parent", nil)

			child := doFSxRequest(t, f.h, "CreateVolume", map[string]any{
				"VolumeType": "OPENZFS", "Name": "child",
				"OpenZFSConfiguration": map[string]any{"ParentVolumeId": parent},
			})
			require.Equal(t, http.StatusOK, child.Code, child.Body.String())

			childID := responseID(t, child.Body.Bytes(), "Volume", "VolumeId")
			body := map[string]any{"VolumeId": parent}

			if tt.options != nil {
				body["OpenZFSConfiguration"] = map[string]any{"Options": tt.options}
			}

			rec := doFSxRequest(t, f.h, "DeleteVolume", body)
			require.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())

			got := doFSxRequest(t, f.h, "DescribeVolumes", map[string]any{"VolumeIds": []string{childID}})
			assert.Equal(t, tt.wantChild, got.Code == http.StatusOK)
		})
	}
}

func TestOpenZFSVolume_CloneOriginSnapshot(t *testing.T) {
	t.Parallel()

	f := newZFSFixture(t)
	src := f.mustCreateVolume(t, "src", nil)
	snapID, snapARN := f.snapshot(t, src, "origin")

	clone := f.mustCreateVolume(t, "clone", map[string]any{
		"OriginSnapshot": map[string]any{"CopyStrategy": "CLONE", "SnapshotARN": snapARN},
	})

	cfg := f.describe(t, clone)
	assert.Equal(t, map[string]any{"CopyStrategy": "CLONE", "SnapshotARN": snapARN}, cfg["OriginSnapshot"])

	del := doFSxRequest(t, f.h, "DeleteSnapshot", map[string]any{"SnapshotId": snapID})
	assert.Equal(t, http.StatusBadRequest, del.Code, "origin snapshot of a clone cannot be deleted")

	rec := doFSxRequest(t, f.h, "DeleteVolume", map[string]any{"VolumeId": src})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "source with a clone needs the delete option")

	rec = doFSxRequest(t, f.h, "DeleteVolume", map[string]any{
		"VolumeId":             src,
		"OpenZFSConfiguration": map[string]any{"Options": []string{"DELETE_CHILD_VOLUMES_AND_SNAPSHOTS"}},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	gone := doFSxRequest(t, f.h, "DescribeVolumes", map[string]any{"VolumeIds": []string{clone}})
	assert.Equal(t, http.StatusBadRequest, gone.Code)
}

func TestOpenZFSVolume_RestoreOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		options    []string
		withClone  bool
		wantStatus int
	}{
		{name: "intermediate blocks", wantStatus: http.StatusBadRequest},
		{name: "intermediate deleted", options: []string{"DELETE_INTERMEDIATE_SNAPSHOTS"}, wantStatus: http.StatusOK},
		{
			name:       "clone blocks",
			options:    []string{"DELETE_INTERMEDIATE_SNAPSHOTS"},
			withClone:  true,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "clone deleted",
			options:    []string{"DELETE_INTERMEDIATE_SNAPSHOTS", "DELETE_CLONED_VOLUMES"},
			withClone:  true,
			wantStatus: http.StatusOK,
		},
		{name: "bad option", options: []string{"NOPE"}, wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newZFSFixture(t)
			vol := f.mustCreateVolume(t, "v", nil)
			first, _ := f.snapshot(t, vol, "first")
			_, secondARN := f.snapshot(t, vol, "second")

			if tt.withClone {
				f.mustCreateVolume(t, "clone", map[string]any{
					"OriginSnapshot": map[string]any{"CopyStrategy": "CLONE", "SnapshotARN": secondARN},
				})
			}

			body := map[string]any{"VolumeId": vol, "SnapshotId": first}
			if tt.options != nil {
				body["Options"] = tt.options
			}

			rec := doFSxRequest(t, f.h, "RestoreVolumeFromSnapshot", body)
			assert.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())

			if tt.wantStatus == http.StatusOK {
				list := doFSxRequest(t, f.h, "DescribeSnapshots", map[string]any{
					"Filters": []map[string]any{{"Name": "volume-id", "Values": []string{vol}}},
				})
				require.Equal(t, http.StatusOK, list.Code)
				assert.NotContains(t, list.Body.String(), "second")
			}
		})
	}
}

func TestCopySnapshotAndUpdateVolume_StrategyAndOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		strategy   string
		options    []string
		wantStatus int
	}{
		{name: "full copy", strategy: "FULL_COPY", wantStatus: http.StatusOK},
		{
			name:       "incremental with options",
			strategy:   "INCREMENTAL_COPY",
			options:    []string{"DELETE_INTERMEDIATE_DATA"},
			wantStatus: http.StatusOK,
		},
		{name: "clone rejected", strategy: "CLONE", wantStatus: http.StatusBadRequest},
		{name: "bad option", strategy: "FULL_COPY", options: []string{"X"}, wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newZFSFixture(t)
			vol := f.mustCreateVolume(t, "v", nil)
			_, arn := f.snapshot(t, vol, "s")

			body := map[string]any{"VolumeId": vol, "SourceSnapshotARN": arn, "CopyStrategy": tt.strategy}
			if tt.options != nil {
				body["Options"] = tt.options
			}

			rec := doFSxRequest(t, f.h, "CopySnapshotAndUpdateVolume", body)
			assert.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())
		})
	}
}
