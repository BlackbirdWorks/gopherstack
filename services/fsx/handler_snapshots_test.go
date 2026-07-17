package fsx_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/fsx"
)

func TestFSx_Snapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
		wantErr  bool
	}{
		{
			name:     "create snapshot returns SnapshotId",
			wantCode: http.StatusOK,
		},
		{
			name:     "missing VolumeId returns 400",
			wantCode: http.StatusBadRequest,
			wantErr:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			var body map[string]any
			if !tc.wantErr {
				fsID := createFS(t, h, "ONTAP")
				volID := createVolume(t, h, fsID, "ONTAP", "vol1")
				body = map[string]any{"VolumeId": volID, "Name": "snap1"}
			} else {
				body = map[string]any{"Name": "snap1"}
			}

			rec := doFSxRequest(t, h, "CreateSnapshot", body)
			require.Equal(t, tc.wantCode, rec.Code)

			if !tc.wantErr {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				s := out["Snapshot"].(map[string]any)
				assert.Contains(t, s["SnapshotId"].(string), "fsvolsnap-")
				assert.Equal(t, "AVAILABLE", s["Lifecycle"])
				assert.Equal(t, "snap1", s["Name"])
			}
		})
	}
}

func TestFSx_SnapshotLifecycle(t *testing.T) {
	t.Parallel()

	t.Run("describe/update/delete cycle", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		b := fsx.GetBackend(h)
		fsID := createFS(t, h, "ONTAP")
		volID := createVolume(t, h, fsID, "ONTAP", "vol1")

		rec := doFSxRequest(t, h, "CreateSnapshot", map[string]any{
			"VolumeId": volID,
			"Name":     "snap1",
		})
		require.Equal(t, http.StatusOK, rec.Code)
		var cr map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cr))
		snapID := cr["Snapshot"].(map[string]any)["SnapshotId"].(string)

		assert.Equal(t, 1, fsx.SnapshotCount(b))

		// describe
		rec2 := doFSxRequest(t, h, "DescribeSnapshots", map[string]any{
			"SnapshotIds": []string{snapID},
		})
		require.Equal(t, http.StatusOK, rec2.Code)
		var dr map[string]any
		require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &dr))
		assert.Len(t, dr["Snapshots"].([]any), 1)

		// update
		rec3 := doFSxRequest(t, h, "UpdateSnapshot", map[string]any{
			"SnapshotId": snapID,
			"Name":       "snap-renamed",
		})
		require.Equal(t, http.StatusOK, rec3.Code)
		var ur map[string]any
		require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &ur))
		assert.Equal(t, "snap-renamed", ur["Snapshot"].(map[string]any)["Name"])

		// delete
		rec4 := doFSxRequest(t, h, "DeleteSnapshot", map[string]any{"SnapshotId": snapID})
		require.Equal(t, http.StatusOK, rec4.Code)
		var del map[string]any
		require.NoError(t, json.Unmarshal(rec4.Body.Bytes(), &del))
		assert.Equal(t, snapID, del["SnapshotId"])
		assert.Equal(t, "DELETING", del["Lifecycle"])
		assert.Equal(t, 0, fsx.SnapshotCount(b))
	})
}

func TestFSx_CopySnapshotAndUpdateVolume(t *testing.T) {
	t.Parallel()

	t.Run("copy snapshot and update volume", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		fsID := createFS(t, h, "ONTAP")
		volID := createVolume(t, h, fsID, "ONTAP", "vol1")

		rec := doFSxRequest(t, h, "CreateSnapshot", map[string]any{
			"VolumeId": volID,
			"Name":     "snap1",
		})
		require.Equal(t, http.StatusOK, rec.Code)
		var cr map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cr))
		snapARN := cr["Snapshot"].(map[string]any)["ResourceARN"].(string)

		rec2 := doFSxRequest(t, h, "CopySnapshotAndUpdateVolume", map[string]any{
			"VolumeId":          volID,
			"SourceSnapshotARN": snapARN,
		})
		require.Equal(t, http.StatusOK, rec2.Code)
		var out map[string]any
		require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out))
		assert.Equal(t, volID, out["Volume"].(map[string]any)["VolumeId"])
	})
}
