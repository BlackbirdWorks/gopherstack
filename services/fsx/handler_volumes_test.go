package fsx_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/fsx"
)

func TestFSx_Volume(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		volType  string
		volName  string
		wantCode int
		wantErr  bool
	}{
		{
			name:     "create ONTAP volume",
			volType:  "ONTAP",
			volName:  "vol1",
			wantCode: http.StatusOK,
		},
		{
			name:     "create OPENZFS volume",
			volType:  "OPENZFS",
			volName:  "vol2",
			wantCode: http.StatusOK,
		},
		{
			name:     "missing VolumeType returns 400",
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
				body = map[string]any{
					"VolumeType":   tc.volType,
					"FileSystemId": fsID,
					"Name":         tc.volName,
				}
			} else {
				body = map[string]any{"Name": "vol1"}
			}

			rec := doFSxRequest(t, h, "CreateVolume", body)
			require.Equal(t, tc.wantCode, rec.Code)

			if !tc.wantErr {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				v := out["Volume"].(map[string]any)
				assert.Contains(t, v["VolumeId"].(string), "fsvol-")
				assert.Equal(t, "AVAILABLE", v["Lifecycle"])
				assert.Equal(t, tc.volType, v["VolumeType"])
			}
		})
	}
}

func TestFSx_VolumeLifecycle(t *testing.T) {
	t.Parallel()

	t.Run("describe/update/delete cycle", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		b := fsx.GetBackend(h)
		fsID := createFS(t, h, "ONTAP")
		volID := createVolume(t, h, fsID, "ONTAP", "vol1")

		assert.Equal(t, 1, fsx.VolumeCount(b))

		// describe
		rec := doFSxRequest(t, h, "DescribeVolumes", map[string]any{
			"VolumeIds": []string{volID},
		})
		require.Equal(t, http.StatusOK, rec.Code)
		var dr map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &dr))
		assert.Len(t, dr["Volumes"].([]any), 1)

		// update
		rec2 := doFSxRequest(t, h, "UpdateVolume", map[string]any{
			"VolumeId": volID,
			"Name":     "vol-renamed",
		})
		require.Equal(t, http.StatusOK, rec2.Code)
		var ur map[string]any
		require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &ur))
		assert.Equal(t, "vol-renamed", ur["Volume"].(map[string]any)["Name"])

		// delete
		rec3 := doFSxRequest(t, h, "DeleteVolume", map[string]any{"VolumeId": volID})
		require.Equal(t, http.StatusOK, rec3.Code)
		var del map[string]any
		require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &del))
		assert.Equal(t, volID, del["VolumeId"])
		assert.Equal(t, "DELETING", del["Lifecycle"])
		assert.Equal(t, 0, fsx.VolumeCount(b))
	})
}

func TestFSx_CreateVolumeFromBackup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
		wantErr  bool
	}{
		{
			name:     "creates volume from backup",
			wantCode: http.StatusOK,
		},
		{
			name:     "unknown backup returns 400",
			wantCode: http.StatusBadRequest,
			wantErr:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			var backupID string
			if !tc.wantErr {
				backupID = createFSandBackup(t, h, "ONTAP")
			} else {
				backupID = "backup-does-not-exist"
			}

			rec := doFSxRequest(t, h, "CreateVolumeFromBackup", map[string]any{
				"BackupId": backupID,
				"Name":     "restored-vol",
			})
			require.Equal(t, tc.wantCode, rec.Code)

			if !tc.wantErr {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				v := out["Volume"].(map[string]any)
				assert.Contains(t, v["VolumeId"].(string), "fsvol-")
				assert.Equal(t, "restored-vol", v["Name"])
			}
		})
	}
}

func TestFSx_RestoreVolumeFromSnapshot(t *testing.T) {
	t.Parallel()

	t.Run("restore volume from snapshot", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		fsID := createFS(t, h, "ONTAP")
		volID := createVolume(t, h, fsID, "ONTAP", "vol1")

		// create snapshot
		rec := doFSxRequest(t, h, "CreateSnapshot", map[string]any{
			"VolumeId": volID,
			"Name":     "snap1",
		})
		require.Equal(t, http.StatusOK, rec.Code)
		var cr map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cr))
		snapID := cr["Snapshot"].(map[string]any)["SnapshotId"].(string)

		// restore
		rec2 := doFSxRequest(t, h, "RestoreVolumeFromSnapshot", map[string]any{
			"VolumeId":   volID,
			"SnapshotId": snapID,
		})
		require.Equal(t, http.StatusOK, rec2.Code)
		var out map[string]any
		require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out))
		assert.Equal(t, volID, out["Volume"].(map[string]any)["VolumeId"])
	})
}
