package fsx_test

import (
	"encoding/json"
	"maps"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/fsx"
)

func responseID(t *testing.T, body []byte, outKey, idKey string) string {
	t.Helper()

	var out map[string]map[string]any

	require.NoError(t, json.Unmarshal(body, &out))

	id, ok := out[outKey][idKey].(string)
	require.True(t, ok, string(body))

	return id
}

func TestCreateOps_ClientRequestToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		build   func(t *testing.T, h *fsx.Handler) map[string]any
		changed map[string]any
		name    string
		op      string
		outKey  string
		idKey   string
		noDupe  bool
	}{
		{
			name: "snapshot", op: "CreateSnapshot", outKey: "Snapshot", idKey: "SnapshotId",
			build: func(t *testing.T, h *fsx.Handler) map[string]any {
				t.Helper()

				return map[string]any{"VolumeId": createVolume(t, h, "", "OPENZFS", "v"), "Name": "snap"}
			},
			changed: map[string]any{"Name": "other"},
		},
		{
			name: "backup", op: "CreateBackup", outKey: "Backup", idKey: "BackupId",
			build: func(t *testing.T, h *fsx.Handler) map[string]any {
				t.Helper()

				return map[string]any{"FileSystemId": createFS(t, h, "LUSTRE")}
			},
			changed: map[string]any{"Tags": []map[string]string{{"Key": "k", "Value": "v"}}},
		},
		{
			name: "copy backup", op: "CopyBackup", outKey: "Backup", idKey: "BackupId",
			build: func(t *testing.T, h *fsx.Handler) map[string]any {
				t.Helper()

				return map[string]any{"SourceBackupId": createFSandBackup(t, h, "LUSTRE")}
			},
			changed: map[string]any{"CopyTags": true},
		},
		{
			name: "data repository association", op: "CreateDataRepositoryAssociation",
			outKey: "Association", idKey: "AssociationId",
			build: func(t *testing.T, h *fsx.Handler) map[string]any {
				t.Helper()

				return map[string]any{
					"FileSystemId": createFS(t, h, "LUSTRE"), "FileSystemPath": "/a", "DataRepositoryPath": "s3://b",
				}
			},
			changed: map[string]any{"FileSystemPath": "/other"},
		},
		{
			name: "data repository task", op: "CreateDataRepositoryTask",
			outKey: "DataRepositoryTask", idKey: "TaskId", noDupe: true,
			build: func(t *testing.T, h *fsx.Handler) map[string]any {
				t.Helper()

				return map[string]any{
					"FileSystemId": createFS(t, h, "LUSTRE"), "Type": "EXPORT_TO_REPOSITORY",
					"Report": map[string]any{"Enabled": false},
				}
			},
			changed: map[string]any{"Type": "IMPORT_METADATA_FROM_REPOSITORY"},
		},
		{
			name: "file cache", op: "CreateFileCache", outKey: "FileCache", idKey: "FileCacheId",
			build: func(t *testing.T, _ *fsx.Handler) map[string]any {
				t.Helper()

				return map[string]any{
					"FileCacheType": "LUSTRE", "FileCacheTypeVersion": "2.12", "SubnetIds": []string{"subnet-1"},
					"StorageCapacity": 1200, "LustreConfiguration": fileCacheLustreConfigBody(),
				}
			},
			changed: map[string]any{"StorageCapacity": 2400},
		},
		{
			name: "storage virtual machine", op: "CreateStorageVirtualMachine",
			outKey: "StorageVirtualMachine", idKey: "StorageVirtualMachineId",
			build: func(t *testing.T, h *fsx.Handler) map[string]any {
				t.Helper()

				return map[string]any{"FileSystemId": createFS(t, h, "ONTAP"), "Name": "svm"}
			},
			changed: map[string]any{"Name": "other"},
		},
		{
			name: "volume", op: "CreateVolume", outKey: "Volume", idKey: "VolumeId",
			build: func(t *testing.T, h *fsx.Handler) map[string]any {
				t.Helper()

				fsID := createFS(t, h, "OPENZFS")

				return map[string]any{
					"VolumeType": "OPENZFS", "Name": "vol",
					"OpenZFSConfiguration": map[string]any{"ParentVolumeId": openZFSRootVolumeID(t, h, fsID)},
				}
			},
			changed: map[string]any{"Name": "other"},
		},
		{
			name: "s3 access point", op: "CreateAndAttachS3AccessPoint",
			outKey: "S3AccessPointAttachment", idKey: "Name", noDupe: true,
			build: func(t *testing.T, h *fsx.Handler) map[string]any {
				t.Helper()

				return map[string]any{
					"Name": "ap", "Type": "ONTAP",
					"OntapConfiguration": map[string]any{"VolumeId": createVolume(t, h, "", "ONTAP", "v")},
				}
			},
			changed: map[string]any{"Name": "other"},
		},
		{
			name: "file system from backup", op: "CreateFileSystemFromBackup",
			outKey: "FileSystem", idKey: "FileSystemId",
			build: func(t *testing.T, h *fsx.Handler) map[string]any {
				t.Helper()

				return map[string]any{
					"BackupId": createFSandBackup(t, h, "LUSTRE"), "SubnetIds": []string{"subnet-0123456789abcdef0"},
				}
			},
			changed: map[string]any{"KmsKeyId": "other"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := tt.build(t, h)
			body["ClientRequestToken"] = "tok-1"

			first := doFSxRequest(t, h, tt.op, body)
			require.Equal(t, http.StatusOK, first.Code, first.Body.String())

			second := doFSxRequest(t, h, tt.op, body)
			require.Equal(t, http.StatusOK, second.Code, second.Body.String())
			assert.Equal(t,
				responseID(t, first.Body.Bytes(), tt.outKey, tt.idKey),
				responseID(t, second.Body.Bytes(), tt.outKey, tt.idKey), "same token replays")

			mismatch := maps.Clone(body)
			maps.Copy(mismatch, tt.changed)

			bad := doFSxRequest(t, h, tt.op, mismatch)
			require.Equal(t, http.StatusBadRequest, bad.Code, bad.Body.String())
			assert.Contains(t, bad.Body.String(), "IncompatibleParameterError")

			if tt.noDupe {
				return
			}

			delete(body, "ClientRequestToken")

			third := doFSxRequest(t, h, tt.op, body)
			require.Equal(t, http.StatusOK, third.Code, third.Body.String())
			assert.NotEqual(t,
				responseID(t, first.Body.Bytes(), tt.outKey, tt.idKey),
				responseID(t, third.Body.Bytes(), tt.outKey, tt.idKey), "no token creates anew")
		})
	}
}

func TestNonCreatingOps_ClientRequestToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(t *testing.T, h *fsx.Handler) map[string]any
		changed map[string]any
		name    string
		op      string
	}{
		{
			name: "associate aliases", op: "AssociateFileSystemAliases",
			setup: func(t *testing.T, h *fsx.Handler) map[string]any {
				t.Helper()

				return map[string]any{"FileSystemId": createFS(t, h, "WINDOWS"), "Aliases": []string{"a.example.com"}}
			},
			changed: map[string]any{"Aliases": []string{"b.example.com"}},
		},
		{
			name: "update volume", op: "UpdateVolume",
			setup: func(t *testing.T, h *fsx.Handler) map[string]any {
				t.Helper()

				return map[string]any{"VolumeId": createVolume(t, h, "", "OPENZFS", "v"), "Name": "renamed"}
			},
			changed: map[string]any{"Name": "other"},
		},
		{
			name: "restore volume from snapshot", op: "RestoreVolumeFromSnapshot",
			setup: func(t *testing.T, h *fsx.Handler) map[string]any {
				t.Helper()

				volID := createVolume(t, h, "", "OPENZFS", "v")
				snap := doFSxRequest(t, h, "CreateSnapshot", map[string]any{"VolumeId": volID, "Name": "s"})

				return map[string]any{
					"VolumeId": volID, "SnapshotId": responseID(t, snap.Body.Bytes(), "Snapshot", "SnapshotId"),
				}
			},
			changed: map[string]any{"Options": []string{"DELETE_CLONED_VOLUMES"}},
		},
		{
			name: "update file cache", op: "UpdateFileCache",
			setup: func(t *testing.T, h *fsx.Handler) map[string]any {
				t.Helper()

				return map[string]any{
					"FileCacheId":         createFileCache(t, h, "LUSTRE"),
					"LustreConfiguration": map[string]any{"WeeklyMaintenanceStartTime": "1:01:00"},
				}
			},
			changed: map[string]any{"LustreConfiguration": map[string]any{"WeeklyMaintenanceStartTime": "2:01:00"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := tt.setup(t, h)
			body["ClientRequestToken"] = "tok-1"

			require.Equal(t, http.StatusOK, doFSxRequest(t, h, tt.op, body).Code)
			require.Equal(t, http.StatusOK, doFSxRequest(t, h, tt.op, body).Code, "same token replays")

			mismatch := maps.Clone(body)
			maps.Copy(mismatch, tt.changed)

			bad := doFSxRequest(t, h, tt.op, mismatch)
			require.Equal(t, http.StatusBadRequest, bad.Code, bad.Body.String())
			assert.Contains(t, bad.Body.String(), "IncompatibleParameterError")
		})
	}
}
