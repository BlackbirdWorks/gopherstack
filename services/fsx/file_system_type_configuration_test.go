package fsx_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFSx_FileSystem_WindowsConfiguration locks the wire shape of the
// WindowsConfiguration block that real AWS always returns for a WINDOWS
// file system. Before this test was added, CreateFileSystem never populated
// WindowsConfiguration at all (only LustreConfiguration was modeled), which
// would break any real aws-sdk-go-v2 client / terraform-provider-aws Read
// path expecting this block on a WINDOWS file system.
func TestFSx_FileSystem_WindowsConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doFSxRequest(t, h, "CreateFileSystem", map[string]any{
		"FileSystemType": "WINDOWS",
		"WindowsConfiguration": map[string]any{
			"ActiveDirectoryId":  "d-1234567890",
			"DeploymentType":     "SINGLE_AZ_1",
			"ThroughputCapacity": 16,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	fs := out["FileSystem"].(map[string]any)

	require.Contains(t, fs, "WindowsConfiguration")
	winCfg := fs["WindowsConfiguration"].(map[string]any)
	assert.Equal(t, "d-1234567890", winCfg["ActiveDirectoryId"])
	assert.Equal(t, "SINGLE_AZ_1", winCfg["DeploymentType"])
	assert.InEpsilon(t, float64(16), winCfg["ThroughputCapacity"], 0)
	assert.NotContains(t, fs, "OntapConfiguration")
	assert.NotContains(t, fs, "OpenZFSConfiguration")
}

// TestFSx_FileSystem_OntapConfiguration locks the wire shape of the
// OntapConfiguration block (including its nested Endpoints), which real AWS
// always returns for an ONTAP file system.
func TestFSx_FileSystem_OntapConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doFSxRequest(t, h, "CreateFileSystem", map[string]any{
		"FileSystemType": "ONTAP",
		"OntapConfiguration": map[string]any{
			"DeploymentType":     "MULTI_AZ_1",
			"ThroughputCapacity": 128,
			"PreferredSubnetId":  "subnet-abc123",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	fs := out["FileSystem"].(map[string]any)

	require.Contains(t, fs, "OntapConfiguration")
	ontapCfg := fs["OntapConfiguration"].(map[string]any)
	assert.Equal(t, "MULTI_AZ_1", ontapCfg["DeploymentType"])
	assert.InEpsilon(t, float64(128), ontapCfg["ThroughputCapacity"], 0)
	assert.Equal(t, "subnet-abc123", ontapCfg["PreferredSubnetId"])
	assert.InEpsilon(t, float64(1), ontapCfg["HAPairs"], 0)

	require.Contains(t, ontapCfg, "Endpoints")
	endpoints := ontapCfg["Endpoints"].(map[string]any)
	require.Contains(t, endpoints, "Management")
	require.Contains(t, endpoints, "Intercluster")
}

// TestFSx_FileSystem_OpenZFSConfiguration locks the wire shape of the
// OpenZFSConfiguration block, including RootVolumeId, which real AWS
// auto-populates with the ID of a real, describable root volume.
func TestFSx_FileSystem_OpenZFSConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doFSxRequest(t, h, "CreateFileSystem", map[string]any{
		"FileSystemType": "OPENZFS",
		"OpenZFSConfiguration": map[string]any{
			"DeploymentType":     "SINGLE_AZ_1",
			"ThroughputCapacity": 64,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	fs := out["FileSystem"].(map[string]any)

	require.Contains(t, fs, "OpenZFSConfiguration")
	zfsCfg := fs["OpenZFSConfiguration"].(map[string]any)
	assert.Equal(t, "SINGLE_AZ_1", zfsCfg["DeploymentType"])
	assert.InEpsilon(t, float64(64), zfsCfg["ThroughputCapacity"], 0)

	rootVolumeID, ok := zfsCfg["RootVolumeId"].(string)
	require.True(t, ok, "RootVolumeId must be populated")
	assert.NotEmpty(t, rootVolumeID)

	// The root volume must be a real, describable Volume -- not a disguised
	// no-op ID that DescribeVolumes can't find.
	describeRec := doFSxRequest(t, h, "DescribeVolumes", map[string]any{"VolumeIds": []string{rootVolumeID}})
	require.Equal(t, http.StatusOK, describeRec.Code)

	var describeOut map[string]any
	require.NoError(t, json.Unmarshal(describeRec.Body.Bytes(), &describeOut))
	volumes := describeOut["Volumes"].([]any)
	require.Len(t, volumes, 1)
	assert.Equal(t, "fsx", volumes[0].(map[string]any)["Name"])
}

// TestFSx_CreateFileSystem_MissingConfiguration verifies that CreateFileSystem
// rejects WINDOWS/ONTAP/OPENZFS requests missing the required type-specific
// configuration block with MissingFileSystemConfiguration, matching real AWS
// FSx's exception of that name.
func TestFSx_CreateFileSystem_MissingConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		fsType string
	}{
		{name: "windows_without_config", fsType: "WINDOWS"},
		{name: "ontap_without_config", fsType: "ONTAP"},
		{name: "openzfs_without_config", fsType: "OPENZFS"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doFSxRequest(t, h, "CreateFileSystem", map[string]any{"FileSystemType": tc.fsType})
			require.Equal(t, http.StatusBadRequest, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Equal(t, "MissingFileSystemConfiguration", out["__type"])
		})
	}
}

// TestFSx_CreateFileSystem_RequiredConfigMembers verifies that CreateFileSystem
// rejects a present-but-incomplete type-specific configuration block (e.g. a
// WindowsConfiguration with no ThroughputCapacity) with BadRequest, matching
// real AWS FSx's required-member validation.
func TestFSx_CreateFileSystem_RequiredConfigMembers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		config map[string]any
		name   string
		fsType string
	}{
		{
			name:   "windows_missing_throughput_capacity",
			fsType: "WINDOWS",
			config: map[string]any{"DeploymentType": "SINGLE_AZ_1"},
		},
		{
			name:   "ontap_missing_deployment_type",
			fsType: "ONTAP",
			config: map[string]any{"ThroughputCapacity": 128},
		},
		{
			name:   "openzfs_missing_deployment_type",
			fsType: "OPENZFS",
			config: map[string]any{"ThroughputCapacity": 64},
		},
		{
			name:   "openzfs_missing_throughput_capacity",
			fsType: "OPENZFS",
			config: map[string]any{"DeploymentType": "SINGLE_AZ_1"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := map[string]any{"FileSystemType": tc.fsType}

			switch tc.fsType {
			case "WINDOWS":
				body["WindowsConfiguration"] = tc.config
			case "ONTAP":
				body["OntapConfiguration"] = tc.config
			case "OPENZFS":
				body["OpenZFSConfiguration"] = tc.config
			}

			rec := doFSxRequest(t, h, "CreateFileSystem", body)
			require.Equal(t, http.StatusBadRequest, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Equal(t, "BadRequest", out["__type"])
		})
	}
}

// TestFSx_UpdateFileSystem_TypeSpecificConfig verifies that UpdateFileSystem
// applies per-type configuration updates (e.g. WindowsConfiguration's
// ThroughputCapacity), matching real AWS's "only overwrites existing
// properties with non-null values provided in the request" semantics.
func TestFSx_UpdateFileSystem_TypeSpecificConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	id := createFS(t, h, "WINDOWS")

	rec := doFSxRequest(t, h, "UpdateFileSystem", map[string]any{
		"FileSystemId": id,
		"WindowsConfiguration": map[string]any{
			"ThroughputCapacity": 32,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	fs := out["FileSystem"].(map[string]any)
	winCfg := fs["WindowsConfiguration"].(map[string]any)
	assert.InEpsilon(t, float64(32), winCfg["ThroughputCapacity"], 0)
}

// TestFSx_CreateFileSystemFromBackup_CarriesTypeConfig verifies that a
// WINDOWS file system restored from a backup still returns a populated
// WindowsConfiguration block (ThroughputCapacity/DeploymentType carried over
// from the source file system) rather than an all-zero-valued one, matching
// real AWS's behavior of carrying these settings over from the backup's
// source file system.
func TestFSx_CreateFileSystemFromBackup_CarriesTypeConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	fsID := createFS(t, h, "WINDOWS")

	backupRec := doFSxRequest(t, h, "CreateBackup", map[string]any{"FileSystemId": fsID})
	require.Equal(t, http.StatusOK, backupRec.Code)

	var backupOut map[string]any
	require.NoError(t, json.Unmarshal(backupRec.Body.Bytes(), &backupOut))
	backupID := backupOut["Backup"].(map[string]any)["BackupId"].(string)

	restoreRec := doFSxRequest(t, h, "CreateFileSystemFromBackup", map[string]any{"BackupId": backupID})
	require.Equal(t, http.StatusOK, restoreRec.Code)

	var restoreOut map[string]any
	require.NoError(t, json.Unmarshal(restoreRec.Body.Bytes(), &restoreOut))
	restoredFS := restoreOut["FileSystem"].(map[string]any)

	require.Contains(t, restoredFS, "WindowsConfiguration")
	winCfg := restoredFS["WindowsConfiguration"].(map[string]any)
	assert.InEpsilon(t, float64(8), winCfg["ThroughputCapacity"], 0, "ThroughputCapacity must carry over")
	assert.NotEmpty(t, restoredFS["DNSName"], "restored file system must get its own DNSName")
}
