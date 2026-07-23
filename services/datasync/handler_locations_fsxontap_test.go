package datasync_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDataSync_FsxOntap covers the FSx ONTAP location lifecycle.
func TestDataSync_FsxOntap(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create with NFS protocol
	rec := doRequest(t, h, "CreateLocationFsxOntap", map[string]any{
		"StorageVirtualMachineArn": "arn:aws:fsx:us-east-1:000000000000:storage-virtual-machine/fs-01/svm-01",
		"Subdirectory":             "/share",
		"SecurityGroupArns":        []string{"arn:aws:ec2:us-east-1:000000000000:security-group/sg-abc"},
		"Protocol": map[string]any{
			"NFS": map[string]any{
				"MountOptions": map[string]any{"Version": "NFS3"},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	locArn := createResp["LocationArn"].(string)

	// Describe
	rec = doRequest(t, h, "DescribeLocationFsxOntap", map[string]any{"LocationArn": locArn})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	// NFS protocol -> "nfs://" scheme (ONTAP has no distinct "ontap://" or
	// "fsxn://" scheme; DataSync reuses the underlying access protocol's own
	// scheme, matching how FSx Windows reuses "smb://" -- see
	// fsxOntapURIScheme).
	assert.Contains(t, descResp["LocationUri"].(string), "nfs://")
	assert.NotNil(t, descResp["Protocol"])
	assert.Equal(t, "arn:aws:fsx:us-east-1:000000000000:file-system/fs-01", descResp["FsxFilesystemArn"])

	// Update to SMB protocol flips the LocationUri scheme to "smb://".
	rec = doRequest(t, h, "UpdateLocationFsxOntap", map[string]any{
		"LocationArn":  locArn,
		"Subdirectory": "/updated",
		"Protocol": map[string]any{
			"SMB": map[string]any{"User": "svcuser"},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "DescribeLocationFsxOntap", map[string]any{"LocationArn": locArn})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Contains(t, descResp["LocationUri"].(string), "smb://")

	// Missing StorageVirtualMachineArn
	rec = doRequest(t, h, "CreateLocationFsxOntap", map[string]any{"Subdirectory": "/x"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// Not found
	rec = doRequest(t, h, "DescribeLocationFsxOntap", map[string]any{
		"LocationArn": "arn:aws:datasync:us-east-1:000000000000:location/notexist",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
