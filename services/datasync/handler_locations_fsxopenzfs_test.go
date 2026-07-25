package datasync_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDataSync_FsxOpenZfs covers the FSx OpenZFS location lifecycle.
func TestDataSync_FsxOpenZfs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create
	rec := doRequest(t, h, "CreateLocationFsxOpenZfs", map[string]any{
		"FsxFilesystemArn":  "arn:aws:fsx:us-east-1:000000000000:file-system/fs-openzfs01",
		"Subdirectory":      "/data",
		"SecurityGroupArns": []string{"arn:aws:ec2:us-east-1:000000000000:security-group/sg-xyz"},
		"Protocol": map[string]any{
			"NFS": map[string]any{
				"MountOptions": map[string]any{"Version": "AUTOMATIC"},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	locArn := createResp["LocationArn"].(string)

	// Describe
	rec = doRequest(t, h, "DescribeLocationFsxOpenZfs", map[string]any{"LocationArn": locArn})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Contains(t, descResp["LocationUri"].(string), "fsxz://")
	// The real DescribeLocationFsxOpenZfsOutput has no FsxFilesystemArn field.
	assert.Nil(t, descResp["FsxFilesystemArn"])

	// Update
	rec = doRequest(t, h, "UpdateLocationFsxOpenZfs", map[string]any{
		"LocationArn":  locArn,
		"Subdirectory": "/updated",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Missing FsxFilesystemArn
	rec = doRequest(t, h, "CreateLocationFsxOpenZfs", map[string]any{"Subdirectory": "/x"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// Not found
	rec = doRequest(t, h, "DescribeLocationFsxOpenZfs", map[string]any{
		"LocationArn": "arn:aws:datasync:us-east-1:000000000000:location/notexist",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
