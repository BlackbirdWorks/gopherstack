package datasync_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDataSync_FsxLustre covers the FSx Lustre location lifecycle.
func TestDataSync_FsxLustre(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create
	rec := doRequest(t, h, "CreateLocationFsxLustre", map[string]any{
		"FsxFilesystemArn":  "arn:aws:fsx:us-east-1:000000000000:file-system/fs-lustre01",
		"Subdirectory":      "/data",
		"SecurityGroupArns": []string{"arn:aws:ec2:us-east-1:000000000000:security-group/sg-abc"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	locArn := createResp["LocationArn"].(string)

	// Describe
	rec = doRequest(t, h, "DescribeLocationFsxLustre", map[string]any{"LocationArn": locArn})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	// "fsxl://", not the bare "lustre://" real AWS's LocationUri pattern
	// (`^(efs|nfs|s3|smb|hdfs|fsx[a-z0-9-]+)://...$`) rules out -- see
	// fsxLustreURIScheme.
	assert.Contains(t, descResp["LocationUri"].(string), "fsxl://")
	assert.Len(t, descResp["SecurityGroupArns"], 1)
	// The real DescribeLocationFsxLustreOutput has no FsxFilesystemArn field.
	assert.Nil(t, descResp["FsxFilesystemArn"])

	// Update
	rec = doRequest(t, h, "UpdateLocationFsxLustre", map[string]any{
		"LocationArn":  locArn,
		"Subdirectory": "/updated",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Missing FsxFilesystemArn
	rec = doRequest(t, h, "CreateLocationFsxLustre", map[string]any{"Subdirectory": "/x"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// Not found
	rec = doRequest(t, h, "DescribeLocationFsxLustre", map[string]any{
		"LocationArn": "arn:aws:datasync:us-east-1:000000000000:location/notexist",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
