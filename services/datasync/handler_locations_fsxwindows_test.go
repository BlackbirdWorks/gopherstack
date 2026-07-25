package datasync_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDataSync_FsxWindows covers the FSx Windows location lifecycle.
func TestDataSync_FsxWindows(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create
	rec := doRequest(t, h, "CreateLocationFsxWindows", map[string]any{
		"FsxFilesystemArn":  "arn:aws:fsx:us-east-1:000000000000:file-system/fs-windows01",
		"Subdirectory":      "/share",
		"Domain":            "CORP",
		"User":              "svcuser",
		"Password":          "s3cr3t",
		"SecurityGroupArns": []string{"arn:aws:ec2:us-east-1:000000000000:security-group/sg-win"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	locArn := createResp["LocationArn"].(string)

	// Describe
	rec = doRequest(t, h, "DescribeLocationFsxWindows", map[string]any{"LocationArn": locArn})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Contains(t, descResp["LocationUri"].(string), "smb://")
	assert.Equal(t, "CORP", descResp["Domain"])
	assert.Equal(t, "svcuser", descResp["User"])
	// Password not returned in describe
	assert.Nil(t, descResp["Password"])
	// The real DescribeLocationFsxWindowsOutput has no FsxFilesystemArn field.
	assert.Nil(t, descResp["FsxFilesystemArn"])

	// Update
	rec = doRequest(t, h, "UpdateLocationFsxWindows", map[string]any{
		"LocationArn":  locArn,
		"Domain":       "NEWDOMAIN",
		"User":         "newuser",
		"Password":     "newpass",
		"Subdirectory": "/updated",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify update
	rec = doRequest(t, h, "DescribeLocationFsxWindows", map[string]any{"LocationArn": locArn})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "NEWDOMAIN", descResp["Domain"])

	// Missing FsxFilesystemArn
	rec = doRequest(t, h, "CreateLocationFsxWindows", map[string]any{
		"User": "u", "Password": "p",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// Not found
	rec = doRequest(t, h, "DescribeLocationFsxWindows", map[string]any{
		"LocationArn": "arn:aws:datasync:us-east-1:000000000000:location/notexist",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
