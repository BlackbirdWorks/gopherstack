package datasync_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDataSync_Efs covers the EFS location lifecycle.
func TestDataSync_Efs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create
	rec := doRequest(t, h, "CreateLocationEfs", map[string]any{
		"EfsFilesystemArn":    "arn:aws:elasticfilesystem:us-east-1:000000000000:file-system/fs-12345678",
		"Subdirectory":        "/data",
		"InTransitEncryption": "TLS1_2",
		"Ec2Config": map[string]any{
			"SubnetArn":         "arn:aws:ec2:us-east-1:000000000000:subnet/subnet-12345",
			"SecurityGroupArns": []string{"arn:aws:ec2:us-east-1:000000000000:security-group/sg-12345"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	locArn := createResp["LocationArn"].(string)

	// Describe
	rec = doRequest(t, h, "DescribeLocationEfs", map[string]any{"LocationArn": locArn})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, locArn, descResp["LocationArn"])
	assert.Equal(t, "TLS1_2", descResp["InTransitEncryption"])
	assert.Contains(t, descResp["LocationUri"].(string), "efs://")
	assert.NotNil(t, descResp["Ec2Config"])
	// The real DescribeLocationEfsOutput has no EfsFilesystemArn field.
	assert.Nil(t, descResp["EfsFilesystemArn"])

	// Update
	rec = doRequest(t, h, "UpdateLocationEfs", map[string]any{
		"LocationArn":         locArn,
		"Subdirectory":        "/updated",
		"InTransitEncryption": "NONE",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Missing EfsFilesystemArn
	rec = doRequest(t, h, "CreateLocationEfs", map[string]any{"Subdirectory": "/x"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// Not found
	rec = doRequest(t, h, "DescribeLocationEfs", map[string]any{
		"LocationArn": "arn:aws:datasync:us-east-1:000000000000:location/notexist",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
