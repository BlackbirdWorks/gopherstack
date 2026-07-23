package datasync_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDataSync_ObjectStorage covers the ObjectStorage location lifecycle.
func TestDataSync_ObjectStorage(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create
	rec := doRequest(t, h, "CreateLocationObjectStorage", map[string]any{
		"ServerHostname": "s3.example.com",
		"ServerProtocol": "HTTPS",
		"ServerPort":     int32(443),
		"BucketName":     "my-bucket",
		"Subdirectory":   "/data",
		"AccessKey":      "AKIAIOSFODNN7EXAMPLE",
		"SecretKey":      "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		"AgentArns": []string{
			"arn:aws:datasync:us-east-1:000000000000:agent/agent1",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	locArn := createResp["LocationArn"].(string)

	// Describe
	rec = doRequest(t, h, "DescribeLocationObjectStorage", map[string]any{"LocationArn": locArn})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Contains(t, descResp["LocationUri"].(string), "object-storage://")
	assert.Equal(t, "HTTPS", descResp["ServerProtocol"])
	// SecretKey not returned
	assert.Nil(t, descResp["SecretKey"])
	// The real DescribeLocationObjectStorageOutput has no top-level
	// ServerHostname or BucketName field (folded into LocationUri only).
	assert.Nil(t, descResp["ServerHostname"])
	assert.Nil(t, descResp["BucketName"])

	// Update
	rec = doRequest(t, h, "UpdateLocationObjectStorage", map[string]any{
		"LocationArn":    locArn,
		"Subdirectory":   "/updated",
		"ServerProtocol": "HTTP",
		"ServerPort":     int32(80),
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify update
	rec = doRequest(t, h, "DescribeLocationObjectStorage", map[string]any{"LocationArn": locArn})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "HTTP", descResp["ServerProtocol"])

	// Missing ServerHostname
	rec = doRequest(t, h, "CreateLocationObjectStorage", map[string]any{"BucketName": "b"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// Missing BucketName
	rec = doRequest(t, h, "CreateLocationObjectStorage", map[string]any{"ServerHostname": "s3.example.com"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// Not found
	rec = doRequest(t, h, "DescribeLocationObjectStorage", map[string]any{
		"LocationArn": "arn:aws:datasync:us-east-1:000000000000:location/notexist",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
