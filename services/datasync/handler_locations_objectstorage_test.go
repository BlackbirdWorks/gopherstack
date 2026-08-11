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
	agentArn := createTestAgent(t, h)

	// Create
	rec := doRequest(t, h, "CreateLocationObjectStorage", map[string]any{
		"ServerHostname": "s3.example.com",
		"ServerProtocol": "HTTPS",
		"ServerPort":     int32(443),
		"BucketName":     "my-bucket",
		"Subdirectory":   "/data",
		"AccessKey":      "AKIAIOSFODNN7EXAMPLE",
		"SecretKey":      "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		"AgentArns":      []string{agentArn},
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

// TestDataSync_UpdateLocationObjectStorage_ServerHostname covers
// gopherstack-2xhy: UpdateLocationObjectStorageInput.ServerHostname
// (aws-sdk-go-v2/service/datasync v1.61.4
// api_op_UpdateLocationObjectStorage.go:100) must update the location's
// LocationUri, not get silently dropped.
func TestDataSync_UpdateLocationObjectStorage_ServerHostname(t *testing.T) {
	t.Parallel()

	tests := []struct {
		update  map[string]any
		name    string
		wantURI string
	}{
		{
			name:    "hostname alone",
			update:  map[string]any{"ServerHostname": "new.example.com"},
			wantURI: "object-storage://new.example.com/my-bucket/data",
		},
		{
			name:    "hostname absent",
			update:  map[string]any{"Subdirectory": "/updated"},
			wantURI: "object-storage://s3.example.com/my-bucket/updated",
		},
		{
			name: "hostname with server protocol",
			update: map[string]any{
				"ServerHostname": "combo.example.com",
				"ServerProtocol": "HTTP",
			},
			wantURI: "object-storage://combo.example.com/my-bucket/data",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			agentArn := createTestAgent(t, h)

			rec := doRequest(t, h, "CreateLocationObjectStorage", map[string]any{
				"ServerHostname": "s3.example.com",
				"ServerProtocol": "HTTPS",
				"BucketName":     "my-bucket",
				"Subdirectory":   "/data",
				"AgentArns":      []string{agentArn},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			locArn, ok := createResp["LocationArn"].(string)
			require.True(t, ok)

			tt.update["LocationArn"] = locArn
			rec = doRequest(t, h, "UpdateLocationObjectStorage", tt.update)
			require.Equal(t, http.StatusOK, rec.Code)

			rec = doRequest(t, h, "DescribeLocationObjectStorage", map[string]any{"LocationArn": locArn})
			require.Equal(t, http.StatusOK, rec.Code)

			var descResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
			assert.Equal(t, tt.wantURI, descResp["LocationUri"])

			if proto, isSet := tt.update["ServerProtocol"]; isSet {
				assert.Equal(t, proto, descResp["ServerProtocol"])
			}
		})
	}
}
