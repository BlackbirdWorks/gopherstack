package datasync_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDataSync_Nfs covers the NFS location lifecycle.
func TestDataSync_Nfs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	agentArn1 := createTestAgent(t, h)
	agentArn2 := createTestAgent(t, h)

	// Create
	rec := doRequest(t, h, "CreateLocationNfs", map[string]any{
		"ServerHostname": "nfs.example.com",
		"Subdirectory":   "/exports/data",
		"MountOptions":   map[string]any{"Version": "NFS4_0"},
		"OnPremConfig": map[string]any{
			"AgentArns": []string{agentArn1},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	locArn := createResp["LocationArn"].(string)

	// Describe
	rec = doRequest(t, h, "DescribeLocationNfs", map[string]any{"LocationArn": locArn})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Contains(t, descResp["LocationUri"].(string), "nfs://")
	assert.NotNil(t, descResp["MountOptions"])
	assert.NotNil(t, descResp["OnPremConfig"])
	// The real DescribeLocationNfsOutput has no top-level ServerHostname
	// field (folded into LocationUri only).
	assert.Nil(t, descResp["ServerHostname"])

	// Update
	rec = doRequest(t, h, "UpdateLocationNfs", map[string]any{
		"LocationArn":  locArn,
		"Subdirectory": "/exports/updated",
		"MountOptions": map[string]any{"Version": "NFS4_1"},
		"OnPremConfig": map[string]any{
			"AgentArns": []string{agentArn2},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify update
	rec = doRequest(t, h, "DescribeLocationNfs", map[string]any{"LocationArn": locArn})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	mo := descResp["MountOptions"].(map[string]any)
	assert.Equal(t, "NFS4_1", mo["Version"])

	// Missing ServerHostname
	rec = doRequest(t, h, "CreateLocationNfs", map[string]any{"Subdirectory": "/x"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// Not found
	rec = doRequest(t, h, "DescribeLocationNfs", map[string]any{
		"LocationArn": "arn:aws:datasync:us-east-1:000000000000:location/notexist",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
