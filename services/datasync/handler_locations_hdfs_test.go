package datasync_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDataSync_Hdfs covers the HDFS location lifecycle.
func TestDataSync_Hdfs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	agentArn := createTestAgent(t, h)

	// Create with SIMPLE auth
	rec := doRequest(t, h, "CreateLocationHdfs", map[string]any{
		"NameNodes": []any{
			map[string]any{"Hostname": "namenode1.example.com", "Port": 8020},
		},
		"AuthenticationType": "SIMPLE",
		"SimpleUser":         "hadoop",
		"Subdirectory":       "/user/data",
		"BlockSize":          int64(134217728),
		"ReplicationFactor":  int32(3),
		"AgentArns":          []string{agentArn},
		"QopConfiguration": map[string]any{
			"DataTransferProtection": "PRIVACY",
			"RpcProtection":          "PRIVACY",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	locArn := createResp["LocationArn"].(string)

	// Describe
	rec = doRequest(t, h, "DescribeLocationHdfs", map[string]any{"LocationArn": locArn})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Contains(t, descResp["LocationUri"].(string), "hdfs://")
	assert.Equal(t, "SIMPLE", descResp["AuthenticationType"])
	assert.Equal(t, "hadoop", descResp["SimpleUser"])
	assert.Len(t, descResp["NameNodes"], 1)
	assert.NotNil(t, descResp["QopConfiguration"])

	// Update
	rec = doRequest(t, h, "UpdateLocationHdfs", map[string]any{
		"LocationArn": locArn,
		"SimpleUser":  "newuser",
		"NameNodes": []any{
			map[string]any{"Hostname": "namenode2.example.com", "Port": 8020},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify update
	rec = doRequest(t, h, "DescribeLocationHdfs", map[string]any{"LocationArn": locArn})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "newuser", descResp["SimpleUser"])
	nameNodes := descResp["NameNodes"].([]any)
	require.Len(t, nameNodes, 1)
	assert.Equal(t, "namenode2.example.com", nameNodes[0].(map[string]any)["Hostname"])

	// Missing NameNodes
	rec = doRequest(t, h, "CreateLocationHdfs", map[string]any{
		"AuthenticationType": "SIMPLE",
		"SimpleUser":         "hadoop",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// Not found
	rec = doRequest(t, h, "DescribeLocationHdfs", map[string]any{
		"LocationArn": "arn:aws:datasync:us-east-1:000000000000:location/notexist",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
