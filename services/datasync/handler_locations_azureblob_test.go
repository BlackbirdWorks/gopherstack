package datasync_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDataSync_AzureBlob covers the AzureBlob location lifecycle.
func TestDataSync_AzureBlob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	agentArn := createTestAgent(t, h)

	// Create
	rec := doRequest(t, h, "CreateLocationAzureBlob", map[string]any{
		"ContainerUrl":       "https://myaccount.blob.core.windows.net/mycontainer",
		"Subdirectory":       "/data",
		"BlobType":           "BLOCK",
		"AccessTier":         "HOT",
		"AuthenticationType": "SAS",
		"SasConfiguration": map[string]any{
			"Token": "sv=2020-08-04&ss=b&srt=sco&sp=rwdlacupx&se=2023-01-01T00:00:00Z",
		},
		"AgentArns": []string{agentArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	locArn, ok := createResp["LocationArn"].(string)
	require.True(t, ok)
	require.NotEmpty(t, locArn)

	// Describe
	rec = doRequest(t, h, "DescribeLocationAzureBlob", map[string]any{"LocationArn": locArn})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, locArn, descResp["LocationArn"])
	assert.Equal(t, "BLOCK", descResp["BlobType"])
	assert.Equal(t, "HOT", descResp["AccessTier"])
	assert.Equal(t, "SAS", descResp["AuthenticationType"])
	assert.Contains(t, descResp["LocationUri"].(string), "azure-blob://")
	// AWS never echoes the SAS token back on Describe, and there is no
	// separate ContainerUrl member (folded into LocationUri only).
	assert.Nil(t, descResp["SasConfiguration"])
	assert.Nil(t, descResp["ContainerUrl"])

	// Update
	rec = doRequest(t, h, "UpdateLocationAzureBlob", map[string]any{
		"LocationArn":  locArn,
		"Subdirectory": "/updated",
		"AccessTier":   "COOL",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Describe after update
	rec = doRequest(t, h, "DescribeLocationAzureBlob", map[string]any{"LocationArn": locArn})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "COOL", descResp["AccessTier"])

	// Not found
	rec = doRequest(t, h, "DescribeLocationAzureBlob", map[string]any{
		"LocationArn": "arn:aws:datasync:us-east-1:000000000000:location/notexist",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// Missing required field
	rec = doRequest(t, h, "CreateLocationAzureBlob", map[string]any{"Subdirectory": "/x"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
