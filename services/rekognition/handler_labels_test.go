package rekognition_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDetectLabels_ReturnsEmptyList(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "DetectLabels", map[string]any{"Image": map[string]any{}})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotNil(t, resp["Labels"])
}

func TestAsyncVideoJobs_LabelDetection(t *testing.T) { //nolint:paralleltest // stateful sequential
	h := newTestHandler(t)

	// Start job
	rec := doRequest(t, h, "StartLabelDetection", map[string]any{"Video": map[string]any{}})
	require.Equal(t, http.StatusOK, rec.Code, "StartLabelDetection")

	var startResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
	jobID, ok := startResp["JobId"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, jobID)

	// First poll returns IN_PROGRESS
	rec = doRequest(t, h, "GetLabelDetection", map[string]any{"JobId": jobID})
	require.Equal(t, http.StatusOK, rec.Code, "GetLabelDetection")

	var firstResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &firstResp))
	assert.Equal(t, "IN_PROGRESS", firstResp["JobStatus"])

	// Second poll returns SUCCEEDED
	rec = doRequest(t, h, "GetLabelDetection", map[string]any{"JobId": jobID})
	require.Equal(t, http.StatusOK, rec.Code, "GetLabelDetection")

	var secondResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &secondResp))
	assert.Equal(t, "SUCCEEDED", secondResp["JobStatus"])
	assert.NotNil(t, secondResp["Labels"])
}

func TestAsyncVideoJobs_LabelDetection_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "GetLabelDetection", map[string]any{"JobId": "00000000-0000-0000-0000-000000000000"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAsyncVideoJobs_LabelDetection_MissingJobId(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "GetLabelDetection", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
