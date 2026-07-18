package rekognition_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDetectText_ReturnsEmptyList(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "DetectText", map[string]any{"Image": map[string]any{}})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotNil(t, resp["TextDetections"])
}

func TestAsyncVideoJobs_TextDetection(t *testing.T) { //nolint:paralleltest // stateful sequential
	h := newTestHandler(t)

	// Start job
	rec := doRequest(t, h, "StartTextDetection", map[string]any{"Video": map[string]any{}})
	require.Equal(t, http.StatusOK, rec.Code, "StartTextDetection")

	var startResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
	jobID, ok := startResp["JobId"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, jobID)

	// First poll returns IN_PROGRESS
	rec = doRequest(t, h, "GetTextDetection", map[string]any{"JobId": jobID})
	require.Equal(t, http.StatusOK, rec.Code, "GetTextDetection")

	var firstResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &firstResp))
	assert.Equal(t, "IN_PROGRESS", firstResp["JobStatus"])

	// Second poll returns SUCCEEDED
	rec = doRequest(t, h, "GetTextDetection", map[string]any{"JobId": jobID})
	require.Equal(t, http.StatusOK, rec.Code, "GetTextDetection")

	var secondResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &secondResp))
	assert.Equal(t, "SUCCEEDED", secondResp["JobStatus"])
	assert.NotNil(t, secondResp["TextDetections"])
}

func TestAsyncVideoJobs_TextDetection_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "GetTextDetection", map[string]any{"JobId": "00000000-0000-0000-0000-000000000000"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAsyncVideoJobs_TextDetection_MissingJobId(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "GetTextDetection", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
