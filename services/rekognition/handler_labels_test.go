package rekognition_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rekognition"
)

// TestDetectLabels_MinConfidenceDefault locks in the real SDK's documented
// default: "you can specify MinConfidence to control the confidence
// threshold for the labels returned. The default is 55%."
// (api_op_DetectLabels.go). An omitted MinConfidence must resolve to 55, not
// some other value.
func TestDetectLabels_MinConfidenceDefault(t *testing.T) {
	t.Parallel()

	assert.InDelta(t, 55.0, rekognition.ResolveDetectLabelsMinConfidence(0), 0.001)
	// An explicit value passes through unchanged.
	assert.InDelta(t, 42.0, rekognition.ResolveDetectLabelsMinConfidence(42.0), 0.001)
}

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
