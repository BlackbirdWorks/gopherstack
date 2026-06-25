package rekognition_test

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// extractLabels extracts the label list from a ListDatasetLabels response body.
// AWS returns either DatasetLabelStats or DatasetLabels depending on the version.
func extractLabels(t *testing.T, body []byte) []any {
	t.Helper()

	var resp map[string]any
	require.NoError(t, json.Unmarshal(body, &resp))

	if v, ok := resp["DatasetLabelStats"].([]any); ok {
		return v
	}

	if v, ok := resp["DatasetLabels"].([]any); ok {
		return v
	}

	return nil
}

// =============================================================================
// ListDatasetLabels parity
// =============================================================================

func TestParity_ListDatasetLabels_SingleLabel(t *testing.T) { //nolint:paralleltest // stateful sequential
	h := newTestHandler(t)

	// Create project + dataset
	rec := doRequest(t, h, "CreateProject", map[string]any{"ProjectName": "lbl-proj"})
	require.Equal(t, http.StatusOK, rec.Code)
	var projResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &projResp))
	projARN := projResp["ProjectArn"].(string)

	rec = doRequest(t, h, "CreateDataset", map[string]any{
		"ProjectArn":  projARN,
		"DatasetType": "TRAIN",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var dsResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &dsResp))
	dsARN := dsResp["DatasetArn"].(string)

	// Add two entries: cat appears twice, dog once
	entry1 := []byte(`{"source-ref":"s3://b/img1.jpg","labels-metadata":{"class-name":"cat"}}`)
	entry2 := []byte(`{"source-ref":"s3://b/img2.jpg","labels-metadata":{"class-name":"dog"}}`)
	entry3 := []byte(`{"source-ref":"s3://b/img3.jpg","labels-metadata":{"class-name":"cat"}}`)

	for _, e := range [][]byte{entry1, entry2, entry3} {
		rec = doRequest(t, h, "UpdateDatasetEntries", map[string]any{
			"DatasetArn": dsARN,
			"Changes":    e,
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec = doRequest(t, h, "ListDatasetLabels", map[string]any{"DatasetArn": dsARN})
	require.Equal(t, http.StatusOK, rec.Code)

	labels := extractLabels(t, rec.Body.Bytes())
	require.NotNil(t, labels, "expected DatasetLabelStats or DatasetLabels key")
	require.Len(t, labels, 2)

	// Sorted by name: cat, dog
	label0 := labels[0].(map[string]any)
	label1 := labels[1].(map[string]any)
	assert.Equal(t, "cat", label0["LabelName"])
	assert.InDelta(t, float64(2), label0["EntryCount"], 0)
	assert.Equal(t, "dog", label1["LabelName"])
	assert.InDelta(t, float64(1), label1["EntryCount"], 0)
}

func TestParity_ListDatasetLabels_MultiLabel(t *testing.T) { //nolint:paralleltest // stateful sequential
	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateProject", map[string]any{"ProjectName": "ml-proj"})
	require.Equal(t, http.StatusOK, rec.Code)
	var projResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &projResp))

	rec = doRequest(t, h, "CreateDataset", map[string]any{
		"ProjectArn":  projResp["ProjectArn"].(string),
		"DatasetType": "TRAIN",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var dsResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &dsResp))
	dsARN := dsResp["DatasetArn"].(string)

	// Multi-label entry: sunglasses + hat
	entry := []byte(`{"source-ref":"s3://b/img.jpg","labels-metadata":{"class-map":{"sunglasses":1,"hat":1}}}`)
	rec = doRequest(t, h, "UpdateDatasetEntries", map[string]any{
		"DatasetArn": dsARN,
		"Changes":    entry,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "ListDatasetLabels", map[string]any{"DatasetArn": dsARN})
	require.Equal(t, http.StatusOK, rec.Code)

	labels := extractLabels(t, rec.Body.Bytes())
	require.Len(t, labels, 2)

	names := []string{
		labels[0].(map[string]any)["LabelName"].(string),
		labels[1].(map[string]any)["LabelName"].(string),
	}
	assert.ElementsMatch(t, []string{"hat", "sunglasses"}, names)
}

func TestParity_ListDatasetLabels_OpaqueToken(t *testing.T) { //nolint:paralleltest // stateful sequential
	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateProject", map[string]any{"ProjectName": "page-proj"})
	require.Equal(t, http.StatusOK, rec.Code)
	var projResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &projResp))

	rec = doRequest(t, h, "CreateDataset", map[string]any{
		"ProjectArn":  projResp["ProjectArn"].(string),
		"DatasetType": "TRAIN",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var dsResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &dsResp))
	dsARN := dsResp["DatasetArn"].(string)

	// Add 3 entries with distinct labels
	for i, lbl := range []string{"apple", "banana", "cherry"} {
		entry, _ := json.Marshal(map[string]any{
			"source-ref":      "s3://b/img" + string(rune('0'+i)) + ".jpg",
			"labels-metadata": map[string]any{"class-name": lbl},
		})
		rec = doRequest(t, h, "UpdateDatasetEntries", map[string]any{
			"DatasetArn": dsARN,
			"Changes":    entry,
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// First page: limit 2
	rec = doRequest(t, h, "ListDatasetLabels", map[string]any{
		"DatasetArn": dsARN,
		"MaxResults": 2,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	firstPageLabels := extractLabels(t, rec.Body.Bytes())
	require.Len(t, firstPageLabels, 2)

	var page1 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page1))

	nextToken, hasToken := page1["NextToken"].(string)
	require.True(t, hasToken, "expected NextToken for more pages")
	assert.NotEmpty(t, nextToken)

	// Verify token is base64url-encoded JSON
	decoded, err := base64.RawURLEncoding.DecodeString(nextToken)
	require.NoError(t, err)
	var tok map[string]any
	require.NoError(t, json.Unmarshal(decoded, &tok))
	assert.InDelta(t, float64(2), tok["o"], 0)

	// Second page
	rec = doRequest(t, h, "ListDatasetLabels", map[string]any{
		"DatasetArn": dsARN,
		"MaxResults": 2,
		"NextToken":  nextToken,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	secondPageLabels := extractLabels(t, rec.Body.Bytes())
	require.Len(t, secondPageLabels, 1, "expected 1 label on second page")

	var page2 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page2))
	_, hasMore := page2["NextToken"].(string)
	assert.False(t, hasMore, "should not have next token on last page")
}

func TestParity_ListDatasetLabels_InvalidToken(t *testing.T) { //nolint:paralleltest // stateful sequential
	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateProject", map[string]any{"ProjectName": "inv-proj"})
	require.Equal(t, http.StatusOK, rec.Code)
	var projResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &projResp))

	rec = doRequest(t, h, "CreateDataset", map[string]any{
		"ProjectArn":  projResp["ProjectArn"].(string),
		"DatasetType": "TRAIN",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var dsResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &dsResp))

	// Invalid token treated as offset 0 (returns from start, no error)
	rec = doRequest(t, h, "ListDatasetLabels", map[string]any{
		"DatasetArn": dsResp["DatasetArn"].(string),
		"NextToken":  "!!!invalid!!!",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// =============================================================================
// DistributeDatasetEntries parity
// =============================================================================

func TestParity_DistributeDatasetEntries_SetsInProgress(t *testing.T) { //nolint:paralleltest // stateful
	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateProject", map[string]any{"ProjectName": "dist-proj"})
	require.Equal(t, http.StatusOK, rec.Code)
	var projResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &projResp))
	projARN := projResp["ProjectArn"].(string)

	rec = doRequest(t, h, "CreateDataset", map[string]any{
		"ProjectArn":  projARN,
		"DatasetType": "TRAIN",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var dsResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &dsResp))
	dsARN := dsResp["DatasetArn"].(string)

	rec = doRequest(t, h, "DistributeDatasetEntries", map[string]any{
		"Datasets": []any{
			map[string]any{"DatasetArn": dsARN},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// DescribeDataset should show UPDATE_IN_PROGRESS
	rec = doRequest(t, h, "DescribeDataset", map[string]any{"DatasetArn": dsARN})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	desc := descResp["DatasetDescription"].(map[string]any)
	assert.Equal(t, "UPDATE_IN_PROGRESS", desc["Status"])
}

func TestParity_DistributeDatasetEntries_UnknownDataset(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "DistributeDatasetEntries", map[string]any{
		"Datasets": []any{
			map[string]any{"DatasetArn": "arn:aws:rekognition:us-east-1:000000000000:project/x/dataset/train/1"},
		},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// =============================================================================
// CreateFaceLivenessSession confidence parity
// =============================================================================

func TestParity_FaceLiveness_ConfidenceRange(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create multiple sessions and verify confidence stays in [75, 100)
	for i := range 10 {
		_ = i
		rec := doRequest(t, h, "CreateFaceLivenessSession", map[string]any{})
		require.Equal(t, http.StatusOK, rec.Code)

		var createResp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
		sessionID := createResp["SessionId"].(string)

		rec = doRequest(t, h, "GetFaceLivenessSessionResults", map[string]any{"SessionId": sessionID})
		require.Equal(t, http.StatusOK, rec.Code)

		var resultResp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resultResp))
		confidence := resultResp["Confidence"].(float64)
		assert.GreaterOrEqual(t, confidence, float64(75), "confidence must be >= 75")
		assert.Less(t, confidence, float64(100), "confidence must be < 100")
	}
}

func TestParity_FaceLiveness_TwoSessionsDifferentConfidence(t *testing.T) { //nolint:paralleltest // stateful
	h := newTestHandler(t)

	getConfidence := func(sessionID string) float64 {
		t.Helper()
		rec := doRequest(t, h, "GetFaceLivenessSessionResults", map[string]any{"SessionId": sessionID})
		require.Equal(t, http.StatusOK, rec.Code)
		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

		return resp["Confidence"].(float64)
	}

	// Create many sessions; at least some should differ in confidence.
	confidences := make(map[float64]bool)
	for range 20 {
		rec := doRequest(t, h, "CreateFaceLivenessSession", map[string]any{})
		require.Equal(t, http.StatusOK, rec.Code)
		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		c := getConfidence(resp["SessionId"].(string))
		confidences[c] = true
	}

	assert.Greater(t, len(confidences), 1, "expected varied confidence values across sessions")
}

// =============================================================================
// MediaAnalysisJob parity
// =============================================================================

func TestParity_MediaAnalysisJob_Lifecycle(t *testing.T) { //nolint:paralleltest // stateful sequential
	h := newTestHandler(t)

	// Start job
	rec := doRequest(t, h, "StartMediaAnalysisJob", map[string]any{
		"JobName":          "test-job",
		"OperationsConfig": map[string]any{},
		"Input":            map[string]any{},
		"OutputConfig":     map[string]any{},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var startResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
	jobID, ok := startResp["JobId"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, jobID)

	// Get job
	rec = doRequest(t, h, "GetMediaAnalysisJob", map[string]any{"JobId": jobID})
	require.Equal(t, http.StatusOK, rec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	assert.Equal(t, jobID, getResp["JobId"])
	assert.NotEmpty(t, getResp["Status"])

	// List jobs
	rec = doRequest(t, h, "ListMediaAnalysisJobs", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	jobs, ok := listResp["MediaAnalysisJobs"].([]any)
	require.True(t, ok)
	assert.GreaterOrEqual(t, len(jobs), 1)
}

func TestParity_MediaAnalysisJob_MissingID_ReturnsError(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "GetMediaAnalysisJob", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestParity_MediaAnalysisJob_UnknownID_ReturnsError(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "GetMediaAnalysisJob", map[string]any{"JobId": "nonexistent-job-id"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
