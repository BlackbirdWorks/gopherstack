package rekognition_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// MediaAnalysisJob
// =============================================================================

func TestMediaAnalysisJob_Lifecycle(t *testing.T) { //nolint:paralleltest // stateful sequential
	h := newTestHandler(t)

	// Start job
	rec := doRequest(t, h, "StartMediaAnalysisJob", map[string]any{
		"JobName": "test-job",
		"OperationsConfig": map[string]any{
			"DetectModerationLabels": map[string]any{
				"MinConfidence":  50.0,
				"ProjectVersion": "arn:aws:rekognition:us-east-1:000000000000:project/p1/version/v1/123",
			},
		},
		"Input": map[string]any{
			"S3Object": map[string]any{"Bucket": "in-bucket", "Name": "in-key"},
		},
		"OutputConfig": map[string]any{
			"S3Bucket":    "out-bucket",
			"S3KeyPrefix": "out-prefix",
		},
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

	input, ok := getResp["Input"].(map[string]any)
	require.True(t, ok, "Input must be echoed back on GetMediaAnalysisJob")
	s3obj, ok := input["S3Object"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "in-bucket", s3obj["Bucket"])
	assert.Equal(t, "in-key", s3obj["Name"])

	outputConfig, ok := getResp["OutputConfig"].(map[string]any)
	require.True(t, ok, "OutputConfig must be echoed back on GetMediaAnalysisJob")
	assert.Equal(t, "out-bucket", outputConfig["S3Bucket"])
	assert.Equal(t, "out-prefix", outputConfig["S3KeyPrefix"])

	opsConfig, ok := getResp["OperationsConfig"].(map[string]any)
	require.True(t, ok, "OperationsConfig must be echoed back on GetMediaAnalysisJob")
	dml, ok := opsConfig["DetectModerationLabels"].(map[string]any)
	require.True(t, ok)
	assert.InDelta(t, 50.0, dml["MinConfidence"], 0.001)

	// List jobs
	rec = doRequest(t, h, "ListMediaAnalysisJobs", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	jobs, ok := listResp["MediaAnalysisJobs"].([]any)
	require.True(t, ok)
	require.GreaterOrEqual(t, len(jobs), 1)

	firstJob, ok := jobs[0].(map[string]any)
	require.True(t, ok)
	assert.Contains(t, firstJob, "Input")
	assert.Contains(t, firstJob, "OutputConfig")
	assert.Contains(t, firstJob, "OperationsConfig")
}

func TestMediaAnalysisJob_MissingRequiredFields_ReturnsError(t *testing.T) {
	t.Parallel()

	validInput := map[string]any{"S3Object": map[string]any{"Bucket": "b", "Name": "k"}}
	validOpsConfig := map[string]any{}
	validOutputConfig := map[string]any{"S3Bucket": "b"}

	tests := map[string]map[string]any{
		"missing operations config": {
			"Input":        validInput,
			"OutputConfig": validOutputConfig,
		},
		"missing input": {
			"OperationsConfig": validOpsConfig,
			"OutputConfig":     validOutputConfig,
		},
		"missing input s3object": {
			"Input":            map[string]any{},
			"OperationsConfig": validOpsConfig,
			"OutputConfig":     validOutputConfig,
		},
		"missing output config": {
			"Input":            validInput,
			"OperationsConfig": validOpsConfig,
		},
		"missing output config s3bucket": {
			"Input":            validInput,
			"OperationsConfig": validOpsConfig,
			"OutputConfig":     map[string]any{},
		},
	}

	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "StartMediaAnalysisJob", body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestMediaAnalysisJob_MissingID_ReturnsError(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "GetMediaAnalysisJob", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestMediaAnalysisJob_UnknownID_ReturnsError(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "GetMediaAnalysisJob", map[string]any{"JobId": "nonexistent-job-id"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// =============================================================================
// Async Video Jobs: person tracking / segment detection
// =============================================================================

func TestAsyncVideoJobs_PersonTrackingAndSegmentDetection(t *testing.T) { //nolint:paralleltest // stateful sequential
	h := newTestHandler(t)

	type jobFlow struct {
		startBody   any
		checkGet    func(t *testing.T, body []byte)
		startAction string
		getAction   string
	}

	flows := []jobFlow{
		{
			startAction: "StartPersonTracking",
			startBody:   map[string]any{"Video": map[string]any{}},
			getAction:   "GetPersonTracking",
			checkGet: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, "SUCCEEDED", resp["JobStatus"])
				assert.NotNil(t, resp["Persons"])
			},
		},
		{
			startAction: "StartSegmentDetection",
			startBody:   map[string]any{"Video": map[string]any{}, "SegmentTypes": []string{"SHOT"}},
			getAction:   "GetSegmentDetection",
			checkGet: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, "SUCCEEDED", resp["JobStatus"])
				assert.NotNil(t, resp["Segments"])
			},
		},
	}

	for _, flow := range flows { //nolint:paralleltest // existing issue.
		t.Run(flow.startAction+"/"+flow.getAction, func(t *testing.T) {
			// Start job
			rec := doRequest(t, h, flow.startAction, flow.startBody)
			require.Equal(t, http.StatusOK, rec.Code, flow.startAction)

			var startResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
			jobID, ok := startResp["JobId"].(string)
			require.True(t, ok)
			assert.NotEmpty(t, jobID)

			// First poll returns IN_PROGRESS
			rec = doRequest(t, h, flow.getAction, map[string]any{"JobId": jobID})
			require.Equal(t, http.StatusOK, rec.Code, flow.getAction)

			var firstResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &firstResp))
			assert.Equal(t, "IN_PROGRESS", firstResp["JobStatus"])

			// Second poll returns SUCCEEDED
			rec = doRequest(t, h, flow.getAction, map[string]any{"JobId": jobID})
			require.Equal(t, http.StatusOK, rec.Code, flow.getAction)

			if flow.checkGet != nil {
				flow.checkGet(t, rec.Body.Bytes())
			}
		})
	}
}

func TestAsyncVideoJobs_PersonTrackingAndSegmentDetection_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	getActions := []string{"GetPersonTracking", "GetSegmentDetection"}

	for _, action := range getActions {
		t.Run(action+"_unknown_job", func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, action, map[string]any{"JobId": "00000000-0000-0000-0000-000000000000"})
			assert.Equal(t, http.StatusBadRequest, rec.Code, action)
		})
	}
}

func TestAsyncVideoJobs_PersonTrackingAndSegmentDetection_MissingJobId(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	getActions := []string{"GetPersonTracking", "GetSegmentDetection"}

	for _, action := range getActions {
		t.Run(action+"_missing_job_id", func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, action, map[string]any{})
			assert.Equal(t, http.StatusBadRequest, rec.Code, action)
		})
	}
}

func TestMediaAnalysisJobs(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	// Start job
	rec := doRequest(t, h, "StartMediaAnalysisJob", map[string]any{
		"JobName":          "my-analysis",
		"OperationsConfig": map[string]any{},
		"Input":            map[string]any{"S3Object": map[string]any{"Bucket": "b", "Name": "k"}},
		"OutputConfig":     map[string]any{"S3Bucket": "b"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var startResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
	jobID := startResp["JobId"].(string)
	assert.NotEmpty(t, jobID)

	// Get job
	t.Run("GetMediaAnalysisJob returns job", func(t *testing.T) { //nolint:paralleltest // existing issue.
		rec := doRequest(t, h, "GetMediaAnalysisJob", map[string]any{"JobId": jobID}) //nolint:govet // existing issue.
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Equal(t, jobID, resp["JobId"])
		assert.Equal(t, "my-analysis", resp["JobName"])
		assert.Equal(t, "SUCCEEDED", resp["Status"])
	})

	// List jobs
	t.Run("ListMediaAnalysisJobs returns job", func(t *testing.T) { //nolint:paralleltest // existing issue.
		rec := doRequest(t, h, "ListMediaAnalysisJobs", map[string]any{}) //nolint:govet // existing issue.
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		jobs, ok := resp["MediaAnalysisJobs"].([]any)
		require.True(t, ok)
		assert.Len(t, jobs, 1)
	})

	// Not found
	t.Run("GetMediaAnalysisJob unknown returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		rec := doRequest( //nolint:govet // existing issue.
			t,
			h,
			"GetMediaAnalysisJob",
			map[string]any{"JobId": "does-not-exist"},
		)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	// Missing ID
	t.Run("GetMediaAnalysisJob missing ID returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		rec := doRequest(t, h, "GetMediaAnalysisJob", map[string]any{}) //nolint:govet // existing issue.
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}
