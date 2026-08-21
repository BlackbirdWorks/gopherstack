package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_EdgePackagingJobLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create
	rec := doSageMakerRequest(t, h, "CreateEdgePackagingJob", map[string]any{
		"EdgePackagingJobName": "my-edge-job",
		"ModelName":            "my-model",
		"ModelVersion":         "1.0",
		"RoleArn":              "arn:aws:iam::000000000000:role/TestRole",
		"CompilationJobName":   "my-comp-job",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	assert.Contains(t, createResp["EdgePackagingJobArn"], "my-edge-job")

	// Describe
	rec = doSageMakerRequest(t, h, "DescribeEdgePackagingJob", map[string]any{
		"EdgePackagingJobName": "my-edge-job",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "my-edge-job", descResp["EdgePackagingJobName"])
	assert.Equal(t, "my-model", descResp["ModelName"])
	assert.Equal(t, "1.0", descResp["ModelVersion"])
	assert.Equal(t, "STARTING", descResp["EdgePackagingJobStatus"])

	// List
	rec = doSageMakerRequest(t, h, "ListEdgePackagingJobs", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	summaries := listResp["EdgePackagingJobSummaries"].([]any)
	assert.Len(t, summaries, 1)

	// Stop
	rec = doSageMakerRequest(t, h, "StopEdgePackagingJob", map[string]any{
		"EdgePackagingJobName": "my-edge-job",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify stopping -- correct immediately after Stop, but an assertion that
	// only checks this moment cannot catch a machine that never advances.
	rec = doSageMakerRequest(t, h, "DescribeEdgePackagingJob", map[string]any{
		"EdgePackagingJobName": "my-edge-job",
	})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "STOPPING", descResp["EdgePackagingJobStatus"])

	// Confirm it actually reaches STOPPED.
	require.Eventually(t, func() bool {
		descRec := doSageMakerRequest(t, h, "DescribeEdgePackagingJob", map[string]any{
			"EdgePackagingJobName": "my-edge-job",
		})
		var out map[string]any
		require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &out))

		return out["EdgePackagingJobStatus"] == "STOPPED"
	}, 2*time.Second, 10*time.Millisecond)
}

func TestHandler_EdgePackagingJob_ReachesCompleted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateEdgePackagingJob", map[string]any{
		"EdgePackagingJobName": "edge-job-completes",
		"ModelName":            "my-model",
		"RoleArn":              "arn:aws:iam::000000000000:role/TestRole",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	require.Eventually(t, func() bool {
		descRec := doSageMakerRequest(t, h, "DescribeEdgePackagingJob", map[string]any{
			"EdgePackagingJobName": "edge-job-completes",
		})
		var out map[string]any
		require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &out))

		return out["EdgePackagingJobStatus"] == "COMPLETED"
	}, 2*time.Second, 10*time.Millisecond)
}

func TestHandler_EdgePackagingJob_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "DescribeEdgePackagingJob", map[string]any{
		"EdgePackagingJobName": "nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_EdgePackagingJob_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{"EdgePackagingJobName": "dup-job"}
	doSageMakerRequest(t, h, "CreateEdgePackagingJob", body)

	rec := doSageMakerRequest(t, h, "CreateEdgePackagingJob", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListEdgePackagingJobs_StatusFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateEdgePackagingJob", map[string]any{"EdgePackagingJobName": "job-a"})
	doSageMakerRequest(t, h, "CreateEdgePackagingJob", map[string]any{"EdgePackagingJobName": "job-b"})
	doSageMakerRequest(t, h, "StopEdgePackagingJob", map[string]any{"EdgePackagingJobName": "job-a"})

	rec := doSageMakerRequest(t, h, "ListEdgePackagingJobs", map[string]any{
		"StatusEquals": "STARTING",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	summaries := resp["EdgePackagingJobSummaries"].([]any)
	assert.Len(t, summaries, 1)
}

// ---------------------------------------------------------------------------
// InferenceRecommendationsJob tests
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Job / JobSchemaVersion (the generic model-customization job family added
// by CreateJob et al. — distinct from every other job type in this
// service; see jobs.go's doc comment)
// ---------------------------------------------------------------------------

func TestHandler_JobLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "create rejects an unknown JobConfigSchemaVersion",
			run: func(t *testing.T) {
				t.Helper()

				h := newTestHandler(t)

				rec := doSageMakerRequest(t, h, "CreateJob", map[string]any{
					"JobName":                "job-1",
					"JobCategory":            "AgentRFT",
					"JobConfigDocument":      `{"foo":"bar"}`,
					"JobConfigSchemaVersion": "9.9",
					"RoleArn":                "arn:aws:iam::000000000000:role/TestRole",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "create then describe starts InProgress, stop transitions to Stopping",
			run: func(t *testing.T) {
				t.Helper()

				h := newTestHandler(t)

				rec := doSageMakerRequest(t, h, "CreateJob", map[string]any{
					"JobName":                "job-2",
					"JobCategory":            "AgentRFT",
					"JobConfigDocument":      `{"foo":"bar"}`,
					"JobConfigSchemaVersion": "1.0",
					"RoleArn":                "arn:aws:iam::000000000000:role/TestRole",
				})
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				var createResp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
				assert.Contains(t, createResp["JobArn"], "job-2")

				rec = doSageMakerRequest(t, h, "DescribeJob", map[string]any{
					"JobCategory": "AgentRFT", "JobName": "job-2",
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				var descResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
				assert.Equal(t, "InProgress", descResp["JobStatus"])
				assert.Equal(t, "AgentRFT", descResp["JobCategory"])
				assert.NotEmpty(t, descResp["SecondaryStatusTransitions"])

				// A different JobCategory for the same JobName must 404 —
				// Describe/Delete/Stop are scoped by category (see
				// resolveJobLocked's doc comment).
				rec = doSageMakerRequest(t, h, "DescribeJob", map[string]any{
					"JobCategory": "AgentRFTEvaluation", "JobName": "job-2",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)

				rec = doSageMakerRequest(t, h, "StopJob", map[string]any{
					"JobCategory": "AgentRFT", "JobName": "job-2",
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				rec = doSageMakerRequest(t, h, "DescribeJob", map[string]any{
					"JobCategory": "AgentRFT", "JobName": "job-2",
				})
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
				assert.Equal(t, "Stopping", descResp["JobStatus"])
			},
		},
		{
			name: "delete rejects a still-running job, succeeds once stopped",
			run: func(t *testing.T) {
				t.Helper()

				h := newTestHandler(t)
				doSageMakerRequest(t, h, "CreateJob", map[string]any{
					"JobName":                "job-del",
					"JobCategory":            "AgentRFT",
					"JobConfigDocument":      `{"foo":"bar"}`,
					"JobConfigSchemaVersion": "1.0",
					"RoleArn":                "arn:aws:iam::000000000000:role/TestRole",
				})

				rec := doSageMakerRequest(t, h, "DeleteJob", map[string]any{
					"JobCategory": "AgentRFT", "JobName": "job-del",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code, "DeleteJob must reject a running job")

				doSageMakerRequest(t, h, "StopJob", map[string]any{"JobCategory": "AgentRFT", "JobName": "job-del"})

				rec = doSageMakerRequest(t, h, "DeleteJob", map[string]any{
					"JobCategory": "AgentRFT", "JobName": "job-del",
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				rec = doSageMakerRequest(t, h, "DescribeJob", map[string]any{
					"JobCategory": "AgentRFT", "JobName": "job-del",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "list is scoped to the requested JobCategory",
			run: func(t *testing.T) {
				t.Helper()

				h := newTestHandler(t)
				doSageMakerRequest(t, h, "CreateJob", map[string]any{
					"JobName": "job-cat-a", "JobCategory": "AgentRFT",
					"JobConfigDocument": `{}`, "JobConfigSchemaVersion": "1.0",
					"RoleArn": "arn:aws:iam::000000000000:role/TestRole",
				})
				doSageMakerRequest(t, h, "CreateJob", map[string]any{
					"JobName": "job-cat-b", "JobCategory": "AgentRFTEvaluation",
					"JobConfigDocument": `{}`, "JobConfigSchemaVersion": "1.0",
					"RoleArn": "arn:aws:iam::000000000000:role/TestRole",
				})

				rec := doSageMakerRequest(t, h, "ListJobs", map[string]any{"JobCategory": "AgentRFT"})
				assert.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				items := resp["JobSummaries"].([]any)
				require.Len(t, items, 1)
				assert.Equal(t, "job-cat-a", items[0].(map[string]any)["JobName"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.run(t)
		})
	}
}

func TestHandler_JobSchemaVersionLookups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "DescribeJobSchemaVersion returns the latest version by default",
			run: func(t *testing.T) {
				t.Helper()

				h := newTestHandler(t)

				rec := doSageMakerRequest(t, h, "DescribeJobSchemaVersion", map[string]any{
					"JobCategory": "AgentRFT",
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, "1.0", resp["JobConfigSchemaVersion"])
				assert.NotEmpty(t, resp["JobConfigSchema"])
			},
		},
		{
			name: "DescribeJobSchemaVersion rejects an unknown version",
			run: func(t *testing.T) {
				t.Helper()

				h := newTestHandler(t)

				rec := doSageMakerRequest(t, h, "DescribeJobSchemaVersion", map[string]any{
					"JobCategory":            "AgentRFT",
					"JobConfigSchemaVersion": "9.9",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "ListJobSchemaVersions returns the known versions for the category",
			run: func(t *testing.T) {
				t.Helper()

				h := newTestHandler(t)

				rec := doSageMakerRequest(t, h, "ListJobSchemaVersions", map[string]any{
					"JobCategory": "AgentRFT",
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				items := resp["JobConfigSchemas"].([]any)
				require.Len(t, items, 1)
				assert.Equal(t, "1.0", items[0].(map[string]any)["JobConfigSchemaVersion"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.run(t)
		})
	}
}
