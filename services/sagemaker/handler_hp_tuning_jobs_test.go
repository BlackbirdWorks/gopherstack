package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sagemaker"
)

func TestHandler_ListTrainingJobsForHyperParameterTuningJob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupJobs     func(t *testing.T, h *sagemaker.Handler)
		name          string
		jobName       string
		wantCode      int
		wantSummaries int
	}{
		{
			name:          "empty_training_jobs",
			jobName:       "my-hp-job",
			setupJobs:     func(_ *testing.T, _ *sagemaker.Handler) {},
			wantCode:      http.StatusOK,
			wantSummaries: 0,
		},
		{
			name:    "populated_training_jobs",
			jobName: "my-hp-job-2",
			setupJobs: func(t *testing.T, h *sagemaker.Handler) {
				t.Helper()
				doSageMakerRequest(t, h, "CreateTrainingJob", map[string]any{
					"TrainingJobName": "my-hp-job-2-001",
					"AlgorithmSpecification": map[string]any{
						"TrainingInputMode": "File",
					},
					"RoleArn": "arn:aws:iam::123456789012:role/SageMakerRole",
					"OutputDataConfig": map[string]any{
						"S3OutputPath": "s3://bucket/output",
					},
					"ResourceConfig": map[string]any{
						"InstanceCount":  1,
						"InstanceType":   "ml.m5.large",
						"VolumeSizeInGB": 10,
					},
					"StoppingCondition": map[string]any{
						"MaxRuntimeInSeconds": 3600,
					},
				})
			},
			wantCode:      http.StatusOK,
			wantSummaries: 1,
		},
		{
			name:          "not_found",
			jobName:       "nonexistent",
			setupJobs:     func(_ *testing.T, _ *sagemaker.Handler) {},
			wantCode:      http.StatusBadRequest,
			wantSummaries: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.wantCode == http.StatusOK {
				doSageMakerRequest(t, h, "CreateHyperParameterTuningJob", map[string]any{
					"HyperParameterTuningJobName": tt.jobName,
					"HyperParameterTuningJobConfig": map[string]any{
						"Strategy": "Bayesian",
					},
				})
			}

			tt.setupJobs(t, h)

			rec := doSageMakerRequest(t, h, "ListTrainingJobsForHyperParameterTuningJob", map[string]any{
				"HyperParameterTuningJobName": tt.jobName,
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				summaries, ok := resp["TrainingJobSummaries"].([]any)
				require.True(t, ok)
				assert.Len(t, summaries, tt.wantSummaries)
			}
		})
	}
}

func TestHandler_DescribeHyperParameterTuningJob_WireShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateHyperParameterTuningJob", map[string]any{
		"HyperParameterTuningJobName": "wire-shape-job",
		"HyperParameterTuningJobConfig": map[string]any{
			"Strategy": "Bayesian",
			"ResourceLimits": map[string]any{
				"MaxNumberOfTrainingJobs": 10,
				"MaxParallelTrainingJobs": 2,
				"MaxRuntimeInSeconds":     3600,
			},
		},
	})

	rec := doSageMakerRequest(t, h, "DescribeHyperParameterTuningJob", map[string]any{
		"HyperParameterTuningJobName": "wire-shape-job",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	// Real AWS nests Strategy/ResourceLimits inside HyperParameterTuningJobConfig
	// rather than emitting a flat top-level Strategy field — a client reading
	// output.HyperParameterTuningJobConfig.Strategy would get nothing from the
	// old flat shape.
	cfg, ok := resp["HyperParameterTuningJobConfig"].(map[string]any)
	require.True(t, ok, "HyperParameterTuningJobConfig must be a nested object")
	assert.Equal(t, "Bayesian", cfg["Strategy"])

	limits, ok := cfg["ResourceLimits"].(map[string]any)
	require.True(t, ok, "ResourceLimits must be nested under HyperParameterTuningJobConfig")
	assert.InDelta(t, float64(10), limits["MaxNumberOfTrainingJobs"], 0)
	assert.InDelta(t, float64(2), limits["MaxParallelTrainingJobs"], 0)
	assert.InDelta(t, float64(3600), limits["MaxRuntimeInSeconds"], 0)

	// ObjectiveStatusCounters and TrainingJobStatusCounters are both
	// "This member is required" in the real DescribeHyperParameterTuningJobOutput —
	// real AWS SDK client code dereferences them unconditionally, so the emulator
	// must always emit both objects even though this backend never launches child
	// training jobs (hence the zero counts).
	require.Contains(t, resp, "ObjectiveStatusCounters")
	objCounters, ok := resp["ObjectiveStatusCounters"].(map[string]any)
	require.True(t, ok)
	assert.InDelta(t, float64(0), objCounters["Succeeded"], 0)
	assert.InDelta(t, float64(0), objCounters["Pending"], 0)
	assert.InDelta(t, float64(0), objCounters["Failed"], 0)

	require.Contains(t, resp, "TrainingJobStatusCounters")
	tjCounters, ok := resp["TrainingJobStatusCounters"].(map[string]any)
	require.True(t, ok)
	assert.InDelta(t, float64(0), tjCounters["Completed"], 0)
	assert.InDelta(t, float64(0), tjCounters["InProgress"], 0)
}

func TestHandler_ListHyperParameterTuningJobs_WireShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateHyperParameterTuningJob", map[string]any{
		"HyperParameterTuningJobName": "wire-shape-list-job",
		"HyperParameterTuningJobConfig": map[string]any{
			"Strategy": "Random",
			"ResourceLimits": map[string]any{
				"MaxParallelTrainingJobs": 4,
			},
		},
	})

	rec := doSageMakerRequest(t, h, "ListHyperParameterTuningJobs", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	summaries, ok := resp["HyperParameterTuningJobSummaries"].([]any)
	require.True(t, ok)
	require.Len(t, summaries, 1)

	summary, ok := summaries[0].(map[string]any)
	require.True(t, ok)

	// Unlike Describe, ListHyperParameterTuningJobsSummary keeps Strategy flat
	// (it is not nested under a config object in the real HyperParameterTuningJobSummary
	// shape) but ObjectiveStatusCounters/TrainingJobStatusCounters are still
	// required members.
	assert.Equal(t, "Random", summary["Strategy"])
	require.Contains(t, summary, "ObjectiveStatusCounters")
	require.Contains(t, summary, "TrainingJobStatusCounters")
	require.Contains(t, summary, "ResourceLimits")
}

func TestHandler_HyperParameterTuningJobLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create HPT job.
	recCreate := doSageMakerRequest(t, h, "CreateHyperParameterTuningJob", map[string]any{
		"HyperParameterTuningJobName": "my-hpt-job",
		"HyperParameterTuningJobConfig": map[string]any{
			"Strategy": "Bayesian",
			"ResourceLimits": map[string]any{
				"MaxNumberOfTrainingJobs": 10,
				"MaxParallelTrainingJobs": 2,
			},
		},
	})
	assert.Equal(t, http.StatusOK, recCreate.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(recCreate.Body.Bytes(), &createOut))
	assert.NotEmpty(t, createOut["HyperParameterTuningJobArn"])

	// Describe.
	recDesc := doSageMakerRequest(t, h, "DescribeHyperParameterTuningJob", map[string]any{
		"HyperParameterTuningJobName": "my-hpt-job",
	})
	assert.Equal(t, http.StatusOK, recDesc.Code)

	// List.
	recList := doSageMakerRequest(t, h, "ListHyperParameterTuningJobs", map[string]any{})
	assert.Equal(t, http.StatusOK, recList.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &listOut))
	assert.Len(t, listOut["HyperParameterTuningJobSummaries"].([]any), 1)

	// Stop.
	recStop := doSageMakerRequest(t, h, "StopHyperParameterTuningJob", map[string]any{
		"HyperParameterTuningJobName": "my-hpt-job",
	})
	assert.Equal(t, http.StatusOK, recStop.Code)

	// Delete.
	recDelete := doSageMakerRequest(t, h, "DeleteHyperParameterTuningJob", map[string]any{
		"HyperParameterTuningJobName": "my-hpt-job",
	})
	assert.Equal(t, http.StatusOK, recDelete.Code)
}
