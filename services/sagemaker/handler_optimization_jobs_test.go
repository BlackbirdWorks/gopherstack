package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sagemakersdk "github.com/aws/aws-sdk-go-v2/service/sagemaker"
	smtypes "github.com/aws/aws-sdk-go-v2/service/sagemaker/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// validOptimizationJobBody returns a CreateOptimizationJob request carrying
// every required member (api_op_CreateOptimizationJob.go:48-114): RoleArn,
// DeploymentInstanceType, ModelSource, OptimizationConfigs, OutputConfig,
// StoppingCondition, in addition to OptimizationJobName. Existing tests
// before this pass sent only OptimizationJobName -- and passed, because the
// handler decoded none of the others -- which is exactly the
// parsed-then-ignored... no, never-parsed-at-all defect this pass fixes;
// they are rewritten here to exercise the now-enforced required fields.
func validOptimizationJobBody(name string) map[string]any {
	return map[string]any{
		"OptimizationJobName":    name,
		"RoleArn":                "arn:aws:iam::000000000000:role/TestRole",
		"DeploymentInstanceType": "ml.g5.2xlarge",
		"ModelSource": map[string]any{
			"S3": map[string]any{"S3Uri": "s3://bucket/model/"},
		},
		"OptimizationConfigs": []any{
			map[string]any{
				"ModelQuantizationConfig": map[string]any{"Image": "acct.dkr.ecr.region.amazonaws.com/lmi:latest"},
			},
		},
		"OutputConfig": map[string]any{"S3OutputLocation": "s3://bucket/output/"},
		"StoppingCondition": map[string]any{
			"MaxRuntimeInSeconds": 3600,
		},
	}
}

func TestHandler_CreateOptimizationJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateOptimizationJob", validOptimizationJobBody("my-opt-job"))
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["OptimizationJobArn"], "my-opt-job")
}

// TestHandler_CreateOptimizationJob_RequiredFieldsEnforced asserts each of
// CreateOptimizationJobInput's required members (besides OptimizationJobName)
// is now actually validated -- previously the handler didn't decode any of
// them, so a request missing all of them succeeded.
func TestHandler_CreateOptimizationJob_RequiredFieldsEnforced(t *testing.T) {
	t.Parallel()

	tests := []struct {
		missing string
	}{
		{missing: "RoleArn"},
		{missing: "DeploymentInstanceType"},
		{missing: "ModelSource"},
		{missing: "OptimizationConfigs"},
		{missing: "OutputConfig"},
	}

	for _, tc := range tests {
		t.Run(tc.missing, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			body := validOptimizationJobBody("opt-missing-" + tc.missing)
			delete(body, tc.missing)

			rec := doSageMakerRequest(t, h, "CreateOptimizationJob", body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestHandler_DescribeOptimizationJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateOptimizationJob", validOptimizationJobBody("opt-1"))
	rec := doSageMakerRequest(t, h, "DescribeOptimizationJob", map[string]any{"OptimizationJobName": "opt-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "opt-1", resp["OptimizationJobName"])
	assert.Equal(t, "ml.g5.2xlarge", resp["DeploymentInstanceType"])
}

func TestHandler_StopOptimizationJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateOptimizationJob", validOptimizationJobBody("opt-stop"))
	rec := doSageMakerRequest(t, h, "StopOptimizationJob", map[string]any{"OptimizationJobName": "opt-stop"})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_DeleteOptimizationJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateOptimizationJob", validOptimizationJobBody("opt-del"))
	rec := doSageMakerRequest(t, h, "DeleteOptimizationJob", map[string]any{"OptimizationJobName": "opt-del"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeOptimizationJob", map[string]any{"OptimizationJobName": "opt-del"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// StudioLifecycleConfig
// ---------------------------------------------------------------------------

func TestHandler_ListOptimizationJobs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Empty initially
	rec := doSageMakerRequest(t, h, "ListOptimizationJobs", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["OptimizationJobSummaries"])

	// Create and list
	doSageMakerRequest(t, h, "CreateOptimizationJob", validOptimizationJobBody("my-opt-job"))

	rec = doSageMakerRequest(t, h, "ListOptimizationJobs", map[string]any{})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	summaries := resp["OptimizationJobSummaries"].([]any)
	assert.Len(t, summaries, 1)
	s := summaries[0].(map[string]any)
	assert.Equal(t, "my-opt-job", s["OptimizationJobName"])
	assert.Equal(t, "ml.g5.2xlarge", s["DeploymentInstanceType"])
	assert.Contains(t, s["OptimizationTypes"], "Quantization")
}

// TestHandler_ListOptimizationJobs_FilterSortPage_RealClient asserts
// ListOptimizationJobsInput's LastModifiedTimeAfter/Before, SortBy/SortOrder,
// and OptimizationContains -- previously the handler decoded only NextToken,
// silently ignoring every other filter and the doc's stated CreationTime/
// Ascending default sort.
func TestHandler_ListOptimizationJobs_FilterSortPage_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	names := []string{"beta-opt", "alpha-opt"}
	for _, n := range names {
		_, err := client.CreateOptimizationJob(t.Context(), &sagemakersdk.CreateOptimizationJobInput{
			OptimizationJobName:    aws.String(n),
			RoleArn:                aws.String("arn:aws:iam::000000000000:role/TestRole"),
			DeploymentInstanceType: smtypes.OptimizationJobDeploymentInstanceType("ml.g5.2xlarge"),
			ModelSource: &smtypes.OptimizationJobModelSource{
				S3: &smtypes.OptimizationJobModelSourceS3{S3Uri: aws.String("s3://bucket/model/")},
			},
			OptimizationConfigs: []smtypes.OptimizationConfig{
				&smtypes.OptimizationConfigMemberModelQuantizationConfig{
					Value: smtypes.ModelQuantizationConfig{
						Image: aws.String("acct.dkr.ecr.region.amazonaws.com/lmi:latest"),
					},
				},
			},
			OutputConfig: &smtypes.OptimizationJobOutputConfig{
				S3OutputLocation: aws.String("s3://bucket/output/"),
			},
			StoppingCondition: &smtypes.StoppingCondition{MaxRuntimeInSeconds: aws.Int32(3600)},
		})
		require.NoError(t, err)
	}

	t.Run("ascending sort by name", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListOptimizationJobs(t.Context(), &sagemakersdk.ListOptimizationJobsInput{
			SortBy: smtypes.ListOptimizationJobsSortByName, SortOrder: smtypes.SortOrderAscending,
		})
		require.NoError(t, err)
		require.Len(t, out.OptimizationJobSummaries, 2)
		assert.Equal(t, "alpha-opt", aws.ToString(out.OptimizationJobSummaries[0].OptimizationJobName))
		assert.Equal(t, "beta-opt", aws.ToString(out.OptimizationJobSummaries[1].OptimizationJobName))
	})

	t.Run("optimization contains matches", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListOptimizationJobs(t.Context(), &sagemakersdk.ListOptimizationJobsInput{
			OptimizationContains: aws.String("Quantization"),
		})
		require.NoError(t, err)
		assert.Len(t, out.OptimizationJobSummaries, 2)
	})

	t.Run("optimization contains no match", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListOptimizationJobs(t.Context(), &sagemakersdk.ListOptimizationJobsInput{
			OptimizationContains: aws.String("Compilation"),
		})
		require.NoError(t, err)
		assert.Empty(t, out.OptimizationJobSummaries)
	})

	t.Run("last modified time after future excludes", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListOptimizationJobs(t.Context(), &sagemakersdk.ListOptimizationJobsInput{
			LastModifiedTimeAfter: aws.Time(time.Now().Add(time.Hour)),
		})
		require.NoError(t, err)
		assert.Empty(t, out.OptimizationJobSummaries)
	})
}

// ---------------------------------------------------------------------------
// AIWorkloadConfig
// ---------------------------------------------------------------------------

func TestHandler_AIWorkloadConfigLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "create then describe returns the stored config",
			run: func(t *testing.T) {
				t.Helper()

				h := newTestHandler(t)

				rec := doSageMakerRequest(t, h, "CreateAIWorkloadConfig", map[string]any{
					"AIWorkloadConfigName": "cfg-1",
					"AIWorkloadConfigs":    map[string]any{"WorkloadSpec": map[string]any{"Yaml": "spec: v1"}},
				})
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				var createResp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
				assert.Contains(t, createResp["AIWorkloadConfigArn"], "cfg-1")

				rec = doSageMakerRequest(t, h, "DescribeAIWorkloadConfig", map[string]any{
					"AIWorkloadConfigName": "cfg-1",
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				var descResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
				assert.Equal(t, "cfg-1", descResp["AIWorkloadConfigName"])
				assert.NotEmpty(t, descResp["CreationTime"])
			},
		},
		{
			name: "duplicate name is rejected",
			run: func(t *testing.T) {
				t.Helper()

				h := newTestHandler(t)
				body := map[string]any{"AIWorkloadConfigName": "cfg-dup"}
				doSageMakerRequest(t, h, "CreateAIWorkloadConfig", body)

				rec := doSageMakerRequest(t, h, "CreateAIWorkloadConfig", body)
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "describe missing config returns not found",
			run: func(t *testing.T) {
				t.Helper()

				h := newTestHandler(t)

				rec := doSageMakerRequest(t, h, "DescribeAIWorkloadConfig", map[string]any{
					"AIWorkloadConfigName": "nonexistent",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)

				var out map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Equal(t, "ResourceNotFound", out["__type"])
			},
		},
		{
			name: "delete then describe returns not found",
			run: func(t *testing.T) {
				t.Helper()

				h := newTestHandler(t)
				doSageMakerRequest(t, h, "CreateAIWorkloadConfig", map[string]any{"AIWorkloadConfigName": "cfg-del"})

				rec := doSageMakerRequest(t, h, "DeleteAIWorkloadConfig", map[string]any{
					"AIWorkloadConfigName": "cfg-del",
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				rec = doSageMakerRequest(t, h, "DescribeAIWorkloadConfig", map[string]any{
					"AIWorkloadConfigName": "cfg-del",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "list returns created configs",
			run: func(t *testing.T) {
				t.Helper()

				h := newTestHandler(t)
				doSageMakerRequest(t, h, "CreateAIWorkloadConfig", map[string]any{"AIWorkloadConfigName": "cfg-list"})

				rec := doSageMakerRequest(t, h, "ListAIWorkloadConfigs", map[string]any{})
				assert.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				items := resp["AIWorkloadConfigs"].([]any)
				assert.Len(t, items, 1)
				assert.Equal(t, "cfg-list", items[0].(map[string]any)["AIWorkloadConfigName"])
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
