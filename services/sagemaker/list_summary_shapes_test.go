package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	sagemakersdk "github.com/aws/aws-sdk-go-v2/service/sagemaker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListSummaries_MemberRoundTrips drives each op's Create then List
// through the real aws-sdk-go-v2 client and asserts a member that was
// missing from the List summary (over-wide response class,
// gopherstack-dv4s) now round-trips. Every field asserted here is
// populated synchronously at Create, so no async completion wait is needed.
func TestListSummaries_MemberRoundTrips(t *testing.T) {
	t.Parallel()

	t.Run("action source", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		client := newTestSageMakerClient(t, h)

		rr := doSageMakerRequest(t, h, "CreateAction", map[string]any{
			"ActionName": "act-summary",
			"ActionType": "ModelDeployment",
			"Source":     map[string]any{"SourceUri": "s3://bucket/source", "SourceType": "Artifact"},
		})
		require.Equal(t, http.StatusOK, rr.Code)

		out, err := client.ListActions(t.Context(), &sagemakersdk.ListActionsInput{})
		require.NoError(t, err)
		require.Len(t, out.ActionSummaries, 1)
		require.NotNil(t, out.ActionSummaries[0].Source)
		assert.Equal(t, "s3://bucket/source", *out.ActionSummaries[0].Source.SourceUri)
		assert.Equal(t, "Artifact", *out.ActionSummaries[0].Source.SourceType)
	})

	t.Run("code repository git config", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		client := newTestSageMakerClient(t, h)

		rr := doSageMakerRequest(t, h, "CreateCodeRepository", map[string]any{
			"CodeRepositoryName": "repo-summary",
			"GitConfig":          map[string]any{"RepositoryUrl": "https://example.com/repo.git", "Branch": "main"},
		})
		require.Equal(t, http.StatusOK, rr.Code)

		out, err := client.ListCodeRepositories(t.Context(), &sagemakersdk.ListCodeRepositoriesInput{})
		require.NoError(t, err)
		require.Len(t, out.CodeRepositorySummaryList, 1)
		require.NotNil(t, out.CodeRepositorySummaryList[0].GitConfig)
		assert.Equal(t, "https://example.com/repo.git", *out.CodeRepositorySummaryList[0].GitConfig.RepositoryUrl)
		assert.Equal(t, "main", *out.CodeRepositorySummaryList[0].GitConfig.Branch)
	})

	t.Run("project description", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		client := newTestSageMakerClient(t, h)

		rr := doSageMakerRequest(t, h, "CreateProject", map[string]any{
			"ProjectName":        "proj-summary",
			"ProjectDescription": "a test project",
		})
		require.Equal(t, http.StatusOK, rr.Code)

		out, err := client.ListProjects(t.Context(), &sagemakersdk.ListProjectsInput{})
		require.NoError(t, err)
		require.Len(t, out.ProjectSummaryList, 1)
		assert.Equal(t, "a test project", *out.ProjectSummaryList[0].ProjectDescription)
	})

	t.Run("optimization job max instance count", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		client := newTestSageMakerClient(t, h)

		rr := doSageMakerRequest(t, h, "CreateOptimizationJob", map[string]any{
			"OptimizationJobName":    "opt-summary",
			"RoleArn":                "arn:aws:iam::000000000000:role/x",
			"DeploymentInstanceType": "ml.g5.xlarge",
			"MaxInstanceCount":       3,
			"ModelSource": map[string]any{
				"S3": map[string]any{"S3Uri": "s3://bucket/model.tar.gz"},
			},
			"OptimizationConfigs": []map[string]any{
				{"ModelQuantizationConfig": map[string]any{}},
			},
			"OutputConfig":      map[string]any{"S3OutputLocation": "s3://bucket/optimized/"},
			"StoppingCondition": map[string]any{"MaxRuntimeInSeconds": 3600},
		})
		require.Equal(t, http.StatusOK, rr.Code)

		out, err := client.ListOptimizationJobs(t.Context(), &sagemakersdk.ListOptimizationJobsInput{})
		require.NoError(t, err)
		require.Len(t, out.OptimizationJobSummaries, 1)
		require.NotNil(t, out.OptimizationJobSummaries[0].MaxInstanceCount)
		assert.Equal(t, int32(3), *out.OptimizationJobSummaries[0].MaxInstanceCount)
	})

	t.Run("training job secondary status", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		client := newTestSageMakerClient(t, h)

		rr := doSageMakerRequest(t, h, "CreateTrainingJob", map[string]any{
			"TrainingJobName": "tj-summary",
			"RoleArn":         "arn:aws:iam::000000000000:role/x",
			"AlgorithmSpecification": map[string]any{
				"TrainingImage":     "1234.dkr.ecr.us-east-1.amazonaws.com/img:latest",
				"TrainingInputMode": "File",
			},
			"OutputDataConfig": map[string]any{"S3OutputPath": "s3://bucket/output/"},
			"ResourceConfig": map[string]any{
				"InstanceType":   "ml.m5.large",
				"InstanceCount":  1,
				"VolumeSizeInGB": 10,
			},
			"StoppingCondition": map[string]any{"MaxRuntimeInSeconds": 3600},
		})
		require.Equal(t, http.StatusOK, rr.Code)

		out, err := client.ListTrainingJobs(t.Context(), &sagemakersdk.ListTrainingJobsInput{})
		require.NoError(t, err)
		require.Len(t, out.TrainingJobSummaries, 1)
		assert.NotEmpty(t, out.TrainingJobSummaries[0].SecondaryStatus)
	})

	t.Run("inference experiment role arn", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		client := newTestSageMakerClient(t, h)

		rr := doSageMakerRequest(t, h, "CreateInferenceExperiment", map[string]any{
			"Name":         "ie-summary",
			"Type":         "ShadowMode",
			"RoleArn":      "arn:aws:iam::000000000000:role/x",
			"EndpointName": "ep-summary",
			"ModelVariants": []map[string]any{
				{"ModelName": "model-summary", "VariantName": "variant-1"},
			},
			"ShadowModeConfig": map[string]any{
				"SourceModelVariantName": "variant-1",
				"ShadowModelVariants": []map[string]any{
					{"ShadowModelVariantName": "variant-1", "SamplingPercentage": 10},
				},
			},
		})
		require.Equal(t, http.StatusOK, rr.Code)

		out, err := client.ListInferenceExperiments(t.Context(), &sagemakersdk.ListInferenceExperimentsInput{})
		require.NoError(t, err)
		require.Len(t, out.InferenceExperiments, 1)
		require.NotNil(t, out.InferenceExperiments[0].RoleArn)
		assert.Equal(t, "arn:aws:iam::000000000000:role/x", *out.InferenceExperiments[0].RoleArn)
	})
}

// TestListInferenceExperiments_SummaryOmitsArn proves the leak fix: real
// types.InferenceExperimentSummary (types.go:1110-1157) has no Arn member at
// all -- ListInferenceExperiments previously emitted one anyway. Checked at
// the raw-body level since a typed client discards unknown keys and so can't
// see a leak.
func TestListInferenceExperiments_SummaryOmitsArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rr := doSageMakerRequest(t, h, "CreateInferenceExperiment", map[string]any{
		"Name":         "ie-leak",
		"Type":         "ShadowMode",
		"RoleArn":      "arn:aws:iam::000000000000:role/x",
		"EndpointName": "ep-leak",
		"ModelVariants": []map[string]any{
			{"ModelName": "model-leak-ie", "VariantName": "variant-1"},
		},
		"ShadowModeConfig": map[string]any{
			"SourceModelVariantName": "variant-1",
			"ShadowModelVariants": []map[string]any{
				{"ShadowModelVariantName": "variant-1", "SamplingPercentage": 10},
			},
		},
	})
	require.Equal(t, http.StatusOK, rr.Code)

	rr = doSageMakerRequest(t, h, "ListInferenceExperiments", map[string]any{})
	require.Equal(t, http.StatusOK, rr.Code)

	var resp struct {
		InferenceExperiments []map[string]any `json:"InferenceExperiments"`
	}
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	require.Len(t, resp.InferenceExperiments, 1)

	_, hasArn := resp.InferenceExperiments[0]["Arn"]
	assert.False(t, hasArn, "InferenceExperimentSummary has no Arn member on the real SDK type")
	assert.Equal(t, "arn:aws:iam::000000000000:role/x", resp.InferenceExperiments[0]["RoleArn"])
}

// TestListTransformJobs_SummaryOmitsModelName proves the leak fix: real
// types.TransformJobSummary (types.go:22320-22355) has no ModelName member --
// only TransformJob (the Describe-shaped type) does. Checked at the raw-body
// level for the same reason as above.
func TestListTransformJobs_SummaryOmitsModelName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rr := doSageMakerRequest(t, h, "CreateModel", map[string]any{
		"ModelName": "model-leak",
		"PrimaryContainer": map[string]any{
			"Image": "1234.dkr.ecr.us-east-1.amazonaws.com/img:latest",
		},
		"ExecutionRoleArn": "arn:aws:iam::000000000000:role/x",
	})
	require.Equal(t, http.StatusOK, rr.Code)

	rr = doSageMakerRequest(t, h, "CreateTransformJob", map[string]any{
		"TransformJobName": "tj-leak",
		"ModelName":        "model-leak",
		"TransformInput": map[string]any{
			"DataSource": map[string]any{"S3DataSource": map[string]any{
				"S3DataType": "S3Prefix", "S3Uri": "s3://bucket/input/",
			}},
		},
		"TransformOutput":    map[string]any{"S3OutputPath": "s3://bucket/output/"},
		"TransformResources": map[string]any{"InstanceType": "ml.m5.large", "InstanceCount": 1},
	})
	require.Equal(t, http.StatusOK, rr.Code)

	rr = doSageMakerRequest(t, h, "ListTransformJobs", map[string]any{})
	require.Equal(t, http.StatusOK, rr.Code)

	var resp struct {
		TransformJobSummaries []map[string]any `json:"TransformJobSummaries"`
	}
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	require.Len(t, resp.TransformJobSummaries, 1)

	_, hasModelName := resp.TransformJobSummaries[0]["ModelName"]
	assert.False(t, hasModelName, "TransformJobSummary has no ModelName member on the real SDK type")
	assert.Equal(t, "tj-leak", resp.TransformJobSummaries[0]["TransformJobName"])
}
