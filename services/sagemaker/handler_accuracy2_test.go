package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sagemaker"
)

// ---------------------------------------------------------------------------
// TransformJob tests (gap #12)
// ---------------------------------------------------------------------------

func TestHandler_TransformJobLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create
	rec := doSageMakerRequest(t, h, "CreateTransformJob", map[string]any{
		"TransformJobName": "my-transform",
		"ModelName":        "my-model",
		"TransformInput": map[string]any{
			"DataSource": map[string]any{
				"S3DataSource": map[string]any{
					"S3Uri":      "s3://bucket/input",
					"S3DataType": "S3Prefix",
				},
			},
			"ContentType": "text/csv",
		},
		"TransformOutput": map[string]any{
			"S3OutputPath": "s3://bucket/output",
		},
		"TransformResources": map[string]any{
			"InstanceType":  "ml.m5.large",
			"InstanceCount": 1,
		},
		"BatchStrategy": "MultiRecord",
		"Environment":   map[string]string{"KEY": "VALUE"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	assert.NotEmpty(t, createResp["TransformJobArn"])

	// Describe — InProgress initially
	rec = doSageMakerRequest(t, h, "DescribeTransformJob", map[string]any{
		"TransformJobName": "my-transform",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "my-transform", descResp["TransformJobName"])
	assert.Equal(t, "my-model", descResp["ModelName"])
	assert.Equal(t, "InProgress", descResp["TransformJobStatus"])
	assert.Equal(t, "MultiRecord", descResp["BatchStrategy"])

	// List
	rec = doSageMakerRequest(t, h, "ListTransformJobs", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	summaries := listResp["TransformJobSummaries"].([]any)
	assert.Len(t, summaries, 1)

	// Wait for completion
	time.Sleep(400 * time.Millisecond)
	rec = doSageMakerRequest(t, h, "DescribeTransformJob", map[string]any{
		"TransformJobName": "my-transform",
	})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "Completed", descResp["TransformJobStatus"])
}

func TestHandler_TransformJob_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "DescribeTransformJob", map[string]any{
		"TransformJobName": "nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_TransformJob_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createBody := map[string]any{
		"TransformJobName": "dup-transform",
		"ModelName":        "my-model",
		"TransformInput": map[string]any{
			"DataSource": map[string]any{
				"S3DataSource": map[string]any{"S3Uri": "s3://bucket/input"},
			},
		},
		"TransformOutput":    map[string]any{"S3OutputPath": "s3://bucket/output"},
		"TransformResources": map[string]any{"InstanceType": "ml.m5.large", "InstanceCount": 1},
	}

	rec := doSageMakerRequest(t, h, "CreateTransformJob", createBody)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "CreateTransformJob", createBody)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_StopTransformJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateTransformJob", map[string]any{
		"TransformJobName": "stop-me",
		"ModelName":        "my-model",
		"TransformInput": map[string]any{
			"DataSource": map[string]any{
				"S3DataSource": map[string]any{"S3Uri": "s3://bucket/input"},
			},
		},
		"TransformOutput":    map[string]any{"S3OutputPath": "s3://bucket/output"},
		"TransformResources": map[string]any{"InstanceType": "ml.m5.large", "InstanceCount": 1},
	})

	rec := doSageMakerRequest(t, h, "StopTransformJob", map[string]any{
		"TransformJobName": "stop-me",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Should now be Stopping
	rec = doSageMakerRequest(t, h, "DescribeTransformJob", map[string]any{
		"TransformJobName": "stop-me",
	})
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "Stopping", descResp["TransformJobStatus"])
}

func TestHandler_ListTransformJobs_StatusFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i, name := range []string{"tj-1", "tj-2"} {
		doSageMakerRequest(t, h, "CreateTransformJob", map[string]any{
			"TransformJobName": name,
			"ModelName":        "model",
			"TransformInput": map[string]any{
				"DataSource": map[string]any{
					"S3DataSource": map[string]any{"S3Uri": "s3://b/in"},
				},
			},
			"TransformOutput":    map[string]any{"S3OutputPath": "s3://b/out"},
			"TransformResources": map[string]any{"InstanceType": "ml.m5.large", "InstanceCount": 1},
		})
		if i == 0 {
			doSageMakerRequest(t, h, "StopTransformJob", map[string]any{"TransformJobName": name})
		}
	}

	rec := doSageMakerRequest(t, h, "ListTransformJobs", map[string]any{
		"StatusEquals": "InProgress",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	summaries := listResp["TransformJobSummaries"].([]any)
	assert.Len(t, summaries, 1)
	assert.Equal(t, "tj-2", summaries[0].(map[string]any)["TransformJobName"])
}

// ---------------------------------------------------------------------------
// UpdateFeatureGroup tests (gap #19)
// ---------------------------------------------------------------------------

func TestHandler_UpdateFeatureGroup(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create feature group
	rec := doSageMakerRequest(t, h, "CreateFeatureGroup", map[string]any{
		"FeatureGroupName":            "my-features",
		"RecordIdentifierFeatureName": "id",
		"EventTimeFeatureName":        "ts",
		"FeatureDefinitions": []any{
			map[string]any{"FeatureName": "id", "FeatureType": "String"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Update: add a new feature definition
	rec = doSageMakerRequest(t, h, "UpdateFeatureGroup", map[string]any{
		"FeatureGroupName": "my-features",
		"FeatureAdditions": []any{
			map[string]any{"FeatureName": "score", "FeatureType": "Fractional"},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var updateResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updateResp))
	assert.NotEmpty(t, updateResp["FeatureGroupArn"])

	// Describe should show 2 features now
	rec = doSageMakerRequest(t, h, "DescribeFeatureGroup", map[string]any{
		"FeatureGroupName": "my-features",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	defs := descResp["FeatureDefinitions"].([]any)
	assert.Len(t, defs, 2)
}

func TestHandler_UpdateFeatureGroup_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "UpdateFeatureGroup", map[string]any{
		"FeatureGroupName": "nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// UpdateExperiment, UpdateTrial, UpdateTrialComponent (gap #26)
// ---------------------------------------------------------------------------

func TestHandler_UpdateExperiment(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateExperiment", map[string]any{
		"ExperimentName": "my-exp",
	})

	rec := doSageMakerRequest(t, h, "UpdateExperiment", map[string]any{
		"ExperimentName": "my-exp",
		"DisplayName":    "My Experiment",
		"Description":    "A test experiment",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var updateResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updateResp))
	assert.NotEmpty(t, updateResp["ExperimentArn"])

	// Describe returns updated fields
	rec = doSageMakerRequest(t, h, "DescribeExperiment", map[string]any{
		"ExperimentName": "my-exp",
	})
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "My Experiment", descResp["DisplayName"])
	assert.Equal(t, "A test experiment", descResp["Description"])
}

func TestHandler_UpdateExperiment_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "UpdateExperiment", map[string]any{
		"ExperimentName": "nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_UpdateTrial(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateExperiment", map[string]any{"ExperimentName": "exp"})
	doSageMakerRequest(t, h, "CreateTrial", map[string]any{
		"TrialName":      "my-trial",
		"ExperimentName": "exp",
	})

	rec := doSageMakerRequest(t, h, "UpdateTrial", map[string]any{
		"TrialName":   "my-trial",
		"DisplayName": "Trial Display",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var updateResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updateResp))
	assert.NotEmpty(t, updateResp["TrialArn"])

	// Describe returns updated DisplayName
	rec = doSageMakerRequest(t, h, "DescribeTrial", map[string]any{"TrialName": "my-trial"})
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "Trial Display", descResp["DisplayName"])
}

func TestHandler_UpdateTrialComponent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateTrialComponent", map[string]any{
		"TrialComponentName": "my-tc",
	})

	rec := doSageMakerRequest(t, h, "UpdateTrialComponent", map[string]any{
		"TrialComponentName": "my-tc",
		"DisplayName":        "TC Display",
		"Status":             "InProgress",
		"Parameters": map[string]any{
			"lr": map[string]any{"NumberValue": 0.001},
		},
		"InputArtifacts": map[string]any{
			"train": map[string]any{"Value": "s3://bucket/train", "MediaType": "text/csv"},
		},
		"OutputArtifacts": map[string]any{
			"model": map[string]any{"Value": "s3://bucket/model.tar.gz"},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var updateResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updateResp))
	assert.NotEmpty(t, updateResp["TrialComponentArn"])

	// Describe returns updated fields
	rec = doSageMakerRequest(t, h, "DescribeTrialComponent", map[string]any{
		"TrialComponentName": "my-tc",
	})
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "TC Display", descResp["DisplayName"])
	assert.Equal(t, "InProgress", descResp["Status"])
	assert.NotNil(t, descResp["Parameters"])
	assert.NotNil(t, descResp["InputArtifacts"])
	assert.NotNil(t, descResp["OutputArtifacts"])
}

// ---------------------------------------------------------------------------
// Pipeline full fields (gaps #23, #25)
// ---------------------------------------------------------------------------

func TestHandler_CreatePipeline_FullFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreatePipeline", map[string]any{
		"PipelineName":        "my-pipeline",
		"PipelineDisplayName": "My Pipeline",
		"PipelineDescription": "A test pipeline",
		"RoleArn":             "arn:aws:iam::000000000000:role/SageMakerRole",
		"ParallelismConfiguration": map[string]any{
			"MaxParallelExecutionSteps": 5,
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Describe returns new fields
	rec = doSageMakerRequest(t, h, "DescribePipeline", map[string]any{
		"PipelineName": "my-pipeline",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "My Pipeline", descResp["PipelineDisplayName"])
	assert.Equal(t, "A test pipeline", descResp["PipelineDescription"])
	assert.NotNil(t, descResp["ParallelismConfiguration"])
}

func TestHandler_UpdatePipeline_FullFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreatePipeline", map[string]any{
		"PipelineName": "my-pipeline",
		"RoleArn":      "arn:aws:iam::000000000000:role/Role",
	})

	rec := doSageMakerRequest(t, h, "UpdatePipeline", map[string]any{
		"PipelineName":        "my-pipeline",
		"PipelineDisplayName": "Updated Display",
		"PipelineDescription": "Updated desc",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribePipeline", map[string]any{
		"PipelineName": "my-pipeline",
	})
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "Updated Display", descResp["PipelineDisplayName"])
	assert.Equal(t, "Updated desc", descResp["PipelineDescription"])
}

func TestHandler_StartPipelineExecution_WithParams(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreatePipeline", map[string]any{
		"PipelineName": "param-pipeline",
	})

	rec := doSageMakerRequest(t, h, "StartPipelineExecution", map[string]any{
		"PipelineName":                 "param-pipeline",
		"PipelineExecutionDisplayName": "Run 1",
		"PipelineExecutionDescription": "First run",
		"PipelineParameters": []any{
			map[string]any{"Name": "learning_rate", "Value": "0.001"},
			map[string]any{"Name": "epochs", "Value": "10"},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var startResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
	execArn := startResp["PipelineExecutionArn"]
	assert.NotEmpty(t, execArn)

	// Describe returns parameters
	rec = doSageMakerRequest(t, h, "DescribePipelineExecution", map[string]any{
		"PipelineExecutionArn": execArn,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "Run 1", descResp["PipelineExecutionDisplayName"])
	assert.Equal(t, "First run", descResp["PipelineExecutionDescription"])
	params := descResp["PipelineParameters"].([]any)
	assert.Len(t, params, 2)
}

// ---------------------------------------------------------------------------
// DescribeEndpoint — ProductionVariants + FailureReason (gap #9)
// ---------------------------------------------------------------------------

func TestHandler_DescribeEndpoint_FullResponse(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create endpoint config with a production variant
	doSageMakerRequest(t, h, "CreateEndpointConfig", map[string]any{
		"EndpointConfigName": "ec",
		"ProductionVariants": []any{
			map[string]any{
				"VariantName":          "main",
				"ModelName":            "my-model",
				"InitialInstanceCount": 1,
				"InstanceType":         "ml.m5.large",
				"InitialVariantWeight": 1.0,
			},
		},
	})

	// Create endpoint
	rec := doSageMakerRequest(t, h, "CreateEndpoint", map[string]any{
		"EndpointName":       "ep1",
		"EndpointConfigName": "ec",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Describe
	rec = doSageMakerRequest(t, h, "DescribeEndpoint", map[string]any{
		"EndpointName": "ep1",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "ep1", descResp["EndpointName"])
	assert.NotEmpty(t, descResp["EndpointArn"])
	// Status is Creating initially (FSM transitions async)
	assert.Contains(t, []any{"Creating", "InService"}, descResp["EndpointStatus"])

	// ProductionVariants must be populated from the endpoint config, using
	// the AWS DescribeEndpoint wire shape (Desired*/Current*, not Initial*).
	variants, ok := descResp["ProductionVariants"].([]any)
	require.True(t, ok, "ProductionVariants must be present in DescribeEndpoint response")
	require.Len(t, variants, 1)
	variant, ok := variants[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "main", variant["VariantName"])
	assert.InDelta(t, 1.0, variant["DesiredWeight"], 0.0001)
	assert.InDelta(t, float64(1), variant["DesiredInstanceCount"], 0.0001)
	assert.Nil(t, variant["CurrentWeight"])
	assert.Nil(t, variant["CurrentInstanceCount"])
}

func TestHandler_CreateEndpoint_UnknownEndpointConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateEndpoint", map[string]any{
		"EndpointName":       "ep-missing-config",
		"EndpointConfigName": "does-not-exist",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "ValidationException", errResp["__type"])

	rec = doSageMakerRequest(t, h, "DescribeEndpoint", map[string]any{
		"EndpointName": "ep-missing-config",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_DescribeEndpoint_EventuallyInService(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateEndpointConfig", map[string]any{
		"EndpointConfigName": "ec2",
		"ProductionVariants": []any{
			map[string]any{
				"VariantName":          "v1",
				"ModelName":            "m1",
				"InitialInstanceCount": 1,
				"InstanceType":         "ml.m5.large",
				"InitialVariantWeight": 1.0,
			},
		},
	})

	doSageMakerRequest(t, h, "CreateEndpoint", map[string]any{
		"EndpointName":       "ep2",
		"EndpointConfigName": "ec2",
	})

	// Wait for lifecycle
	time.Sleep(400 * time.Millisecond)

	rec := doSageMakerRequest(t, h, "DescribeEndpoint", map[string]any{
		"EndpointName": "ep2",
	})
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "InService", descResp["EndpointStatus"])

	variants, ok := descResp["ProductionVariants"].([]any)
	require.True(t, ok)
	require.Len(t, variants, 1)
	variant, ok := variants[0].(map[string]any)
	require.True(t, ok)
	// Once InService, Current* mirrors Desired*.
	assert.InDelta(t, 1.0, variant["CurrentWeight"], 0.0001)
	assert.InDelta(t, float64(1), variant["CurrentInstanceCount"], 0.0001)
}

// ---------------------------------------------------------------------------
// Snapshot/Restore preserves TransformJob, featureRecords, featureMetadata (gap #28)
// ---------------------------------------------------------------------------

func TestHandler_Persistence_TransformJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateTransformJob", map[string]any{
		"TransformJobName": "snap-transform",
		"ModelName":        "snap-model",
		"TransformInput": map[string]any{
			"DataSource": map[string]any{
				"S3DataSource": map[string]any{"S3Uri": "s3://bucket/input"},
			},
		},
		"TransformOutput":    map[string]any{"S3OutputPath": "s3://bucket/output"},
		"TransformResources": map[string]any{"InstanceType": "ml.m5.large", "InstanceCount": 1},
	})

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	h2 := sagemaker.NewHandler(sagemaker.NewInMemoryBackend("000000000000", "us-east-1"))
	require.NoError(t, h2.Restore(t.Context(), snap))

	rec := doSageMakerRequest(t, h2, "DescribeTransformJob", map[string]any{
		"TransformJobName": "snap-transform",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---------------------------------------------------------------------------
// Tag coverage extended to featureGroups, experiments, trials (gap #27)
// ---------------------------------------------------------------------------

func TestHandler_Tags_FeatureGroup(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateFeatureGroup", map[string]any{
		"FeatureGroupName":            "tagged-fg",
		"RecordIdentifierFeatureName": "id",
		"EventTimeFeatureName":        "ts",
		"Tags": []any{
			map[string]any{"Key": "env", "Value": "test"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	fgARN := createResp["FeatureGroupArn"]
	require.NotEmpty(t, fgARN)

	// ListTags should find the tag via ARN
	rec = doSageMakerRequest(t, h, "ListTags", map[string]any{
		"ResourceArn": fgARN,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var tagsResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &tagsResp))
	tags := tagsResp["Tags"].([]any)
	require.Len(t, tags, 1)
	assert.Equal(t, "env", tags[0].(map[string]any)["Key"])
}

func TestHandler_Tags_Experiment(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateExperiment", map[string]any{
		"ExperimentName": "tagged-exp",
		"Tags": []any{
			map[string]any{"Key": "project", "Value": "ml"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	expARN := createResp["ExperimentArn"]
	require.NotEmpty(t, expARN)

	rec = doSageMakerRequest(t, h, "ListTags", map[string]any{
		"ResourceArn": expARN,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var tagsResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &tagsResp))
	tags := tagsResp["Tags"].([]any)
	require.Len(t, tags, 1)
	assert.Equal(t, "project", tags[0].(map[string]any)["Key"])
}

func TestHandler_Tags_TransformJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateTransformJob", map[string]any{
		"TransformJobName": "tagged-transform",
		"ModelName":        "m1",
		"TransformInput": map[string]any{
			"DataSource": map[string]any{
				"S3DataSource": map[string]any{"S3Uri": "s3://b/i"},
			},
		},
		"TransformOutput":    map[string]any{"S3OutputPath": "s3://b/o"},
		"TransformResources": map[string]any{"InstanceType": "ml.m5.large", "InstanceCount": 1},
		"Tags": []any{
			map[string]any{"Key": "owner", "Value": "alice"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	jobARN := createResp["TransformJobArn"]
	require.NotEmpty(t, jobARN)

	rec = doSageMakerRequest(t, h, "ListTags", map[string]any{
		"ResourceArn": jobARN,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var tagsResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &tagsResp))
	tags := tagsResp["Tags"].([]any)
	require.Len(t, tags, 1)
	assert.Equal(t, "owner", tags[0].(map[string]any)["Key"])
}

// ---------------------------------------------------------------------------
// GetSupportedOperations covers new ops
// ---------------------------------------------------------------------------

func TestHandler_SupportedOps_NewOps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	required := []string{
		"CreateTransformJob",
		"DescribeTransformJob",
		"ListTransformJobs",
		"StopTransformJob",
		"UpdateFeatureGroup",
		"UpdateExperiment",
		"UpdateTrial",
		"UpdateTrialComponent",
		"ListPipelineParametersForExecution",
	}

	for _, op := range required {
		assert.Contains(t, ops, op, "missing op: %s", op)
	}
}

func TestHandler_ListPipelineParametersForExecution(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create pipeline and start execution with parameters.
	doSageMakerRequest(t, h, "CreatePipeline", map[string]any{
		"PipelineName":       "param-pipe",
		"PipelineDefinition": `{"Version":"2020-12-01","Steps":[]}`,
		"RoleArn":            "arn:aws:iam::000000000000:role/Role",
	})

	rec := doSageMakerRequest(t, h, "StartPipelineExecution", map[string]any{
		"PipelineName":                 "param-pipe",
		"PipelineExecutionDisplayName": "run-1",
		"PipelineParameters": []map[string]string{
			{"Name": "LearningRate", "Value": "0.01"},
			{"Name": "BatchSize", "Value": "32"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var startResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
	execArn := startResp["PipelineExecutionArn"]
	require.NotEmpty(t, execArn)

	// ListPipelineParametersForExecution returns the stored parameters.
	rec2 := doSageMakerRequest(t, h, "ListPipelineParametersForExecution", map[string]any{
		"PipelineExecutionArn": execArn,
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var listResp struct {
		PipelineParameters []struct {
			Name  string `json:"Name"`
			Value string `json:"Value"`
		} `json:"PipelineParameters"`
	}
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &listResp))
	assert.Len(t, listResp.PipelineParameters, 2)

	names := make(map[string]string)
	for _, p := range listResp.PipelineParameters {
		names[p.Name] = p.Value
	}
	assert.Equal(t, "0.01", names["LearningRate"])
	assert.Equal(t, "32", names["BatchSize"])
}

func TestHandler_ListPipelineParametersForExecution_Empty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreatePipeline", map[string]any{
		"PipelineName":       "empty-param-pipe",
		"PipelineDefinition": `{"Version":"2020-12-01","Steps":[]}`,
		"RoleArn":            "arn:aws:iam::000000000000:role/Role",
	})

	rec := doSageMakerRequest(t, h, "StartPipelineExecution", map[string]any{
		"PipelineName": "empty-param-pipe",
	})
	var startResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))

	rec2 := doSageMakerRequest(t, h, "ListPipelineParametersForExecution", map[string]any{
		"PipelineExecutionArn": startResp["PipelineExecutionArn"],
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var listResp struct {
		PipelineParameters []any `json:"PipelineParameters"`
	}
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &listResp))
	assert.NotNil(t, listResp.PipelineParameters)
	assert.Empty(t, listResp.PipelineParameters)
}

func TestHandler_ListPipelineParametersForExecution_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "ListPipelineParametersForExecution", map[string]any{
		"PipelineExecutionArn": "arn:aws:sagemaker:us-east-1:000000000000:pipeline/nonexistent/execution/abc",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
