package sagemaker_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sagemaker"
)

// TestParity_CreateModel_PrimaryContainerAndContainersAreMutuallyExclusive verifies that
// providing both PrimaryContainer and Containers returns a 400. Real AWS rejects this
// combination with a ValidationException.
func TestParity_CreateModel_PrimaryContainerAndContainersAreMutuallyExclusive(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doSageMakerRequest(t, h, "CreateModel", map[string]any{
		"ModelName":        "dual-container-model",
		"ExecutionRoleArn": "arn:aws:iam::123456789012:role/SageMakerRole",
		"PrimaryContainer": map[string]any{
			"Image": "123456789012.dkr.ecr.us-east-1.amazonaws.com/my-image:v1",
		},
		"Containers": []map[string]any{
			{"Image": "123456789012.dkr.ecr.us-east-1.amazonaws.com/my-image:v2"},
		},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code,
		"CreateModel with both PrimaryContainer and Containers must return 400; body: %s",
		rec.Body.String())
}

// TestParity_UpdateNotebookInstance_RequiresStoppedState verifies that updating a notebook
// instance that is not in Stopped status returns 400. Real AWS returns ValidationException
// for updates on InService, Pending, Stopping, or other non-Stopped notebooks.
func TestParity_UpdateNotebookInstance_RequiresStoppedState(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a notebook instance.
	rec := doSageMakerRequest(t, h, "CreateNotebookInstance", map[string]any{
		"NotebookInstanceName": "update-state-nb",
		"InstanceType":         "ml.t2.medium",
		"RoleArn":              "arn:aws:iam::123456789012:role/SageMakerRole",
	})
	require.Equal(t, http.StatusOK, rec.Code, "CreateNotebookInstance failed: %s", rec.Body.String())

	// While still in Pending/InService state (freshly created), update must be rejected.
	rec = doSageMakerRequest(t, h, "UpdateNotebookInstance", map[string]any{
		"NotebookInstanceName": "update-state-nb",
		"InstanceType":         "ml.t3.medium",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code,
		"UpdateNotebookInstance on non-Stopped notebook must return 400; body: %s",
		rec.Body.String())
}

// TestParity_CompilationJob_InputOutputConfigRoundtrip verifies that InputConfig, OutputConfig,
// and StoppingCondition provided at CreateCompilationJob are persisted and returned by
// DescribeCompilationJob. Real AWS stores and returns these fields.
func TestParity_CompilationJob_InputOutputConfigRoundtrip(t *testing.T) {
	t.Parallel()

	b := sagemaker.NewInMemoryBackend("000000000000", "us-east-1")
	ctx := context.Background()

	_, err := b.CreateCompilationJob(ctx, "roundtrip-job", "arn:aws:iam::123456789012:role/Neo", nil)
	require.NoError(t, err)

	inputCfg := &sagemaker.CompilationInputConfig{
		S3Uri:     "s3://my-bucket/model.tar.gz",
		Framework: "TENSORFLOW",
	}
	outputCfg := &sagemaker.CompilationOutputConfig{
		S3OutputLocation: "s3://my-bucket/output/",
		TargetDevice:     "ml_c5",
	}
	sc := &sagemaker.StoppingCondition{MaxRuntimeInSeconds: 300}

	err = b.SetCompilationJobExtras(ctx, "roundtrip-job", inputCfg, outputCfg, sc)
	require.NoError(t, err)

	got, err := b.DescribeCompilationJob(ctx, "roundtrip-job")
	require.NoError(t, err)

	require.NotNil(t, got.InputConfig, "InputConfig must be persisted")
	assert.Equal(t, "s3://my-bucket/model.tar.gz", got.InputConfig.S3Uri)
	assert.Equal(t, "TENSORFLOW", got.InputConfig.Framework)

	require.NotNil(t, got.OutputConfig, "OutputConfig must be persisted")
	assert.Equal(t, "s3://my-bucket/output/", got.OutputConfig.S3OutputLocation)
	assert.Equal(t, "ml_c5", got.OutputConfig.TargetDevice)

	require.NotNil(t, got.StoppingCondition, "StoppingCondition must be persisted")
	assert.Equal(t, int32(300), got.StoppingCondition.MaxRuntimeInSeconds)
}

// TestParity_CompilationJob_HandlerCapturesInputOutputConfig verifies that the HTTP handler
// passes InputConfig and OutputConfig through to the backend on creation.
func TestParity_CompilationJob_HandlerCapturesInputOutputConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doSageMakerRequest(t, h, "CreateCompilationJob", map[string]any{
		"CompilationJobName": "handler-roundtrip-job",
		"RoleArn":            "arn:aws:iam::123456789012:role/Neo",
		"InputConfig": map[string]any{
			"S3Uri":     "s3://bucket/model.tar.gz",
			"Framework": "PYTORCH",
		},
		"OutputConfig": map[string]any{
			"S3OutputLocation": "s3://bucket/out/",
			"TargetDevice":     "jetson_nano",
		},
		"StoppingCondition": map[string]any{
			"MaxRuntimeInSeconds": 600,
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code,
		"CreateCompilationJob failed: %s", createRec.Body.String())

	descRec := doSageMakerRequest(t, h, "DescribeCompilationJob", map[string]any{
		"CompilationJobName": "handler-roundtrip-job",
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var out struct {
		InputConfig *struct {
			S3Uri     string `json:"S3Uri"`
			Framework string `json:"Framework"`
		} `json:"InputConfig"`
		OutputConfig *struct {
			S3OutputLocation string `json:"S3OutputLocation"`
			TargetDevice     string `json:"TargetDevice"`
		} `json:"OutputConfig"`
		StoppingCondition *struct {
			MaxRuntimeInSeconds int32 `json:"MaxRuntimeInSeconds"`
		} `json:"StoppingCondition"`
	}
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &out))

	require.NotNil(t, out.InputConfig, "InputConfig must be returned by DescribeCompilationJob")
	assert.Equal(t, "s3://bucket/model.tar.gz", out.InputConfig.S3Uri)
	assert.Equal(t, "PYTORCH", out.InputConfig.Framework)

	require.NotNil(t, out.OutputConfig, "OutputConfig must be returned by DescribeCompilationJob")
	assert.Equal(t, "s3://bucket/out/", out.OutputConfig.S3OutputLocation)
	assert.Equal(t, "jetson_nano", out.OutputConfig.TargetDevice)

	require.NotNil(t, out.StoppingCondition, "StoppingCondition must be returned by DescribeCompilationJob")
	assert.Equal(t, int32(600), out.StoppingCondition.MaxRuntimeInSeconds)
}

// TestParity_AutoMLJob_OutputDataConfigRoundtrip verifies that OutputDataConfig and
// AutoMLJobObjective provided at CreateAutoMLJob are persisted and returned by
// DescribeAutoMLJob. Real AWS stores and returns these fields.
func TestParity_AutoMLJob_OutputDataConfigRoundtrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doSageMakerRequest(t, h, "CreateAutoMLJob", map[string]any{
		"AutoMLJobName": "automl-output-roundtrip",
		"RoleArn":       "arn:aws:iam::123456789012:role/AutoML",
		"OutputDataConfig": map[string]any{
			"S3OutputPath": "s3://my-bucket/automl-output/",
		},
		"AutoMLJobObjective": map[string]any{
			"MetricName": "Accuracy",
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code,
		"CreateAutoMLJob failed: %s", createRec.Body.String())

	descRec := doSageMakerRequest(t, h, "DescribeAutoMLJob", map[string]any{
		"AutoMLJobName": "automl-output-roundtrip",
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var out struct {
		OutputDataConfig *struct {
			S3OutputPath string `json:"S3OutputPath"`
		} `json:"OutputDataConfig"`
		AutoMLJobObjective *struct {
			MetricName string `json:"MetricName"`
		} `json:"AutoMLJobObjective"`
	}
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &out))

	require.NotNil(t, out.OutputDataConfig, "OutputDataConfig must be returned by DescribeAutoMLJob")
	assert.Equal(t, "s3://my-bucket/automl-output/", out.OutputDataConfig.S3OutputPath)

	require.NotNil(t, out.AutoMLJobObjective, "AutoMLJobObjective must be returned by DescribeAutoMLJob")
	assert.Equal(t, "Accuracy", out.AutoMLJobObjective.MetricName)
}
