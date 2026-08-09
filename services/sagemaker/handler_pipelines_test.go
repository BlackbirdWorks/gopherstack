package sagemaker_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sagemakersdk "github.com/aws/aws-sdk-go-v2/service/sagemaker"
	smtypes "github.com/aws/aws-sdk-go-v2/service/sagemaker/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sagemaker"
)

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

// ---------------------------------------------------------------------------
// CreatePipeline/UpdatePipeline: PipelineDefinitionS3Location rejection
// (gopherstack-i359) — honouring it needs a real cross-service S3 GetObject,
// out of scope here, so it is rejected explicitly instead of accepted and
// silently dropped (leaving a client's pipeline created with an empty
// PipelineDefinition and no error).
// ---------------------------------------------------------------------------

func TestHandler_CreatePipeline_S3Location_Rejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreatePipeline", map[string]any{
		"PipelineName": "s3-def-pipeline",
		"RoleArn":      "arn:aws:iam::000000000000:role/Role",
		"PipelineDefinitionS3Location": map[string]any{
			"Bucket":    "my-bucket",
			"ObjectKey": "defs/pipeline.json",
		},
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "ValidationException", body["__type"])

	rec = doSageMakerRequest(t, h, "DescribePipeline", map[string]any{"PipelineName": "s3-def-pipeline"})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "the pipeline must not have been created")
}

func TestHandler_UpdatePipeline_S3Location_Rejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreatePipeline", map[string]any{
		"PipelineName": "s3-def-pipeline-2",
		"RoleArn":      "arn:aws:iam::000000000000:role/Role",
	})

	rec := doSageMakerRequest(t, h, "UpdatePipeline", map[string]any{
		"PipelineName": "s3-def-pipeline-2",
		"PipelineDefinitionS3Location": map[string]any{
			"Bucket":    "my-bucket",
			"ObjectKey": "defs/pipeline.json",
		},
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "ValidationException", body["__type"])
}

// TestHandler_CreatePipeline_S3Location_Rejected_RealClient confirms the
// rejection fires against the real SDK's wire encoding of
// PipelineDefinitionS3Location (types/types.go:17313, sagemaker@v1.263.2),
// not just this test file's own hand-built JSON.
func TestHandler_CreatePipeline_S3Location_Rejected_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	_, err := client.CreatePipeline(t.Context(), &sagemakersdk.CreatePipelineInput{
		PipelineName: aws.String("s3-def-real"),
		RoleArn:      aws.String("arn:aws:iam::000000000000:role/Role"),
		PipelineDefinitionS3Location: &smtypes.PipelineDefinitionS3Location{
			Bucket:    aws.String("my-bucket"),
			ObjectKey: aws.String("defs/pipeline.json"),
		},
	})
	require.Error(t, err)
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

func TestHandler_StartPipelineExecution_ParallelismAndSelectiveExecution(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreatePipeline", map[string]any{
		"PipelineName": "sel-pipeline",
		"RoleArn":      "arn:aws:iam::000000000000:role/Role",
	})

	rec := doSageMakerRequest(t, h, "StartPipelineExecution", map[string]any{
		"PipelineName": "sel-pipeline",
		"ParallelismConfiguration": map[string]any{
			"MaxParallelExecutionSteps": 3,
		},
		"SelectiveExecutionConfig": map[string]any{
			"SourcePipelineExecutionArn": "arn:aws:sagemaker:us-east-1:000000000000:pipeline/sel-pipeline/execution/prior",
			"SelectedSteps": []any{
				map[string]any{"StepName": "TrainStep"},
			},
		},
		"PipelineVersionId": 2,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var startResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
	execArn := startResp["PipelineExecutionArn"]
	require.NotEmpty(t, execArn)

	rec = doSageMakerRequest(t, h, "DescribePipelineExecution", map[string]any{
		"PipelineExecutionArn": execArn,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))

	parallelism, ok := descResp["ParallelismConfiguration"].(map[string]any)
	require.True(t, ok, "ParallelismConfiguration must be present in DescribePipelineExecution")
	assert.InEpsilon(t, float64(3), parallelism["MaxParallelExecutionSteps"], 0)

	sec, ok := descResp["SelectiveExecutionConfig"].(map[string]any)
	require.True(t, ok, "SelectiveExecutionConfig must be present in DescribePipelineExecution")
	assert.Equal(t,
		"arn:aws:sagemaker:us-east-1:000000000000:pipeline/sel-pipeline/execution/prior",
		sec["SourcePipelineExecutionArn"],
	)

	assert.InEpsilon(t, float64(2), descResp["PipelineVersionId"], 0)
}

// ---------------------------------------------------------------------------
// DescribeEndpoint — ProductionVariants + FailureReason (gap #9)
// ---------------------------------------------------------------------------

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

func TestHandler_PipelineLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create pipeline.
	recCreate := doSageMakerRequest(t, h, "CreatePipeline", map[string]any{
		"PipelineName":       "my-pipeline",
		"PipelineDefinition": `{"Version":"2020-12-01","Steps":[]}`,
	})
	assert.Equal(t, http.StatusOK, recCreate.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(recCreate.Body.Bytes(), &createOut))
	assert.NotEmpty(t, createOut["PipelineArn"])

	// Describe pipeline.
	recDesc := doSageMakerRequest(
		t,
		h,
		"DescribePipeline",
		map[string]any{"PipelineName": "my-pipeline"},
	)
	assert.Equal(t, http.StatusOK, recDesc.Code)

	// List pipelines.
	recList := doSageMakerRequest(t, h, "ListPipelines", map[string]any{})
	assert.Equal(t, http.StatusOK, recList.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &listOut))
	assert.Len(t, listOut["PipelineSummaries"].([]any), 1)

	// Update pipeline.
	recUpdate := doSageMakerRequest(t, h, "UpdatePipeline", map[string]any{
		"PipelineName":       "my-pipeline",
		"PipelineDefinition": `{"Version":"2020-12-01","Steps":[{"Name":"step1"}]}`,
	})
	assert.Equal(t, http.StatusOK, recUpdate.Code)

	// Start pipeline execution.
	recExec := doSageMakerRequest(t, h, "StartPipelineExecution", map[string]any{
		"PipelineName": "my-pipeline",
	})
	assert.Equal(t, http.StatusOK, recExec.Code)

	var execOut map[string]any
	require.NoError(t, json.Unmarshal(recExec.Body.Bytes(), &execOut))
	execArn := execOut["PipelineExecutionArn"].(string)
	assert.NotEmpty(t, execArn)

	// Describe pipeline execution.
	recDescExec := doSageMakerRequest(t, h, "DescribePipelineExecution", map[string]any{
		"PipelineExecutionArn": execArn,
	})
	assert.Equal(t, http.StatusOK, recDescExec.Code)

	// List pipeline executions.
	recListExec := doSageMakerRequest(t, h, "ListPipelineExecutions", map[string]any{
		"PipelineName": "my-pipeline",
	})
	assert.Equal(t, http.StatusOK, recListExec.Code)

	// Delete pipeline.
	recDelete := doSageMakerRequest(
		t,
		h,
		"DeletePipeline",
		map[string]any{"PipelineName": "my-pipeline"},
	)
	assert.Equal(t, http.StatusOK, recDelete.Code)
}

func TestHandler_Pipeline_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, op := range []string{"DescribePipeline", "UpdatePipeline", "DeletePipeline"} {
		t.Run(op, func(t *testing.T) {
			t.Parallel()

			rec := doSageMakerRequest(t, h, op, map[string]any{"PipelineName": "nonexistent"})
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestHandler_Pipeline_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{"PipelineName": "dup-pipeline"}
	rec := doSageMakerRequest(t, h, "CreatePipeline", body)
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doSageMakerRequest(t, h, "CreatePipeline", body)
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

// TestHandler_Pipeline_Duplicate_RealClient asserts the wire error type is
// ConflictException -- CreatePipeline's documented error (botocore
// sagemaker/2017-07-24@1.43.56 service-2.json), not the generic
// ResourceInUse gopherstack-kbxx found this mapped to.
func TestHandler_Pipeline_Duplicate_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	in := &sagemakersdk.CreatePipelineInput{
		PipelineName: aws.String("dup-pipeline-real"),
		RoleArn:      aws.String("arn:aws:iam::000000000000:role/SageMakerRole"),
	}

	_, err := client.CreatePipeline(t.Context(), in)
	require.NoError(t, err)

	_, err = client.CreatePipeline(t.Context(), in)
	require.Error(t, err)

	var conflict *smtypes.ConflictException
	require.ErrorAs(t, err, &conflict)
}

// ---------------------------------------------------------------------------
// Pipeline execution step operations
// ---------------------------------------------------------------------------

func TestHandler_PipelineExecutionSteps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create and start pipeline.
	doSageMakerRequest(t, h, "CreatePipeline", map[string]any{"PipelineName": "step-pipeline"})
	recExec := doSageMakerRequest(t, h, "StartPipelineExecution", map[string]any{
		"PipelineName": "step-pipeline",
	})
	require.Equal(t, http.StatusOK, recExec.Code)

	var execOut map[string]any
	require.NoError(t, json.Unmarshal(recExec.Body.Bytes(), &execOut))
	execArn := execOut["PipelineExecutionArn"].(string)

	// ListPipelineExecutionSteps.
	recList := doSageMakerRequest(t, h, "ListPipelineExecutionSteps", map[string]any{
		"PipelineExecutionArn": execArn,
	})
	assert.Equal(t, http.StatusOK, recList.Code)

	// SendPipelineExecutionStepSuccess.
	recSuccess := doSageMakerRequest(t, h, "SendPipelineExecutionStepSuccess", map[string]any{
		"CallbackToken": execArn,
	})
	assert.Equal(t, http.StatusOK, recSuccess.Code)

	// SendPipelineExecutionStepFailure.
	recFail := doSageMakerRequest(t, h, "SendPipelineExecutionStepFailure", map[string]any{
		"CallbackToken": execArn,
		"FailureReason": "test failure",
	})
	assert.Equal(t, http.StatusOK, recFail.Code)

	// RetryPipelineExecution.
	recRetry := doSageMakerRequest(t, h, "RetryPipelineExecution", map[string]any{
		"PipelineExecutionArn": execArn,
	})
	assert.Equal(t, http.StatusOK, recRetry.Code)

	// StopPipelineExecution.
	recStop := doSageMakerRequest(t, h, "StopPipelineExecution", map[string]any{
		"PipelineExecutionArn": execArn,
	})
	assert.Equal(t, http.StatusOK, recStop.Code)
}

func TestBackend_PipelineOps_Direct(t *testing.T) {
	t.Parallel()

	b := sagemaker.NewInMemoryBackend("000000000000", "us-east-1")

	// Create and start a pipeline.
	_, err := b.CreatePipeline(context.Background(), "direct-pipeline", `{"Version":"2020-12-01"}`, "", nil)
	require.NoError(t, err)

	exec, err := b.StartPipelineExecution(context.Background(), "direct-pipeline")
	require.NoError(t, err)
	execArn := exec.PipelineExecutionArn

	// ListPipelineExecutionSteps.
	steps, _ := b.ListPipelineExecutionSteps(context.Background(), execArn, "")
	assert.NotNil(t, steps)

	// SendPipelineExecutionStepSuccess.
	err = b.SendPipelineExecutionStepSuccess(context.Background(), execArn, "step1")
	require.NoError(t, err)

	// SendPipelineExecutionStepFailure.
	err = b.SendPipelineExecutionStepFailure(context.Background(), execArn, "step2", "out of memory")
	require.NoError(t, err)

	// RetryPipelineExecution.
	retried, err := b.RetryPipelineExecution(context.Background(), execArn)
	require.NoError(t, err)
	assert.NotEmpty(t, retried.PipelineExecutionArn)

	// StopPipelineExecution.
	stopped, err := b.StopPipelineExecution(context.Background(), execArn)
	require.NoError(t, err)
	assert.NotEmpty(t, stopped.PipelineExecutionArn)
}

func TestBackend_PipelineOps_NotFound(t *testing.T) {
	t.Parallel()

	b := sagemaker.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.RetryPipelineExecution(context.Background(), "nonexistent-exec-arn")
	require.Error(t, err)

	_, err = b.StopPipelineExecution(context.Background(), "nonexistent-exec-arn")
	require.Error(t, err)
}
