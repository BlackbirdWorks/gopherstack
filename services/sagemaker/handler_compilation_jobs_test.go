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

func TestHandler_CreateCompilationJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateCompilationJob", map[string]any{
		"CompilationJobName": "my-compile",
		"RoleArn":            "arn:test",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["CompilationJobArn"], "my-compile")
}

func TestHandler_DescribeCompilationJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(
		t,
		h,
		"CreateCompilationJob",
		map[string]any{"CompilationJobName": "cj-1", "RoleArn": "arn:test"},
	)

	rec := doSageMakerRequest(t, h, "DescribeCompilationJob", map[string]any{"CompilationJobName": "cj-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "cj-1", resp["CompilationJobName"])
	assert.Equal(t, "INPROGRESS", resp["CompilationJobStatus"])
}

func TestHandler_StopCompilationJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(
		t,
		h,
		"CreateCompilationJob",
		map[string]any{"CompilationJobName": "cj-stop", "RoleArn": "arn:test"},
	)
	rec := doSageMakerRequest(t, h, "StopCompilationJob", map[string]any{"CompilationJobName": "cj-stop"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeCompilationJob", map[string]any{"CompilationJobName": "cj-stop"})
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "STOPPED", resp["CompilationJobStatus"])
}

func TestHandler_ListCompilationJobs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(
		t,
		h,
		"CreateCompilationJob",
		map[string]any{"CompilationJobName": "cj-a", "RoleArn": "arn:test"},
	)
	doSageMakerRequest(
		t,
		h,
		"CreateCompilationJob",
		map[string]any{"CompilationJobName": "cj-b", "RoleArn": "arn:test"},
	)

	rec := doSageMakerRequest(t, h, "ListCompilationJobs", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items := resp["CompilationJobSummaries"].([]any)
	assert.Len(t, items, 2)
}

// ---------------------------------------------------------------------------
// MonitoringSchedule
// ---------------------------------------------------------------------------

func TestCompilationJob_InputOutputConfigRoundtrip(t *testing.T) {
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

// TestCompilationJob_HandlerCapturesInputOutputConfig verifies that the HTTP handler
// passes InputConfig and OutputConfig through to the backend on creation.

func TestCompilationJob_HandlerCapturesInputOutputConfig(t *testing.T) {
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

// TestAutoMLJob_OutputDataConfigRoundtrip verifies that OutputDataConfig and
// AutoMLJobObjective provided at CreateAutoMLJob are persisted and returned by
// DescribeAutoMLJob. Real AWS stores and returns these fields.

func TestCompilationJob_InitialStatus_InProgress(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateCompilationJob", map[string]any{
		"CompilationJobName": "compile-status",
		"RoleArn":            "arn:test",
	})

	rec := doSageMakerRequest(t, h, "DescribeCompilationJob", map[string]any{
		"CompilationJobName": "compile-status",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "INPROGRESS", resp["CompilationJobStatus"])
}

func TestStopCompilationJob_Terminal_Rejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateCompilationJob", map[string]any{
		"CompilationJobName": "compile-terminal",
		"RoleArn":            "arn:test",
	})
	doSageMakerRequest(t, h, "StopCompilationJob", map[string]any{
		"CompilationJobName": "compile-terminal",
	})

	rec := doSageMakerRequest(t, h, "StopCompilationJob", map[string]any{
		"CompilationJobName": "compile-terminal",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// Image: version guard on delete
// ---------------------------------------------------------------------------
