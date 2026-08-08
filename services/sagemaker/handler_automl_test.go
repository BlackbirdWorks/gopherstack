package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateAutoMLJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateAutoMLJob", map[string]any{
		"AutoMLJobName": "my-job",
		"RoleArn":       "arn:aws:iam::000000000000:role/test",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["AutoMLJobArn"], "my-job")
}

func TestHandler_CreateAutoMLJob_InputDataConfigRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateAutoMLJob", map[string]any{
		"AutoMLJobName": "automl-with-input",
		"RoleArn":       "arn:aws:iam::000000000000:role/test",
		"InputDataConfig": []any{
			map[string]any{
				"ChannelType":         "training",
				"TargetAttributeName": "label",
				"DataSource": map[string]any{
					"S3DataSource": map[string]any{
						"S3DataType": "S3Prefix",
						"S3Uri":      "s3://bucket/train/",
					},
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeAutoMLJob", map[string]any{
		"AutoMLJobName": "automl-with-input",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))

	channels, ok := descResp["InputDataConfig"].([]any)
	require.True(t, ok, "DescribeAutoMLJob must always emit InputDataConfig (required field)")
	require.Len(t, channels, 1)

	channel, ok := channels[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "label", channel["TargetAttributeName"])

	dataSource, ok := channel["DataSource"].(map[string]any)
	require.True(t, ok)
	s3Source, ok := dataSource["S3DataSource"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "s3://bucket/train/", s3Source["S3Uri"])
}

func TestHandler_CreateAutoMLJob_InputDataConfigDefaultsToEmptyList(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateAutoMLJob", map[string]any{
		"AutoMLJobName": "automl-no-input",
		"RoleArn":       "arn:test",
	})

	rec := doSageMakerRequest(t, h, "DescribeAutoMLJob", map[string]any{
		"AutoMLJobName": "automl-no-input",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))

	channels, ok := descResp["InputDataConfig"].([]any)
	require.True(t, ok, "InputDataConfig must always be present, even as an empty list")
	assert.Empty(t, channels)
}

func TestHandler_DescribeAutoMLJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateAutoMLJob", map[string]any{"AutoMLJobName": "job-1", "RoleArn": "arn:test"})

	rec := doSageMakerRequest(t, h, "DescribeAutoMLJob", map[string]any{"AutoMLJobName": "job-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "job-1", resp["AutoMLJobName"])
	assert.Equal(t, "InProgress", resp["AutoMLJobStatus"])
}

func TestHandler_StopAutoMLJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateAutoMLJob", map[string]any{"AutoMLJobName": "job-stop", "RoleArn": "arn:test"})
	rec := doSageMakerRequest(t, h, "StopAutoMLJob", map[string]any{"AutoMLJobName": "job-stop"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeAutoMLJob", map[string]any{"AutoMLJobName": "job-stop"})
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "Stopped", resp["AutoMLJobStatus"])
}

func TestHandler_ListAutoMLJobs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateAutoMLJob", map[string]any{"AutoMLJobName": "job-a", "RoleArn": "arn:test"})
	doSageMakerRequest(t, h, "CreateAutoMLJob", map[string]any{"AutoMLJobName": "job-b", "RoleArn": "arn:test"})

	rec := doSageMakerRequest(t, h, "ListAutoMLJobs", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items := resp["AutoMLJobSummaries"].([]any)
	assert.Len(t, items, 2)
}

// ---------------------------------------------------------------------------
// CodeRepository
// ---------------------------------------------------------------------------

func TestAutoMLJob_OutputDataConfigRoundtrip(t *testing.T) {
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

// ---------------------------------------------------------------------------
// ModelPackageGroup: dependency guard
// ---------------------------------------------------------------------------

func TestAutoMLJob_InitialStatus_InProgress(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateAutoMLJob", map[string]any{
		"AutoMLJobName": "automl-status",
		"RoleArn":       "arn:test",
	})

	rec := doSageMakerRequest(t, h, "DescribeAutoMLJob", map[string]any{
		"AutoMLJobName": "automl-status",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "InProgress", resp["AutoMLJobStatus"])
}

func TestStopAutoMLJob_Terminal_Rejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateAutoMLJob", map[string]any{
		"AutoMLJobName": "automl-terminal",
		"RoleArn":       "arn:test",
	})
	doSageMakerRequest(t, h, "StopAutoMLJob", map[string]any{
		"AutoMLJobName": "automl-terminal",
	})

	rec := doSageMakerRequest(t, h, "StopAutoMLJob", map[string]any{
		"AutoMLJobName": "automl-terminal",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// CompilationJob: initial status and terminal-state stop guard
// ---------------------------------------------------------------------------
