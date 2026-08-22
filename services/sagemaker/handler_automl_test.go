package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// autoMLJobRequestBody returns a minimal-but-complete CreateAutoMLJob request
// body for name, supplying every member CreateAutoMLJobInput declares
// required (api_op_CreateAutoMLJob.go:13-52): AutoMLJobName, InputDataConfig,
// OutputDataConfig, RoleArn.
func autoMLJobRequestBody(name string) map[string]any {
	return map[string]any{
		"AutoMLJobName": name,
		"RoleArn":       "arn:aws:iam::000000000000:role/test",
		"InputDataConfig": []any{
			map[string]any{
				"ChannelType": "training",
				"DataSource": map[string]any{
					"S3DataSource": map[string]any{
						"S3DataType": "S3Prefix",
						"S3Uri":      "s3://bucket/train/",
					},
				},
			},
		},
		"OutputDataConfig": map[string]any{
			"S3OutputPath": "s3://bucket/output/",
		},
	}
}

func TestHandler_CreateAutoMLJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateAutoMLJob", autoMLJobRequestBody("my-job"))
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["AutoMLJobArn"], "my-job")
}

func TestHandler_CreateAutoMLJob_RequiredFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(map[string]any)
		name   string
	}{
		{name: "missing name", mutate: func(b map[string]any) { delete(b, "AutoMLJobName") }},
		{name: "missing role arn", mutate: func(b map[string]any) { delete(b, "RoleArn") }},
		{name: "missing input data config", mutate: func(b map[string]any) { delete(b, "InputDataConfig") }},
		{name: "empty input data config", mutate: func(b map[string]any) { b["InputDataConfig"] = []any{} }},
		{name: "missing output data config", mutate: func(b map[string]any) { delete(b, "OutputDataConfig") }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := autoMLJobRequestBody("req-fields")
			tt.mutate(body)

			rec := doSageMakerRequest(t, h, "CreateAutoMLJob", body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestHandler_CreateAutoMLJob_InputDataConfigRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := autoMLJobRequestBody("automl-with-input")
	body["InputDataConfig"] = []any{
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
	}

	rec := doSageMakerRequest(t, h, "CreateAutoMLJob", body)
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

func TestHandler_DescribeAutoMLJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateAutoMLJob", autoMLJobRequestBody("job-1"))

	rec := doSageMakerRequest(t, h, "DescribeAutoMLJob", map[string]any{"AutoMLJobName": "job-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "job-1", resp["AutoMLJobName"])
	assert.Equal(t, "InProgress", resp["AutoMLJobStatus"])
}

func TestHandler_DescribeAutoMLJob_ModelDeployConfigRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := autoMLJobRequestBody("job-deploy-config")
	body["ModelDeployConfig"] = map[string]any{
		"EndpointName": "my-endpoint",
	}
	doSageMakerRequest(t, h, "CreateAutoMLJob", body)

	rec := doSageMakerRequest(t, h, "DescribeAutoMLJob", map[string]any{"AutoMLJobName": "job-deploy-config"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	deployConfig, ok := resp["ModelDeployConfig"].(map[string]any)
	require.True(t, ok, "DescribeAutoMLJob must return ModelDeployConfig when set")
	assert.Equal(t, "my-endpoint", deployConfig["EndpointName"])
}

func TestHandler_StopAutoMLJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateAutoMLJob", autoMLJobRequestBody("job-stop"))
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

	doSageMakerRequest(t, h, "CreateAutoMLJob", autoMLJobRequestBody("job-a"))
	doSageMakerRequest(t, h, "CreateAutoMLJob", autoMLJobRequestBody("job-b"))

	rec := doSageMakerRequest(t, h, "ListAutoMLJobs", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items := resp["AutoMLJobSummaries"].([]any)
	assert.Len(t, items, 2)
}

func TestHandler_ListAutoMLJobs_FilterSort(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateAutoMLJob", autoMLJobRequestBody("job-alpha"))
	doSageMakerRequest(t, h, "CreateAutoMLJob", autoMLJobRequestBody("job-beta"))
	doSageMakerRequest(t, h, "StopAutoMLJob", map[string]any{"AutoMLJobName": "job-beta"})

	rec := doSageMakerRequest(t, h, "ListAutoMLJobs", map[string]any{
		"StatusEquals": "InProgress",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items := resp["AutoMLJobSummaries"].([]any)
	require.Len(t, items, 1)
	assert.Equal(t, "job-alpha", items[0].(map[string]any)["AutoMLJobName"])

	rec = doSageMakerRequest(t, h, "ListAutoMLJobs", map[string]any{
		"SortBy":    "Name",
		"SortOrder": "Descending",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items = resp["AutoMLJobSummaries"].([]any)
	require.Len(t, items, 2)
	assert.Equal(t, "job-beta", items[0].(map[string]any)["AutoMLJobName"])
	assert.Equal(t, "job-alpha", items[1].(map[string]any)["AutoMLJobName"])
}

// ---------------------------------------------------------------------------
// CodeRepository
// ---------------------------------------------------------------------------

func TestAutoMLJob_OutputDataConfigRoundtrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := autoMLJobRequestBody("automl-output-roundtrip")
	body["OutputDataConfig"] = map[string]any{
		"S3OutputPath": "s3://my-bucket/automl-output/",
	}
	body["AutoMLJobObjective"] = map[string]any{
		"MetricName": "Accuracy",
	}

	createRec := doSageMakerRequest(t, h, "CreateAutoMLJob", body)
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

	doSageMakerRequest(t, h, "CreateAutoMLJob", autoMLJobRequestBody("automl-status"))

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

	doSageMakerRequest(t, h, "CreateAutoMLJob", autoMLJobRequestBody("automl-terminal"))
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
