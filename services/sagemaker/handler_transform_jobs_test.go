package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/sagemaker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
