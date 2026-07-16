package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_ModelCardExportJob_Lifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateModelCard", map[string]any{
		"ModelCardName": "my-card",
	})

	createRec := doSageMakerRequest(t, h, "CreateModelCardExportJob", map[string]any{
		"ModelCardExportJobName": "export-1",
		"ModelCardName":          "my-card",
		"OutputConfig":           map[string]any{"S3OutputPath": "s3://bucket/exports"},
	})
	assert.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	jobArn, _ := createResp["ModelCardExportJobArn"].(string)
	require.NotEmpty(t, jobArn)

	describeRec := doSageMakerRequest(t, h, "DescribeModelCardExportJob", map[string]any{
		"ModelCardExportJobArn": jobArn,
	})
	assert.Equal(t, http.StatusOK, describeRec.Code)

	var describeResp map[string]any
	require.NoError(t, json.Unmarshal(describeRec.Body.Bytes(), &describeResp))
	assert.Equal(t, "Completed", describeResp["Status"])
	assert.Equal(t, "export-1", describeResp["ModelCardExportJobName"])
	assert.Equal(t, "my-card", describeResp["ModelCardName"])

	listRec := doSageMakerRequest(t, h, "ListModelCardExportJobs", map[string]any{
		"ModelCardName": "my-card",
	})
	assert.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	summaries, ok := listResp["ModelCardExportJobSummaries"].([]any)
	require.True(t, ok)
	assert.Len(t, summaries, 1)
}

func TestHandler_CreateModelCardExportJob_ModelCardNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateModelCardExportJob", map[string]any{
		"ModelCardExportJobName": "export-1",
		"ModelCardName":          "does-not-exist",
		"OutputConfig":           map[string]any{"S3OutputPath": "s3://bucket/exports"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_DescribeModelCardExportJob_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "DescribeModelCardExportJob", map[string]any{
		"ModelCardExportJobArn": "arn:aws:sagemaker:us-east-1:000000000000:model-card/x/export-job/y",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
