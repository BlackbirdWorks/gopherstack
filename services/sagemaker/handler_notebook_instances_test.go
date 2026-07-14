package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_NotebookInstanceLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create notebook instance.
	recCreate := doSageMakerRequest(t, h, "CreateNotebookInstance", map[string]any{
		"NotebookInstanceName": "my-notebook",
		"InstanceType":         "ml.t2.medium",
		"RoleArn":              "arn:aws:iam::000000000000:role/notebook-role",
	})
	assert.Equal(t, http.StatusOK, recCreate.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(recCreate.Body.Bytes(), &createOut))
	assert.NotEmpty(t, createOut["NotebookInstanceArn"])

	// Describe.
	recDesc := doSageMakerRequest(t, h, "DescribeNotebookInstance", map[string]any{
		"NotebookInstanceName": "my-notebook",
	})
	assert.Equal(t, http.StatusOK, recDesc.Code)

	// List.
	recList := doSageMakerRequest(t, h, "ListNotebookInstances", map[string]any{})
	assert.Equal(t, http.StatusOK, recList.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &listOut))
	assert.Len(t, listOut["NotebookInstances"].([]any), 1)

	// Start.
	recStart := doSageMakerRequest(t, h, "StartNotebookInstance", map[string]any{
		"NotebookInstanceName": "my-notebook",
	})
	assert.Equal(t, http.StatusOK, recStart.Code)

	// Stop before update: real AWS requires Stopped state to update a notebook instance.
	recStop := doSageMakerRequest(t, h, "StopNotebookInstance", map[string]any{
		"NotebookInstanceName": "my-notebook",
	})
	assert.Equal(t, http.StatusOK, recStop.Code)

	// Update (notebook is now Stopped).
	recUpdate := doSageMakerRequest(t, h, "UpdateNotebookInstance", map[string]any{
		"NotebookInstanceName": "my-notebook",
		"InstanceType":         "ml.t3.medium",
	})
	assert.Equal(t, http.StatusOK, recUpdate.Code)

	// CreatePresignedNotebookInstanceUrl.
	recURL := doSageMakerRequest(t, h, "CreatePresignedNotebookInstanceUrl", map[string]any{
		"NotebookInstanceName": "my-notebook",
	})
	assert.Equal(t, http.StatusOK, recURL.Code)

	// Delete.
	recDelete := doSageMakerRequest(t, h, "DeleteNotebookInstance", map[string]any{
		"NotebookInstanceName": "my-notebook",
	})
	assert.Equal(t, http.StatusOK, recDelete.Code)
}

func TestHandler_NotebookInstance_EventuallyInService(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateNotebookInstance", map[string]any{
		"NotebookInstanceName": "async-notebook",
		"InstanceType":         "ml.t2.medium",
		"RoleArn":              "arn:aws:iam::000000000000:role/notebook-role",
	})

	// Wait for async status transition.
	time.Sleep(300 * time.Millisecond)

	recDesc := doSageMakerRequest(t, h, "DescribeNotebookInstance", map[string]any{
		"NotebookInstanceName": "async-notebook",
	})
	assert.Equal(t, http.StatusOK, recDesc.Code)

	var descOut map[string]any
	require.NoError(t, json.Unmarshal(recDesc.Body.Bytes(), &descOut))
	assert.NotEmpty(t, descOut["NotebookInstanceStatus"])
}
