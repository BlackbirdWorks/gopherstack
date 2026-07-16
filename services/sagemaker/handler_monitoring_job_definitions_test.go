package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateDataQualityJobDefinition(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateDataQualityJobDefinition", map[string]any{
		"JobDefinitionName": "dq-def-1",
		"RoleArn":           "arn:aws:iam::000000000000:role/monitor",
		"JobResources":      map[string]any{"ClusterConfig": map[string]any{"InstanceCount": 1}},
		"DataQualityAppSpecification": map[string]any{
			"ImageUri": "123456789012.dkr.ecr.us-east-1.amazonaws.com/monitor:latest",
		},
		"DataQualityJobInput": map[string]any{
			"EndpointInput": map[string]any{"EndpointName": "my-endpoint", "LocalPath": "/opt/ml/input"},
		},
		"DataQualityJobOutputConfig": map[string]any{
			"MonitoringOutputs": []any{},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["JobDefinitionArn"], "dq-def-1")
}

func TestHandler_DescribeDataQualityJobDefinition(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateDataQualityJobDefinition", map[string]any{
		"JobDefinitionName": "dq-def-2",
		"RoleArn":           "arn:aws:iam::000000000000:role/monitor",
		"JobResources":      map[string]any{"ClusterConfig": map[string]any{"InstanceCount": 1}},
		"DataQualityAppSpecification": map[string]any{
			"ImageUri": "123456789012.dkr.ecr.us-east-1.amazonaws.com/monitor:latest",
		},
		"DataQualityJobInput": map[string]any{
			"EndpointInput": map[string]any{"EndpointName": "my-endpoint", "LocalPath": "/opt/ml/input"},
		},
		"DataQualityJobOutputConfig": map[string]any{
			"MonitoringOutputs": []any{},
		},
	})

	rec := doSageMakerRequest(t, h, "DescribeDataQualityJobDefinition", map[string]any{
		"JobDefinitionName": "dq-def-2",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "dq-def-2", resp["JobDefinitionName"])
	assert.Equal(t, "arn:aws:iam::000000000000:role/monitor", resp["RoleArn"])
	assert.NotNil(t, resp["DataQualityAppSpecification"])
	assert.NotNil(t, resp["DataQualityJobInput"])
	assert.NotNil(t, resp["DataQualityJobOutputConfig"])
	assert.NotNil(t, resp["JobResources"])
	assert.NotContains(t, resp, "JobDefinitionType")
}

func TestHandler_DeleteDataQualityJobDefinition(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateDataQualityJobDefinition", map[string]any{
		"JobDefinitionName": "dq-def-del",
	})
	rec := doSageMakerRequest(t, h, "DeleteDataQualityJobDefinition", map[string]any{
		"JobDefinitionName": "dq-def-del",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeDataQualityJobDefinition", map[string]any{
		"JobDefinitionName": "dq-def-del",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_CreateDataQualityJobDefinition_DuplicateReturnsResourceInUse(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateDataQualityJobDefinition", map[string]any{"JobDefinitionName": "dq-dup"})
	rec := doSageMakerRequest(t, h, "CreateDataQualityJobDefinition", map[string]any{"JobDefinitionName": "dq-dup"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ResourceInUse", resp["__type"])
}

// ---------------------------------------------------------------------------
// ModelBiasJobDefinition
// ---------------------------------------------------------------------------

func TestHandler_CreateModelBiasJobDefinition(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateModelBiasJobDefinition", map[string]any{
		"JobDefinitionName": "mb-def-1",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["JobDefinitionArn"], "mb-def-1")
}

func TestHandler_DeleteModelBiasJobDefinition(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateModelBiasJobDefinition", map[string]any{
		"JobDefinitionName": "mb-def-del",
	})
	rec := doSageMakerRequest(t, h, "DeleteModelBiasJobDefinition", map[string]any{
		"JobDefinitionName": "mb-def-del",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeModelBiasJobDefinition", map[string]any{
		"JobDefinitionName": "mb-def-del",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// ModelQualityJobDefinition
// ---------------------------------------------------------------------------

func TestHandler_CreateModelQualityJobDefinition(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateModelQualityJobDefinition", map[string]any{
		"JobDefinitionName": "mq-def-1",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["JobDefinitionArn"], "mq-def-1")
}

func TestHandler_DeleteModelQualityJobDefinition(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateModelQualityJobDefinition", map[string]any{
		"JobDefinitionName": "mq-def-del",
	})
	rec := doSageMakerRequest(t, h, "DeleteModelQualityJobDefinition", map[string]any{
		"JobDefinitionName": "mq-def-del",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---------------------------------------------------------------------------
// ModelExplainabilityJobDefinition
// ---------------------------------------------------------------------------

func TestHandler_CreateModelExplainabilityJobDefinition(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateModelExplainabilityJobDefinition", map[string]any{
		"JobDefinitionName": "me-def-1",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["JobDefinitionArn"], "me-def-1")
}

func TestHandler_DeleteModelExplainabilityJobDefinition(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateModelExplainabilityJobDefinition", map[string]any{
		"JobDefinitionName": "me-def-del",
	})
	rec := doSageMakerRequest(t, h, "DeleteModelExplainabilityJobDefinition", map[string]any{
		"JobDefinitionName": "me-def-del",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---------------------------------------------------------------------------
// HumanTaskUI
// ---------------------------------------------------------------------------
