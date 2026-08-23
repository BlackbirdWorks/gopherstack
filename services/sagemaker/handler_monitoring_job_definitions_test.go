package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// minimalJobDefinitionFixture builds a structurally valid Create*JobDefinition
// request body: JobDefinitionName/RoleArn/JobResources plus the type's own
// AppSpecification/JobInput/JobOutputConfig -- the six members
// parseJobDefRequest (handler_monitoring_job_definitions.go) validates as
// required, shared by all four job definition types.
func minimalJobDefinitionFixture(name, typePrefix string) map[string]any {
	return map[string]any{
		"JobDefinitionName": name,
		"RoleArn":           "arn:aws:iam::000000000000:role/monitor",
		"JobResources":      map[string]any{"ClusterConfig": map[string]any{"InstanceCount": 1}},
		typePrefix + "AppSpecification": map[string]any{
			"ImageUri": "123456789012.dkr.ecr.us-east-1.amazonaws.com/monitor:latest",
		},
		typePrefix + "JobInput": map[string]any{
			"EndpointInput": map[string]any{"EndpointName": "my-endpoint", "LocalPath": "/opt/ml/input"},
		},
		typePrefix + "JobOutputConfig": map[string]any{
			"MonitoringOutputs": []any{},
		},
	}
}

func TestHandler_CreateDataQualityJobDefinition(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateDataQualityJobDefinition",
		minimalJobDefinitionFixture("dq-def-1", "DataQuality"))
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["JobDefinitionArn"], "dq-def-1")
}

func TestHandler_DescribeDataQualityJobDefinition(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateDataQualityJobDefinition",
		minimalJobDefinitionFixture("dq-def-2", "DataQuality"))

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

	doSageMakerRequest(t, h, "CreateDataQualityJobDefinition",
		minimalJobDefinitionFixture("dq-def-del", "DataQuality"))
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

	doSageMakerRequest(t, h, "CreateDataQualityJobDefinition",
		minimalJobDefinitionFixture("dq-dup", "DataQuality"))
	rec := doSageMakerRequest(t, h, "CreateDataQualityJobDefinition",
		minimalJobDefinitionFixture("dq-dup", "DataQuality"))
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

	rec := doSageMakerRequest(t, h, "CreateModelBiasJobDefinition",
		minimalJobDefinitionFixture("mb-def-1", "ModelBias"))
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["JobDefinitionArn"], "mb-def-1")
}

func TestHandler_DeleteModelBiasJobDefinition(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateModelBiasJobDefinition",
		minimalJobDefinitionFixture("mb-def-del", "ModelBias"))
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

	rec := doSageMakerRequest(t, h, "CreateModelQualityJobDefinition",
		minimalJobDefinitionFixture("mq-def-1", "ModelQuality"))
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["JobDefinitionArn"], "mq-def-1")
}

func TestHandler_DeleteModelQualityJobDefinition(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateModelQualityJobDefinition",
		minimalJobDefinitionFixture("mq-def-del", "ModelQuality"))
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

	rec := doSageMakerRequest(t, h, "CreateModelExplainabilityJobDefinition",
		minimalJobDefinitionFixture("me-def-1", "ModelExplainability"))
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["JobDefinitionArn"], "me-def-1")
}

func TestHandler_DeleteModelExplainabilityJobDefinition(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateModelExplainabilityJobDefinition",
		minimalJobDefinitionFixture("me-def-del", "ModelExplainability"))
	rec := doSageMakerRequest(t, h, "DeleteModelExplainabilityJobDefinition", map[string]any{
		"JobDefinitionName": "me-def-del",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestHandler_CreateDataQualityJobDefinition_RequiresAllRequiredMembers
// verifies parseJobDefRequest (handler_monitoring_job_definitions.go), shared
// by all four job definition types, rejects a request missing any of
// JobDefinitionName/RoleArn/JobResources/<Type>AppSpecification/<Type>JobInput/
// <Type>JobOutputConfig -- previously only JobDefinitionName was enforced,
// and RoleArn/JobResources/AppSpecification/JobInput/JobOutputConfig were all
// accepted absent (see this file's other Create* tests before this pass,
// which supplied only JobDefinitionName and got a 200).
func TestHandler_CreateDataQualityJobDefinition_RequiresAllRequiredMembers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		omit string
	}{
		{name: "missing role arn", omit: "RoleArn"},
		{name: "missing job resources", omit: "JobResources"},
		{name: "missing app specification", omit: "DataQualityAppSpecification"},
		{name: "missing job input", omit: "DataQualityJobInput"},
		{name: "missing job output config", omit: "DataQualityJobOutputConfig"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			body := minimalJobDefinitionFixture("dq-missing", "DataQuality")
			delete(body, tt.omit)

			rec := doSageMakerRequest(t, h, "CreateDataQualityJobDefinition", body)
			assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
		})
	}
}

// ---------------------------------------------------------------------------
// HumanTaskUI
// ---------------------------------------------------------------------------
