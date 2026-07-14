package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// DataQualityJobDefinition
//
// NOTE: Create/Describe/Delete*JobDefinition all key on the wire field
// "JobDefinitionName" — NOT a type-prefixed name like
// "DataQualityJobDefinitionName". Confirmed against aws-sdk-go-v2's generated
// sagemaker serializers/deserializers (awsAwsjson11_serializeOpDocumentCreate
// DataQualityJobDefinitionInput etc. all emit/read "JobDefinitionName"). The
// request bodies below were fixed from the type-prefixed (incorrect) name.
// ---------------------------------------------------------------------------

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

func TestHandler_CreateHumanTaskUI(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateHumanTaskUi", map[string]any{
		"HumanTaskUiName": "my-ui",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["HumanTaskUiArn"], "my-ui")
}

func TestHandler_DescribeHumanTaskUI(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateHumanTaskUi", map[string]any{"HumanTaskUiName": "ui-1"})
	rec := doSageMakerRequest(t, h, "DescribeHumanTaskUi", map[string]any{"HumanTaskUiName": "ui-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ui-1", resp["HumanTaskUiName"])
}

func TestHandler_DeleteHumanTaskUI(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateHumanTaskUi", map[string]any{"HumanTaskUiName": "ui-del"})
	rec := doSageMakerRequest(t, h, "DeleteHumanTaskUi", map[string]any{"HumanTaskUiName": "ui-del"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeHumanTaskUi", map[string]any{"HumanTaskUiName": "ui-del"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// Workforce
// ---------------------------------------------------------------------------

func TestHandler_CreateWorkforce(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateWorkforce", map[string]any{
		"WorkforceName": "my-workforce",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["WorkforceArn"], "my-workforce")
}

func TestHandler_DescribeWorkforce(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateWorkforce", map[string]any{"WorkforceName": "wf-1"})
	rec := doSageMakerRequest(t, h, "DescribeWorkforce", map[string]any{"WorkforceName": "wf-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotNil(t, resp["Workforce"])
}

func TestHandler_UpdateWorkforce(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateWorkforce", map[string]any{"WorkforceName": "wf-upd"})
	rec := doSageMakerRequest(t, h, "UpdateWorkforce", map[string]any{"WorkforceName": "wf-upd"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotNil(t, resp["Workforce"])
}

// ---------------------------------------------------------------------------
// FlowDefinition
// ---------------------------------------------------------------------------

func TestHandler_CreateFlowDefinition(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateFlowDefinition", map[string]any{
		"FlowDefinitionName": "my-flow",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["FlowDefinitionArn"], "my-flow")
}

func TestHandler_DescribeFlowDefinition(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateFlowDefinition", map[string]any{"FlowDefinitionName": "flow-1"})
	rec := doSageMakerRequest(t, h, "DescribeFlowDefinition", map[string]any{"FlowDefinitionName": "flow-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "flow-1", resp["FlowDefinitionName"])
}

func TestHandler_DeleteFlowDefinition(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateFlowDefinition", map[string]any{"FlowDefinitionName": "flow-del"})
	rec := doSageMakerRequest(t, h, "DeleteFlowDefinition", map[string]any{"FlowDefinitionName": "flow-del"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeFlowDefinition", map[string]any{"FlowDefinitionName": "flow-del"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// AppImageConfig
// ---------------------------------------------------------------------------

func TestHandler_CreateAppImageConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateAppImageConfig", map[string]any{
		"AppImageConfigName": "my-config",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["AppImageConfigArn"], "my-config")
}

func TestHandler_DescribeAppImageConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateAppImageConfig", map[string]any{"AppImageConfigName": "aic-1"})
	rec := doSageMakerRequest(t, h, "DescribeAppImageConfig", map[string]any{"AppImageConfigName": "aic-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "aic-1", resp["AppImageConfigName"])
}

func TestHandler_DeleteAppImageConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateAppImageConfig", map[string]any{"AppImageConfigName": "aic-del"})
	rec := doSageMakerRequest(t, h, "DeleteAppImageConfig", map[string]any{"AppImageConfigName": "aic-del"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeAppImageConfig", map[string]any{"AppImageConfigName": "aic-del"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_UpdateAppImageConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateAppImageConfig", map[string]any{"AppImageConfigName": "aic-upd"})
	rec := doSageMakerRequest(t, h, "UpdateAppImageConfig", map[string]any{"AppImageConfigName": "aic-upd"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["AppImageConfigArn"], "aic-upd")
}

// ---------------------------------------------------------------------------
// InferenceExperiment
// ---------------------------------------------------------------------------

func TestHandler_CreateInferenceExperiment(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateInferenceExperiment", map[string]any{
		"Name": "my-exp",
		"Type": "ShadowMode",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["InferenceExperimentArn"], "my-exp")
}

func TestHandler_DescribeInferenceExperiment(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateInferenceExperiment", map[string]any{"Name": "exp-1"})
	rec := doSageMakerRequest(t, h, "DescribeInferenceExperiment", map[string]any{"Name": "exp-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "exp-1", resp["Name"])
}

func TestHandler_StopInferenceExperiment(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateInferenceExperiment", map[string]any{"Name": "exp-stop"})
	rec := doSageMakerRequest(t, h, "StopInferenceExperiment", map[string]any{"Name": "exp-stop"})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_DeleteInferenceExperiment(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateInferenceExperiment", map[string]any{"Name": "exp-del"})
	rec := doSageMakerRequest(t, h, "DeleteInferenceExperiment", map[string]any{"Name": "exp-del"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeInferenceExperiment", map[string]any{"Name": "exp-del"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// MlflowTrackingServer
// ---------------------------------------------------------------------------

func TestHandler_CreateMlflowTrackingServer(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateMlflowTrackingServer", map[string]any{
		"TrackingServerName": "my-server",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["TrackingServerArn"], "my-server")
}

func TestHandler_DescribeMlflowTrackingServer(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateMlflowTrackingServer", map[string]any{"TrackingServerName": "ts-1"})
	rec := doSageMakerRequest(t, h, "DescribeMlflowTrackingServer", map[string]any{"TrackingServerName": "ts-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ts-1", resp["TrackingServerName"])
}

func TestHandler_StartStopMlflowTrackingServer(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateMlflowTrackingServer", map[string]any{"TrackingServerName": "ts-ss"})

	rec := doSageMakerRequest(t, h, "StartMlflowTrackingServer", map[string]any{"TrackingServerName": "ts-ss"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "StopMlflowTrackingServer", map[string]any{"TrackingServerName": "ts-ss"})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_DeleteMlflowTrackingServer(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateMlflowTrackingServer", map[string]any{"TrackingServerName": "ts-del"})
	rec := doSageMakerRequest(t, h, "DeleteMlflowTrackingServer", map[string]any{"TrackingServerName": "ts-del"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeMlflowTrackingServer", map[string]any{"TrackingServerName": "ts-del"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// ModelCard
// ---------------------------------------------------------------------------

func TestHandler_CreateModelCard(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateModelCard", map[string]any{
		"ModelCardName": "my-card",
		"Content":       "{}",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["ModelCardArn"], "my-card")
}

func TestHandler_DescribeModelCard(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateModelCard", map[string]any{"ModelCardName": "card-1"})
	rec := doSageMakerRequest(t, h, "DescribeModelCard", map[string]any{"ModelCardName": "card-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "card-1", resp["ModelCardName"])
}

func TestHandler_UpdateModelCard(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateModelCard", map[string]any{"ModelCardName": "card-upd"})
	rec := doSageMakerRequest(t, h, "UpdateModelCard", map[string]any{
		"ModelCardName": "card-upd",
		"Content":       "{\"updated\": true}",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify version incremented
	rec = doSageMakerRequest(t, h, "DescribeModelCard", map[string]any{"ModelCardName": "card-upd"})
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.InDelta(t, float64(2), resp["ModelCardVersion"], 0)
}

func TestHandler_DeleteModelCard(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateModelCard", map[string]any{"ModelCardName": "card-del"})
	rec := doSageMakerRequest(t, h, "DeleteModelCard", map[string]any{"ModelCardName": "card-del"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeModelCard", map[string]any{"ModelCardName": "card-del"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// OptimizationJob
// ---------------------------------------------------------------------------

func TestHandler_CreateOptimizationJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateOptimizationJob", map[string]any{
		"OptimizationJobName": "my-opt-job",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["OptimizationJobArn"], "my-opt-job")
}

func TestHandler_DescribeOptimizationJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateOptimizationJob", map[string]any{"OptimizationJobName": "opt-1"})
	rec := doSageMakerRequest(t, h, "DescribeOptimizationJob", map[string]any{"OptimizationJobName": "opt-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "opt-1", resp["OptimizationJobName"])
}

func TestHandler_StopOptimizationJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateOptimizationJob", map[string]any{"OptimizationJobName": "opt-stop"})
	rec := doSageMakerRequest(t, h, "StopOptimizationJob", map[string]any{"OptimizationJobName": "opt-stop"})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_DeleteOptimizationJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateOptimizationJob", map[string]any{"OptimizationJobName": "opt-del"})
	rec := doSageMakerRequest(t, h, "DeleteOptimizationJob", map[string]any{"OptimizationJobName": "opt-del"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeOptimizationJob", map[string]any{"OptimizationJobName": "opt-del"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// StudioLifecycleConfig
// ---------------------------------------------------------------------------

func TestHandler_CreateStudioLifecycleConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateStudioLifecycleConfig", map[string]any{
		"StudioLifecycleConfigName":    "my-lc",
		"StudioLifecycleConfigAppType": "JupyterServer",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["StudioLifecycleConfigArn"], "my-lc")
}

func TestHandler_DescribeStudioLifecycleConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateStudioLifecycleConfig", map[string]any{
		"StudioLifecycleConfigName": "lc-1",
	})
	rec := doSageMakerRequest(t, h, "DescribeStudioLifecycleConfig", map[string]any{
		"StudioLifecycleConfigName": "lc-1",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "lc-1", resp["StudioLifecycleConfigName"])
}

func TestHandler_DeleteStudioLifecycleConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateStudioLifecycleConfig", map[string]any{
		"StudioLifecycleConfigName": "lc-del",
	})
	rec := doSageMakerRequest(t, h, "DeleteStudioLifecycleConfig", map[string]any{
		"StudioLifecycleConfigName": "lc-del",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeStudioLifecycleConfig", map[string]any{
		"StudioLifecycleConfigName": "lc-del",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// PartnerApp
// ---------------------------------------------------------------------------

func TestHandler_CreatePartnerApp(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreatePartnerApp", map[string]any{
		"Name": "my-app",
		"Type": "custom",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["Arn"], "my-app")
}

func TestHandler_DescribePartnerApp(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doSageMakerRequest(t, h, "CreatePartnerApp", map[string]any{
		"Name": "app-1",
	})
	var createResp map[string]string
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))

	rec := doSageMakerRequest(t, h, "DescribePartnerApp", map[string]any{"Arn": createResp["Arn"]})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "app-1", resp["Name"])
}

func TestHandler_DeletePartnerApp(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doSageMakerRequest(t, h, "CreatePartnerApp", map[string]any{"Name": "app-del"})
	var createResp map[string]string
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))

	rec := doSageMakerRequest(t, h, "DeletePartnerApp", map[string]any{"Arn": createResp["Arn"]})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribePartnerApp", map[string]any{"Arn": createResp["Arn"]})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// TrainingPlan
// ---------------------------------------------------------------------------

func TestHandler_CreateTrainingPlan(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateTrainingPlan", map[string]any{
		"TrainingPlanName": "my-plan",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["TrainingPlanArn"], "my-plan")
}

func TestHandler_DescribeTrainingPlan(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateTrainingPlan", map[string]any{"TrainingPlanName": "plan-1"})
	rec := doSageMakerRequest(t, h, "DescribeTrainingPlan", map[string]any{"TrainingPlanName": "plan-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "plan-1", resp["TrainingPlanName"])
}

// ---------------------------------------------------------------------------
// MlflowTrackingServer — CreatePresignedMlflowTrackingServerUrl
// ---------------------------------------------------------------------------

func TestHandler_CreatePresignedMlflowTrackingServerUrl(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateMlflowTrackingServer", map[string]any{
		"TrackingServerName": "ts-1",
	})

	rec := doSageMakerRequest(t, h, "CreatePresignedMlflowTrackingServerUrl", map[string]any{
		"TrackingServerName": "ts-1",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["AuthorizedUrl"], "ts-1")
}

func TestHandler_CreatePresignedMlflowTrackingServerUrl_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreatePresignedMlflowTrackingServerUrl", map[string]any{
		"TrackingServerName": "missing",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// MlflowApp
// ---------------------------------------------------------------------------

func TestHandler_MlflowApp_Lifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doSageMakerRequest(t, h, "CreateMlflowApp", map[string]any{
		"Name":             "app-1",
		"ArtifactStoreUri": "s3://bucket/path",
		"RoleArn":          "arn:aws:iam::000000000000:role/mlflow",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]string
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	appARN := createResp["Arn"]
	require.Contains(t, appARN, "mlflow-app/app-1")

	describeRec := doSageMakerRequest(t, h, "DescribeMlflowApp", map[string]any{"Arn": appARN})
	assert.Equal(t, http.StatusOK, describeRec.Code)

	var describeResp map[string]any
	require.NoError(t, json.Unmarshal(describeRec.Body.Bytes(), &describeResp))
	assert.Equal(t, "app-1", describeResp["Name"])
	assert.Equal(t, "Created", describeResp["Status"])
	assert.Equal(t, "s3://bucket/path", describeResp["ArtifactStoreUri"])

	updateRec := doSageMakerRequest(t, h, "UpdateMlflowApp", map[string]any{
		"Arn":              appARN,
		"ArtifactStoreUri": "s3://bucket/new-path",
	})
	assert.Equal(t, http.StatusOK, updateRec.Code)

	describeRec = doSageMakerRequest(t, h, "DescribeMlflowApp", map[string]any{"Arn": appARN})
	require.NoError(t, json.Unmarshal(describeRec.Body.Bytes(), &describeResp))
	assert.Equal(t, "s3://bucket/new-path", describeResp["ArtifactStoreUri"])

	presignRec := doSageMakerRequest(t, h, "CreatePresignedMlflowAppUrl", map[string]any{"Arn": appARN})
	assert.Equal(t, http.StatusOK, presignRec.Code)

	var presignResp map[string]string
	require.NoError(t, json.Unmarshal(presignRec.Body.Bytes(), &presignResp))
	assert.Contains(t, presignResp["AuthorizedUrl"], "app-1")

	listRec := doSageMakerRequest(t, h, "ListMlflowApps", map[string]any{})
	assert.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	summaries, ok := listResp["Summaries"].([]any)
	require.True(t, ok)
	assert.Len(t, summaries, 1)

	deleteRec := doSageMakerRequest(t, h, "DeleteMlflowApp", map[string]any{"Arn": appARN})
	assert.Equal(t, http.StatusOK, deleteRec.Code)

	describeRec = doSageMakerRequest(t, h, "DescribeMlflowApp", map[string]any{"Arn": appARN})
	assert.Equal(t, http.StatusBadRequest, describeRec.Code)
}

func TestHandler_CreateMlflowApp_MissingName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateMlflowApp", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_DescribeMlflowApp_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "DescribeMlflowApp", map[string]any{
		"Arn": "arn:aws:sagemaker:us-east-1:0:mlflow-app/missing",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_CreateMlflowApp_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{"Name": "dup-app"}

	rec := doSageMakerRequest(t, h, "CreateMlflowApp", body)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec2 := doSageMakerRequest(t, h, "CreateMlflowApp", body)
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

// ---------------------------------------------------------------------------
// PartnerApp — UpdatePartnerApp / ListPartnerApps / CreatePartnerAppPresignedUrl
// ---------------------------------------------------------------------------

func TestHandler_PartnerApp_ExtendedLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doSageMakerRequest(t, h, "CreatePartnerApp", map[string]any{
		"Name":             "papp-1",
		"Type":             "comet",
		"ExecutionRoleArn": "arn:aws:iam::000000000000:role/partner",
		"Tier":             "small",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]string
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	appARN := createResp["Arn"]

	describeRec := doSageMakerRequest(t, h, "DescribePartnerApp", map[string]any{"Arn": appARN})
	assert.Equal(t, http.StatusOK, describeRec.Code)

	var describeResp map[string]any
	require.NoError(t, json.Unmarshal(describeRec.Body.Bytes(), &describeResp))
	assert.Equal(t, "papp-1", describeResp["Name"])
	assert.Equal(t, "comet", describeResp["Type"])
	assert.Equal(t, "arn:aws:iam::000000000000:role/partner", describeResp["ExecutionRoleArn"])
	assert.Equal(t, "small", describeResp["Tier"])

	updateRec := doSageMakerRequest(t, h, "UpdatePartnerApp", map[string]any{
		"Arn":  appARN,
		"Tier": "large",
	})
	assert.Equal(t, http.StatusOK, updateRec.Code)

	describeRec = doSageMakerRequest(t, h, "DescribePartnerApp", map[string]any{"Arn": appARN})
	require.NoError(t, json.Unmarshal(describeRec.Body.Bytes(), &describeResp))
	assert.Equal(t, "large", describeResp["Tier"])

	listRec := doSageMakerRequest(t, h, "ListPartnerApps", map[string]any{})
	assert.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	summaries, ok := listResp["Summaries"].([]any)
	require.True(t, ok)
	assert.Len(t, summaries, 1)

	presignRec := doSageMakerRequest(t, h, "CreatePartnerAppPresignedUrl", map[string]any{"Arn": appARN})
	assert.Equal(t, http.StatusOK, presignRec.Code)

	var presignResp map[string]string
	require.NoError(t, json.Unmarshal(presignRec.Body.Bytes(), &presignResp))
	assert.Contains(t, presignResp["Url"], "papp-1")
}

func TestHandler_UpdatePartnerApp_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "UpdatePartnerApp", map[string]any{
		"Arn":  "arn:aws:sagemaker:us-east-1:0:partner-app/missing",
		"Tier": "large",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_UpdatePartnerApp_MissingArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "UpdatePartnerApp", map[string]any{"Tier": "large"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_CreatePartnerAppPresignedUrl_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreatePartnerAppPresignedUrl", map[string]any{
		"Arn": "arn:aws:sagemaker:us-east-1:0:partner-app/missing",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListPartnerApps_Empty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "ListPartnerApps", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["Summaries"])
}

func TestHandler_DeletePartnerApp_ReturnsArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doSageMakerRequest(t, h, "CreatePartnerApp", map[string]any{"Name": "papp-del"})
	var createResp map[string]string
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))

	rec := doSageMakerRequest(t, h, "DeletePartnerApp", map[string]any{"Arn": createResp["Arn"]})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, createResp["Arn"], resp["Arn"])
}

func TestHandler_InferenceExperiment_StartAndUpdate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateInferenceExperiment", map[string]any{"Name": "inf-exp"})

	recStart := doSageMakerRequest(t, h, "StartInferenceExperiment", map[string]any{"Name": "inf-exp"})
	require.Equal(t, http.StatusOK, recStart.Code)

	recUpdate := doSageMakerRequest(t, h, "UpdateInferenceExperiment", map[string]any{
		"Name": "inf-exp", "Description": "updated",
	})
	require.Equal(t, http.StatusOK, recUpdate.Code)

	recDescribe := doSageMakerRequest(t, h, "DescribeInferenceExperiment", map[string]any{"Name": "inf-exp"})
	var out map[string]any
	require.NoError(t, json.Unmarshal(recDescribe.Body.Bytes(), &out))
	assert.Equal(t, "Running", out["Status"])
	assert.Equal(t, "updated", out["Description"])
}

func TestHandler_InferenceExperiment_StartAndUpdate_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, op := range []string{"StartInferenceExperiment", "UpdateInferenceExperiment"} {
		t.Run(op, func(t *testing.T) {
			t.Parallel()

			rec := doSageMakerRequest(t, h, op, map[string]any{"Name": "no-such-experiment"})
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// ---------------------------------------------------------------------------
// HubContent Update/UpdateReference
// ---------------------------------------------------------------------------
