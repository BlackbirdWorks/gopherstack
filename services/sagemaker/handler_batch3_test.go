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
// ---------------------------------------------------------------------------

func TestHandler_CreateDataQualityJobDefinition(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateDataQualityJobDefinition", map[string]any{
		"DataQualityJobDefinitionName": "dq-def-1",
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
		"DataQualityJobDefinitionName": "dq-def-2",
	})

	rec := doSageMakerRequest(t, h, "DescribeDataQualityJobDefinition", map[string]any{
		"DataQualityJobDefinitionName": "dq-def-2",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "dq-def-2", resp["JobDefinitionName"])
}

func TestHandler_DeleteDataQualityJobDefinition(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateDataQualityJobDefinition", map[string]any{
		"DataQualityJobDefinitionName": "dq-def-del",
	})
	rec := doSageMakerRequest(t, h, "DeleteDataQualityJobDefinition", map[string]any{
		"DataQualityJobDefinitionName": "dq-def-del",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeDataQualityJobDefinition", map[string]any{
		"DataQualityJobDefinitionName": "dq-def-del",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// ModelBiasJobDefinition
// ---------------------------------------------------------------------------

func TestHandler_CreateModelBiasJobDefinition(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateModelBiasJobDefinition", map[string]any{
		"ModelBiasJobDefinitionName": "mb-def-1",
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
		"ModelBiasJobDefinitionName": "mb-def-del",
	})
	rec := doSageMakerRequest(t, h, "DeleteModelBiasJobDefinition", map[string]any{
		"ModelBiasJobDefinitionName": "mb-def-del",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeModelBiasJobDefinition", map[string]any{
		"ModelBiasJobDefinitionName": "mb-def-del",
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
		"ModelQualityJobDefinitionName": "mq-def-1",
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
		"ModelQualityJobDefinitionName": "mq-def-del",
	})
	rec := doSageMakerRequest(t, h, "DeleteModelQualityJobDefinition", map[string]any{
		"ModelQualityJobDefinitionName": "mq-def-del",
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
		"ModelExplainabilityJobDefinitionName": "me-def-1",
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
		"ModelExplainabilityJobDefinitionName": "me-def-del",
	})
	rec := doSageMakerRequest(t, h, "DeleteModelExplainabilityJobDefinition", map[string]any{
		"ModelExplainabilityJobDefinitionName": "me-def-del",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---------------------------------------------------------------------------
// HumanTaskUi
// ---------------------------------------------------------------------------

func TestHandler_CreateHumanTaskUi(t *testing.T) {
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

func TestHandler_DescribeHumanTaskUi(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateHumanTaskUi", map[string]any{"HumanTaskUiName": "ui-1"})
	rec := doSageMakerRequest(t, h, "DescribeHumanTaskUi", map[string]any{"HumanTaskUiName": "ui-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ui-1", resp["HumanTaskUiName"])
}

func TestHandler_DeleteHumanTaskUi(t *testing.T) {
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
	assert.Equal(t, float64(2), resp["ModelCardVersion"])
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
