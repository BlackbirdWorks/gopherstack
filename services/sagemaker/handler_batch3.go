package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// batch3SupportedOperations returns the 50 real stateful operations implemented in batch 3.
func batch3SupportedOperations() []string {
	return []string{
		// DataQuality job definitions
		"CreateDataQualityJobDefinition",
		"DescribeDataQualityJobDefinition",
		"DeleteDataQualityJobDefinition",
		// ModelBias job definitions
		"CreateModelBiasJobDefinition",
		"DescribeModelBiasJobDefinition",
		"DeleteModelBiasJobDefinition",
		// ModelQuality job definitions
		"CreateModelQualityJobDefinition",
		"DescribeModelQualityJobDefinition",
		"DeleteModelQualityJobDefinition",
		// ModelExplainability job definitions
		"CreateModelExplainabilityJobDefinition",
		"DescribeModelExplainabilityJobDefinition",
		"DeleteModelExplainabilityJobDefinition",
		// HumanTaskUI
		"CreateHumanTaskUi",
		"DescribeHumanTaskUi",
		"DeleteHumanTaskUi",
		// Workforce
		"CreateWorkforce",
		"DescribeWorkforce",
		"UpdateWorkforce",
		// FlowDefinition
		"CreateFlowDefinition",
		"DescribeFlowDefinition",
		"DeleteFlowDefinition",
		// AppImageConfig
		"CreateAppImageConfig",
		"DescribeAppImageConfig",
		"DeleteAppImageConfig",
		"UpdateAppImageConfig",
		// InferenceExperiment
		"CreateInferenceExperiment",
		"DescribeInferenceExperiment",
		"StopInferenceExperiment",
		"DeleteInferenceExperiment",
		// MlflowTrackingServer
		"CreateMlflowTrackingServer",
		"DescribeMlflowTrackingServer",
		"DeleteMlflowTrackingServer",
		"StartMlflowTrackingServer",
		"StopMlflowTrackingServer",
		// ModelCard
		"CreateModelCard",
		"DescribeModelCard",
		"UpdateModelCard",
		"DeleteModelCard",
		// OptimizationJob
		"CreateOptimizationJob",
		"DescribeOptimizationJob",
		"DeleteOptimizationJob",
		"StopOptimizationJob",
		// StudioLifecycleConfig
		"CreateStudioLifecycleConfig",
		"DescribeStudioLifecycleConfig",
		"DeleteStudioLifecycleConfig",
		// PartnerApp
		"CreatePartnerApp",
		"DescribePartnerApp",
		"DeletePartnerApp",
		// TrainingPlan
		"CreateTrainingPlan",
		"DescribeTrainingPlan",
	}
}

// dispatchBatch3Ops dispatches the 50 real stateful operations (batch 3).
//
//nolint:cyclop,gocyclo,funlen // large switch is required for dispatching many operations
func (h *Handler) dispatchBatch3Ops(
	_ context.Context,
	op string,
	body []byte,
) ([]byte, bool, error) {
	switch op {
	// DataQualityJobDefinition
	case "CreateDataQualityJobDefinition":
		r, err := h.handleCreateDataQualityJobDefinition(body)

		return r, true, err
	case "DescribeDataQualityJobDefinition":
		r, err := h.handleDescribeDataQualityJobDefinition(body)

		return r, true, err
	case "DeleteDataQualityJobDefinition":
		return nil, true, h.handleDeleteDataQualityJobDefinition(body)

	// ModelBiasJobDefinition
	case "CreateModelBiasJobDefinition":
		r, err := h.handleCreateModelBiasJobDefinition(body)

		return r, true, err
	case "DescribeModelBiasJobDefinition":
		r, err := h.handleDescribeModelBiasJobDefinition(body)

		return r, true, err
	case "DeleteModelBiasJobDefinition":
		return nil, true, h.handleDeleteModelBiasJobDefinition(body)

	// ModelQualityJobDefinition
	case "CreateModelQualityJobDefinition":
		r, err := h.handleCreateModelQualityJobDefinition(body)

		return r, true, err
	case "DescribeModelQualityJobDefinition":
		r, err := h.handleDescribeModelQualityJobDefinition(body)

		return r, true, err
	case "DeleteModelQualityJobDefinition":
		return nil, true, h.handleDeleteModelQualityJobDefinition(body)

	// ModelExplainabilityJobDefinition
	case "CreateModelExplainabilityJobDefinition":
		r, err := h.handleCreateModelExplainabilityJobDefinition(body)

		return r, true, err
	case "DescribeModelExplainabilityJobDefinition":
		r, err := h.handleDescribeModelExplainabilityJobDefinition(body)

		return r, true, err
	case "DeleteModelExplainabilityJobDefinition":
		return nil, true, h.handleDeleteModelExplainabilityJobDefinition(body)

	// HumanTaskUI
	case "CreateHumanTaskUi":
		r, err := h.handleCreateHumanTaskUI(body)

		return r, true, err
	case "DescribeHumanTaskUi":
		r, err := h.handleDescribeHumanTaskUI(body)

		return r, true, err
	case "DeleteHumanTaskUi":
		return nil, true, h.handleDeleteHumanTaskUI(body)

	// Workforce
	case "CreateWorkforce":
		r, err := h.handleCreateWorkforce(body)

		return r, true, err
	case "DescribeWorkforce":
		r, err := h.handleDescribeWorkforce(body)

		return r, true, err
	case "UpdateWorkforce":
		r, err := h.handleUpdateWorkforce(body)

		return r, true, err

	// FlowDefinition
	case "CreateFlowDefinition":
		r, err := h.handleCreateFlowDefinition(body)

		return r, true, err
	case "DescribeFlowDefinition":
		r, err := h.handleDescribeFlowDefinition(body)

		return r, true, err
	case "DeleteFlowDefinition":
		return nil, true, h.handleDeleteFlowDefinition(body)

	// AppImageConfig
	case "CreateAppImageConfig":
		r, err := h.handleCreateAppImageConfig(body)

		return r, true, err
	case "DescribeAppImageConfig":
		r, err := h.handleDescribeAppImageConfig(body)

		return r, true, err
	case "DeleteAppImageConfig":
		return nil, true, h.handleDeleteAppImageConfig(body)
	case "UpdateAppImageConfig":
		r, err := h.handleUpdateAppImageConfig(body)

		return r, true, err

	// InferenceExperiment
	case "CreateInferenceExperiment":
		r, err := h.handleCreateInferenceExperiment(body)

		return r, true, err
	case "DescribeInferenceExperiment":
		r, err := h.handleDescribeInferenceExperiment(body)

		return r, true, err
	case "StopInferenceExperiment":
		return nil, true, h.handleStopInferenceExperiment(body)
	case "DeleteInferenceExperiment":
		return nil, true, h.handleDeleteInferenceExperiment(body)

	// MlflowTrackingServer
	case "CreateMlflowTrackingServer":
		r, err := h.handleCreateMlflowTrackingServer(body)

		return r, true, err
	case "DescribeMlflowTrackingServer":
		r, err := h.handleDescribeMlflowTrackingServer(body)

		return r, true, err
	case "DeleteMlflowTrackingServer":
		return nil, true, h.handleDeleteMlflowTrackingServer(body)
	case "StartMlflowTrackingServer":
		return nil, true, h.handleStartMlflowTrackingServer(body)
	case "StopMlflowTrackingServer":
		return nil, true, h.handleStopMlflowTrackingServer(body)

	// ModelCard
	case "CreateModelCard":
		r, err := h.handleCreateModelCard(body)

		return r, true, err
	case "DescribeModelCard":
		r, err := h.handleDescribeModelCard(body)

		return r, true, err
	case "UpdateModelCard":
		r, err := h.handleUpdateModelCard(body)

		return r, true, err
	case "DeleteModelCard":
		return nil, true, h.handleDeleteModelCard(body)

	// OptimizationJob
	case "CreateOptimizationJob":
		r, err := h.handleCreateOptimizationJob(body)

		return r, true, err
	case "DescribeOptimizationJob":
		r, err := h.handleDescribeOptimizationJob(body)

		return r, true, err
	case "DeleteOptimizationJob":
		return nil, true, h.handleDeleteOptimizationJob(body)
	case "StopOptimizationJob":
		return nil, true, h.handleStopOptimizationJob(body)

	// StudioLifecycleConfig
	case "CreateStudioLifecycleConfig":
		r, err := h.handleCreateStudioLifecycleConfig(body)

		return r, true, err
	case "DescribeStudioLifecycleConfig":
		r, err := h.handleDescribeStudioLifecycleConfig(body)

		return r, true, err
	case "DeleteStudioLifecycleConfig":
		return nil, true, h.handleDeleteStudioLifecycleConfig(body)

	// PartnerApp
	case "CreatePartnerApp":
		r, err := h.handleCreatePartnerApp(body)

		return r, true, err
	case "DescribePartnerApp":
		r, err := h.handleDescribePartnerApp(body)

		return r, true, err
	case "DeletePartnerApp":
		return nil, true, h.handleDeletePartnerApp(body)

	// TrainingPlan
	case "CreateTrainingPlan":
		r, err := h.handleCreateTrainingPlan(body)

		return r, true, err
	case "DescribeTrainingPlan":
		r, err := h.handleDescribeTrainingPlan(body)

		return r, true, err
	}

	return nil, false, nil
}

// ---------------------------------------------------------------------------
// DataQualityJobDefinition handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateDataQualityJobDefinition(body []byte) ([]byte, error) {
	var req struct {
		Tags                         map[string]string `json:"Tags"`
		DataQualityJobDefinitionName string            `json:"DataQualityJobDefinitionName"`
		RoleArn                      string            `json:"RoleArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.DataQualityJobDefinitionName == "" {
		return nil, fmt.Errorf("%w: DataQualityJobDefinitionName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateDataQualityJobDefinition(req.DataQualityJobDefinitionName, req.RoleArn, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyJobDefinitionArn: result.JobDefinitionArn})
}

func (h *Handler) handleDescribeDataQualityJobDefinition(body []byte) ([]byte, error) {
	var req struct {
		DataQualityJobDefinitionName string `json:"DataQualityJobDefinitionName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.DataQualityJobDefinitionName == "" {
		return nil, fmt.Errorf("%w: DataQualityJobDefinitionName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeDataQualityJobDefinition(req.DataQualityJobDefinitionName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeleteDataQualityJobDefinition(body []byte) error {
	var req struct {
		DataQualityJobDefinitionName string `json:"DataQualityJobDefinitionName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.DataQualityJobDefinitionName == "" {
		return fmt.Errorf("%w: DataQualityJobDefinitionName is required", errInvalidRequest)
	}

	return h.Backend.DeleteDataQualityJobDefinition(req.DataQualityJobDefinitionName)
}

// ---------------------------------------------------------------------------
// ModelBiasJobDefinition handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateModelBiasJobDefinition(body []byte) ([]byte, error) {
	var req struct {
		Tags                       map[string]string `json:"Tags"`
		ModelBiasJobDefinitionName string            `json:"ModelBiasJobDefinitionName"`
		RoleArn                    string            `json:"RoleArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelBiasJobDefinitionName == "" {
		return nil, fmt.Errorf("%w: ModelBiasJobDefinitionName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateModelBiasJobDefinition(req.ModelBiasJobDefinitionName, req.RoleArn, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyJobDefinitionArn: result.JobDefinitionArn})
}

func (h *Handler) handleDescribeModelBiasJobDefinition(body []byte) ([]byte, error) {
	var req struct {
		ModelBiasJobDefinitionName string `json:"ModelBiasJobDefinitionName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelBiasJobDefinitionName == "" {
		return nil, fmt.Errorf("%w: ModelBiasJobDefinitionName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeModelBiasJobDefinition(req.ModelBiasJobDefinitionName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeleteModelBiasJobDefinition(body []byte) error {
	var req struct {
		ModelBiasJobDefinitionName string `json:"ModelBiasJobDefinitionName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelBiasJobDefinitionName == "" {
		return fmt.Errorf("%w: ModelBiasJobDefinitionName is required", errInvalidRequest)
	}

	return h.Backend.DeleteModelBiasJobDefinition(req.ModelBiasJobDefinitionName)
}

// ---------------------------------------------------------------------------
// ModelQualityJobDefinition handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateModelQualityJobDefinition(body []byte) ([]byte, error) {
	var req struct {
		Tags                          map[string]string `json:"Tags"`
		ModelQualityJobDefinitionName string            `json:"ModelQualityJobDefinitionName"`
		RoleArn                       string            `json:"RoleArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelQualityJobDefinitionName == "" {
		return nil, fmt.Errorf("%w: ModelQualityJobDefinitionName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateModelQualityJobDefinition(
		req.ModelQualityJobDefinitionName, req.RoleArn, req.Tags,
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyJobDefinitionArn: result.JobDefinitionArn})
}

func (h *Handler) handleDescribeModelQualityJobDefinition(body []byte) ([]byte, error) {
	var req struct {
		ModelQualityJobDefinitionName string `json:"ModelQualityJobDefinitionName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelQualityJobDefinitionName == "" {
		return nil, fmt.Errorf("%w: ModelQualityJobDefinitionName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeModelQualityJobDefinition(req.ModelQualityJobDefinitionName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeleteModelQualityJobDefinition(body []byte) error {
	var req struct {
		ModelQualityJobDefinitionName string `json:"ModelQualityJobDefinitionName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelQualityJobDefinitionName == "" {
		return fmt.Errorf("%w: ModelQualityJobDefinitionName is required", errInvalidRequest)
	}

	return h.Backend.DeleteModelQualityJobDefinition(req.ModelQualityJobDefinitionName)
}

// ---------------------------------------------------------------------------
// ModelExplainabilityJobDefinition handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateModelExplainabilityJobDefinition(body []byte) ([]byte, error) {
	var req struct {
		Tags                                 map[string]string `json:"Tags"`
		ModelExplainabilityJobDefinitionName string            `json:"ModelExplainabilityJobDefinitionName"`
		RoleArn                              string            `json:"RoleArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelExplainabilityJobDefinitionName == "" {
		return nil, fmt.Errorf("%w: ModelExplainabilityJobDefinitionName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateModelExplainabilityJobDefinition(
		req.ModelExplainabilityJobDefinitionName, req.RoleArn, req.Tags,
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyJobDefinitionArn: result.JobDefinitionArn})
}

func (h *Handler) handleDescribeModelExplainabilityJobDefinition(body []byte) ([]byte, error) {
	var req struct {
		ModelExplainabilityJobDefinitionName string `json:"ModelExplainabilityJobDefinitionName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelExplainabilityJobDefinitionName == "" {
		return nil, fmt.Errorf("%w: ModelExplainabilityJobDefinitionName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeModelExplainabilityJobDefinition(req.ModelExplainabilityJobDefinitionName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeleteModelExplainabilityJobDefinition(body []byte) error {
	var req struct {
		ModelExplainabilityJobDefinitionName string `json:"ModelExplainabilityJobDefinitionName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelExplainabilityJobDefinitionName == "" {
		return fmt.Errorf("%w: ModelExplainabilityJobDefinitionName is required", errInvalidRequest)
	}

	return h.Backend.DeleteModelExplainabilityJobDefinition(req.ModelExplainabilityJobDefinitionName)
}

// ---------------------------------------------------------------------------
// HumanTaskUI handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateHumanTaskUI(body []byte) ([]byte, error) {
	var req struct {
		Tags            map[string]string `json:"Tags"`
		HumanTaskUIName string            `json:"HumanTaskUiName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.HumanTaskUIName == "" {
		return nil, fmt.Errorf("%w: HumanTaskUiName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateHumanTaskUI(req.HumanTaskUIName, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyHumanTaskUIArn: result.HumanTaskUIArn})
}

func (h *Handler) handleDescribeHumanTaskUI(body []byte) ([]byte, error) {
	var req struct {
		HumanTaskUIName string `json:"HumanTaskUiName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.HumanTaskUIName == "" {
		return nil, fmt.Errorf("%w: HumanTaskUiName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeHumanTaskUI(req.HumanTaskUIName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeleteHumanTaskUI(body []byte) error {
	var req struct {
		HumanTaskUIName string `json:"HumanTaskUiName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.HumanTaskUIName == "" {
		return fmt.Errorf("%w: HumanTaskUiName is required", errInvalidRequest)
	}

	return h.Backend.DeleteHumanTaskUI(req.HumanTaskUIName)
}

// ---------------------------------------------------------------------------
// Workforce handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateWorkforce(body []byte) ([]byte, error) {
	var req struct {
		Tags          map[string]string `json:"Tags"`
		WorkforceName string            `json:"WorkforceName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.WorkforceName == "" {
		return nil, fmt.Errorf("%w: WorkforceName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateWorkforce(req.WorkforceName, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyWorkforceArn: result.WorkforceArn})
}

func (h *Handler) handleDescribeWorkforce(body []byte) ([]byte, error) {
	var req struct {
		WorkforceName string `json:"WorkforceName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.WorkforceName == "" {
		return nil, fmt.Errorf("%w: WorkforceName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeWorkforce(req.WorkforceName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"Workforce": result})
}

func (h *Handler) handleUpdateWorkforce(body []byte) ([]byte, error) {
	var req struct {
		WorkforceName string `json:"WorkforceName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.WorkforceName == "" {
		return nil, fmt.Errorf("%w: WorkforceName is required", errInvalidRequest)
	}

	result, err := h.Backend.UpdateWorkforce(req.WorkforceName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"Workforce": result})
}

// ---------------------------------------------------------------------------
// FlowDefinition handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateFlowDefinition(body []byte) ([]byte, error) {
	var req struct {
		Tags               map[string]string `json:"Tags"`
		FlowDefinitionName string            `json:"FlowDefinitionName"`
		RoleArn            string            `json:"RoleArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.FlowDefinitionName == "" {
		return nil, fmt.Errorf("%w: FlowDefinitionName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateFlowDefinition(req.FlowDefinitionName, req.RoleArn, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyFlowDefinitionArn: result.FlowDefinitionArn})
}

func (h *Handler) handleDescribeFlowDefinition(body []byte) ([]byte, error) {
	var req struct {
		FlowDefinitionName string `json:"FlowDefinitionName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.FlowDefinitionName == "" {
		return nil, fmt.Errorf("%w: FlowDefinitionName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeFlowDefinition(req.FlowDefinitionName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeleteFlowDefinition(body []byte) error {
	var req struct {
		FlowDefinitionName string `json:"FlowDefinitionName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.FlowDefinitionName == "" {
		return fmt.Errorf("%w: FlowDefinitionName is required", errInvalidRequest)
	}

	return h.Backend.DeleteFlowDefinition(req.FlowDefinitionName)
}

// ---------------------------------------------------------------------------
// AppImageConfig handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateAppImageConfig(body []byte) ([]byte, error) {
	var req struct {
		Tags               map[string]string `json:"Tags"`
		AppImageConfigName string            `json:"AppImageConfigName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.AppImageConfigName == "" {
		return nil, fmt.Errorf("%w: AppImageConfigName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateAppImageConfig(req.AppImageConfigName, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyAppImageConfigArn: result.AppImageConfigArn})
}

func (h *Handler) handleDescribeAppImageConfig(body []byte) ([]byte, error) {
	var req struct {
		AppImageConfigName string `json:"AppImageConfigName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.AppImageConfigName == "" {
		return nil, fmt.Errorf("%w: AppImageConfigName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeAppImageConfig(req.AppImageConfigName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeleteAppImageConfig(body []byte) error {
	var req struct {
		AppImageConfigName string `json:"AppImageConfigName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.AppImageConfigName == "" {
		return fmt.Errorf("%w: AppImageConfigName is required", errInvalidRequest)
	}

	return h.Backend.DeleteAppImageConfig(req.AppImageConfigName)
}

func (h *Handler) handleUpdateAppImageConfig(body []byte) ([]byte, error) {
	var req struct {
		AppImageConfigName string `json:"AppImageConfigName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.AppImageConfigName == "" {
		return nil, fmt.Errorf("%w: AppImageConfigName is required", errInvalidRequest)
	}

	result, err := h.Backend.UpdateAppImageConfig(req.AppImageConfigName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyAppImageConfigArn: result.AppImageConfigArn})
}

// ---------------------------------------------------------------------------
// InferenceExperiment handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateInferenceExperiment(body []byte) ([]byte, error) {
	var req struct {
		Tags    map[string]string `json:"Tags"`
		Name    string            `json:"Name"`
		Type    string            `json:"Type"`
		RoleArn string            `json:"RoleArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateInferenceExperiment(req.Name, req.Type, req.RoleArn, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyInferenceExperimentArn: result.Arn})
}

func (h *Handler) handleDescribeInferenceExperiment(body []byte) ([]byte, error) {
	var req struct {
		Name string `json:"Name"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeInferenceExperiment(req.Name)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleStopInferenceExperiment(body []byte) error {
	var req struct {
		Name string `json:"Name"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	return h.Backend.StopInferenceExperiment(req.Name)
}

func (h *Handler) handleDeleteInferenceExperiment(body []byte) error {
	var req struct {
		Name string `json:"Name"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	return h.Backend.DeleteInferenceExperiment(req.Name)
}

// ---------------------------------------------------------------------------
// MlflowTrackingServer handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateMlflowTrackingServer(body []byte) ([]byte, error) {
	var req struct {
		Tags               map[string]string `json:"Tags"`
		TrackingServerName string            `json:"TrackingServerName"`
		RoleArn            string            `json:"RoleArn"`
		MlflowVersion      string            `json:"MlflowVersion"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrackingServerName == "" {
		return nil, fmt.Errorf("%w: TrackingServerName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateMlflowTrackingServer(
		req.TrackingServerName, req.RoleArn, req.MlflowVersion, req.Tags,
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyTrackingServerArn: result.TrackingServerArn})
}

func (h *Handler) handleDescribeMlflowTrackingServer(body []byte) ([]byte, error) {
	var req struct {
		TrackingServerName string `json:"TrackingServerName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrackingServerName == "" {
		return nil, fmt.Errorf("%w: TrackingServerName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeMlflowTrackingServer(req.TrackingServerName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeleteMlflowTrackingServer(body []byte) error {
	var req struct {
		TrackingServerName string `json:"TrackingServerName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrackingServerName == "" {
		return fmt.Errorf("%w: TrackingServerName is required", errInvalidRequest)
	}

	return h.Backend.DeleteMlflowTrackingServer(req.TrackingServerName)
}

func (h *Handler) handleStartMlflowTrackingServer(body []byte) error {
	var req struct {
		TrackingServerName string `json:"TrackingServerName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrackingServerName == "" {
		return fmt.Errorf("%w: TrackingServerName is required", errInvalidRequest)
	}

	return h.Backend.StartMlflowTrackingServer(req.TrackingServerName)
}

func (h *Handler) handleStopMlflowTrackingServer(body []byte) error {
	var req struct {
		TrackingServerName string `json:"TrackingServerName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrackingServerName == "" {
		return fmt.Errorf("%w: TrackingServerName is required", errInvalidRequest)
	}

	return h.Backend.StopMlflowTrackingServer(req.TrackingServerName)
}

// ---------------------------------------------------------------------------
// ModelCard handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateModelCard(body []byte) ([]byte, error) {
	var req struct {
		Tags          map[string]string `json:"Tags"`
		ModelCardName string            `json:"ModelCardName"`
		Content       string            `json:"Content"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelCardName == "" {
		return nil, fmt.Errorf("%w: ModelCardName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateModelCard(req.ModelCardName, req.Content, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyModelCardArn: result.ModelCardArn})
}

func (h *Handler) handleDescribeModelCard(body []byte) ([]byte, error) {
	var req struct {
		ModelCardName string `json:"ModelCardName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelCardName == "" {
		return nil, fmt.Errorf("%w: ModelCardName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeModelCard(req.ModelCardName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleUpdateModelCard(body []byte) ([]byte, error) {
	var req struct {
		ModelCardName string `json:"ModelCardName"`
		Content       string `json:"Content"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelCardName == "" {
		return nil, fmt.Errorf("%w: ModelCardName is required", errInvalidRequest)
	}

	result, err := h.Backend.UpdateModelCard(req.ModelCardName, req.Content)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyModelCardArn: result.ModelCardArn})
}

func (h *Handler) handleDeleteModelCard(body []byte) error {
	var req struct {
		ModelCardName string `json:"ModelCardName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelCardName == "" {
		return fmt.Errorf("%w: ModelCardName is required", errInvalidRequest)
	}

	return h.Backend.DeleteModelCard(req.ModelCardName)
}

// ---------------------------------------------------------------------------
// OptimizationJob handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateOptimizationJob(body []byte) ([]byte, error) {
	var req struct {
		Tags                map[string]string `json:"Tags"`
		OptimizationJobName string            `json:"OptimizationJobName"`
		RoleArn             string            `json:"RoleArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.OptimizationJobName == "" {
		return nil, fmt.Errorf("%w: OptimizationJobName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateOptimizationJob(req.OptimizationJobName, req.RoleArn, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyOptimizationJobArn: result.OptimizationJobArn})
}

func (h *Handler) handleDescribeOptimizationJob(body []byte) ([]byte, error) {
	var req struct {
		OptimizationJobName string `json:"OptimizationJobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.OptimizationJobName == "" {
		return nil, fmt.Errorf("%w: OptimizationJobName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeOptimizationJob(req.OptimizationJobName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeleteOptimizationJob(body []byte) error {
	var req struct {
		OptimizationJobName string `json:"OptimizationJobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.OptimizationJobName == "" {
		return fmt.Errorf("%w: OptimizationJobName is required", errInvalidRequest)
	}

	return h.Backend.DeleteOptimizationJob(req.OptimizationJobName)
}

func (h *Handler) handleStopOptimizationJob(body []byte) error {
	var req struct {
		OptimizationJobName string `json:"OptimizationJobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.OptimizationJobName == "" {
		return fmt.Errorf("%w: OptimizationJobName is required", errInvalidRequest)
	}

	return h.Backend.StopOptimizationJob(req.OptimizationJobName)
}

// ---------------------------------------------------------------------------
// StudioLifecycleConfig handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateStudioLifecycleConfig(body []byte) ([]byte, error) {
	var req struct {
		Tags                         map[string]string `json:"Tags"`
		StudioLifecycleConfigName    string            `json:"StudioLifecycleConfigName"`
		StudioLifecycleConfigAppType string            `json:"StudioLifecycleConfigAppType"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.StudioLifecycleConfigName == "" {
		return nil, fmt.Errorf("%w: StudioLifecycleConfigName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateStudioLifecycleConfig(
		req.StudioLifecycleConfigName, req.StudioLifecycleConfigAppType, req.Tags,
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyStudioLifecycleConfigArn: result.StudioLifecycleConfigArn})
}

func (h *Handler) handleDescribeStudioLifecycleConfig(body []byte) ([]byte, error) {
	var req struct {
		StudioLifecycleConfigName string `json:"StudioLifecycleConfigName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.StudioLifecycleConfigName == "" {
		return nil, fmt.Errorf("%w: StudioLifecycleConfigName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeStudioLifecycleConfig(req.StudioLifecycleConfigName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeleteStudioLifecycleConfig(body []byte) error {
	var req struct {
		StudioLifecycleConfigName string `json:"StudioLifecycleConfigName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.StudioLifecycleConfigName == "" {
		return fmt.Errorf("%w: StudioLifecycleConfigName is required", errInvalidRequest)
	}

	return h.Backend.DeleteStudioLifecycleConfig(req.StudioLifecycleConfigName)
}

// ---------------------------------------------------------------------------
// PartnerApp handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreatePartnerApp(body []byte) ([]byte, error) {
	var req struct {
		Tags map[string]string `json:"Tags"`
		Name string            `json:"Name"`
		Type string            `json:"Type"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	result, err := h.Backend.CreatePartnerApp(req.Name, req.Type, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyGenericArn: result.Arn})
}

func (h *Handler) handleDescribePartnerApp(body []byte) ([]byte, error) {
	var req struct {
		Arn string `json:"Arn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Arn == "" {
		return nil, fmt.Errorf("%w: Arn is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribePartnerApp(req.Arn)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeletePartnerApp(body []byte) error {
	var req struct {
		Arn string `json:"Arn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Arn == "" {
		return fmt.Errorf("%w: Arn is required", errInvalidRequest)
	}

	return h.Backend.DeletePartnerApp(req.Arn)
}

// ---------------------------------------------------------------------------
// TrainingPlan handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateTrainingPlan(body []byte) ([]byte, error) {
	var req struct {
		Tags             map[string]string `json:"Tags"`
		TrainingPlanName string            `json:"TrainingPlanName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrainingPlanName == "" {
		return nil, fmt.Errorf("%w: TrainingPlanName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateTrainingPlan(req.TrainingPlanName, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyTrainingPlanArn: result.TrainingPlanArn})
}

func (h *Handler) handleDescribeTrainingPlan(body []byte) ([]byte, error) {
	var req struct {
		TrainingPlanName string `json:"TrainingPlanName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrainingPlanName == "" {
		return nil, fmt.Errorf("%w: TrainingPlanName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeTrainingPlan(req.TrainingPlanName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}
