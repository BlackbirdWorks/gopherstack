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
	ctx context.Context,
	op string,
	body []byte,
) ([]byte, bool, error) {
	switch op {
	// DataQualityJobDefinition
	case "CreateDataQualityJobDefinition":
		r, err := h.handleCreateDataQualityJobDefinition(ctx, body)

		return r, true, err
	case "DescribeDataQualityJobDefinition":
		r, err := h.handleDescribeDataQualityJobDefinition(ctx, body)

		return r, true, err
	case "DeleteDataQualityJobDefinition":
		return nil, true, h.handleDeleteDataQualityJobDefinition(ctx, body)

	// ModelBiasJobDefinition
	case "CreateModelBiasJobDefinition":
		r, err := h.handleCreateModelBiasJobDefinition(ctx, body)

		return r, true, err
	case "DescribeModelBiasJobDefinition":
		r, err := h.handleDescribeModelBiasJobDefinition(ctx, body)

		return r, true, err
	case "DeleteModelBiasJobDefinition":
		return nil, true, h.handleDeleteModelBiasJobDefinition(ctx, body)

	// ModelQualityJobDefinition
	case "CreateModelQualityJobDefinition":
		r, err := h.handleCreateModelQualityJobDefinition(ctx, body)

		return r, true, err
	case "DescribeModelQualityJobDefinition":
		r, err := h.handleDescribeModelQualityJobDefinition(ctx, body)

		return r, true, err
	case "DeleteModelQualityJobDefinition":
		return nil, true, h.handleDeleteModelQualityJobDefinition(ctx, body)

	// ModelExplainabilityJobDefinition
	case "CreateModelExplainabilityJobDefinition":
		r, err := h.handleCreateModelExplainabilityJobDefinition(ctx, body)

		return r, true, err
	case "DescribeModelExplainabilityJobDefinition":
		r, err := h.handleDescribeModelExplainabilityJobDefinition(ctx, body)

		return r, true, err
	case "DeleteModelExplainabilityJobDefinition":
		return nil, true, h.handleDeleteModelExplainabilityJobDefinition(ctx, body)

	// HumanTaskUI
	case "CreateHumanTaskUi":
		r, err := h.handleCreateHumanTaskUI(ctx, body)

		return r, true, err
	case "DescribeHumanTaskUi":
		r, err := h.handleDescribeHumanTaskUI(ctx, body)

		return r, true, err
	case "DeleteHumanTaskUi":
		return nil, true, h.handleDeleteHumanTaskUI(ctx, body)

	// Workforce
	case "CreateWorkforce":
		r, err := h.handleCreateWorkforce(ctx, body)

		return r, true, err
	case "DescribeWorkforce":
		r, err := h.handleDescribeWorkforce(ctx, body)

		return r, true, err
	case "UpdateWorkforce":
		r, err := h.handleUpdateWorkforce(ctx, body)

		return r, true, err

	// FlowDefinition
	case "CreateFlowDefinition":
		r, err := h.handleCreateFlowDefinition(ctx, body)

		return r, true, err
	case "DescribeFlowDefinition":
		r, err := h.handleDescribeFlowDefinition(ctx, body)

		return r, true, err
	case "DeleteFlowDefinition":
		return nil, true, h.handleDeleteFlowDefinition(ctx, body)

	// AppImageConfig
	case "CreateAppImageConfig":
		r, err := h.handleCreateAppImageConfig(ctx, body)

		return r, true, err
	case "DescribeAppImageConfig":
		r, err := h.handleDescribeAppImageConfig(ctx, body)

		return r, true, err
	case "DeleteAppImageConfig":
		return nil, true, h.handleDeleteAppImageConfig(ctx, body)
	case "UpdateAppImageConfig":
		r, err := h.handleUpdateAppImageConfig(ctx, body)

		return r, true, err

	// InferenceExperiment
	case "CreateInferenceExperiment":
		r, err := h.handleCreateInferenceExperiment(ctx, body)

		return r, true, err
	case "DescribeInferenceExperiment":
		r, err := h.handleDescribeInferenceExperiment(ctx, body)

		return r, true, err
	case "StopInferenceExperiment":
		return nil, true, h.handleStopInferenceExperiment(ctx, body)
	case "DeleteInferenceExperiment":
		return nil, true, h.handleDeleteInferenceExperiment(ctx, body)

	// MlflowTrackingServer
	case "CreateMlflowTrackingServer":
		r, err := h.handleCreateMlflowTrackingServer(ctx, body)

		return r, true, err
	case "DescribeMlflowTrackingServer":
		r, err := h.handleDescribeMlflowTrackingServer(ctx, body)

		return r, true, err
	case "DeleteMlflowTrackingServer":
		return nil, true, h.handleDeleteMlflowTrackingServer(ctx, body)
	case "StartMlflowTrackingServer":
		return nil, true, h.handleStartMlflowTrackingServer(ctx, body)
	case "StopMlflowTrackingServer":
		return nil, true, h.handleStopMlflowTrackingServer(ctx, body)

	// ModelCard
	case "CreateModelCard":
		r, err := h.handleCreateModelCard(ctx, body)

		return r, true, err
	case "DescribeModelCard":
		r, err := h.handleDescribeModelCard(ctx, body)

		return r, true, err
	case "UpdateModelCard":
		r, err := h.handleUpdateModelCard(ctx, body)

		return r, true, err
	case "DeleteModelCard":
		return nil, true, h.handleDeleteModelCard(ctx, body)

	// OptimizationJob
	case "CreateOptimizationJob":
		r, err := h.handleCreateOptimizationJob(ctx, body)

		return r, true, err
	case "DescribeOptimizationJob":
		r, err := h.handleDescribeOptimizationJob(ctx, body)

		return r, true, err
	case "DeleteOptimizationJob":
		return nil, true, h.handleDeleteOptimizationJob(ctx, body)
	case "StopOptimizationJob":
		return nil, true, h.handleStopOptimizationJob(ctx, body)

	// StudioLifecycleConfig
	case "CreateStudioLifecycleConfig":
		r, err := h.handleCreateStudioLifecycleConfig(ctx, body)

		return r, true, err
	case "DescribeStudioLifecycleConfig":
		r, err := h.handleDescribeStudioLifecycleConfig(ctx, body)

		return r, true, err
	case "DeleteStudioLifecycleConfig":
		return nil, true, h.handleDeleteStudioLifecycleConfig(ctx, body)

	// PartnerApp
	case "CreatePartnerApp":
		r, err := h.handleCreatePartnerApp(ctx, body)

		return r, true, err
	case "DescribePartnerApp":
		r, err := h.handleDescribePartnerApp(ctx, body)

		return r, true, err
	case "DeletePartnerApp":
		return nil, true, h.handleDeletePartnerApp(ctx, body)

	// TrainingPlan
	case "CreateTrainingPlan":
		r, err := h.handleCreateTrainingPlan(ctx, body)

		return r, true, err
	case "DescribeTrainingPlan":
		r, err := h.handleDescribeTrainingPlan(ctx, body)

		return r, true, err
	}

	return nil, false, nil
}

// ---------------------------------------------------------------------------
// DataQualityJobDefinition handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateDataQualityJobDefinition(ctx context.Context, body []byte) ([]byte, error) {
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

	result, err := h.Backend.CreateDataQualityJobDefinition(
		ctx,
		req.DataQualityJobDefinitionName,
		req.RoleArn,
		req.Tags,
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyJobDefinitionArn: result.JobDefinitionArn})
}

func (h *Handler) handleDescribeDataQualityJobDefinition(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		DataQualityJobDefinitionName string `json:"DataQualityJobDefinitionName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.DataQualityJobDefinitionName == "" {
		return nil, fmt.Errorf("%w: DataQualityJobDefinitionName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeDataQualityJobDefinition(ctx, req.DataQualityJobDefinitionName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeleteDataQualityJobDefinition(ctx context.Context, body []byte) error {
	var req struct {
		DataQualityJobDefinitionName string `json:"DataQualityJobDefinitionName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.DataQualityJobDefinitionName == "" {
		return fmt.Errorf("%w: DataQualityJobDefinitionName is required", errInvalidRequest)
	}

	return h.Backend.DeleteDataQualityJobDefinition(ctx, req.DataQualityJobDefinitionName)
}

// ---------------------------------------------------------------------------
// ModelBiasJobDefinition handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateModelBiasJobDefinition(ctx context.Context, body []byte) ([]byte, error) {
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

	result, err := h.Backend.CreateModelBiasJobDefinition(ctx, req.ModelBiasJobDefinitionName, req.RoleArn, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyJobDefinitionArn: result.JobDefinitionArn})
}

func (h *Handler) handleDescribeModelBiasJobDefinition(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ModelBiasJobDefinitionName string `json:"ModelBiasJobDefinitionName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelBiasJobDefinitionName == "" {
		return nil, fmt.Errorf("%w: ModelBiasJobDefinitionName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeModelBiasJobDefinition(ctx, req.ModelBiasJobDefinitionName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeleteModelBiasJobDefinition(ctx context.Context, body []byte) error {
	var req struct {
		ModelBiasJobDefinitionName string `json:"ModelBiasJobDefinitionName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelBiasJobDefinitionName == "" {
		return fmt.Errorf("%w: ModelBiasJobDefinitionName is required", errInvalidRequest)
	}

	return h.Backend.DeleteModelBiasJobDefinition(ctx, req.ModelBiasJobDefinitionName)
}

// ---------------------------------------------------------------------------
// ModelQualityJobDefinition handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateModelQualityJobDefinition(ctx context.Context, body []byte) ([]byte, error) {
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

	result, err := h.Backend.CreateModelQualityJobDefinition(ctx,
		req.ModelQualityJobDefinitionName, req.RoleArn, req.Tags,
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyJobDefinitionArn: result.JobDefinitionArn})
}

func (h *Handler) handleDescribeModelQualityJobDefinition(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ModelQualityJobDefinitionName string `json:"ModelQualityJobDefinitionName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelQualityJobDefinitionName == "" {
		return nil, fmt.Errorf("%w: ModelQualityJobDefinitionName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeModelQualityJobDefinition(ctx, req.ModelQualityJobDefinitionName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeleteModelQualityJobDefinition(ctx context.Context, body []byte) error {
	var req struct {
		ModelQualityJobDefinitionName string `json:"ModelQualityJobDefinitionName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelQualityJobDefinitionName == "" {
		return fmt.Errorf("%w: ModelQualityJobDefinitionName is required", errInvalidRequest)
	}

	return h.Backend.DeleteModelQualityJobDefinition(ctx, req.ModelQualityJobDefinitionName)
}

// ---------------------------------------------------------------------------
// ModelExplainabilityJobDefinition handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateModelExplainabilityJobDefinition(ctx context.Context, body []byte) ([]byte, error) {
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

	result, err := h.Backend.CreateModelExplainabilityJobDefinition(ctx,
		req.ModelExplainabilityJobDefinitionName, req.RoleArn, req.Tags,
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyJobDefinitionArn: result.JobDefinitionArn})
}

func (h *Handler) handleDescribeModelExplainabilityJobDefinition(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ModelExplainabilityJobDefinitionName string `json:"ModelExplainabilityJobDefinitionName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelExplainabilityJobDefinitionName == "" {
		return nil, fmt.Errorf("%w: ModelExplainabilityJobDefinitionName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeModelExplainabilityJobDefinition(ctx, req.ModelExplainabilityJobDefinitionName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeleteModelExplainabilityJobDefinition(ctx context.Context, body []byte) error {
	var req struct {
		ModelExplainabilityJobDefinitionName string `json:"ModelExplainabilityJobDefinitionName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelExplainabilityJobDefinitionName == "" {
		return fmt.Errorf("%w: ModelExplainabilityJobDefinitionName is required", errInvalidRequest)
	}

	return h.Backend.DeleteModelExplainabilityJobDefinition(ctx, req.ModelExplainabilityJobDefinitionName)
}

// ---------------------------------------------------------------------------
// HumanTaskUI handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateHumanTaskUI(ctx context.Context, body []byte) ([]byte, error) {
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

	result, err := h.Backend.CreateHumanTaskUI(ctx, req.HumanTaskUIName, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyHumanTaskUIArn: result.HumanTaskUIArn})
}

func (h *Handler) handleDescribeHumanTaskUI(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		HumanTaskUIName string `json:"HumanTaskUiName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.HumanTaskUIName == "" {
		return nil, fmt.Errorf("%w: HumanTaskUiName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeHumanTaskUI(ctx, req.HumanTaskUIName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeleteHumanTaskUI(ctx context.Context, body []byte) error {
	var req struct {
		HumanTaskUIName string `json:"HumanTaskUiName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.HumanTaskUIName == "" {
		return fmt.Errorf("%w: HumanTaskUiName is required", errInvalidRequest)
	}

	return h.Backend.DeleteHumanTaskUI(ctx, req.HumanTaskUIName)
}

// ---------------------------------------------------------------------------
// Workforce handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateWorkforce(ctx context.Context, body []byte) ([]byte, error) {
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

	result, err := h.Backend.CreateWorkforce(ctx, req.WorkforceName, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyWorkforceArn: result.WorkforceArn})
}

func (h *Handler) handleDescribeWorkforce(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		WorkforceName string `json:"WorkforceName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.WorkforceName == "" {
		return nil, fmt.Errorf("%w: WorkforceName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeWorkforce(ctx, req.WorkforceName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"Workforce": result})
}

func (h *Handler) handleUpdateWorkforce(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		WorkforceName string `json:"WorkforceName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.WorkforceName == "" {
		return nil, fmt.Errorf("%w: WorkforceName is required", errInvalidRequest)
	}

	result, err := h.Backend.UpdateWorkforce(ctx, req.WorkforceName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"Workforce": result})
}

// ---------------------------------------------------------------------------
// FlowDefinition handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateFlowDefinition(ctx context.Context, body []byte) ([]byte, error) {
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

	result, err := h.Backend.CreateFlowDefinition(ctx, req.FlowDefinitionName, req.RoleArn, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyFlowDefinitionArn: result.FlowDefinitionArn})
}

func (h *Handler) handleDescribeFlowDefinition(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		FlowDefinitionName string `json:"FlowDefinitionName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.FlowDefinitionName == "" {
		return nil, fmt.Errorf("%w: FlowDefinitionName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeFlowDefinition(ctx, req.FlowDefinitionName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeleteFlowDefinition(ctx context.Context, body []byte) error {
	var req struct {
		FlowDefinitionName string `json:"FlowDefinitionName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.FlowDefinitionName == "" {
		return fmt.Errorf("%w: FlowDefinitionName is required", errInvalidRequest)
	}

	return h.Backend.DeleteFlowDefinition(ctx, req.FlowDefinitionName)
}

// ---------------------------------------------------------------------------
// AppImageConfig handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateAppImageConfig(ctx context.Context, body []byte) ([]byte, error) {
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

	result, err := h.Backend.CreateAppImageConfig(ctx, req.AppImageConfigName, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyAppImageConfigArn: result.AppImageConfigArn})
}

func (h *Handler) handleDescribeAppImageConfig(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		AppImageConfigName string `json:"AppImageConfigName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.AppImageConfigName == "" {
		return nil, fmt.Errorf("%w: AppImageConfigName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeAppImageConfig(ctx, req.AppImageConfigName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeleteAppImageConfig(ctx context.Context, body []byte) error {
	var req struct {
		AppImageConfigName string `json:"AppImageConfigName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.AppImageConfigName == "" {
		return fmt.Errorf("%w: AppImageConfigName is required", errInvalidRequest)
	}

	return h.Backend.DeleteAppImageConfig(ctx, req.AppImageConfigName)
}

func (h *Handler) handleUpdateAppImageConfig(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		AppImageConfigName string `json:"AppImageConfigName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.AppImageConfigName == "" {
		return nil, fmt.Errorf("%w: AppImageConfigName is required", errInvalidRequest)
	}

	result, err := h.Backend.UpdateAppImageConfig(ctx, req.AppImageConfigName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyAppImageConfigArn: result.AppImageConfigArn})
}

// ---------------------------------------------------------------------------
// InferenceExperiment handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateInferenceExperiment(ctx context.Context, body []byte) ([]byte, error) {
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

	result, err := h.Backend.CreateInferenceExperiment(ctx, req.Name, req.Type, req.RoleArn, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyInferenceExperimentArn: result.Arn})
}

func (h *Handler) handleDescribeInferenceExperiment(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name string `json:"Name"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeInferenceExperiment(ctx, req.Name)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleStopInferenceExperiment(ctx context.Context, body []byte) error {
	var req struct {
		Name string `json:"Name"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	return h.Backend.StopInferenceExperiment(ctx, req.Name)
}

func (h *Handler) handleDeleteInferenceExperiment(ctx context.Context, body []byte) error {
	var req struct {
		Name string `json:"Name"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	return h.Backend.DeleteInferenceExperiment(ctx, req.Name)
}

// ---------------------------------------------------------------------------
// MlflowTrackingServer handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateMlflowTrackingServer(ctx context.Context, body []byte) ([]byte, error) {
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

	result, err := h.Backend.CreateMlflowTrackingServer(ctx,
		req.TrackingServerName, req.RoleArn, req.MlflowVersion, req.Tags,
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyTrackingServerArn: result.TrackingServerArn})
}

func (h *Handler) handleDescribeMlflowTrackingServer(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		TrackingServerName string `json:"TrackingServerName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrackingServerName == "" {
		return nil, fmt.Errorf("%w: TrackingServerName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeMlflowTrackingServer(ctx, req.TrackingServerName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeleteMlflowTrackingServer(ctx context.Context, body []byte) error {
	var req struct {
		TrackingServerName string `json:"TrackingServerName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrackingServerName == "" {
		return fmt.Errorf("%w: TrackingServerName is required", errInvalidRequest)
	}

	return h.Backend.DeleteMlflowTrackingServer(ctx, req.TrackingServerName)
}

func (h *Handler) handleStartMlflowTrackingServer(ctx context.Context, body []byte) error {
	var req struct {
		TrackingServerName string `json:"TrackingServerName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrackingServerName == "" {
		return fmt.Errorf("%w: TrackingServerName is required", errInvalidRequest)
	}

	return h.Backend.StartMlflowTrackingServer(ctx, req.TrackingServerName)
}

func (h *Handler) handleStopMlflowTrackingServer(ctx context.Context, body []byte) error {
	var req struct {
		TrackingServerName string `json:"TrackingServerName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrackingServerName == "" {
		return fmt.Errorf("%w: TrackingServerName is required", errInvalidRequest)
	}

	return h.Backend.StopMlflowTrackingServer(ctx, req.TrackingServerName)
}

// ---------------------------------------------------------------------------
// ModelCard handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateModelCard(ctx context.Context, body []byte) ([]byte, error) {
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

	result, err := h.Backend.CreateModelCard(ctx, req.ModelCardName, req.Content, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyModelCardArn: result.ModelCardArn})
}

func (h *Handler) handleDescribeModelCard(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ModelCardName string `json:"ModelCardName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelCardName == "" {
		return nil, fmt.Errorf("%w: ModelCardName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeModelCard(ctx, req.ModelCardName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleUpdateModelCard(ctx context.Context, body []byte) ([]byte, error) {
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

	result, err := h.Backend.UpdateModelCard(ctx, req.ModelCardName, req.Content)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyModelCardArn: result.ModelCardArn})
}

func (h *Handler) handleDeleteModelCard(ctx context.Context, body []byte) error {
	var req struct {
		ModelCardName string `json:"ModelCardName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelCardName == "" {
		return fmt.Errorf("%w: ModelCardName is required", errInvalidRequest)
	}

	return h.Backend.DeleteModelCard(ctx, req.ModelCardName)
}

// ---------------------------------------------------------------------------
// OptimizationJob handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateOptimizationJob(ctx context.Context, body []byte) ([]byte, error) {
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

	result, err := h.Backend.CreateOptimizationJob(ctx, req.OptimizationJobName, req.RoleArn, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyOptimizationJobArn: result.OptimizationJobArn})
}

func (h *Handler) handleDescribeOptimizationJob(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		OptimizationJobName string `json:"OptimizationJobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.OptimizationJobName == "" {
		return nil, fmt.Errorf("%w: OptimizationJobName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeOptimizationJob(ctx, req.OptimizationJobName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeleteOptimizationJob(ctx context.Context, body []byte) error {
	var req struct {
		OptimizationJobName string `json:"OptimizationJobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.OptimizationJobName == "" {
		return fmt.Errorf("%w: OptimizationJobName is required", errInvalidRequest)
	}

	return h.Backend.DeleteOptimizationJob(ctx, req.OptimizationJobName)
}

func (h *Handler) handleStopOptimizationJob(ctx context.Context, body []byte) error {
	var req struct {
		OptimizationJobName string `json:"OptimizationJobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.OptimizationJobName == "" {
		return fmt.Errorf("%w: OptimizationJobName is required", errInvalidRequest)
	}

	return h.Backend.StopOptimizationJob(ctx, req.OptimizationJobName)
}

// ---------------------------------------------------------------------------
// StudioLifecycleConfig handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateStudioLifecycleConfig(ctx context.Context, body []byte) ([]byte, error) {
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

	result, err := h.Backend.CreateStudioLifecycleConfig(ctx,
		req.StudioLifecycleConfigName, req.StudioLifecycleConfigAppType, req.Tags,
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyStudioLifecycleConfigArn: result.StudioLifecycleConfigArn})
}

func (h *Handler) handleDescribeStudioLifecycleConfig(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		StudioLifecycleConfigName string `json:"StudioLifecycleConfigName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.StudioLifecycleConfigName == "" {
		return nil, fmt.Errorf("%w: StudioLifecycleConfigName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeStudioLifecycleConfig(ctx, req.StudioLifecycleConfigName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeleteStudioLifecycleConfig(ctx context.Context, body []byte) error {
	var req struct {
		StudioLifecycleConfigName string `json:"StudioLifecycleConfigName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.StudioLifecycleConfigName == "" {
		return fmt.Errorf("%w: StudioLifecycleConfigName is required", errInvalidRequest)
	}

	return h.Backend.DeleteStudioLifecycleConfig(ctx, req.StudioLifecycleConfigName)
}

// ---------------------------------------------------------------------------
// PartnerApp handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreatePartnerApp(ctx context.Context, body []byte) ([]byte, error) {
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

	result, err := h.Backend.CreatePartnerApp(ctx, req.Name, req.Type, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyGenericArn: result.Arn})
}

func (h *Handler) handleDescribePartnerApp(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Arn string `json:"Arn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Arn == "" {
		return nil, fmt.Errorf("%w: Arn is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribePartnerApp(ctx, req.Arn)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeletePartnerApp(ctx context.Context, body []byte) error {
	var req struct {
		Arn string `json:"Arn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Arn == "" {
		return fmt.Errorf("%w: Arn is required", errInvalidRequest)
	}

	return h.Backend.DeletePartnerApp(ctx, req.Arn)
}

// ---------------------------------------------------------------------------
// TrainingPlan handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateTrainingPlan(ctx context.Context, body []byte) ([]byte, error) {
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

	result, err := h.Backend.CreateTrainingPlan(ctx, req.TrainingPlanName, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyTrainingPlanArn: result.TrainingPlanArn})
}

func (h *Handler) handleDescribeTrainingPlan(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		TrainingPlanName string `json:"TrainingPlanName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrainingPlanName == "" {
		return nil, fmt.Errorf("%w: TrainingPlanName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeTrainingPlan(ctx, req.TrainingPlanName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}
