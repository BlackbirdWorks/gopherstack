package sagemaker

import (
	"context"
)

const (
	keyEndpointNameField = "EndpointName"
	keyTagsField         = "Tags"
)

// batch3SupportedOperations returns the 50 real stateful operations implemented in batch 3.
func batch3SupportedOperations() []string {
	return []string{
		// DataQuality job definitions
		"CreateDataQualityJobDefinition",
		"DescribeDataQualityJobDefinition",
		"DeleteDataQualityJobDefinition",
		"ListDataQualityJobDefinitions",
		// ModelBias job definitions
		"CreateModelBiasJobDefinition",
		"DescribeModelBiasJobDefinition",
		"DeleteModelBiasJobDefinition",
		"ListModelBiasJobDefinitions",
		// ModelQuality job definitions
		"CreateModelQualityJobDefinition",
		"DescribeModelQualityJobDefinition",
		"DeleteModelQualityJobDefinition",
		"ListModelQualityJobDefinitions",
		// ModelExplainability job definitions
		"CreateModelExplainabilityJobDefinition",
		"DescribeModelExplainabilityJobDefinition",
		"DeleteModelExplainabilityJobDefinition",
		"ListModelExplainabilityJobDefinitions",
		// MonitoringAlert / MonitoringExecution
		"ListMonitoringAlerts",
		"ListMonitoringAlertHistory",
		"UpdateMonitoringAlert",
		"ListMonitoringExecutions",
		// HumanTaskUI
		"CreateHumanTaskUi",
		"DescribeHumanTaskUi",
		"DeleteHumanTaskUi",
		// Workforce
		"CreateWorkforce",
		"DescribeWorkforce",
		"UpdateWorkforce",
		"DeleteWorkforce",
		"ListWorkforces",
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
		"StartInferenceExperiment",
		"UpdateInferenceExperiment",
		// MlflowTrackingServer
		"CreateMlflowTrackingServer",
		"DescribeMlflowTrackingServer",
		"DeleteMlflowTrackingServer",
		"StartMlflowTrackingServer",
		"StopMlflowTrackingServer",
		"CreatePresignedMlflowTrackingServerUrl",
		// MlflowApp
		"CreateMlflowApp",
		"DescribeMlflowApp",
		"DeleteMlflowApp",
		"UpdateMlflowApp",
		"ListMlflowApps",
		"CreatePresignedMlflowAppUrl",
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
		"UpdatePartnerApp",
		"ListPartnerApps",
		"CreatePartnerAppPresignedUrl",
		// TrainingPlan
		"CreateTrainingPlan",
		"DescribeTrainingPlan",
	}
}

// dispatchBatch3Ops dispatches the 50 real stateful operations (batch 3) by
// delegating to one sub-dispatcher per resource family, so no single switch
// needs a case for every operation.
func (h *Handler) dispatchBatch3Ops(
	ctx context.Context,
	op string,
	body []byte,
) ([]byte, bool, error) {
	if r, ok, err := h.dispatchDataQualityModelBiasOps(ctx, op, body); ok {
		return r, true, err
	}

	if r, ok, err := h.dispatchModelQualityExplainabilityOps(ctx, op, body); ok {
		return r, true, err
	}

	if r, ok, err := h.dispatchMonitoringAlertHumanTaskOps(ctx, op, body); ok {
		return r, true, err
	}

	if r, ok, err := h.dispatchWorkforceFlowOps(ctx, op, body); ok {
		return r, true, err
	}

	if r, ok, err := h.dispatchAppImageInferenceExperimentOps(ctx, op, body); ok {
		return r, true, err
	}

	if r, ok, err := h.dispatchMlflowTrackingServerOps(ctx, op, body); ok {
		return r, true, err
	}

	if r, ok, err := h.dispatchMlflowAppOps(ctx, op, body); ok {
		return r, true, err
	}

	if r, ok, err := h.dispatchModelCardOptimizationOps(ctx, op, body); ok {
		return r, true, err
	}

	if r, ok, err := h.dispatchStudioPartnerAppOps(ctx, op, body); ok {
		return r, true, err
	}

	return h.dispatchTrainingPlanOps(ctx, op, body)
}

func (h *Handler) dispatchDataQualityModelBiasOps(
	ctx context.Context, op string, body []byte,
) ([]byte, bool, error) {
	switch op {
	case "CreateDataQualityJobDefinition":
		r, err := h.handleCreateDataQualityJobDefinition(ctx, body)

		return r, true, err
	case "DescribeDataQualityJobDefinition":
		r, err := h.handleDescribeDataQualityJobDefinition(ctx, body)

		return r, true, err
	case "DeleteDataQualityJobDefinition":
		return nil, true, h.handleDeleteDataQualityJobDefinition(ctx, body)
	case "ListDataQualityJobDefinitions":
		r, err := h.handleListDataQualityJobDefinitions(ctx, body)

		return r, true, err
	case "CreateModelBiasJobDefinition":
		r, err := h.handleCreateModelBiasJobDefinition(ctx, body)

		return r, true, err
	case "DescribeModelBiasJobDefinition":
		r, err := h.handleDescribeModelBiasJobDefinition(ctx, body)

		return r, true, err
	case "DeleteModelBiasJobDefinition":
		return nil, true, h.handleDeleteModelBiasJobDefinition(ctx, body)
	case "ListModelBiasJobDefinitions":
		r, err := h.handleListModelBiasJobDefinitions(ctx, body)

		return r, true, err
	}

	return nil, false, nil
}

func (h *Handler) dispatchModelQualityExplainabilityOps(
	ctx context.Context, op string, body []byte,
) ([]byte, bool, error) {
	switch op {
	case "CreateModelQualityJobDefinition":
		r, err := h.handleCreateModelQualityJobDefinition(ctx, body)

		return r, true, err
	case "DescribeModelQualityJobDefinition":
		r, err := h.handleDescribeModelQualityJobDefinition(ctx, body)

		return r, true, err
	case "DeleteModelQualityJobDefinition":
		return nil, true, h.handleDeleteModelQualityJobDefinition(ctx, body)
	case "ListModelQualityJobDefinitions":
		r, err := h.handleListModelQualityJobDefinitions(ctx, body)

		return r, true, err
	case "CreateModelExplainabilityJobDefinition":
		r, err := h.handleCreateModelExplainabilityJobDefinition(ctx, body)

		return r, true, err
	case "DescribeModelExplainabilityJobDefinition":
		r, err := h.handleDescribeModelExplainabilityJobDefinition(ctx, body)

		return r, true, err
	case "DeleteModelExplainabilityJobDefinition":
		return nil, true, h.handleDeleteModelExplainabilityJobDefinition(ctx, body)
	case "ListModelExplainabilityJobDefinitions":
		r, err := h.handleListModelExplainabilityJobDefinitions(ctx, body)

		return r, true, err
	}

	return nil, false, nil
}

func (h *Handler) dispatchMonitoringAlertHumanTaskOps(
	ctx context.Context, op string, body []byte,
) ([]byte, bool, error) {
	switch op {
	case "ListMonitoringAlerts":
		r, err := h.handleListMonitoringAlerts(ctx, body)

		return r, true, err
	case "ListMonitoringAlertHistory":
		r, err := h.handleListMonitoringAlertHistory(ctx, body)

		return r, true, err
	case "UpdateMonitoringAlert":
		r, err := h.handleUpdateMonitoringAlert(ctx, body)

		return r, true, err
	case "ListMonitoringExecutions":
		r, err := h.handleListMonitoringExecutions(ctx, body)

		return r, true, err
	case "CreateHumanTaskUi":
		r, err := h.handleCreateHumanTaskUI(ctx, body)

		return r, true, err
	case "DescribeHumanTaskUi":
		r, err := h.handleDescribeHumanTaskUI(ctx, body)

		return r, true, err
	case "DeleteHumanTaskUi":
		return nil, true, h.handleDeleteHumanTaskUI(ctx, body)
	}

	return nil, false, nil
}

func (h *Handler) dispatchWorkforceFlowOps(
	ctx context.Context, op string, body []byte,
) ([]byte, bool, error) {
	switch op {
	case "CreateWorkforce":
		r, err := h.handleCreateWorkforce(ctx, body)

		return r, true, err
	case "DescribeWorkforce":
		r, err := h.handleDescribeWorkforce(ctx, body)

		return r, true, err
	case "UpdateWorkforce":
		r, err := h.handleUpdateWorkforce(ctx, body)

		return r, true, err
	case "DeleteWorkforce":
		r, err := h.handleDeleteWorkforce(ctx, body)

		return r, true, err
	case "ListWorkforces":
		r, err := h.handleListWorkforces(ctx, body)

		return r, true, err
	case "CreateFlowDefinition":
		r, err := h.handleCreateFlowDefinition(ctx, body)

		return r, true, err
	case "DescribeFlowDefinition":
		r, err := h.handleDescribeFlowDefinition(ctx, body)

		return r, true, err
	case "DeleteFlowDefinition":
		return nil, true, h.handleDeleteFlowDefinition(ctx, body)
	}

	return nil, false, nil
}

func (h *Handler) dispatchAppImageInferenceExperimentOps(
	ctx context.Context, op string, body []byte,
) ([]byte, bool, error) {
	switch op {
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
	case "CreateInferenceExperiment":
		r, err := h.handleCreateInferenceExperiment(ctx, body)

		return r, true, err
	case "DescribeInferenceExperiment":
		r, err := h.handleDescribeInferenceExperiment(ctx, body)

		return r, true, err
	case "StopInferenceExperiment":
		r, err := h.handleStopInferenceExperiment(ctx, body)

		return r, true, err
	case "DeleteInferenceExperiment":
		r, err := h.handleDeleteInferenceExperiment(ctx, body)

		return r, true, err
	case "StartInferenceExperiment":
		r, err := h.handleStartInferenceExperiment(ctx, body)

		return r, true, err
	case "UpdateInferenceExperiment":
		r, err := h.handleUpdateInferenceExperiment(ctx, body)

		return r, true, err
	}

	return nil, false, nil
}

func (h *Handler) dispatchMlflowTrackingServerOps(
	ctx context.Context, op string, body []byte,
) ([]byte, bool, error) {
	switch op {
	case "CreateMlflowTrackingServer":
		r, err := h.handleCreateMlflowTrackingServer(ctx, body)

		return r, true, err
	case "DescribeMlflowTrackingServer":
		r, err := h.handleDescribeMlflowTrackingServer(ctx, body)

		return r, true, err
	case "DeleteMlflowTrackingServer":
		r, err := h.handleDeleteMlflowTrackingServer(ctx, body)

		return r, true, err
	case "StartMlflowTrackingServer":
		r, err := h.handleStartMlflowTrackingServer(ctx, body)

		return r, true, err
	case "StopMlflowTrackingServer":
		r, err := h.handleStopMlflowTrackingServer(ctx, body)

		return r, true, err
	case "CreatePresignedMlflowTrackingServerUrl":
		r, err := h.handleCreatePresignedMlflowTrackingServerURL(ctx, body)

		return r, true, err
	}

	return nil, false, nil
}

func (h *Handler) dispatchMlflowAppOps(
	ctx context.Context, op string, body []byte,
) ([]byte, bool, error) {
	switch op {
	case "CreateMlflowApp":
		r, err := h.handleCreateMlflowApp(ctx, body)

		return r, true, err
	case "DescribeMlflowApp":
		r, err := h.handleDescribeMlflowApp(ctx, body)

		return r, true, err
	case "DeleteMlflowApp":
		r, err := h.handleDeleteMlflowApp(ctx, body)

		return r, true, err
	case "UpdateMlflowApp":
		r, err := h.handleUpdateMlflowApp(ctx, body)

		return r, true, err
	case "ListMlflowApps":
		r, err := h.handleListMlflowApps(ctx, body)

		return r, true, err
	case "CreatePresignedMlflowAppUrl":
		r, err := h.handleCreatePresignedMlflowAppURL(ctx, body)

		return r, true, err
	}

	return nil, false, nil
}

func (h *Handler) dispatchModelCardOptimizationOps(
	ctx context.Context, op string, body []byte,
) ([]byte, bool, error) {
	switch op {
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
	}

	return nil, false, nil
}

func (h *Handler) dispatchStudioPartnerAppOps(
	ctx context.Context, op string, body []byte,
) ([]byte, bool, error) {
	switch op {
	case "CreateStudioLifecycleConfig":
		r, err := h.handleCreateStudioLifecycleConfig(ctx, body)

		return r, true, err
	case "DescribeStudioLifecycleConfig":
		r, err := h.handleDescribeStudioLifecycleConfig(ctx, body)

		return r, true, err
	case "DeleteStudioLifecycleConfig":
		return nil, true, h.handleDeleteStudioLifecycleConfig(ctx, body)
	case "CreatePartnerApp":
		r, err := h.handleCreatePartnerApp(ctx, body)

		return r, true, err
	case "DescribePartnerApp":
		r, err := h.handleDescribePartnerApp(ctx, body)

		return r, true, err
	case "DeletePartnerApp":
		r, err := h.handleDeletePartnerApp(ctx, body)

		return r, true, err
	case "UpdatePartnerApp":
		r, err := h.handleUpdatePartnerApp(ctx, body)

		return r, true, err
	case "ListPartnerApps":
		r, err := h.handleListPartnerApps(ctx, body)

		return r, true, err
	case "CreatePartnerAppPresignedUrl":
		r, err := h.handleCreatePartnerAppPresignedURL(ctx, body)

		return r, true, err
	}

	return nil, false, nil
}

func (h *Handler) dispatchTrainingPlanOps(
	ctx context.Context, op string, body []byte,
) ([]byte, bool, error) {
	switch op {
	case "CreateTrainingPlan":
		r, err := h.handleCreateTrainingPlan(ctx, body)

		return r, true, err
	case "DescribeTrainingPlan":
		r, err := h.handleDescribeTrainingPlan(ctx, body)

		return r, true, err
	}

	return nil, false, nil
}
