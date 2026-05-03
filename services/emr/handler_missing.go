package emr

import "context"

type describeJobFlowsInput struct{}
type describeJobFlowsOutput struct{}

func (h *Handler) handleDescribeJobFlows(_ context.Context, _ *describeJobFlowsInput) (*describeJobFlowsOutput, error) {
	return &describeJobFlowsOutput{}, nil
}

type describeNotebookExecutionInput struct{}
type describeNotebookExecutionOutput struct{}

func (h *Handler) handleDescribeNotebookExecution(_ context.Context, _ *describeNotebookExecutionInput) (*describeNotebookExecutionOutput, error) {
	return &describeNotebookExecutionOutput{}, nil
}

type describePersistentAppUIInput struct{}
type describePersistentAppUIOutput struct{}

func (h *Handler) handleDescribePersistentAppUI(_ context.Context, _ *describePersistentAppUIInput) (*describePersistentAppUIOutput, error) {
	return &describePersistentAppUIOutput{}, nil
}

type describeReleaseLabelInput struct{}
type describeReleaseLabelOutput struct{}

func (h *Handler) handleDescribeReleaseLabel(_ context.Context, _ *describeReleaseLabelInput) (*describeReleaseLabelOutput, error) {
	return &describeReleaseLabelOutput{}, nil
}

type describeStepInput struct{}
type describeStepOutput struct{}

func (h *Handler) handleDescribeStep(_ context.Context, _ *describeStepInput) (*describeStepOutput, error) {
	return &describeStepOutput{}, nil
}

type describeStudioInput struct{}
type describeStudioOutput struct{}

func (h *Handler) handleDescribeStudio(_ context.Context, _ *describeStudioInput) (*describeStudioOutput, error) {
	return &describeStudioOutput{}, nil
}

type getBlockPublicAccessConfigurationInput struct{}
type getBlockPublicAccessConfigurationOutput struct{}

func (h *Handler) handleGetBlockPublicAccessConfiguration(_ context.Context, _ *getBlockPublicAccessConfigurationInput) (*getBlockPublicAccessConfigurationOutput, error) {
	return &getBlockPublicAccessConfigurationOutput{}, nil
}

type getClusterSessionCredentialsInput struct{}
type getClusterSessionCredentialsOutput struct{}

func (h *Handler) handleGetClusterSessionCredentials(_ context.Context, _ *getClusterSessionCredentialsInput) (*getClusterSessionCredentialsOutput, error) {
	return &getClusterSessionCredentialsOutput{}, nil
}

type getOnClusterAppUIPresignedURLInput struct{}
type getOnClusterAppUIPresignedURLOutput struct{}

func (h *Handler) handleGetOnClusterAppUIPresignedURL(_ context.Context, _ *getOnClusterAppUIPresignedURLInput) (*getOnClusterAppUIPresignedURLOutput, error) {
	return &getOnClusterAppUIPresignedURLOutput{}, nil
}

type getPersistentAppUIPresignedURLInput struct{}
type getPersistentAppUIPresignedURLOutput struct{}

func (h *Handler) handleGetPersistentAppUIPresignedURL(_ context.Context, _ *getPersistentAppUIPresignedURLInput) (*getPersistentAppUIPresignedURLOutput, error) {
	return &getPersistentAppUIPresignedURLOutput{}, nil
}

type getStudioSessionMappingInput struct{}
type getStudioSessionMappingOutput struct{}

func (h *Handler) handleGetStudioSessionMapping(_ context.Context, _ *getStudioSessionMappingInput) (*getStudioSessionMappingOutput, error) {
	return &getStudioSessionMappingOutput{}, nil
}

type listInstancesInput struct{}
type listInstancesOutput struct{}

func (h *Handler) handleListInstances(_ context.Context, _ *listInstancesInput) (*listInstancesOutput, error) {
	return &listInstancesOutput{}, nil
}

type listNotebookExecutionsInput struct{}
type listNotebookExecutionsOutput struct{}

func (h *Handler) handleListNotebookExecutions(_ context.Context, _ *listNotebookExecutionsInput) (*listNotebookExecutionsOutput, error) {
	return &listNotebookExecutionsOutput{}, nil
}

type listReleaseLabelsInput struct{}
type listReleaseLabelsOutput struct{}

func (h *Handler) handleListReleaseLabels(_ context.Context, _ *listReleaseLabelsInput) (*listReleaseLabelsOutput, error) {
	return &listReleaseLabelsOutput{}, nil
}

type listSecurityConfigurationsInput struct{}
type listSecurityConfigurationsOutput struct{}

func (h *Handler) handleListSecurityConfigurations(_ context.Context, _ *listSecurityConfigurationsInput) (*listSecurityConfigurationsOutput, error) {
	return &listSecurityConfigurationsOutput{}, nil
}

type listStudioSessionMappingsInput struct{}
type listStudioSessionMappingsOutput struct{}

func (h *Handler) handleListStudioSessionMappings(_ context.Context, _ *listStudioSessionMappingsInput) (*listStudioSessionMappingsOutput, error) {
	return &listStudioSessionMappingsOutput{}, nil
}

type listStudiosInput struct{}
type listStudiosOutput struct{}

func (h *Handler) handleListStudios(_ context.Context, _ *listStudiosInput) (*listStudiosOutput, error) {
	return &listStudiosOutput{}, nil
}

type listSupportedInstanceTypesInput struct{}
type listSupportedInstanceTypesOutput struct{}

func (h *Handler) handleListSupportedInstanceTypes(_ context.Context, _ *listSupportedInstanceTypesInput) (*listSupportedInstanceTypesOutput, error) {
	return &listSupportedInstanceTypesOutput{}, nil
}

type modifyClusterInput struct{}
type modifyClusterOutput struct{}

func (h *Handler) handleModifyCluster(_ context.Context, _ *modifyClusterInput) (*modifyClusterOutput, error) {
	return &modifyClusterOutput{}, nil
}

type modifyInstanceFleetInput struct{}
type modifyInstanceFleetOutput struct{}

func (h *Handler) handleModifyInstanceFleet(_ context.Context, _ *modifyInstanceFleetInput) (*modifyInstanceFleetOutput, error) {
	return &modifyInstanceFleetOutput{}, nil
}

type modifyInstanceGroupsInput struct{}
type modifyInstanceGroupsOutput struct{}

func (h *Handler) handleModifyInstanceGroups(_ context.Context, _ *modifyInstanceGroupsInput) (*modifyInstanceGroupsOutput, error) {
	return &modifyInstanceGroupsOutput{}, nil
}

type putAutoScalingPolicyInput struct{}
type putAutoScalingPolicyOutput struct{}

func (h *Handler) handlePutAutoScalingPolicy(_ context.Context, _ *putAutoScalingPolicyInput) (*putAutoScalingPolicyOutput, error) {
	return &putAutoScalingPolicyOutput{}, nil
}

type putAutoTerminationPolicyInput struct{}
type putAutoTerminationPolicyOutput struct{}

func (h *Handler) handlePutAutoTerminationPolicy(_ context.Context, _ *putAutoTerminationPolicyInput) (*putAutoTerminationPolicyOutput, error) {
	return &putAutoTerminationPolicyOutput{}, nil
}

type putBlockPublicAccessConfigurationInput struct{}
type putBlockPublicAccessConfigurationOutput struct{}

func (h *Handler) handlePutBlockPublicAccessConfiguration(_ context.Context, _ *putBlockPublicAccessConfigurationInput) (*putBlockPublicAccessConfigurationOutput, error) {
	return &putBlockPublicAccessConfigurationOutput{}, nil
}

type putManagedScalingPolicyInput struct{}
type putManagedScalingPolicyOutput struct{}

func (h *Handler) handlePutManagedScalingPolicy(_ context.Context, _ *putManagedScalingPolicyInput) (*putManagedScalingPolicyOutput, error) {
	return &putManagedScalingPolicyOutput{}, nil
}

type removeAutoScalingPolicyInput struct{}
type removeAutoScalingPolicyOutput struct{}

func (h *Handler) handleRemoveAutoScalingPolicy(_ context.Context, _ *removeAutoScalingPolicyInput) (*removeAutoScalingPolicyOutput, error) {
	return &removeAutoScalingPolicyOutput{}, nil
}

type removeAutoTerminationPolicyInput struct{}
type removeAutoTerminationPolicyOutput struct{}

func (h *Handler) handleRemoveAutoTerminationPolicy(_ context.Context, _ *removeAutoTerminationPolicyInput) (*removeAutoTerminationPolicyOutput, error) {
	return &removeAutoTerminationPolicyOutput{}, nil
}

type removeManagedScalingPolicyInput struct{}
type removeManagedScalingPolicyOutput struct{}

func (h *Handler) handleRemoveManagedScalingPolicy(_ context.Context, _ *removeManagedScalingPolicyInput) (*removeManagedScalingPolicyOutput, error) {
	return &removeManagedScalingPolicyOutput{}, nil
}

type setKeepJobFlowAliveWhenNoStepsInput struct{}
type setKeepJobFlowAliveWhenNoStepsOutput struct{}

func (h *Handler) handleSetKeepJobFlowAliveWhenNoSteps(_ context.Context, _ *setKeepJobFlowAliveWhenNoStepsInput) (*setKeepJobFlowAliveWhenNoStepsOutput, error) {
	return &setKeepJobFlowAliveWhenNoStepsOutput{}, nil
}

type setTerminationProtectionInput struct{}
type setTerminationProtectionOutput struct{}

func (h *Handler) handleSetTerminationProtection(_ context.Context, _ *setTerminationProtectionInput) (*setTerminationProtectionOutput, error) {
	return &setTerminationProtectionOutput{}, nil
}

type setUnhealthyNodeReplacementInput struct{}
type setUnhealthyNodeReplacementOutput struct{}

func (h *Handler) handleSetUnhealthyNodeReplacement(_ context.Context, _ *setUnhealthyNodeReplacementInput) (*setUnhealthyNodeReplacementOutput, error) {
	return &setUnhealthyNodeReplacementOutput{}, nil
}

type setVisibleToAllUsersInput struct{}
type setVisibleToAllUsersOutput struct{}

func (h *Handler) handleSetVisibleToAllUsers(_ context.Context, _ *setVisibleToAllUsersInput) (*setVisibleToAllUsersOutput, error) {
	return &setVisibleToAllUsersOutput{}, nil
}

type startNotebookExecutionInput struct{}
type startNotebookExecutionOutput struct{}

func (h *Handler) handleStartNotebookExecution(_ context.Context, _ *startNotebookExecutionInput) (*startNotebookExecutionOutput, error) {
	return &startNotebookExecutionOutput{}, nil
}

type stopNotebookExecutionInput struct{}
type stopNotebookExecutionOutput struct{}

func (h *Handler) handleStopNotebookExecution(_ context.Context, _ *stopNotebookExecutionInput) (*stopNotebookExecutionOutput, error) {
	return &stopNotebookExecutionOutput{}, nil
}

type updateStudioInput struct{}
type updateStudioOutput struct{}

func (h *Handler) handleUpdateStudio(_ context.Context, _ *updateStudioInput) (*updateStudioOutput, error) {
	return &updateStudioOutput{}, nil
}

type updateStudioSessionMappingInput struct{}
type updateStudioSessionMappingOutput struct{}

func (h *Handler) handleUpdateStudioSessionMapping(_ context.Context, _ *updateStudioSessionMappingInput) (*updateStudioSessionMappingOutput, error) {
	return &updateStudioSessionMappingOutput{}, nil
}

