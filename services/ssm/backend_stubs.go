package ssm

import (
	"fmt"
	"maps"
	"slices"
	"strconv"
	"time"

	"github.com/google/uuid"
)

// backend_stubs.go provides stub implementations for the 123 SSM operations
// that are acknowledged but not yet fully implemented.  Each stub returns an
// empty success response, which is sufficient for the SDK-completeness test
// and for callers that only need the operation to not error.

// --- Stub input/output types ---

// StubOutput is a generic empty response used by all stub operations.
type StubOutput struct{}

// CreateResourceDataSyncInput is the request for CreateResourceDataSync.
type CreateResourceDataSyncInput struct{}

// DeleteActivationInput is the request for DeleteActivation.
type DeleteActivationInput struct {
	ActivationID string `json:"ActivationId"`
}

// DeleteAssociationInput is the request for DeleteAssociation.
type DeleteAssociationInput struct {
	AssociationID string `json:"AssociationId,omitempty"`
	Name          string `json:"Name,omitempty"`
	InstanceID    string `json:"InstanceId,omitempty"`
}

// DeleteInventoryInput is the request for DeleteInventory.
type DeleteInventoryInput struct{}

// DeleteMaintenanceWindowInput is the request for DeleteMaintenanceWindow.
type DeleteMaintenanceWindowInput struct {
	WindowID string `json:"WindowId"`
}

// DeleteOpsItemInput is the request for DeleteOpsItem.
type DeleteOpsItemInput struct {
	OpsItemID string `json:"OpsItemId"`
}

// DeleteOpsMetadataInput is the request for DeleteOpsMetadata.
type DeleteOpsMetadataInput struct {
	OpsMetadataArn string `json:"OpsMetadataArn"`
}

// DeletePatchBaselineInput is the request for DeletePatchBaseline.
type DeletePatchBaselineInput struct {
	BaselineID string `json:"BaselineId"`
}

// DeleteResourceDataSyncInput is the request for DeleteResourceDataSync.
type DeleteResourceDataSyncInput struct{}

// DeleteResourcePolicyInput is the request for DeleteResourcePolicy.
type DeleteResourcePolicyInput struct{}

// DeregisterManagedInstanceInput is the request for DeregisterManagedInstance.
type DeregisterManagedInstanceInput struct{}

// DeregisterPatchBaselineForPatchGroupInput is the request for DeregisterPatchBaselineForPatchGroup.
type DeregisterPatchBaselineForPatchGroupInput struct {
	BaselineID string `json:"BaselineId"`
	PatchGroup string `json:"PatchGroup"`
}

// DeregisterTargetFromMaintenanceWindowInput is the request for DeregisterTargetFromMaintenanceWindow.
type DeregisterTargetFromMaintenanceWindowInput struct {
	WindowID       string `json:"WindowId"`
	WindowTargetID string `json:"WindowTargetId"`
}

// DeregisterTaskFromMaintenanceWindowInput is the request for DeregisterTaskFromMaintenanceWindow.
type DeregisterTaskFromMaintenanceWindowInput struct {
	WindowID     string `json:"WindowId"`
	WindowTaskID string `json:"WindowTaskId"`
}

// DescribeActivationsInput is the request for DescribeActivations.
type DescribeActivationsInput struct{}

// DescribeActivationsOutput is the response for DescribeActivations.
type DescribeActivationsOutput struct {
	ActivationList []Activation `json:"ActivationList"`
}

// DescribeAssociationInput is the request for DescribeAssociation.
type DescribeAssociationInput struct {
	AssociationID string `json:"AssociationId,omitempty"`
	Name          string `json:"Name,omitempty"`
	InstanceID    string `json:"InstanceId,omitempty"`
}

// DescribeAssociationOutput is the response for DescribeAssociation.
type DescribeAssociationOutput struct {
	AssociationDescription Association `json:"AssociationDescription"`
}

// DescribeAssociationExecutionTargetsInput is the request for DescribeAssociationExecutionTargets.
type DescribeAssociationExecutionTargetsInput struct{}

// DescribeAssociationExecutionTargetsOutput is the response for DescribeAssociationExecutionTargets.
type DescribeAssociationExecutionTargetsOutput struct{}

// DescribeAssociationExecutionsInput is the request for DescribeAssociationExecutions.
type DescribeAssociationExecutionsInput struct{}

// DescribeAssociationExecutionsOutput is the response for DescribeAssociationExecutions.
type DescribeAssociationExecutionsOutput struct{}

// DescribeAutomationExecutionsInput is the request for DescribeAutomationExecutions.
type DescribeAutomationExecutionsInput struct{}

// DescribeAutomationExecutionsOutput is the response for DescribeAutomationExecutions.
type DescribeAutomationExecutionsOutput struct{}

// DescribeAutomationStepExecutionsInput is the request for DescribeAutomationStepExecutions.
type DescribeAutomationStepExecutionsInput struct{}

// DescribeAutomationStepExecutionsOutput is the response for DescribeAutomationStepExecutions.
type DescribeAutomationStepExecutionsOutput struct{}

// DescribeAvailablePatchesInput is the request for DescribeAvailablePatches.
type DescribeAvailablePatchesInput struct{}

// DescribeAvailablePatchesOutput is the response for DescribeAvailablePatches.
type DescribeAvailablePatchesOutput struct{}

// DescribeEffectiveInstanceAssociationsInput is the request for DescribeEffectiveInstanceAssociations.
type DescribeEffectiveInstanceAssociationsInput struct{}

// DescribeEffectiveInstanceAssociationsOutput is the response for DescribeEffectiveInstanceAssociations.
type DescribeEffectiveInstanceAssociationsOutput struct{}

// DescribeEffectivePatchesForPatchBaselineInput is the request for DescribeEffectivePatchesForPatchBaseline.
type DescribeEffectivePatchesForPatchBaselineInput struct{}

// DescribeEffectivePatchesForPatchBaselineOutput is the response for DescribeEffectivePatchesForPatchBaseline.
type DescribeEffectivePatchesForPatchBaselineOutput struct{}

// DescribeInstanceAssociationsStatusInput is the request for DescribeInstanceAssociationsStatus.
type DescribeInstanceAssociationsStatusInput struct{}

// DescribeInstanceAssociationsStatusOutput is the response for DescribeInstanceAssociationsStatus.
type DescribeInstanceAssociationsStatusOutput struct{}

// DescribeInstanceInformationInput is the request for DescribeInstanceInformation.
type DescribeInstanceInformationInput struct{}

// DescribeInstanceInformationOutput is the response for DescribeInstanceInformation.
type DescribeInstanceInformationOutput struct{}

// DescribeInstancePatchStatesInput is the request for DescribeInstancePatchStates.
type DescribeInstancePatchStatesInput struct{}

// DescribeInstancePatchStatesOutput is the response for DescribeInstancePatchStates.
type DescribeInstancePatchStatesOutput struct{}

// DescribeInstancePatchStatesForPatchGroupInput is the request for DescribeInstancePatchStatesForPatchGroup.
type DescribeInstancePatchStatesForPatchGroupInput struct{}

// DescribeInstancePatchStatesForPatchGroupOutput is the response for DescribeInstancePatchStatesForPatchGroup.
type DescribeInstancePatchStatesForPatchGroupOutput struct{}

// DescribeInstancePatchesInput is the request for DescribeInstancePatches.
type DescribeInstancePatchesInput struct{}

// DescribeInstancePatchesOutput is the response for DescribeInstancePatches.
type DescribeInstancePatchesOutput struct{}

// DescribeInstancePropertiesInput is the request for DescribeInstanceProperties.
type DescribeInstancePropertiesInput struct{}

// DescribeInstancePropertiesOutput is the response for DescribeInstanceProperties.
type DescribeInstancePropertiesOutput struct{}

// DescribeInventoryDeletionsInput is the request for DescribeInventoryDeletions.
type DescribeInventoryDeletionsInput struct{}

// DescribeInventoryDeletionsOutput is the response for DescribeInventoryDeletions.
type DescribeInventoryDeletionsOutput struct{}

// DescribeMaintenanceWindowExecutionTaskInvocationsInput is the request payload.
type DescribeMaintenanceWindowExecutionTaskInvocationsInput struct{}

// DescribeMaintenanceWindowExecutionTaskInvocationsOutput is the response payload.
type DescribeMaintenanceWindowExecutionTaskInvocationsOutput struct{}

// DescribeMaintenanceWindowExecutionTasksInput is the request payload.
type DescribeMaintenanceWindowExecutionTasksInput struct{}

// DescribeMaintenanceWindowExecutionTasksOutput is the response payload.
type DescribeMaintenanceWindowExecutionTasksOutput struct{}

// DescribeMaintenanceWindowExecutionsInput is the request payload.
type DescribeMaintenanceWindowExecutionsInput struct{}

// DescribeMaintenanceWindowExecutionsOutput is the response payload.
type DescribeMaintenanceWindowExecutionsOutput struct{}

// DescribeMaintenanceWindowScheduleInput is the request payload.
type DescribeMaintenanceWindowScheduleInput struct{}

// DescribeMaintenanceWindowScheduleOutput is the response payload.
type DescribeMaintenanceWindowScheduleOutput struct{}

// DescribeMaintenanceWindowTargetsInput is the request payload.
type DescribeMaintenanceWindowTargetsInput struct {
	WindowID string `json:"WindowId"`
}

// DescribeMaintenanceWindowTargetsOutput is the response payload.
type DescribeMaintenanceWindowTargetsOutput struct {
	Targets []MaintenanceWindowTarget `json:"Targets"`
}

// DescribeMaintenanceWindowTasksInput is the request payload.
type DescribeMaintenanceWindowTasksInput struct {
	WindowID string `json:"WindowId"`
}

// DescribeMaintenanceWindowTasksOutput is the response payload.
type DescribeMaintenanceWindowTasksOutput struct {
	Tasks []MaintenanceWindowTask `json:"Tasks"`
}

// DescribeMaintenanceWindowsInput is the request payload for DescribeMaintenanceWindows.
type DescribeMaintenanceWindowsInput struct {
	MaxResults *int64 `json:"MaxResults,omitempty"`
	NextToken  string `json:"NextToken,omitempty"`
}

// DescribeMaintenanceWindowsOutput is the response payload for DescribeMaintenanceWindows.
type DescribeMaintenanceWindowsOutput struct {
	NextToken        string                      `json:"NextToken,omitempty"`
	WindowIdentities []MaintenanceWindowIdentity `json:"WindowIdentities"`
}

// MaintenanceWindowIdentity is a lightweight maintenance window listing entry.
type MaintenanceWindowIdentity struct {
	WindowID    string `json:"WindowId"`
	Name        string `json:"Name"`
	Description string `json:"Description,omitempty"`
	Schedule    string `json:"Schedule"`
	Duration    int32  `json:"Duration"`
	Cutoff      int32  `json:"Cutoff"`
	Enabled     bool   `json:"Enabled"`
}

// DescribeMaintenanceWindowsForTargetInput is the request payload.
type DescribeMaintenanceWindowsForTargetInput struct{}

// DescribeMaintenanceWindowsForTargetOutput is the response payload.
type DescribeMaintenanceWindowsForTargetOutput struct{}

// DescribeOpsItemsInput is the request payload for DescribeOpsItems.
type DescribeOpsItemsInput struct {
	MaxResults *int64 `json:"MaxResults,omitempty"`
	NextToken  string `json:"NextToken,omitempty"`
}

// DescribeOpsItemsOutput is the response payload for DescribeOpsItems.
type DescribeOpsItemsOutput struct {
	NextToken        string           `json:"NextToken,omitempty"`
	OpsItemSummaries []OpsItemSummary `json:"OpsItemSummaries"`
}

// OpsItemSummary is a lightweight OpsItem listing entry.
type OpsItemSummary struct {
	OpsItemID   string  `json:"OpsItemId"`
	Title       string  `json:"Title"`
	Status      string  `json:"Status"`
	Source      string  `json:"Source"`
	CreatedTime float64 `json:"CreatedTime"`
}

// DescribePatchBaselinesInput is the request payload for DescribePatchBaselines.
type DescribePatchBaselinesInput struct {
	MaxResults *int64 `json:"MaxResults,omitempty"`
	NextToken  string `json:"NextToken,omitempty"`
}

// DescribePatchBaselinesOutput is the response payload for DescribePatchBaselines.
type DescribePatchBaselinesOutput struct {
	NextToken          string                  `json:"NextToken,omitempty"`
	BaselineIdentities []PatchBaselineIdentity `json:"BaselineIdentities"`
}

// PatchBaselineIdentity is a lightweight patch baseline listing entry.
type PatchBaselineIdentity struct {
	BaselineID      string `json:"BaselineId"`
	BaselineName    string `json:"BaselineName"`
	OperatingSystem string `json:"OperatingSystem,omitempty"`
	Description     string `json:"Description,omitempty"`
}

// DescribePatchGroupStateInput is the request payload.
type DescribePatchGroupStateInput struct{}

// DescribePatchGroupStateOutput is the response payload.
type DescribePatchGroupStateOutput struct{}

// DescribePatchGroupsInput is the request payload.
type DescribePatchGroupsInput struct{}

// DescribePatchGroupsOutput is the response payload.
type DescribePatchGroupsOutput struct{}

// DescribePatchPropertiesInput is the request payload.
type DescribePatchPropertiesInput struct{}

// DescribePatchPropertiesOutput is the response payload.
type DescribePatchPropertiesOutput struct{}

// DescribeSessionsInput is the request payload.
type DescribeSessionsInput struct{}

// DescribeSessionsOutput is the response payload.
type DescribeSessionsOutput struct{}

// DisassociateOpsItemRelatedItemInput is the request payload.
type DisassociateOpsItemRelatedItemInput struct{}

// GetAccessTokenInput is the request payload.
type GetAccessTokenInput struct{}

// GetAccessTokenOutput is the response payload.
type GetAccessTokenOutput struct{}

// GetAutomationExecutionInput is the request payload.
type GetAutomationExecutionInput struct{}

// GetAutomationExecutionOutput is the response payload.
type GetAutomationExecutionOutput struct{}

// GetCalendarStateInput is the request payload.
type GetCalendarStateInput struct{}

// GetCalendarStateOutput is the response payload.
type GetCalendarStateOutput struct{}

// GetConnectionStatusInput is the request payload.
type GetConnectionStatusInput struct{}

// GetConnectionStatusOutput is the response payload.
type GetConnectionStatusOutput struct{}

// GetDefaultPatchBaselineInput is the request payload.
type GetDefaultPatchBaselineInput struct{}

// GetDefaultPatchBaselineOutput is the response payload.
type GetDefaultPatchBaselineOutput struct{}

// GetDeployablePatchSnapshotForInstanceInput is the request payload.
type GetDeployablePatchSnapshotForInstanceInput struct{}

// GetDeployablePatchSnapshotForInstanceOutput is the response payload.
type GetDeployablePatchSnapshotForInstanceOutput struct{}

// GetExecutionPreviewInput is the request payload.
type GetExecutionPreviewInput struct{}

// GetExecutionPreviewOutput is the response payload.
type GetExecutionPreviewOutput struct{}

// GetInventoryInput is the request payload.
type GetInventoryInput struct{}

// GetInventoryOutput is the response payload.
type GetInventoryOutput struct{}

// GetInventorySchemaInput is the request payload.
type GetInventorySchemaInput struct{}

// GetInventorySchemaOutput is the response payload.
type GetInventorySchemaOutput struct{}

// GetMaintenanceWindowInput is the request payload for GetMaintenanceWindow.
type GetMaintenanceWindowInput struct {
	WindowID string `json:"WindowId"`
}

// GetMaintenanceWindowOutput is the response payload for GetMaintenanceWindow.
type GetMaintenanceWindowOutput struct {
	MaintenanceWindow
}

// GetMaintenanceWindowExecutionInput is the request payload.
type GetMaintenanceWindowExecutionInput struct{}

// GetMaintenanceWindowExecutionOutput is the response payload.
type GetMaintenanceWindowExecutionOutput struct{}

// GetMaintenanceWindowExecutionTaskInput is the request payload.
type GetMaintenanceWindowExecutionTaskInput struct{}

// GetMaintenanceWindowExecutionTaskOutput is the response payload.
type GetMaintenanceWindowExecutionTaskOutput struct{}

// GetMaintenanceWindowExecutionTaskInvocationInput is the request payload.
type GetMaintenanceWindowExecutionTaskInvocationInput struct{}

// GetMaintenanceWindowExecutionTaskInvocationOutput is the response payload.
type GetMaintenanceWindowExecutionTaskInvocationOutput struct{}

// GetMaintenanceWindowTaskInput is the request payload.
type GetMaintenanceWindowTaskInput struct{}

// GetMaintenanceWindowTaskOutput is the response payload.
type GetMaintenanceWindowTaskOutput struct{}

// GetOpsItemInput is the request payload for GetOpsItem.
type GetOpsItemInput struct {
	OpsItemID string `json:"OpsItemId"`
}

// GetOpsItemOutput is the response payload for GetOpsItem.
type GetOpsItemOutput struct {
	OpsItem OpsItem `json:"OpsItem"`
}

// GetOpsMetadataInput is the request payload for GetOpsMetadata.
type GetOpsMetadataInput struct {
	OpsMetadataArn string `json:"OpsMetadataArn"`
}

// GetOpsMetadataOutput is the response payload for GetOpsMetadata.
type GetOpsMetadataOutput struct {
	OpsMetadata
}

// GetOpsSummaryInput is the request payload.
type GetOpsSummaryInput struct{}

// GetOpsSummaryOutput is the response payload.
type GetOpsSummaryOutput struct{}

// GetPatchBaselineInput is the request payload for GetPatchBaseline.
type GetPatchBaselineInput struct {
	BaselineID string `json:"BaselineId"`
}

// GetPatchBaselineOutput is the response payload for GetPatchBaseline.
type GetPatchBaselineOutput struct {
	PatchBaseline
}

// GetPatchBaselineForPatchGroupInput is the request payload.
type GetPatchBaselineForPatchGroupInput struct{}

// GetPatchBaselineForPatchGroupOutput is the response payload.
type GetPatchBaselineForPatchGroupOutput struct{}

// GetResourcePoliciesInput is the request payload.
type GetResourcePoliciesInput struct{}

// GetResourcePoliciesOutput is the response payload.
type GetResourcePoliciesOutput struct{}

// GetServiceSettingInput is the request payload.
type GetServiceSettingInput struct{}

// GetServiceSettingOutput is the response payload.
type GetServiceSettingOutput struct{}

// LabelParameterVersionInput is the request payload.
type LabelParameterVersionInput struct {
	Name    string   `json:"Name"`
	Labels  []string `json:"Labels"`
	Version int64    `json:"ParameterVersion,omitempty"`
}

// LabelParameterVersionOutput is the response payload.
type LabelParameterVersionOutput struct {
	InvalidLabels    []string `json:"InvalidLabels,omitempty"`
	ParameterVersion int64    `json:"ParameterVersion"`
}

// ListAssociationVersionsInput is the request payload.
type ListAssociationVersionsInput struct{}

// ListAssociationVersionsOutput is the response payload.
type ListAssociationVersionsOutput struct{}

// ListAssociationsInput is the request payload.
type ListAssociationsInput struct{}

// ListAssociationsOutput is the response payload.
type ListAssociationsOutput struct {
	Associations []Association `json:"Associations"`
}

// ListComplianceItemsInput is the request payload.
type ListComplianceItemsInput struct{}

// ListComplianceItemsOutput is the response payload.
type ListComplianceItemsOutput struct{}

// ListComplianceSummariesInput is the request payload.
type ListComplianceSummariesInput struct{}

// ListComplianceSummariesOutput is the response payload.
type ListComplianceSummariesOutput struct{}

// ListDocumentMetadataHistoryInput is the request payload.
type ListDocumentMetadataHistoryInput struct{}

// ListDocumentMetadataHistoryOutput is the response payload.
type ListDocumentMetadataHistoryOutput struct{}

// ListInventoryEntriesInput is the request payload.
type ListInventoryEntriesInput struct{}

// ListInventoryEntriesOutput is the response payload.
type ListInventoryEntriesOutput struct{}

// ListNodesInput is the request payload.
type ListNodesInput struct{}

// ListNodesOutput is the response payload.
type ListNodesOutput struct{}

// ListNodesSummaryInput is the request payload.
type ListNodesSummaryInput struct{}

// ListNodesSummaryOutput is the response payload.
type ListNodesSummaryOutput struct{}

// ListOpsItemEventsInput is the request payload.
type ListOpsItemEventsInput struct{}

// ListOpsItemEventsOutput is the response payload.
type ListOpsItemEventsOutput struct{}

// ListOpsItemRelatedItemsInput is the request payload.
type ListOpsItemRelatedItemsInput struct{}

// ListOpsItemRelatedItemsOutput is the response payload.
type ListOpsItemRelatedItemsOutput struct{}

// ListOpsMetadataInput is the request payload.
type ListOpsMetadataInput struct{}

// ListOpsMetadataOutput is the response payload.
type ListOpsMetadataOutput struct{}

// ListResourceComplianceSummariesInput is the request payload.
type ListResourceComplianceSummariesInput struct{}

// ListResourceComplianceSummariesOutput is the response payload.
type ListResourceComplianceSummariesOutput struct{}

// ListResourceDataSyncInput is the request payload.
type ListResourceDataSyncInput struct{}

// ListResourceDataSyncOutput is the response payload.
type ListResourceDataSyncOutput struct{}

// PutComplianceItemsInput is the request payload.
type PutComplianceItemsInput struct{}

// PutInventoryInput is the request payload.
type PutInventoryInput struct{}

// PutResourcePolicyInput is the request payload.
type PutResourcePolicyInput struct{}

// PutResourcePolicyOutput is the response payload.
type PutResourcePolicyOutput struct{}

// RegisterDefaultPatchBaselineInput is the request payload.
type RegisterDefaultPatchBaselineInput struct{}

// RegisterDefaultPatchBaselineOutput is the response payload.
type RegisterDefaultPatchBaselineOutput struct{}

// RegisterPatchBaselineForPatchGroupInput is the request payload.
type RegisterPatchBaselineForPatchGroupInput struct {
	BaselineID string `json:"BaselineId"`
	PatchGroup string `json:"PatchGroup"`
}

// RegisterPatchBaselineForPatchGroupOutput is the response payload.
type RegisterPatchBaselineForPatchGroupOutput struct {
	BaselineID string `json:"BaselineId"`
	PatchGroup string `json:"PatchGroup"`
}

// WindowTarget is a target specification for maintenance window tasks.
type WindowTarget struct {
	Key    string   `json:"Key"`
	Values []string `json:"Values"`
}

// RegisterTargetWithMaintenanceWindowInput is the request payload.
type RegisterTargetWithMaintenanceWindowInput struct {
	WindowID     string         `json:"WindowId"`
	ResourceType string         `json:"ResourceType"`
	OwnerInfo    string         `json:"OwnerInfo,omitempty"`
	Name         string         `json:"Name,omitempty"`
	Description  string         `json:"Description,omitempty"`
	Targets      []WindowTarget `json:"Targets"`
}

// RegisterTargetWithMaintenanceWindowOutput is the response payload.
type RegisterTargetWithMaintenanceWindowOutput struct {
	WindowTargetID string `json:"WindowTargetId"`
}

// RegisterTaskWithMaintenanceWindowInput is the request payload.
type RegisterTaskWithMaintenanceWindowInput struct {
	WindowID    string `json:"WindowId"`
	TaskArn     string `json:"TaskArn"`
	TaskType    string `json:"TaskType"`
	Name        string `json:"Name,omitempty"`
	Description string `json:"Description,omitempty"`
	Priority    int32  `json:"Priority,omitempty"`
}

// RegisterTaskWithMaintenanceWindowOutput is the response payload.
type RegisterTaskWithMaintenanceWindowOutput struct {
	WindowTaskID string `json:"WindowTaskId"`
}

// ResetServiceSettingInput is the request payload.
type ResetServiceSettingInput struct{}

// ResetServiceSettingOutput is the response payload.
type ResetServiceSettingOutput struct{}

// ResumeSessionInput is the request payload.
type ResumeSessionInput struct{}

// ResumeSessionOutput is the response payload.
type ResumeSessionOutput struct{}

// SendAutomationSignalInput is the request payload.
type SendAutomationSignalInput struct{}

// StartAccessRequestInput is the request payload.
type StartAccessRequestInput struct{}

// StartAccessRequestOutput is the response payload.
type StartAccessRequestOutput struct{}

// StartAssociationsOnceInput is the request payload.
type StartAssociationsOnceInput struct{}

// StartAutomationExecutionInput is the request payload.
type StartAutomationExecutionInput struct{}

// StartAutomationExecutionOutput is the response payload.
type StartAutomationExecutionOutput struct{}

// StartChangeRequestExecutionInput is the request payload.
type StartChangeRequestExecutionInput struct{}

// StartChangeRequestExecutionOutput is the response payload.
type StartChangeRequestExecutionOutput struct{}

// StartExecutionPreviewInput is the request payload.
type StartExecutionPreviewInput struct{}

// StartExecutionPreviewOutput is the response payload.
type StartExecutionPreviewOutput struct{}

// StartSessionInput is the request payload.
type StartSessionInput struct {
	Target                  string `json:"Target"`
	DocumentName            string `json:"DocumentName,omitempty"`
	Reason                  string `json:"Reason,omitempty"`
	OutputS3BucketName      string `json:"OutputS3BucketName,omitempty"`
	OutputS3KeyPrefix       string `json:"OutputS3KeyPrefix,omitempty"`
	CloudWatchLogGroupName  string `json:"CloudWatchLogGroupName,omitempty"`
	CloudWatchOutputEnabled bool   `json:"CloudWatchOutputEnabled,omitempty"`
}

// StartSessionOutput is the response payload.
type StartSessionOutput struct {
	SessionID  string `json:"SessionId"`
	StreamURL  string `json:"StreamUrl"`
	TokenValue string `json:"TokenValue"`
}

// StopAutomationExecutionInput is the request payload.
type StopAutomationExecutionInput struct{}

// TerminateSessionInput is the request payload.
type TerminateSessionInput struct {
	SessionID string `json:"SessionId"`
}

// TerminateSessionOutput is the response payload.
type TerminateSessionOutput struct {
	SessionID string `json:"SessionId"`
}

// UnlabelParameterVersionInput is the request payload.
type UnlabelParameterVersionInput struct{}

// UnlabelParameterVersionOutput is the response payload.
type UnlabelParameterVersionOutput struct{}

// UpdateAssociationInput is the request payload.
type UpdateAssociationInput struct {
	AssociationID   string              `json:"AssociationId"`
	AssociationName string              `json:"AssociationName,omitempty"`
	DocumentVersion string              `json:"DocumentVersion,omitempty"`
	Parameters      map[string][]string `json:"Parameters,omitempty"`
	Targets         []AssociationTarget `json:"Targets,omitempty"`
}

// UpdateAssociationOutput is the response payload.
type UpdateAssociationOutput struct {
	AssociationDescription Association `json:"AssociationDescription"`
}

// UpdateAssociationStatusInput is the request payload.
type UpdateAssociationStatusInput struct{}

// UpdateAssociationStatusOutput is the response payload.
type UpdateAssociationStatusOutput struct{}

// UpdateDocumentDefaultVersionInput is the request payload.
type UpdateDocumentDefaultVersionInput struct{}

// UpdateDocumentDefaultVersionOutput is the response payload.
type UpdateDocumentDefaultVersionOutput struct{}

// UpdateDocumentMetadataInput is the request payload.
type UpdateDocumentMetadataInput struct{}

// UpdateMaintenanceWindowInput is the request payload for UpdateMaintenanceWindow.
type UpdateMaintenanceWindowInput struct {
	Enabled     *bool  `json:"Enabled,omitempty"`
	WindowID    string `json:"WindowId"`
	Name        string `json:"Name,omitempty"`
	Description string `json:"Description,omitempty"`
	Schedule    string `json:"Schedule,omitempty"`
	Duration    int32  `json:"Duration,omitempty"`
	Cutoff      int32  `json:"Cutoff,omitempty"`
}

// UpdateMaintenanceWindowOutput is the response payload for UpdateMaintenanceWindow.
type UpdateMaintenanceWindowOutput struct {
	MaintenanceWindow
}

// UpdateMaintenanceWindowTargetInput is the request payload.
type UpdateMaintenanceWindowTargetInput struct{}

// UpdateMaintenanceWindowTargetOutput is the response payload.
type UpdateMaintenanceWindowTargetOutput struct{}

// UpdateMaintenanceWindowTaskInput is the request payload.
type UpdateMaintenanceWindowTaskInput struct{}

// UpdateMaintenanceWindowTaskOutput is the response payload.
type UpdateMaintenanceWindowTaskOutput struct{}

// UpdateManagedInstanceRoleInput is the request payload.
type UpdateManagedInstanceRoleInput struct{}

// UpdateOpsItemInput is the request payload for UpdateOpsItem.
type UpdateOpsItemInput struct {
	OpsItemID   string `json:"OpsItemId"`
	Title       string `json:"Title,omitempty"`
	Description string `json:"Description,omitempty"`
	Status      string `json:"Status,omitempty"`
}

// UpdateOpsMetadataInput is the request payload for UpdateOpsMetadata.
type UpdateOpsMetadataInput struct {
	Metadata       map[string]MetadataValue `json:"Metadata,omitempty"`
	OpsMetadataArn string                   `json:"OpsMetadataArn"`
}

// UpdateOpsMetadataOutput is the response payload for UpdateOpsMetadata.
type UpdateOpsMetadataOutput struct {
	OpsMetadataArn string `json:"OpsMetadataArn"`
}

// UpdatePatchBaselineInput is the request payload for UpdatePatchBaseline.
type UpdatePatchBaselineInput struct {
	BaselineID      string   `json:"BaselineId"`
	Name            string   `json:"Name,omitempty"`
	Description     string   `json:"Description,omitempty"`
	ApprovedPatches []string `json:"ApprovedPatches,omitempty"`
	RejectedPatches []string `json:"RejectedPatches,omitempty"`
}

// UpdatePatchBaselineOutput is the response payload for UpdatePatchBaseline.
type UpdatePatchBaselineOutput struct {
	PatchBaseline
}

// UpdateResourceDataSyncInput is the request payload.
type UpdateResourceDataSyncInput struct{}

// UpdateServiceSettingInput is the request payload.
type UpdateServiceSettingInput struct{}

// --- Stub backend methods ---

// CreateResourceDataSync is a stub implementation.
func (b *InMemoryBackend) CreateResourceDataSync(_ *CreateResourceDataSyncInput) (*StubOutput, error) {
	return &StubOutput{}, nil
}

// DeleteActivation removes a stored activation by ID.
func (b *InMemoryBackend) DeleteActivation(input *DeleteActivationInput) (*StubOutput, error) {
	b.mu.Lock("DeleteActivation")
	defer b.mu.Unlock()

	if _, exists := b.activations[input.ActivationID]; !exists {
		return nil, ErrActivationNotFound
	}

	delete(b.activations, input.ActivationID)
	delete(b.miscResourceTags, input.ActivationID)

	return &StubOutput{}, nil
}

// DeleteAssociation removes a stored association by ID.
func (b *InMemoryBackend) DeleteAssociation(input *DeleteAssociationInput) (*StubOutput, error) {
	b.mu.Lock("DeleteAssociation")
	defer b.mu.Unlock()

	if _, exists := b.associations[input.AssociationID]; !exists {
		return nil, ErrAssociationNotFound
	}

	delete(b.associations, input.AssociationID)

	return &StubOutput{}, nil
}

// DeleteInventory is a stub implementation.
func (b *InMemoryBackend) DeleteInventory(_ *DeleteInventoryInput) (*StubOutput, error) {
	return &StubOutput{}, nil
}

// DeleteMaintenanceWindow removes a maintenance window by ID.
func (b *InMemoryBackend) DeleteMaintenanceWindow(input *DeleteMaintenanceWindowInput) (*StubOutput, error) {
	b.mu.Lock("DeleteMaintenanceWindow")
	defer b.mu.Unlock()

	if _, exists := b.maintenanceWindows[input.WindowID]; !exists {
		return nil, ErrMaintenanceWindowNotFound
	}

	delete(b.maintenanceWindows, input.WindowID)

	return &StubOutput{}, nil
}

// DeleteOpsItem removes an OpsItem by ID.
func (b *InMemoryBackend) DeleteOpsItem(input *DeleteOpsItemInput) (*StubOutput, error) {
	b.mu.Lock("DeleteOpsItem")
	defer b.mu.Unlock()

	if _, exists := b.opsItems[input.OpsItemID]; !exists {
		return nil, ErrOpsItemNotFound
	}

	delete(b.opsItems, input.OpsItemID)
	delete(b.opsItemRelatedItems, input.OpsItemID)

	return &StubOutput{}, nil
}

// DeleteOpsMetadata removes OpsMetadata by ARN.
func (b *InMemoryBackend) DeleteOpsMetadata(input *DeleteOpsMetadataInput) (*StubOutput, error) {
	b.mu.Lock("DeleteOpsMetadata")
	defer b.mu.Unlock()

	meta, exists := b.opsMetadata[input.OpsMetadataArn]
	if !exists {
		return nil, ErrOpsMetadataNotFound
	}

	delete(b.resourceIDToOpsMetadataArn, meta.ResourceID)
	delete(b.opsMetadata, input.OpsMetadataArn)

	return &StubOutput{}, nil
}

// DeletePatchBaseline removes a patch baseline by ID.
func (b *InMemoryBackend) DeletePatchBaseline(input *DeletePatchBaselineInput) (*StubOutput, error) {
	b.mu.Lock("DeletePatchBaseline")
	defer b.mu.Unlock()

	if _, exists := b.patchBaselines[input.BaselineID]; !exists {
		return nil, ErrPatchBaselineNotFound
	}

	delete(b.patchBaselines, input.BaselineID)

	return &StubOutput{}, nil
}

// DeleteResourceDataSync is a stub implementation.
func (b *InMemoryBackend) DeleteResourceDataSync(_ *DeleteResourceDataSyncInput) (*StubOutput, error) {
	return &StubOutput{}, nil
}

// DeleteResourcePolicy is a stub implementation.
func (b *InMemoryBackend) DeleteResourcePolicy(_ *DeleteResourcePolicyInput) (*StubOutput, error) {
	return &StubOutput{}, nil
}

// DeregisterManagedInstance is a stub implementation.
func (b *InMemoryBackend) DeregisterManagedInstance(_ *DeregisterManagedInstanceInput) (*StubOutput, error) {
	return &StubOutput{}, nil
}

// DeregisterPatchBaselineForPatchGroup removes a patch group association.
func (b *InMemoryBackend) DeregisterPatchBaselineForPatchGroup(
	input *DeregisterPatchBaselineForPatchGroupInput,
) (*StubOutput, error) {
	b.mu.Lock("DeregisterPatchBaselineForPatchGroup")
	defer b.mu.Unlock()

	delete(b.patchGroupToBaseline, input.PatchGroup)

	return &StubOutput{}, nil
}

// DeregisterTargetFromMaintenanceWindow removes a target from a maintenance window.
func (b *InMemoryBackend) DeregisterTargetFromMaintenanceWindow(
	input *DeregisterTargetFromMaintenanceWindowInput,
) (*StubOutput, error) {
	b.mu.Lock("DeregisterTargetFromMaintenanceWindow")
	defer b.mu.Unlock()

	if _, exists := b.maintenanceWindowTargets[input.WindowTargetID]; !exists {
		return nil, ErrMaintenanceWindowNotFound
	}

	delete(b.maintenanceWindowTargets, input.WindowTargetID)

	return &StubOutput{}, nil
}

// DeregisterTaskFromMaintenanceWindow removes a task from a maintenance window.
func (b *InMemoryBackend) DeregisterTaskFromMaintenanceWindow(
	input *DeregisterTaskFromMaintenanceWindowInput,
) (*StubOutput, error) {
	b.mu.Lock("DeregisterTaskFromMaintenanceWindow")
	defer b.mu.Unlock()

	if _, exists := b.maintenanceWindowTasks[input.WindowTaskID]; !exists {
		return nil, ErrMaintenanceWindowNotFound
	}

	delete(b.maintenanceWindowTasks, input.WindowTaskID)

	return &StubOutput{}, nil
}

// DescribeActivations lists stored activations.
func (b *InMemoryBackend) DescribeActivations(_ *DescribeActivationsInput) (*DescribeActivationsOutput, error) {
	b.mu.RLock("DescribeActivations")
	defer b.mu.RUnlock()

	list := make([]Activation, 0, len(b.activations))
	for _, a := range b.activations {
		list = append(list, a)
	}

	return &DescribeActivationsOutput{ActivationList: list}, nil
}

// DescribeAssociation retrieves an association by name or ID.
func (b *InMemoryBackend) DescribeAssociation(input *DescribeAssociationInput) (*DescribeAssociationOutput, error) {
	b.mu.RLock("DescribeAssociation")
	defer b.mu.RUnlock()

	for _, assoc := range b.associations {
		if (input.AssociationID != "" && assoc.AssociationID == input.AssociationID) ||
			(input.Name != "" && assoc.Name == input.Name && (input.InstanceID == "" || assoc.InstanceID == input.InstanceID)) {
			return &DescribeAssociationOutput{AssociationDescription: assoc}, nil
		}
	}

	return nil, ErrAssociationNotFound
}

// DescribeAssociationExecutionTargets is a stub implementation.
func (b *InMemoryBackend) DescribeAssociationExecutionTargets(
	_ *DescribeAssociationExecutionTargetsInput,
) (*DescribeAssociationExecutionTargetsOutput, error) {
	return &DescribeAssociationExecutionTargetsOutput{}, nil
}

// DescribeAssociationExecutions is a stub implementation.
func (b *InMemoryBackend) DescribeAssociationExecutions(
	_ *DescribeAssociationExecutionsInput,
) (*DescribeAssociationExecutionsOutput, error) {
	return &DescribeAssociationExecutionsOutput{}, nil
}

// DescribeAutomationExecutions is a stub implementation.
func (b *InMemoryBackend) DescribeAutomationExecutions(
	_ *DescribeAutomationExecutionsInput,
) (*DescribeAutomationExecutionsOutput, error) {
	return &DescribeAutomationExecutionsOutput{}, nil
}

// DescribeAutomationStepExecutions is a stub implementation.
func (b *InMemoryBackend) DescribeAutomationStepExecutions(
	_ *DescribeAutomationStepExecutionsInput,
) (*DescribeAutomationStepExecutionsOutput, error) {
	return &DescribeAutomationStepExecutionsOutput{}, nil
}

// DescribeAvailablePatches is a stub implementation.
func (b *InMemoryBackend) DescribeAvailablePatches(
	_ *DescribeAvailablePatchesInput,
) (*DescribeAvailablePatchesOutput, error) {
	return &DescribeAvailablePatchesOutput{}, nil
}

// DescribeEffectiveInstanceAssociations is a stub implementation.
func (b *InMemoryBackend) DescribeEffectiveInstanceAssociations(
	_ *DescribeEffectiveInstanceAssociationsInput,
) (*DescribeEffectiveInstanceAssociationsOutput, error) {
	return &DescribeEffectiveInstanceAssociationsOutput{}, nil
}

// DescribeEffectivePatchesForPatchBaseline is a stub implementation.
func (b *InMemoryBackend) DescribeEffectivePatchesForPatchBaseline(
	_ *DescribeEffectivePatchesForPatchBaselineInput,
) (*DescribeEffectivePatchesForPatchBaselineOutput, error) {
	return &DescribeEffectivePatchesForPatchBaselineOutput{}, nil
}

// DescribeInstanceAssociationsStatus is a stub implementation.
func (b *InMemoryBackend) DescribeInstanceAssociationsStatus(
	_ *DescribeInstanceAssociationsStatusInput,
) (*DescribeInstanceAssociationsStatusOutput, error) {
	return &DescribeInstanceAssociationsStatusOutput{}, nil
}

// DescribeInstanceInformation is a stub implementation.
func (b *InMemoryBackend) DescribeInstanceInformation(
	_ *DescribeInstanceInformationInput,
) (*DescribeInstanceInformationOutput, error) {
	return &DescribeInstanceInformationOutput{}, nil
}

// DescribeInstancePatchStates is a stub implementation.
func (b *InMemoryBackend) DescribeInstancePatchStates(
	_ *DescribeInstancePatchStatesInput,
) (*DescribeInstancePatchStatesOutput, error) {
	return &DescribeInstancePatchStatesOutput{}, nil
}

// DescribeInstancePatchStatesForPatchGroup is a stub implementation.
func (b *InMemoryBackend) DescribeInstancePatchStatesForPatchGroup(
	_ *DescribeInstancePatchStatesForPatchGroupInput,
) (*DescribeInstancePatchStatesForPatchGroupOutput, error) {
	return &DescribeInstancePatchStatesForPatchGroupOutput{}, nil
}

// DescribeInstancePatches is a stub implementation.
func (b *InMemoryBackend) DescribeInstancePatches(
	_ *DescribeInstancePatchesInput,
) (*DescribeInstancePatchesOutput, error) {
	return &DescribeInstancePatchesOutput{}, nil
}

// DescribeInstanceProperties is a stub implementation.
func (b *InMemoryBackend) DescribeInstanceProperties(
	_ *DescribeInstancePropertiesInput,
) (*DescribeInstancePropertiesOutput, error) {
	return &DescribeInstancePropertiesOutput{}, nil
}

// DescribeInventoryDeletions is a stub implementation.
func (b *InMemoryBackend) DescribeInventoryDeletions(
	_ *DescribeInventoryDeletionsInput,
) (*DescribeInventoryDeletionsOutput, error) {
	return &DescribeInventoryDeletionsOutput{}, nil
}

// DescribeMaintenanceWindowExecutionTaskInvocations is a stub implementation.
func (b *InMemoryBackend) DescribeMaintenanceWindowExecutionTaskInvocations(
	_ *DescribeMaintenanceWindowExecutionTaskInvocationsInput,
) (*DescribeMaintenanceWindowExecutionTaskInvocationsOutput, error) {
	return &DescribeMaintenanceWindowExecutionTaskInvocationsOutput{}, nil
}

// DescribeMaintenanceWindowExecutionTasks is a stub implementation.
func (b *InMemoryBackend) DescribeMaintenanceWindowExecutionTasks(
	_ *DescribeMaintenanceWindowExecutionTasksInput,
) (*DescribeMaintenanceWindowExecutionTasksOutput, error) {
	return &DescribeMaintenanceWindowExecutionTasksOutput{}, nil
}

// DescribeMaintenanceWindowExecutions is a stub implementation.
func (b *InMemoryBackend) DescribeMaintenanceWindowExecutions(
	_ *DescribeMaintenanceWindowExecutionsInput,
) (*DescribeMaintenanceWindowExecutionsOutput, error) {
	return &DescribeMaintenanceWindowExecutionsOutput{}, nil
}

// DescribeMaintenanceWindowSchedule is a stub implementation.
func (b *InMemoryBackend) DescribeMaintenanceWindowSchedule(
	_ *DescribeMaintenanceWindowScheduleInput,
) (*DescribeMaintenanceWindowScheduleOutput, error) {
	return &DescribeMaintenanceWindowScheduleOutput{}, nil
}

// DescribeMaintenanceWindowTargets lists targets registered with a maintenance window.
func (b *InMemoryBackend) DescribeMaintenanceWindowTargets(
	input *DescribeMaintenanceWindowTargetsInput,
) (*DescribeMaintenanceWindowTargetsOutput, error) {
	b.mu.RLock("DescribeMaintenanceWindowTargets")
	defer b.mu.RUnlock()

	var targets []MaintenanceWindowTarget
	for _, t := range b.maintenanceWindowTargets {
		if t.WindowID == input.WindowID {
			targets = append(targets, t)
		}
	}

	if targets == nil {
		targets = []MaintenanceWindowTarget{}
	}

	return &DescribeMaintenanceWindowTargetsOutput{Targets: targets}, nil
}

// DescribeMaintenanceWindowTasks lists tasks registered with a maintenance window.
func (b *InMemoryBackend) DescribeMaintenanceWindowTasks(
	input *DescribeMaintenanceWindowTasksInput,
) (*DescribeMaintenanceWindowTasksOutput, error) {
	b.mu.RLock("DescribeMaintenanceWindowTasks")
	defer b.mu.RUnlock()

	var tasks []MaintenanceWindowTask
	for _, t := range b.maintenanceWindowTasks {
		if t.WindowID == input.WindowID {
			tasks = append(tasks, t)
		}
	}

	if tasks == nil {
		tasks = []MaintenanceWindowTask{}
	}

	return &DescribeMaintenanceWindowTasksOutput{Tasks: tasks}, nil
}

// DescribeMaintenanceWindows lists maintenance windows.
func (b *InMemoryBackend) DescribeMaintenanceWindows(
	input *DescribeMaintenanceWindowsInput,
) (*DescribeMaintenanceWindowsOutput, error) {
	b.mu.RLock("DescribeMaintenanceWindows")
	defer b.mu.RUnlock()

	all := make([]MaintenanceWindowIdentity, 0, len(b.maintenanceWindows))
	for _, mw := range b.maintenanceWindows {
		all = append(all, MaintenanceWindowIdentity{
			WindowID:    mw.WindowID,
			Name:        mw.Name,
			Description: mw.Description,
			Enabled:     mw.Enabled,
			Duration:    mw.Duration,
			Cutoff:      mw.Cutoff,
			Schedule:    mw.Schedule,
		})
	}

	startIdx := parseNextToken(input.NextToken)

	const defaultMWMaxResults = 50

	maxResults := int64(defaultMWMaxResults)
	if input.MaxResults != nil && *input.MaxResults > 0 {
		maxResults = *input.MaxResults
	}

	if startIdx >= len(all) {
		return &DescribeMaintenanceWindowsOutput{WindowIdentities: []MaintenanceWindowIdentity{}}, nil
	}

	end := startIdx + int(maxResults)

	var nextToken string

	if end < len(all) {
		nextToken = strconv.Itoa(end)
	} else {
		end = len(all)
	}

	return &DescribeMaintenanceWindowsOutput{
		WindowIdentities: all[startIdx:end],
		NextToken:        nextToken,
	}, nil
}

// DescribeMaintenanceWindowsForTarget is a stub implementation.
func (b *InMemoryBackend) DescribeMaintenanceWindowsForTarget(
	_ *DescribeMaintenanceWindowsForTargetInput,
) (*DescribeMaintenanceWindowsForTargetOutput, error) {
	return &DescribeMaintenanceWindowsForTargetOutput{}, nil
}

// DescribeOpsItems lists OpsItems.
func (b *InMemoryBackend) DescribeOpsItems(input *DescribeOpsItemsInput) (*DescribeOpsItemsOutput, error) {
	b.mu.RLock("DescribeOpsItems")
	defer b.mu.RUnlock()

	all := make([]OpsItemSummary, 0, len(b.opsItems))
	for _, item := range b.opsItems {
		all = append(all, OpsItemSummary{
			OpsItemID:   item.OpsItemID,
			Title:       item.Title,
			Status:      item.Status,
			Source:      item.Source,
			CreatedTime: item.CreatedTime,
		})
	}

	startIdx := parseNextToken(input.NextToken)

	const defaultOpsItemMaxResults = 50

	maxResults := int64(defaultOpsItemMaxResults)
	if input.MaxResults != nil && *input.MaxResults > 0 {
		maxResults = *input.MaxResults
	}

	if startIdx >= len(all) {
		return &DescribeOpsItemsOutput{OpsItemSummaries: []OpsItemSummary{}}, nil
	}

	end := startIdx + int(maxResults)

	var nextToken string

	if end < len(all) {
		nextToken = strconv.Itoa(end)
	} else {
		end = len(all)
	}

	return &DescribeOpsItemsOutput{
		OpsItemSummaries: all[startIdx:end],
		NextToken:        nextToken,
	}, nil
}

// DescribePatchBaselines lists patch baselines.
func (b *InMemoryBackend) DescribePatchBaselines(
	input *DescribePatchBaselinesInput,
) (*DescribePatchBaselinesOutput, error) {
	b.mu.RLock("DescribePatchBaselines")
	defer b.mu.RUnlock()

	all := make([]PatchBaselineIdentity, 0, len(b.patchBaselines))
	for _, bl := range b.patchBaselines {
		all = append(all, PatchBaselineIdentity{
			BaselineID:      bl.BaselineID,
			BaselineName:    bl.Name,
			OperatingSystem: bl.OperatingSystem,
			Description:     bl.Description,
		})
	}

	startIdx := parseNextToken(input.NextToken)

	const defaultBaselineMaxResults = 50

	maxResults := int64(defaultBaselineMaxResults)
	if input.MaxResults != nil && *input.MaxResults > 0 {
		maxResults = *input.MaxResults
	}

	if startIdx >= len(all) {
		return &DescribePatchBaselinesOutput{BaselineIdentities: []PatchBaselineIdentity{}}, nil
	}

	end := startIdx + int(maxResults)

	var nextToken string

	if end < len(all) {
		nextToken = strconv.Itoa(end)
	} else {
		end = len(all)
	}

	return &DescribePatchBaselinesOutput{
		BaselineIdentities: all[startIdx:end],
		NextToken:          nextToken,
	}, nil
}

// DescribePatchGroupState is a stub implementation.
func (b *InMemoryBackend) DescribePatchGroupState(
	_ *DescribePatchGroupStateInput,
) (*DescribePatchGroupStateOutput, error) {
	return &DescribePatchGroupStateOutput{}, nil
}

// DescribePatchGroups is a stub implementation.
func (b *InMemoryBackend) DescribePatchGroups(_ *DescribePatchGroupsInput) (*DescribePatchGroupsOutput, error) {
	return &DescribePatchGroupsOutput{}, nil
}

// DescribePatchProperties is a stub implementation.
func (b *InMemoryBackend) DescribePatchProperties(
	_ *DescribePatchPropertiesInput,
) (*DescribePatchPropertiesOutput, error) {
	return &DescribePatchPropertiesOutput{}, nil
}

// DescribeSessions is a stub implementation.
func (b *InMemoryBackend) DescribeSessions(_ *DescribeSessionsInput) (*DescribeSessionsOutput, error) {
	return &DescribeSessionsOutput{}, nil
}

// DisassociateOpsItemRelatedItem is a stub implementation.
func (b *InMemoryBackend) DisassociateOpsItemRelatedItem(
	_ *DisassociateOpsItemRelatedItemInput,
) (*StubOutput, error) {
	return &StubOutput{}, nil
}

// GetAccessToken is a stub implementation.
func (b *InMemoryBackend) GetAccessToken(_ *GetAccessTokenInput) (*GetAccessTokenOutput, error) {
	return &GetAccessTokenOutput{}, nil
}

// GetAutomationExecution is a stub implementation.
func (b *InMemoryBackend) GetAutomationExecution(
	_ *GetAutomationExecutionInput,
) (*GetAutomationExecutionOutput, error) {
	return &GetAutomationExecutionOutput{}, nil
}

// GetCalendarState is a stub implementation.
func (b *InMemoryBackend) GetCalendarState(_ *GetCalendarStateInput) (*GetCalendarStateOutput, error) {
	return &GetCalendarStateOutput{}, nil
}

// GetConnectionStatus is a stub implementation.
func (b *InMemoryBackend) GetConnectionStatus(_ *GetConnectionStatusInput) (*GetConnectionStatusOutput, error) {
	return &GetConnectionStatusOutput{}, nil
}

// GetDefaultPatchBaseline is a stub implementation.
func (b *InMemoryBackend) GetDefaultPatchBaseline(
	_ *GetDefaultPatchBaselineInput,
) (*GetDefaultPatchBaselineOutput, error) {
	return &GetDefaultPatchBaselineOutput{}, nil
}

// GetDeployablePatchSnapshotForInstance is a stub implementation.
func (b *InMemoryBackend) GetDeployablePatchSnapshotForInstance(
	_ *GetDeployablePatchSnapshotForInstanceInput,
) (*GetDeployablePatchSnapshotForInstanceOutput, error) {
	return &GetDeployablePatchSnapshotForInstanceOutput{}, nil
}

// GetExecutionPreview is a stub implementation.
func (b *InMemoryBackend) GetExecutionPreview(_ *GetExecutionPreviewInput) (*GetExecutionPreviewOutput, error) {
	return &GetExecutionPreviewOutput{}, nil
}

// GetInventory is a stub implementation.
func (b *InMemoryBackend) GetInventory(_ *GetInventoryInput) (*GetInventoryOutput, error) {
	return &GetInventoryOutput{}, nil
}

// GetInventorySchema is a stub implementation.
func (b *InMemoryBackend) GetInventorySchema(_ *GetInventorySchemaInput) (*GetInventorySchemaOutput, error) {
	return &GetInventorySchemaOutput{}, nil
}

// GetMaintenanceWindow retrieves a maintenance window by ID.
func (b *InMemoryBackend) GetMaintenanceWindow(input *GetMaintenanceWindowInput) (*GetMaintenanceWindowOutput, error) {
	b.mu.RLock("GetMaintenanceWindow")
	defer b.mu.RUnlock()

	mw, exists := b.maintenanceWindows[input.WindowID]
	if !exists {
		return nil, ErrMaintenanceWindowNotFound
	}

	return &GetMaintenanceWindowOutput{MaintenanceWindow: mw}, nil
}

// GetMaintenanceWindowExecution is a stub implementation.
func (b *InMemoryBackend) GetMaintenanceWindowExecution(
	_ *GetMaintenanceWindowExecutionInput,
) (*GetMaintenanceWindowExecutionOutput, error) {
	return &GetMaintenanceWindowExecutionOutput{}, nil
}

// GetMaintenanceWindowExecutionTask is a stub implementation.
func (b *InMemoryBackend) GetMaintenanceWindowExecutionTask(
	_ *GetMaintenanceWindowExecutionTaskInput,
) (*GetMaintenanceWindowExecutionTaskOutput, error) {
	return &GetMaintenanceWindowExecutionTaskOutput{}, nil
}

// GetMaintenanceWindowExecutionTaskInvocation is a stub implementation.
func (b *InMemoryBackend) GetMaintenanceWindowExecutionTaskInvocation(
	_ *GetMaintenanceWindowExecutionTaskInvocationInput,
) (*GetMaintenanceWindowExecutionTaskInvocationOutput, error) {
	return &GetMaintenanceWindowExecutionTaskInvocationOutput{}, nil
}

// GetMaintenanceWindowTask is a stub implementation.
func (b *InMemoryBackend) GetMaintenanceWindowTask(
	_ *GetMaintenanceWindowTaskInput,
) (*GetMaintenanceWindowTaskOutput, error) {
	return &GetMaintenanceWindowTaskOutput{}, nil
}

// GetOpsItem retrieves an OpsItem by ID.
func (b *InMemoryBackend) GetOpsItem(input *GetOpsItemInput) (*GetOpsItemOutput, error) {
	b.mu.RLock("GetOpsItem")
	defer b.mu.RUnlock()

	item, exists := b.opsItems[input.OpsItemID]
	if !exists {
		return nil, ErrOpsItemNotFound
	}

	return &GetOpsItemOutput{OpsItem: item}, nil
}

// GetOpsMetadata retrieves OpsMetadata by ARN.
func (b *InMemoryBackend) GetOpsMetadata(input *GetOpsMetadataInput) (*GetOpsMetadataOutput, error) {
	b.mu.RLock("GetOpsMetadata")
	defer b.mu.RUnlock()

	meta, exists := b.opsMetadata[input.OpsMetadataArn]
	if !exists {
		return nil, ErrOpsMetadataNotFound
	}

	return &GetOpsMetadataOutput{OpsMetadata: meta}, nil
}

// GetOpsSummary is a stub implementation.
func (b *InMemoryBackend) GetOpsSummary(_ *GetOpsSummaryInput) (*GetOpsSummaryOutput, error) {
	return &GetOpsSummaryOutput{}, nil
}

// GetPatchBaseline retrieves a patch baseline by ID.
func (b *InMemoryBackend) GetPatchBaseline(input *GetPatchBaselineInput) (*GetPatchBaselineOutput, error) {
	b.mu.RLock("GetPatchBaseline")
	defer b.mu.RUnlock()

	bl, exists := b.patchBaselines[input.BaselineID]
	if !exists {
		return nil, ErrPatchBaselineNotFound
	}

	return &GetPatchBaselineOutput{PatchBaseline: bl}, nil
}

// GetPatchBaselineForPatchGroup is a stub implementation.
func (b *InMemoryBackend) GetPatchBaselineForPatchGroup(
	_ *GetPatchBaselineForPatchGroupInput,
) (*GetPatchBaselineForPatchGroupOutput, error) {
	return &GetPatchBaselineForPatchGroupOutput{}, nil
}

// GetResourcePolicies is a stub implementation.
func (b *InMemoryBackend) GetResourcePolicies(_ *GetResourcePoliciesInput) (*GetResourcePoliciesOutput, error) {
	return &GetResourcePoliciesOutput{}, nil
}

// GetServiceSetting is a stub implementation.
func (b *InMemoryBackend) GetServiceSetting(_ *GetServiceSettingInput) (*GetServiceSettingOutput, error) {
	return &GetServiceSettingOutput{}, nil
}

// LabelParameterVersion applies labels to a specific parameter version.
func (b *InMemoryBackend) LabelParameterVersion(
	input *LabelParameterVersionInput,
) (*LabelParameterVersionOutput, error) {
	b.mu.Lock("LabelParameterVersion")
	defer b.mu.Unlock()

	history, exists := b.history[input.Name]
	if !exists {
		return nil, ErrParameterNotFound
	}

	// Find the target version — default to the latest version.
	targetVersion := input.Version
	if targetVersion == 0 {
		if param, ok := b.parameters[input.Name]; ok {
			targetVersion = param.Version
		}
	}

	idx := -1
	for i, h := range history {
		if h.Version == targetVersion {
			idx = i

			break
		}
	}

	if idx == -1 {
		return nil, fmt.Errorf("%w: version %d not found for parameter %q",
			ErrValidationException, targetVersion, input.Name)
	}

	for _, label := range input.Labels {
		if !slices.Contains(history[idx].Labels, label) {
			history[idx].Labels = append(history[idx].Labels, label)
		}
	}

	b.history[input.Name] = history

	return &LabelParameterVersionOutput{ParameterVersion: targetVersion}, nil
}

// ListAssociationVersions is a stub implementation.
func (b *InMemoryBackend) ListAssociationVersions(
	_ *ListAssociationVersionsInput,
) (*ListAssociationVersionsOutput, error) {
	return &ListAssociationVersionsOutput{}, nil
}

// ListAssociations lists all stored associations.
func (b *InMemoryBackend) ListAssociations(_ *ListAssociationsInput) (*ListAssociationsOutput, error) {
	b.mu.RLock("ListAssociations")
	defer b.mu.RUnlock()

	list := make([]Association, 0, len(b.associations))
	for _, a := range b.associations {
		list = append(list, a)
	}

	return &ListAssociationsOutput{Associations: list}, nil
}

// ListComplianceItems is a stub implementation.
func (b *InMemoryBackend) ListComplianceItems(_ *ListComplianceItemsInput) (*ListComplianceItemsOutput, error) {
	return &ListComplianceItemsOutput{}, nil
}

// ListComplianceSummaries is a stub implementation.
func (b *InMemoryBackend) ListComplianceSummaries(
	_ *ListComplianceSummariesInput,
) (*ListComplianceSummariesOutput, error) {
	return &ListComplianceSummariesOutput{}, nil
}

// ListDocumentMetadataHistory is a stub implementation.
func (b *InMemoryBackend) ListDocumentMetadataHistory(
	_ *ListDocumentMetadataHistoryInput,
) (*ListDocumentMetadataHistoryOutput, error) {
	return &ListDocumentMetadataHistoryOutput{}, nil
}

// ListInventoryEntries is a stub implementation.
func (b *InMemoryBackend) ListInventoryEntries(_ *ListInventoryEntriesInput) (*ListInventoryEntriesOutput, error) {
	return &ListInventoryEntriesOutput{}, nil
}

// ListNodes is a stub implementation.
func (b *InMemoryBackend) ListNodes(_ *ListNodesInput) (*ListNodesOutput, error) {
	return &ListNodesOutput{}, nil
}

// ListNodesSummary is a stub implementation.
func (b *InMemoryBackend) ListNodesSummary(_ *ListNodesSummaryInput) (*ListNodesSummaryOutput, error) {
	return &ListNodesSummaryOutput{}, nil
}

// ListOpsItemEvents is a stub implementation.
func (b *InMemoryBackend) ListOpsItemEvents(_ *ListOpsItemEventsInput) (*ListOpsItemEventsOutput, error) {
	return &ListOpsItemEventsOutput{}, nil
}

// ListOpsItemRelatedItems is a stub implementation.
func (b *InMemoryBackend) ListOpsItemRelatedItems(
	_ *ListOpsItemRelatedItemsInput,
) (*ListOpsItemRelatedItemsOutput, error) {
	return &ListOpsItemRelatedItemsOutput{}, nil
}

// ListOpsMetadata is a stub implementation.
func (b *InMemoryBackend) ListOpsMetadata(_ *ListOpsMetadataInput) (*ListOpsMetadataOutput, error) {
	return &ListOpsMetadataOutput{}, nil
}

// ListResourceComplianceSummaries is a stub implementation.
func (b *InMemoryBackend) ListResourceComplianceSummaries(
	_ *ListResourceComplianceSummariesInput,
) (*ListResourceComplianceSummariesOutput, error) {
	return &ListResourceComplianceSummariesOutput{}, nil
}

// ListResourceDataSync is a stub implementation.
func (b *InMemoryBackend) ListResourceDataSync(_ *ListResourceDataSyncInput) (*ListResourceDataSyncOutput, error) {
	return &ListResourceDataSyncOutput{}, nil
}

// PutComplianceItems is a stub implementation.
func (b *InMemoryBackend) PutComplianceItems(_ *PutComplianceItemsInput) (*StubOutput, error) {
	return &StubOutput{}, nil
}

// PutInventory is a stub implementation.
func (b *InMemoryBackend) PutInventory(_ *PutInventoryInput) (*StubOutput, error) {
	return &StubOutput{}, nil
}

// PutResourcePolicy is a stub implementation.
func (b *InMemoryBackend) PutResourcePolicy(_ *PutResourcePolicyInput) (*PutResourcePolicyOutput, error) {
	return &PutResourcePolicyOutput{}, nil
}

// RegisterDefaultPatchBaseline is a stub implementation.
func (b *InMemoryBackend) RegisterDefaultPatchBaseline(
	_ *RegisterDefaultPatchBaselineInput,
) (*RegisterDefaultPatchBaselineOutput, error) {
	return &RegisterDefaultPatchBaselineOutput{}, nil
}

// RegisterPatchBaselineForPatchGroup associates a baseline with a patch group.
func (b *InMemoryBackend) RegisterPatchBaselineForPatchGroup(
	input *RegisterPatchBaselineForPatchGroupInput,
) (*RegisterPatchBaselineForPatchGroupOutput, error) {
	b.mu.Lock("RegisterPatchBaselineForPatchGroup")
	defer b.mu.Unlock()

	if _, exists := b.patchBaselines[input.BaselineID]; !exists {
		return nil, ErrPatchBaselineNotFound
	}

	b.patchGroupToBaseline[input.PatchGroup] = input.BaselineID

	return &RegisterPatchBaselineForPatchGroupOutput{
		BaselineID: input.BaselineID,
		PatchGroup: input.PatchGroup,
	}, nil
}

// RegisterTargetWithMaintenanceWindow registers a target with a maintenance window.
func (b *InMemoryBackend) RegisterTargetWithMaintenanceWindow(
	input *RegisterTargetWithMaintenanceWindowInput,
) (*RegisterTargetWithMaintenanceWindowOutput, error) {
	b.mu.Lock("RegisterTargetWithMaintenanceWindow")
	defer b.mu.Unlock()

	if _, exists := b.maintenanceWindows[input.WindowID]; !exists {
		return nil, ErrMaintenanceWindowNotFound
	}

	targetID := windowTargetIDPrefix + uuid.NewString()
	target := MaintenanceWindowTarget{
		WindowID:       input.WindowID,
		WindowTargetID: targetID,
		ResourceType:   input.ResourceType,
		Targets:        input.Targets,
		OwnerInfo:      input.OwnerInfo,
		Description:    input.Description,
		Name:           input.Name,
	}

	b.maintenanceWindowTargets[targetID] = target

	return &RegisterTargetWithMaintenanceWindowOutput{WindowTargetID: targetID}, nil
}

// RegisterTaskWithMaintenanceWindow registers a task with a maintenance window.
func (b *InMemoryBackend) RegisterTaskWithMaintenanceWindow(
	input *RegisterTaskWithMaintenanceWindowInput,
) (*RegisterTaskWithMaintenanceWindowOutput, error) {
	b.mu.Lock("RegisterTaskWithMaintenanceWindow")
	defer b.mu.Unlock()

	if _, exists := b.maintenanceWindows[input.WindowID]; !exists {
		return nil, ErrMaintenanceWindowNotFound
	}

	taskID := windowTaskIDPrefix + uuid.NewString()
	task := MaintenanceWindowTask{
		WindowID:     input.WindowID,
		WindowTaskID: taskID,
		TaskArn:      input.TaskArn,
		TaskType:     input.TaskType,
		Priority:     input.Priority,
		Name:         input.Name,
		Description:  input.Description,
	}

	b.maintenanceWindowTasks[taskID] = task

	return &RegisterTaskWithMaintenanceWindowOutput{WindowTaskID: taskID}, nil
}

// ResetServiceSetting is a stub implementation.
func (b *InMemoryBackend) ResetServiceSetting(_ *ResetServiceSettingInput) (*ResetServiceSettingOutput, error) {
	return &ResetServiceSettingOutput{}, nil
}

// ResumeSession is a stub implementation.
func (b *InMemoryBackend) ResumeSession(_ *ResumeSessionInput) (*ResumeSessionOutput, error) {
	return &ResumeSessionOutput{}, nil
}

// SendAutomationSignal is a stub implementation.
func (b *InMemoryBackend) SendAutomationSignal(_ *SendAutomationSignalInput) (*StubOutput, error) {
	return &StubOutput{}, nil
}

// StartAccessRequest is a stub implementation.
func (b *InMemoryBackend) StartAccessRequest(_ *StartAccessRequestInput) (*StartAccessRequestOutput, error) {
	return &StartAccessRequestOutput{}, nil
}

// StartAssociationsOnce is a stub implementation.
func (b *InMemoryBackend) StartAssociationsOnce(_ *StartAssociationsOnceInput) (*StubOutput, error) {
	return &StubOutput{}, nil
}

// StartAutomationExecution is a stub implementation.
func (b *InMemoryBackend) StartAutomationExecution(
	_ *StartAutomationExecutionInput,
) (*StartAutomationExecutionOutput, error) {
	return &StartAutomationExecutionOutput{}, nil
}

// StartChangeRequestExecution is a stub implementation.
func (b *InMemoryBackend) StartChangeRequestExecution(
	_ *StartChangeRequestExecutionInput,
) (*StartChangeRequestExecutionOutput, error) {
	return &StartChangeRequestExecutionOutput{}, nil
}

// StartExecutionPreview is a stub implementation.
func (b *InMemoryBackend) StartExecutionPreview(_ *StartExecutionPreviewInput) (*StartExecutionPreviewOutput, error) {
	return &StartExecutionPreviewOutput{}, nil
}

// StartSession creates a new SSM Session Manager session.
func (b *InMemoryBackend) StartSession(input *StartSessionInput) (*StartSessionOutput, error) {
	b.mu.Lock("StartSession")
	defer b.mu.Unlock()

	sessionID := sessionIDPrefix + uuid.NewString()

	sess := Session{
		SessionID:               sessionID,
		Target:                  input.Target,
		Status:                  sessionStatusConnected,
		StartDate:               UnixTimeFloat(timeNow()),
		StreamURL:               "wss://gopherstack-ssm-session/" + sessionID,
		TokenValue:              uuid.NewString(),
		OutputS3BucketName:      input.OutputS3BucketName,
		OutputS3KeyPrefix:       input.OutputS3KeyPrefix,
		CloudWatchOutputEnabled: input.CloudWatchOutputEnabled,
		CloudWatchLogGroupName:  input.CloudWatchLogGroupName,
	}

	b.sessions[sessionID] = sess

	return &StartSessionOutput{
		SessionID:  sessionID,
		StreamURL:  sess.StreamURL,
		TokenValue: sess.TokenValue,
	}, nil
}

// StopAutomationExecution is a stub implementation.
func (b *InMemoryBackend) StopAutomationExecution(_ *StopAutomationExecutionInput) (*StubOutput, error) {
	return &StubOutput{}, nil
}

// TerminateSession terminates an active SSM session.
func (b *InMemoryBackend) TerminateSession(input *TerminateSessionInput) (*TerminateSessionOutput, error) {
	b.mu.Lock("TerminateSession")
	defer b.mu.Unlock()

	sess, exists := b.sessions[input.SessionID]
	if !exists {
		return &TerminateSessionOutput{SessionID: input.SessionID}, nil
	}

	sess.Status = sessionStatusTerminated
	sess.EndDate = UnixTimeFloat(timeNow())
	b.sessions[input.SessionID] = sess

	return &TerminateSessionOutput{SessionID: input.SessionID}, nil
}

// UnlabelParameterVersion is a stub implementation.
func (b *InMemoryBackend) UnlabelParameterVersion(
	_ *UnlabelParameterVersionInput,
) (*UnlabelParameterVersionOutput, error) {
	return &UnlabelParameterVersionOutput{}, nil
}

// UpdateAssociation updates an existing association.
func (b *InMemoryBackend) UpdateAssociation(input *UpdateAssociationInput) (*UpdateAssociationOutput, error) {
	b.mu.Lock("UpdateAssociation")
	defer b.mu.Unlock()

	assoc, exists := b.associations[input.AssociationID]
	if !exists {
		return nil, ErrAssociationNotFound
	}

	if input.AssociationName != "" {
		assoc.AssociationName = input.AssociationName
	}

	if input.DocumentVersion != "" {
		assoc.DocumentVersion = input.DocumentVersion
	}

	if input.Parameters != nil {
		assoc.Parameters = copyAssocParameters(input.Parameters)
	}

	if input.Targets != nil {
		assoc.Targets = copyAssocTargets(input.Targets)
	}

	assoc.LastUpdateAssociationDate = UnixTimeFloat(timeNow())
	b.associations[input.AssociationID] = assoc

	return &UpdateAssociationOutput{AssociationDescription: assoc}, nil
}

// UpdateAssociationStatus is a stub implementation.
func (b *InMemoryBackend) UpdateAssociationStatus(
	_ *UpdateAssociationStatusInput,
) (*UpdateAssociationStatusOutput, error) {
	return &UpdateAssociationStatusOutput{}, nil
}

// UpdateDocumentDefaultVersion is a stub implementation.
func (b *InMemoryBackend) UpdateDocumentDefaultVersion(
	_ *UpdateDocumentDefaultVersionInput,
) (*UpdateDocumentDefaultVersionOutput, error) {
	return &UpdateDocumentDefaultVersionOutput{}, nil
}

// UpdateDocumentMetadata is a stub implementation.
func (b *InMemoryBackend) UpdateDocumentMetadata(_ *UpdateDocumentMetadataInput) (*StubOutput, error) {
	return &StubOutput{}, nil
}

// UpdateMaintenanceWindow updates a maintenance window.
func (b *InMemoryBackend) UpdateMaintenanceWindow(
	input *UpdateMaintenanceWindowInput,
) (*UpdateMaintenanceWindowOutput, error) {
	b.mu.Lock("UpdateMaintenanceWindow")
	defer b.mu.Unlock()

	mw, exists := b.maintenanceWindows[input.WindowID]
	if !exists {
		return nil, ErrMaintenanceWindowNotFound
	}

	if input.Name != "" {
		mw.Name = input.Name
	}

	if input.Description != "" {
		mw.Description = input.Description
	}

	if input.Schedule != "" {
		mw.Schedule = input.Schedule
	}

	if input.Duration != 0 {
		mw.Duration = input.Duration
	}

	if input.Cutoff != 0 {
		mw.Cutoff = input.Cutoff
	}

	if input.Enabled != nil {
		mw.Enabled = *input.Enabled
	}

	mw.ModifiedDate = UnixTimeFloat(timeNow())
	b.maintenanceWindows[input.WindowID] = mw

	return &UpdateMaintenanceWindowOutput{MaintenanceWindow: mw}, nil
}

// UpdateMaintenanceWindowTarget is a stub implementation.
func (b *InMemoryBackend) UpdateMaintenanceWindowTarget(
	_ *UpdateMaintenanceWindowTargetInput,
) (*UpdateMaintenanceWindowTargetOutput, error) {
	return &UpdateMaintenanceWindowTargetOutput{}, nil
}

// UpdateMaintenanceWindowTask is a stub implementation.
func (b *InMemoryBackend) UpdateMaintenanceWindowTask(
	_ *UpdateMaintenanceWindowTaskInput,
) (*UpdateMaintenanceWindowTaskOutput, error) {
	return &UpdateMaintenanceWindowTaskOutput{}, nil
}

// UpdateManagedInstanceRole is a stub implementation.
func (b *InMemoryBackend) UpdateManagedInstanceRole(_ *UpdateManagedInstanceRoleInput) (*StubOutput, error) {
	return &StubOutput{}, nil
}

// UpdateOpsItem updates an OpsItem.
func (b *InMemoryBackend) UpdateOpsItem(input *UpdateOpsItemInput) (*StubOutput, error) {
	b.mu.Lock("UpdateOpsItem")
	defer b.mu.Unlock()

	item, exists := b.opsItems[input.OpsItemID]
	if !exists {
		return nil, ErrOpsItemNotFound
	}

	if input.Title != "" {
		item.Title = input.Title
	}

	if input.Description != "" {
		item.Description = input.Description
	}

	if input.Status != "" {
		item.Status = input.Status
	}

	item.LastModifiedTime = UnixTimeFloat(timeNow())
	b.opsItems[input.OpsItemID] = item

	return &StubOutput{}, nil
}

// UpdateOpsMetadata updates OpsMetadata.
func (b *InMemoryBackend) UpdateOpsMetadata(input *UpdateOpsMetadataInput) (*UpdateOpsMetadataOutput, error) {
	b.mu.Lock("UpdateOpsMetadata")
	defer b.mu.Unlock()

	meta, exists := b.opsMetadata[input.OpsMetadataArn]
	if !exists {
		return nil, ErrOpsMetadataNotFound
	}

	if input.Metadata != nil {
		if meta.Metadata == nil {
			meta.Metadata = make(map[string]MetadataValue)
		}

		maps.Copy(meta.Metadata, input.Metadata)
	}

	meta.LastModifiedDate = UnixTimeFloat(timeNow())
	b.opsMetadata[input.OpsMetadataArn] = meta

	return &UpdateOpsMetadataOutput{OpsMetadataArn: input.OpsMetadataArn}, nil
}

// UpdatePatchBaseline updates a patch baseline.
func (b *InMemoryBackend) UpdatePatchBaseline(input *UpdatePatchBaselineInput) (*UpdatePatchBaselineOutput, error) {
	b.mu.Lock("UpdatePatchBaseline")
	defer b.mu.Unlock()

	bl, exists := b.patchBaselines[input.BaselineID]
	if !exists {
		return nil, ErrPatchBaselineNotFound
	}

	if input.Name != "" {
		bl.Name = input.Name
	}

	if input.Description != "" {
		bl.Description = input.Description
	}

	if len(input.ApprovedPatches) > 0 {
		bl.ApprovedPatches = input.ApprovedPatches
	}

	if len(input.RejectedPatches) > 0 {
		bl.RejectedPatches = input.RejectedPatches
	}

	bl.ModifiedDate = UnixTimeFloat(timeNow())
	b.patchBaselines[input.BaselineID] = bl

	return &UpdatePatchBaselineOutput{PatchBaseline: bl}, nil
}

// UpdateResourceDataSync is a stub implementation.
func (b *InMemoryBackend) UpdateResourceDataSync(_ *UpdateResourceDataSyncInput) (*StubOutput, error) {
	return &StubOutput{}, nil
}

// UpdateServiceSetting is a stub implementation.
func (b *InMemoryBackend) UpdateServiceSetting(_ *UpdateServiceSettingInput) (*StubOutput, error) {
	return &StubOutput{}, nil
}

// timeNow is a variable so tests can override it.
//
//nolint:gochecknoglobals // intentional hook for test time injection
var timeNow = time.Now
