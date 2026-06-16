package ssm

import (
	"context"
	"maps"
	"slices"
	"strconv"
	"time"

	"github.com/google/uuid"
)

// backend_stubs.go provides stub implementations for the SSM operations
// that are acknowledged but not yet fully implemented.  Each stub returns an
// empty success response, which is sufficient for the SDK-completeness test
// and for callers that only need the operation to not error.

// --- Stub input/output types ---

// StubOutput is a generic empty response used by all stub operations.
type StubOutput struct{}

// CreateResourceDataSyncInput is the request for CreateResourceDataSync.
type CreateResourceDataSyncInput struct {
	SyncName string `json:"SyncName"`
	SyncType string `json:"SyncType,omitempty"`
}

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

// DeleteResourceDataSyncInput is the request for DeleteResourceDataSync.
type DeleteResourceDataSyncInput struct {
	SyncName string `json:"SyncName"`
}

// DeleteResourcePolicyInput is the request for DeleteResourcePolicy.
type DeleteResourcePolicyInput struct {
	ResourceARN string `json:"ResourceArn"`
	PolicyID    string `json:"PolicyId"`
}

// DeregisterManagedInstanceInput is the request for DeregisterManagedInstance.
type DeregisterManagedInstanceInput struct {
	InstanceID string `json:"InstanceId"`
}

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
type DescribeAssociationExecutionTargetsInput struct {
	AssociationID string `json:"AssociationId"`
	ExecutionID   string `json:"ExecutionId,omitempty"`
}

// DescribeAssociationExecutionTargetsOutput is the response for DescribeAssociationExecutionTargets.
type DescribeAssociationExecutionTargetsOutput struct{}

// DescribeAssociationExecutionsInput is the request for DescribeAssociationExecutions.
type DescribeAssociationExecutionsInput struct {
	AssociationID string `json:"AssociationId"`
}

// DescribeAssociationExecutionsOutput is the response for DescribeAssociationExecutions.
type DescribeAssociationExecutionsOutput struct{}

// DescribeAutomationExecutionsInput is the request for DescribeAutomationExecutions.
type DescribeAutomationExecutionsInput struct{}

// DescribeAutomationExecutionsOutput is the response for DescribeAutomationExecutions.
type DescribeAutomationExecutionsOutput struct{}

// DescribeAutomationStepExecutionsInput is the request for DescribeAutomationStepExecutions.
type DescribeAutomationStepExecutionsInput struct {
	AutomationExecutionID string `json:"AutomationExecutionId"`
}

// DescribeAutomationStepExecutionsOutput is the response for DescribeAutomationStepExecutions.
type DescribeAutomationStepExecutionsOutput struct{}

// DescribeAvailablePatchesInput is the request for DescribeAvailablePatches.
type DescribeAvailablePatchesInput struct{}

// DescribeAvailablePatchesOutput is the response for DescribeAvailablePatches.
type DescribeAvailablePatchesOutput struct{}

// DescribeEffectiveInstanceAssociationsInput is the request for DescribeEffectiveInstanceAssociations.
type DescribeEffectiveInstanceAssociationsInput struct {
	InstanceID string `json:"InstanceId"`
}

// DescribeEffectiveInstanceAssociationsOutput is the response for DescribeEffectiveInstanceAssociations.
type DescribeEffectiveInstanceAssociationsOutput struct{}

// DescribeInstanceAssociationsStatusInput is the request for DescribeInstanceAssociationsStatus.
type DescribeInstanceAssociationsStatusInput struct {
	InstanceID string `json:"InstanceId"`
}

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

// DescribeMaintenanceWindowExecutionTaskInvocationsInput is the request payload.
type DescribeMaintenanceWindowExecutionTaskInvocationsInput struct {
	WindowExecutionID string `json:"WindowExecutionId"`
	TaskID            string `json:"TaskId,omitempty"`
}

// DescribeMaintenanceWindowExecutionTaskInvocationsOutput is the response payload.
type DescribeMaintenanceWindowExecutionTaskInvocationsOutput struct{}

// DescribeMaintenanceWindowExecutionTasksInput is the request payload.
type DescribeMaintenanceWindowExecutionTasksInput struct {
	WindowExecutionID string `json:"WindowExecutionId"`
}

// DescribeMaintenanceWindowExecutionTasksOutput is the response payload.
type DescribeMaintenanceWindowExecutionTasksOutput struct{}

// DescribeMaintenanceWindowExecutionsInput is the request payload.
type DescribeMaintenanceWindowExecutionsInput struct {
	WindowID string `json:"WindowId"`
}

// DescribeMaintenanceWindowExecutionsOutput is the response payload.
type DescribeMaintenanceWindowExecutionsOutput struct{}

// DescribeMaintenanceWindowScheduleInput is the request payload.
type DescribeMaintenanceWindowScheduleInput struct {
	WindowID string `json:"WindowId,omitempty"`
}

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

// OpsItemFilter is a filter for DescribeOpsItems.
type OpsItemFilter struct {
	Key      string   `json:"Key"`
	Operator string   `json:"Operator,omitempty"`
	Values   []string `json:"Values"`
}

// DescribeOpsItemsInput is the request payload for DescribeOpsItems.
type DescribeOpsItemsInput struct {
	MaxResults     *int64          `json:"MaxResults,omitempty"`
	NextToken      string          `json:"NextToken,omitempty"`
	OpsItemFilters []OpsItemFilter `json:"OpsItemFilters,omitempty"`
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

// PatchBaselineFilter is a key-value filter for DescribePatchBaselines.
type PatchBaselineFilter struct {
	Key    string   `json:"Key"`
	Values []string `json:"Values"`
}

// DescribePatchBaselinesInput is the request payload for DescribePatchBaselines.
type DescribePatchBaselinesInput struct {
	MaxResults *int64                `json:"MaxResults,omitempty"`
	NextToken  string                `json:"NextToken,omitempty"`
	Filters    []PatchBaselineFilter `json:"Filters,omitempty"`
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

// DescribeSessionsInput is the request payload.
type DescribeSessionsInput struct {
	State string `json:"State,omitempty"`
}

// DescribeSessionsOutput is the response payload.
type DescribeSessionsOutput struct{}

// GetAccessTokenInput is the request payload.
type GetAccessTokenInput struct{}

// GetAccessTokenOutput is the response payload.
type GetAccessTokenOutput struct{}

// GetAutomationExecutionInput is the request payload.
type GetAutomationExecutionInput struct {
	AutomationExecutionID string `json:"AutomationExecutionId"`
}

// GetAutomationExecutionOutput is the response payload.
type GetAutomationExecutionOutput struct{}

// GetCalendarStateInput is the request payload.
type GetCalendarStateInput struct {
	AtTime        string   `json:"AtTime,omitempty"`
	CalendarNames []string `json:"CalendarNames,omitempty"`
}

// GetCalendarStateOutput is the response payload.
type GetCalendarStateOutput struct{}

// GetConnectionStatusInput is the request payload.
type GetConnectionStatusInput struct {
	Target string `json:"Target"`
}

// GetConnectionStatusOutput is the response payload.
type GetConnectionStatusOutput struct{}

// GetExecutionPreviewInput is the request payload.
type GetExecutionPreviewInput struct {
	ExecutionPreviewID string `json:"ExecutionPreviewId"`
}

// GetExecutionPreviewOutput is the response payload.
type GetExecutionPreviewOutput struct{}

// GetMaintenanceWindowInput is the request payload for GetMaintenanceWindow.
type GetMaintenanceWindowInput struct {
	WindowID string `json:"WindowId"`
}

// GetMaintenanceWindowOutput is the response payload for GetMaintenanceWindow.
type GetMaintenanceWindowOutput struct {
	MaintenanceWindow
}

// GetMaintenanceWindowExecutionInput is the request payload.
type GetMaintenanceWindowExecutionInput struct {
	WindowID          string `json:"WindowId"`
	WindowExecutionID string `json:"WindowExecutionId"`
}

// GetMaintenanceWindowExecutionOutput is the response payload.
type GetMaintenanceWindowExecutionOutput struct{}

// GetMaintenanceWindowExecutionTaskInput is the request payload.
type GetMaintenanceWindowExecutionTaskInput struct {
	WindowExecutionID string `json:"WindowExecutionId"`
	TaskExecutionID   string `json:"TaskExecutionId"`
}

// GetMaintenanceWindowExecutionTaskOutput is the response payload.
type GetMaintenanceWindowExecutionTaskOutput struct{}

// GetMaintenanceWindowExecutionTaskInvocationInput is the request payload.
type GetMaintenanceWindowExecutionTaskInvocationInput struct {
	WindowExecutionID string `json:"WindowExecutionId"`
	TaskExecutionID   string `json:"TaskExecutionId"`
	InvocationID      string `json:"InvocationId"`
}

// GetMaintenanceWindowExecutionTaskInvocationOutput is the response payload.
type GetMaintenanceWindowExecutionTaskInvocationOutput struct{}

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

// GetResourcePoliciesInput is the request payload.
type GetResourcePoliciesInput struct {
	ResourceARN string `json:"ResourceArn"`
}

// GetResourcePoliciesOutput is the response payload.
type GetResourcePoliciesOutput struct{}

// GetServiceSettingInput is the request payload.
type GetServiceSettingInput struct {
	SettingID string `json:"SettingId"`
}

// GetServiceSettingOutput is the response payload.
type GetServiceSettingOutput struct{}

// LabelParameterVersionInput is the request payload.
type LabelParameterVersionInput struct {
	Name             string   `json:"Name"`
	Labels           []string `json:"Labels"`
	ParameterVersion int64    `json:"ParameterVersion,omitempty"`
}

// LabelParameterVersionOutput is the response payload.
type LabelParameterVersionOutput struct{}

// ListAssociationVersionsInput is the request payload.
type ListAssociationVersionsInput struct {
	AssociationID string `json:"AssociationId"`
}

// ListAssociationVersionsOutput is the response payload.
type ListAssociationVersionsOutput struct{}

// ListAssociationsInput is the request payload.
type ListAssociationsInput struct{}

// ListAssociationsOutput is the response payload.
type ListAssociationsOutput struct {
	Associations []Association `json:"Associations"`
}

// ListNodesInput is the request payload.
type ListNodesInput struct{}

// ListNodesOutput is the response payload.
type ListNodesOutput struct{}

// ListNodesSummaryInput is the request payload.
type ListNodesSummaryInput struct{}

// ListNodesSummaryOutput is the response payload.
type ListNodesSummaryOutput struct{}

// ListOpsMetadataInput is the request payload.
type ListOpsMetadataInput struct{}

// ListOpsMetadataOutput is the response payload.
type ListOpsMetadataOutput struct{}

// ListResourceDataSyncInput is the request payload.
type ListResourceDataSyncInput struct{}

// ListResourceDataSyncOutput is the response payload.
type ListResourceDataSyncOutput struct{}

// PutResourcePolicyInput is the request payload.
type PutResourcePolicyInput struct {
	ResourceARN string `json:"ResourceArn"`
	Policy      string `json:"Policy"`
}

// PutResourcePolicyOutput is the response payload.
type PutResourcePolicyOutput struct{}

// WindowTarget is a target specification for maintenance window tasks.
type WindowTarget struct {
	Key    string   `json:"Key"`
	Values []string `json:"Values"`
}

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
	WindowID       string `json:"WindowId"`
	TaskArn        string `json:"TaskArn"`
	TaskType       string `json:"TaskType"`
	Name           string `json:"Name,omitempty"`
	Description    string `json:"Description,omitempty"`
	ServiceRoleArn string `json:"ServiceRoleArn,omitempty"`
	MaxConcurrency string `json:"MaxConcurrency,omitempty"`
	MaxErrors      string `json:"MaxErrors,omitempty"`
	Priority       int32  `json:"Priority,omitempty"`
}

// RegisterTaskWithMaintenanceWindowOutput is the response payload.
type RegisterTaskWithMaintenanceWindowOutput struct {
	WindowTaskID string `json:"WindowTaskId"`
}

// ResetServiceSettingInput is the request payload.
type ResetServiceSettingInput struct {
	SettingID string `json:"SettingId"`
}

// ResetServiceSettingOutput is the response payload.
type ResetServiceSettingOutput struct{}

// ResumeSessionInput is the request payload.
type ResumeSessionInput struct {
	SessionID string `json:"SessionId"`
}

// ResumeSessionOutput is the response payload.
type ResumeSessionOutput struct{}

// SendAutomationSignalInput is the request payload.
type SendAutomationSignalInput struct {
	AutomationExecutionID string `json:"AutomationExecutionId"`
	SignalType            string `json:"SignalType,omitempty"`
}

// StartAccessRequestInput is the request payload.
type StartAccessRequestInput struct{}

// StartAccessRequestOutput is the response payload.
type StartAccessRequestOutput struct{}

// StartAssociationsOnceInput is the request payload.
type StartAssociationsOnceInput struct {
	AssociationIDs []string `json:"AssociationIds"`
}

// StartAutomationExecutionInput is the request payload.
type StartAutomationExecutionInput struct {
	Parameters      map[string][]string `json:"Parameters,omitempty"`
	DocumentName    string              `json:"DocumentName"`
	DocumentVersion string              `json:"DocumentVersion,omitempty"`
	Mode            string              `json:"Mode,omitempty"`
	MaxConcurrency  string              `json:"MaxConcurrency,omitempty"`
	MaxErrors       string              `json:"MaxErrors,omitempty"`
}

// StartAutomationExecutionOutput is the response payload.
type StartAutomationExecutionOutput struct{}

// StartChangeRequestExecutionInput is the request payload.
type StartChangeRequestExecutionInput struct {
	DocumentName string `json:"DocumentName"`
}

// StartChangeRequestExecutionOutput is the response payload.
type StartChangeRequestExecutionOutput struct{}

// StartExecutionPreviewInput is the request payload.
type StartExecutionPreviewInput struct {
	DocumentName string `json:"DocumentName,omitempty"`
}

// StartExecutionPreviewOutput is the response payload.
type StartExecutionPreviewOutput struct{}

// StartSessionInput is the request payload.
type StartSessionInput struct {
	Parameters              map[string][]string `json:"Parameters,omitempty"`
	Target                  string              `json:"Target"`
	DocumentName            string              `json:"DocumentName,omitempty"`
	Reason                  string              `json:"Reason,omitempty"`
	OutputS3BucketName      string              `json:"OutputS3BucketName,omitempty"`
	OutputS3KeyPrefix       string              `json:"OutputS3KeyPrefix,omitempty"`
	CloudWatchLogGroupName  string              `json:"CloudWatchLogGroupName,omitempty"`
	CloudWatchOutputEnabled bool                `json:"CloudWatchOutputEnabled,omitempty"`
}

// StartSessionOutput is the response payload.
type StartSessionOutput struct {
	SessionID  string `json:"SessionId"`
	StreamURL  string `json:"StreamUrl"`
	TokenValue string `json:"TokenValue"`
}

// StopAutomationExecutionInput is the request payload.
type StopAutomationExecutionInput struct {
	AutomationExecutionID string `json:"AutomationExecutionId"`
}

// TerminateSessionInput is the request payload.
type TerminateSessionInput struct {
	SessionID string `json:"SessionId"`
}

// TerminateSessionOutput is the response payload.
type TerminateSessionOutput struct {
	SessionID string `json:"SessionId"`
}

// UnlabelParameterVersionInput is the request payload.
type UnlabelParameterVersionInput struct {
	Name             string   `json:"Name"`
	Labels           []string `json:"Labels"`
	ParameterVersion int64    `json:"ParameterVersion,omitempty"`
}

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
type UpdateAssociationStatusInput struct {
	InstanceID        string                 `json:"InstanceId"`
	Name              string                 `json:"Name"`
	AssociationStatus AssociationStatusValue `json:"AssociationStatus"`
}

// UpdateAssociationStatusOutput is the response payload.
type UpdateAssociationStatusOutput struct {
	AssociationDescription Association `json:"AssociationDescription"`
}

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

// UpdateManagedInstanceRoleInput is the request payload.
type UpdateManagedInstanceRoleInput struct {
	InstanceID string `json:"InstanceId"`
	IamRole    string `json:"IamRole"`
}

// UpdateOpsItemInput is the request payload for UpdateOpsItem.
type UpdateOpsItemInput struct {
	OperationalData map[string]OpsItemDataValue `json:"OperationalData,omitempty"`
	OpsItemID       string                      `json:"OpsItemId"`
	Title           string                      `json:"Title,omitempty"`
	Description     string                      `json:"Description,omitempty"`
	Status          string                      `json:"Status,omitempty"`
	Severity        string                      `json:"Severity,omitempty"`
	Category        string                      `json:"Category,omitempty"`
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
	BaselineID                     string   `json:"BaselineId"`
	Name                           string   `json:"Name,omitempty"`
	Description                    string   `json:"Description,omitempty"`
	ApprovedPatchesComplianceLevel string   `json:"ApprovedPatchesComplianceLevel,omitempty"`
	ApprovedPatches                []string `json:"ApprovedPatches,omitempty"`
	RejectedPatches                []string `json:"RejectedPatches,omitempty"`
}

// UpdatePatchBaselineOutput is the response payload for UpdatePatchBaseline.
type UpdatePatchBaselineOutput struct {
	PatchBaseline
}

// UpdateResourceDataSyncInput is the request payload.
type UpdateResourceDataSyncInput struct {
	SyncName string `json:"SyncName"`
}

// UpdateServiceSettingInput is the request payload.
type UpdateServiceSettingInput struct {
	SettingID    string `json:"SettingId"`
	SettingValue string `json:"SettingValue"`
}

// DeleteActivation removes a stored activation by ID.
func (b *InMemoryBackend) DeleteActivation(ctx context.Context, input *DeleteActivationInput) (*StubOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("DeleteActivation")
	defer b.mu.Unlock()

	activations := b.activationsStore(region)
	if _, exists := activations[input.ActivationID]; !exists {
		return nil, ErrActivationNotFound
	}

	delete(activations, input.ActivationID)
	delete(b.miscResourceTagsStore(region), input.ActivationID)

	return &StubOutput{}, nil
}

// DeleteAssociation removes a stored association by ID.
func (b *InMemoryBackend) DeleteAssociation(ctx context.Context, input *DeleteAssociationInput) (*StubOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("DeleteAssociation")
	defer b.mu.Unlock()

	associations := b.associationsStore(region)
	if _, exists := associations[input.AssociationID]; !exists {
		return nil, ErrAssociationNotFound
	}

	delete(associations, input.AssociationID)

	return &StubOutput{}, nil
}

// DeregisterPatchBaselineForPatchGroup removes a patch group association.
func (b *InMemoryBackend) DeregisterPatchBaselineForPatchGroup(
	ctx context.Context,
	input *DeregisterPatchBaselineForPatchGroupInput,
) (*StubOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("DeregisterPatchBaselineForPatchGroup")
	defer b.mu.Unlock()

	delete(b.patchGroupToBaselineStore(region), input.PatchGroup)

	return &StubOutput{}, nil
}

// DeregisterTargetFromMaintenanceWindow removes a target from a maintenance window.
func (b *InMemoryBackend) DeregisterTargetFromMaintenanceWindow(
	ctx context.Context,
	input *DeregisterTargetFromMaintenanceWindowInput,
) (*StubOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("DeregisterTargetFromMaintenanceWindow")
	defer b.mu.Unlock()

	targets := b.maintenanceWindowTargetsStore(region)
	if _, exists := targets[input.WindowTargetID]; !exists {
		return nil, ErrMaintenanceWindowNotFound
	}

	delete(targets, input.WindowTargetID)

	return &StubOutput{}, nil
}

// DeregisterTaskFromMaintenanceWindow removes a task from a maintenance window.
func (b *InMemoryBackend) DeregisterTaskFromMaintenanceWindow(
	ctx context.Context,
	input *DeregisterTaskFromMaintenanceWindowInput,
) (*StubOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("DeregisterTaskFromMaintenanceWindow")
	defer b.mu.Unlock()

	tasks := b.maintenanceWindowTasksStore(region)
	if _, exists := tasks[input.WindowTaskID]; !exists {
		return nil, ErrMaintenanceWindowNotFound
	}

	delete(tasks, input.WindowTaskID)

	return &StubOutput{}, nil
}

// DescribeActivations lists stored activations.
func (b *InMemoryBackend) DescribeActivations(
	ctx context.Context,
	_ *DescribeActivationsInput,
) (*DescribeActivationsOutput, error) {
	region := getRegion(ctx)
	b.mu.RLock("DescribeActivations")
	defer b.mu.RUnlock()

	activations := b.activationsStore(region)
	list := make([]Activation, 0, len(activations))
	for _, a := range activations {
		list = append(list, a)
	}

	return &DescribeActivationsOutput{ActivationList: list}, nil
}

// DescribeAssociation retrieves an association by name or ID.
func (b *InMemoryBackend) DescribeAssociation(
	ctx context.Context,
	input *DescribeAssociationInput,
) (*DescribeAssociationOutput, error) {
	region := getRegion(ctx)
	b.mu.RLock("DescribeAssociation")
	defer b.mu.RUnlock()

	for _, assoc := range b.associationsStore(region) {
		if (input.AssociationID != "" && assoc.AssociationID == input.AssociationID) ||
			(input.Name != "" && assoc.Name == input.Name && (input.InstanceID == "" || assoc.InstanceID == input.InstanceID)) {
			return &DescribeAssociationOutput{AssociationDescription: assoc}, nil
		}
	}

	return nil, ErrAssociationNotFound
}

// DescribeAvailablePatches is a stub implementation.
func (b *InMemoryBackend) DescribeAvailablePatches(
	_ context.Context,
	_ *DescribeAvailablePatchesInput,
) (*DescribeAvailablePatchesOutput, error) {
	return &DescribeAvailablePatchesOutput{}, nil
}

// DescribeInstancePatchStatesForPatchGroup is a stub implementation.
func (b *InMemoryBackend) DescribeInstancePatchStatesForPatchGroup(
	_ context.Context,
	_ *DescribeInstancePatchStatesForPatchGroupInput,
) (*DescribeInstancePatchStatesForPatchGroupOutput, error) {
	return &DescribeInstancePatchStatesForPatchGroupOutput{}, nil
}

// DescribeInstancePatches is a stub implementation.
func (b *InMemoryBackend) DescribeInstancePatches(
	_ context.Context,
	_ *DescribeInstancePatchesInput,
) (*DescribeInstancePatchesOutput, error) {
	return &DescribeInstancePatchesOutput{}, nil
}

// DescribeInstanceProperties is a stub implementation.
func (b *InMemoryBackend) DescribeInstanceProperties(
	_ context.Context,
	_ *DescribeInstancePropertiesInput,
) (*DescribeInstancePropertiesOutput, error) {
	return &DescribeInstancePropertiesOutput{}, nil
}

// DescribeMaintenanceWindowTargets lists targets registered with a maintenance window.
func (b *InMemoryBackend) DescribeMaintenanceWindowTargets(
	ctx context.Context,
	input *DescribeMaintenanceWindowTargetsInput,
) (*DescribeMaintenanceWindowTargetsOutput, error) {
	region := getRegion(ctx)
	b.mu.RLock("DescribeMaintenanceWindowTargets")
	defer b.mu.RUnlock()

	var targets []MaintenanceWindowTarget
	for _, t := range b.maintenanceWindowTargetsStore(region) {
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
	ctx context.Context,
	input *DescribeMaintenanceWindowTasksInput,
) (*DescribeMaintenanceWindowTasksOutput, error) {
	region := getRegion(ctx)
	b.mu.RLock("DescribeMaintenanceWindowTasks")
	defer b.mu.RUnlock()

	var tasks []MaintenanceWindowTask
	for _, t := range b.maintenanceWindowTasksStore(region) {
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
	ctx context.Context,
	input *DescribeMaintenanceWindowsInput,
) (*DescribeMaintenanceWindowsOutput, error) {
	region := getRegion(ctx)
	b.mu.RLock("DescribeMaintenanceWindows")
	defer b.mu.RUnlock()

	mws := b.maintenanceWindowsStore(region)
	all := make([]MaintenanceWindowIdentity, 0, len(mws))
	for _, mw := range mws {
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

// opsItemMatchesFilters returns true when the item satisfies all provided filters.
func opsItemMatchesFilters(item OpsItem, filters []OpsItemFilter) bool {
	for _, f := range filters {
		var fieldValue string

		switch f.Key {
		case "Status":
			fieldValue = item.Status
		case "Title":
			fieldValue = item.Title
		case "Source":
			fieldValue = item.Source
		default:
			continue
		}

		if !slices.Contains(f.Values, fieldValue) {
			return false
		}
	}

	return true
}

// DescribeOpsItems lists OpsItems.
func (b *InMemoryBackend) DescribeOpsItems(
	ctx context.Context,
	input *DescribeOpsItemsInput,
) (*DescribeOpsItemsOutput, error) {
	region := getRegion(ctx)
	b.mu.RLock("DescribeOpsItems")
	defer b.mu.RUnlock()

	items := b.opsItemsStore(region)
	all := make([]OpsItemSummary, 0, len(items))
	for _, item := range items {
		if !opsItemMatchesFilters(item, input.OpsItemFilters) {
			continue
		}

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

// patchBaselineMatchesFilters returns true when bl satisfies all provided key-value filters.
// Supported filter keys: OPERATING_SYSTEM, NAME_PREFIX.
func patchBaselineMatchesFilters(bl PatchBaseline, filters []PatchBaselineFilter) bool {
	for _, f := range filters {
		var fieldValue string

		switch f.Key {
		case "OPERATING_SYSTEM":
			fieldValue = bl.OperatingSystem
		case "NAME_PREFIX":
			for _, v := range f.Values {
				if len(bl.Name) >= len(v) && bl.Name[:len(v)] == v {
					fieldValue = v
				}
			}
		default:
			continue
		}

		if !slices.Contains(f.Values, fieldValue) {
			return false
		}
	}

	return true
}

// DescribePatchBaselines lists patch baselines with optional OS and name filters.
func (b *InMemoryBackend) DescribePatchBaselines(
	ctx context.Context,
	input *DescribePatchBaselinesInput,
) (*DescribePatchBaselinesOutput, error) {
	region := getRegion(ctx)
	b.mu.RLock("DescribePatchBaselines")
	defer b.mu.RUnlock()

	baselines := b.patchBaselinesStore(region)
	all := make([]PatchBaselineIdentity, 0, len(baselines))
	for _, bl := range baselines {
		if !patchBaselineMatchesFilters(bl, input.Filters) {
			continue
		}

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

// GetMaintenanceWindow retrieves a maintenance window by ID.
func (b *InMemoryBackend) GetMaintenanceWindow(
	ctx context.Context,
	input *GetMaintenanceWindowInput,
) (*GetMaintenanceWindowOutput, error) {
	region := getRegion(ctx)
	b.mu.RLock("GetMaintenanceWindow")
	defer b.mu.RUnlock()

	mw, exists := b.maintenanceWindowsStore(region)[input.WindowID]
	if !exists {
		return nil, ErrMaintenanceWindowNotFound
	}

	return &GetMaintenanceWindowOutput{MaintenanceWindow: mw}, nil
}

// GetOpsItem retrieves an OpsItem by ID.
func (b *InMemoryBackend) GetOpsItem(ctx context.Context, input *GetOpsItemInput) (*GetOpsItemOutput, error) {
	region := getRegion(ctx)
	b.mu.RLock("GetOpsItem")
	defer b.mu.RUnlock()

	item, exists := b.opsItemsStore(region)[input.OpsItemID]
	if !exists {
		return nil, ErrOpsItemNotFound
	}

	return &GetOpsItemOutput{OpsItem: item}, nil
}

// GetOpsMetadata retrieves OpsMetadata by ARN.
func (b *InMemoryBackend) GetOpsMetadata(
	ctx context.Context,
	input *GetOpsMetadataInput,
) (*GetOpsMetadataOutput, error) {
	region := getRegion(ctx)
	b.mu.RLock("GetOpsMetadata")
	defer b.mu.RUnlock()

	meta, exists := b.opsMetadataStore(region)[input.OpsMetadataArn]
	if !exists {
		return nil, ErrOpsMetadataNotFound
	}

	return &GetOpsMetadataOutput{OpsMetadata: meta}, nil
}

// GetPatchBaseline retrieves a patch baseline by ID.
func (b *InMemoryBackend) GetPatchBaseline(
	ctx context.Context,
	input *GetPatchBaselineInput,
) (*GetPatchBaselineOutput, error) {
	region := getRegion(ctx)
	b.mu.RLock("GetPatchBaseline")
	defer b.mu.RUnlock()

	bl, exists := b.patchBaselinesStore(region)[input.BaselineID]
	if !exists {
		return nil, ErrPatchBaselineNotFound
	}

	return &GetPatchBaselineOutput{PatchBaseline: bl}, nil
}

// ListAssociations lists all stored associations.
func (b *InMemoryBackend) ListAssociations(
	ctx context.Context,
	_ *ListAssociationsInput,
) (*ListAssociationsOutput, error) {
	region := getRegion(ctx)
	b.mu.RLock("ListAssociations")
	defer b.mu.RUnlock()

	associations := b.associationsStore(region)
	list := make([]Association, 0, len(associations))
	for _, a := range associations {
		list = append(list, a)
	}

	return &ListAssociationsOutput{Associations: list}, nil
}

// RegisterPatchBaselineForPatchGroup associates a baseline with a patch group.
func (b *InMemoryBackend) RegisterPatchBaselineForPatchGroup(
	ctx context.Context,
	input *RegisterPatchBaselineForPatchGroupInput,
) (*RegisterPatchBaselineForPatchGroupOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("RegisterPatchBaselineForPatchGroup")
	defer b.mu.Unlock()

	if _, exists := b.patchBaselinesStore(region)[input.BaselineID]; !exists {
		return nil, ErrPatchBaselineNotFound
	}

	b.patchGroupToBaselineStore(region)[input.PatchGroup] = input.BaselineID

	return &RegisterPatchBaselineForPatchGroupOutput{
		BaselineID: input.BaselineID,
		PatchGroup: input.PatchGroup,
	}, nil
}

// RegisterTargetWithMaintenanceWindow registers a target with a maintenance window.
func (b *InMemoryBackend) RegisterTargetWithMaintenanceWindow(
	ctx context.Context,
	input *RegisterTargetWithMaintenanceWindowInput,
) (*RegisterTargetWithMaintenanceWindowOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("RegisterTargetWithMaintenanceWindow")
	defer b.mu.Unlock()

	if _, exists := b.maintenanceWindowsStore(region)[input.WindowID]; !exists {
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

	b.maintenanceWindowTargetsStore(region)[targetID] = target

	return &RegisterTargetWithMaintenanceWindowOutput{WindowTargetID: targetID}, nil
}

// RegisterTaskWithMaintenanceWindow registers a task with a maintenance window.
func (b *InMemoryBackend) RegisterTaskWithMaintenanceWindow(
	ctx context.Context,
	input *RegisterTaskWithMaintenanceWindowInput,
) (*RegisterTaskWithMaintenanceWindowOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("RegisterTaskWithMaintenanceWindow")
	defer b.mu.Unlock()

	if _, exists := b.maintenanceWindowsStore(region)[input.WindowID]; !exists {
		return nil, ErrMaintenanceWindowNotFound
	}

	taskID := windowTaskIDPrefix + uuid.NewString()
	task := MaintenanceWindowTask{
		WindowID:       input.WindowID,
		WindowTaskID:   taskID,
		TaskArn:        input.TaskArn,
		TaskType:       input.TaskType,
		Priority:       input.Priority,
		Name:           input.Name,
		Description:    input.Description,
		ServiceRoleArn: input.ServiceRoleArn,
		MaxConcurrency: input.MaxConcurrency,
		MaxErrors:      input.MaxErrors,
	}

	b.maintenanceWindowTasksStore(region)[taskID] = task

	return &RegisterTaskWithMaintenanceWindowOutput{WindowTaskID: taskID}, nil
}

// StartSession creates a new SSM Session Manager session.
func (b *InMemoryBackend) StartSession(ctx context.Context, input *StartSessionInput) (*StartSessionOutput, error) {
	region := getRegion(ctx)
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
		DocumentName:            input.DocumentName,
		Reason:                  input.Reason,
		CloudWatchOutputEnabled: input.CloudWatchOutputEnabled,
		CloudWatchLogGroupName:  input.CloudWatchLogGroupName,
		Parameters:              input.Parameters,
	}

	if input.OutputS3BucketName != "" {
		sess.OutputURL = &SessionOutputS3{
			S3BucketName: input.OutputS3BucketName,
			S3KeyPrefix:  input.OutputS3KeyPrefix,
		}
	}

	b.sessionsStore(region)[sessionID] = sess

	return &StartSessionOutput{
		SessionID:  sessionID,
		StreamURL:  sess.StreamURL,
		TokenValue: sess.TokenValue,
	}, nil
}

// TerminateSession terminates an active SSM session.
func (b *InMemoryBackend) TerminateSession(
	ctx context.Context,
	input *TerminateSessionInput,
) (*TerminateSessionOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("TerminateSession")
	defer b.mu.Unlock()

	sessions := b.sessionsStore(region)
	sess, exists := sessions[input.SessionID]
	if !exists {
		return &TerminateSessionOutput{SessionID: input.SessionID}, nil
	}

	sess.Status = sessionStatusTerminated
	sess.EndDate = UnixTimeFloat(timeNow())
	sessions[input.SessionID] = sess

	return &TerminateSessionOutput{SessionID: input.SessionID}, nil
}

// UpdateAssociation updates an existing association.
func (b *InMemoryBackend) UpdateAssociation(
	ctx context.Context,
	input *UpdateAssociationInput,
) (*UpdateAssociationOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("UpdateAssociation")
	defer b.mu.Unlock()

	associations := b.associationsStore(region)
	assoc, exists := associations[input.AssociationID]
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
	associations[input.AssociationID] = assoc

	return &UpdateAssociationOutput{AssociationDescription: assoc}, nil
}

// UpdateMaintenanceWindow updates a maintenance window.
func (b *InMemoryBackend) UpdateMaintenanceWindow(
	ctx context.Context,
	input *UpdateMaintenanceWindowInput,
) (*UpdateMaintenanceWindowOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("UpdateMaintenanceWindow")
	defer b.mu.Unlock()

	windows := b.maintenanceWindowsStore(region)
	mw, exists := windows[input.WindowID]
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
	windows[input.WindowID] = mw

	return &UpdateMaintenanceWindowOutput{MaintenanceWindow: mw}, nil
}

// UpdateOpsItem updates an OpsItem including OperationalData.
func (b *InMemoryBackend) UpdateOpsItem(ctx context.Context, input *UpdateOpsItemInput) (*StubOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("UpdateOpsItem")
	defer b.mu.Unlock()

	items := b.opsItemsStore(region)
	item, exists := items[input.OpsItemID]
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

	if input.Severity != "" {
		item.Severity = input.Severity
	}

	if input.Category != "" {
		item.Category = input.Category
	}

	if len(input.OperationalData) > 0 {
		if item.OperationalData == nil {
			item.OperationalData = make(map[string]OpsItemDataValue)
		}

		maps.Copy(item.OperationalData, input.OperationalData)
	}

	item.LastModifiedTime = UnixTimeFloat(timeNow())
	items[input.OpsItemID] = item

	// Record an event for the update.
	b.opsItemEvents[region] = append(b.opsItemEvents[region], OpsItemEventSummary{
		OpsItemID: input.OpsItemID,
		EventID:   "event-update-" + input.OpsItemID,
	})

	return &StubOutput{}, nil
}

// UpdateOpsMetadata updates OpsMetadata.
func (b *InMemoryBackend) UpdateOpsMetadata(
	ctx context.Context,
	input *UpdateOpsMetadataInput,
) (*UpdateOpsMetadataOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("UpdateOpsMetadata")
	defer b.mu.Unlock()

	opsMetadata := b.opsMetadataStore(region)
	meta, exists := opsMetadata[input.OpsMetadataArn]
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
	opsMetadata[input.OpsMetadataArn] = meta

	return &UpdateOpsMetadataOutput{OpsMetadataArn: input.OpsMetadataArn}, nil
}

// UpdatePatchBaseline updates a patch baseline.
func (b *InMemoryBackend) UpdatePatchBaseline(
	ctx context.Context,
	input *UpdatePatchBaselineInput,
) (*UpdatePatchBaselineOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("UpdatePatchBaseline")
	defer b.mu.Unlock()

	baselines := b.patchBaselinesStore(region)
	bl, exists := baselines[input.BaselineID]
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

	if input.ApprovedPatchesComplianceLevel != "" {
		bl.ApprovedPatchesComplianceLevel = input.ApprovedPatchesComplianceLevel
	}

	bl.ModifiedDate = UnixTimeFloat(timeNow())
	baselines[input.BaselineID] = bl

	return &UpdatePatchBaselineOutput{PatchBaseline: bl}, nil
}

// timeNow is a variable so tests can override it.
//
//nolint:gochecknoglobals // intentional hook for test time injection
var timeNow = time.Now
