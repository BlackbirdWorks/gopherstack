package ssm

import "time"

// AssociationStatusValue is the status payload in UpdateAssociationStatus.
type AssociationStatusValue struct {
	Name             string `json:"Name"`
	ExecutionSummary string `json:"ExecutionSummary,omitempty"`
}

// --- ResourceDataSync ---

// ResourceDataSync represents a resource data sync configuration.
type ResourceDataSync struct {
	SyncName        string    `json:"SyncName"`
	SyncType        string    `json:"SyncType"`
	LastStatus      string    `json:"LastStatus"`
	SyncCreatedTime time.Time `json:"SyncCreatedTime"`
	LastSyncTime    time.Time `json:"LastSyncTime,omitempty"`
}

// --- AutomationExecution ---

// AutomationExecution represents a running or completed SSM automation execution.
type AutomationExecution struct {
	AutomationExecutionID   string                  `json:"AutomationExecutionId"`
	DocumentName            string                  `json:"DocumentName"`
	DocumentVersion         string                  `json:"DocumentVersion"`
	Status                  string                  `json:"AutomationExecutionStatus"`
	StartTime               time.Time               `json:"StartTime"`
	EndTime                 *time.Time              `json:"EndTime,omitempty"`
	ExecutionType           string                  `json:"ExecutionType"` // "Standard" or "ChangeRequest"
	Steps                   []AutomationStepExec    `json:"StepExecutions,omitempty"`
}

// AutomationStepExec represents a single step in an automation execution.
type AutomationStepExec struct {
	StepName   string `json:"StepName"`
	Action     string `json:"Action"`
	StepStatus string `json:"StepStatus"`
}

// --- ServiceSetting ---

// ServiceSetting represents an SSM service setting key-value pair.
type ServiceSetting struct {
	SettingID    string `json:"SettingId"`
	SettingValue string `json:"SettingValue"`
	Status       string `json:"Status"`
}

// --- ResourcePolicy ---

// ResourcePolicy stores a policy document attached to an SSM resource.
type ResourcePolicy struct {
	PolicyID   string `json:"PolicyId"`
	PolicyHash string `json:"PolicyHash"`
	Policy     string `json:"Policy"`
}

// --- ExecutionPreview ---

// ExecutionPreview represents a preview of an SSM automation execution.
type ExecutionPreview struct {
	ExecutionPreviewID string `json:"ExecutionPreviewId"`
	Status             string `json:"Status"`
	DocumentName       string `json:"DocumentName"`
}

// --- Extended input/output types for previously-empty stubs ---

// CreateResourceDataSyncInputFull replaces the empty stub for CreateResourceDataSync.
type CreateResourceDataSyncInputFull struct {
	SyncName string `json:"SyncName"`
	SyncType string `json:"SyncType,omitempty"`
}

// ListResourceDataSyncOutputFull extends the empty stub output.
type ListResourceDataSyncOutputFull struct {
	ResourceDataSyncItems []ResourceDataSync `json:"ResourceDataSyncItems"`
	NextToken             string             `json:"NextToken,omitempty"`
}

// GetServiceSettingOutputFull extends the empty stub output.
type GetServiceSettingOutputFull struct {
	ServiceSetting *ServiceSetting `json:"ServiceSetting,omitempty"`
}

// GetResourcePoliciesOutputFull extends the empty stub output.
type GetResourcePoliciesOutputFull struct {
	Policies []ResourcePolicy `json:"Policies"`
}

// GetAutomationExecutionOutputFull extends the empty stub output.
type GetAutomationExecutionOutputFull struct {
	AutomationExecution *AutomationExecution `json:"AutomationExecution,omitempty"`
}

// DescribeAutomationExecutionsOutputFull extends the empty stub output.
type DescribeAutomationExecutionsOutputFull struct {
	AutomationExecutionMetadataList []AutomationExecution `json:"AutomationExecutionMetadataList"`
	NextToken                       string                `json:"NextToken,omitempty"`
}

// DescribeAutomationStepExecutionsOutputFull extends the empty stub output.
type DescribeAutomationStepExecutionsOutputFull struct {
	StepExecutions []AutomationStepExec `json:"StepExecutions"`
	NextToken      string               `json:"NextToken,omitempty"`
}

// DescribeSessionsOutputFull extends the empty stub output.
type DescribeSessionsOutputFull struct {
	Sessions  []Session `json:"Sessions"`
	NextToken string    `json:"NextToken,omitempty"`
}

// GetCalendarStateOutputFull has a State field.
type GetCalendarStateOutputFull struct {
	State       string `json:"State"`
	NextTransitionTime string `json:"NextTransitionTime,omitempty"`
}

// GetConnectionStatusOutputFull has a status field.
type GetConnectionStatusOutputFull struct {
	Target string `json:"Target"`
	Status string `json:"Status"`
}

// GetOpsSummaryOutputFull has summary counts.
type GetOpsSummaryOutputFull struct {
	Entities []OpsSummaryEntity `json:"Entities,omitempty"`
}

// OpsSummaryEntity represents a summary entry for ops items.
type OpsSummaryEntity struct {
	ID   string `json:"Id"`
	Data map[string]OpsSummaryValue `json:"Data,omitempty"`
}

// OpsSummaryValue holds aggregated value for an ops summary.
type OpsSummaryValue struct {
	Count int    `json:"Count"`
	Unit  string `json:"Unit"`
}

// ListOpsMetadataOutputFull extends the empty output.
type ListOpsMetadataOutputFull struct {
	OpsMetadataList []OpsMetadata `json:"OpsMetadataList"`
	NextToken       string        `json:"NextToken,omitempty"`
}

// ListAssociationsOutputFull extends the stub list output.
type ListAssociationsOutputFull struct {
	Associations []Association `json:"Associations"`
	NextToken    string        `json:"NextToken,omitempty"`
}

// ListAssociationVersionsOutputFull extends the empty output.
type ListAssociationVersionsOutputFull struct {
	AssociationVersions []Association `json:"AssociationVersions"`
	NextToken           string        `json:"NextToken,omitempty"`
}

// DescribeAssociationExecutionsOutputFull extends the empty output.
type DescribeAssociationExecutionsOutputFull struct {
	AssociationExecutions []AssociationExecution `json:"AssociationExecutions"`
	NextToken             string                 `json:"NextToken,omitempty"`
}

// AssociationExecution represents a single execution record of an association.
type AssociationExecution struct {
	AssociationID      string    `json:"AssociationId"`
	ExecutionID        string    `json:"ExecutionId"`
	Status             string    `json:"Status"`
	ExecutionDate      time.Time `json:"ExecutionDate"`
}

// DescribeAssociationExecutionTargetsOutputFull extends the empty output.
type DescribeAssociationExecutionTargetsOutputFull struct {
	AssociationExecutionTargets []AssociationExecutionTarget `json:"AssociationExecutionTargets"`
	NextToken                   string                       `json:"NextToken,omitempty"`
}

// AssociationExecutionTarget represents a single target of an association execution.
type AssociationExecutionTarget struct {
	AssociationID string `json:"AssociationId"`
	ExecutionID   string `json:"ExecutionId"`
	ResourceID    string `json:"ResourceId"`
	ResourceType  string `json:"ResourceType"`
	Status        string `json:"Status"`
}

// MaintenanceWindowExecution represents a single execution of a maintenance window.
type MaintenanceWindowExecution struct {
	WindowID          string    `json:"WindowId"`
	WindowExecutionID string    `json:"WindowExecutionId"`
	Status            string    `json:"Status"`
	StartTime         time.Time `json:"StartTime"`
	EndTime           *time.Time `json:"EndTime,omitempty"`
}

// MaintenanceWindowExecutionTask represents a task run within a window execution.
type MaintenanceWindowExecutionTask struct {
	WindowExecutionID string    `json:"WindowExecutionId"`
	TaskExecutionID   string    `json:"TaskExecutionId"`
	TaskARN           string    `json:"TaskArn"`
	Status            string    `json:"Status"`
	StartTime         time.Time `json:"StartTime"`
}

// MaintenanceWindowExecutionTaskInvocation represents a single invocation of a task.
type MaintenanceWindowExecutionTaskInvocation struct {
	WindowExecutionID string    `json:"WindowExecutionId"`
	TaskExecutionID   string    `json:"TaskExecutionId"`
	InvocationID      string    `json:"InvocationId"`
	Status            string    `json:"Status"`
	StartTime         time.Time `json:"StartTime"`
}

// DescribeMaintenanceWindowExecutionsOutputFull has executions list.
type DescribeMaintenanceWindowExecutionsOutputFull struct {
	WindowExecutions []MaintenanceWindowExecution `json:"WindowExecutions"`
	NextToken        string                       `json:"NextToken,omitempty"`
}

// DescribeMaintenanceWindowExecutionTasksOutputFull has tasks list.
type DescribeMaintenanceWindowExecutionTasksOutputFull struct {
	WindowExecutionTaskIdentities []MaintenanceWindowExecutionTask `json:"WindowExecutionTaskIdentities"`
	NextToken                     string                          `json:"NextToken,omitempty"`
}

// DescribeMaintenanceWindowExecutionTaskInvocationsOutputFull has invocations.
type DescribeMaintenanceWindowExecutionTaskInvocationsOutputFull struct {
	WindowExecutionTaskInvocationIdentities []MaintenanceWindowExecutionTaskInvocation `json:"WindowExecutionTaskInvocationIdentities"`
	NextToken string `json:"NextToken,omitempty"`
}

// DescribeMaintenanceWindowScheduleOutputFull has schedule entries.
type DescribeMaintenanceWindowScheduleOutputFull struct {
	ScheduledWindowExecutions []ScheduledWindowExecution `json:"ScheduledWindowExecutions"`
	NextToken                 string                     `json:"NextToken,omitempty"`
}

// ScheduledWindowExecution represents a future scheduled window execution.
type ScheduledWindowExecution struct {
	WindowID   string `json:"WindowId"`
	Name       string `json:"Name"`
	ExecutionTime string `json:"ExecutionTime"`
}

// NodeInfo represents an SSM managed node (instance).
type NodeInfo struct {
	InstanceID       string    `json:"InstanceId"`
	PlatformType     string    `json:"PlatformType"`
	AgentVersion     string    `json:"AgentVersion"`
	RegistrationDate time.Time `json:"RegistrationDate"`
}

// ListNodesOutputFull has nodes list.
type ListNodesOutputFull struct {
	Nodes     []NodeInfo `json:"Nodes"`
	NextToken string     `json:"NextToken,omitempty"`
}

// ListNodesSummaryOutputFull has summary.
type ListNodesSummaryOutputFull struct {
	Summary   []map[string]string `json:"Summary"`
	NextToken string              `json:"NextToken,omitempty"`
}

// DescribeEffectiveInstanceAssociationsOutputFull has effective associations.
type DescribeEffectiveInstanceAssociationsOutputFull struct {
	Associations []InstanceAssociationInfo `json:"Associations"`
	NextToken    string                   `json:"NextToken,omitempty"`
}

// InstanceAssociationInfo is a minimal association info for an instance.
type InstanceAssociationInfo struct {
	AssociationID      string `json:"AssociationId"`
	Name               string `json:"Name"`
	DocumentVersion    string `json:"DocumentVersion"`
	AssociationVersion string `json:"AssociationVersion"`
}

// DescribeInstanceAssociationsStatusOutputFull has status info.
type DescribeInstanceAssociationsStatusOutputFull struct {
	InstanceAssociationStatusInfos []InstanceAssociationStatusInfo `json:"InstanceAssociationStatusInfos"`
	NextToken                      string                          `json:"NextToken,omitempty"`
}

// InstanceAssociationStatusInfo has status of an association on an instance.
type InstanceAssociationStatusInfo struct {
	AssociationID string    `json:"AssociationId"`
	Name          string    `json:"Name"`
	Status        string    `json:"Status"`
	ExecutionDate time.Time `json:"ExecutionDate"`
}

// DescribeInstanceInformationOutputFull extends the empty stub.
type DescribeInstanceInformationOutputFull struct {
	InstanceInformationList []InstanceInformation `json:"InstanceInformationList"`
	NextToken               string                `json:"NextToken,omitempty"`
}

// InstanceInformation represents info about a managed instance.
type InstanceInformation struct {
	InstanceID       string    `json:"InstanceId"`
	PingStatus       string    `json:"PingStatus"`
	AgentVersion     string    `json:"AgentVersion"`
	PlatformType     string    `json:"PlatformType"`
	RegistrationDate time.Time `json:"RegistrationDate"`
}

// DescribeInstancePatchStatesOutputFull extends the empty stub.
type DescribeInstancePatchStatesOutputFull struct {
	InstancePatchStates []InstancePatchState `json:"InstancePatchStates"`
	NextToken           string               `json:"NextToken,omitempty"`
}

// InstancePatchState represents patch compliance state for an instance.
type InstancePatchState struct {
	InstanceID         string    `json:"InstanceId"`
	PatchGroup         string    `json:"PatchGroup"`
	BaselineID         string    `json:"BaselineId"`
	OperationStartTime time.Time `json:"OperationStartTime"`
	Operation          string    `json:"Operation"`
	FailedCount        int       `json:"FailedCount"`
	InstalledCount     int       `json:"InstalledCount"`
	MissingCount       int       `json:"MissingCount"`
}

// GetExecutionPreviewOutputFull extends the empty stub.
type GetExecutionPreviewOutputFull struct {
	ExecutionPreviewID string            `json:"ExecutionPreviewId"`
	Status             string            `json:"Status"`
	ExecutionPreview   *ExecutionPreview `json:"ExecutionPreview,omitempty"`
}

// StartExecutionPreviewOutputFull extends the empty stub.
type StartExecutionPreviewOutputFull struct {
	ExecutionPreviewID string `json:"ExecutionPreviewId"`
}

// StartAutomationExecutionOutputFull extends the empty stub.
type StartAutomationExecutionOutputFull struct {
	AutomationExecutionID string `json:"AutomationExecutionId"`
}

// StartChangeRequestExecutionOutputFull extends the empty stub.
type StartChangeRequestExecutionOutputFull struct {
	AutomationExecutionID string `json:"AutomationExecutionId"`
}

// GetAccessTokenOutputFull extends the empty stub.
type GetAccessTokenOutputFull struct {
	TokenValue      string `json:"TokenValue"`
	AccessRequestID string `json:"AccessRequestId,omitempty"`
}

// StartAccessRequestOutputFull extends the empty stub.
type StartAccessRequestOutputFull struct {
	AccessRequestID string `json:"AccessRequestId"`
}

// ResumeSessionOutputFull extends the empty stub.
type ResumeSessionOutputFull struct {
	SessionID  string `json:"SessionId"`
	StreamURL  string `json:"StreamUrl"`
	TokenValue string `json:"TokenValue"`
}

// ResetServiceSettingOutputFull extends the empty stub.
type ResetServiceSettingOutputFull struct {
	ServiceSetting *ServiceSetting `json:"ServiceSetting,omitempty"`
}

// PutResourcePolicyOutputFull extends the empty stub.
type PutResourcePolicyOutputFull struct {
	PolicyID   string `json:"PolicyId"`
	PolicyHash string `json:"PolicyHash"`
}

// LabelParameterVersionOutputFull extends the empty stub.
type LabelParameterVersionOutputFull struct {
	InvalidLabels []string `json:"InvalidLabels"`
	AddedLabels   []string `json:"AddedLabels"`
}

// UnlabelParameterVersionOutputFull extends the empty stub.
type UnlabelParameterVersionOutputFull struct {
	InvalidLabels []string `json:"InvalidLabels"`
	RemovedLabels []string `json:"RemovedLabels"`
}

// UpdateAssociationStatusOutputFull extends the empty stub.
type UpdateAssociationStatusOutputFull struct {
	AssociationDescription Association `json:"AssociationDescription"`
}

// GetMaintenanceWindowExecutionOutputFull extends the empty stub.
type GetMaintenanceWindowExecutionOutputFull struct {
	WindowID          string `json:"WindowId"`
	WindowExecutionID string `json:"WindowExecutionId"`
	Status            string `json:"Status"`
}

// GetMaintenanceWindowExecutionTaskOutputFull extends the empty stub.
type GetMaintenanceWindowExecutionTaskOutputFull struct {
	Status string `json:"Status"`
}

// GetMaintenanceWindowExecutionTaskInvocationOutputFull extends the empty stub.
type GetMaintenanceWindowExecutionTaskInvocationOutputFull struct {
	Status string `json:"Status"`
}
