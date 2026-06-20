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
	SyncCreatedTime time.Time `json:"SyncCreatedTime"`
	LastSyncTime    time.Time `json:"LastSyncTime,omitzero"`
	SyncName        string    `json:"SyncName"`
	SyncType        string    `json:"SyncType"`
	LastStatus      string    `json:"LastStatus"`
}

// --- AutomationExecution ---

// AutomationExecution represents a running or completed SSM automation execution.
type AutomationExecution struct {
	Parameters            map[string][]string  `json:"Parameters,omitempty"`
	StartTime             time.Time            `json:"StartTime"`
	EndTime               *time.Time           `json:"EndTime,omitempty"`
	AutomationExecutionID string               `json:"AutomationExecutionId"`
	DocumentName          string               `json:"DocumentName"`
	DocumentVersion       string               `json:"DocumentVersion"`
	Status                string               `json:"AutomationExecutionStatus"`
	ExecutionType         string               `json:"ExecutionType"` // "Standard" or "ChangeRequest"
	Mode                  string               `json:"Mode,omitempty"`
	Steps                 []AutomationStepExec `json:"StepExecutions,omitempty"`
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
	NextToken             string             `json:"NextToken,omitempty"`
	ResourceDataSyncItems []ResourceDataSync `json:"ResourceDataSyncItems"`
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
	NextToken                       string                `json:"NextToken,omitempty"`
	AutomationExecutionMetadataList []AutomationExecution `json:"AutomationExecutionMetadataList"`
}

// DescribeAutomationStepExecutionsOutputFull extends the empty stub output.
type DescribeAutomationStepExecutionsOutputFull struct {
	NextToken      string               `json:"NextToken,omitempty"`
	StepExecutions []AutomationStepExec `json:"StepExecutions"`
}

// DescribeSessionsOutputFull extends the empty stub output.
type DescribeSessionsOutputFull struct {
	NextToken string    `json:"NextToken,omitempty"`
	Sessions  []Session `json:"Sessions"`
}

// GetCalendarStateOutputFull has a State field.
type GetCalendarStateOutputFull struct {
	State              string `json:"State"`
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
	Data map[string]OpsSummaryValue `json:"Data,omitempty"`
	ID   string                     `json:"Id"`
}

// OpsSummaryValue holds aggregated value for an ops summary.
type OpsSummaryValue struct {
	Unit  string `json:"Unit"`
	Count int    `json:"Count"`
}

// ListOpsMetadataOutputFull extends the empty output.
type ListOpsMetadataOutputFull struct {
	NextToken       string        `json:"NextToken,omitempty"`
	OpsMetadataList []OpsMetadata `json:"OpsMetadataList"`
}

// ListAssociationsOutputFull extends the stub list output.
type ListAssociationsOutputFull struct {
	NextToken    string        `json:"NextToken,omitempty"`
	Associations []Association `json:"Associations"`
}

// ListAssociationVersionsOutputFull extends the empty output.
type ListAssociationVersionsOutputFull struct {
	NextToken           string        `json:"NextToken,omitempty"`
	AssociationVersions []Association `json:"AssociationVersions"`
}

// DescribeAssociationExecutionsOutputFull extends the empty output.
type DescribeAssociationExecutionsOutputFull struct {
	NextToken             string                 `json:"NextToken,omitempty"`
	AssociationExecutions []AssociationExecution `json:"AssociationExecutions"`
}

// AssociationExecution represents a single execution record of an association.
type AssociationExecution struct {
	ExecutionDate time.Time `json:"ExecutionDate"`
	AssociationID string    `json:"AssociationId"`
	ExecutionID   string    `json:"ExecutionId"`
	Status        string    `json:"Status"`
}

// DescribeAssociationExecutionTargetsOutputFull extends the empty output.
type DescribeAssociationExecutionTargetsOutputFull struct {
	NextToken                   string                       `json:"NextToken,omitempty"`
	AssociationExecutionTargets []AssociationExecutionTarget `json:"AssociationExecutionTargets"`
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
	StartTime         time.Time  `json:"StartTime"`
	EndTime           *time.Time `json:"EndTime,omitempty"`
	WindowID          string     `json:"WindowId"`
	WindowExecutionID string     `json:"WindowExecutionId"`
	Status            string     `json:"Status"`
}

// MaintenanceWindowExecutionTask represents a task run within a window execution.
type MaintenanceWindowExecutionTask struct {
	StartTime         time.Time `json:"StartTime"`
	WindowExecutionID string    `json:"WindowExecutionId"`
	TaskExecutionID   string    `json:"TaskExecutionId"`
	TaskARN           string    `json:"TaskArn"`
	Status            string    `json:"Status"`
}

// MaintenanceWindowExecutionTaskInvocation represents a single invocation of a task.
type MaintenanceWindowExecutionTaskInvocation struct {
	StartTime         time.Time `json:"StartTime"`
	WindowExecutionID string    `json:"WindowExecutionId"`
	TaskExecutionID   string    `json:"TaskExecutionId"`
	InvocationID      string    `json:"InvocationId"`
	Status            string    `json:"Status"`
}

// DescribeMaintenanceWindowExecutionsOutputFull has executions list.
type DescribeMaintenanceWindowExecutionsOutputFull struct {
	NextToken        string                       `json:"NextToken,omitempty"`
	WindowExecutions []MaintenanceWindowExecution `json:"WindowExecutions"`
}

// DescribeMaintenanceWindowExecutionTasksOutputFull has tasks list.
type DescribeMaintenanceWindowExecutionTasksOutputFull struct {
	NextToken                     string                           `json:"NextToken,omitempty"`
	WindowExecutionTaskIdentities []MaintenanceWindowExecutionTask `json:"WindowExecutionTaskIdentities"`
}

// DescribeMaintenanceWindowExecutionTaskInvocationsOutputFull has invocations.
type DescribeMaintenanceWindowExecutionTaskInvocationsOutputFull struct {
	NextToken                               string                                     `json:"NextToken,omitempty"`
	WindowExecutionTaskInvocationIdentities []MaintenanceWindowExecutionTaskInvocation `json:"WindowExecutionTaskInvocationIdentities"` //nolint:lll // AWS API field name is long by design
}

// DescribeMaintenanceWindowScheduleOutputFull has schedule entries.
type DescribeMaintenanceWindowScheduleOutputFull struct {
	NextToken                 string                     `json:"NextToken,omitempty"`
	ScheduledWindowExecutions []ScheduledWindowExecution `json:"ScheduledWindowExecutions"`
}

// ScheduledWindowExecution represents a future scheduled window execution.
type ScheduledWindowExecution struct {
	WindowID      string `json:"WindowId"`
	Name          string `json:"Name"`
	ExecutionTime string `json:"ExecutionTime"`
}

// NodeInfo represents an SSM managed node (instance).
type NodeInfo struct {
	RegistrationDate time.Time `json:"RegistrationDate"`
	InstanceID       string    `json:"InstanceId"`
	PlatformType     string    `json:"PlatformType"`
	AgentVersion     string    `json:"AgentVersion"`
}

// ListNodesOutputFull has nodes list.
type ListNodesOutputFull struct {
	NextToken string     `json:"NextToken,omitempty"`
	Nodes     []NodeInfo `json:"Nodes"`
}

// ListNodesSummaryOutputFull has summary.
type ListNodesSummaryOutputFull struct {
	NextToken string              `json:"NextToken,omitempty"`
	Summary   []map[string]string `json:"Summary"`
}

// DescribeEffectiveInstanceAssociationsOutputFull has effective associations.
type DescribeEffectiveInstanceAssociationsOutputFull struct {
	NextToken    string                    `json:"NextToken,omitempty"`
	Associations []InstanceAssociationInfo `json:"Associations"`
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
	NextToken                      string                          `json:"NextToken,omitempty"`
	InstanceAssociationStatusInfos []InstanceAssociationStatusInfo `json:"InstanceAssociationStatusInfos"`
}

// InstanceAssociationStatusInfo has status of an association on an instance.
type InstanceAssociationStatusInfo struct {
	ExecutionDate time.Time `json:"ExecutionDate"`
	AssociationID string    `json:"AssociationId"`
	Name          string    `json:"Name"`
	Status        string    `json:"Status"`
}

// DescribeInstanceInformationOutputFull extends the empty stub.
type DescribeInstanceInformationOutputFull struct {
	NextToken               string                `json:"NextToken,omitempty"`
	InstanceInformationList []InstanceInformation `json:"InstanceInformationList"`
}

// InstanceInformation represents info about a managed instance.
type InstanceInformation struct {
	RegistrationDate time.Time `json:"RegistrationDate"`
	InstanceID       string    `json:"InstanceId"`
	PingStatus       string    `json:"PingStatus"`
	AgentVersion     string    `json:"AgentVersion"`
	PlatformType     string    `json:"PlatformType"`
}

// DescribeInstancePatchStatesOutputFull extends the empty stub.
type DescribeInstancePatchStatesOutputFull struct {
	NextToken           string               `json:"NextToken,omitempty"`
	InstancePatchStates []InstancePatchState `json:"InstancePatchStates"`
}

// InstancePatchState represents patch compliance state for an instance.
type InstancePatchState struct {
	OperationStartTime time.Time `json:"OperationStartTime"`
	InstanceID         string    `json:"InstanceId"`
	PatchGroup         string    `json:"PatchGroup"`
	BaselineID         string    `json:"BaselineId"`
	Operation          string    `json:"Operation"`
	FailedCount        int       `json:"FailedCount"`
	InstalledCount     int       `json:"InstalledCount"`
	MissingCount       int       `json:"MissingCount"`
}

// GetExecutionPreviewOutputFull extends the empty stub.
type GetExecutionPreviewOutputFull struct {
	ExecutionPreview   *ExecutionPreview `json:"ExecutionPreview,omitempty"`
	ExecutionPreviewID string            `json:"ExecutionPreviewId"`
	Status             string            `json:"Status"`
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
	// ParameterVersion is the version of the parameter the labels were attached
	// to. AWS returns this so callers know which version a label-without-version
	// request resolved to.
	ParameterVersion int64 `json:"ParameterVersion"`
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
