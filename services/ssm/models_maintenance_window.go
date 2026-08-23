package ssm

// DeregisterTargetFromMaintenanceWindowOutput is the response for DeregisterTargetFromMaintenanceWindow.
type DeregisterTargetFromMaintenanceWindowOutput struct {
	WindowID       string `json:"WindowId"`
	WindowTargetID string `json:"WindowTargetId"`
}

// DeregisterTaskFromMaintenanceWindowOutput is the response for DeregisterTaskFromMaintenanceWindow.
type DeregisterTaskFromMaintenanceWindowOutput struct {
	WindowID     string `json:"WindowId"`
	WindowTaskID string `json:"WindowTaskId"`
}

// DeleteMaintenanceWindowInput is the request for DeleteMaintenanceWindow.
type DeleteMaintenanceWindowInput struct {
	WindowID string `json:"WindowId"`
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

// DescribeMaintenanceWindowExecutionTaskInvocationsInput is the request
// payload. TaskId is required on the real op
// (api_op_DescribeMaintenanceWindowExecutionTaskInvocations.go) despite the
// name suggesting it might be optional given DescribeMaintenanceWindowExecutionTasks
// already scopes to WindowExecutionId alone.
type DescribeMaintenanceWindowExecutionTaskInvocationsInput struct {
	WindowExecutionID string `json:"WindowExecutionId"`
	TaskID            string `json:"TaskId"`
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

// MaintenanceWindowIdentity is a lightweight maintenance window listing entry
// (real types.MaintenanceWindowIdentity, types.go:3706). NextExecutionTime
// uses the same fixed mwExecutionScheduleHours-from-now heuristic
// DescribeMaintenanceWindowSchedule already uses -- this backend has no real
// cron/rate-expression evaluator to derive a genuine next-run time from
// Schedule.
type MaintenanceWindowIdentity struct {
	WindowID          string `json:"WindowId"`
	Name              string `json:"Name"`
	Description       string `json:"Description,omitempty"`
	Schedule          string `json:"Schedule"`
	ScheduleTimezone  string `json:"ScheduleTimezone,omitempty"`
	StartDate         string `json:"StartDate,omitempty"`
	EndDate           string `json:"EndDate,omitempty"`
	NextExecutionTime string `json:"NextExecutionTime,omitempty"`
	Duration          int32  `json:"Duration"`
	Cutoff            int32  `json:"Cutoff"`
	ScheduleOffset    int32  `json:"ScheduleOffset,omitempty"`
	Enabled           bool   `json:"Enabled"`
}

// GetMaintenanceWindowInput is the request payload for GetMaintenanceWindow.
type GetMaintenanceWindowInput struct {
	WindowID string `json:"WindowId"`
}

// GetMaintenanceWindowOutput is the response payload for GetMaintenanceWindow.
type GetMaintenanceWindowOutput struct {
	MaintenanceWindow
}

// GetMaintenanceWindowExecutionInput is the request payload. WindowID has no
// member on the real op at all (api_op_GetMaintenanceWindowExecution.go
// declares only WindowExecutionId) -- harmless for a real aws-sdk-go-v2
// caller, which has no struct field to send it in, but kept here as an
// internal convenience the backend also derives from WindowExecutionId when
// absent (mwWindowIDFromExec).
type GetMaintenanceWindowExecutionInput struct {
	WindowID          string `json:"WindowId,omitempty"`
	WindowExecutionID string `json:"WindowExecutionId"`
}

// GetMaintenanceWindowExecutionOutput is the response payload.
type GetMaintenanceWindowExecutionOutput struct{}

// GetMaintenanceWindowExecutionTaskInput is the request payload.
// GetMaintenanceWindowExecutionTaskInput is the request payload. TaskExecutionID's
// real wire key is "TaskId", NOT "TaskExecutionId"
// (serializers.go awsAwsjson11_serializeOpDocumentGetMaintenanceWindowExecutionTaskInput
// emits only "TaskId"/"WindowExecutionId") -- the response side genuinely
// does use "TaskExecutionId" for a related-but-distinct member, which is
// presumably how this got confused. Pre-fix, a real client's TaskId was
// silently dropped by json.Unmarshal on every call.
type GetMaintenanceWindowExecutionTaskInput struct {
	WindowExecutionID string `json:"WindowExecutionId"`
	TaskExecutionID   string `json:"TaskId"`
}

// GetMaintenanceWindowExecutionTaskOutput is the response payload.
type GetMaintenanceWindowExecutionTaskOutput struct{}

// GetMaintenanceWindowExecutionTaskInvocationInput is the request payload.
// GetMaintenanceWindowExecutionTaskInvocationInput is the request payload.
// TaskExecutionID's real wire key is "TaskId", NOT "TaskExecutionId" (same
// bug and same class as GetMaintenanceWindowExecutionTaskInput -- confirmed
// separately against api_op_GetMaintenanceWindowExecutionTaskInvocation.go).
type GetMaintenanceWindowExecutionTaskInvocationInput struct {
	WindowExecutionID string `json:"WindowExecutionId"`
	TaskExecutionID   string `json:"TaskId"`
	InvocationID      string `json:"InvocationId"`
}

// GetMaintenanceWindowExecutionTaskInvocationOutput is the response payload.
type GetMaintenanceWindowExecutionTaskInvocationOutput struct{}

// WindowTarget is a target specification for maintenance window tasks.
type WindowTarget struct {
	Key    string   `json:"Key"`
	Values []string `json:"Values"`
}

// RegisterTargetWithMaintenanceWindowInput is the request payload.
type RegisterTargetWithMaintenanceWindowInput struct {
	WindowID     string         `json:"WindowId"`
	ResourceType string         `json:"ResourceType"`
	OwnerInfo    string         `json:"OwnerInformation,omitempty"`
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
	WindowID       string         `json:"WindowId"`
	TaskArn        string         `json:"TaskArn"`
	TaskType       string         `json:"TaskType"`
	Name           string         `json:"Name,omitempty"`
	Description    string         `json:"Description,omitempty"`
	ServiceRoleArn string         `json:"ServiceRoleArn,omitempty"`
	MaxConcurrency string         `json:"MaxConcurrency,omitempty"`
	MaxErrors      string         `json:"MaxErrors,omitempty"`
	Targets        []WindowTarget `json:"Targets,omitempty"`
	Priority       int32          `json:"Priority,omitempty"`
}

// RegisterTaskWithMaintenanceWindowOutput is the response payload.
type RegisterTaskWithMaintenanceWindowOutput struct {
	WindowTaskID string `json:"WindowTaskId"`
}

// UpdateMaintenanceWindowInput is the request payload for UpdateMaintenanceWindow.
type UpdateMaintenanceWindowInput struct {
	Enabled                  *bool  `json:"Enabled,omitempty"`
	AllowUnassociatedTargets *bool  `json:"AllowUnassociatedTargets,omitempty"`
	ScheduleOffset           *int32 `json:"ScheduleOffset,omitempty"`
	WindowID                 string `json:"WindowId"`
	Name                     string `json:"Name,omitempty"`
	Description              string `json:"Description,omitempty"`
	Schedule                 string `json:"Schedule,omitempty"`
	ScheduleTimezone         string `json:"ScheduleTimezone,omitempty"`
	StartDate                string `json:"StartDate,omitempty"`
	EndDate                  string `json:"EndDate,omitempty"`
	Duration                 int32  `json:"Duration,omitempty"`
	Cutoff                   int32  `json:"Cutoff,omitempty"`
}

// UpdateMaintenanceWindowOutput is the response payload for UpdateMaintenanceWindow.
type UpdateMaintenanceWindowOutput struct {
	MaintenanceWindow
}

// MaintenanceWindow represents an SSM maintenance window.
type MaintenanceWindow struct {
	WindowID         string `json:"WindowId"`
	Name             string `json:"Name"`
	Description      string `json:"Description,omitempty"`
	Schedule         string `json:"Schedule"`
	ScheduleTimezone string `json:"ScheduleTimezone,omitempty"`
	StartDate        string `json:"StartDate,omitempty"`
	EndDate          string `json:"EndDate,omitempty"`
	// NextExecutionTime is real (types.go GetMaintenanceWindowOutput) but
	// output-only -- it is computed fresh per response (mwNextExecutionTime),
	// never persisted, matching the same fixed-offset heuristic
	// DescribeMaintenanceWindowSchedule already uses.
	NextExecutionTime        string  `json:"NextExecutionTime,omitempty"`
	ScheduleOffset           int32   `json:"ScheduleOffset,omitempty"`
	Duration                 int32   `json:"Duration"`
	Cutoff                   int32   `json:"Cutoff"`
	AllowUnassociatedTargets bool    `json:"AllowUnassociatedTargets"`
	Enabled                  bool    `json:"Enabled"`
	CreatedDate              float64 `json:"CreatedDate"`
	ModifiedDate             float64 `json:"ModifiedDate"`
}

// CreateMaintenanceWindowInput is the request payload for CreateMaintenanceWindow.
type CreateMaintenanceWindowInput struct {
	Name                     string `json:"Name"`
	Description              string `json:"Description,omitempty"`
	Schedule                 string `json:"Schedule"`
	ScheduleTimezone         string `json:"ScheduleTimezone,omitempty"`
	StartDate                string `json:"StartDate,omitempty"`
	EndDate                  string `json:"EndDate,omitempty"`
	Tags                     []Tag  `json:"Tags,omitempty"`
	ScheduleOffset           int32  `json:"ScheduleOffset,omitempty"`
	Duration                 int32  `json:"Duration"`
	Cutoff                   int32  `json:"Cutoff"`
	AllowUnassociatedTargets bool   `json:"AllowUnassociatedTargets"`
}

// CreateMaintenanceWindowOutput is the response payload for CreateMaintenanceWindow.
type CreateMaintenanceWindowOutput struct {
	WindowID string `json:"WindowId"`
}

// CancelMaintenanceWindowExecutionInput is the request payload for CancelMaintenanceWindowExecution.
type CancelMaintenanceWindowExecutionInput struct {
	WindowExecutionID string `json:"WindowExecutionId"`
}

// CancelMaintenanceWindowExecutionOutput is the response payload for CancelMaintenanceWindowExecution.
type CancelMaintenanceWindowExecutionOutput struct {
	WindowExecutionID string `json:"WindowExecutionId"`
}

// MaintenanceWindowTarget represents a registered target for a maintenance window.
type MaintenanceWindowTarget struct {
	WindowID       string         `json:"WindowId"`
	WindowTargetID string         `json:"WindowTargetId"`
	ResourceType   string         `json:"ResourceType"`
	OwnerInfo      string         `json:"OwnerInformation,omitempty"`
	Name           string         `json:"Name,omitempty"`
	Description    string         `json:"Description,omitempty"`
	Targets        []WindowTarget `json:"Targets,omitempty"`
}

// MaintenanceWindowTask represents a registered task for a maintenance window.
// MaintenanceWindowTask's TaskType member has real wire key "Type"
// (deserializers.go awsAwsjson11_deserializeDocumentMaintenanceWindowTask,
// used by DescribeMaintenanceWindowTasks/GetMaintenanceWindowTask), NOT
// "TaskType" -- the request side (RegisterTaskWithMaintenanceWindowInput)
// genuinely does use "TaskType" (serializers.go
// awsAwsjson11_serializeOpDocumentRegisterTaskWithMaintenanceWindowInput),
// the same request/response wire-key inconsistency already found on
// GetMaintenanceWindowExecutionTask.
type MaintenanceWindowTask struct {
	WindowID       string         `json:"WindowId"`
	WindowTaskID   string         `json:"WindowTaskId"`
	TaskArn        string         `json:"TaskArn"`
	TaskType       string         `json:"Type"`
	Name           string         `json:"Name,omitempty"`
	Description    string         `json:"Description,omitempty"`
	ServiceRoleArn string         `json:"ServiceRoleArn,omitempty"`
	MaxConcurrency string         `json:"MaxConcurrency,omitempty"`
	MaxErrors      string         `json:"MaxErrors,omitempty"`
	Targets        []WindowTarget `json:"Targets,omitempty"`
	Priority       int32          `json:"Priority,omitempty"`
}

// MaintenanceWindowExecution represents a single execution of a maintenance window.
type MaintenanceWindowExecution struct {
	WindowID          string  `json:"WindowId"`
	WindowExecutionID string  `json:"WindowExecutionId"`
	Status            string  `json:"Status"`
	StartTime         float64 `json:"StartTime"`
	EndTime           float64 `json:"EndTime,omitempty"`
}

// MaintenanceWindowExecutionTask represents a task run within a window execution.
type MaintenanceWindowExecutionTask struct {
	WindowExecutionID string  `json:"WindowExecutionId"`
	TaskExecutionID   string  `json:"TaskExecutionId"`
	TaskARN           string  `json:"TaskArn"`
	Status            string  `json:"Status"`
	StartTime         float64 `json:"StartTime"`
}

// MaintenanceWindowExecutionTaskInvocation represents a single invocation of a task.
type MaintenanceWindowExecutionTaskInvocation struct {
	WindowExecutionID string  `json:"WindowExecutionId"`
	TaskExecutionID   string  `json:"TaskExecutionId"`
	InvocationID      string  `json:"InvocationId"`
	Status            string  `json:"Status"`
	StartTime         float64 `json:"StartTime"`
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

// GetMaintenanceWindowExecutionOutputFull is the response for GetMaintenanceWindowExecution.
type GetMaintenanceWindowExecutionOutputFull struct {
	WindowID          string  `json:"WindowId"`
	WindowExecutionID string  `json:"WindowExecutionId"`
	Status            string  `json:"Status"`
	StatusDetails     string  `json:"StatusDetails,omitempty"`
	StartTime         float64 `json:"StartTime"`
	EndTime           float64 `json:"EndTime,omitempty"`
}

// GetMaintenanceWindowExecutionTaskOutputFull is the response for
// GetMaintenanceWindowExecutionTask. Its task-type member's real wire key is
// "Type" (deserializers.go awsAwsjson11_deserializeOpDocumentGetMaintenanceWindowExecutionTaskOutput,
// case "Type") -- an AWS API inconsistency, since the sibling
// DescribeMaintenanceWindowExecutionTasks/MaintenanceWindowExecutionTaskIdentity
// really does use "TaskType" for the same concept, confirmed separately.
// ServiceRole is a real member with no Go field at all.
type GetMaintenanceWindowExecutionTaskOutputFull struct {
	WindowExecutionID string  `json:"WindowExecutionId,omitempty"`
	TaskExecutionID   string  `json:"TaskExecutionId,omitempty"`
	TaskARN           string  `json:"TaskArn,omitempty"`
	TaskType          string  `json:"Type,omitempty"`
	Status            string  `json:"Status"`
	StatusDetails     string  `json:"StatusDetails,omitempty"`
	MaxConcurrency    string  `json:"MaxConcurrency,omitempty"`
	MaxErrors         string  `json:"MaxErrors,omitempty"`
	ServiceRole       string  `json:"ServiceRole,omitempty"`
	StartTime         float64 `json:"StartTime"`
	EndTime           float64 `json:"EndTime,omitempty"`
	Priority          int32   `json:"Priority,omitempty"`
}

// GetMaintenanceWindowExecutionTaskInvocationOutputFull is the response for
// GetMaintenanceWindowExecutionTaskInvocation. Parameters (real, the actual
// command/automation parameters used for this invocation) is not modeled --
// this backend does not track per-invocation parameter snapshots, only the
// task-level defaults (MaintenanceWindowTask), disclosed rather than
// fabricated.
type GetMaintenanceWindowExecutionTaskInvocationOutputFull struct {
	WindowExecutionID string  `json:"WindowExecutionId,omitempty"`
	TaskExecutionID   string  `json:"TaskExecutionId,omitempty"`
	InvocationID      string  `json:"InvocationId,omitempty"`
	ExecutionID       string  `json:"ExecutionId,omitempty"`
	TaskType          string  `json:"TaskType,omitempty"`
	OwnerInformation  string  `json:"OwnerInformation,omitempty"`
	Status            string  `json:"Status"`
	StatusDetails     string  `json:"StatusDetails,omitempty"`
	WindowTargetID    string  `json:"WindowTargetId,omitempty"`
	StartTime         float64 `json:"StartTime"`
	EndTime           float64 `json:"EndTime,omitempty"`
}

// DeleteMaintenanceWindowOutput is the response payload for DeleteMaintenanceWindow.
type DeleteMaintenanceWindowOutput struct {
	WindowID string `json:"WindowId"`
}

// GetMaintenanceWindowTaskInput is the request payload for GetMaintenanceWindowTask.
type GetMaintenanceWindowTaskInput struct {
	WindowID     string `json:"WindowId"`
	WindowTaskID string `json:"WindowTaskId"`
}

// GetMaintenanceWindowTaskOutput is the response payload for
// GetMaintenanceWindowTask. It cannot simply embed MaintenanceWindowTask:
// despite describing the same concept, the two real response shapes use
// different wire keys for the task-type member -- GetMaintenanceWindowTaskOutput
// itself (api_op_GetMaintenanceWindowTask.go) uses "TaskType"
// (deserializers.go awsAwsjson11_deserializeOpDocumentGetMaintenanceWindowTaskOutput,
// case "TaskType"), while the shared types.MaintenanceWindowTask used by
// DescribeMaintenanceWindowTasks uses "Type" instead, confirmed separately.
type GetMaintenanceWindowTaskOutput struct {
	WindowID       string         `json:"WindowId,omitempty"`
	WindowTaskID   string         `json:"WindowTaskId,omitempty"`
	TaskArn        string         `json:"TaskArn,omitempty"`
	TaskType       string         `json:"TaskType,omitempty"`
	Name           string         `json:"Name,omitempty"`
	Description    string         `json:"Description,omitempty"`
	ServiceRoleArn string         `json:"ServiceRoleArn,omitempty"`
	MaxConcurrency string         `json:"MaxConcurrency,omitempty"`
	MaxErrors      string         `json:"MaxErrors,omitempty"`
	Targets        []WindowTarget `json:"Targets,omitempty"`
	Priority       int32          `json:"Priority,omitempty"`
}

// maintenanceWindowTaskToGetOutput projects a stored MaintenanceWindowTask
// onto GetMaintenanceWindowTaskOutput's own wire shape -- see that type's
// doc comment for why they cannot share a struct.
func maintenanceWindowTaskToGetOutput(t *MaintenanceWindowTask) GetMaintenanceWindowTaskOutput {
	return GetMaintenanceWindowTaskOutput{
		WindowID:       t.WindowID,
		WindowTaskID:   t.WindowTaskID,
		TaskArn:        t.TaskArn,
		TaskType:       t.TaskType,
		Name:           t.Name,
		Description:    t.Description,
		ServiceRoleArn: t.ServiceRoleArn,
		MaxConcurrency: t.MaxConcurrency,
		MaxErrors:      t.MaxErrors,
		Targets:        t.Targets,
		Priority:       t.Priority,
	}
}

// UpdateMaintenanceWindowTargetInput is the request payload for UpdateMaintenanceWindowTarget.
// Fields ordered for alignment.
type UpdateMaintenanceWindowTargetInput struct {
	WindowID       string         `json:"WindowId"`
	WindowTargetID string         `json:"WindowTargetId"`
	OwnerInfo      string         `json:"OwnerInformation,omitempty"`
	Name           string         `json:"Name,omitempty"`
	Description    string         `json:"Description,omitempty"`
	Targets        []WindowTarget `json:"Targets,omitempty"`
}

// UpdateMaintenanceWindowTargetOutput is the response payload for UpdateMaintenanceWindowTarget.
type UpdateMaintenanceWindowTargetOutput struct {
	WindowID       string         `json:"WindowId,omitempty"`
	WindowTargetID string         `json:"WindowTargetId,omitempty"`
	OwnerInfo      string         `json:"OwnerInformation,omitempty"`
	Name           string         `json:"Name,omitempty"`
	Description    string         `json:"Description,omitempty"`
	Targets        []WindowTarget `json:"Targets,omitempty"`
}

// UpdateMaintenanceWindowTaskInput is the request payload for UpdateMaintenanceWindowTask.
// Fields ordered for alignment.
type UpdateMaintenanceWindowTaskInput struct {
	Priority       *int32         `json:"Priority,omitempty"`
	WindowID       string         `json:"WindowId"`
	WindowTaskID   string         `json:"WindowTaskId"`
	TaskArn        string         `json:"TaskArn,omitempty"`
	Name           string         `json:"Name,omitempty"`
	Description    string         `json:"Description,omitempty"`
	ServiceRoleArn string         `json:"ServiceRoleArn,omitempty"`
	MaxConcurrency string         `json:"MaxConcurrency,omitempty"`
	MaxErrors      string         `json:"MaxErrors,omitempty"`
	Targets        []WindowTarget `json:"Targets,omitempty"`
}

// UpdateMaintenanceWindowTaskOutput is the response payload for UpdateMaintenanceWindowTask.
type UpdateMaintenanceWindowTaskOutput struct {
	WindowID       string         `json:"WindowId,omitempty"`
	WindowTaskID   string         `json:"WindowTaskId,omitempty"`
	TaskArn        string         `json:"TaskArn,omitempty"`
	Name           string         `json:"Name,omitempty"`
	Description    string         `json:"Description,omitempty"`
	ServiceRoleArn string         `json:"ServiceRoleArn,omitempty"`
	MaxConcurrency string         `json:"MaxConcurrency,omitempty"`
	MaxErrors      string         `json:"MaxErrors,omitempty"`
	Targets        []WindowTarget `json:"Targets,omitempty"`
	Priority       int32          `json:"Priority,omitempty"`
}

// DescribeMaintenanceWindowsForTargetInput is the request payload.
// Fields ordered for alignment.
type DescribeMaintenanceWindowsForTargetInput struct {
	MaxResults   *int64         `json:"MaxResults,omitempty"`
	ResourceType string         `json:"ResourceType,omitempty"`
	NextToken    string         `json:"NextToken,omitempty"`
	Targets      []WindowTarget `json:"Targets,omitempty"`
}

// DescribeMaintenanceWindowsForTargetOutput is the response payload.
type DescribeMaintenanceWindowsForTargetOutput struct {
	NextToken        string                      `json:"NextToken,omitempty"`
	WindowIdentities []MaintenanceWindowIdentity `json:"WindowIdentities"`
}
