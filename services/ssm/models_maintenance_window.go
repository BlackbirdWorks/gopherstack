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
	WindowID                 string  `json:"WindowId"`
	Name                     string  `json:"Name"`
	Description              string  `json:"Description,omitempty"`
	Schedule                 string  `json:"Schedule"`
	ScheduleTimezone         string  `json:"ScheduleTimezone,omitempty"`
	StartDate                string  `json:"StartDate,omitempty"`
	EndDate                  string  `json:"EndDate,omitempty"`
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
	OwnerInfo      string         `json:"OwnerInfo,omitempty"`
	Name           string         `json:"Name,omitempty"`
	Description    string         `json:"Description,omitempty"`
	Targets        []WindowTarget `json:"Targets,omitempty"`
}

// MaintenanceWindowTask represents a registered task for a maintenance window.
type MaintenanceWindowTask struct {
	WindowID       string         `json:"WindowId"`
	WindowTaskID   string         `json:"WindowTaskId"`
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

// GetMaintenanceWindowExecutionTaskOutputFull is the response for GetMaintenanceWindowExecutionTask.
type GetMaintenanceWindowExecutionTaskOutputFull struct {
	WindowExecutionID string  `json:"WindowExecutionId,omitempty"`
	TaskExecutionID   string  `json:"TaskExecutionId,omitempty"`
	TaskARN           string  `json:"TaskArn,omitempty"`
	TaskType          string  `json:"TaskType,omitempty"`
	Status            string  `json:"Status"`
	StatusDetails     string  `json:"StatusDetails,omitempty"`
	MaxConcurrency    string  `json:"MaxConcurrency,omitempty"`
	MaxErrors         string  `json:"MaxErrors,omitempty"`
	StartTime         float64 `json:"StartTime"`
	EndTime           float64 `json:"EndTime,omitempty"`
	Priority          int32   `json:"Priority,omitempty"`
}

// GetMaintenanceWindowExecutionTaskInvocationOutputFull is the response for
// GetMaintenanceWindowExecutionTaskInvocation.
type GetMaintenanceWindowExecutionTaskInvocationOutputFull struct {
	WindowExecutionID string  `json:"WindowExecutionId,omitempty"`
	TaskExecutionID   string  `json:"TaskExecutionId,omitempty"`
	InvocationID      string  `json:"InvocationId,omitempty"`
	ExecutionID       string  `json:"ExecutionId,omitempty"`
	TaskType          string  `json:"TaskType,omitempty"`
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

// GetMaintenanceWindowTaskOutput is the response payload for GetMaintenanceWindowTask.
type GetMaintenanceWindowTaskOutput struct {
	MaintenanceWindowTask
}

// UpdateMaintenanceWindowTargetInput is the request payload for UpdateMaintenanceWindowTarget.
// Fields ordered for alignment.
type UpdateMaintenanceWindowTargetInput struct {
	WindowID       string         `json:"WindowId"`
	WindowTargetID string         `json:"WindowTargetId"`
	OwnerInfo      string         `json:"OwnerInfo,omitempty"`
	Name           string         `json:"Name,omitempty"`
	Description    string         `json:"Description,omitempty"`
	Targets        []WindowTarget `json:"Targets,omitempty"`
}

// UpdateMaintenanceWindowTargetOutput is the response payload for UpdateMaintenanceWindowTarget.
type UpdateMaintenanceWindowTargetOutput struct {
	WindowID       string         `json:"WindowId,omitempty"`
	WindowTargetID string         `json:"WindowTargetId,omitempty"`
	OwnerInfo      string         `json:"OwnerInfo,omitempty"`
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
