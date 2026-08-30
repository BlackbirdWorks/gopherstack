package ssm

// SendAutomationSignalOutput is the response for SendAutomationSignal.
type SendAutomationSignalOutput struct{}

// StopAutomationExecutionOutput is the response for StopAutomationExecution.
type StopAutomationExecutionOutput struct{}

// AutomationExecutionFilterEntry filters DescribeAutomationExecutions
// results by ExecutionId, ExecutionStatus or DocumentNamePrefix
// (api_op_DescribeAutomationExecutions.go Filters member,
// types.AutomationExecutionFilterKey).
type AutomationExecutionFilterEntry struct {
	Key    string   `json:"Key,omitempty"`
	Values []string `json:"Values,omitempty"`
}

// DescribeAutomationExecutionsInput is the request for DescribeAutomationExecutions.
type DescribeAutomationExecutionsInput struct {
	MaxResults *int32                           `json:"MaxResults,omitempty"`
	NextToken  string                           `json:"NextToken,omitempty"`
	Filters    []AutomationExecutionFilterEntry `json:"Filters,omitempty"`
}

// DescribeAutomationExecutionsOutput is the response for DescribeAutomationExecutions.
type DescribeAutomationExecutionsOutput struct{}

// DescribeAutomationStepExecutionsInput is the request for DescribeAutomationStepExecutions.
type DescribeAutomationStepExecutionsInput struct {
	MaxResults            *int32 `json:"MaxResults,omitempty"`
	AutomationExecutionID string `json:"AutomationExecutionId"`
	NextToken             string `json:"NextToken,omitempty"`
}

// DescribeAutomationStepExecutionsOutput is the response for DescribeAutomationStepExecutions.
type DescribeAutomationStepExecutionsOutput struct{}

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

// GetExecutionPreviewInput is the request payload.
type GetExecutionPreviewInput struct {
	ExecutionPreviewID string `json:"ExecutionPreviewId"`
}

// GetExecutionPreviewOutput is the response payload.
type GetExecutionPreviewOutput struct{}

// SendAutomationSignalInput is the request payload. Payload (real, required
// for StartStep/StopStep/Resume signal types to name the target step) is
// accepted but not consulted -- see SendAutomationSignal's doc comment.
type SendAutomationSignalInput struct {
	Payload               map[string][]string `json:"Payload,omitempty"`
	AutomationExecutionID string              `json:"AutomationExecutionId"`
	SignalType            string              `json:"SignalType,omitempty"`
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

// Runbook mirrors types.Runbook (types.go:5718). TargetLocations/TargetMaps/
// TargetParameterName/Targets are deliberately not modeled -- the same
// shallow-scalar simplification StartAutomationExecutionInput already makes
// for its own Targets/TargetLocations/TargetParameterName (this file, above),
// an established convention in this backend, not new scope for this fix.
type Runbook struct {
	Parameters      map[string][]string `json:"Parameters,omitempty"`
	DocumentName    string              `json:"DocumentName"`
	DocumentVersion string              `json:"DocumentVersion,omitempty"`
	MaxConcurrency  string              `json:"MaxConcurrency,omitempty"`
	MaxErrors       string              `json:"MaxErrors,omitempty"`
}

// StartChangeRequestExecutionInput is the request payload.
type StartChangeRequestExecutionInput struct {
	DocumentName string    `json:"DocumentName"`
	Runbooks     []Runbook `json:"Runbooks"`
}

// StartChangeRequestExecutionOutput is the response payload.
type StartChangeRequestExecutionOutput struct{}

// StartExecutionPreviewInput is the request payload.
type StartExecutionPreviewInput struct {
	DocumentName string `json:"DocumentName,omitempty"`
}

// StartExecutionPreviewOutput is the response payload.
type StartExecutionPreviewOutput struct{}

// StopAutomationExecutionInput is the request payload. Type selects Cancel
// (the default) or Complete (real SDK types.StopType).
type StopAutomationExecutionInput struct {
	AutomationExecutionID string `json:"AutomationExecutionId"`
	Type                  string `json:"Type,omitempty"`
}

// AutomationExecution represents a running or completed SSM automation execution.
// Also serialized as AutomationExecutionMetadata for DescribeAutomationExecutions.
type AutomationExecution struct {
	Parameters            map[string][]string `json:"Parameters,omitempty"`
	Mode                  string              `json:"Mode,omitempty"`
	DocumentName          string              `json:"DocumentName"`
	DocumentVersion       string              `json:"DocumentVersion"`
	Status                string              `json:"AutomationExecutionStatus"`
	AutomationExecutionID string              `json:"AutomationExecutionId"`
	FailureMessage        string              `json:"FailureMessage,omitempty"`
	// AutomationSubtype (real SDK types.AutomationExecution.AutomationSubtype,
	// types.go:874) -- "Currently, the only supported value is ChangeRequest";
	// omitted for standard executions, matching real AWS. There is no real
	// "ExecutionType" member on either AutomationExecution or
	// AutomationExecutionMetadata at all -- that wire key belongs to the
	// unrelated ComplianceExecutionSummary type
	// (deserializers.go:27630/models_inventory.go's own ExecutionType field).
	AutomationSubtype string `json:"AutomationSubtype,omitempty"`
	MaxConcurrency    string `json:"MaxConcurrency,omitempty"`
	MaxErrors         string `json:"MaxErrors,omitempty"`
	// Never populated: real SSM sets this for a non-critical issue its engine
	// detects mid-run (types.go:801-803), but every execution here always
	// completes every step to Success (completeAutomationLocked) with no
	// partial-failure/degraded path to report one from.
	WarningMessage string `json:"WarningMessage,omitempty"`
	// Runbooks is populated only by StartChangeRequestExecution (the only op
	// whose real Input carries Runbooks); always empty for executions started
	// via StartAutomationExecution, matching real AWS.
	Runbooks      []Runbook            `json:"Runbooks,omitempty"`
	Steps         []AutomationStepExec `json:"StepExecutions,omitempty"`
	StartTime     float64              `json:"ExecutionStartTime"`
	EndTime       float64              `json:"ExecutionEndTime,omitempty"`
	completeAfter float64
}

// AutomationStepExec represents a single step in an automation execution.
type AutomationStepExec struct {
	StepName        string `json:"StepName"`
	Action          string `json:"Action"`
	StepStatus      string `json:"StepStatus"`
	StepExecutionID string `json:"StepExecutionId,omitempty"`
	FailureMessage  string `json:"FailureMessage,omitempty"`
	// Never populated: see AutomationExecution.WarningMessage.
	WarningMessage     string  `json:"WarningMessage,omitempty"`
	ExecutionStartTime float64 `json:"ExecutionStartTime,omitempty"`
	ExecutionEndTime   float64 `json:"ExecutionEndTime,omitempty"`
}

// ExecutionPreview represents a preview of an SSM automation execution.
type ExecutionPreview struct {
	ExecutionPreviewID string `json:"ExecutionPreviewId"`
	Status             string `json:"Status"`
	DocumentName       string `json:"DocumentName"`
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

// GetCalendarStateOutputFull has a State field.
type GetCalendarStateOutputFull struct {
	State              string `json:"State"`
	AtTime             string `json:"AtTime,omitempty"`
	NextTransitionTime string `json:"NextTransitionTime,omitempty"`
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
