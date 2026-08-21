package ssm

// Command represents a recorded SSM command.
type Command struct {
	Parameters         map[string][]string `json:"Parameters,omitempty"`
	CommandID          string              `json:"CommandId"`
	DocumentName       string              `json:"DocumentName"`
	DocumentVersion    string              `json:"DocumentVersion,omitempty"`
	Status             string              `json:"Status"`
	Comment            string              `json:"Comment,omitempty"`
	OutputS3BucketName string              `json:"OutputS3BucketName,omitempty"`
	OutputS3KeyPrefix  string              `json:"OutputS3KeyPrefix,omitempty"`
	OutputS3Region     string              `json:"OutputS3Region,omitempty"`
	StatusDetails      string              `json:"StatusDetails,omitempty"`
	// ServiceRole is the real wire key for SendCommandInput.ServiceRoleArn
	// echoed on Command (api_op_SendCommand.go/types.Command -- the input and
	// output member names genuinely differ).
	ServiceRole       string   `json:"ServiceRole,omitempty"`
	MaxConcurrency    string   `json:"MaxConcurrency,omitempty"`
	MaxErrors         string   `json:"MaxErrors,omitempty"`
	InstanceIDs       []string `json:"InstanceIds,omitempty"`
	Targets           []any    `json:"Targets,omitempty"`
	RequestedDateTime float64  `json:"RequestedDateTime"`
	ExpiresAfter      float64  `json:"ExpiresAfter"`
	TimeoutSeconds    int32    `json:"TimeoutSeconds,omitempty"`
	// TargetCount/CompletedCount/ErrorCount are computed from the command's
	// invocations at read time (commandCounts, commands.go) -- not stored,
	// so they can never drift from the invocations they summarize.
	TargetCount    int32 `json:"TargetCount,omitempty"`
	CompletedCount int32 `json:"CompletedCount,omitempty"`
	ErrorCount     int32 `json:"ErrorCount,omitempty"`
	// completeAfter is the Unix timestamp (seconds) at which an InProgress
	// command lazily transitions to its terminal status. Zero means the command
	// completes on the next read (or was created without an exec delay).
	completeAfter float64
}

// CommandInvocation represents the invocation of a command on an instance.
type CommandInvocation struct {
	CommandID             string `json:"CommandId"`
	InstanceID            string `json:"InstanceId"`
	DocumentName          string `json:"DocumentName"`
	DocumentVersion       string `json:"DocumentVersion,omitempty"`
	Status                string `json:"Status"`
	StatusDetails         string `json:"StatusDetails"`
	StandardOutputContent string `json:"StandardOutputContent,omitempty"`
	StandardErrorContent  string `json:"StandardErrorContent,omitempty"`
	StandardOutputURL     string `json:"StandardOutputUrl,omitempty"`
	StandardErrorURL      string `json:"StandardErrorUrl,omitempty"`
	Comment               string `json:"Comment,omitempty"`
	// pendingStdout/pendingStderr hold the rendered command output that is
	// revealed (copied into StandardOutputContent/StandardErrorContent) only
	// once the invocation reaches a terminal status, mirroring AWS behaviour
	// where output is unavailable while a command is still InProgress.
	pendingStdout string
	pendingStderr string
	// finalStatus is the terminal status this invocation will resolve to.
	finalStatus       string
	RequestedDateTime float64 `json:"RequestedDateTime"`
	// ExecutionStartDateTime/ExecutionEndDateTime are real
	// GetCommandInvocationOutput members (api_op_GetCommandInvocation.go:90-
	// 109) written as ISO 8601 strings, NOT epoch numbers like every other
	// timestamp in this package -- confirmed against the op's own doc
	// comment, which gives an explicit ISO 8601 example. executionEndUnix
	// tracks completion internally (float64 epoch, this package's usual
	// convention) and is formatted to ExecutionEndDateTime only once the
	// invocation reaches a terminal status.
	executionEndUnix float64
}

// SendCommandInput is the request payload for SendCommand.
type SendCommandInput struct {
	Parameters         map[string][]string `json:"Parameters,omitempty"`
	DocumentName       string              `json:"DocumentName"`
	DocumentVersion    string              `json:"DocumentVersion,omitempty"`
	Comment            string              `json:"Comment,omitempty"`
	OutputS3BucketName string              `json:"OutputS3BucketName,omitempty"`
	OutputS3KeyPrefix  string              `json:"OutputS3KeyPrefix,omitempty"`
	OutputS3Region     string              `json:"OutputS3Region,omitempty"`
	ServiceRoleArn     string              `json:"ServiceRoleArn,omitempty"`
	MaxConcurrency     string              `json:"MaxConcurrency,omitempty"`
	MaxErrors          string              `json:"MaxErrors,omitempty"`
	InstanceIDs        []string            `json:"InstanceIds,omitempty"`
	Targets            []any               `json:"Targets,omitempty"`
	TimeoutSeconds     int32               `json:"TimeoutSeconds,omitempty"`
}

// SendCommandOutput is the response payload for SendCommand.
type SendCommandOutput struct {
	Command Command `json:"Command"`
}

// ListCommandsInput is the request payload for ListCommands.
type ListCommandsInput struct {
	CommandID  string `json:"CommandId,omitempty"`
	InstanceID string `json:"InstanceId,omitempty"`
	MaxResults *int64 `json:"MaxResults,omitempty"`
	NextToken  string `json:"NextToken,omitempty"`
}

// ListCommandsOutput is the response payload for ListCommands.
type ListCommandsOutput struct {
	NextToken string    `json:"NextToken,omitempty"`
	Commands  []Command `json:"Commands"`
}

// GetCommandInvocationInput is the request payload for GetCommandInvocation.
type GetCommandInvocationInput struct {
	CommandID  string `json:"CommandId"`
	InstanceID string `json:"InstanceId"`
	// PluginName selects one document step's output. This backend has no
	// per-plugin execution model (a whole document runs as a single unit --
	// see CommandPlugins in PARITY.md), so it is accepted and echoed back
	// on the response rather than used to select among plugins.
	PluginName string `json:"PluginName,omitempty"`
}

// GetCommandInvocationOutput is the response payload for GetCommandInvocation.
type GetCommandInvocationOutput struct {
	CommandID              string `json:"CommandId"`
	InstanceID             string `json:"InstanceId"`
	DocumentName           string `json:"DocumentName"`
	DocumentVersion        string `json:"DocumentVersion,omitempty"`
	PluginName             string `json:"PluginName,omitempty"`
	Status                 string `json:"Status"`
	StatusDetails          string `json:"StatusDetails"`
	StandardOutputContent  string `json:"StandardOutputContent,omitempty"`
	StandardErrorContent   string `json:"StandardErrorContent,omitempty"`
	StandardOutputURL      string `json:"StandardOutputUrl,omitempty"`
	StandardErrorURL       string `json:"StandardErrorUrl,omitempty"`
	Comment                string `json:"Comment,omitempty"`
	ExecutionStartDateTime string `json:"ExecutionStartDateTime,omitempty"`
	ExecutionEndDateTime   string `json:"ExecutionEndDateTime,omitempty"`
}

// ListCommandInvocationsInput is the request payload for ListCommandInvocations.
type ListCommandInvocationsInput struct {
	CommandID  string `json:"CommandId,omitempty"`
	InstanceID string `json:"InstanceId,omitempty"`
	MaxResults *int64 `json:"MaxResults,omitempty"`
	NextToken  string `json:"NextToken,omitempty"`
}

// ListCommandInvocationsOutput is the response payload for ListCommandInvocations.
type ListCommandInvocationsOutput struct {
	NextToken          string              `json:"NextToken,omitempty"`
	CommandInvocations []CommandInvocation `json:"CommandInvocations"`
}

// CancelCommandInput is the request payload for CancelCommand.
type CancelCommandInput struct {
	CommandID   string   `json:"CommandId"`
	InstanceIDs []string `json:"InstanceIds,omitempty"`
}

// CancelCommandOutput is the response payload for CancelCommand.
type CancelCommandOutput struct{}
