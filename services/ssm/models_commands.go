package ssm

// Command represents a recorded SSM command.
type Command struct {
	Parameters             map[string][]string     `json:"Parameters,omitempty"`
	CloudWatchOutputConfig *CloudWatchOutputConfig `json:"CloudWatchOutputConfig,omitempty"`
	OutputS3BucketName     string                  `json:"OutputS3BucketName,omitempty"`
	CommandID              string                  `json:"CommandId"`
	Status                 string                  `json:"Status"`
	Comment                string                  `json:"Comment,omitempty"`
	DocumentName           string                  `json:"DocumentName"`
	OutputS3KeyPrefix      string                  `json:"OutputS3KeyPrefix,omitempty"`
	OutputS3Region         string                  `json:"OutputS3Region,omitempty"`
	StatusDetails          string                  `json:"StatusDetails,omitempty"`
	ServiceRole            string                  `json:"ServiceRole,omitempty"`
	MaxErrors              string                  `json:"MaxErrors,omitempty"`
	DocumentVersion        string                  `json:"DocumentVersion,omitempty"`
	MaxConcurrency         string                  `json:"MaxConcurrency,omitempty"`
	InstanceIDs            []string                `json:"InstanceIds,omitempty"`
	Targets                []CommandTarget         `json:"Targets,omitempty"`
	RequestedDateTime      float64                 `json:"RequestedDateTime"`
	ExpiresAfter           float64                 `json:"ExpiresAfter"`
	completeAfter          float64
	terminalAt             float64
	TimeoutSeconds         int32 `json:"TimeoutSeconds,omitempty"`
	TargetCount            int32 `json:"TargetCount,omitempty"`
	CompletedCount         int32 `json:"CompletedCount,omitempty"`
	ErrorCount             int32 `json:"ErrorCount,omitempty"`
}

// CommandInvocation represents the invocation of a command on an instance.
type CommandInvocation struct {
	Comment               string `json:"Comment,omitempty"`
	InstanceID            string `json:"InstanceId"`
	DocumentName          string `json:"DocumentName"`
	DocumentVersion       string `json:"DocumentVersion,omitempty"`
	Status                string `json:"Status"`
	StatusDetails         string `json:"StatusDetails"`
	StandardOutputContent string `json:"StandardOutputContent,omitempty"`
	StandardErrorContent  string `json:"StandardErrorContent,omitempty"`
	StandardErrorURL      string `json:"StandardErrorUrl,omitempty"`
	StandardOutputURL     string `json:"StandardOutputUrl,omitempty"`
	pendingStderr         string
	pendingStdout         string
	CommandID             string `json:"CommandId"`
	finalStatus           string
	CommandPlugins        []CommandPlugin `json:"CommandPlugins,omitempty"`
	startedUnix           float64
	RequestedDateTime     float64 `json:"RequestedDateTime"`
	executionEndUnix      float64
	ResponseCode          int32 `json:"ResponseCode"`
	direct                bool
}

// SendCommandInput is the request payload for SendCommand.
type SendCommandInput struct {
	Parameters             map[string][]string     `json:"Parameters,omitempty"`
	CloudWatchOutputConfig *CloudWatchOutputConfig `json:"CloudWatchOutputConfig,omitempty"`
	OutputS3KeyPrefix      string                  `json:"OutputS3KeyPrefix,omitempty"`
	DocumentHashType       string                  `json:"DocumentHashType,omitempty"`
	Comment                string                  `json:"Comment,omitempty"`
	OutputS3BucketName     string                  `json:"OutputS3BucketName,omitempty"`
	DocumentVersion        string                  `json:"DocumentVersion,omitempty"`
	OutputS3Region         string                  `json:"OutputS3Region,omitempty"`
	ServiceRoleArn         string                  `json:"ServiceRoleArn,omitempty"`
	MaxConcurrency         string                  `json:"MaxConcurrency,omitempty"`
	MaxErrors              string                  `json:"MaxErrors,omitempty"`
	DocumentName           string                  `json:"DocumentName"`
	InstanceIDs            []string                `json:"InstanceIds,omitempty"`
	Targets                []CommandTarget         `json:"Targets,omitempty"`
	TimeoutSeconds         int32                   `json:"TimeoutSeconds,omitempty"`
}

// CloudWatchOutputConfig is types.CloudWatchOutputConfig.
type CloudWatchOutputConfig struct {
	CloudWatchLogGroupName  string `json:"CloudWatchLogGroupName,omitempty"`
	CloudWatchOutputEnabled bool   `json:"CloudWatchOutputEnabled"`
}

// CommandPlugin is one plugin's result inside an invocation (types.CommandPlugin).
type CommandPlugin struct {
	StandardOutputURL      string `json:"StandardOutputUrl,omitempty"`
	Status                 string `json:"Status"`
	StatusDetails          string `json:"StatusDetails"`
	Output                 string `json:"Output"`
	OutputS3BucketName     string `json:"OutputS3BucketName,omitempty"`
	OutputS3KeyPrefix      string `json:"OutputS3KeyPrefix,omitempty"`
	Name                   string `json:"Name"`
	StandardErrorURL       string `json:"StandardErrorUrl,omitempty"`
	stdout                 string
	stderr                 string
	ResponseStartDateTime  float64 `json:"ResponseStartDateTime,omitempty"`
	ResponseFinishDateTime float64 `json:"ResponseFinishDateTime,omitempty"`
	ResponseCode           int32   `json:"ResponseCode"`
}

// CommandTarget names managed nodes via a Key/Values search criterion.
// Matches the SDK's generic types.Target{Key,Values} shape (same convention
// as AccessRequestTarget in models_sessions.go). SendCommand's own doc
// comment on types.Command.Targets: "Targets is required if you don't
// provide one or more managed node IDs in the call" -- the documented
// at-scale alternative to InstanceIds.
type CommandTarget struct {
	Key    string   `json:"Key"`
	Values []string `json:"Values"`
}

// SendCommandOutput is the response payload for SendCommand.
type SendCommandOutput struct {
	Command Command `json:"Command"`
}

// CommandFilter is a ListCommands/ListCommandInvocations Filters entry
// (api_op_ListCommands.go types.CommandFilter; wire keys "key"/"value" per
// serializers.go:9891-9897, not "Key"/"Value").
type CommandFilter struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// ListCommandsInput is the request payload for ListCommands.
type ListCommandsInput struct {
	MaxResults *int64          `json:"MaxResults,omitempty"`
	CommandID  string          `json:"CommandId,omitempty"`
	InstanceID string          `json:"InstanceId,omitempty"`
	NextToken  string          `json:"NextToken,omitempty"`
	Filters    []CommandFilter `json:"Filters,omitempty"`
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
	CloudWatchOutputConfig *CloudWatchOutputConfig `json:"CloudWatchOutputConfig,omitempty"`
	StandardErrorContent   string                  `json:"StandardErrorContent,omitempty"`
	Comment                string                  `json:"Comment,omitempty"`
	DocumentVersion        string                  `json:"DocumentVersion,omitempty"`
	PluginName             string                  `json:"PluginName,omitempty"`
	Status                 string                  `json:"Status"`
	StatusDetails          string                  `json:"StatusDetails"`
	DocumentName           string                  `json:"DocumentName"`
	StandardOutputURL      string                  `json:"StandardOutputUrl,omitempty"`
	StandardOutputContent  string                  `json:"StandardOutputContent,omitempty"`
	StandardErrorURL       string                  `json:"StandardErrorUrl,omitempty"`
	CommandID              string                  `json:"CommandId"`
	ExecutionStartDateTime string                  `json:"ExecutionStartDateTime,omitempty"`
	ExecutionEndDateTime   string                  `json:"ExecutionEndDateTime,omitempty"`
	InstanceID             string                  `json:"InstanceId"`
	ResponseCode           int32                   `json:"ResponseCode"`
}

// ListCommandInvocationsInput is the request payload for ListCommandInvocations.
// ListCommandInvocationsInput is the request payload for ListCommandInvocations.
//
// Details is not modeled: real AWS only populates CommandInvocation.
// CommandPlugins (per-plugin status/output) when Details=true, and this
// backend has no CommandPlugin type or per-plugin execution state at all
// (CommandInvocation stores one flat stdout/stderr per invocation) --
// accepted on the wire and otherwise inert.
type ListCommandInvocationsInput struct {
	MaxResults *int64          `json:"MaxResults,omitempty"`
	CommandID  string          `json:"CommandId,omitempty"`
	InstanceID string          `json:"InstanceId,omitempty"`
	NextToken  string          `json:"NextToken,omitempty"`
	Filters    []CommandFilter `json:"Filters,omitempty"`
	Details    bool            `json:"Details,omitempty"`
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
