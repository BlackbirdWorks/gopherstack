package lambda

import "time"

// PackageTypeImage is the only supported Lambda package type.
const PackageTypeImage = "Image"

// FunctionState represents the lifecycle state of a Lambda function.
type FunctionState string

const (
	// FunctionStateActive means the function is ready to be invoked.
	FunctionStateActive FunctionState = "Active"
	// FunctionStatePending means the function is being created.
	FunctionStatePending FunctionState = "Pending"
	// FunctionStateFailed means the function failed to deploy.
	FunctionStateFailed FunctionState = "Failed"
)

// InvocationType for Lambda invocations.
type InvocationType string

const (
	// InvocationTypeRequestResponse waits for the function to return.
	InvocationTypeRequestResponse InvocationType = "RequestResponse"
	// InvocationTypeEvent fires and forgets.
	InvocationTypeEvent InvocationType = "Event"
	// InvocationTypeDryRun validates without executing.
	InvocationTypeDryRun InvocationType = "DryRun"
)

// FunctionCode holds the code location for an image-based Lambda function.
type FunctionCode struct {
	ImageURI string `json:"ImageUri"`
}

// FunctionConfiguration represents a Lambda function's configuration.
type FunctionConfiguration struct {
	Environment  *EnvironmentConfig `json:"Environment,omitempty"`
	FunctionName string             `json:"FunctionName"`
	FunctionArn  string             `json:"FunctionArn"`
	Description  string             `json:"Description"`
	ImageURI     string             `json:"ImageUri"`
	PackageType  string             `json:"PackageType"`
	StateReason  string             `json:"StateReason,omitempty"`
	Role         string             `json:"Role"`
	LastModified string             `json:"LastModified"`
	Runtime      string             `json:"Runtime,omitempty"`
	RevisionID   string             `json:"RevisionId"`
	CreatedAt    time.Time          `json:"-"`
	State        FunctionState      `json:"State"`
	MemorySize   int                `json:"MemorySize"`
	Timeout      int                `json:"Timeout"`
	CodeSize     int64              `json:"CodeSize"`
}

// EnvironmentConfig holds Lambda function environment variables.
type EnvironmentConfig struct {
	Variables map[string]string `json:"Variables"`
}

// CreateFunctionInput holds the request body for CreateFunction.
type CreateFunctionInput struct {
	Environment  *EnvironmentConfig `json:"Environment,omitempty"`
	ImageConfig  *ImageConfig       `json:"ImageConfig,omitempty"`
	Code         *FunctionCode      `json:"Code"`
	FunctionName string             `json:"FunctionName"`
	Description  string             `json:"Description"`
	PackageType  string             `json:"PackageType"`
	Role         string             `json:"Role"`
	MemorySize   int                `json:"MemorySize"`
	Timeout      int                `json:"Timeout"`
}

// ImageConfig holds optional image command/entrypoint overrides.
type ImageConfig struct {
	Command          []string `json:"Command,omitempty"`
	WorkingDirectory string   `json:"WorkingDirectory,omitempty"`
	EntryPoint       []string `json:"EntryPoint,omitempty"`
}

// UpdateFunctionCodeInput holds the request body for UpdateFunctionCode.
type UpdateFunctionCodeInput struct {
	ImageURI string `json:"ImageUri"`
}

// UpdateFunctionConfigurationInput holds the request body for UpdateFunctionConfiguration.
type UpdateFunctionConfigurationInput struct {
	Environment *EnvironmentConfig `json:"Environment,omitempty"`
	Description string             `json:"Description,omitempty"`
	Role        string             `json:"Role,omitempty"`
	MemorySize  int                `json:"MemorySize,omitempty"`
	Timeout     int                `json:"Timeout,omitempty"`
}

// GetFunctionOutput is the response for GetFunction.
type GetFunctionOutput struct {
	Configuration *FunctionConfiguration `json:"Configuration"`
	Code          *FunctionCodeLocation  `json:"Code,omitempty"`
}

// FunctionCodeLocation describes where the function code is stored.
type FunctionCodeLocation struct {
	ImageURI       string `json:"ImageUri,omitempty"`
	RepositoryType string `json:"RepositoryType,omitempty"`
}

// ListFunctionsOutput is the response for ListFunctions.
type ListFunctionsOutput struct {
	NextMarker string                   `json:"NextMarker,omitempty"`
	Functions  []*FunctionConfiguration `json:"Functions"`
}

// Error represents an error response from Lambda.
type Error struct {
	Type    string `json:"__type"`
	Message string `json:"message"`
}
