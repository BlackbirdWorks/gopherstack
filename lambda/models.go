package lambda

import "time"

// PackageTypeImage is the Image-based Lambda package type (Docker image).
const PackageTypeImage = "Image"

// PackageTypeZip is the Zip-based Lambda package type (code archive).
const PackageTypeZip = "Zip"

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

// FunctionCode holds the code location for a Lambda function.
// For Image package type, only ImageUri is used.
// For Zip package type, either ZipFile (inline base64) or S3Bucket+S3Key is used.
type FunctionCode struct {
	ZipFile  []byte `json:"ZipFile,omitempty"`
	ImageURI string `json:"ImageUri,omitempty"`
	S3Bucket string `json:"S3Bucket,omitempty"`
	S3Key    string `json:"S3Key,omitempty"`
}

// FunctionConfiguration represents a Lambda function's configuration.
type FunctionConfiguration struct {
	Environment  *EnvironmentConfig `json:"Environment,omitempty"`
	FunctionName string             `json:"FunctionName"`
	FunctionArn  string             `json:"FunctionArn"`
	Description  string             `json:"Description"`
	ImageURI     string             `json:"ImageUri,omitempty"`
	PackageType  string             `json:"PackageType"`
	StateReason  string             `json:"StateReason,omitempty"`
	Role         string             `json:"Role"`
	LastModified string             `json:"LastModified"`
	Runtime      string             `json:"Runtime,omitempty"`
	Handler      string             `json:"Handler,omitempty"`
	RevisionID   string             `json:"RevisionId"`
	CreatedAt    time.Time          `json:"-"`
	// ZipData holds the raw zip bytes for Zip-packaged functions.
	// This field is internal and never serialized to JSON.
	ZipData []byte `json:"-"`
	// S3BucketCode and S3KeyCode hold the S3 location for Zip-packaged functions
	// when code is stored in S3. These fields are internal.
	S3BucketCode string `json:"-"`
	S3KeyCode    string `json:"-"`
	State        FunctionState `json:"State"`
	MemorySize   int           `json:"MemorySize"`
	Timeout      int           `json:"Timeout"`
	CodeSize     int64         `json:"CodeSize"`
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
	Runtime      string             `json:"Runtime,omitempty"`
	Handler      string             `json:"Handler,omitempty"`
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
	ZipFile  []byte `json:"ZipFile,omitempty"`
	ImageURI string `json:"ImageUri,omitempty"`
	S3Bucket string `json:"S3Bucket,omitempty"`
	S3Key    string `json:"S3Key,omitempty"`
}

// UpdateFunctionConfigurationInput holds the request body for UpdateFunctionConfiguration.
type UpdateFunctionConfigurationInput struct {
	Environment *EnvironmentConfig `json:"Environment,omitempty"`
	Description string             `json:"Description,omitempty"`
	Runtime     string             `json:"Runtime,omitempty"`
	Handler     string             `json:"Handler,omitempty"`
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
	Location       string `json:"Location,omitempty"`
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
