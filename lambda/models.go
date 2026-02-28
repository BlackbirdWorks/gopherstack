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

// InvocationType for Lambda invocations. Defined as a type alias so that
// *InMemoryBackend directly satisfies the LambdaInvoker interfaces defined
// across multiple service packages without adapter structs.
type InvocationType = string

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
	ImageURI string `json:"ImageUri,omitempty"`
	S3Bucket string `json:"S3Bucket,omitempty"`
	S3Key    string `json:"S3Key,omitempty"`
	ZipFile  []byte `json:"ZipFile,omitempty"`
}

// FunctionConfiguration represents a Lambda function's configuration.
type FunctionConfiguration struct {
	CreatedAt    time.Time          `json:"-"`
	Environment  *EnvironmentConfig `json:"Environment,omitempty"`
	Handler      string             `json:"Handler,omitempty"`
	RevisionID   string             `json:"RevisionId"`
	ImageURI     string             `json:"ImageUri,omitempty"`
	PackageType  string             `json:"PackageType"`
	StateReason  string             `json:"StateReason,omitempty"`
	Role         string             `json:"Role"`
	LastModified string             `json:"LastModified"`
	Runtime      string             `json:"Runtime,omitempty"`
	FunctionArn  string             `json:"FunctionArn"`
	Description  string             `json:"Description"`
	FunctionName string             `json:"FunctionName"`
	State        FunctionState      `json:"State"`
	S3BucketCode string             `json:"-"`
	S3KeyCode    string             `json:"-"`
	ZipData      []byte             `json:"-"`
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
	ImageURI string `json:"ImageUri,omitempty"`
	S3Bucket string `json:"S3Bucket,omitempty"`
	S3Key    string `json:"S3Key,omitempty"`
	ZipFile  []byte `json:"ZipFile,omitempty"`
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

// FunctionURLConfig holds the configuration for a Lambda function URL.
type FunctionURLConfig struct {
	FunctionArn      string `json:"FunctionArn"`
	FunctionURL      string `json:"FunctionUrl"`
	AuthType         string `json:"AuthType"`
	CreationTime     string `json:"CreationTime"`
	LastModifiedTime string `json:"LastModifiedTime"`
}

// CreateFunctionURLConfigInput is the request body for CreateFunctionUrlConfig.
type CreateFunctionURLConfigInput struct {
	AuthType string `json:"AuthType"`
}

// ListFunctionURLConfigsOutput is the response for listing function URL configs.
type ListFunctionURLConfigsOutput struct {
	FunctionURLConfigs []*FunctionURLConfig `json:"FunctionUrlConfigs"`
}

// FunctionVersion holds an immutable snapshot of a Lambda function configuration at publish time.
type FunctionVersion struct {
	CreatedAt    string             `json:"LastModified"`
	Environment  *EnvironmentConfig `json:"Environment,omitempty"`
	Handler      string             `json:"Handler,omitempty"`
	RevisionID   string             `json:"RevisionId"`
	ImageURI     string             `json:"ImageUri,omitempty"`
	PackageType  string             `json:"PackageType"`
	Role         string             `json:"Role"`
	Runtime      string             `json:"Runtime,omitempty"`
	FunctionArn  string             `json:"FunctionArn"`
	Description  string             `json:"Description"`
	FunctionName string             `json:"FunctionName"`
	State        FunctionState      `json:"State"`
	Version      string             `json:"Version"`
	MemorySize   int                `json:"MemorySize"`
	Timeout      int                `json:"Timeout"`
	CodeSize     int64              `json:"CodeSize"`
}

// ListVersionsByFunctionOutput is the response for ListVersionsByFunction.
type ListVersionsByFunctionOutput struct {
	NextMarker string             `json:"NextMarker,omitempty"`
	Versions   []*FunctionVersion `json:"Versions"`
}

// FunctionAlias holds an alias mapping (alias name → version number).
type FunctionAlias struct {
	AliasArn        string `json:"AliasArn"`
	Description     string `json:"Description,omitempty"`
	FunctionVersion string `json:"FunctionVersion"`
	Name            string `json:"Name"`
	RevisionID      string `json:"RevisionId"`
}

// CreateAliasInput holds the request body for CreateAlias.
type CreateAliasInput struct {
	Description     string `json:"Description,omitempty"`
	FunctionVersion string `json:"FunctionVersion"`
	Name            string `json:"Name"`
}

// UpdateAliasInput holds the request body for UpdateAlias.
type UpdateAliasInput struct {
	Description     string `json:"Description,omitempty"`
	FunctionVersion string `json:"FunctionVersion,omitempty"`
}

// ListAliasesOutput is the response for ListAliases.
type ListAliasesOutput struct {
	NextMarker string           `json:"NextMarker,omitempty"`
	Aliases    []*FunctionAlias `json:"Aliases"`
}
