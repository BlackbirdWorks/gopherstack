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

// LastUpdateStatus represents the status of the last update to a Lambda function.
type LastUpdateStatus string

const (
	// LastUpdateStatusSuccessful means the last update succeeded.
	LastUpdateStatusSuccessful LastUpdateStatus = "Successful"
	// LastUpdateStatusFailed means the last update failed.
	LastUpdateStatusFailed LastUpdateStatus = "Failed"
	// LastUpdateStatusInProgress means an update is in progress.
	LastUpdateStatusInProgress LastUpdateStatus = "InProgress"
)

// InvocationType for Lambda invocations. Must remain a type alias (= string) because
// multiple packages (apigateway, eventbridge, secretsmanager, stepfunctions) define
// LambdaInvoker interfaces with InvokeFunction(... invocationType string ...).
// A defined type would prevent *InMemoryBackend from satisfying those interfaces.
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

// FunctionLayer holds a layer reference attached to a Lambda function.
type FunctionLayer struct {
	Arn      string `json:"Arn,omitempty"`
	CodeSize int64  `json:"CodeSize,omitempty"`
}

// FunctionConfiguration represents a Lambda function's configuration.
type FunctionConfiguration struct {
	CreatedAt                    time.Time          `json:"-"`
	Environment                  *EnvironmentConfig `json:"Environment,omitempty"`
	ReservedConcurrentExecutions *int               `json:"ReservedConcurrentExecutions,omitempty"`
	ImageURI                     string             `json:"ImageUri,omitempty"`
	LastUpdateStatus             LastUpdateStatus   `json:"LastUpdateStatus"`
	PackageType                  string             `json:"PackageType"`
	StateReason                  string             `json:"StateReason,omitempty"`
	Role                         string             `json:"Role"`
	LastModified                 string             `json:"LastModified"`
	Runtime                      string             `json:"Runtime,omitempty"`
	RevisionID                   string             `json:"RevisionId"`
	Description                  string             `json:"Description"`
	FunctionArn                  string             `json:"FunctionArn"`
	State                        FunctionState      `json:"State"`
	FunctionName                 string             `json:"FunctionName"`
	S3BucketCode                 string             `json:"-"`
	S3KeyCode                    string             `json:"-"`
	Handler                      string             `json:"Handler,omitempty"`
	Version                      string             `json:"Version,omitempty"`
	ZipData                      []byte             `json:"-"`
	Layers                       []*FunctionLayer   `json:"Layers,omitempty"`
	MemorySize                   int                `json:"MemorySize"`
	Timeout                      int                `json:"Timeout"`
	CodeSize                     int64              `json:"CodeSize"`
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
	// Layers is a list of layer ARN strings supplied by the client.
	Layers     []string `json:"Layers,omitempty"`
	MemorySize int      `json:"MemorySize"`
	Timeout    int      `json:"Timeout"`
	// Publish, when true, creates the function and immediately publishes version 1.
	Publish bool `json:"Publish,omitempty"`
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
	Layers      []string           `json:"Layers,omitempty"`
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
	Layers       []*FunctionLayer   `json:"Layers,omitempty"`
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

// LayerVersionContent holds the content details for a layer version.
type LayerVersionContent struct {
	CodeSha256 string `json:"CodeSha256,omitempty"`
	Location   string `json:"Location,omitempty"`
	CodeSize   int64  `json:"CodeSize"`
}

// LayerVersion holds an immutable snapshot of a layer version.
type LayerVersion struct {
	Content            *LayerVersionContent `json:"Content,omitempty"`
	LayerVersionArn    string               `json:"LayerVersionArn"`
	Description        string               `json:"Description,omitempty"`
	CreatedDate        string               `json:"CreatedDate"`
	LicenseInfo        string               `json:"LicenseInfo,omitempty"`
	CompatibleRuntimes []string             `json:"CompatibleRuntimes,omitempty"`
	ZipData            []byte               `json:"-"`
	Version            int64                `json:"Version"`
}

// Layer holds a summary of a layer with its latest version.
type Layer struct {
	LatestMatchingVersion *LayerVersion `json:"LatestMatchingVersion,omitempty"`
	LayerArn              string        `json:"LayerArn"`
	LayerName             string        `json:"LayerName"`
}

// PublishLayerVersionInput is the request body for PublishLayerVersion.
type PublishLayerVersionInput struct {
	Content            *LayerVersionContentInput `json:"Content"`
	Description        string                    `json:"Description,omitempty"`
	LayerName          string                    `json:"-"`
	LicenseInfo        string                    `json:"LicenseInfo,omitempty"`
	CompatibleRuntimes []string                  `json:"CompatibleRuntimes,omitempty"`
}

// LayerVersionContentInput holds the zip content for a new layer version.
type LayerVersionContentInput struct {
	S3Bucket string `json:"S3Bucket,omitempty"`
	S3Key    string `json:"S3Key,omitempty"`
	ZipFile  []byte `json:"ZipFile,omitempty"`
}

// PublishLayerVersionOutput is the response for PublishLayerVersion.
type PublishLayerVersionOutput struct {
	Content            *LayerVersionContent `json:"Content"`
	LayerVersionArn    string               `json:"LayerVersionArn"`
	LayerArn           string               `json:"LayerArn"`
	Description        string               `json:"Description,omitempty"`
	CreatedDate        string               `json:"CreatedDate"`
	LicenseInfo        string               `json:"LicenseInfo,omitempty"`
	CompatibleRuntimes []string             `json:"CompatibleRuntimes,omitempty"`
	Version            int64                `json:"Version"`
}

// GetLayerVersionOutput is the response for GetLayerVersion.
type GetLayerVersionOutput struct {
	Content            *LayerVersionContent `json:"Content"`
	LayerVersionArn    string               `json:"LayerVersionArn"`
	LayerArn           string               `json:"LayerArn"`
	Description        string               `json:"Description,omitempty"`
	CreatedDate        string               `json:"CreatedDate"`
	LicenseInfo        string               `json:"LicenseInfo,omitempty"`
	CompatibleRuntimes []string             `json:"CompatibleRuntimes,omitempty"`
	Version            int64                `json:"Version"`
}

// ListLayersOutput is the response for ListLayers.
type ListLayersOutput struct {
	NextMarker string   `json:"NextMarker,omitempty"`
	Layers     []*Layer `json:"Layers"`
}

// ListLayerVersionsOutput is the response for ListLayerVersions.
type ListLayerVersionsOutput struct {
	NextMarker    string          `json:"NextMarker,omitempty"`
	LayerVersions []*LayerVersion `json:"LayerVersions"`
}

// LayerVersionStatement is a single statement in a layer version resource policy.
type LayerVersionStatement struct {
	Principal   string `json:"Principal"`
	Action      string `json:"Action"`
	StatementID string `json:"StatementId"`
}

// LayerVersionPolicy holds the resource policy for a layer version.
type LayerVersionPolicy struct {
	Policy     string `json:"Policy"`
	RevisionID string `json:"RevisionId"`
}

// AddLayerVersionPermissionInput is the request body for AddLayerVersionPermission.
type AddLayerVersionPermissionInput struct {
	Action         string `json:"Action"`
	Principal      string `json:"Principal"`
	StatementID    string `json:"StatementId"`
	OrganizationID string `json:"OrganizationId,omitempty"`
}

// AddLayerVersionPermissionOutput is the response for AddLayerVersionPermission.
type AddLayerVersionPermissionOutput struct {
	Statement  string `json:"Statement"`
	RevisionID string `json:"RevisionId"`
}

// Destination holds the ARN for an async invocation destination (SQS, SNS, Lambda, or EventBridge).
type Destination struct {
	Destination string `json:"Destination"`
}

// DestinationConfig holds optional success and failure destinations for async invocations.
type DestinationConfig struct {
	OnFailure *Destination `json:"OnFailure,omitempty"`
	OnSuccess *Destination `json:"OnSuccess,omitempty"`
}

// FunctionEventInvokeConfig holds the async invocation configuration for a Lambda function.
type FunctionEventInvokeConfig struct {
	LastModified             time.Time          `json:"LastModified"`
	DestinationConfig        *DestinationConfig `json:"DestinationConfig,omitempty"`
	MaximumEventAgeInSeconds *int               `json:"MaximumEventAgeInSeconds,omitempty"`
	MaximumRetryAttempts     *int               `json:"MaximumRetryAttempts,omitempty"`
	FunctionArn              string             `json:"FunctionArn"`
}

// PutFunctionEventInvokeConfigInput is the shared request body for Put/Update FunctionEventInvokeConfig.
type PutFunctionEventInvokeConfigInput struct {
	DestinationConfig        *DestinationConfig `json:"DestinationConfig,omitempty"`
	MaximumEventAgeInSeconds *int               `json:"MaximumEventAgeInSeconds,omitempty"`
	MaximumRetryAttempts     *int               `json:"MaximumRetryAttempts,omitempty"`
}

// ListFunctionEventInvokeConfigsOutput is the response for ListFunctionEventInvokeConfigs.
type ListFunctionEventInvokeConfigsOutput struct {
	NextMarker                 string                       `json:"NextMarker,omitempty"`
	FunctionEventInvokeConfigs []*FunctionEventInvokeConfig `json:"FunctionEventInvokeConfigs"`
}

// FunctionConcurrency holds the reserved concurrency configuration for a Lambda function.
type FunctionConcurrency struct {
	ReservedConcurrentExecutions int `json:"ReservedConcurrentExecutions"`
}

// PutFunctionConcurrencyInput is the request body for PutFunctionConcurrency.
type PutFunctionConcurrencyInput struct {
	ReservedConcurrentExecutions int `json:"ReservedConcurrentExecutions"`
}

// ProvisionedConcurrencyConfig holds the provisioned concurrency configuration for a function version or alias.
type ProvisionedConcurrencyConfig struct {
	FunctionArn                              string `json:"FunctionArn,omitempty"`
	LastModified                             string `json:"LastModified"`
	Status                                   string `json:"Status"`
	StatusReason                             string `json:"StatusReason,omitempty"`
	AllocatedProvisionedConcurrentExecutions int    `json:"AllocatedProvisionedConcurrentExecutions"`
	AvailableProvisionedConcurrentExecutions int    `json:"AvailableProvisionedConcurrentExecutions"`
	RequestedProvisionedConcurrentExecutions int    `json:"RequestedProvisionedConcurrentExecutions"`
}

// PutProvisionedConcurrencyConfigInput is the request body for PutProvisionedConcurrencyConfig.
type PutProvisionedConcurrencyConfigInput struct {
	ProvisionedConcurrentExecutions int `json:"ProvisionedConcurrentExecutions"`
}

// ListProvisionedConcurrencyConfigsOutput is the response for ListProvisionedConcurrencyConfigs.
type ListProvisionedConcurrencyConfigsOutput struct {
	NextMarker                    string                          `json:"NextMarker,omitempty"`
	ProvisionedConcurrencyConfigs []*ProvisionedConcurrencyConfig `json:"ProvisionedConcurrencyConfigs"`
}
