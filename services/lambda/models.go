package lambda

import (
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

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
	// FunctionStateInactive means the function has been idle and its execution environment has been reclaimed.
	FunctionStateInactive FunctionState = "Inactive"
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

// EphemeralStorageConfig holds the ephemeral storage (/tmp) configuration for a Lambda function.
type EphemeralStorageConfig struct {
	Size int32 `json:"Size"`
}

// VpcConfig holds the VPC configuration for a Lambda function.
type VpcConfig struct {
	Ipv6AllowedForDualStack *bool    `json:"Ipv6AllowedForDualStack,omitempty"`
	VpcID                   string   `json:"VpcId,omitempty"`
	SubnetIDs               []string `json:"SubnetIds,omitempty"`
	SecurityGroupIDs        []string `json:"SecurityGroupIds,omitempty"`
}

// TracingConfig holds the X-Ray tracing configuration for a Lambda function.
type TracingConfig struct {
	Mode string `json:"Mode"`
}

// FileSystemConfig holds an EFS mount configuration for a Lambda function.
type FileSystemConfig struct {
	Arn            string `json:"Arn"`
	LocalMountPath string `json:"LocalMountPath"`
}

// DeadLetterConfig holds the dead-letter queue/topic configuration for a Lambda function.
type DeadLetterConfig struct {
	TargetArn string `json:"TargetArn,omitempty"`
}

// LoggingConfig holds the function's logging configuration.
// Real AWS returns this on every GetFunction / GetFunctionConfiguration response.
type LoggingConfig struct {
	ApplicationLogLevel string `json:"ApplicationLogLevel,omitempty"`
	LogFormat           string `json:"LogFormat"`
	LogGroup            string `json:"LogGroup,omitempty"`
	SystemLogLevel      string `json:"SystemLogLevel,omitempty"`
}

type FunctionConfiguration struct {
	CreatedAt                    time.Time               `json:"-"`
	Environment                  *EnvironmentConfig      `json:"Environment,omitempty"`
	EphemeralStorage             *EphemeralStorageConfig `json:"EphemeralStorage,omitempty"`
	LoggingConfig                *LoggingConfig          `json:"LoggingConfig,omitempty"`
	ReservedConcurrentExecutions *int                    `json:"ReservedConcurrentExecutions,omitempty"`
	VpcConfig                    *VpcConfig              `json:"VpcConfig,omitempty"`
	TracingConfig                *TracingConfig          `json:"TracingConfig,omitempty"`
	DeadLetterConfig             *DeadLetterConfig       `json:"DeadLetterConfig,omitempty"`
	ImageConfig                  *ImageConfig            `json:"ImageConfig,omitempty"`
	Tags                         map[string]string       `json:"Tags,omitempty"`
	SnapStart                    *SnapStartResponse      `json:"SnapStart,omitempty"`
	ImageURI                     string                  `json:"ImageUri,omitempty"`
	LastUpdateStatus             LastUpdateStatus        `json:"LastUpdateStatus"`
	LastUpdateStatusReason       string                  `json:"LastUpdateStatusReason,omitempty"`
	// MasterArn is the ARN of the owner function for Lambda@Edge replicas.
	// When set, GetFunctionConfiguration returns this field to signal the function is an edge replica.
	MasterArn         string              `json:"MasterArn,omitempty"`
	PackageType       string              `json:"PackageType"`
	StateReason       string              `json:"StateReason,omitempty"`
	StateReasonCode   string              `json:"StateReasonCode,omitempty"`
	Role              string              `json:"Role"`
	LastModified      string              `json:"LastModified"`
	Runtime           string              `json:"Runtime,omitempty"`
	RevisionID        string              `json:"RevisionId"`
	Description       string              `json:"Description"`
	FunctionArn       string              `json:"FunctionArn"`
	State             FunctionState       `json:"State"`
	FunctionName      string              `json:"FunctionName"`
	CodeSha256        string              `json:"CodeSha256,omitempty"`
	S3BucketCode      string              `json:"-"`
	S3KeyCode         string              `json:"-"`
	Handler           string              `json:"Handler,omitempty"`
	Version           string              `json:"Version,omitempty"`
	FileSystemConfigs []*FileSystemConfig `json:"FileSystemConfigs,omitempty"`
	ZipData           []byte              `json:"-"`
	Layers            []*FunctionLayer    `json:"Layers,omitempty"`
	Architectures     []string            `json:"Architectures,omitempty"`
	MemorySize        int                 `json:"MemorySize"`
	Timeout           int                 `json:"Timeout"`
	CodeSize          int64               `json:"CodeSize"`
}

// EnvironmentConfig holds Lambda function environment variables.
type EnvironmentConfig struct {
	Variables map[string]string `json:"Variables"`
}

// CreateFunctionInput holds the request body for CreateFunction.
type CreateFunctionInput struct {
	Environment       *EnvironmentConfig      `json:"Environment,omitempty"`
	ImageConfig       *ImageConfig            `json:"ImageConfig,omitempty"`
	VpcConfig         *VpcConfig              `json:"VpcConfig,omitempty"`
	TracingConfig     *TracingConfig          `json:"TracingConfig,omitempty"`
	DeadLetterConfig  *DeadLetterConfig       `json:"DeadLetterConfig,omitempty"`
	EphemeralStorage  *EphemeralStorageConfig `json:"EphemeralStorage,omitempty"`
	Code              *FunctionCode           `json:"Code"`
	SnapStart         *SnapStart              `json:"SnapStart,omitempty"`
	Tags              map[string]string       `json:"Tags,omitempty"`
	FunctionName      string                  `json:"FunctionName"`
	Description       string                  `json:"Description"`
	PackageType       string                  `json:"PackageType"`
	Runtime           string                  `json:"Runtime,omitempty"`
	Handler           string                  `json:"Handler,omitempty"`
	Role              string                  `json:"Role"`
	FileSystemConfigs []*FileSystemConfig     `json:"FileSystemConfigs,omitempty"`
	// Layers is a list of layer ARN strings supplied by the client.
	Layers        []string `json:"Layers,omitempty"`
	Architectures []string `json:"Architectures,omitempty"`
	MemorySize    int      `json:"MemorySize"`
	Timeout       int      `json:"Timeout"`
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
	Architectures []string `json:"Architectures,omitempty"`
	ImageURI      string   `json:"ImageUri,omitempty"`
	S3Bucket      string   `json:"S3Bucket,omitempty"`
	S3Key         string   `json:"S3Key,omitempty"`
	ZipFile       []byte   `json:"ZipFile,omitempty"`
	// Publish, when true, publishes a new numbered version after the code update.
	Publish bool `json:"Publish,omitempty"`
}

// UpdateFunctionConfigurationInput holds the request body for UpdateFunctionConfiguration.
type UpdateFunctionConfigurationInput struct {
	Environment       *EnvironmentConfig      `json:"Environment,omitempty"`
	VpcConfig         *VpcConfig              `json:"VpcConfig,omitempty"`
	TracingConfig     *TracingConfig          `json:"TracingConfig,omitempty"`
	DeadLetterConfig  *DeadLetterConfig       `json:"DeadLetterConfig,omitempty"`
	EphemeralStorage  *EphemeralStorageConfig `json:"EphemeralStorage,omitempty"`
	SnapStart         *SnapStart              `json:"SnapStart,omitempty"`
	Description       string                  `json:"Description,omitempty"`
	Runtime           string                  `json:"Runtime,omitempty"`
	Handler           string                  `json:"Handler,omitempty"`
	Role              string                  `json:"Role,omitempty"`
	FileSystemConfigs []*FileSystemConfig     `json:"FileSystemConfigs,omitempty"`
	Layers            []string                `json:"Layers,omitempty"`
	MemorySize        int                     `json:"MemorySize,omitempty"`
	Timeout           int                     `json:"Timeout,omitempty"`
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

// FunctionURLCors holds CORS configuration for a Lambda function URL.
type FunctionURLCors struct {
	AllowOrigins     []string `json:"AllowOrigins,omitempty"`
	AllowMethods     []string `json:"AllowMethods,omitempty"`
	AllowHeaders     []string `json:"AllowHeaders,omitempty"`
	ExposeHeaders    []string `json:"ExposeHeaders,omitempty"`
	MaxAge           int      `json:"MaxAge,omitempty"`
	AllowCredentials bool     `json:"AllowCredentials,omitempty"`
}

// FunctionURLConfig holds the configuration for a Lambda function URL.
type FunctionURLConfig struct {
	Cors             *FunctionURLCors `json:"Cors,omitempty"`
	FunctionArn      string           `json:"FunctionArn"`
	FunctionURL      string           `json:"FunctionUrl"`
	AuthType         string           `json:"AuthType"`
	InvokeMode       string           `json:"InvokeMode,omitempty"`
	CreationTime     string           `json:"CreationTime"`
	LastModifiedTime string           `json:"LastModifiedTime"`
}

// CreateFunctionURLConfigInput is the request body for CreateFunctionUrlConfig.
type CreateFunctionURLConfigInput struct {
	Cors       *FunctionURLCors `json:"Cors,omitempty"`
	AuthType   string           `json:"AuthType"`
	InvokeMode string           `json:"InvokeMode,omitempty"`
}

// ListFunctionURLConfigsOutput is the response for listing function URL configs.
type ListFunctionURLConfigsOutput struct {
	FunctionURLConfigs []*FunctionURLConfig `json:"FunctionUrlConfigs"`
}

// FunctionVersion holds an immutable snapshot of a Lambda function configuration at publish time.
type FunctionVersion struct {
	Environment       *EnvironmentConfig  `json:"Environment,omitempty"`
	VpcConfig         *VpcConfig          `json:"VpcConfig,omitempty"`
	TracingConfig     *TracingConfig      `json:"TracingConfig,omitempty"`
	FileSystemConfigs []*FileSystemConfig `json:"FileSystemConfigs,omitempty"`
	DeadLetterConfig  *DeadLetterConfig   `json:"DeadLetterConfig,omitempty"`
	ImageConfig       *ImageConfig        `json:"ImageConfig,omitempty"`
	SnapStart         *SnapStartResponse  `json:"SnapStart,omitempty"`
	FunctionArn       string              `json:"FunctionArn"`
	FunctionName      string              `json:"FunctionName"`
	RevisionID        string              `json:"RevisionId"`
	ImageURI          string              `json:"ImageUri,omitempty"`
	PackageType       string              `json:"PackageType"`
	Role              string              `json:"Role"`
	Runtime           string              `json:"Runtime,omitempty"`
	CreatedAt         string              `json:"LastModified"`
	Handler           string              `json:"Handler,omitempty"`
	State             FunctionState       `json:"State"`
	Description       string              `json:"Description"`
	Version           string              `json:"Version"`
	CodeSha256        string              `json:"CodeSha256,omitempty"`
	Layers            []*FunctionLayer    `json:"Layers,omitempty"`
	MemorySize        int                 `json:"MemorySize"`
	Timeout           int                 `json:"Timeout"`
	CodeSize          int64               `json:"CodeSize"`
}

// ListVersionsByFunctionOutput is the response for ListVersionsByFunction.
type ListVersionsByFunctionOutput struct {
	NextMarker string             `json:"NextMarker,omitempty"`
	Versions   []*FunctionVersion `json:"Versions"`
}

// AliasRoutingConfig holds weighted traffic routing for a Lambda alias.
type AliasRoutingConfig struct {
	AdditionalVersionWeights map[string]float64 `json:"AdditionalVersionWeights,omitempty"`
}

// FunctionAlias holds an alias mapping (alias name → version number).
type FunctionAlias struct {
	RoutingConfig   *AliasRoutingConfig `json:"RoutingConfig,omitempty"`
	AliasArn        string              `json:"AliasArn"`
	Description     string              `json:"Description,omitempty"`
	FunctionVersion string              `json:"FunctionVersion"`
	Name            string              `json:"Name"`
	RevisionID      string              `json:"RevisionId"`
}

// CreateAliasInput holds the request body for CreateAlias.
type CreateAliasInput struct {
	RoutingConfig   *AliasRoutingConfig `json:"RoutingConfig,omitempty"`
	Description     string              `json:"Description,omitempty"`
	FunctionVersion string              `json:"FunctionVersion"`
	Name            string              `json:"Name"`
}

// UpdateAliasInput holds the request body for UpdateAlias.
type UpdateAliasInput struct {
	RoutingConfig   *AliasRoutingConfig `json:"RoutingConfig,omitempty"`
	Description     string              `json:"Description,omitempty"`
	FunctionVersion string              `json:"FunctionVersion,omitempty"`
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

// FunctionPermission is a single statement in a function resource-based policy.
type FunctionPermission struct {
	Action        string `json:"Action"`
	Effect        string `json:"Effect"`
	FunctionName  string `json:"-"`
	Principal     string `json:"Principal"`
	SourceAccount string `json:"SourceAccount,omitempty"`
	SourceArn     string `json:"SourceArn,omitempty"`
	StatementID   string `json:"Sid"`
}

// AddPermissionInput is the request body for AddPermission.
type AddPermissionInput struct {
	Action        string `json:"Action"`
	Principal     string `json:"Principal"`
	StatementID   string `json:"StatementId"`
	SourceAccount string `json:"SourceAccount,omitempty"`
	SourceArn     string `json:"SourceArn,omitempty"`
}

// AddPermissionOutput is the response for AddPermission.
type AddPermissionOutput struct {
	Statement *string `json:"Statement,omitempty"`
}

// GetPolicyOutput is the response for GetPolicy (returns the function resource policy JSON).
type GetPolicyOutput struct {
	Policy     *string `json:"Policy,omitempty"`
	RevisionID *string `json:"RevisionId,omitempty"`
}

// AllowedPublishers holds the signing profile version ARNs allowed for code signing.
type AllowedPublishers struct {
	SigningProfileVersionArns []string `json:"SigningProfileVersionArns"`
}

// CodeSigningPolicies holds the Lambda code signing policy.
type CodeSigningPolicies struct {
	UntrustedArtifactOnDeployment string `json:"UntrustedArtifactOnDeployment,omitempty"`
}

// CodeSigningConfig holds a Lambda code signing configuration.
type CodeSigningConfig struct {
	AllowedPublishers    *AllowedPublishers   `json:"AllowedPublishers"`
	CodeSigningConfigArn string               `json:"CodeSigningConfigArn"`
	CodeSigningConfigID  string               `json:"CodeSigningConfigId"`
	CodeSigningPolicies  *CodeSigningPolicies `json:"CodeSigningPolicies,omitempty"`
	Description          string               `json:"Description,omitempty"`
	LastModified         string               `json:"LastModified"`
}

// CreateCodeSigningConfigInput is the request body for CreateCodeSigningConfig.
type CreateCodeSigningConfigInput struct {
	AllowedPublishers   *AllowedPublishers   `json:"AllowedPublishers"`
	CodeSigningPolicies *CodeSigningPolicies `json:"CodeSigningPolicies,omitempty"`
	Description         string               `json:"Description,omitempty"`
}

// CreateCodeSigningConfigOutput is the response for CreateCodeSigningConfig.
type CreateCodeSigningConfigOutput struct {
	CodeSigningConfig *CodeSigningConfig `json:"CodeSigningConfig"`
}

// UpdateCodeSigningConfigInput is the request body for UpdateCodeSigningConfig.
type UpdateCodeSigningConfigInput struct {
	AllowedPublishers   *AllowedPublishers   `json:"AllowedPublishers,omitempty"`
	CodeSigningPolicies *CodeSigningPolicies `json:"CodeSigningPolicies,omitempty"`
	Description         string               `json:"Description,omitempty"`
}

// UpdateCodeSigningConfigOutput is the response for UpdateCodeSigningConfig.
type UpdateCodeSigningConfigOutput struct {
	CodeSigningConfig *CodeSigningConfig `json:"CodeSigningConfig"`
}

// ListCodeSigningConfigsOutput is the response for ListCodeSigningConfigs.
type ListCodeSigningConfigsOutput struct {
	NextMarker         string               `json:"NextMarker,omitempty"`
	CodeSigningConfigs []*CodeSigningConfig `json:"CodeSigningConfigs"`
}

// GetFunctionCodeSigningConfigOutput is the response for GetFunctionCodeSigningConfig.
type GetFunctionCodeSigningConfigOutput struct {
	CodeSigningConfigArn string `json:"CodeSigningConfigArn"`
	FunctionName         string `json:"FunctionName"`
}

// PutFunctionCodeSigningConfigInput is the request body for PutFunctionCodeSigningConfig.
type PutFunctionCodeSigningConfigInput struct {
	CodeSigningConfigArn string `json:"CodeSigningConfigArn"`
}

// PutFunctionCodeSigningConfigOutput is the response for PutFunctionCodeSigningConfig.
type PutFunctionCodeSigningConfigOutput struct {
	CodeSigningConfigArn string `json:"CodeSigningConfigArn"`
	FunctionName         string `json:"FunctionName"`
}

// ListFunctionsByCodeSigningConfigOutput is the response for ListFunctionsByCodeSigningConfig.
type ListFunctionsByCodeSigningConfigOutput struct {
	NextMarker   string   `json:"NextMarker,omitempty"`
	FunctionArns []string `json:"FunctionArns"`
}

// CapacityProvider holds a Lambda capacity provider configuration.
type CapacityProvider struct {
	CapacityProviderArn       string `json:"CapacityProviderArn"`
	LastModifiedTime          string `json:"LastModifiedTime"`
	Name                      string `json:"Name"`
	Status                    string `json:"Status,omitempty"`
	TargetOnDemandConcurrency int    `json:"TargetOnDemandConcurrency,omitempty"`
}

// CreateCapacityProviderInput is the request body for CreateCapacityProvider.
type CreateCapacityProviderInput struct {
	Name                      string `json:"Name"`
	TargetOnDemandConcurrency int    `json:"TargetOnDemandConcurrency,omitempty"`
}

// CreateCapacityProviderOutput is the response for CreateCapacityProvider.
type CreateCapacityProviderOutput struct {
	CapacityProvider *CapacityProvider `json:"CapacityProvider"`
}

// UpdateCapacityProviderInput is the request body for UpdateCapacityProvider.
type UpdateCapacityProviderInput struct {
	TargetOnDemandConcurrency int `json:"TargetOnDemandConcurrency,omitempty"`
}

// UpdateCapacityProviderOutput is the response for UpdateCapacityProvider.
type UpdateCapacityProviderOutput struct {
	CapacityProvider *CapacityProvider `json:"CapacityProvider"`
}

// ListCapacityProvidersOutput is the response for ListCapacityProviders.
type ListCapacityProvidersOutput struct {
	NextMarker        string              `json:"NextMarker,omitempty"`
	CapacityProviders []*CapacityProvider `json:"CapacityProviders"`
}

// AccountLimit holds the Lambda account-level limits.
type AccountLimit struct {
	CodeSizeUnzipped               int64 `json:"CodeSizeUnzipped"`
	CodeSizeZipped                 int64 `json:"CodeSizeZipped"`
	ConcurrentExecutions           int   `json:"ConcurrentExecutions"`
	TotalCodeSize                  int64 `json:"TotalCodeSize"`
	UnreservedConcurrentExecutions int   `json:"UnreservedConcurrentExecutions"`
}

// AccountUsage holds the Lambda account-level usage.
type AccountUsage struct {
	FunctionCount int   `json:"FunctionCount"`
	TotalCodeSize int64 `json:"TotalCodeSize"`
}

// AccountSettingsOutput is the response for GetAccountSettings.
type AccountSettingsOutput struct {
	AccountLimit *AccountLimit `json:"AccountLimit"`
	AccountUsage *AccountUsage `json:"AccountUsage"`
}

// buildCodeSigningConfigARN constructs a Lambda code signing config ARN.
func buildCodeSigningConfigARN(region, accountID, cscID string) string {
	return arn.Build("lambda", region, accountID, fmt.Sprintf("code-signing-config:%s", cscID))
}

// buildCapacityProviderARN constructs a Lambda capacity provider ARN.
func buildCapacityProviderARN(region, accountID, name string) string {
	return arn.Build("lambda", region, accountID, fmt.Sprintf("capacity-provider:%s", name))
}

// CheckpointDurableExecutionInput is the request body for CheckpointDurableExecution.
type CheckpointDurableExecutionInput struct {
	Marker *string `json:"Marker,omitempty"`
}

// CheckpointDurableExecutionOutput is the response for CheckpointDurableExecution.
type CheckpointDurableExecutionOutput struct{}

// UpdateFunctionURLConfigInput is the request body for UpdateFunctionUrlConfig.
type UpdateFunctionURLConfigInput struct {
	Cors     *FunctionURLCors `json:"Cors,omitempty"`
	AuthType string           `json:"AuthType,omitempty"`
}

// RuntimeManagementConfig holds the runtime management configuration for a Lambda function.
type RuntimeManagementConfig struct {
	// RuntimeVersionArn is set when UpdateRuntimeOn is "Manual".
	RuntimeVersionArn string `json:"RuntimeVersionArn,omitempty"`
	// UpdateRuntimeOn controls when the runtime is updated: "Auto", "FunctionUpdate", or "Manual".
	UpdateRuntimeOn string `json:"UpdateRuntimeOn"`
	FunctionArn     string `json:"FunctionArn,omitempty"`
}

// PutRuntimeManagementConfigInput is the request body for PutRuntimeManagementConfig.
type PutRuntimeManagementConfigInput struct {
	RuntimeVersionArn string `json:"RuntimeVersionArn,omitempty"`
	UpdateRuntimeOn   string `json:"UpdateRuntimeOn"`
}

// FunctionRecursionConfig holds the recursion detection configuration for a Lambda function.
type FunctionRecursionConfig struct {
	RecursiveLoop string `json:"RecursiveLoop"`
}

// PutFunctionRecursionConfigInput is the request body for PutFunctionRecursionConfig.
type PutFunctionRecursionConfigInput struct {
	RecursiveLoop string `json:"RecursiveLoop"`
}

// FunctionScalingConfig holds the scaling configuration for a Lambda function.
type FunctionScalingConfig struct {
	MaximumConcurrency *int   `json:"MaximumConcurrency,omitempty"`
	FunctionArn        string `json:"FunctionArn,omitempty"`
}

// PutFunctionScalingConfigInput is the request body for PutFunctionScalingConfig.
type PutFunctionScalingConfigInput struct {
	MaximumConcurrency *int `json:"MaximumConcurrency,omitempty"`
}

// SnapStart holds the SnapStart configuration for a Lambda function.
// ApplyOn can be "PublishedVersions" or "None".
type SnapStart struct {
	ApplyOn string `json:"ApplyOn"`
}

// SnapStartResponse is the SnapStart field returned in GetFunction/GetFunctionConfiguration.
type SnapStartResponse struct {
	ApplyOn            string `json:"ApplyOn"`
	OptimizationStatus string `json:"OptimizationStatus"`
}
