package lambdaweb

import (
	"time"
)

// Wire shapes follow aws-sdk-go-v2/service/lambdaweb v1.2.5 (restJson1 via schemas/schemas.go): member names are
// the lowerCamel schema names, timestamps are date-time strings, and only the Policy/RevisionId/Tags members of the
// policy and tag operations use capitalised JSON names.

const timeLayout = "2006-01-02T15:04:05.000Z"

func fmtTime(t time.Time) string { return t.UTC().Format(timeLayout) }

type s3ObjectDTO struct {
	Bucket    string `json:"bucket"`
	Key       string `json:"key"`
	VersionID string `json:"versionId,omitempty"`
}

type codeConfigDTO struct {
	S3Object *s3ObjectDTO `json:"s3Object,omitempty"`
}

type runtimeConfigDTO struct {
	Runtime string `json:"runtime"`
}

type buildConfigDTO struct {
	CodeConfig    *codeConfigDTO    `json:"codeConfig,omitempty"`
	RuntimeConfig *runtimeConfigDTO `json:"runtimeConfig,omitempty"`
}

type loggingConfigDTO struct {
	ApplicationLogLevel string `json:"applicationLogLevel,omitempty"`
	LogGroup            string `json:"logGroup,omitempty"`
	SystemLogLevel      string `json:"systemLogLevel,omitempty"`
}

type telemetryConfigDTO struct {
	LoggingConfig *loggingConfigDTO `json:"loggingConfig,omitempty"`
}

type serviceConfigDTO struct {
	EnvironmentVariables         map[string]string   `json:"environmentVariables,omitempty"`
	TelemetryConfig              *telemetryConfigDTO `json:"telemetryConfig,omitempty"`
	MaxConcurrencyPerEnvironment *int32              `json:"maxConcurrencyPerEnvironment,omitempty"`
	TimeoutSeconds               *int32              `json:"timeoutSeconds,omitempty"`
	ExecutionRoleArn             string              `json:"executionRoleArn"`
	EgressNetworkConnectorArn    string              `json:"egressNetworkConnectorArn,omitempty"`
}

type revisionConfigDTO struct {
	BuildConfig   *buildConfigDTO   `json:"buildConfig,omitempty"`
	ServiceConfig *serviceConfigDTO `json:"serviceConfig,omitempty"`
	Description   string            `json:"description,omitempty"`
	KmsKeyArn     string            `json:"kmsKeyArn,omitempty"`
}

type scalingConfigDTO struct {
	MaxEnvironments *int32 `json:"maxEnvironments,omitempty"`
}

type throttleConfigDTO struct {
	RateLimit *int32 `json:"rateLimit,omitempty"`
}

type revisionWeightDTO struct {
	RevisionID string `json:"revisionId"`
	Weight     int32  `json:"weight"`
}

type endpointConfigDTO struct {
	ScalingConfig      *scalingConfigDTO  `json:"scalingConfig,omitempty"`
	ThrottleConfig     *throttleConfigDTO `json:"throttleConfig,omitempty"`
	EndpointName       string             `json:"endpointName"`
	Description        string             `json:"description,omitempty"`
	EndpointType       string             `json:"endpointType"`
	AuthType           string             `json:"authType"`
	AutoDeploymentMode string             `json:"autoDeploymentMode,omitempty"`
	Regions            []string           `json:"regions,omitempty"`
}

type filterDTO struct {
	Name   string   `json:"name"`
	Values []string `json:"values"`
}

type listRequest struct {
	NextToken  string      `json:"nextToken,omitempty"`
	Filters    []filterDTO `json:"filters,omitempty"`
	MaxResults int         `json:"maxResults,omitempty"`
}

type createFunctionRequest struct {
	EndpointConfig *endpointConfigDTO `json:"endpointConfig,omitempty"`
	RevisionConfig *revisionConfigDTO `json:"revisionConfig,omitempty"`
	Tags           map[string]string  `json:"tags,omitempty"`
	FunctionName   string             `json:"functionName"`
}

type createEndpointRequest struct {
	ScalingConfig      *scalingConfigDTO   `json:"scalingConfig,omitempty"`
	ThrottleConfig     *throttleConfigDTO  `json:"throttleConfig,omitempty"`
	EndpointName       string              `json:"endpointName"`
	Description        string              `json:"description,omitempty"`
	EndpointType       string              `json:"endpointType"`
	AuthType           string              `json:"authType"`
	AutoDeploymentMode string              `json:"autoDeploymentMode,omitempty"`
	Regions            []string            `json:"regions,omitempty"`
	RevisionWeights    []revisionWeightDTO `json:"revisionWeights,omitempty"`
}

type updateEndpointRequest struct {
	ScalingConfig      *scalingConfigDTO    `json:"scalingConfig,omitempty"`
	ThrottleConfig     *throttleConfigDTO   `json:"throttleConfig,omitempty"`
	Description        *string              `json:"description,omitempty"`
	RevisionWeights    *[]revisionWeightDTO `json:"revisionWeights,omitempty"`
	AuthType           string               `json:"authType,omitempty"`
	AutoDeploymentMode string               `json:"autoDeploymentMode,omitempty"`
}

type createRevisionRequest struct {
	BuildConfig   *buildConfigDTO   `json:"buildConfig,omitempty"`
	ServiceConfig *serviceConfigDTO `json:"serviceConfig,omitempty"`
	Description   string            `json:"description,omitempty"`
	KmsKeyArn     string            `json:"kmsKeyArn,omitempty"`
}

type putPolicyRequest struct {
	Policy     string `json:"Policy"`
	RevisionID string `json:"RevisionId,omitempty"`
}

type policyResponse struct {
	Policy     string `json:"Policy"`
	RevisionID string `json:"RevisionId"`
}

type tagRequest struct {
	Tags map[string]string `json:"Tags"`
}

type tagsResponse struct {
	Tags map[string]string `json:"Tags"`
}

type functionSummaryDTO struct {
	CreatedAt    string `json:"createdAt"`
	FunctionArn  string `json:"functionArn"`
	FunctionName string `json:"functionName"`
	State        string `json:"state"`
	StateReason  string `json:"stateReason"`
	UpdatedAt    string `json:"updatedAt"`
}

type revisionSummaryDTO struct {
	CreatedAt   string `json:"createdAt"`
	Description string `json:"description,omitempty"`
	RevisionArn string `json:"revisionArn"`
	RevisionID  string `json:"revisionId"`
	State       string `json:"state"`
	StateReason string `json:"stateReason"`
}

type endpointSummaryDTO struct {
	ScalingConfig      *scalingConfigDTO   `json:"scalingConfig,omitempty"`
	ThrottleConfig     *throttleConfigDTO  `json:"throttleConfig,omitempty"`
	EndpointName       string              `json:"endpointName"`
	EndpointType       string              `json:"endpointType"`
	CreatedAt          string              `json:"createdAt"`
	Description        string              `json:"description,omitempty"`
	DomainName         string              `json:"domainName"`
	EndpointArn        string              `json:"endpointArn"`
	AuthType           string              `json:"authType"`
	AutoDeploymentMode string              `json:"autoDeploymentMode"`
	UpdatedAt          string              `json:"updatedAt"`
	UpdateStatusReason string              `json:"updateStatusReason,omitempty"`
	State              string              `json:"state"`
	StateReason        string              `json:"stateReason"`
	UpdateStatus       string              `json:"updateStatus,omitempty"`
	RevisionWeights    []revisionWeightDTO `json:"revisionWeights"`
	Regions            []string            `json:"regions"`
}

type regionalEndpointDTO struct {
	ScalingConfig      *scalingConfigDTO   `json:"scalingConfig,omitempty"`
	ThrottleConfig     *throttleConfigDTO  `json:"throttleConfig,omitempty"`
	AuthType           string              `json:"authType"`
	DomainName         string              `json:"domainName,omitempty"`
	State              string              `json:"state"`
	StateReason        string              `json:"stateReason"`
	UpdateStatus       string              `json:"updateStatus,omitempty"`
	UpdateStatusReason string              `json:"updateStatusReason,omitempty"`
	RevisionWeights    []revisionWeightDTO `json:"revisionWeights"`
}

type endpointResponse struct {
	ScalingConfig      *scalingConfigDTO              `json:"scalingConfig,omitempty"`
	ThrottleConfig     *throttleConfigDTO             `json:"throttleConfig,omitempty"`
	RegionalEndpoints  map[string]regionalEndpointDTO `json:"regionalEndpoints"`
	EndpointArn        string                         `json:"endpointArn"`
	EndpointType       string                         `json:"endpointType"`
	CreatedAt          string                         `json:"createdAt"`
	Description        string                         `json:"description,omitempty"`
	DomainName         string                         `json:"domainName"`
	AuthType           string                         `json:"authType"`
	EndpointName       string                         `json:"endpointName"`
	AutoDeploymentMode string                         `json:"autoDeploymentMode"`
	FunctionArn        string                         `json:"functionArn"`
	UpdatedAt          string                         `json:"updatedAt"`
	UpdateStatusReason string                         `json:"updateStatusReason,omitempty"`
	State              string                         `json:"state"`
	StateReason        string                         `json:"stateReason"`
	UpdateStatus       string                         `json:"updateStatus,omitempty"`
	RevisionWeights    []revisionWeightDTO            `json:"revisionWeights"`
	Regions            []string                       `json:"regions"`
}

type revisionResponse struct {
	ServiceConfig serviceConfigDTO   `json:"serviceConfig"`
	BuildConfig   buildConfigDTO     `json:"buildConfig"`
	CreatedAt     string             `json:"createdAt"`
	Description   string             `json:"description,omitempty"`
	FunctionArn   string             `json:"functionArn"`
	KmsKeyArn     string             `json:"kmsKeyArn,omitempty"`
	RevisionArn   string             `json:"revisionArn"`
	RevisionID    string             `json:"revisionId"`
	State         string             `json:"state"`
	StateReason   string             `json:"stateReason"`
	Errors        []revisionErrorDTO `json:"errors,omitempty"`
}

type revisionErrorDTO struct {
	Attribute    string `json:"attribute"`
	ErrorCode    string `json:"errorCode"`
	ErrorMessage string `json:"errorMessage"`
}

type createFunctionResponse struct {
	Endpoint     *endpointSummaryDTO `json:"endpoint,omitempty"`
	Revision     *revisionSummaryDTO `json:"revision,omitempty"`
	Tags         map[string]string   `json:"tags,omitempty"`
	CreatedAt    string              `json:"createdAt"`
	FunctionArn  string              `json:"functionArn"`
	FunctionName string              `json:"functionName"`
	State        string              `json:"state"`
	StateReason  string              `json:"stateReason"`
	UpdatedAt    string              `json:"updatedAt"`
}

type listFunctionsResponse struct {
	NextToken string               `json:"nextToken,omitempty"`
	Functions []functionSummaryDTO `json:"functions"`
}

type listRevisionsResponse struct {
	NextToken string               `json:"nextToken,omitempty"`
	Revisions []revisionSummaryDTO `json:"revisions"`
}

type listEndpointsResponse struct {
	NextToken string               `json:"nextToken,omitempty"`
	Endpoints []endpointSummaryDTO `json:"endpoints"`
}

type accountQuotasDTO struct {
	MaxEndpointsPerFunction int32 `json:"maxEndpointsPerFunction"`
	MaxRevisionsPerFunction int32 `json:"maxRevisionsPerFunction"`
	MaxTotalArmVCpus        int32 `json:"maxTotalArmVCpus"`
	MaxTotalRateLimit       int32 `json:"maxTotalRateLimit"`
}

type accountUsageDTO struct {
	FunctionCount int32 `json:"functionCount"`
}

type accountSettingsResponse struct {
	AccountQuotas accountQuotasDTO `json:"accountQuotas"`
	AccountUsage  accountUsageDTO  `json:"accountUsage"`
}

type errorResponse struct {
	Message      string `json:"message"`
	ResourceID   string `json:"resourceId,omitempty"`
	ResourceType string `json:"resourceType,omitempty"`
	Type         string `json:"__type,omitempty"`
}
