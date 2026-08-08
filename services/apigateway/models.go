package apigateway

import (
	"encoding/json"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// Integration type constants.
const (
	IntegrationTypeMock      = "MOCK"
	IntegrationTypeHTTP      = "HTTP"
	IntegrationTypeHTTPProxy = "HTTP_PROXY"
	IntegrationTypeAWS       = "AWS"
	IntegrationTypeAWSProxy  = "AWS_PROXY"
)

// Authorization type constants.
const (
	AuthTypeNone            = "NONE"
	AuthTypeAWSIAM          = "AWS_IAM"
	AuthTypeCustom          = "CUSTOM"
	AuthTypeCognitoUserPool = "COGNITO_USER_POOLS"
)

// unixEpochTime wraps [time.Time] and marshals to/from a JSON number (Unix seconds),
// which is the format expected by the AWS SDK v2 API Gateway client.
type unixEpochTime struct {
	time.Time
}

func (t unixEpochTime) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.Unix())
}

func (t *unixEpochTime) UnmarshalJSON(b []byte) error {
	var epoch int64
	if err := json.Unmarshal(b, &epoch); err != nil {
		return err
	}

	t.Time = time.Unix(epoch, 0)

	return nil
}

// EndpointConfiguration describes the endpoint types for a REST API.
type EndpointConfiguration struct {
	IPAddressType  string   `json:"ipAddressType,omitempty"`
	VpcEndpointIDs []string `json:"vpcEndpointIds,omitempty"`
	Types          []string `json:"types,omitempty"`
}

// MutualTLSAuthentication configures mutual TLS authentication for a custom
// domain name (aws-sdk-go-v2/service/apigateway/types.MutualTlsAuthentication).
type MutualTLSAuthentication struct {
	TruststoreURI      string   `json:"truststoreUri,omitempty"`
	TruststoreVersion  string   `json:"truststoreVersion,omitempty"`
	TruststoreWarnings []string `json:"truststoreWarnings,omitempty"`
}

// RestAPI represents an API Gateway REST API.
type RestAPI struct {
	CreatedDate               unixEpochTime          `json:"createdDate"`
	EndpointConfiguration     *EndpointConfiguration `json:"endpointConfiguration,omitempty"`
	Tags                      *tags.Tags             `json:"tags,omitempty"`
	Policy                    string                 `json:"policy,omitempty"`
	Name                      string                 `json:"name"`
	Description               string                 `json:"description,omitempty"`
	ID                        string                 `json:"id"`
	APIKeySource              string                 `json:"apiKeySource,omitempty"`
	RootResourceID            string                 `json:"rootResourceId,omitempty"`
	APIStatus                 string                 `json:"apiStatus,omitempty"`
	APIStatusMessage          string                 `json:"apiStatusMessage,omitempty"`
	EndpointAccessMode        string                 `json:"endpointAccessMode,omitempty"`
	BinaryMediaTypes          []string               `json:"binaryMediaTypes,omitempty"`
	MinimumCompressionSize    int                    `json:"minimumCompressionSize,omitempty"`
	DisableExecuteAPIEndpoint bool                   `json:"disableExecuteApiEndpoint,omitempty"`
}

// CorsConfiguration holds CORS settings for a resource.
type CorsConfiguration struct {
	AllowHeaders  []string `json:"allowHeaders,omitempty"`
	AllowMethods  []string `json:"allowMethods,omitempty"`
	AllowOrigins  []string `json:"allowOrigins,omitempty"`
	ExposeHeaders []string `json:"exposeHeaders,omitempty"`
	MaxAge        int      `json:"maxAge,omitempty"`
}

// Resource represents an API Gateway resource.
type Resource struct {
	ResourceMethods   map[string]*Method `json:"resourceMethods,omitempty"`
	CorsConfiguration *CorsConfiguration `json:"corsConfiguration,omitempty"`
	ID                string             `json:"id"`
	ParentID          string             `json:"parentId,omitempty"`
	PathPart          string             `json:"pathPart,omitempty"`
	Path              string             `json:"path"`
	RestAPIID         string             `json:"-"`
}

// Method represents an API Gateway method on a resource.
type Method struct {
	RequestParameters  map[string]bool            `json:"requestParameters,omitempty"`
	RequestModels      map[string]string          `json:"requestModels,omitempty"`
	MethodIntegration  *Integration               `json:"methodIntegration,omitempty"`
	MethodResponses    map[string]*MethodResponse `json:"methodResponses,omitempty"`
	HTTPMethod         string                     `json:"httpMethod"`
	AuthorizationType  string                     `json:"authorizationType"`
	AuthorizerID       string                     `json:"authorizerId,omitempty"`
	RequestValidatorID string                     `json:"requestValidatorId,omitempty"`
	OperationName      string                     `json:"operationName,omitempty"`
	APIKeyRequired     bool                       `json:"apiKeyRequired"`
}

// Integration represents a method integration.
type Integration struct {
	RequestTemplates     map[string]string               `json:"requestTemplates,omitempty"`
	RequestParameters    map[string]string               `json:"requestParameters,omitempty"`
	IntegrationResponses map[string]*IntegrationResponse `json:"integrationResponses,omitempty"`
	ConnectionID         string                          `json:"connectionId,omitempty"`
	Type                 string                          `json:"type"`
	HTTPMethod           string                          `json:"httpMethod,omitempty"`
	URI                  string                          `json:"uri,omitempty"`
	PassthroughBehavior  string                          `json:"passthroughBehavior,omitempty"`
	ConnectionType       string                          `json:"connectionType,omitempty"`
	ContentHandling      string                          `json:"contentHandling,omitempty"`
	Credentials          string                          `json:"credentials,omitempty"`
	CacheNamespace       string                          `json:"cacheNamespace,omitempty"`
	CacheKeyParameters   []string                        `json:"cacheKeyParameters,omitempty"`
	TimeoutInMillis      int                             `json:"timeoutInMillis,omitempty"`
}

// IntegrationResponse represents a response from an integration.
type IntegrationResponse struct {
	ResponseTemplates  map[string]string `json:"responseTemplates,omitempty"`
	ResponseParameters map[string]string `json:"responseParameters,omitempty"`
	StatusCode         string            `json:"statusCode"`
	SelectionPattern   string            `json:"selectionPattern,omitempty"`
	ContentHandling    string            `json:"contentHandling,omitempty"`
}

// MethodResponse represents a method response configuration.
type MethodResponse struct {
	ResponseModels     map[string]string `json:"responseModels,omitempty"`
	ResponseParameters map[string]bool   `json:"responseParameters,omitempty"`
	StatusCode         string            `json:"statusCode"`
}

// CanarySettings holds canary deployment configuration for a stage.
type CanarySettings struct {
	StageVariableOverrides map[string]string `json:"stageVariableOverrides,omitempty"`
	DeploymentID           string            `json:"deploymentId,omitempty"`
	PercentTraffic         float64           `json:"percentTraffic,omitempty"`
	UseStageCache          bool              `json:"useStageCache,omitempty"`
}

// AccessLogSettings configures CloudWatch access logging for a stage.
type AccessLogSettings struct {
	DestinationARN string `json:"destinationArn,omitempty"`
	Format         string `json:"format,omitempty"`
}

// MethodSetting holds per-method CloudWatch logging and throttling settings.
type MethodSetting struct {
	LoggingLevel                           string  `json:"loggingLevel,omitempty"`
	UnauthorizedCacheControlHeaderStrategy string  `json:"unauthorizedCacheControlHeaderStrategy,omitempty"`
	ThrottlingRateLimit                    float64 `json:"throttlingRateLimit,omitempty"`
	ThrottlingBurstLimit                   int     `json:"throttlingBurstLimit,omitempty"`
	CacheTTLInSeconds                      int     `json:"cacheTtlInSeconds,omitempty"`
	DataTraceEnabled                       bool    `json:"dataTraceEnabled,omitempty"`
	MetricsEnabled                         bool    `json:"metricsEnabled,omitempty"`
	CachingEnabled                         bool    `json:"cachingEnabled,omitempty"`
	CacheDataEncrypted                     bool    `json:"cacheDataEncrypted,omitempty"`
	RequireAuthorizationForCacheControl    bool    `json:"requireAuthorizationForCacheControl,omitempty"`
}

// Stage represents a deployment stage.
type Stage struct {
	CanarySettings      *CanarySettings          `json:"canarySettings,omitempty"`
	AccessLogSettings   *AccessLogSettings       `json:"accessLogSettings,omitempty"`
	MethodSettings      map[string]MethodSetting `json:"methodSettings,omitempty"`
	Variables           map[string]string        `json:"variables,omitempty"`
	CreatedDate         unixEpochTime            `json:"createdDate"`
	LastUpdatedDate     unixEpochTime            `json:"lastUpdatedDate"`
	StageName           string                   `json:"stageName"`
	RestAPIID           string                   `json:"-"`
	DeploymentID        string                   `json:"deploymentId"`
	Description         string                   `json:"description,omitempty"`
	ClientCertificateID string                   `json:"clientCertificateId,omitempty"`
	// CacheClusterSize is the cache cluster's capacity in GB (e.g. "0.5"), only
	// meaningful when CacheClusterEnabled is true.
	CacheClusterSize string `json:"cacheClusterSize,omitempty"`
	// CacheClusterStatus mirrors AWS's CacheClusterStatus enum
	// (AVAILABLE/NOT_AVAILABLE/...), derived from CacheClusterEnabled.
	CacheClusterStatus string `json:"cacheClusterStatus,omitempty"`
	// InvokeURL is the invoke URL for this stage (non-AWS field used by gopherstack UI).
	InvokeURL string `json:"invokeUrl,omitempty"`
	// DocumentationVersion associates this stage with a snapshot of API
	// documentation (types.Stage.DocumentationVersion in the SDK).
	DocumentationVersion string `json:"documentationVersion,omitempty"`
	TracingEnabled       bool   `json:"tracingEnabled,omitempty"`
	CacheClusterEnabled  bool   `json:"cacheClusterEnabled,omitempty"`
}

// Deployment represents a REST API deployment.
type Deployment struct {
	CreatedDate unixEpochTime `json:"createdDate"`
	ID          string        `json:"id"`
	RestAPIID   string        `json:"-"`
	Description string        `json:"description,omitempty"`
}

// PutMethodInput is the input for PutMethod.
type PutMethodInput struct {
	RequestParameters  map[string]bool   `json:"requestParameters,omitempty"`
	RequestModels      map[string]string `json:"requestModels,omitempty"`
	RestAPIID          string            `json:"restApiId"`
	ResourceID         string            `json:"resourceId"`
	HTTPMethod         string            `json:"httpMethod"`
	AuthorizationType  string            `json:"authorizationType"`
	AuthorizerID       string            `json:"authorizerId,omitempty"`
	RequestValidatorID string            `json:"requestValidatorId,omitempty"`
	OperationName      string            `json:"operationName,omitempty"`
	APIKeyRequired     bool              `json:"apiKeyRequired"`
}

// PutIntegrationInput is the input for PutIntegration.
type PutIntegrationInput struct {
	RequestTemplates    map[string]string `json:"requestTemplates,omitempty"`
	RequestParameters   map[string]string `json:"requestParameters,omitempty"`
	PassthroughBehavior string            `json:"passthroughBehavior,omitempty"`
	Type                string            `json:"type"`
	HTTPMethod          string            `json:"httpMethod,omitempty"`
	URI                 string            `json:"uri,omitempty"`
	ConnectionType      string            `json:"connectionType,omitempty"`
	ConnectionID        string            `json:"connectionId,omitempty"`
	ContentHandling     string            `json:"contentHandling,omitempty"`
	Credentials         string            `json:"credentials,omitempty"`
	CacheNamespace      string            `json:"cacheNamespace,omitempty"`
	CacheKeyParameters  []string          `json:"cacheKeyParameters,omitempty"`
	TimeoutInMillis     int               `json:"timeoutInMillis,omitempty"`
}

// PutMethodResponseInput is the input for PutMethodResponse.
type PutMethodResponseInput struct {
	ResponseModels     map[string]string `json:"responseModels,omitempty"`
	ResponseParameters map[string]bool   `json:"responseParameters,omitempty"`
}

// PutIntegrationResponseInput is the input for PutIntegrationResponse.
type PutIntegrationResponseInput struct {
	ResponseTemplates  map[string]string `json:"responseTemplates,omitempty"`
	ResponseParameters map[string]string `json:"responseParameters,omitempty"`
	SelectionPattern   string            `json:"selectionPattern,omitempty"`
	ContentHandling    string            `json:"contentHandling,omitempty"`
}

// Authorizer represents an API Gateway authorizer.
type Authorizer struct {
	ID                           string `json:"id"`
	Name                         string `json:"name"`
	Type                         string `json:"type"`
	AuthorizerURI                string `json:"authorizerUri,omitempty"`
	AuthorizerCredentials        string `json:"authorizerCredentials,omitempty"`
	IdentitySource               string `json:"identitySource,omitempty"`
	IdentityValidationExpression string `json:"identityValidationExpression,omitempty"`
	// RestAPIID identifies the owning REST API. It is internal storage-layer
	// identity (composite key for the backend's flat store.Table[Authorizer]),
	// never part of the wire response, matching the same json:"-" convention
	// already used by Resource/Stage/Deployment/Model for the identical purpose.
	RestAPIID                    string   `json:"-"`
	ProviderARNs                 []string `json:"providerARNs,omitempty"`
	AuthorizerResultTTLInSeconds int      `json:"authorizerResultTtlInSeconds,omitempty"`
}

// CreateAuthorizerInput is the input for CreateAuthorizer.
type CreateAuthorizerInput struct {
	Name                         string   `json:"name"`
	Type                         string   `json:"type"`
	AuthorizerURI                string   `json:"authorizerUri,omitempty"`
	AuthorizerCredentials        string   `json:"authorizerCredentials,omitempty"`
	IdentitySource               string   `json:"identitySource,omitempty"`
	IdentityValidationExpression string   `json:"identityValidationExpression,omitempty"`
	ProviderARNs                 []string `json:"providerARNs,omitempty"`
	AuthorizerResultTTLInSeconds int      `json:"authorizerResultTtlInSeconds,omitempty"`
}

// UpdateAuthorizerInput is the input for UpdateAuthorizer. IdentitySource is
// a *string (see updateAuthorizerInput's doc comment in handler_authorizers.go)
// so an explicit PATCH "remove" of "/identitySource" (AWS-documented as
// supported) can be told apart from the field being absent from the PATCH.
type UpdateAuthorizerInput struct {
	IdentitySource               *string  `json:"identitySource,omitempty"`
	Name                         string   `json:"name,omitempty"`
	Type                         string   `json:"type,omitempty"`
	AuthorizerURI                string   `json:"authorizerUri,omitempty"`
	AuthorizerCredentials        string   `json:"authorizerCredentials,omitempty"`
	IdentityValidationExpression string   `json:"identityValidationExpression,omitempty"`
	ProviderARNs                 []string `json:"providerARNs,omitempty"`
	AuthorizerResultTTLInSeconds int      `json:"authorizerResultTtlInSeconds,omitempty"`
}

// RequestValidator represents an API Gateway request validator.
type RequestValidator struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	// RestAPIID identifies the owning REST API. Internal storage-layer identity
	// only (composite key for the backend's flat store.Table[RequestValidator]);
	// never part of the wire response — see the identical Authorizer.RestAPIID doc.
	RestAPIID                 string `json:"-"`
	ValidateRequestBody       bool   `json:"validateRequestBody"`
	ValidateRequestParameters bool   `json:"validateRequestParameters"`
}

// CreateRequestValidatorInput is the input for CreateRequestValidator.
type CreateRequestValidatorInput struct {
	Name                      string `json:"name"`
	ValidateRequestBody       bool   `json:"validateRequestBody"`
	ValidateRequestParameters bool   `json:"validateRequestParameters"`
}

// UpdateRequestValidatorInput is the input for UpdateRequestValidator.
type UpdateRequestValidatorInput struct {
	ValidateRequestBody       *bool  `json:"validateRequestBody,omitempty"`
	ValidateRequestParameters *bool  `json:"validateRequestParameters,omitempty"`
	Name                      string `json:"name,omitempty"`
}

// AuthorizerResponse is the response returned by a Lambda authorizer function.
type AuthorizerResponse struct {
	Context        map[string]any  `json:"context,omitempty"`
	PolicyDocument *PolicyDocument `json:"policyDocument,omitempty"`
	PrincipalID    string          `json:"principalId"`
}

// PolicyDocument is an IAM policy document as returned by Lambda authorizers.
type PolicyDocument struct {
	Version   string            `json:"Version,omitempty"`
	Statement []PolicyStatement `json:"Statement"`
}

// PolicyStatement is a single statement within an IAM policy document.
type PolicyStatement struct {
	Action   any    `json:"Action"`
	Resource any    `json:"Resource"`
	Effect   string `json:"Effect"`
}

// ErrorResponse is the JSON error format for API Gateway clients.
type ErrorResponse struct {
	Type    string `json:"__type"`
	Message string `json:"message"`
}

// APIKey represents an API Gateway API key.
type APIKey struct {
	CreatedDate     unixEpochTime `json:"createdDate"`
	LastUpdatedDate unixEpochTime `json:"lastUpdatedDate"`
	Tags            *tags.Tags    `json:"tags,omitempty"`
	ID              string        `json:"id"`
	Name            string        `json:"name"`
	Description     string        `json:"description,omitempty"`
	Value           string        `json:"value,omitempty"`
	// CustomerID is an AWS Marketplace customer identifier, when integrating
	// with the AWS SaaS Marketplace (types.ApiKey.CustomerId in the SDK).
	CustomerID string `json:"customerId,omitempty"`
	// StageKeys lists the "{restApiId}/{stageName}" stage associations for
	// this API key (types.ApiKey.StageKeys / types.CreateApiKeyOutput.StageKeys
	// in the SDK -- deprecated in favor of usage plans, but still a real,
	// settable field: CreateApiKeyInput.StageKeys and UpdateApiKey's
	// "/stages" add/remove PATCH path both still work against it).
	StageKeys []string `json:"stageKeys,omitempty"`
	Enabled   bool     `json:"enabled"`
}

// StageKeyInput identifies a REST API stage to associate with an API key at
// creation time (types.StageKey in the SDK).
type StageKeyInput struct {
	RestAPIID string `json:"restApiId,omitempty"`
	StageName string `json:"stageName,omitempty"`
}

// CreateAPIKeyInput is the input for CreateAPIKey.
type CreateAPIKeyInput struct {
	Tags        *tags.Tags      `json:"tags,omitempty"`
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Value       string          `json:"value,omitempty"`
	CustomerID  string          `json:"customerId,omitempty"`
	StageKeys   []StageKeyInput `json:"stageKeys,omitempty"`
	Enabled     bool            `json:"enabled"`
}

// BasePathMapping maps a base path on a custom domain to an API stage.
type BasePathMapping struct {
	DomainName string `json:"domainName,omitempty"`
	BasePath   string `json:"basePath"`
	RestAPIID  string `json:"restApiId"`
	Stage      string `json:"stage,omitempty"`
}

// CreateBasePathMappingInput is the input for CreateBasePathMapping.
type CreateBasePathMappingInput struct {
	DomainName string `json:"domainName"`
	BasePath   string `json:"basePath"`
	RestAPIID  string `json:"restApiId"`
	Stage      string `json:"stage,omitempty"`
}

// DocumentationLocation specifies where a documentation part applies.
type DocumentationLocation struct {
	Type       string `json:"type"`
	Path       string `json:"path,omitempty"`
	Method     string `json:"method,omitempty"`
	StatusCode string `json:"statusCode,omitempty"`
	Name       string `json:"name,omitempty"`
}

// DocumentationPart represents a piece of API documentation.
type DocumentationPart struct {
	Location   DocumentationLocation `json:"location"`
	ID         string                `json:"id"`
	RestAPIID  string                `json:"-"`
	Properties string                `json:"properties"`
}

// CreateDocumentationPartInput is the input for CreateDocumentationPart.
type CreateDocumentationPartInput struct {
	Location   DocumentationLocation `json:"location"`
	RestAPIID  string                `json:"restApiId"`
	Properties string                `json:"properties"`
}

// DocumentationVersion represents a versioned snapshot of API documentation.
type DocumentationVersion struct {
	CreatedDate unixEpochTime `json:"createdDate"`
	RestAPIID   string        `json:"-"`
	Version     string        `json:"version"`
	Description string        `json:"description,omitempty"`
}

// CreateDocumentationVersionInput is the input for CreateDocumentationVersion.
type CreateDocumentationVersionInput struct {
	RestAPIID   string `json:"restApiId"`
	Version     string `json:"documentationVersion"`
	Description string `json:"description,omitempty"`
}

// DomainName represents a custom domain name for an API.
type DomainName struct {
	CreatedDate              *unixEpochTime           `json:"createdDate,omitempty"`
	Tags                     *tags.Tags               `json:"tags,omitempty"`
	EndpointConfiguration    *EndpointConfiguration   `json:"endpointConfiguration,omitempty"`
	MutualTLSAuthentication  *MutualTLSAuthentication `json:"mutualTlsAuthentication,omitempty"`
	DomainNameValue          string                   `json:"domainName"`
	CertificateARN           string                   `json:"certificateArn,omitempty"`
	RegionalCertificateARN   string                   `json:"regionalCertificateArn,omitempty"`
	DistributionDomainName   string                   `json:"distributionDomainName,omitempty"`
	DistributionHostedZoneID string                   `json:"distributionHostedZoneId,omitempty"`
	RegionalDomainName       string                   `json:"regionalDomainName,omitempty"`
	RegionalHostedZoneID     string                   `json:"regionalHostedZoneId,omitempty"`
	SecurityPolicy           string                   `json:"securityPolicy,omitempty"`
	DomainNameStatus         string                   `json:"domainNameStatus,omitempty"`
}

// CreateDomainNameInput is the input for CreateDomainName.
type CreateDomainNameInput struct {
	Tags                   *tags.Tags             `json:"tags,omitempty"`
	EndpointConfiguration  *EndpointConfiguration `json:"endpointConfiguration,omitempty"`
	DomainName             string                 `json:"domainName"`
	CertificateARN         string                 `json:"certificateArn,omitempty"`
	RegionalCertificateARN string                 `json:"regionalCertificateArn,omitempty"`
	SecurityPolicy         string                 `json:"securityPolicy,omitempty"`
}

// DomainNameAccessAssociation links a domain name to an access source such as a VPC endpoint.
type DomainNameAccessAssociation struct {
	DomainNameAccessAssociationARN string `json:"domainNameAccessAssociationArn,omitempty"`
	DomainNameARN                  string `json:"domainNameArn"`
	AccessAssociationSource        string `json:"accessAssociationSource"`
	AccessAssociationSourceType    string `json:"accessAssociationSourceType"`
}

// CreateDomainNameAccessAssociationInput is the input for CreateDomainNameAccessAssociation.
type CreateDomainNameAccessAssociationInput struct {
	DomainNameARN               string `json:"domainNameArn"`
	AccessAssociationSource     string `json:"accessAssociationSource"`
	AccessAssociationSourceType string `json:"accessAssociationSourceType"`
}

// Model represents a data model for a REST API.
type Model struct {
	ID          string `json:"id"`
	RestAPIID   string `json:"-"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	ContentType string `json:"contentType,omitempty"`
	Schema      string `json:"schema,omitempty"`
}

// CreateModelInput is the input for CreateModel.
type CreateModelInput struct {
	RestAPIID   string `json:"restApiId"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	ContentType string `json:"contentType,omitempty"`
	Schema      string `json:"schema,omitempty"`
}

// CreateStageInput is the input for the standalone CreateStage operation.
type CreateStageInput struct {
	CanarySettings       *CanarySettings          `json:"canarySettings,omitempty"`
	AccessLogSettings    *AccessLogSettings       `json:"accessLogSettings,omitempty"`
	MethodSettings       map[string]MethodSetting `json:"methodSettings,omitempty"`
	Variables            map[string]string        `json:"variables,omitempty"`
	RestAPIID            string                   `json:"restApiId"`
	StageName            string                   `json:"stageName"`
	DeploymentID         string                   `json:"deploymentId"`
	Description          string                   `json:"description,omitempty"`
	ClientCertificateID  string                   `json:"clientCertificateId,omitempty"`
	CacheClusterSize     string                   `json:"cacheClusterSize,omitempty"`
	DocumentationVersion string                   `json:"documentationVersion,omitempty"`
	TracingEnabled       bool                     `json:"tracingEnabled,omitempty"`
	CacheClusterEnabled  bool                     `json:"cacheClusterEnabled,omitempty"`
}

// ThrottleSettings controls request rate limiting for a usage plan.
type ThrottleSettings struct {
	BurstLimit int     `json:"burstLimit,omitempty"`
	RateLimit  float64 `json:"rateLimit,omitempty"`
}

// QuotaSettings controls request quota limiting for a usage plan.
type QuotaSettings struct {
	Period string `json:"period,omitempty"`
	Limit  int    `json:"limit,omitempty"`
	Offset int    `json:"offset,omitempty"`
}

// UsagePlan represents an API Gateway usage plan.
type UsagePlan struct {
	Tags        *tags.Tags            `json:"tags,omitempty"`
	Throttle    *ThrottleSettings     `json:"throttle,omitempty"`
	Quota       *QuotaSettings        `json:"quota,omitempty"`
	ID          string                `json:"id"`
	Name        string                `json:"name"`
	Description string                `json:"description,omitempty"`
	APIStages   []APIStageAssociation `json:"apiStages,omitempty"`
}

// CreateUsagePlanInput is the input for CreateUsagePlan.
type CreateUsagePlanInput struct {
	Tags        *tags.Tags            `json:"tags,omitempty"`
	Throttle    *ThrottleSettings     `json:"throttle,omitempty"`
	Quota       *QuotaSettings        `json:"quota,omitempty"`
	Name        string                `json:"name"`
	Description string                `json:"description,omitempty"`
	APIStages   []APIStageAssociation `json:"apiStages,omitempty"`
}

// UsagePlanKey represents an API key associated with a usage plan.
type UsagePlanKey struct {
	ID    string `json:"id"`
	Type  string `json:"type"`
	Value string `json:"value,omitempty"`
	Name  string `json:"name,omitempty"`
	// UsagePlanID identifies the owning usage plan. Internal storage-layer
	// identity only (composite key for the backend's flat
	// store.Table[UsagePlanKey]); never part of the wire response — see the
	// identical Authorizer.RestAPIID doc.
	UsagePlanID string `json:"-"`
}

// CreateUsagePlanKeyInput is the input for CreateUsagePlanKey.
type CreateUsagePlanKeyInput struct {
	UsagePlanID string `json:"usagePlanId"`
	KeyID       string `json:"keyId"`
	KeyType     string `json:"keyType"`
}

// UpdateAPIKeyInput is the input for UpdateAPIKey.
type UpdateAPIKeyInput struct {
	Enabled     *bool  `json:"enabled,omitempty"`
	Name        string `json:"name,omitempty"`
	Description string `json:"description,omitempty"`
	CustomerID  string `json:"customerId,omitempty"`
	// StageKeys is the flattened result of PATCH "/stages" add/remove ops
	// (see patch.go's applyAPIKeyPatchOp), each formatted "{restApiId}/{stageName}".
	StageKeys []string `json:"stageKeys,omitempty"`
}

// UpdateModelInput is the input for UpdateModel.
type UpdateModelInput struct {
	Description string `json:"description,omitempty"`
	Schema      string `json:"schema,omitempty"`
}

// UpdateStageInput is the input for UpdateStage.
type UpdateStageInput struct {
	CanarySettings       *CanarySettings          `json:"canarySettings,omitempty"`
	AccessLogSettings    *AccessLogSettings       `json:"accessLogSettings,omitempty"`
	TracingEnabled       *bool                    `json:"tracingEnabled,omitempty"`
	CacheClusterEnabled  *bool                    `json:"cacheClusterEnabled,omitempty"`
	MethodSettings       map[string]MethodSetting `json:"methodSettings,omitempty"`
	Variables            map[string]string        `json:"variables,omitempty"`
	DeploymentID         string                   `json:"deploymentId,omitempty"`
	Description          string                   `json:"description,omitempty"`
	ClientCertificateID  string                   `json:"clientCertificateId,omitempty"`
	CacheClusterSize     string                   `json:"cacheClusterSize,omitempty"`
	DocumentationVersion string                   `json:"documentationVersion,omitempty"`
}

// Account represents the API Gateway account settings.
type Account struct {
	ThrottleSettings  *ThrottleSettings `json:"throttleSettings,omitempty"`
	APIKeyVersion     string            `json:"apiKeyVersion,omitempty"`
	CloudwatchRoleARN string            `json:"cloudwatchRoleArn,omitempty"`
	Features          []string          `json:"features,omitempty"`
}

// CreateRestAPIInput is the input for CreateRestApi.
type CreateRestAPIInput struct {
	EndpointConfiguration     *EndpointConfiguration `json:"endpointConfiguration,omitempty"`
	Tags                      *tags.Tags             `json:"tags,omitempty"`
	Name                      string                 `json:"name"`
	Description               string                 `json:"description,omitempty"`
	Policy                    string                 `json:"policy,omitempty"`
	APIKeySource              string                 `json:"apiKeySource,omitempty"`
	EndpointAccessMode        string                 `json:"endpointAccessMode,omitempty"`
	BinaryMediaTypes          []string               `json:"binaryMediaTypes,omitempty"`
	MinimumCompressionSize    int                    `json:"minimumCompressionSize,omitempty"`
	DisableExecuteAPIEndpoint bool                   `json:"disableExecuteApiEndpoint,omitempty"`
}

// UpdateRestAPIInput is the input for UpdateRestApi. Description is a
// *string (rather than the plain string every other UpdateRestAPIInput field
// uses) because UpdateRestApi's PATCH is the only Update* op in this service
// where a bare top-level scalar's "remove" op is both AWS-documented as
// supported (patch-operations.html: UpdateRestApi "/description" row) and
// implemented here (see patch.go's removableTopLevelScalar) — a plain string
// can't distinguish "explicitly cleared to empty" from "not provided in this
// PATCH at all", which a zero-value merge check would otherwise silently drop.
type UpdateRestAPIInput struct {
	EndpointConfiguration     *EndpointConfiguration `json:"endpointConfiguration,omitempty"`
	MinimumCompressionSize    *int                   `json:"minimumCompressionSize,omitempty"`
	Description               *string                `json:"description,omitempty"`
	DisableExecuteAPIEndpoint *bool                  `json:"disableExecuteApiEndpoint,omitempty"`
	Name                      string                 `json:"name,omitempty"`
	Policy                    string                 `json:"policy,omitempty"`
	APIKeySource              string                 `json:"apiKeySource,omitempty"`
	EndpointAccessMode        string                 `json:"endpointAccessMode,omitempty"`
	BinaryMediaTypes          []string               `json:"binaryMediaTypes,omitempty"`
}

// UpdateDeploymentInput is the input for UpdateDeployment.
type UpdateDeploymentInput struct {
	Description string `json:"description,omitempty"`
}

// UpdateResourceInput is the input for UpdateResource (rename pathPart or set CORS).
type UpdateResourceInput struct {
	CorsConfiguration *CorsConfiguration `json:"corsConfiguration,omitempty"`
	PathPart          string             `json:"pathPart,omitempty"`
}

// TestInvokeMethodInput is the input for TestInvokeMethod.
type TestInvokeMethodInput struct {
	Headers             map[string]string `json:"headers,omitempty"`
	StageVariables      map[string]string `json:"stageVariables,omitempty"`
	PathWithQueryString string            `json:"pathWithQueryString,omitempty"`
	Body                string            `json:"body,omitempty"`
	RestAPIID           string            `json:"restApiId"`
	ResourceID          string            `json:"resourceId"`
	HTTPMethod          string            `json:"httpMethod"`
}

// TestInvokeMethodOutput is the output from TestInvokeMethod.
type TestInvokeMethodOutput struct {
	Headers map[string]string `json:"headers,omitempty"`
	Log     string            `json:"log,omitempty"`
	Body    string            `json:"body,omitempty"`
	Status  int               `json:"status"`
	Latency int64             `json:"latency"`
}

// UpdateUsagePlanInput is the input for UpdateUsagePlan.
type UpdateUsagePlanInput struct {
	Throttle    *ThrottleSettings     `json:"throttle,omitempty"`
	Quota       *QuotaSettings        `json:"quota,omitempty"`
	Name        string                `json:"name,omitempty"`
	Description string                `json:"description,omitempty"`
	UsagePlanID string                `json:"usagePlanId"`
	APIStages   []APIStageAssociation `json:"apiStages,omitempty"`
}

// APIStageAssociation associates a usage plan with a specific REST API stage.
type APIStageAssociation struct {
	Throttle  map[string]*ThrottleSettings `json:"throttle,omitempty"`
	RestAPIID string                       `json:"restApiId,omitempty"`
	Stage     string                       `json:"stage,omitempty"`
}

// UpdateDomainNameInput is the input for UpdateDomainName. CertificateARN and
// RegionalCertificateARN are *string (rather than plain string) because
// UpdateDomainName's PATCH documents "remove" as supported for both
// "/certificateArn" and "/regionalCertificateArn" (patch-operations.html) —
// a plain string can't distinguish "explicitly cleared" from "not provided
// in this PATCH at all".
type UpdateDomainNameInput struct {
	EndpointConfiguration   *EndpointConfiguration   `json:"endpointConfiguration,omitempty"`
	MutualTLSAuthentication *MutualTLSAuthentication `json:"mutualTlsAuthentication,omitempty"`
	CertificateARN          *string                  `json:"certificateArn,omitempty"`
	RegionalCertificateARN  *string                  `json:"regionalCertificateArn,omitempty"`
	SecurityPolicy          string                   `json:"securityPolicy,omitempty"`
	DomainName              string                   `json:"domainName"`
}

// UpdateBasePathMappingInput is the input for UpdateBasePathMapping.
type UpdateBasePathMappingInput struct {
	DomainName string `json:"domainName"`
	BasePath   string `json:"basePath"`
	RestAPIID  string `json:"restApiId,omitempty"`
	Stage      string `json:"stage,omitempty"`
}

// UpdateDocumentationPartInput is the input for UpdateDocumentationPart.
type UpdateDocumentationPartInput struct {
	RestAPIID  string `json:"restApiId"`
	DocPartID  string `json:"docPartId"`
	Properties string `json:"properties,omitempty"`
}

// UpdateDocumentationVersionInput is the input for UpdateDocumentationVersion.
type UpdateDocumentationVersionInput struct {
	RestAPIID            string `json:"restApiId"`
	DocumentationVersion string `json:"documentationVersion"`
	Description          string `json:"description,omitempty"`
}

// UpdateMethodInput is the input for UpdateMethod.
type UpdateMethodInput struct {
	RequestModels     map[string]string `json:"requestModels,omitempty"`
	APIKeyRequired    *bool             `json:"apiKeyRequired,omitempty"`
	RestAPIID         string            `json:"restApiId"`
	ResourceID        string            `json:"resourceId"`
	HTTPMethod        string            `json:"httpMethod"`
	AuthorizationType string            `json:"authorizationType,omitempty"`
	AuthorizerID      string            `json:"authorizerId,omitempty"`
	OperationName     string            `json:"operationName,omitempty"`
}

// UpdateIntegrationInput is the input for UpdateIntegration.
type UpdateIntegrationInput struct {
	RequestTemplates      map[string]string `json:"requestTemplates,omitempty"`
	RequestParameters     map[string]string `json:"requestParameters,omitempty"`
	IntegrationHTTPMethod string            `json:"integrationHttpMethod,omitempty"`
	PassthroughBehavior   string            `json:"passthroughBehavior,omitempty"`
	ResourceID            string            `json:"resourceId"`
	HTTPMethod            string            `json:"httpMethod"`
	URI                   string            `json:"uri,omitempty"`
	IntegrationType       string            `json:"type,omitempty"`
	CacheNamespace        string            `json:"cacheNamespace,omitempty"`
	RestAPIID             string            `json:"restApiId"`
	ConnectionType        string            `json:"connectionType,omitempty"`
	ConnectionID          string            `json:"connectionId,omitempty"`
	ContentHandling       string            `json:"contentHandling,omitempty"`
	Credentials           string            `json:"credentials,omitempty"`
	CacheKeyParameters    []string          `json:"cacheKeyParameters,omitempty"`
	TimeoutInMillis       int               `json:"timeoutInMillis,omitempty"`
}

// UpdateIntegrationResponseInput is the input for UpdateIntegrationResponse.
type UpdateIntegrationResponseInput struct {
	ResponseTemplates  map[string]string `json:"responseTemplates,omitempty"`
	ResponseParameters map[string]string `json:"responseParameters,omitempty"`
	RestAPIID          string            `json:"restApiId"`
	ResourceID         string            `json:"resourceId"`
	HTTPMethod         string            `json:"httpMethod"`
	StatusCode         string            `json:"statusCode"`
	SelectionPattern   string            `json:"selectionPattern,omitempty"`
	ContentHandling    string            `json:"contentHandling,omitempty"`
}

// UpdateMethodResponseInput is the input for UpdateMethodResponse.
type UpdateMethodResponseInput struct {
	ResponseModels     map[string]string `json:"responseModels,omitempty"`
	ResponseParameters map[string]bool   `json:"responseParameters,omitempty"`
	RestAPIID          string            `json:"restApiId"`
	ResourceID         string            `json:"resourceId"`
	HTTPMethod         string            `json:"httpMethod"`
	StatusCode         string            `json:"statusCode"`
}

// UpdateAccountInput is the input for UpdateAccount. The real AWS wire shape
// carries only "patchOperations" (see aws-sdk-go-v2 apigateway
// UpdateAccountInput); handler.go flattens the patch document into these
// named fields (CloudwatchRoleARN via top-level "/cloudwatchRoleArn",
// ThrottleSettings via nested "/throttle/{rateLimit,burstLimit}" — see patch.go).
type UpdateAccountInput struct {
	ThrottleSettings  *ThrottleSettings `json:"throttleSettings,omitempty"`
	CloudwatchRoleARN string            `json:"cloudwatchRoleArn,omitempty"`
}

// TestInvokeAuthorizerInput is the input for TestInvokeAuthorizer.
type TestInvokeAuthorizerInput struct {
	Headers        map[string]string `json:"headers,omitempty"`
	StageVariables map[string]string `json:"stageVariables,omitempty"`
	Body           string            `json:"body,omitempty"`
	RestAPIID      string            `json:"restApiId"`
	AuthorizerID   string            `json:"authorizerId"`
	Identity       string            `json:"identity,omitempty"`
}

// TestInvokeAuthorizerOutput is the output from TestInvokeAuthorizer.
type TestInvokeAuthorizerOutput struct {
	Claims              map[string]string `json:"claims,omitempty"`
	Context             map[string]string `json:"context,omitempty"`
	Log                 string            `json:"log,omitempty"`
	PrincipalID         string            `json:"principalId"`
	PolicyDocument      string            `json:"policy,omitempty"`
	ClientStatus        int               `json:"clientStatus"`
	Latency             int64             `json:"latency"`
	AuthorizationStatus int               `json:"authorization"`
}

// GatewayResponse represents a gateway response configuration.
type GatewayResponse struct {
	StatusCode         string            `json:"statusCode,omitempty"`
	ResponseParameters map[string]string `json:"responseParameters,omitempty"`
	ResponseTemplates  map[string]string `json:"responseTemplates,omitempty"`
	ResponseType       string            `json:"responseType"`
	RestAPIID          string            `json:"restApiId"`
	DefaultResponse    bool              `json:"defaultResponse,omitempty"`
}

// PutGatewayResponseInput is the input for PutGatewayResponse.
type PutGatewayResponseInput struct {
	StatusCode         string            `json:"statusCode,omitempty"`
	ResponseParameters map[string]string `json:"responseParameters,omitempty"`
	ResponseTemplates  map[string]string `json:"responseTemplates,omitempty"`
	RestAPIID          string            `json:"restApiId"`
	ResponseType       string            `json:"responseType"`
}

// ClientCertificate represents an API Gateway client certificate.
type ClientCertificate struct {
	CreatedDate           unixEpochTime `json:"createdDate"`
	ExpirationDate        unixEpochTime `json:"expirationDate"`
	PemEncodedCertificate string        `json:"pemEncodedCertificate"`
	Description           string        `json:"description"`
	ClientCertificateID   string        `json:"clientCertificateId"`
}

// GenerateClientCertificateInput is the input for GenerateClientCertificate.
type GenerateClientCertificateInput struct {
	Tags        map[string]string `json:"tags,omitempty"`
	Description string            `json:"description,omitempty"`
}

// GetUsageInput is the input for GetUsage.
type GetUsageInput struct {
	UsagePlanID string `json:"usagePlanId"`
	StartDate   string `json:"startDate"`
	EndDate     string `json:"endDate"`
	Position    string `json:"position,omitempty"`
	Limit       int    `json:"limit,omitempty"`
}

// UsageData represents the usage data response.
type UsageData struct {
	Items       map[string][]any `json:"items"`
	StartDate   string           `json:"startDate"`
	EndDate     string           `json:"endDate"`
	UsagePlanID string           `json:"usagePlanId"`
	Position    string           `json:"position,omitempty"`
}

// ImportRestAPIInput is the input for ImportRestApi.
type ImportRestAPIInput struct {
	Body           []byte `json:"body,omitempty"`
	FailOnWarnings bool   `json:"failOnWarnings,omitempty"`
}

// VpcLink represents a VPC Link for private integrations.
type VpcLink struct {
	Tags        map[string]string `json:"tags,omitempty"`
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Description string            `json:"description,omitempty"`
	Status      string            `json:"status"`
	TargetARNs  []string          `json:"targetArns,omitempty"`
}

// CreateVpcLinkInput is the input for CreateVpcLink.
type CreateVpcLinkInput struct {
	Tags        map[string]string `json:"tags,omitempty"`
	Name        string            `json:"name"`
	Description string            `json:"description,omitempty"`
	TargetARNs  []string          `json:"targetArns,omitempty"`
}

// UpdateVpcLinkInput is the input for UpdateVpcLink.
type UpdateVpcLinkInput struct {
	VpcLinkID   string `json:"vpcLinkId"`
	Name        string `json:"name,omitempty"`
	Description string `json:"description,omitempty"`
}

// UpdateClientCertificateInput is the input for UpdateClientCertificate.
type UpdateClientCertificateInput struct {
	ClientCertificateID string `json:"clientCertificateId"`
	Description         string `json:"description,omitempty"`
}
