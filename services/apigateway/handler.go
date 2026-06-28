package apigateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	keyPosition       = "position"
	litTrue           = "true"
	headerContentType = "Content-Type"
)

const (
	keyRestAPIID            = "restApiId"
	keyResourceArn          = "resourceArn"
	keyAPIKeyID             = "apiKeyId"
	keyDomainName           = "domainName"
	keyBasePath             = "basePath"
	keyUsagePlanID          = "usagePlanId"
	keyResourceID           = "resourceId"
	keyDeploymentID         = "deploymentId"
	keyStageName            = "stageName"
	keyAuthorizerID         = "authorizerId"
	keyRequestValidatorID   = "requestValidatorId"
	keyModelName            = "modelName"
	keyDocPartID            = "docPartId"
	keyDocumentationVersion = "documentationVersion"
	keyResponseType         = "responseType"
	keyHTTPMethod           = "httpMethod"
	keyStatusCode           = "statusCode"
	keySdkType              = "sdkType"
	// keyItem is the response collection key used by AWS API Gateway list
	// operations. The AWS Go SDK v2 deserializer expects the singular "item"
	// for every list response (it is the wire name in the smithy model).
	keyItem = "item"
)

const (
	opUpdateRequestValidator = "UpdateRequestValidator"
	opUpdateStage            = "UpdateStage"
	opUpdateRestAPI          = "UpdateRestApi"
	opUpdateResource         = "UpdateResource"
	opUpdateUsagePlan        = "UpdateUsagePlan"
)

const (
	opGetDocumentationParts      = "GetDocumentationParts"
	opGetDocumentationVersion    = "GetDocumentationVersion"
	opGetDocumentationVersions   = "GetDocumentationVersions"
	opGetDomainName              = "GetDomainName"
	opGetDomainNames             = "GetDomainNames"
	opGetGatewayResponse         = "GetGatewayResponse"
	opGetGatewayResponses        = "GetGatewayResponses"
	opGetIntegration             = "GetIntegration"
	opGetIntegrationResponse     = "GetIntegrationResponse"
	opGetMethod                  = "GetMethod"
	opGetMethodResponse          = "GetMethodResponse"
	opGetModel                   = "GetModel"
	opGetModelTemplate           = "GetModelTemplate"
	opGetModels                  = "GetModels"
	opGetRequestValidator        = "GetRequestValidator"
	opGetRequestValidators       = "GetRequestValidators"
	opGetResource                = "GetResource"
	opGetResources               = "GetResources"
	opGetRestAPI                 = "GetRestApi"
	opGetRestApis                = "GetRestApis"
	opGetStage                   = "GetStage"
	opGetStages                  = "GetStages"
	opGetTags                    = "GetTags"
	opGetUsage                   = "GetUsage"
	opGetUsagePlan               = "GetUsagePlan"
	opGetUsagePlanKey            = "GetUsagePlanKey"
	opGetUsagePlanKeys           = "GetUsagePlanKeys"
	opGetUsagePlans              = "GetUsagePlans"
	opPutGatewayResponse         = "PutGatewayResponse"
	opPutIntegration             = "PutIntegration"
	opPutIntegrationResponse     = "PutIntegrationResponse"
	opPutMethod                  = "PutMethod"
	opPutMethodResponse          = "PutMethodResponse"
	opTagResource                = "TagResource"
	opTestInvokeAuthorizer       = "TestInvokeAuthorizer"
	opTestInvokeMethod           = "TestInvokeMethod"
	opUntagResource              = "UntagResource"
	opUpdateAccount              = "UpdateAccount"
	opUpdateAPIKey               = "UpdateApiKey"
	opUpdateAuthorizer           = "UpdateAuthorizer"
	opUpdateBasePathMapping      = "UpdateBasePathMapping"
	opUpdateDeployment           = "UpdateDeployment"
	opUpdateDocumentationPart    = "UpdateDocumentationPart"
	opUpdateDocumentationVersion = "UpdateDocumentationVersion"
	opUpdateDomainName           = "UpdateDomainName"
	opUpdateIntegration          = "UpdateIntegration"
	opUpdateIntegrationResponse  = "UpdateIntegrationResponse"
	opUpdateMethod               = "UpdateMethod"
	opUpdateMethodResponse       = "UpdateMethodResponse"
	opUpdateModel                = "UpdateModel"

	opCreateAPIKey                      = "CreateApiKey"
	opCreateAuthorizer                  = "CreateAuthorizer"
	opCreateBasePathMapping             = "CreateBasePathMapping"
	opCreateDeployment                  = "CreateDeployment"
	opCreateDocumentationPart           = "CreateDocumentationPart"
	opCreateDocumentationVersion        = "CreateDocumentationVersion"
	opCreateDomainName                  = "CreateDomainName"
	opCreateDomainNameAccessAssociation = "CreateDomainNameAccessAssociation"
	opCreateModel                       = "CreateModel"
	opCreateRequestValidator            = "CreateRequestValidator"
	opCreateResource                    = "CreateResource"
	opCreateRestAPI                     = "CreateRestApi"
	opCreateStage                       = "CreateStage"
	opCreateUsagePlan                   = "CreateUsagePlan"
	opCreateUsagePlanKey                = "CreateUsagePlanKey"
	opDeleteAPIKey                      = "DeleteApiKey"
	opDeleteAuthorizer                  = "DeleteAuthorizer"
	opDeleteBasePathMapping             = "DeleteBasePathMapping"
	opDeleteClientCertificate           = "DeleteClientCertificate"
	opDeleteDeployment                  = "DeleteDeployment"
	opDeleteDocumentationPart           = "DeleteDocumentationPart"
	opDeleteDocumentationVersion        = "DeleteDocumentationVersion"
	opDeleteDomainName                  = "DeleteDomainName"
	opDeleteGatewayResponse             = "DeleteGatewayResponse"
	opDeleteIntegration                 = "DeleteIntegration"
	opDeleteIntegrationResponse         = "DeleteIntegrationResponse"
	opDeleteMethod                      = "DeleteMethod"
	opDeleteMethodResponse              = "DeleteMethodResponse"
	opDeleteModel                       = "DeleteModel"
	opDeleteRequestValidator            = "DeleteRequestValidator"
	opDeleteResource                    = "DeleteResource"
	opDeleteRestAPI                     = "DeleteRestApi"
	opDeleteStage                       = "DeleteStage"
	opDeleteUsagePlan                   = "DeleteUsagePlan"
	opDeleteUsagePlanKey                = "DeleteUsagePlanKey"
	opFlushStageAuthorizersCache        = "FlushStageAuthorizersCache"
	opFlushStageCache                   = "FlushStageCache"
	opGenerateClientCertificate         = "GenerateClientCertificate"
	opGetAccount                        = "GetAccount"
	opGetAPIKey                         = "GetApiKey"
	opGetAPIKeys                        = "GetApiKeys"
	opGetAuthorizer                     = "GetAuthorizer"
	opGetAuthorizers                    = "GetAuthorizers"
	opGetBasePathMapping                = "GetBasePathMapping"
	opGetBasePathMappings               = "GetBasePathMappings"
	opGetClientCertificate              = "GetClientCertificate"
	opGetClientCertificates             = "GetClientCertificates"
	opGetDeployment                     = "GetDeployment"
	opGetDeployments                    = "GetDeployments"
	opGetDocumentationPart              = "GetDocumentationPart"
)

var errUnknownOperation = errors.New("UnknownOperationException")

// path depth constants for URL segment counting.
const (
	pathDepth1 = 1
	pathDepth2 = 2
	pathDepth3 = 3
	pathDepth4 = 4
	pathDepth5 = 5
)

// path segment constants used in REST route matching.
const (
	apiGWUnknownOp                       = "Unknown"
	apiGWSegResources                    = "resources"
	apiGWSegDeployment                   = "deployments"
	apiGWSegStages                       = "stages"
	apiGWSegMethods                      = "methods"
	apiGWSegInteg                        = "integration"
	apiGWSegResponses                    = "responses"
	apiGWSegAuthorizers                  = "authorizers"
	apiGWSegValidators                   = "requestvalidators"
	apiGWSegAPIKeys                      = "apikeys"
	apiGWSegDomainNames                  = "domainnames"
	apiGWSegBasePathMappings             = "basepathmappings"
	apiGWSegAccessAssociations           = "accessassociations"
	apiGWSegDocumentation                = "documentation"
	apiGWSegDocParts                     = "parts"
	apiGWSegDocVersions                  = "versions"
	apiGWSegModels                       = "models"
	apiGWSegUsagePlans                   = "usageplans"
	apiGWSegUsagePlanKeys                = "keys"
	apiGWSegGatewayResponses             = "gatewayresponses"
	apiGWSegClientCerts                  = "clientcertificates"
	apiGWSegSdkTypes                     = "sdktypes"
	apiGWSegSdks                         = "sdks"
	apiGWSegDomainNameAccessAssociations = "domainnameaccessassociations"

	// apiGWMinTagPathSegs is the minimum number of path segments for a /tags/{arn} path.
	apiGWMinTagPathSegs = 2
)

type createRestAPIInput = CreateRestAPIInput

type deleteRestAPIInput struct {
	RestAPIID string `json:"restApiId"`
}

type getRestAPIInput struct {
	RestAPIID string `json:"restApiId"`
}

type getRestApisInput struct {
	Position string `json:"position"`
	Limit    int    `json:"limit"`
}

type getResourcesInput struct {
	RestAPIID string `json:"restApiId"`
	Position  string `json:"position"`
	Limit     int    `json:"limit"`
}

type getResourceInput struct {
	RestAPIID  string `json:"restApiId"`
	ResourceID string `json:"resourceId"`
}

type createResourceInput struct {
	RestAPIID string `json:"restApiId"`
	ParentID  string `json:"parentId"`
	PathPart  string `json:"pathPart"`
}

type deleteResourceInput struct {
	RestAPIID  string `json:"restApiId"`
	ResourceID string `json:"resourceId"`
}

type putMethodInput struct {
	RequestParameters  map[string]bool   `json:"requestParameters,omitempty"`
	RequestModels      map[string]string `json:"requestModels,omitempty"`
	RestAPIID          string            `json:"restApiId"`
	ResourceID         string            `json:"resourceId"`
	HTTPMethod         string            `json:"httpMethod"`
	AuthorizationType  string            `json:"authorizationType"`
	AuthorizerID       string            `json:"authorizerId"`
	RequestValidatorID string            `json:"requestValidatorId"`
	OperationName      string            `json:"operationName,omitempty"`
	APIKeyRequired     bool              `json:"apiKeyRequired"`
}

type getMethodInput struct {
	RestAPIID  string `json:"restApiId"`
	ResourceID string `json:"resourceId"`
	HTTPMethod string `json:"httpMethod"`
}

type deleteMethodInput struct {
	RestAPIID  string `json:"restApiId"`
	ResourceID string `json:"resourceId"`
	HTTPMethod string `json:"httpMethod"`
}

type putIntegrationInput struct {
	RestAPIID  string `json:"restApiId"`
	ResourceID string `json:"resourceId"`
	HTTPMethod string `json:"httpMethod"`
	PutIntegrationInput
}

type getIntegrationInput struct {
	RestAPIID  string `json:"restApiId"`
	ResourceID string `json:"resourceId"`
	HTTPMethod string `json:"httpMethod"`
}

type deleteIntegrationInput struct {
	RestAPIID  string `json:"restApiId"`
	ResourceID string `json:"resourceId"`
	HTTPMethod string `json:"httpMethod"`
}

type createDeploymentInput struct {
	Variables        map[string]string `json:"variables,omitempty"`
	RestAPIID        string            `json:"restApiId"`
	StageName        string            `json:"stageName"`
	Description      string            `json:"description"`
	StageDescription string            `json:"stageDescription,omitempty"`
	TracingEnabled   bool              `json:"tracingEnabled,omitempty"`
}

type getDeploymentInput struct {
	RestAPIID    string `json:"restApiId"`
	DeploymentID string `json:"deploymentId"`
}

type getDeploymentsInput struct {
	RestAPIID string `json:"restApiId"`
}

type deleteDeploymentInput struct {
	RestAPIID    string `json:"restApiId"`
	DeploymentID string `json:"deploymentId"`
}

type getStagesInput struct {
	RestAPIID string `json:"restApiId"`
}

type getStageInput struct {
	RestAPIID string `json:"restApiId"`
	StageName string `json:"stageName"`
}

type deleteStageInput struct {
	RestAPIID string `json:"restApiId"`
	StageName string `json:"stageName"`
}

type putMethodResponseInput struct {
	PutMethodResponseInput

	RestAPIID  string `json:"restApiId"`
	ResourceID string `json:"resourceId"`
	HTTPMethod string `json:"httpMethod"`
	StatusCode string `json:"statusCode"`
}

type getMethodResponseInput struct {
	RestAPIID  string `json:"restApiId"`
	ResourceID string `json:"resourceId"`
	HTTPMethod string `json:"httpMethod"`
	StatusCode string `json:"statusCode"`
}

type deleteMethodResponseInput struct {
	RestAPIID  string `json:"restApiId"`
	ResourceID string `json:"resourceId"`
	HTTPMethod string `json:"httpMethod"`
	StatusCode string `json:"statusCode"`
}

type putIntegrationResponseInput struct {
	PutIntegrationResponseInput

	RestAPIID  string `json:"restApiId"`
	ResourceID string `json:"resourceId"`
	HTTPMethod string `json:"httpMethod"`
	StatusCode string `json:"statusCode"`
}

type getIntegrationResponseInput struct {
	RestAPIID  string `json:"restApiId"`
	ResourceID string `json:"resourceId"`
	HTTPMethod string `json:"httpMethod"`
	StatusCode string `json:"statusCode"`
}

type deleteIntegrationResponseInput struct {
	RestAPIID  string `json:"restApiId"`
	ResourceID string `json:"resourceId"`
	HTTPMethod string `json:"httpMethod"`
	StatusCode string `json:"statusCode"`
}

type createAuthorizerInput struct {
	RestAPIID                    string   `json:"restApiId"`
	Name                         string   `json:"name"`
	Type                         string   `json:"type"`
	AuthorizerURI                string   `json:"authorizerUri,omitempty"`
	AuthorizerCredentials        string   `json:"authorizerCredentials,omitempty"`
	IdentitySource               string   `json:"identitySource,omitempty"`
	IdentityValidationExpression string   `json:"identityValidationExpression,omitempty"`
	ProviderARNs                 []string `json:"providerARNs,omitempty"`
	AuthorizerResultTTLInSeconds int      `json:"authorizerResultTtlInSeconds,omitempty"`
}

type getAuthorizerInput struct {
	RestAPIID    string `json:"restApiId"`
	AuthorizerID string `json:"authorizerId"`
}

type getAuthorizersInput struct {
	RestAPIID string `json:"restApiId"`
}

type updateAuthorizerInput struct {
	RestAPIID                    string   `json:"restApiId"`
	AuthorizerID                 string   `json:"authorizerId"`
	Name                         string   `json:"name,omitempty"`
	Type                         string   `json:"type,omitempty"`
	AuthorizerURI                string   `json:"authorizerUri,omitempty"`
	AuthorizerCredentials        string   `json:"authorizerCredentials,omitempty"`
	IdentitySource               string   `json:"identitySource,omitempty"`
	IdentityValidationExpression string   `json:"identityValidationExpression,omitempty"`
	ProviderARNs                 []string `json:"providerARNs,omitempty"`
	AuthorizerResultTTLInSeconds int      `json:"authorizerResultTtlInSeconds,omitempty"`
}

type deleteAuthorizerInput struct {
	RestAPIID    string `json:"restApiId"`
	AuthorizerID string `json:"authorizerId"`
}

type createRequestValidatorInput struct {
	RestAPIID                 string `json:"restApiId"`
	Name                      string `json:"name"`
	ValidateRequestBody       bool   `json:"validateRequestBody"`
	ValidateRequestParameters bool   `json:"validateRequestParameters"`
}

type getRequestValidatorInput struct {
	RestAPIID   string `json:"restApiId"`
	ValidatorID string `json:"requestValidatorId"`
}

type getRequestValidatorsInput struct {
	RestAPIID string `json:"restApiId"`
}

type updateRequestValidatorInput struct {
	UpdateRequestValidatorInput

	RestAPIID   string `json:"restApiId"`
	ValidatorID string `json:"requestValidatorId"`
}

type deleteRequestValidatorInput struct {
	RestAPIID   string `json:"restApiId"`
	ValidatorID string `json:"requestValidatorId"`
}

type getAPIKeyInput struct {
	APIKeyID     string `json:"apiKeyId"`
	IncludeValue string `json:"includeValue"`
}

type getAPIKeysPageInput struct {
	Position     string `json:"position"`
	IncludeValue string `json:"includeValue"`
	Limit        int    `json:"limit"`
}

type getDomainNamesPageInput struct {
	Position string `json:"position"`
	Limit    int    `json:"limit"`
}

type getUsagePlansPageInput struct {
	Position string `json:"position"`
	Limit    int    `json:"limit"`
}

type deleteAPIKeyInput struct {
	APIKeyID string `json:"apiKeyId"`
}

type updateAPIKeyInput struct {
	UpdateAPIKeyInput
	APIKeyID string `json:"apiKeyId"`
}

type getDomainNameInput struct {
	DomainName string `json:"domainName"`
}

type deleteDomainNameInput struct {
	DomainName string `json:"domainName"`
}

type getBasePathMappingInput struct {
	DomainName string `json:"domainName"`
	BasePath   string `json:"basePath"`
}

type getBasePathMappingsInput struct {
	DomainName string `json:"domainName"`
}

type deleteBasePathMappingInput struct {
	DomainName string `json:"domainName"`
	BasePath   string `json:"basePath"`
}

type getModelInput struct {
	RestAPIID string `json:"restApiId"`
	ModelName string `json:"modelName"`
}

type getModelsInput struct {
	RestAPIID string `json:"restApiId"`
}

type deleteModelInput struct {
	RestAPIID string `json:"restApiId"`
	ModelName string `json:"modelName"`
}

type updateModelInput struct {
	UpdateModelInput
	RestAPIID string `json:"restApiId"`
	ModelName string `json:"modelName"`
}

type updateStageHandlerInput struct {
	UpdateStageInput
	RestAPIID string `json:"restApiId"`
	StageName string `json:"stageName"`
}

type flushStageCacheInput struct {
	RestAPIID string `json:"restApiId"`
	StageName string `json:"stageName"`
}

type getUsagePlanInput struct {
	UsagePlanID string `json:"usagePlanId"`
}

type deleteUsagePlanInput struct {
	UsagePlanID string `json:"usagePlanId"`
}

type getUsagePlanKeyInput struct {
	UsagePlanID string `json:"usagePlanId"`
	KeyID       string `json:"keyId"`
}

type updateUsageInput struct {
	UsagePlanID string `json:"usagePlanId"`
	KeyID       string `json:"keyId"`
}

type getUsagePlanKeysInput struct {
	UsagePlanID string `json:"usagePlanId"`
}

type deleteUsagePlanKeyInput struct {
	UsagePlanID string `json:"usagePlanId"`
	KeyID       string `json:"keyId"`
}

type getDocumentationPartInput struct {
	RestAPIID string `json:"restApiId"`
	DocPartID string `json:"docPartId"`
}

type getDocumentationPartsInput struct {
	RestAPIID string `json:"restApiId"`
}

type deleteDocumentationPartInput struct {
	RestAPIID string `json:"restApiId"`
	DocPartID string `json:"docPartId"`
}

type getDocumentationVersionInput struct {
	RestAPIID  string `json:"restApiId"`
	DocVersion string `json:"documentationVersion"`
}

type getDocumentationVersionsInput struct {
	RestAPIID string `json:"restApiId"`
}

type deleteDocumentationVersionInput struct {
	RestAPIID  string `json:"restApiId"`
	DocVersion string `json:"documentationVersion"`
}

type updateRestAPIHandlerInput struct {
	RestAPIID string `json:"restApiId"`
	UpdateRestAPIInput
}

type updateDeploymentHandlerInput struct {
	UpdateDeploymentInput
	RestAPIID    string `json:"restApiId"`
	DeploymentID string `json:"deploymentId"`
}

type updateResourceHandlerInput struct {
	UpdateResourceInput
	RestAPIID  string `json:"restApiId"`
	ResourceID string `json:"resourceId"`
}

type getTagsInput struct {
	ResourceARN string `json:"resourceArn"`
}

type tagResourceInput struct {
	Tags        map[string]string `json:"tags"`
	ResourceARN string            `json:"resourceArn"`
}

type untagResourceInput struct {
	ResourceARN string   `json:"resourceArn"`
	TagKeys     []string `json:"tagKeys"`
}

type testInvokeMethodHandlerInput struct {
	TestInvokeMethodInput
}

type getVpcLinkInput struct {
	VpcLinkID string `json:"vpcLinkId"`
}

type deleteVpcLinkInput struct {
	VpcLinkID string `json:"vpcLinkId"`
}

type getExportInput struct {
	RestAPIID  string `json:"restApiId"`
	StageName  string `json:"stageName"`
	ExportType string `json:"exportType"`
}

// Handler is the Echo HTTP service handler for API Gateway operations.
type Handler struct {
	Backend        StorageBackend
	authCache      *authorizerCache
	lambda         LambdaInvoker
	httpClient     *http.Client
	selRegexpCache sync.Map // map[string]*regexp.Regexp — compiled selection-pattern regexps
}

// NewHandler creates a new API Gateway handler with a default HTTP client timeout.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{
		Backend:    backend,
		authCache:  newAuthorizerCache(),
		httpClient: &http.Client{Timeout: apiGWHTTPTimeout},
	}
}

// SetLambdaInvoker configures the Lambda invoker for AWS_PROXY integrations.
func (h *Handler) SetLambdaInvoker(lambda LambdaInvoker) {
	h.lambda = lambda
}

// SetHTTPClient configures the HTTP client used for HTTP/HTTP_PROXY integrations.
// If not set, a dedicated client with a 30-second timeout is used.
func (h *Handler) SetHTTPClient(c *http.Client) {
	h.httpClient = c
}

// apiGWHTTPTimeout is the timeout applied to HTTP/HTTP_PROXY integration requests.
const apiGWHTTPTimeout = 30 * time.Second

// getHTTPClient returns the configured HTTP client.
func (h *Handler) getHTTPClient() *http.Client {
	return h.httpClient
}

// Name returns the service name.
func (h *Handler) Name() string { return "APIGateway" }

// GetSupportedOperations returns all mocked API Gateway operations.
//
//nolint:funlen // enumerating all supported ops necessarily results in a long function
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		opCreateRestAPI,
		opDeleteRestAPI,
		opGetRestAPI,
		opGetRestApis,
		opGetResources,
		opGetResource,
		opCreateResource,
		opDeleteResource,
		opPutMethod,
		opGetMethod,
		opDeleteMethod,
		opPutMethodResponse,
		opGetMethodResponse,
		opDeleteMethodResponse,
		opPutIntegration,
		opGetIntegration,
		opDeleteIntegration,
		opPutIntegrationResponse,
		opGetIntegrationResponse,
		opDeleteIntegrationResponse,
		opCreateDeployment,
		opGetDeployment,
		opGetDeployments,
		opDeleteDeployment,
		opGetStages,
		opGetStage,
		opDeleteStage,
		opCreateAuthorizer,
		opGetAuthorizer,
		opGetAuthorizers,
		opUpdateAuthorizer,
		opDeleteAuthorizer,
		opCreateRequestValidator,
		opGetRequestValidator,
		opGetRequestValidators,
		opUpdateRequestValidator,
		opDeleteRequestValidator,
		opCreateAPIKey,
		opGetAPIKey,
		opGetAPIKeys,
		opDeleteAPIKey,
		opUpdateAPIKey,
		opCreateBasePathMapping,
		opGetBasePathMapping,
		opGetBasePathMappings,
		opDeleteBasePathMapping,
		opCreateDocumentationPart,
		opGetDocumentationPart,
		opGetDocumentationParts,
		opDeleteDocumentationPart,
		opCreateDocumentationVersion,
		opGetDocumentationVersion,
		opGetDocumentationVersions,
		opDeleteDocumentationVersion,
		opCreateDomainName,
		opGetDomainName,
		opGetDomainNames,
		opDeleteDomainName,
		opCreateDomainNameAccessAssociation,
		opCreateModel,
		opGetModel,
		opGetModels,
		opDeleteModel,
		opUpdateModel,
		opCreateStage,
		opUpdateStage,
		opFlushStageCache,
		opFlushStageAuthorizersCache,
		opCreateUsagePlan,
		opGetUsagePlan,
		opGetUsagePlans,
		opDeleteUsagePlan,
		opCreateUsagePlanKey,
		opGetUsagePlanKey,
		opGetUsagePlanKeys,
		opDeleteUsagePlanKey,
		opUpdateRestAPI,
		opUpdateDeployment,
		opUpdateResource,
		opGetAccount,
		opGetTags,
		opTagResource,
		opUntagResource,
		opTestInvokeMethod,
		opUpdateUsagePlan,
		opUpdateDomainName,
		opUpdateBasePathMapping,
		opUpdateDocumentationPart,
		opUpdateDocumentationVersion,
		opUpdateMethod,
		opUpdateIntegration,
		opUpdateIntegrationResponse,
		opUpdateMethodResponse,
		opUpdateAccount,
		opTestInvokeAuthorizer,
		opGetModelTemplate,
		opGetGatewayResponse,
		opGetGatewayResponses,
		opPutGatewayResponse,
		opDeleteGatewayResponse,
		opGenerateClientCertificate,
		opGetClientCertificate,
		opGetClientCertificates,
		opDeleteClientCertificate,
		opGetUsage,
		// Stub implementations.
		opCreateVpcLink,
		opDeleteDomainNameAccessAssociation,
		opDeleteVpcLink,
		opGetDomainNameAccessAssociations,
		opGetExport,
		opGetSdk,
		opGetSdkType,
		opGetSdkTypes,
		opGetVpcLink,
		opGetVpcLinks,
		opImportAPIKeys,
		opImportDocumentationParts,
		opImportRestAPI,
		opPutRestAPI,
		opRejectDomainNameAccessAssociation,
		opUpdateClientCertificate,
		opUpdateGatewayResponse,
		opUpdateUsage,
		opUpdateVpcLink,
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "apigateway" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this API Gateway instance handles.
func (h *Handler) ChaosRegions() []string { return []string{config.DefaultRegion} }

// RouteMatcher returns a matcher for API Gateway requests.
// Matches X-Amz-Target (JSON protocol) and REST paths (/restapis/..., /apikeys, /domainnames/..., /usageplans/...).
// The /tags/{arn} path is only matched when the ARN belongs to an API Gateway resource
// (contains ":apigateway:") so that other services (e.g. FIS) can own their own tag routes.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		if strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), "APIGateway.") {
			return true
		}

		path := c.Request().URL.Path

		if strings.HasPrefix(path, "/restapis") ||
			strings.HasPrefix(path, "/apikeys") ||
			strings.HasPrefix(path, "/domainnames") ||
			strings.HasPrefix(path, "/domainnameaccessassociations") ||
			strings.HasPrefix(path, "/usageplans") ||
			strings.HasPrefix(path, "/sdktypes") ||
			(path == "/account" || strings.HasPrefix(path, "/account/")) ||
			strings.HasPrefix(path, "/"+apiGWSegClientCerts) {
			return true
		}

		// /tags/{arn} — only claim this path when the ARN is an API Gateway resource.
		// API Gateway ARNs contain ":apigateway:" (e.g. arn:aws:apigateway:us-east-1::/restapis/xyz).
		if after, ok := strings.CutPrefix(path, "/tags/"); ok {
			return strings.Contains(after, ":apigateway:")
		}

		return false
	}
}

// MatchPriority returns the routing priority for the API Gateway handler.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the operation name from the X-Amz-Target header or REST path.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	parts := strings.Split(target, ".")
	const targetParts = 2
	if len(parts) == targetParts {
		return parts[1]
	}

	op, _, _ := parseAPIGWRESTPath(c.Request().Method, c.Request().URL.Path)

	return op
}

// ExtractResource extracts the resource identifier from the request body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var data map[string]any
	if uerr := json.Unmarshal(body, &data); uerr != nil {
		return ""
	}

	for _, key := range []string{keyRestAPIID, keyAPIName} {
		if v, ok := data[key].(string); ok && v != "" {
			return v
		}
	}

	return ""
}

// Handler returns the Echo handler function for API Gateway requests.
//
//nolint:cyclop // top-level request dispatcher; complexity is inherent to action routing
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		if c.Request().Method == http.MethodGet && c.Request().URL.Path == "/" {
			return c.JSON(http.StatusOK, h.GetSupportedOperations())
		}

		// Handle proxy invocations for deployed API stages.
		// Path format: /proxy/{apiId}/{stageName}/{resourcePath}
		if strings.HasPrefix(c.Request().URL.Path, "/proxy/") {
			return h.handleStageProxyEcho(c)
		}

		// Handle data-plane invocations via the standard AWS endpoint format.
		// Path format: /restapis/{apiId}/{stageName}/_user_request_/{resourcePath}
		if isUserRequestPath(c.Request().URL.Path) {
			return h.handleUserRequestEcho(c)
		}

		// REST API paths: /restapis/..., /apikeys, /domainnames/..., /usageplans/...
		path := c.Request().URL.Path
		tagsAfter, isTagsPath := strings.CutPrefix(path, "/tags/")
		isAPIGWTagPath := isTagsPath && strings.Contains(tagsAfter, ":apigateway:")
		isRESTPath := strings.HasPrefix(path, "/restapis") ||
			strings.HasPrefix(path, "/apikeys") ||
			strings.HasPrefix(path, "/domainnames") ||
			strings.HasPrefix(path, "/domainnameaccessassociations") ||
			strings.HasPrefix(path, "/usageplans") ||
			strings.HasPrefix(path, "/sdktypes") ||
			(path == "/account" || strings.HasPrefix(path, "/account/")) ||
			strings.HasPrefix(path, "/"+apiGWSegClientCerts) ||
			isAPIGWTagPath

		if isRESTPath && !strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), "APIGateway.") {
			return h.handleRESTAPI(c)
		}

		return h.handleJSONProtocol(c)
	}
}

// handleJSONProtocol handles requests using the JSON protocol (X-Amz-Target header).
func (h *Handler) handleJSONProtocol(c *echo.Context) error {
	ctx := c.Request().Context()
	log := logger.Load(ctx)

	if c.Request().Method != http.MethodPost {
		return c.String(http.StatusMethodNotAllowed, "Method not allowed")
	}

	target := c.Request().Header.Get("X-Amz-Target")
	if target == "" {
		return c.String(http.StatusBadRequest, "Missing X-Amz-Target")
	}

	parts := strings.Split(target, ".")
	const targetParts = 2
	if len(parts) != targetParts {
		return c.String(http.StatusBadRequest, "Invalid X-Amz-Target")
	}
	action := parts[1]

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		log.ErrorContext(ctx, "failed to read request body", "error", err)

		return c.String(http.StatusInternalServerError, "internal server error")
	}

	log.DebugContext(ctx, "APIGateway request", "action", action)

	statusCode, response, reqErr := h.dispatch(ctx, action, body)
	if reqErr != nil {
		return h.handleError(ctx, c, action, reqErr)
	}

	c.Response().Header().Set(headerContentType, "application/x-amz-json-1.1")
	if statusCode == http.StatusNoContent {
		return c.NoContent(http.StatusNoContent)
	}

	return c.JSONBlob(statusCode, response)
}

// handleRESTAPI handles REST-style API Gateway calls (e.g. from the AWS SDK v2).
// It parses the URL path, extracts path parameters, merges them with the request
// body, and dispatches to the existing typed action handlers.
func (h *Handler) handleRESTAPI(c *echo.Context) error {
	ctx := c.Request().Context()

	action, pathParams, ok := parseAPIGWRESTPath(c.Request().Method, c.Request().URL.Path)
	if !ok {
		return c.String(http.StatusNotFound, "not found")
	}

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		logger.Load(ctx).ErrorContext(ctx, "failed to read request body", "error", err)

		return c.String(http.StatusInternalServerError, "internal server error")
	}

	// OpenAPI import (ImportRestApi / PutRestApi) carries the raw spec document
	// as the HTTP body. These are detected here because they share REST paths
	// with CreateRestApi (POST /restapis) and UpdateRestApi (PUT /restapis/{id})
	// but are distinguished by the request method/query, and the body must be
	// passed through verbatim rather than treated as a flat field object.
	if importAction, importBody, isImport := detectImportRESTAPI(
		c.Request().Method, action, pathParams, c.Request().URL.Query(), body,
	); isImport {
		return h.dispatchAndRespond(ctx, c, importAction, importBody, contentTypeJSON)
	}

	// GET requests have no body; normalise to an empty JSON object so that
	// json.Unmarshal calls in the action handlers don't fail with
	// "unexpected end of JSON input".
	if len(body) == 0 {
		body = []byte("{}")
	}

	// Convert RFC 6902 patch arrays to flat JSON objects so PATCH handlers can
	// read fields directly (e.g. [{"op":"replace","path":"/name","value":"x"}]
	// becomes {"name":"x"}).
	body = normalizePatchBody(body)

	// Merge path parameters into the JSON body so existing handlers can read them.
	for k, v := range pathParams {
		body = injectJSONFieldAPIGW(body, k, v)
	}

	// Merge query string parameters (e.g., limit, position) into the JSON body.
	for k, v := range c.Request().URL.Query() {
		if len(v) > 0 {
			body = injectJSONFieldAPIGW(body, k, v[0])
		}
	}

	return h.dispatchAndRespond(ctx, c, action, body, contentTypeJSON)
}

// dispatchAndRespond runs an action through the dispatch table and writes the
// HTTP response, including correct handling of 204 No Content responses.
func (h *Handler) dispatchAndRespond(
	ctx context.Context, c *echo.Context, action string, body []byte, contentType string,
) error {
	statusCode, response, reqErr := h.dispatch(ctx, action, body)
	if reqErr != nil {
		return h.handleError(ctx, c, action, reqErr)
	}

	c.Response().Header().Set(headerContentType, contentType)
	if statusCode == http.StatusNoContent {
		return c.NoContent(http.StatusNoContent)
	}

	return c.JSONBlob(statusCode, response)
}

// detectImportRESTAPI recognises ImportRestApi (POST /restapis?mode=import) and
// PutRestApi (PUT /restapis/{id}) requests, returning the resolved action and a
// JSON-encoded typed input whose Body field carries the raw spec document. The
// AWS SDK sends the OpenAPI/Swagger document as the verbatim HTTP body, so it
// must not be merged with path/query parameters like other operations.
func detectImportRESTAPI(
	method, action string, pathParams map[string]string, query url.Values, body []byte,
) (string, []byte, bool) {
	switch {
	case action == opCreateRestAPI && method == http.MethodPost && query.Get("mode") == "import":
		in := ImportRestAPIInput{
			Body:           body,
			FailOnWarnings: query.Get("failonwarnings") == litTrue,
		}
		encoded, err := json.Marshal(in)
		if err != nil {
			return "", nil, false
		}

		return opImportRestAPI, encoded, true
	case action == opCreateAPIKey && method == http.MethodPost && query.Get("mode") == "import":
		// ImportApiKeys (POST /apikeys?mode=import&format=csv) carries the raw API
		// key file (csv or json) as the verbatim HTTP body.
		in := importAPIKeysInput{
			Body:   body,
			Format: query.Get("format"),
		}
		encoded, err := json.Marshal(in)
		if err != nil {
			return "", nil, false
		}

		return opImportAPIKeys, encoded, true
	case action == opPutRestAPI && method == http.MethodPut && pathParams[keyRestAPIID] != "":
		in := PutRestAPIInput{
			RestAPIID:      pathParams[keyRestAPIID],
			Mode:           query.Get("mode"),
			FailOnWarnings: query.Get("failonwarnings") == litTrue,
			Body:           body,
		}
		encoded, err := json.Marshal(in)
		if err != nil {
			return "", nil, false
		}

		return opPutRestAPI, encoded, true
	}

	return "", nil, false
}

// normalizePatchBody converts a JSON patch array (RFC 6902) to a flat JSON object.
// AWS API Gateway REST PATCH endpoints accept patch operations like
// [{"op":"replace","path":"/description","value":"foo"}].
// This converts them to {"description":"foo"} so existing handlers can read them.
// Bodies that are not JSON arrays are returned unchanged.
func normalizePatchBody(body []byte) []byte {
	if len(body) == 0 || body[0] != '[' {
		return body
	}

	var ops []struct {
		Op    string          `json:"op"`
		Path  string          `json:"path"`
		Value json.RawMessage `json:"value"`
	}

	if err := json.Unmarshal(body, &ops); err != nil || len(ops) == 0 {
		return body
	}

	// Verify at least one entry looks like a patch op (has an "op" field).
	if ops[0].Op == "" {
		return body
	}

	m := make(map[string]json.RawMessage)

	for _, op := range ops {
		if op.Op != "replace" && op.Op != "add" {
			continue
		}

		field := strings.TrimPrefix(op.Path, "/")
		if field == "" {
			continue
		}

		m[field] = op.Value
	}

	out, err := json.Marshal(m)
	if err != nil {
		return body
	}

	return out
}

// injectJSONFieldAPIGW merges a key/value string pair into a JSON object body.
func injectJSONFieldAPIGW(body []byte, key, value string) []byte {
	var m map[string]json.RawMessage
	if len(body) > 0 {
		if err := json.Unmarshal(body, &m); err != nil {
			m = make(map[string]json.RawMessage)
		}
	} else {
		m = make(map[string]json.RawMessage)
	}

	quoted, _ := json.Marshal(value)
	m[key] = json.RawMessage(quoted)

	result, _ := json.Marshal(m)

	return result
}

// parseAPIGWRESTPath maps an HTTP method + URL path to an API Gateway operation name
// and extracts path parameters. Returns ("Unknown", nil, false) when no pattern matches.
func parseAPIGWRESTPath(method, path string) (string, map[string]string, bool) {
	// Strip leading "/" and split into path segments.
	segs := strings.Split(strings.TrimPrefix(path, "/"), "/")
	n := len(segs)

	if n == 0 {
		return apiGWUnknownOp, nil, false
	}

	switch segs[0] {
	case "restapis":
		return parseAPIGWRestAPIsPath(method, segs, n)
	case apiGWSegAPIKeys:
		return parseAPIGWAPIKeysPath(method, segs, n)
	case apiGWSegDomainNames:
		return parseAPIGWDomainNamesPath(method, segs, n)
	case apiGWSegUsagePlans:
		return parseAPIGWUsagePlansPath(method, segs, n)
	case "account":
		return parseAPIGWAccountPath(method, segs, n)
	case "tags":
		return parseAPIGWTagsPath(method, segs, n)
	case apiGWSegClientCerts:
		return parseAPIGWClientCertificatesPath(method, segs, n)
	case apiGWSegSdkTypes:
		return parseAPIGWSdkTypesPath(method, segs, n)
	case apiGWSegDomainNameAccessAssociations:
		// GET /domainnameaccessassociations → GetDomainNameAccessAssociations
		if n == pathDepth1 && method == http.MethodGet {
			return opGetDomainNameAccessAssociations, nil, true
		}

		return apiGWUnknownOp, nil, false
	}

	return apiGWUnknownOp, nil, false
}

// parseAPIGWSdkTypesPath handles /sdktypes and /sdktypes/{id} paths.
func parseAPIGWSdkTypesPath(method string, segs []string, n int) (string, map[string]string, bool) {
	switch {
	// GET /sdktypes → GetSdkTypes
	case n == pathDepth1 && method == http.MethodGet:
		return opGetSdkTypes, nil, true
	// GET /sdktypes/{id} → GetSdkType
	case n == pathDepth2 && method == http.MethodGet:
		return opGetSdkType, map[string]string{"id": segs[1]}, true
	}

	return apiGWUnknownOp, nil, false
}

// parseAPIGWAccountPath handles /account paths.
func parseAPIGWAccountPath(method string, _ []string, n int) (string, map[string]string, bool) {
	// GET /account → GetAccount
	if n == 1 && method == http.MethodGet {
		return opGetAccount, nil, true
	}
	// PATCH /account → UpdateAccount
	if n == 1 && method == http.MethodPatch {
		return opUpdateAccount, nil, true
	}

	return apiGWUnknownOp, nil, false
}

// parseAPIGWClientCertificatesPath handles /clientcertificates/... paths.
func parseAPIGWClientCertificatesPath(method string, segs []string, n int) (string, map[string]string, bool) {
	switch {
	// GET /clientcertificates → GetClientCertificates
	case n == 1 && method == http.MethodGet:
		return opGetClientCertificates, nil, true
	// POST /clientcertificates → GenerateClientCertificate
	case n == 1 && method == http.MethodPost:
		return opGenerateClientCertificate, nil, true
	// GET /clientcertificates/{id} → GetClientCertificate
	case n == pathDepth2 && method == http.MethodGet:
		return opGetClientCertificate, map[string]string{"clientCertificateId": segs[1]}, true
	// DELETE /clientcertificates/{id} → DeleteClientCertificate
	case n == pathDepth2 && method == http.MethodDelete:
		return opDeleteClientCertificate, map[string]string{"clientCertificateId": segs[1]}, true
	}

	return apiGWUnknownOp, nil, false
}

// parseAPIGWTagsPath handles /tags/{resourceArn} paths.
func parseAPIGWTagsPath(method string, segs []string, n int) (string, map[string]string, bool) {
	if n < apiGWMinTagPathSegs {
		return apiGWUnknownOp, nil, false
	}
	// Re-join remaining segs to reconstruct the ARN (which may contain slashes).
	arn := strings.Join(segs[1:], "/")

	switch method {
	// GET /tags/{resourceArn} → GetTags
	case http.MethodGet:
		return opGetTags, map[string]string{keyResourceArn: arn}, true
	// PUT /tags/{resourceArn} → TagResource
	case http.MethodPut:
		return opTagResource, map[string]string{keyResourceArn: arn}, true
	// DELETE /tags/{resourceArn} → UntagResource
	case http.MethodDelete:
		return opUntagResource, map[string]string{keyResourceArn: arn}, true
	}

	return apiGWUnknownOp, nil, false
}

// parseAPIGWAPIKeysPath handles /apikeys/... paths.
func parseAPIGWAPIKeysPath(method string, segs []string, n int) (string, map[string]string, bool) {
	switch {
	// GET /apikeys → GetApiKeys
	case n == 1 && method == http.MethodGet:
		return opGetAPIKeys, nil, true
	// POST /apikeys → CreateAPIKey
	case n == 1 && method == http.MethodPost:
		return opCreateAPIKey, nil, true
	// GET /apikeys/{id} → GetApiKey
	case n == pathDepth2 && method == http.MethodGet:
		return opGetAPIKey, map[string]string{keyAPIKeyID: segs[1]}, true
	// DELETE /apikeys/{id} → DeleteApiKey
	case n == pathDepth2 && method == http.MethodDelete:
		return opDeleteAPIKey, map[string]string{keyAPIKeyID: segs[1]}, true
	// PATCH /apikeys/{id} → UpdateApiKey
	case n == pathDepth2 && method == http.MethodPatch:
		return opUpdateAPIKey, map[string]string{keyAPIKeyID: segs[1]}, true
	}

	return apiGWUnknownOp, nil, false
}

// parseAPIGWDomainNamesPath handles /domainnames/... paths.
//

// parseAPIGWDomainNamesPath handles /domainnames/... paths.
func parseAPIGWDomainNamesPath(method string, segs []string, n int) (string, map[string]string, bool) {
	switch n {
	case pathDepth1:
		switch method {
		case http.MethodGet:
			return opGetDomainNames, nil, true
		case http.MethodPost:
			return opCreateDomainName, nil, true
		}
	case pathDepth2:
		switch method {
		case http.MethodGet:
			return opGetDomainName, map[string]string{keyDomainName: segs[1]}, true
		case http.MethodDelete:
			return opDeleteDomainName, map[string]string{keyDomainName: segs[1]}, true
		case http.MethodPatch:
			return opUpdateDomainName, map[string]string{keyDomainName: segs[1]}, true
		}
	case pathDepth3:
		return parseAPIGWDomainNamesDepth3(method, segs)
	case pathDepth4:
		if segs[2] == apiGWSegBasePathMappings {
			return parseAPIGWDomainNamesBasePathMapping(method, segs)
		}
	}

	return apiGWUnknownOp, nil, false
}

// parseAPIGWDomainNamesDepth3 handles /domainnames/{name}/{sub} paths.
func parseAPIGWDomainNamesDepth3(method string, segs []string) (string, map[string]string, bool) {
	switch segs[2] {
	case apiGWSegBasePathMappings:
		switch method {
		case http.MethodGet:
			return opGetBasePathMappings, map[string]string{keyDomainName: segs[1]}, true
		case http.MethodPost:
			return opCreateBasePathMapping, map[string]string{keyDomainName: segs[1]}, true
		}
	case apiGWSegAccessAssociations:
		if method == http.MethodPost {
			return opCreateDomainNameAccessAssociation, map[string]string{keyDomainName: segs[1]}, true
		}
	}

	return apiGWUnknownOp, nil, false
}

// parseAPIGWDomainNamesBasePathMapping handles /domainnames/{name}/basepathmappings/{basePath} paths.
func parseAPIGWDomainNamesBasePathMapping(method string, segs []string) (string, map[string]string, bool) {
	params := map[string]string{keyDomainName: segs[1], keyBasePath: segs[3]}

	switch method {
	case http.MethodGet:
		return opGetBasePathMapping, params, true
	case http.MethodPatch:
		return opUpdateBasePathMapping, params, true
	case http.MethodDelete:
		return opDeleteBasePathMapping, params, true
	}

	return apiGWUnknownOp, nil, false
}

// parseAPIGWUsagePlansPath handles /usageplans/... paths.
func parseAPIGWUsagePlansPath(method string, segs []string, n int) (string, map[string]string, bool) {
	switch n {
	case pathDepth1:
		switch method {
		case http.MethodGet:
			return opGetUsagePlans, nil, true
		case http.MethodPost:
			return opCreateUsagePlan, nil, true
		}
	case pathDepth2:
		planParam := map[string]string{keyUsagePlanID: segs[1]}
		switch method {
		case http.MethodGet:
			return opGetUsagePlan, planParam, true
		case http.MethodDelete:
			return opDeleteUsagePlan, planParam, true
		case http.MethodPatch:
			return opUpdateUsagePlan, planParam, true
		}
	case pathDepth3:
		return parseAPIGWUsagePlansDepth3(method, segs)
	case pathDepth4:
		return parseAPIGWUsagePlansDepth4(method, segs)
	case pathDepth5:
		return parseAPIGWUsagePlansDepth5(method, segs)
	}

	return apiGWUnknownOp, nil, false
}

// parseAPIGWUsagePlansDepth3 handles /usageplans/{id}/{sub} paths.
func parseAPIGWUsagePlansDepth3(method string, segs []string) (string, map[string]string, bool) {
	planParam := map[string]string{keyUsagePlanID: segs[1]}

	switch segs[2] {
	case "usage":
		if method == http.MethodGet {
			return opGetUsage, planParam, true
		}
	case apiGWSegUsagePlanKeys:
		switch method {
		case http.MethodGet:
			return opGetUsagePlanKeys, planParam, true
		case http.MethodPost:
			return opCreateUsagePlanKey, planParam, true
		}
	}

	return apiGWUnknownOp, nil, false
}

// parseAPIGWUsagePlansDepth4 handles /usageplans/{id}/keys/{keyId} paths.
func parseAPIGWUsagePlansDepth4(method string, segs []string) (string, map[string]string, bool) {
	if segs[2] != apiGWSegUsagePlanKeys {
		return apiGWUnknownOp, nil, false
	}

	params := map[string]string{keyUsagePlanID: segs[1], "keyId": segs[3]}

	switch method {
	case http.MethodGet:
		return opGetUsagePlanKey, params, true
	case http.MethodDelete:
		return opDeleteUsagePlanKey, params, true
	}

	return apiGWUnknownOp, nil, false
}

// parseAPIGWUsagePlansDepth5 handles /usageplans/{id}/keys/{keyId}/usage paths.
func parseAPIGWUsagePlansDepth5(method string, segs []string) (string, map[string]string, bool) {
	if segs[2] != apiGWSegUsagePlanKeys || segs[4] != "usage" {
		return apiGWUnknownOp, nil, false
	}

	params := map[string]string{keyUsagePlanID: segs[1], "keyId": segs[3]}

	if method == http.MethodPatch {
		return opUpdateUsage, params, true
	}

	return apiGWUnknownOp, nil, false
}

// parseAPIGWRestAPIsPath handles /restapis/... paths.
//
// parseAPIGWRestAPIsPath handles /restapis/... paths.
func parseAPIGWRestAPIsPath(method string, segs []string, n int) (string, map[string]string, bool) {
	apiID := ""
	if n >= pathDepth2 {
		apiID = segs[1]
	}

	switch n {
	case pathDepth1:
		switch method {
		case http.MethodPost:
			return opCreateRestAPI, nil, true
		case http.MethodGet:
			return opGetRestApis, nil, true
		}
	case pathDepth2:
		return parseAPIGWRestAPIsDepth2(method, apiID)
	case pathDepth3:
		return parseAPIGWRestAPIsDepth3(method, segs, apiID)
	case pathDepth4:
		return parseAPIGWRestAPIsDepth4(method, segs, apiID)
	default:
		return parseAPIGWRestAPIsDeepPath(method, segs, n, apiID)
	}

	return apiGWUnknownOp, nil, false
}

// parseAPIGWRestAPIsDepth2 handles /restapis/{id} paths.
func parseAPIGWRestAPIsDepth2(method, apiID string) (string, map[string]string, bool) {
	params := map[string]string{keyRestAPIID: apiID}
	switch method {
	case http.MethodGet:
		return opGetRestAPI, params, true
	case http.MethodDelete:
		return opDeleteRestAPI, params, true
	case http.MethodPatch:
		return opUpdateRestAPI, params, true
	case http.MethodPut:
		// PUT /restapis/{id} is PutRestApi (OpenAPI import into an existing
		// API). The body is the raw spec; detectImportRESTAPI handles it.
		return opPutRestAPI, params, true
	}

	return apiGWUnknownOp, nil, false
}

// parseAPIGWRestAPIsDepth3 handles /restapis/{id}/{subresource} paths.
func parseAPIGWRestAPIsDepth3(method string, segs []string, apiID string) (string, map[string]string, bool) {
	apiParam := map[string]string{keyRestAPIID: apiID}

	if op, params, ok := parseAPIGWRestAPIsDepth3Core(method, segs[2], apiParam); ok {
		return op, params, ok
	}

	return parseAPIGWRestAPIsDepth3Extended(method, segs[2], apiParam)
}

// parseAPIGWRestAPIsDepth3Core handles resources, deployments, and stages depth-3 paths.
func parseAPIGWRestAPIsDepth3Core(method, sub string, apiParam map[string]string) (string, map[string]string, bool) {
	switch sub {
	case apiGWSegResources:
		if method == http.MethodGet {
			return opGetResources, apiParam, true
		}
	case apiGWSegDeployment:
		switch method {
		case http.MethodPost:
			return opCreateDeployment, apiParam, true
		case http.MethodGet:
			return opGetDeployments, apiParam, true
		}
	case apiGWSegStages:
		switch method {
		case http.MethodGet:
			return opGetStages, apiParam, true
		case http.MethodPost:
			return opCreateStage, apiParam, true
		}
	}

	return apiGWUnknownOp, nil, false
}

// parseAPIGWRestAPIsDepth3Extended handles authorizers, validators, models, and gateway responses depth-3 paths.
func parseAPIGWRestAPIsDepth3Extended(
	method, sub string,
	apiParam map[string]string,
) (string, map[string]string, bool) {
	switch sub {
	case apiGWSegAuthorizers:
		switch method {
		case http.MethodPost:
			return opCreateAuthorizer, apiParam, true
		case http.MethodGet:
			return opGetAuthorizers, apiParam, true
		}
	case apiGWSegValidators:
		switch method {
		case http.MethodPost:
			return opCreateRequestValidator, apiParam, true
		case http.MethodGet:
			return opGetRequestValidators, apiParam, true
		}
	case apiGWSegModels:
		switch method {
		case http.MethodPost:
			return opCreateModel, apiParam, true
		case http.MethodGet:
			return opGetModels, apiParam, true
		}
	case apiGWSegGatewayResponses:
		if method == http.MethodGet {
			return opGetGatewayResponses, apiParam, true
		}
	}

	return apiGWUnknownOp, nil, false
}

// parseAPIGWRestAPIsDepth4 handles /restapis/{id}/{subresource}/{item} paths.
func parseAPIGWRestAPIsDepth4(method string, segs []string, apiID string) (string, map[string]string, bool) {
	if op, params, ok := parseAPIGWRestAPIsDepth4Core(method, segs, apiID); ok {
		return op, params, ok
	}

	return parseAPIGWRestAPIsDepth4Extended(method, segs, apiID)
}

// parseAPIGWRestAPIsDepth4Core handles resources, deployments, and stages depth-4 paths.
func parseAPIGWRestAPIsDepth4Core(method string, segs []string, apiID string) (string, map[string]string, bool) {
	switch segs[2] {
	case apiGWSegResources:
		params := map[string]string{keyRestAPIID: apiID, keyResourceID: segs[3]}
		switch method {
		case http.MethodGet:
			return opGetResource, params, true
		case http.MethodPost:
			return opCreateResource, map[string]string{keyRestAPIID: apiID, "parentId": segs[3]}, true
		case http.MethodPatch:
			return opUpdateResource, params, true
		case http.MethodDelete:
			return opDeleteResource, params, true
		}
	case apiGWSegDeployment:
		params := map[string]string{keyRestAPIID: apiID, keyDeploymentID: segs[3]}
		switch method {
		case http.MethodGet:
			return opGetDeployment, params, true
		case http.MethodPatch:
			return opUpdateDeployment, params, true
		case http.MethodDelete:
			return opDeleteDeployment, params, true
		}
	case apiGWSegStages:
		params := map[string]string{keyRestAPIID: apiID, keyStageName: segs[3]}
		switch method {
		case http.MethodGet:
			return opGetStage, params, true
		case http.MethodDelete:
			return opDeleteStage, params, true
		case http.MethodPatch:
			return opUpdateStage, params, true
		}
	}

	return apiGWUnknownOp, nil, false
}

// parseAPIGWRestAPIsDepth4Extended handles authorizers, validators, models, gateway responses, and doc depth-4 paths.
func parseAPIGWRestAPIsDepth4Extended(method string, segs []string, apiID string) (string, map[string]string, bool) {
	if op, params, ok := parseAPIGWRestAPIsDepth4AuthVal(method, segs, apiID); ok {
		return op, params, ok
	}

	return parseAPIGWRestAPIsDepth4ModelGW(method, segs, apiID)
}

// parseAPIGWRestAPIsDepth4AuthVal handles authorizer and validator depth-4 paths.
func parseAPIGWRestAPIsDepth4AuthVal(method string, segs []string, apiID string) (string, map[string]string, bool) {
	switch segs[2] {
	case apiGWSegAuthorizers:
		params := map[string]string{keyRestAPIID: apiID, keyAuthorizerID: segs[3]}
		switch method {
		case http.MethodGet:
			return opGetAuthorizer, params, true
		case http.MethodPatch:
			return opUpdateAuthorizer, params, true
		case http.MethodDelete:
			return opDeleteAuthorizer, params, true
		}
	case apiGWSegValidators:
		params := map[string]string{keyRestAPIID: apiID, keyRequestValidatorID: segs[3]}
		switch method {
		case http.MethodGet:
			return opGetRequestValidator, params, true
		case http.MethodPatch:
			return opUpdateRequestValidator, params, true
		case http.MethodDelete:
			return opDeleteRequestValidator, params, true
		}
	}

	return apiGWUnknownOp, nil, false
}

// parseAPIGWRestAPIsDepth4ModelGW handles model, gateway response, and documentation depth-4 paths.
func parseAPIGWRestAPIsDepth4ModelGW(method string, segs []string, apiID string) (string, map[string]string, bool) {
	switch segs[2] {
	case apiGWSegModels:
		params := map[string]string{keyRestAPIID: apiID, keyModelName: segs[3]}
		switch method {
		case http.MethodGet:
			return opGetModel, params, true
		case http.MethodDelete:
			return opDeleteModel, params, true
		case http.MethodPatch:
			return opUpdateModel, params, true
		}
	case apiGWSegGatewayResponses:
		params := map[string]string{keyRestAPIID: apiID, keyResponseType: segs[3]}
		switch method {
		case http.MethodGet:
			return opGetGatewayResponse, params, true
		case http.MethodPut:
			return opPutGatewayResponse, params, true
		case http.MethodDelete:
			return opDeleteGatewayResponse, params, true
		}
	case apiGWSegDocumentation:
		return parseAPIGWRestAPIsDocDepth4(method, segs, apiID)
	}

	return apiGWUnknownOp, nil, false
}

// parseAPIGWRestAPIsDocDepth4 handles /restapis/{id}/documentation/{parts|versions} paths.
func parseAPIGWRestAPIsDocDepth4(method string, segs []string, apiID string) (string, map[string]string, bool) {
	apiParam := map[string]string{keyRestAPIID: apiID}

	switch segs[3] {
	case apiGWSegDocParts:
		switch method {
		case http.MethodPost:
			return opCreateDocumentationPart, apiParam, true
		case http.MethodGet:
			return opGetDocumentationParts, apiParam, true
		}
	case apiGWSegDocVersions:
		switch method {
		case http.MethodPost:
			return opCreateDocumentationVersion, apiParam, true
		case http.MethodGet:
			return opGetDocumentationVersions, apiParam, true
		}
	}

	return apiGWUnknownOp, nil, false
}

// parseAPIGWRestAPIsDeepPath handles /restapis/{id}/... paths with n≥5.
func parseAPIGWRestAPIsDeepPath(method string, segs []string, n int, apiID string) (string, map[string]string, bool) {
	if n >= 5 && segs[2] == apiGWSegResources && segs[4] == apiGWSegMethods {
		return parseAPIGWMethodPath(method, segs)
	}

	return parseAPIGWRestAPIsDepth5Plus(method, segs, n, apiID)
}

// parseAPIGWRestAPIsDepth5Plus handles depth-5+ paths (documentation, stage cache, model template).
func parseAPIGWRestAPIsDepth5Plus(method string, segs []string, n int, apiID string) (string, map[string]string, bool) {
	switch segs[2] {
	case apiGWSegStages:
		return parseAPIGWRestAPIsStageDeep(method, segs, n, apiID)
	case apiGWSegDocumentation:
		return parseAPIGWRestAPIsDocDeep(method, segs, n, apiID)
	case apiGWSegModels:
		if n == 5 && segs[4] == "default_template" && method == http.MethodGet {
			return opGetModelTemplate, map[string]string{keyRestAPIID: apiID, keyModelName: segs[3]}, true
		}
	case apiGWSegAuthorizers:
		if n == 5 && segs[4] == "invocations" && method == http.MethodPost {
			return opTestInvokeAuthorizer, map[string]string{keyRestAPIID: apiID, keyAuthorizerID: segs[3]}, true
		}
	}

	return apiGWUnknownOp, nil, false
}

// parseAPIGWRestAPIsStageDeep handles depth-5+ stage paths (cache flush).
func parseAPIGWRestAPIsStageDeep(method string, segs []string, n int, apiID string) (string, map[string]string, bool) {
	stageParams := map[string]string{keyRestAPIID: apiID, keyStageName: segs[3]}

	if n == 5 && segs[4] == "cache" && method == http.MethodDelete {
		return opFlushStageCache, stageParams, true
	}

	if n == 6 && segs[4] == "cache" && segs[5] == "authorizers" && method == http.MethodDelete {
		return opFlushStageAuthorizersCache, stageParams, true
	}

	// GET /restapis/{id}/stages/{stage}/sdks/{sdkType} → GetSdk
	if n == 6 && segs[4] == apiGWSegSdks && method == http.MethodGet {
		sdkParams := map[string]string{keyRestAPIID: apiID, keyStageName: segs[3], keySdkType: segs[5]}

		return opGetSdk, sdkParams, true
	}

	return apiGWUnknownOp, nil, false
}

// parseAPIGWRestAPIsDocDeep handles depth-5 documentation parts/versions paths.
func parseAPIGWRestAPIsDocDeep(method string, segs []string, n int, apiID string) (string, map[string]string, bool) {
	if n != pathDepth5 {
		return apiGWUnknownOp, nil, false
	}

	switch segs[3] {
	case apiGWSegDocParts:
		params := map[string]string{keyRestAPIID: apiID, keyDocPartID: segs[4]}
		switch method {
		case http.MethodGet:
			return opGetDocumentationPart, params, true
		case http.MethodDelete:
			return opDeleteDocumentationPart, params, true
		case http.MethodPatch:
			return opUpdateDocumentationPart, params, true
		}
	case apiGWSegDocVersions:
		params := map[string]string{keyRestAPIID: apiID, keyDocumentationVersion: segs[4]}
		switch method {
		case http.MethodGet:
			return opGetDocumentationVersion, params, true
		case http.MethodDelete:
			return opDeleteDocumentationVersion, params, true
		case http.MethodPatch:
			return opUpdateDocumentationVersion, params, true
		}
	}

	return apiGWUnknownOp, nil, false
}

// parseAPIGWMethodPath handles paths under /restapis/{id}/resources/{resId}/methods/{httpMethod}.
//
// parseAPIGWMethodPath handles paths under /restapis/{id}/resources/{resId}/methods/{httpMethod}.
func parseAPIGWMethodPath(method string, segs []string) (string, map[string]string, bool) {
	const (
		idxID         = 1
		idxResourceID = 3
		idxHTTPMethod = 5
		idxIntegSeg   = 6
		idxRespSeg    = 7
	)

	if len(segs) <= idxHTTPMethod {
		return apiGWUnknownOp, nil, false
	}

	apiID := segs[idxID]
	resID := segs[idxResourceID]
	httpMethod := segs[idxHTTPMethod]
	baseParams := map[string]string{
		keyRestAPIID:  apiID,
		keyResourceID: resID,
		keyHTTPMethod: httpMethod,
	}

	if len(segs) > idxIntegSeg {
		op, params, ok := parseAPIGWMethodSubPath(
			method, segs, idxIntegSeg, idxRespSeg, apiID, resID, httpMethod, baseParams,
		)
		if ok || op == apiGWUnknownOp {
			return op, params, ok
		}
	}

	return parseAPIGWMethodBasePath(method, baseParams)
}

// parseAPIGWMethodSubPath routes method sub-paths (integration and responses).
func parseAPIGWMethodSubPath(
	method string,
	segs []string,
	idxInteg, idxResp int,
	apiID, resID, httpMethod string,
	baseParams map[string]string,
) (string, map[string]string, bool) {
	seg := segs[idxInteg]

	switch seg {
	case apiGWSegInteg:
		return parseAPIGWIntegrationPath(method, segs, idxInteg, idxResp, apiID, resID, httpMethod, baseParams)
	case apiGWSegResponses:
		return parseAPIGWMethodResponsePath(method, segs, idxInteg, apiID, resID, httpMethod)
	}

	return "", nil, false
}

// parseAPIGWIntegrationPath routes integration paths.
func parseAPIGWIntegrationPath(
	method string,
	segs []string,
	idxInteg, idxResp int,
	apiID, resID, httpMethod string,
	baseParams map[string]string,
) (string, map[string]string, bool) {
	if len(segs) > idxResp && segs[idxResp] == apiGWSegResponses {
		if len(segs) <= idxResp+1 {
			return apiGWUnknownOp, nil, false
		}

		params := map[string]string{
			keyRestAPIID:  apiID,
			keyResourceID: resID,
			keyHTTPMethod: httpMethod,
			keyStatusCode: segs[idxResp+1],
		}

		switch method {
		case http.MethodPut:
			return opPutIntegrationResponse, params, true
		case http.MethodGet:
			return opGetIntegrationResponse, params, true
		case http.MethodDelete:
			return opDeleteIntegrationResponse, params, true
		case http.MethodPatch:
			return opUpdateIntegrationResponse, params, true
		}

		return apiGWUnknownOp, nil, false
	}

	switch method {
	case http.MethodPut:
		return opPutIntegration, baseParams, true
	case http.MethodGet:
		return opGetIntegration, baseParams, true
	case http.MethodDelete:
		return opDeleteIntegration, baseParams, true
	case http.MethodPatch:
		return opUpdateIntegration, baseParams, true
	}

	_ = idxInteg

	return apiGWUnknownOp, nil, false
}

// parseAPIGWMethodResponsePath routes method response paths.
func parseAPIGWMethodResponsePath(
	method string,
	segs []string,
	idxInteg int,
	apiID, resID, httpMethod string,
) (string, map[string]string, bool) {
	if len(segs) <= idxInteg+1 {
		return apiGWUnknownOp, nil, false
	}

	params := map[string]string{
		keyRestAPIID:  apiID,
		keyResourceID: resID,
		keyHTTPMethod: httpMethod,
		keyStatusCode: segs[idxInteg+1],
	}

	switch method {
	case http.MethodPut:
		return opPutMethodResponse, params, true
	case http.MethodGet:
		return opGetMethodResponse, params, true
	case http.MethodDelete:
		return opDeleteMethodResponse, params, true
	case http.MethodPatch:
		return opUpdateMethodResponse, params, true
	}

	return apiGWUnknownOp, nil, false
}

// parseAPIGWMethodBasePath routes the base method path (no sub-segment).
func parseAPIGWMethodBasePath(method string, baseParams map[string]string) (string, map[string]string, bool) {
	switch method {
	case http.MethodPut:
		return opPutMethod, baseParams, true
	case http.MethodGet:
		return opGetMethod, baseParams, true
	case http.MethodDelete:
		return opDeleteMethod, baseParams, true
	case http.MethodPost:
		return opTestInvokeMethod, baseParams, true
	case http.MethodPatch:
		return opUpdateMethod, baseParams, true
	}

	return apiGWUnknownOp, nil, false
}

type actionFn func([]byte) (int, any, error)

// handleStageProxyEcho routes /proxy/{apiId}/{stageName}/{path} requests to the Lambda proxy handler.
func (h *Handler) handleStageProxyEcho(c *echo.Context) error {
	// Strip the /proxy/ prefix to get /{apiId}/{stageName}/{path}
	rest := strings.TrimPrefix(c.Request().URL.Path, "/proxy/")
	const minProxyPathParts = 2
	parts := strings.SplitN(rest, "/", 3) //nolint:mnd // 3-part split: apiId, stage, path
	if len(parts) < minProxyPathParts {
		return c.String(http.StatusNotFound, "invalid proxy path")
	}

	apiID := parts[0]
	stageName := parts[1]

	// Rewrite the URL so handleProxyRequest sees "/{stageName}/{resourcePath}".
	resourcePath := "/"
	if len(parts) == 3 && parts[2] != "" {
		resourcePath = "/" + parts[2]
	}

	r := c.Request().Clone(c.Request().Context())
	r.URL.Path = "/" + stageName + resourcePath

	fn := h.handleProxyRequest(apiID, stageName)
	fn(c.Response(), r)

	return nil
}

// isUserRequestPath reports whether the path follows the data-plane format:
// /restapis/{apiId}/{stageName}/_user_request_/{resourcePath...}.
func isUserRequestPath(path string) bool {
	segs := strings.Split(strings.TrimPrefix(path, "/"), "/")
	const minSegs = 4 // restapis, apiId, stageName, _user_request_

	return len(segs) >= minSegs && segs[0] == "restapis" && segs[3] == "_user_request_"
}

// handleUserRequestEcho handles data-plane invocations at the standard AWS endpoint:
// /restapis/{apiId}/{stageName}/_user_request_/{resourcePath...}.
func (h *Handler) handleUserRequestEcho(c *echo.Context) error {
	segs := strings.Split(strings.TrimPrefix(c.Request().URL.Path, "/"), "/")
	// segs: [restapis, {apiId}, {stageName}, _user_request_, {path...}]
	const (
		idxAPIID     = 1
		idxStageName = 2
		idxPathStart = 4
	)

	apiID := segs[idxAPIID]
	stageName := segs[idxStageName]

	resourcePath := "/"
	if len(segs) > idxPathStart && segs[idxPathStart] != "" {
		resourcePath = "/" + strings.Join(segs[idxPathStart:], "/")
	}

	// Rewrite the URL so handleProxyRequest sees "/{stageName}/{resourcePath}".
	r := c.Request().Clone(c.Request().Context())
	r.URL.Path = "/" + stageName + resourcePath

	fn := h.handleProxyRequest(apiID, stageName)
	fn(c.Response(), r)

	return nil
}

// testInvokeMethod executes a test invocation of a method, routing through the real
// integration when Lambda is configured. Falls back to mock/stub for unsupported types.
func (h *Handler) testInvokeMethod(input TestInvokeMethodInput) (*TestInvokeMethodOutput, error) {
	resource, err := h.Backend.GetResource(input.RestAPIID, input.ResourceID)
	if err != nil {
		return nil, fmt.Errorf("%w: resource %s", ErrResourceNotFound, input.ResourceID)
	}

	integration, err := h.Backend.GetIntegration(input.RestAPIID, input.ResourceID, input.HTTPMethod)
	if err != nil {
		integration, _ = h.Backend.GetIntegration(input.RestAPIID, input.ResourceID, "ANY")
	}

	if integration == nil {
		// No integration: return empty 200.
		return &TestInvokeMethodOutput{
			Status:  http.StatusOK,
			Body:    "{}",
			Latency: 1,
			Log:     "Test invocation: no integration configured",
			Headers: map[string]string{headerContentType: contentTypeJSON},
		}, nil
	}

	switch integration.Type {
	case IntegrationTypeMock:
		// For MOCK, select the integration response matching "200" and apply its template.
		body := `{"statusCode": 200}`
		ir, irErr := h.Backend.GetIntegrationResponse(
			input.RestAPIID, input.ResourceID, input.HTTPMethod, "200",
		)
		if irErr == nil {
			if t, ok := ir.ResponseTemplates["application/json"]; ok && t != "" {
				body = t
			}
		}

		return &TestInvokeMethodOutput{
			Status:  http.StatusOK,
			Body:    body,
			Latency: 1,
			Log:     "Test invocation: MOCK integration",
			Headers: map[string]string{headerContentType: contentTypeJSON},
		}, nil

	case IntegrationTypeAWSProxy, "AWS":
		return h.invokeLambdaTestMethod(input, resource, integration)

	default:
		return h.Backend.TestInvokeMethod(input)
	}
}

func (h *Handler) invokeLambdaTestMethod(
	input TestInvokeMethodInput,
	resource *Resource,
	integration *Integration,
) (*TestInvokeMethodOutput, error) {
	if h.lambda == nil {
		return h.Backend.TestInvokeMethod(input)
	}

	// Build a synthetic HTTP request from the TestInvokeMethodInput.
	rawPath := input.PathWithQueryString
	if rawPath == "" {
		rawPath = resource.Path
	}

	syntheticReq, reqErr := http.NewRequestWithContext(
		context.Background(),
		input.HTTPMethod,
		"http://test-invoke-endpoint"+rawPath,
		strings.NewReader(input.Body),
	)
	if reqErr != nil {
		return nil, fmt.Errorf("test invoke: failed to build request: %w", reqErr)
	}

	for k, v := range input.Headers {
		syntheticReq.Header.Set(k, v)
	}

	event, buildErr := BuildProxyEvent(syntheticReq, input.RestAPIID, "test-invoke", resource.Path, rawPath, nil)
	if buildErr != nil {
		return nil, fmt.Errorf("test invoke: failed to build proxy event: %w", buildErr)
	}

	payload, _ := json.Marshal(event)

	lambdaFn := ExtractLambdaFunctionName(integration.URI)
	respBytes, _, invokeErr := h.lambda.InvokeFunction(context.Background(), lambdaFn, "RequestResponse", payload)

	return lambdaTestOutput(respBytes, invokeErr), nil
}

// lambdaTestOutput converts a raw Lambda invocation result into a TestInvokeMethodOutput.
// Any invocation error is surfaced as a 502 response body rather than a Go error,
// because TestInvokeMethod always returns a (possibly error-body) output, never a Go error.
func lambdaTestOutput(respBytes []byte, invokeErr error) *TestInvokeMethodOutput {
	if invokeErr != nil {
		return &TestInvokeMethodOutput{
			Status:  http.StatusBadGateway,
			Body:    `{"message":"Lambda invocation failed"}`,
			Latency: 1,
			Log:     "Test invocation: Lambda error: " + invokeErr.Error(),
			Headers: map[string]string{headerContentType: contentTypeJSON},
		}
	}

	var lambdaResp LambdaProxyResponse
	if json.Unmarshal(respBytes, &lambdaResp) == nil {
		sc := lambdaResp.StatusCode
		if sc == 0 {
			sc = http.StatusOK
		}

		hdrs := lambdaResp.Headers
		if hdrs == nil {
			hdrs = map[string]string{headerContentType: contentTypeJSON}
		}

		return &TestInvokeMethodOutput{
			Status:  sc,
			Body:    lambdaResp.Body,
			Latency: 1,
			Log:     "Test invocation: AWS_PROXY Lambda integration",
			Headers: hdrs,
		}
	}

	return &TestInvokeMethodOutput{
		Status:  http.StatusOK,
		Body:    string(respBytes),
		Latency: 1,
		Log:     "Test invocation: Lambda raw response",
		Headers: map[string]string{headerContentType: contentTypeJSON},
	}
}

func (h *Handler) restAPIActions() map[string]actionFn {
	return map[string]actionFn{
		opCreateRestAPI: func(b []byte) (int, any, error) {
			var input createRestAPIInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			api, err := h.Backend.CreateRestAPI(input)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusCreated, api, nil
		},
		opDeleteRestAPI: func(b []byte) (int, any, error) {
			var input deleteRestAPIInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			if err := h.Backend.DeleteRestAPI(input.RestAPIID); err != nil {
				return 0, nil, err
			}

			return http.StatusAccepted, map[string]any{}, nil
		},
		opGetRestAPI: func(b []byte) (int, any, error) {
			var input getRestAPIInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			api, err := h.Backend.GetRestAPI(input.RestAPIID)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, api, nil
		},
		opGetRestApis: func(b []byte) (int, any, error) {
			var input getRestApisInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			apis, position, err := h.Backend.GetRestAPIs(input.Limit, input.Position)
			if err != nil {
				return 0, nil, err
			}
			if position != "" {
				return http.StatusOK, map[string]any{keyItem: apis, keyPosition: position}, nil
			}

			return http.StatusOK, map[string]any{keyItem: apis}, nil
		},
	}
}

func (h *Handler) resourceActions() map[string]actionFn {
	return map[string]actionFn{
		opGetResources: func(b []byte) (int, any, error) {
			var input getResourcesInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			resources, position, err := h.Backend.GetResources(input.RestAPIID, input.Position, input.Limit)
			if err != nil {
				return 0, nil, err
			}
			if position != "" {
				return http.StatusOK, map[string]any{keyItem: resources, keyPosition: position}, nil
			}

			return http.StatusOK, map[string]any{keyItem: resources}, nil
		},
		opGetResource: func(b []byte) (int, any, error) {
			var input getResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			r, err := h.Backend.GetResource(input.RestAPIID, input.ResourceID)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, r, nil
		},
		opCreateResource: func(b []byte) (int, any, error) {
			var input createResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			r, err := h.Backend.CreateResource(input.RestAPIID, input.ParentID, input.PathPart)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusCreated, r, nil
		},
		opDeleteResource: func(b []byte) (int, any, error) {
			var input deleteResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			if err := h.Backend.DeleteResource(input.RestAPIID, input.ResourceID); err != nil {
				return 0, nil, err
			}

			return http.StatusNoContent, map[string]any{}, nil
		},
	}
}

func (h *Handler) methodActions() map[string]actionFn {
	return map[string]actionFn{
		opPutMethod: func(b []byte) (int, any, error) {
			var input putMethodInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			switch input.AuthorizationType {
			case AuthTypeNone, AuthTypeAWSIAM, AuthTypeCustom, AuthTypeCognitoUserPool:
			default:
				return 0, nil, fmt.Errorf(
					"%w: invalid authorizationType %q; must be NONE, AWS_IAM, CUSTOM, or COGNITO_USER_POOLS",
					ErrInvalidParameter, input.AuthorizationType,
				)
			}
			if (input.AuthorizationType == AuthTypeCustom || input.AuthorizationType == AuthTypeCognitoUserPool) &&
				input.AuthorizerID == "" {
				return 0, nil, fmt.Errorf(
					"%w: authorizerId is required when authorizationType is %s",
					ErrInvalidParameter, input.AuthorizationType,
				)
			}
			m, err := h.Backend.PutMethod(PutMethodInput(input))
			if err != nil {
				return 0, nil, err
			}

			return http.StatusCreated, m, nil
		},
		opGetMethod: func(b []byte) (int, any, error) {
			var input getMethodInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			m, err := h.Backend.GetMethod(input.RestAPIID, input.ResourceID, input.HTTPMethod)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, m, nil
		},
		opDeleteMethod: func(b []byte) (int, any, error) {
			var input deleteMethodInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			if err := h.Backend.DeleteMethod(input.RestAPIID, input.ResourceID, input.HTTPMethod); err != nil {
				return 0, nil, err
			}

			return http.StatusNoContent, map[string]any{}, nil
		},
	}
}

//nolint:dupl // methodResponseActions and integrationResponseActions have similar structure by design
func (h *Handler) methodResponseActions() map[string]actionFn {
	return map[string]actionFn{
		opPutMethodResponse: func(b []byte) (int, any, error) {
			var input putMethodResponseInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			mr, err := h.Backend.PutMethodResponse(
				input.RestAPIID,
				input.ResourceID,
				input.HTTPMethod,
				input.StatusCode,
				input.PutMethodResponseInput,
			)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusCreated, mr, nil
		},
		opGetMethodResponse: func(b []byte) (int, any, error) {
			var input getMethodResponseInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			mr, err := h.Backend.GetMethodResponse(
				input.RestAPIID,
				input.ResourceID,
				input.HTTPMethod,
				input.StatusCode,
			)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, mr, nil
		},
		opDeleteMethodResponse: func(b []byte) (int, any, error) {
			var input deleteMethodResponseInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			if err := h.Backend.DeleteMethodResponse(
				input.RestAPIID,
				input.ResourceID,
				input.HTTPMethod,
				input.StatusCode,
			); err != nil {
				return 0, nil, err
			}

			return http.StatusNoContent, map[string]any{}, nil
		},
	}
}

//nolint:dupl // integrationResponseActions and methodResponseActions have similar structure by design
func (h *Handler) integrationResponseActions() map[string]actionFn {
	return map[string]actionFn{
		opPutIntegrationResponse: func(b []byte) (int, any, error) {
			var input putIntegrationResponseInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			ir, err := h.Backend.PutIntegrationResponse(
				input.RestAPIID,
				input.ResourceID,
				input.HTTPMethod,
				input.StatusCode,
				input.PutIntegrationResponseInput,
			)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusCreated, ir, nil
		},
		opGetIntegrationResponse: func(b []byte) (int, any, error) {
			var input getIntegrationResponseInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			ir, err := h.Backend.GetIntegrationResponse(
				input.RestAPIID,
				input.ResourceID,
				input.HTTPMethod,
				input.StatusCode,
			)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, ir, nil
		},
		opDeleteIntegrationResponse: func(b []byte) (int, any, error) {
			var input deleteIntegrationResponseInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			if err := h.Backend.DeleteIntegrationResponse(
				input.RestAPIID,
				input.ResourceID,
				input.HTTPMethod,
				input.StatusCode,
			); err != nil {
				return 0, nil, err
			}

			return http.StatusNoContent, map[string]any{}, nil
		},
	}
}

func (h *Handler) authorizerActions() map[string]actionFn {
	return map[string]actionFn{
		opCreateAuthorizer: func(b []byte) (int, any, error) {
			var input createAuthorizerInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			auth, err := h.Backend.CreateAuthorizer(input.RestAPIID, CreateAuthorizerInput{
				Name:                         input.Name,
				Type:                         input.Type,
				AuthorizerURI:                input.AuthorizerURI,
				AuthorizerCredentials:        input.AuthorizerCredentials,
				IdentitySource:               input.IdentitySource,
				IdentityValidationExpression: input.IdentityValidationExpression,
				AuthorizerResultTTLInSeconds: input.AuthorizerResultTTLInSeconds,
				ProviderARNs:                 input.ProviderARNs,
			})
			if err != nil {
				return 0, nil, err
			}

			return http.StatusCreated, auth, nil
		},
		opGetAuthorizer: func(b []byte) (int, any, error) {
			var input getAuthorizerInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			auth, err := h.Backend.GetAuthorizer(input.RestAPIID, input.AuthorizerID)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, auth, nil
		},
		opGetAuthorizers: func(b []byte) (int, any, error) {
			var input getAuthorizersInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			auths, err := h.Backend.GetAuthorizers(input.RestAPIID)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, map[string]any{keyItem: auths}, nil
		},
		opUpdateAuthorizer: func(b []byte) (int, any, error) {
			var input updateAuthorizerInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			auth, err := h.Backend.UpdateAuthorizer(input.RestAPIID, input.AuthorizerID, UpdateAuthorizerInput{
				Name:                         input.Name,
				Type:                         input.Type,
				AuthorizerURI:                input.AuthorizerURI,
				AuthorizerCredentials:        input.AuthorizerCredentials,
				IdentitySource:               input.IdentitySource,
				IdentityValidationExpression: input.IdentityValidationExpression,
				AuthorizerResultTTLInSeconds: input.AuthorizerResultTTLInSeconds,
				ProviderARNs:                 input.ProviderARNs,
			})
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, auth, nil
		},
		opDeleteAuthorizer: func(b []byte) (int, any, error) {
			var input deleteAuthorizerInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			if err := h.Backend.DeleteAuthorizer(input.RestAPIID, input.AuthorizerID); err != nil {
				return 0, nil, err
			}

			return http.StatusNoContent, map[string]any{}, nil
		},
	}
}

func (h *Handler) requestValidatorActions() map[string]actionFn {
	return map[string]actionFn{
		opCreateRequestValidator: func(b []byte) (int, any, error) {
			var input createRequestValidatorInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			rv, err := h.Backend.CreateRequestValidator(input.RestAPIID, CreateRequestValidatorInput{
				Name:                      input.Name,
				ValidateRequestBody:       input.ValidateRequestBody,
				ValidateRequestParameters: input.ValidateRequestParameters,
			})
			if err != nil {
				return 0, nil, err
			}

			return http.StatusCreated, rv, nil
		},
		opGetRequestValidator: func(b []byte) (int, any, error) {
			var input getRequestValidatorInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			rv, err := h.Backend.GetRequestValidator(input.RestAPIID, input.ValidatorID)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, rv, nil
		},
		opGetRequestValidators: func(b []byte) (int, any, error) {
			var input getRequestValidatorsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			rvs, err := h.Backend.GetRequestValidators(input.RestAPIID)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, map[string]any{keyItem: rvs}, nil
		},
		opUpdateRequestValidator: func(b []byte) (int, any, error) {
			var input updateRequestValidatorInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			rv, err := h.Backend.UpdateRequestValidator(
				input.RestAPIID,
				input.ValidatorID,
				input.UpdateRequestValidatorInput,
			)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, rv, nil
		},
		opDeleteRequestValidator: func(b []byte) (int, any, error) {
			var input deleteRequestValidatorInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			if err := h.Backend.DeleteRequestValidator(input.RestAPIID, input.ValidatorID); err != nil {
				return 0, nil, err
			}

			return http.StatusNoContent, map[string]any{}, nil
		},
	}
}

func (h *Handler) integrationActions() map[string]actionFn {
	return map[string]actionFn{
		opPutIntegration: func(b []byte) (int, any, error) {
			var input putIntegrationInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			switch input.Type {
			case IntegrationTypeAWS, IntegrationTypeAWSProxy,
				IntegrationTypeHTTP, IntegrationTypeHTTPProxy, IntegrationTypeMock:
			default:
				return 0, nil, fmt.Errorf(
					"%w: invalid integration type %q; must be AWS, AWS_PROXY, HTTP, HTTP_PROXY, or MOCK",
					ErrInvalidParameter, input.Type,
				)
			}
			integ, err := h.Backend.PutIntegration(
				input.RestAPIID,
				input.ResourceID,
				input.HTTPMethod,
				input.PutIntegrationInput,
			)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusCreated, integ, nil
		},
		opGetIntegration: func(b []byte) (int, any, error) {
			var input getIntegrationInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			integ, err := h.Backend.GetIntegration(input.RestAPIID, input.ResourceID, input.HTTPMethod)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, integ, nil
		},
		opDeleteIntegration: func(b []byte) (int, any, error) {
			var input deleteIntegrationInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			if err := h.Backend.DeleteIntegration(input.RestAPIID, input.ResourceID, input.HTTPMethod); err != nil {
				return 0, nil, err
			}

			return http.StatusNoContent, map[string]any{}, nil
		},
	}
}

// deploymentActions returns the action map for deployment and stage operations.
func (h *Handler) deploymentActions() map[string]actionFn {
	m := make(map[string]actionFn)
	maps.Copy(m, h.deploymentCRUDActions())
	maps.Copy(m, h.stageActions())

	return m
}

// createDeploymentAction handles CreateDeployment incl. inline stage update.
func (h *Handler) createDeploymentAction(b []byte) (int, any, error) {
	var input createDeploymentInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	depl, err := h.Backend.CreateDeployment(input.RestAPIID, input.StageName, input.Description)
	if err != nil {
		return 0, nil, err
	}
	h.applyInlineStageUpdate(input)

	return http.StatusCreated, depl, nil
}

func (h *Handler) applyInlineStageUpdate(input createDeploymentInput) {
	if input.StageName == "" {
		return
	}
	if input.StageDescription == "" && len(input.Variables) == 0 && !input.TracingEnabled {
		return
	}
	stageUpd := UpdateStageInput{
		Description: input.StageDescription,
		Variables:   input.Variables,
	}
	if input.TracingEnabled {
		t := true
		stageUpd.TracingEnabled = &t
	}
	_, _ = h.Backend.UpdateStage(input.RestAPIID, input.StageName, stageUpd)
}

// deploymentCRUDActions returns actions for deployment CRUD operations.
func (h *Handler) deploymentCRUDActions() map[string]actionFn {
	return map[string]actionFn{
		opCreateDeployment: h.createDeploymentAction,
		opGetDeployment: func(b []byte) (int, any, error) {
			var input getDeploymentInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			depl, err := h.Backend.GetDeployment(input.RestAPIID, input.DeploymentID)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, depl, nil
		},
		opGetDeployments: func(b []byte) (int, any, error) {
			var input getDeploymentsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			depls, err := h.Backend.GetDeployments(input.RestAPIID)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, map[string]any{keyItem: depls}, nil
		},
		opDeleteDeployment: func(b []byte) (int, any, error) {
			var input deleteDeploymentInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			if err := h.Backend.DeleteDeployment(input.RestAPIID, input.DeploymentID); err != nil {
				return 0, nil, err
			}

			return http.StatusNoContent, map[string]any{}, nil
		},
	}
}

// stageActions returns actions for stage operations.
func (h *Handler) stageActions() map[string]actionFn {
	return map[string]actionFn{
		opGetStages: func(b []byte) (int, any, error) {
			var input getStagesInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			stages, err := h.Backend.GetStages(input.RestAPIID)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, map[string]any{keyItem: stages}, nil
		},
		opGetStage: func(b []byte) (int, any, error) {
			var input getStageInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			stage, err := h.Backend.GetStage(input.RestAPIID, input.StageName)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, stage, nil
		},
		opDeleteStage: func(b []byte) (int, any, error) {
			var input deleteStageInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			if err := h.Backend.DeleteStage(input.RestAPIID, input.StageName); err != nil {
				return 0, nil, err
			}

			return http.StatusNoContent, map[string]any{}, nil
		},
	}
}

func (h *Handler) dispatchTable() map[string]actionFn {
	table := make(map[string]actionFn)
	maps.Copy(table, h.restAPIActions())
	maps.Copy(table, h.resourceActions())
	maps.Copy(table, h.methodActions())
	maps.Copy(table, h.methodResponseActions())
	maps.Copy(table, h.integrationActions())
	maps.Copy(table, h.integrationResponseActions())
	maps.Copy(table, h.deploymentActions())
	maps.Copy(table, h.authorizerActions())
	maps.Copy(table, h.requestValidatorActions())
	maps.Copy(table, h.newResourceActions())
	maps.Copy(table, h.getDeleteUpdateActions())
	maps.Copy(table, h.updatePatchActions())
	maps.Copy(table, h.stubActions())

	return table
}

// newResourceActions returns actions for creating new top-level resources.
func (h *Handler) newResourceActions() map[string]actionFn {
	m := make(map[string]actionFn)
	maps.Copy(m, h.newResourceActionsCore())
	maps.Copy(m, h.newResourceActionsExt())

	return m
}

// newResourceActionsCore returns actions for API key and usage plan creation.
//
//nolint:dupl // similar structure to sibling handler is unavoidable
func (h *Handler) newResourceActionsCore() map[string]actionFn {
	return map[string]actionFn{
		opCreateAPIKey: func(b []byte) (int, any, error) {
			var input CreateAPIKeyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}

			key, err := h.Backend.CreateAPIKey(input)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusCreated, key, nil
		},
		opCreateBasePathMapping: func(b []byte) (int, any, error) {
			var input CreateBasePathMappingInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}

			bpm, err := h.Backend.CreateBasePathMapping(input)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusCreated, bpm, nil
		},
		opCreateDocumentationPart: func(b []byte) (int, any, error) {
			var input CreateDocumentationPartInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}

			part, err := h.Backend.CreateDocumentationPart(input)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusCreated, part, nil
		},
		opCreateDocumentationVersion: func(b []byte) (int, any, error) {
			var input CreateDocumentationVersionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}

			ver, err := h.Backend.CreateDocumentationVersion(input)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusCreated, ver, nil
		},
	}
}

// newResourceActionsExt returns actions for domain name and usage plan key creation.
// newResourceActionsExt returns actions for domain name and extended resource creation.
func (h *Handler) newResourceActionsExt() map[string]actionFn {
	m := make(map[string]actionFn)
	maps.Copy(m, h.newResourceActionsDomainName())
	maps.Copy(m, h.newResourceActionsModelStage())

	return m
}

// newResourceActionsDomainName returns actions for domain name creation.
//
//nolint:dupl // similar structure to sibling handler is unavoidable
func (h *Handler) newResourceActionsDomainName() map[string]actionFn {
	return map[string]actionFn{
		opCreateDomainName: func(b []byte) (int, any, error) {
			var input CreateDomainNameInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}

			dn, err := h.Backend.CreateDomainName(input)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusCreated, dn, nil
		},
		opCreateDomainNameAccessAssociation: func(b []byte) (int, any, error) {
			var input CreateDomainNameAccessAssociationInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}

			assoc, err := h.Backend.CreateDomainNameAccessAssociation(input)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusCreated, assoc, nil
		},
	}
}

// newResourceActionsModelStage returns actions for model, stage, and usage plan creation.
//
//nolint:dupl // similar structure to sibling handler is unavoidable
func (h *Handler) newResourceActionsModelStage() map[string]actionFn {
	return map[string]actionFn{
		opCreateModel: func(b []byte) (int, any, error) {
			var input CreateModelInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}

			model, err := h.Backend.CreateModel(input)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusCreated, model, nil
		},
		opCreateStage: func(b []byte) (int, any, error) {
			var input CreateStageInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}

			stage, err := h.Backend.CreateStage(input)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusCreated, stage, nil
		},
		opCreateUsagePlan: func(b []byte) (int, any, error) {
			var input CreateUsagePlanInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}

			plan, err := h.Backend.CreateUsagePlan(input)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusCreated, plan, nil
		},
		opCreateUsagePlanKey: func(b []byte) (int, any, error) {
			var input CreateUsagePlanKeyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}

			upk, err := h.Backend.CreateUsagePlanKey(input)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusCreated, upk, nil
		},
	}
}

// dispatch routes the action to the correct handler function.
func (h *Handler) dispatch(_ context.Context, action string, body []byte) (int, []byte, error) {
	fn, ok := h.dispatchTable()[action]
	if !ok {
		return 0, nil, fmt.Errorf("%w:%s", errUnknownOperation, action)
	}

	statusCode, response, err := fn(body)
	if err != nil {
		return 0, nil, err
	}

	encoded, err := json.Marshal(response)
	if err != nil {
		return 0, nil, err
	}

	return statusCode, encoded, nil
}

// handleError writes a standardized JSON error response.
func (h *Handler) handleError(ctx context.Context, c *echo.Context, action string, reqErr error) error {
	log := logger.Load(ctx)
	c.Response().Header().Set(headerContentType, "application/x-amz-json-1.1")

	var errType string
	var statusCode int

	switch {
	case errors.Is(reqErr, ErrRestAPINotFound),
		errors.Is(reqErr, ErrResourceNotFound),
		errors.Is(reqErr, ErrMethodNotFound),
		errors.Is(reqErr, ErrMethodResponseNotFound),
		errors.Is(reqErr, ErrIntegrationResponseNotFound),
		errors.Is(reqErr, ErrDeploymentNotFound),
		errors.Is(reqErr, ErrAuthorizerNotFound),
		errors.Is(reqErr, ErrValidatorNotFound),
		errors.Is(reqErr, ErrAPIKeyNotFound),
		errors.Is(reqErr, ErrBasePathMappingNotFound),
		errors.Is(reqErr, ErrDocumentationPartNotFound),
		errors.Is(reqErr, ErrDocumentationVersionNotFound),
		errors.Is(reqErr, ErrDomainNameNotFound),
		errors.Is(reqErr, ErrDomainNameAccessAssociationNotFound),
		errors.Is(reqErr, ErrModelNotFound),
		errors.Is(reqErr, ErrUsagePlanNotFound),
		errors.Is(reqErr, ErrUsagePlanKeyNotFound),
		errors.Is(reqErr, ErrStageNotFound),
		errors.Is(reqErr, ErrNotFound):
		errType = "NotFoundException"
		statusCode = http.StatusNotFound
	case errors.Is(reqErr, ErrAlreadyExists):
		errType = "ConflictException"
		statusCode = http.StatusConflict
	case errors.Is(reqErr, ErrInvalidParameter):
		errType = "BadRequestException"
		statusCode = http.StatusBadRequest
	case errors.Is(reqErr, errUnknownOperation):
		errType = "UnknownOperationException"
		statusCode = http.StatusBadRequest
	default:
		errType = "InternalServerError"
		statusCode = http.StatusInternalServerError
	}

	if statusCode == http.StatusInternalServerError {
		log.ErrorContext(ctx, "APIGateway internal error", "error", reqErr, "action", action)
	} else {
		log.WarnContext(ctx, "APIGateway request error", "error", reqErr, "action", action)
	}

	errResp := ErrorResponse{
		Type:    errType,
		Message: reqErr.Error(),
	}

	payload, _ := json.Marshal(errResp)

	return c.JSONBlob(statusCode, payload)
}

// Reset clears all in-memory state from the backend. It is used by the
// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
func (h *Handler) Reset() {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		b.Reset()
	}
}

// getDeleteUpdateActions returns the action map for get, delete, and update operations.
func (h *Handler) getDeleteUpdateActions() map[string]actionFn {
	m := make(map[string]actionFn)
	maps.Copy(m, h.getDeleteUpdateActionsCore())
	maps.Copy(m, h.getDeleteUpdateActionsExt())
	maps.Copy(m, h.getDeleteUpdateActionsUsage())

	return m
}

// getDeleteUpdateActionsCore returns get/delete/update actions for REST API core resources.
// getDeleteUpdateActionsCore returns get/delete/update actions for core REST API resources.
func (h *Handler) getDeleteUpdateActionsCore() map[string]actionFn {
	m := make(map[string]actionFn)
	maps.Copy(m, h.getDeleteUpdateActionsCore1())
	maps.Copy(m, h.getDeleteUpdateActionsCore2())

	return m
}

// getDeleteUpdateActionsCore1 returns actions for API key and REST API get/delete.
func (h *Handler) getDeleteUpdateActionsCore1() map[string]actionFn {
	m := make(map[string]actionFn)
	maps.Copy(m, h.getDeleteUpdateActionsCore1a())
	maps.Copy(m, h.getDeleteUpdateActionsCore1b())

	return m
}

func (h *Handler) getAPIKeyAction(b []byte) (int, any, error) {
	var input getAPIKeyInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	key, err := h.Backend.GetAPIKey(input.APIKeyID)
	if err != nil {
		return 0, nil, err
	}
	if input.IncludeValue != litTrue {
		key.Value = ""
	}

	return http.StatusOK, key, nil
}

func (h *Handler) getAPIKeysAction(b []byte) (int, any, error) {
	var input getAPIKeysPageInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	keys, position, err := h.fetchAPIKeys(input)
	if err != nil {
		return 0, nil, err
	}
	if input.IncludeValue != litTrue {
		for i := range keys {
			keys[i].Value = ""
		}
	}
	if position != "" {
		return http.StatusOK, map[string]any{keyItem: keys, keyPosition: position}, nil
	}

	return http.StatusOK, map[string]any{keyItem: keys}, nil
}

func (h *Handler) fetchAPIKeys(input getAPIKeysPageInput) ([]APIKey, string, error) {
	if input.Limit == 0 && input.Position == "" {
		keys, err := h.Backend.GetAPIKeys()

		return keys, "", err
	}

	return h.Backend.GetAPIKeysPage(input.Limit, input.Position)
}

func (h *Handler) deleteAPIKeyAction(b []byte) (int, any, error) {
	var input deleteAPIKeyInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	if err := h.Backend.DeleteAPIKey(input.APIKeyID); err != nil {
		return 0, nil, err
	}

	return http.StatusAccepted, map[string]any{}, nil
}

func (h *Handler) getDeleteUpdateActionsCore1a() map[string]actionFn {
	return map[string]actionFn{
		opGetAPIKey:    h.getAPIKeyAction,
		opGetAPIKeys:   h.getAPIKeysAction,
		opDeleteAPIKey: h.deleteAPIKeyAction,
	}
}

func (h *Handler) getDeleteUpdateActionsCore1b() map[string]actionFn {
	return map[string]actionFn{
		opUpdateAPIKey: func(b []byte) (int, any, error) {
			var input updateAPIKeyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			key, err := h.Backend.UpdateAPIKey(input.APIKeyID, input.UpdateAPIKeyInput)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, key, nil
		},
	}
}

func (h *Handler) getDeleteUpdateActionsCore2() map[string]actionFn {
	m := make(map[string]actionFn)
	maps.Copy(m, h.getDeleteUpdateActionsCore2a())
	maps.Copy(m, h.getDeleteUpdateActionsCore2b())

	return m
}

func (h *Handler) getDeleteUpdateActionsCore2a() map[string]actionFn {
	return map[string]actionFn{
		opGetDomainName: func(b []byte) (int, any, error) {
			var input getDomainNameInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			dn, err := h.Backend.GetDomainName(input.DomainName)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, dn, nil
		},
		opGetDomainNames: func(b []byte) (int, any, error) {
			var input getDomainNamesPageInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			if input.Limit == 0 && input.Position == "" {
				dns, err := h.Backend.GetDomainNames()
				if err != nil {
					return 0, nil, err
				}

				return http.StatusOK, map[string]any{keyItem: dns}, nil
			}
			dns, position, err := h.Backend.GetDomainNamesPage(input.Limit, input.Position)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, map[string]any{keyItem: dns, keyPosition: position}, nil
		},
	}
}

func (h *Handler) getDeleteUpdateActionsCore2b() map[string]actionFn {
	return map[string]actionFn{
		opDeleteDomainName: func(b []byte) (int, any, error) {
			var input deleteDomainNameInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			if err := h.Backend.DeleteDomainName(input.DomainName); err != nil {
				return 0, nil, err
			}

			return http.StatusAccepted, map[string]any{}, nil
		},
		opGetBasePathMapping: func(b []byte) (int, any, error) {
			var input getBasePathMappingInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			bpm, err := h.Backend.GetBasePathMapping(input.DomainName, input.BasePath)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, bpm, nil
		},
		opGetBasePathMappings: func(b []byte) (int, any, error) {
			var input getBasePathMappingsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			bpms, err := h.Backend.GetBasePathMappings(input.DomainName)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, map[string]any{keyItem: bpms}, nil
		},
	}
}

// getDeleteUpdateActionsExt returns get/delete/update actions for domain names and base path mappings.
// getDeleteUpdateActionsExt returns get/delete/update actions for domain names and base path mappings.
func (h *Handler) getDeleteUpdateActionsExt() map[string]actionFn {
	m := make(map[string]actionFn)
	maps.Copy(m, h.getDeleteUpdateActionsExt1())
	maps.Copy(m, h.getDeleteUpdateActionsExt2())

	return m
}

//nolint:dupl // similar structure to sibling handler is unavoidable
func (h *Handler) getDeleteUpdateActionsExt1() map[string]actionFn {
	return map[string]actionFn{
		opDeleteBasePathMapping: func(b []byte) (int, any, error) {
			var input deleteBasePathMappingInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			if err := h.Backend.DeleteBasePathMapping(input.DomainName, input.BasePath); err != nil {
				return 0, nil, err
			}

			return http.StatusAccepted, map[string]any{}, nil
		},
		opGetModel: func(b []byte) (int, any, error) {
			var input getModelInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			m, err := h.Backend.GetModel(input.RestAPIID, input.ModelName)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, m, nil
		},
		opGetModels: func(b []byte) (int, any, error) {
			var input getModelsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			ms, err := h.Backend.GetModels(input.RestAPIID)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, map[string]any{keyItem: ms}, nil
		},
		opDeleteModel: func(b []byte) (int, any, error) {
			var input deleteModelInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			if err := h.Backend.DeleteModel(input.RestAPIID, input.ModelName); err != nil {
				return 0, nil, err
			}

			return http.StatusAccepted, map[string]any{}, nil
		},
	}
}

func (h *Handler) getDeleteUpdateActionsExt2() map[string]actionFn {
	m := make(map[string]actionFn)
	maps.Copy(m, h.getDeleteUpdateActionsExt2a())
	maps.Copy(m, h.getDeleteUpdateActionsExt2b())

	return m
}

func (h *Handler) getDeleteUpdateActionsExt2a() map[string]actionFn {
	return map[string]actionFn{
		opUpdateModel: func(b []byte) (int, any, error) {
			var input updateModelInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			m, err := h.Backend.UpdateModel(input.RestAPIID, input.ModelName, input.UpdateModelInput)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, m, nil
		},
		opUpdateStage: func(b []byte) (int, any, error) {
			var input updateStageHandlerInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			stage, err := h.Backend.UpdateStage(input.RestAPIID, input.StageName, input.UpdateStageInput)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, stage, nil
		},
	}
}

func (h *Handler) getDeleteUpdateActionsExt2b() map[string]actionFn {
	return map[string]actionFn{
		opFlushStageCache: func(b []byte) (int, any, error) {
			var input flushStageCacheInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			if _, err := h.Backend.GetStage(input.RestAPIID, input.StageName); err != nil {
				return 0, nil, err
			}

			return http.StatusAccepted, map[string]any{}, nil
		},
		opFlushStageAuthorizersCache: func(b []byte) (int, any, error) {
			var input flushStageCacheInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			h.authCache.flush()

			return http.StatusAccepted, map[string]any{}, nil
		},
		opGetUsagePlan: func(b []byte) (int, any, error) {
			var input getUsagePlanInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			p, err := h.Backend.GetUsagePlan(input.UsagePlanID)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, p, nil
		},
		opGetUsagePlans: func(b []byte) (int, any, error) {
			var input getUsagePlansPageInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			if input.Limit == 0 && input.Position == "" {
				ps, err := h.Backend.GetUsagePlans()
				if err != nil {
					return 0, nil, err
				}

				return http.StatusOK, map[string]any{keyItem: ps}, nil
			}
			ps, position, err := h.Backend.GetUsagePlansPage(input.Limit, input.Position)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, map[string]any{keyItem: ps, keyPosition: position}, nil
		},
	}
}

// getDeleteUpdateActionsUsage returns get/delete/update actions for usage plans and keys.
// getDeleteUpdateActionsUsage returns get/delete/update actions for usage plans, keys, and documentation.
func (h *Handler) getDeleteUpdateActionsUsage() map[string]actionFn {
	m := make(map[string]actionFn)
	maps.Copy(m, h.getDeleteUpdateActionsUsage1())
	maps.Copy(m, h.getDeleteUpdateActionsUsage2())

	return m
}

func (h *Handler) getDeleteUpdateActionsUsage1() map[string]actionFn {
	return map[string]actionFn{
		opDeleteUsagePlan: func(b []byte) (int, any, error) {
			var input deleteUsagePlanInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			if err := h.Backend.DeleteUsagePlan(input.UsagePlanID); err != nil {
				return 0, nil, err
			}

			return http.StatusAccepted, map[string]any{}, nil
		},
		opGetUsagePlanKey: func(b []byte) (int, any, error) {
			var input getUsagePlanKeyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			k, err := h.Backend.GetUsagePlanKey(input.UsagePlanID, input.KeyID)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, k, nil
		},
		opGetUsagePlanKeys: func(b []byte) (int, any, error) {
			var input getUsagePlanKeysInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			ks, err := h.Backend.GetUsagePlanKeys(input.UsagePlanID)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, map[string]any{keyItem: ks}, nil
		},
		opDeleteUsagePlanKey: func(b []byte) (int, any, error) {
			var input deleteUsagePlanKeyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			if err := h.Backend.DeleteUsagePlanKey(input.UsagePlanID, input.KeyID); err != nil {
				return 0, nil, err
			}

			return http.StatusAccepted, map[string]any{}, nil
		},
	}
}

func (h *Handler) getDeleteUpdateActionsUsage2() map[string]actionFn {
	m := make(map[string]actionFn)
	maps.Copy(m, h.getDeleteUpdateActionsUsage2a())
	maps.Copy(m, h.getDeleteUpdateActionsUsage2b())

	return m
}

func (h *Handler) getDeleteUpdateActionsUsage2a() map[string]actionFn {
	return map[string]actionFn{
		opGetDocumentationPart: func(b []byte) (int, any, error) {
			var input getDocumentationPartInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			p, err := h.Backend.GetDocumentationPart(input.RestAPIID, input.DocPartID)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, p, nil
		},
		opGetDocumentationParts: func(b []byte) (int, any, error) {
			var input getDocumentationPartsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			ps, err := h.Backend.GetDocumentationParts(input.RestAPIID)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, map[string]any{keyItem: ps}, nil
		},
	}
}

//nolint:dupl // similar structure to sibling handler is unavoidable
func (h *Handler) getDeleteUpdateActionsUsage2b() map[string]actionFn {
	return map[string]actionFn{
		opDeleteDocumentationPart: func(b []byte) (int, any, error) {
			var input deleteDocumentationPartInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			if err := h.Backend.DeleteDocumentationPart(input.RestAPIID, input.DocPartID); err != nil {
				return 0, nil, err
			}

			return http.StatusNoContent, map[string]any{}, nil
		},
		opGetDocumentationVersion: func(b []byte) (int, any, error) {
			var input getDocumentationVersionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			v, err := h.Backend.GetDocumentationVersion(input.RestAPIID, input.DocVersion)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, v, nil
		},
		opGetDocumentationVersions: func(b []byte) (int, any, error) {
			var input getDocumentationVersionsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			vs, err := h.Backend.GetDocumentationVersions(input.RestAPIID)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, map[string]any{keyItem: vs}, nil
		},
		opDeleteDocumentationVersion: func(b []byte) (int, any, error) {
			var input deleteDocumentationVersionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			if err := h.Backend.DeleteDocumentationVersion(input.RestAPIID, input.DocVersion); err != nil {
				return 0, nil, err
			}

			return http.StatusNoContent, map[string]any{}, nil
		},
	}
}

// updatePatchActions returns the action map for update and patch operations.
func (h *Handler) updatePatchActions() map[string]actionFn {
	m := make(map[string]actionFn)
	maps.Copy(m, h.updatePatchActionsCore())
	maps.Copy(m, h.updatePatchActionsDomain())
	maps.Copy(m, h.updatePatchActionsMisc())

	return m
}

// updatePatchActionsCore returns update/patch actions for core REST API resources.
// updatePatchActionsCore returns update/patch actions for core REST API resources.
func (h *Handler) updatePatchActionsCore() map[string]actionFn {
	m := make(map[string]actionFn)
	maps.Copy(m, h.updatePatchActionsCore1())
	maps.Copy(m, h.updatePatchActionsCore2())

	return m
}

func (h *Handler) updatePatchActionsCore1() map[string]actionFn {
	return map[string]actionFn{
		opUpdateRestAPI: func(b []byte) (int, any, error) {
			var input updateRestAPIHandlerInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			api, err := h.Backend.UpdateRestAPI(input.RestAPIID, input.UpdateRestAPIInput)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, api, nil
		},
		opUpdateDeployment: func(b []byte) (int, any, error) {
			var input updateDeploymentHandlerInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			depl, err := h.Backend.UpdateDeployment(input.RestAPIID, input.DeploymentID, input.UpdateDeploymentInput)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, depl, nil
		},
		opUpdateResource: func(b []byte) (int, any, error) {
			var input updateResourceHandlerInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			res, err := h.Backend.UpdateResource(input.RestAPIID, input.ResourceID, input.UpdateResourceInput)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, res, nil
		},
		opGetAccount: func(_ []byte) (int, any, error) {
			acct, err := h.Backend.GetAccount()
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, acct, nil
		},
	}
}

func (h *Handler) updatePatchActionsCore2() map[string]actionFn {
	return map[string]actionFn{
		opGetTags: func(b []byte) (int, any, error) {
			var input getTagsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			t, err := h.Backend.GetResourceTags(input.ResourceARN)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, map[string]any{"tags": t}, nil
		},
		opTagResource: func(b []byte) (int, any, error) {
			var input tagResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			if err := h.Backend.TagResource(input.ResourceARN, input.Tags); err != nil {
				return 0, nil, err
			}

			return http.StatusNoContent, map[string]any{}, nil
		},
		opUntagResource: func(b []byte) (int, any, error) {
			var input untagResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			if err := h.Backend.UntagResource(input.ResourceARN, input.TagKeys); err != nil {
				return 0, nil, err
			}

			return http.StatusNoContent, map[string]any{}, nil
		},
		opTestInvokeMethod: func(b []byte) (int, any, error) {
			var input testInvokeMethodHandlerInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			out, err := h.testInvokeMethod(input.TestInvokeMethodInput)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, out, nil
		},
		opUpdateUsagePlan: func(b []byte) (int, any, error) {
			var input UpdateUsagePlanInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			out, err := h.Backend.UpdateUsagePlan(input)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, out, nil
		},
	}
}

// updatePatchActionsDomain returns update/patch actions for domain names and related resources.
// updatePatchActionsDomain returns update/patch actions for domain names and related resources.
func (h *Handler) updatePatchActionsDomain() map[string]actionFn {
	m := make(map[string]actionFn)
	maps.Copy(m, h.updatePatchActionsDomain1())
	maps.Copy(m, h.updatePatchActionsDomain2())

	return m
}

//nolint:dupl // similar structure to sibling handler is unavoidable
func (h *Handler) updatePatchActionsDomain1() map[string]actionFn {
	return map[string]actionFn{
		opUpdateDomainName: func(b []byte) (int, any, error) {
			var input UpdateDomainNameInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			out, err := h.Backend.UpdateDomainName(input)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, out, nil
		},
		opUpdateBasePathMapping: func(b []byte) (int, any, error) {
			var input UpdateBasePathMappingInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			out, err := h.Backend.UpdateBasePathMapping(input)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, out, nil
		},
		opUpdateDocumentationPart: func(b []byte) (int, any, error) {
			var input UpdateDocumentationPartInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			out, err := h.Backend.UpdateDocumentationPart(input)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, out, nil
		},
		opUpdateDocumentationVersion: func(b []byte) (int, any, error) {
			var input UpdateDocumentationVersionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			out, err := h.Backend.UpdateDocumentationVersion(input)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, out, nil
		},
	}
}

func (h *Handler) updatePatchActionsDomain2() map[string]actionFn {
	m := make(map[string]actionFn)
	maps.Copy(m, h.updatePatchActionsDomain2a())
	maps.Copy(m, h.updatePatchActionsDomain2b())

	return m
}

//nolint:dupl // similar structure to sibling handler is unavoidable
func (h *Handler) updatePatchActionsDomain2a() map[string]actionFn {
	return map[string]actionFn{
		opUpdateMethod: func(b []byte) (int, any, error) {
			var input UpdateMethodInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			out, err := h.Backend.UpdateMethod(input)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, out, nil
		},
		opUpdateIntegration: func(b []byte) (int, any, error) {
			var input UpdateIntegrationInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			out, err := h.Backend.UpdateIntegration(input)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, out, nil
		},
	}
}

//nolint:dupl // similar structure to sibling handler is unavoidable
func (h *Handler) updatePatchActionsDomain2b() map[string]actionFn {
	return map[string]actionFn{
		opUpdateIntegrationResponse: func(b []byte) (int, any, error) {
			var input UpdateIntegrationResponseInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			out, err := h.Backend.UpdateIntegrationResponse(input)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, out, nil
		},
		opUpdateMethodResponse: func(b []byte) (int, any, error) {
			var input UpdateMethodResponseInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			out, err := h.Backend.UpdateMethodResponse(input)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, out, nil
		},
		opUpdateAccount: func(b []byte) (int, any, error) {
			var input UpdateAccountInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			out, err := h.Backend.UpdateAccount(input)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, out, nil
		},
		opTestInvokeAuthorizer: func(b []byte) (int, any, error) {
			var input TestInvokeAuthorizerInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			out, err := h.Backend.TestInvokeAuthorizer(input)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, out, nil
		},
	}
}

// updatePatchActionsMisc returns update/patch actions for models, usage plans, and other resources.
// updatePatchActionsMisc returns update/patch actions for models, usage plans, and other resources.
func (h *Handler) updatePatchActionsMisc() map[string]actionFn {
	m := make(map[string]actionFn)
	maps.Copy(m, h.updatePatchActionsMisc1())
	maps.Copy(m, h.updatePatchActionsMisc2())

	return m
}

func (h *Handler) updatePatchActionsMisc1() map[string]actionFn {
	return map[string]actionFn{
		opGetModelTemplate: func(b []byte) (int, any, error) {
			var params struct {
				RestAPIID string `json:"restApiId"`
				ModelName string `json:"modelName"`
			}
			if err := json.Unmarshal(b, &params); err != nil {
				return 0, nil, err
			}
			out, err := h.Backend.GetModelTemplate(params.RestAPIID, params.ModelName)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, map[string]any{"value": out}, nil
		},
		opGetGatewayResponse: func(b []byte) (int, any, error) {
			var params struct {
				RestAPIID    string `json:"restApiId"`
				ResponseType string `json:"responseType"`
			}
			if err := json.Unmarshal(b, &params); err != nil {
				return 0, nil, err
			}
			out, err := h.Backend.GetGatewayResponse(params.RestAPIID, params.ResponseType)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, out, nil
		},
		opGetGatewayResponses: func(b []byte) (int, any, error) {
			var params struct {
				RestAPIID string `json:"restApiId"`
			}
			if err := json.Unmarshal(b, &params); err != nil {
				return 0, nil, err
			}
			out, err := h.Backend.GetGatewayResponses(params.RestAPIID)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, map[string]any{keyItem: out}, nil
		},
		opPutGatewayResponse: func(b []byte) (int, any, error) {
			var input PutGatewayResponseInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			out, err := h.Backend.PutGatewayResponse(input)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusCreated, out, nil
		},
		opDeleteGatewayResponse: func(b []byte) (int, any, error) {
			var params struct {
				RestAPIID    string `json:"restApiId"`
				ResponseType string `json:"responseType"`
			}
			if err := json.Unmarshal(b, &params); err != nil {
				return 0, nil, err
			}
			if err := h.Backend.DeleteGatewayResponse(params.RestAPIID, params.ResponseType); err != nil {
				return 0, nil, err
			}

			return http.StatusNoContent, nil, nil
		},
	}
}

func (h *Handler) updatePatchActionsMisc2() map[string]actionFn {
	return map[string]actionFn{
		opGenerateClientCertificate: func(b []byte) (int, any, error) {
			var input GenerateClientCertificateInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			out, err := h.Backend.GenerateClientCertificate(input)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusCreated, out, nil
		},
		opGetClientCertificate: func(b []byte) (int, any, error) {
			var params struct {
				ClientCertificateID string `json:"clientCertificateId"`
			}
			if err := json.Unmarshal(b, &params); err != nil {
				return 0, nil, err
			}
			out, err := h.Backend.GetClientCertificate(params.ClientCertificateID)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, out, nil
		},
		opGetClientCertificates: func(_ []byte) (int, any, error) {
			out, err := h.Backend.GetClientCertificates()
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, map[string]any{keyItem: out}, nil
		},
		opDeleteClientCertificate: func(b []byte) (int, any, error) {
			var params struct {
				ClientCertificateID string `json:"clientCertificateId"`
			}
			if err := json.Unmarshal(b, &params); err != nil {
				return 0, nil, err
			}
			if err := h.Backend.DeleteClientCertificate(params.ClientCertificateID); err != nil {
				return 0, nil, err
			}

			return http.StatusNoContent, nil, nil
		},
		opGetUsage: func(b []byte) (int, any, error) {
			var input GetUsageInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			out, err := h.Backend.GetUsage(input)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, out, nil
		},
	}
}
