package apigateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

var errUnknownOperation = errors.New("UnknownOperationException")

// path segment constants used in REST route matching.
const (
	apiGWUnknownOp             = "Unknown"
	apiGWSegResources          = "resources"
	apiGWSegDeployment         = "deployments"
	apiGWSegStages             = "stages"
	apiGWSegMethods            = "methods"
	apiGWSegInteg              = "integration"
	apiGWSegResponses          = "responses"
	apiGWSegAuthorizers        = "authorizers"
	apiGWSegValidators         = "requestvalidators"
	apiGWSegAPIKeys            = "apikeys"
	apiGWSegDomainNames        = "domainnames"
	apiGWSegBasePathMappings   = "basepathmappings"
	apiGWSegAccessAssociations = "accessassociations"
	apiGWSegDocumentation      = "documentation"
	apiGWSegDocParts           = "parts"
	apiGWSegDocVersions        = "versions"
	apiGWSegModels             = "models"
	apiGWSegUsagePlans         = "usageplans"
	apiGWSegUsagePlanKeys      = "keys"
	apiGWSegGatewayResponses   = "gatewayresponses"
	apiGWSegClientCerts        = "clientcertificates"

	// apiGWMinTagPathSegs is the minimum number of path segments for a /tags/{arn} path.
	apiGWMinTagPathSegs = 2
)

type createRestAPIInput struct {
	Tags        *tags.Tags `json:"tags"`
	Name        string     `json:"name"`
	Description string     `json:"description"`
}

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
	RestAPIID          string `json:"restApiId"`
	ResourceID         string `json:"resourceId"`
	HTTPMethod         string `json:"httpMethod"`
	AuthorizationType  string `json:"authorizationType"`
	AuthorizerID       string `json:"authorizerId"`
	RequestValidatorID string `json:"requestValidatorId"`
	APIKeyRequired     bool   `json:"apiKeyRequired"`
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
	PutIntegrationInput

	RestAPIID  string `json:"restApiId"`
	ResourceID string `json:"resourceId"`
	HTTPMethod string `json:"httpMethod"`
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
	RestAPIID   string `json:"restApiId"`
	StageName   string `json:"stageName"`
	Description string `json:"description"`
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
	APIKeyID string `json:"apiKeyId"`
}

type getAPIKeysPageInput struct {
	Position string `json:"position"`
	Limit    int    `json:"limit"`
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
	UpdateRestAPIInput
	RestAPIID string `json:"restApiId"`
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

// Handler is the Echo HTTP service handler for API Gateway operations.
type Handler struct {
	Backend    StorageBackend
	authCache  *authorizerCache
	lambda     LambdaInvoker
	httpClient *http.Client
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
		"CreateRestApi",
		"DeleteRestApi",
		"GetRestApi",
		"GetRestApis",
		"GetResources",
		"GetResource",
		"CreateResource",
		"DeleteResource",
		"PutMethod",
		"GetMethod",
		"DeleteMethod",
		"PutMethodResponse",
		"GetMethodResponse",
		"DeleteMethodResponse",
		"PutIntegration",
		"GetIntegration",
		"DeleteIntegration",
		"PutIntegrationResponse",
		"GetIntegrationResponse",
		"DeleteIntegrationResponse",
		"CreateDeployment",
		"GetDeployment",
		"GetDeployments",
		"DeleteDeployment",
		"GetStages",
		"GetStage",
		"DeleteStage",
		"CreateAuthorizer",
		"GetAuthorizer",
		"GetAuthorizers",
		"UpdateAuthorizer",
		"DeleteAuthorizer",
		"CreateRequestValidator",
		"GetRequestValidator",
		"GetRequestValidators",
		"UpdateRequestValidator",
		"DeleteRequestValidator",
		"CreateApiKey",
		"GetApiKey",
		"GetApiKeys",
		"DeleteApiKey",
		"UpdateApiKey",
		"CreateBasePathMapping",
		"GetBasePathMapping",
		"GetBasePathMappings",
		"DeleteBasePathMapping",
		"CreateDocumentationPart",
		"GetDocumentationPart",
		"GetDocumentationParts",
		"DeleteDocumentationPart",
		"CreateDocumentationVersion",
		"GetDocumentationVersion",
		"GetDocumentationVersions",
		"DeleteDocumentationVersion",
		"CreateDomainName",
		"GetDomainName",
		"GetDomainNames",
		"DeleteDomainName",
		"CreateDomainNameAccessAssociation",
		"CreateModel",
		"GetModel",
		"GetModels",
		"DeleteModel",
		"UpdateModel",
		"CreateStage",
		"UpdateStage",
		"FlushStageCache",
		"FlushStageAuthorizersCache",
		"CreateUsagePlan",
		"GetUsagePlan",
		"GetUsagePlans",
		"DeleteUsagePlan",
		"CreateUsagePlanKey",
		"GetUsagePlanKey",
		"GetUsagePlanKeys",
		"DeleteUsagePlanKey",
		"UpdateRestApi",
		"UpdateDeployment",
		"UpdateResource",
		"GetAccount",
		"GetTags",
		"TagResource",
		"UntagResource",
		"TestInvokeMethod",
		"UpdateUsagePlan",
		"UpdateDomainName",
		"UpdateBasePathMapping",
		"UpdateDocumentationPart",
		"UpdateDocumentationVersion",
		"UpdateMethod",
		"UpdateIntegration",
		"UpdateIntegrationResponse",
		"UpdateMethodResponse",
		"UpdateAccount",
		"TestInvokeAuthorizer",
		"GetModelTemplate",
		"GetGatewayResponse",
		"GetGatewayResponses",
		"PutGatewayResponse",
		"DeleteGatewayResponse",
		"GenerateClientCertificate",
		"GetClientCertificate",
		"GetClientCertificates",
		"DeleteClientCertificate",
		"GetUsage",
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
			strings.HasPrefix(path, "/usageplans") ||
			strings.HasPrefix(path, "/account") ||
			strings.HasPrefix(path, "/"+apiGWSegClientCerts) {
			return true
		}

		// /tags/{arn} — only claim this path when the ARN is an API Gateway resource.
		// API Gateway ARNs contain ":apigateway:" (e.g. arn:aws:apigateway:us-east-1::/restapis/xyz).
		if after, ok := strings.CutPrefix(path, "/tags/"); ok {
			rest := after

			return strings.Contains(rest, ":apigateway:")
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

	for _, key := range []string{"restApiId", "name"} {
		if v, ok := data[key].(string); ok && v != "" {
			return v
		}
	}

	return ""
}

// Handler returns the Echo handler function for API Gateway requests.
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
		isRESTPath := strings.HasPrefix(path, "/restapis") ||
			strings.HasPrefix(path, "/apikeys") ||
			strings.HasPrefix(path, "/domainnames") ||
			strings.HasPrefix(path, "/usageplans") ||
			strings.HasPrefix(path, "/account") ||
			strings.HasPrefix(path, "/"+apiGWSegClientCerts) ||
			(strings.HasPrefix(path, "/tags/") && strings.Contains(strings.TrimPrefix(path, "/tags/"), ":apigateway:"))

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

	c.Response().Header().Set("Content-Type", "application/x-amz-json-1.1")
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

	// GET requests have no body; normalise to an empty JSON object so that
	// json.Unmarshal calls in the action handlers don't fail with
	// "unexpected end of JSON input".
	if len(body) == 0 {
		body = []byte("{}")
	}

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

	statusCode, response, reqErr := h.dispatch(ctx, action, body)
	if reqErr != nil {
		return h.handleError(ctx, c, action, reqErr)
	}

	c.Response().Header().Set("Content-Type", "application/json")
	if statusCode == http.StatusNoContent {
		return c.NoContent(http.StatusNoContent)
	}

	return c.JSONBlob(statusCode, response)
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
	}

	return apiGWUnknownOp, nil, false
}

// parseAPIGWAccountPath handles /account paths.
func parseAPIGWAccountPath(method string, _ []string, n int) (string, map[string]string, bool) {
	// GET /account → GetAccount
	if n == 1 && method == http.MethodGet {
		return "GetAccount", nil, true
	}
	// PATCH /account → UpdateAccount
	if n == 1 && method == http.MethodPatch {
		return "UpdateAccount", nil, true
	}

	return apiGWUnknownOp, nil, false
}

// parseAPIGWClientCertificatesPath handles /clientcertificates/... paths.
func parseAPIGWClientCertificatesPath(method string, segs []string, n int) (string, map[string]string, bool) {
	switch {
	// GET /clientcertificates → GetClientCertificates
	case n == 1 && method == http.MethodGet:
		return "GetClientCertificates", nil, true
	// POST /clientcertificates → GenerateClientCertificate
	case n == 1 && method == http.MethodPost:
		return "GenerateClientCertificate", nil, true
	// GET /clientcertificates/{id} → GetClientCertificate
	case n == 2 && method == http.MethodGet:
		return "GetClientCertificate", map[string]string{"clientCertificateId": segs[1]}, true
	// DELETE /clientcertificates/{id} → DeleteClientCertificate
	case n == 2 && method == http.MethodDelete:
		return "DeleteClientCertificate", map[string]string{"clientCertificateId": segs[1]}, true
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
		return "GetTags", map[string]string{"resourceArn": arn}, true
	// PUT /tags/{resourceArn} → TagResource
	case http.MethodPut:
		return "TagResource", map[string]string{"resourceArn": arn}, true
	// DELETE /tags/{resourceArn} → UntagResource
	case http.MethodDelete:
		return "UntagResource", map[string]string{"resourceArn": arn}, true
	}

	return apiGWUnknownOp, nil, false
}

// parseAPIGWAPIKeysPath handles /apikeys/... paths.
func parseAPIGWAPIKeysPath(method string, segs []string, n int) (string, map[string]string, bool) {
	switch {
	// GET /apikeys → GetApiKeys
	case n == 1 && method == http.MethodGet:
		return "GetApiKeys", nil, true
	// POST /apikeys → CreateAPIKey
	case n == 1 && method == http.MethodPost:
		return "CreateApiKey", nil, true
	// GET /apikeys/{id} → GetApiKey
	case n == 2 && method == http.MethodGet:
		return "GetApiKey", map[string]string{"apiKeyId": segs[1]}, true
	// DELETE /apikeys/{id} → DeleteApiKey
	case n == 2 && method == http.MethodDelete:
		return "DeleteApiKey", map[string]string{"apiKeyId": segs[1]}, true
	// PATCH /apikeys/{id} → UpdateApiKey
	case n == 2 && method == http.MethodPatch:
		return "UpdateApiKey", map[string]string{"apiKeyId": segs[1]}, true
	}

	return apiGWUnknownOp, nil, false
}

// parseAPIGWDomainNamesPath handles /domainnames/... paths.
//
//nolint:cyclop // path routing table is inherently a multi-branch switch
func parseAPIGWDomainNamesPath(method string, segs []string, n int) (string, map[string]string, bool) {
	switch {
	// GET /domainnames → GetDomainNames
	case n == 1 && method == http.MethodGet:
		return "GetDomainNames", nil, true
	// POST /domainnames → CreateDomainName
	case n == 1 && method == http.MethodPost:
		return "CreateDomainName", nil, true
	// GET /domainnames/{name} → GetDomainName
	case n == 2 && method == http.MethodGet:
		return "GetDomainName", map[string]string{"domainName": segs[1]}, true
	// DELETE /domainnames/{name} → DeleteDomainName
	case n == 2 && method == http.MethodDelete:
		return "DeleteDomainName", map[string]string{"domainName": segs[1]}, true
	// PATCH /domainnames/{name} → UpdateDomainName
	case n == 2 && method == http.MethodPatch:
		return "UpdateDomainName", map[string]string{"domainName": segs[1]}, true
	// GET /domainnames/{name}/basepathmappings → GetBasePathMappings
	case n == 3 && segs[2] == apiGWSegBasePathMappings && method == http.MethodGet:
		return "GetBasePathMappings", map[string]string{"domainName": segs[1]}, true
	// POST /domainnames/{name}/basepathmappings → CreateBasePathMapping
	case n == 3 && segs[2] == apiGWSegBasePathMappings && method == http.MethodPost:
		return "CreateBasePathMapping", map[string]string{"domainName": segs[1]}, true
	// GET /domainnames/{name}/basepathmappings/{basePath} → GetBasePathMapping
	case n == 4 && segs[2] == apiGWSegBasePathMappings && method == http.MethodGet:
		return "GetBasePathMapping", map[string]string{"domainName": segs[1], "basePath": segs[3]}, true
	// PATCH /domainnames/{name}/basepathmappings/{basePath} → UpdateBasePathMapping
	case n == 4 && segs[2] == apiGWSegBasePathMappings && method == http.MethodPatch:
		return "UpdateBasePathMapping", map[string]string{"domainName": segs[1], "basePath": segs[3]}, true
	// DELETE /domainnames/{name}/basepathmappings/{basePath} → DeleteBasePathMapping
	case n == 4 && segs[2] == apiGWSegBasePathMappings && method == http.MethodDelete:
		return "DeleteBasePathMapping", map[string]string{"domainName": segs[1], "basePath": segs[3]}, true
	// POST /domainnames/{domainName}/accessassociations → CreateDomainNameAccessAssociation
	case n == 3 && segs[2] == apiGWSegAccessAssociations && method == http.MethodPost:
		return "CreateDomainNameAccessAssociation", map[string]string{"domainName": segs[1]}, true
	}

	return apiGWUnknownOp, nil, false
}

// parseAPIGWUsagePlansPath handles /usageplans/... paths.
//
//nolint:cyclop // path routing table is inherently a multi-branch switch
func parseAPIGWUsagePlansPath(method string, segs []string, n int) (string, map[string]string, bool) {
	switch {
	// GET /usageplans → GetUsagePlans
	case n == 1 && method == http.MethodGet:
		return "GetUsagePlans", nil, true
	// POST /usageplans → CreateUsagePlan
	case n == 1 && method == http.MethodPost:
		return "CreateUsagePlan", nil, true
	// GET /usageplans/{id} → GetUsagePlan
	case n == 2 && method == http.MethodGet:
		return "GetUsagePlan", map[string]string{"usagePlanId": segs[1]}, true
	// DELETE /usageplans/{id} → DeleteUsagePlan
	case n == 2 && method == http.MethodDelete:
		return "DeleteUsagePlan", map[string]string{"usagePlanId": segs[1]}, true
	// PATCH /usageplans/{id} → UpdateUsagePlan
	case n == 2 && method == http.MethodPatch:
		return "UpdateUsagePlan", map[string]string{"usagePlanId": segs[1]}, true
	// GET /usageplans/{id}/usage → GetUsage
	case n == 3 && segs[2] == "usage" && method == http.MethodGet:
		return "GetUsage", map[string]string{"usagePlanId": segs[1]}, true
	// GET /usageplans/{id}/keys → GetUsagePlanKeys
	case n == 3 && segs[2] == apiGWSegUsagePlanKeys && method == http.MethodGet:
		return "GetUsagePlanKeys", map[string]string{"usagePlanId": segs[1]}, true
	// POST /usageplans/{usagePlanId}/keys → CreateUsagePlanKey
	case n == 3 && segs[2] == apiGWSegUsagePlanKeys && method == http.MethodPost:
		return "CreateUsagePlanKey", map[string]string{"usagePlanId": segs[1]}, true
	// GET /usageplans/{id}/keys/{keyId} → GetUsagePlanKey
	case n == 4 && segs[2] == apiGWSegUsagePlanKeys && method == http.MethodGet:
		return "GetUsagePlanKey", map[string]string{"usagePlanId": segs[1], "keyId": segs[3]}, true
	// DELETE /usageplans/{id}/keys/{keyId} → DeleteUsagePlanKey
	case n == 4 && segs[2] == apiGWSegUsagePlanKeys && method == http.MethodDelete:
		return "DeleteUsagePlanKey", map[string]string{"usagePlanId": segs[1], "keyId": segs[3]}, true
	}

	return apiGWUnknownOp, nil, false
}

// parseAPIGWRestAPIsPath handles /restapis/... paths.
//
//nolint:cyclop,gocyclo,gocognit,funlen // path routing table is inherently a multi-branch switch
func parseAPIGWRestAPIsPath(method string, segs []string, n int) (string, map[string]string, bool) {
	switch {
	// POST /restapis → CreateRestApi
	case n == 1 && method == http.MethodPost:
		return "CreateRestApi", nil, true
	// GET /restapis → GetRestApis
	case n == 1 && method == http.MethodGet:
		return "GetRestApis", nil, true
	// GET /restapis/{id} → GetRestApi
	case n == 2 && method == http.MethodGet:
		return "GetRestApi", map[string]string{"restApiId": segs[1]}, true
	// DELETE /restapis/{id} → DeleteRestApi
	case n == 2 && method == http.MethodDelete:
		return "DeleteRestApi", map[string]string{"restApiId": segs[1]}, true
	// PATCH /restapis/{id} → UpdateRestApi
	case n == 2 && method == http.MethodPatch:
		return "UpdateRestApi", map[string]string{"restApiId": segs[1]}, true
	// GET /restapis/{id}/resources → GetResources
	case n == 3 && segs[2] == apiGWSegResources && method == http.MethodGet:
		return "GetResources", map[string]string{"restApiId": segs[1]}, true
	// GET /restapis/{id}/resources/{resId} → GetResource
	case n == 4 && segs[2] == apiGWSegResources && method == http.MethodGet:
		return "GetResource", map[string]string{"restApiId": segs[1], "resourceId": segs[3]}, true
	// POST /restapis/{id}/resources/{parentId} → CreateResource
	case n == 4 && segs[2] == apiGWSegResources && method == http.MethodPost:
		return "CreateResource", map[string]string{"restApiId": segs[1], "parentId": segs[3]}, true
	// PATCH /restapis/{id}/resources/{resId} → UpdateResource
	case n == 4 && segs[2] == apiGWSegResources && method == http.MethodPatch:
		return "UpdateResource", map[string]string{"restApiId": segs[1], "resourceId": segs[3]}, true
	// DELETE /restapis/{id}/resources/{resId} → DeleteResource
	case n == 4 && segs[2] == apiGWSegResources && method == http.MethodDelete:
		return "DeleteResource", map[string]string{"restApiId": segs[1], "resourceId": segs[3]}, true
	// /restapis/{id}/resources/{resId}/methods/{httpMethod}[/integration]
	case n >= 5 && segs[2] == apiGWSegResources && segs[4] == apiGWSegMethods:
		return parseAPIGWMethodPath(method, segs)
	// POST /restapis/{id}/deployments → CreateDeployment
	case n == 3 && segs[2] == apiGWSegDeployment && method == http.MethodPost:
		return "CreateDeployment", map[string]string{"restApiId": segs[1]}, true
	// GET /restapis/{id}/deployments → GetDeployments
	case n == 3 && segs[2] == apiGWSegDeployment && method == http.MethodGet:
		return "GetDeployments", map[string]string{"restApiId": segs[1]}, true
	// GET /restapis/{id}/deployments/{deplId} → GetDeployment
	case n == 4 && segs[2] == apiGWSegDeployment && method == http.MethodGet:
		return "GetDeployment", map[string]string{"restApiId": segs[1], "deploymentId": segs[3]}, true
	// PATCH /restapis/{id}/deployments/{deplId} → UpdateDeployment
	case n == 4 && segs[2] == apiGWSegDeployment && method == http.MethodPatch:
		return "UpdateDeployment", map[string]string{"restApiId": segs[1], "deploymentId": segs[3]}, true
	// DELETE /restapis/{id}/deployments/{deplId} → DeleteDeployment
	case n == 4 && segs[2] == apiGWSegDeployment && method == http.MethodDelete:
		return "DeleteDeployment", map[string]string{"restApiId": segs[1], "deploymentId": segs[3]}, true
	// GET /restapis/{id}/stages → GetStages
	case n == 3 && segs[2] == apiGWSegStages && method == http.MethodGet:
		return "GetStages", map[string]string{"restApiId": segs[1]}, true
	// GET /restapis/{id}/stages/{stageName} → GetStage
	case n == 4 && segs[2] == apiGWSegStages && method == http.MethodGet:
		return "GetStage", map[string]string{"restApiId": segs[1], "stageName": segs[3]}, true
	// DELETE /restapis/{id}/stages/{stageName} → DeleteStage
	case n == 4 && segs[2] == apiGWSegStages && method == http.MethodDelete:
		return "DeleteStage", map[string]string{"restApiId": segs[1], "stageName": segs[3]}, true
	// POST /restapis/{id}/authorizers → CreateAuthorizer
	case n == 3 && segs[2] == apiGWSegAuthorizers && method == http.MethodPost:
		return "CreateAuthorizer", map[string]string{"restApiId": segs[1]}, true
	// GET /restapis/{id}/authorizers → GetAuthorizers
	case n == 3 && segs[2] == apiGWSegAuthorizers && method == http.MethodGet:
		return "GetAuthorizers", map[string]string{"restApiId": segs[1]}, true
	// GET /restapis/{id}/authorizers/{authId} → GetAuthorizer
	case n == 4 && segs[2] == apiGWSegAuthorizers && method == http.MethodGet:
		return "GetAuthorizer", map[string]string{"restApiId": segs[1], "authorizerId": segs[3]}, true
	// PATCH /restapis/{id}/authorizers/{authId} → UpdateAuthorizer
	case n == 4 && segs[2] == apiGWSegAuthorizers && method == http.MethodPatch:
		return "UpdateAuthorizer", map[string]string{"restApiId": segs[1], "authorizerId": segs[3]}, true
	// DELETE /restapis/{id}/authorizers/{authId} → DeleteAuthorizer
	case n == 4 && segs[2] == apiGWSegAuthorizers && method == http.MethodDelete:
		return "DeleteAuthorizer", map[string]string{"restApiId": segs[1], "authorizerId": segs[3]}, true
	// POST /restapis/{id}/requestvalidators → CreateRequestValidator
	case n == 3 && segs[2] == apiGWSegValidators && method == http.MethodPost:
		return "CreateRequestValidator", map[string]string{"restApiId": segs[1]}, true
	// GET /restapis/{id}/requestvalidators → GetRequestValidators
	case n == 3 && segs[2] == apiGWSegValidators && method == http.MethodGet:
		return "GetRequestValidators", map[string]string{"restApiId": segs[1]}, true
	// GET /restapis/{id}/requestvalidators/{id} → GetRequestValidator
	case n == 4 && segs[2] == apiGWSegValidators && method == http.MethodGet:
		return "GetRequestValidator", map[string]string{"restApiId": segs[1], "requestValidatorId": segs[3]}, true
	// PATCH /restapis/{id}/requestvalidators/{id} → UpdateRequestValidator
	case n == 4 && segs[2] == apiGWSegValidators && method == http.MethodPatch:
		return "UpdateRequestValidator", map[string]string{"restApiId": segs[1], "requestValidatorId": segs[3]}, true
	// DELETE /restapis/{id}/requestvalidators/{id} → DeleteRequestValidator
	case n == 4 && segs[2] == apiGWSegValidators && method == http.MethodDelete:
		return "DeleteRequestValidator", map[string]string{"restApiId": segs[1], "requestValidatorId": segs[3]}, true
	// POST /restapis/{id}/models → CreateModel
	case n == 3 && segs[2] == apiGWSegModels && method == http.MethodPost:
		return "CreateModel", map[string]string{"restApiId": segs[1]}, true
	// GET /restapis/{id}/models → GetModels
	case n == 3 && segs[2] == apiGWSegModels && method == http.MethodGet:
		return "GetModels", map[string]string{"restApiId": segs[1]}, true
	// GET /restapis/{id}/models/{modelName} → GetModel
	case n == 4 && segs[2] == apiGWSegModels && method == http.MethodGet:
		return "GetModel", map[string]string{"restApiId": segs[1], "modelName": segs[3]}, true
	// DELETE /restapis/{id}/models/{modelName} → DeleteModel
	case n == 4 && segs[2] == apiGWSegModels && method == http.MethodDelete:
		return "DeleteModel", map[string]string{"restApiId": segs[1], "modelName": segs[3]}, true
	// PATCH /restapis/{id}/models/{modelName} → UpdateModel
	case n == 4 && segs[2] == apiGWSegModels && method == http.MethodPatch:
		return "UpdateModel", map[string]string{"restApiId": segs[1], "modelName": segs[3]}, true
	// POST /restapis/{id}/stages → CreateStage (standalone)
	case n == 3 && segs[2] == apiGWSegStages && method == http.MethodPost:
		return "CreateStage", map[string]string{"restApiId": segs[1]}, true
	// PATCH /restapis/{id}/stages/{stageName} → UpdateStage
	case n == 4 && segs[2] == apiGWSegStages && method == http.MethodPatch:
		return "UpdateStage", map[string]string{"restApiId": segs[1], "stageName": segs[3]}, true
	// DELETE /restapis/{id}/stages/{stageName}/cache → FlushStageCache
	case n == 5 && segs[2] == apiGWSegStages && segs[4] == "cache" && method == http.MethodDelete:
		return "FlushStageCache", map[string]string{"restApiId": segs[1], "stageName": segs[3]}, true
	// DELETE /restapis/{id}/stages/{stageName}/cache/authorizers → FlushStageAuthorizersCache
	case n == 6 && segs[2] == apiGWSegStages && segs[4] == "cache" &&
		segs[5] == "authorizers" && method == http.MethodDelete:
		return "FlushStageAuthorizersCache", map[string]string{"restApiId": segs[1], "stageName": segs[3]}, true
	// POST /restapis/{id}/documentation/parts → CreateDocumentationPart
	case n == 4 && segs[2] == apiGWSegDocumentation && segs[3] == apiGWSegDocParts && method == http.MethodPost:
		return "CreateDocumentationPart", map[string]string{"restApiId": segs[1]}, true
	// GET /restapis/{id}/documentation/parts → GetDocumentationParts
	case n == 4 && segs[2] == apiGWSegDocumentation && segs[3] == apiGWSegDocParts && method == http.MethodGet:
		return "GetDocumentationParts", map[string]string{"restApiId": segs[1]}, true
	// GET /restapis/{id}/documentation/parts/{docPartId} → GetDocumentationPart
	case n == 5 && segs[2] == apiGWSegDocumentation && segs[3] == apiGWSegDocParts && method == http.MethodGet:
		return "GetDocumentationPart", map[string]string{"restApiId": segs[1], "docPartId": segs[4]}, true
	// DELETE /restapis/{id}/documentation/parts/{docPartId} → DeleteDocumentationPart
	case n == 5 && segs[2] == apiGWSegDocumentation && segs[3] == apiGWSegDocParts && method == http.MethodDelete:
		return "DeleteDocumentationPart", map[string]string{"restApiId": segs[1], "docPartId": segs[4]}, true
	// POST /restapis/{id}/documentation/versions → CreateDocumentationVersion
	case n == 4 && segs[2] == apiGWSegDocumentation && segs[3] == apiGWSegDocVersions && method == http.MethodPost:
		return "CreateDocumentationVersion", map[string]string{"restApiId": segs[1]}, true
	// GET /restapis/{id}/documentation/versions → GetDocumentationVersions
	case n == 4 && segs[2] == apiGWSegDocumentation && segs[3] == apiGWSegDocVersions && method == http.MethodGet:
		return "GetDocumentationVersions", map[string]string{"restApiId": segs[1]}, true
	// GET /restapis/{id}/documentation/versions/{docVersion} → GetDocumentationVersion
	case n == 5 && segs[2] == apiGWSegDocumentation && segs[3] == apiGWSegDocVersions && method == http.MethodGet:
		return "GetDocumentationVersion", map[string]string{"restApiId": segs[1], "documentationVersion": segs[4]}, true
	// DELETE /restapis/{id}/documentation/versions/{docVersion} → DeleteDocumentationVersion
	case n == 5 && segs[2] == apiGWSegDocumentation && segs[3] == apiGWSegDocVersions && method == http.MethodDelete:
		return "DeleteDocumentationVersion", map[string]string{
			"restApiId":            segs[1],
			"documentationVersion": segs[4],
		}, true
	// PATCH /restapis/{id}/documentation/parts/{docPartId} → UpdateDocumentationPart
	case n == 5 && segs[2] == apiGWSegDocumentation && segs[3] == apiGWSegDocParts && method == http.MethodPatch:
		return "UpdateDocumentationPart", map[string]string{"restApiId": segs[1], "docPartId": segs[4]}, true
	// PATCH /restapis/{id}/documentation/versions/{docVersion} → UpdateDocumentationVersion
	case n == 5 && segs[2] == apiGWSegDocumentation && segs[3] == apiGWSegDocVersions && method == http.MethodPatch:
		return "UpdateDocumentationVersion", map[string]string{
			"restApiId":            segs[1],
			"documentationVersion": segs[4],
		}, true
	// GET /restapis/{id}/gatewayresponses → GetGatewayResponses
	case n == 3 && segs[2] == apiGWSegGatewayResponses && method == http.MethodGet:
		return "GetGatewayResponses", map[string]string{"restApiId": segs[1]}, true
	// GET /restapis/{id}/gatewayresponses/{responseType} → GetGatewayResponse
	case n == 4 && segs[2] == apiGWSegGatewayResponses && method == http.MethodGet:
		return "GetGatewayResponse", map[string]string{"restApiId": segs[1], "responseType": segs[3]}, true
	// PUT /restapis/{id}/gatewayresponses/{responseType} → PutGatewayResponse
	case n == 4 && segs[2] == apiGWSegGatewayResponses && method == http.MethodPut:
		return "PutGatewayResponse", map[string]string{"restApiId": segs[1], "responseType": segs[3]}, true
	// DELETE /restapis/{id}/gatewayresponses/{responseType} → DeleteGatewayResponse
	case n == 4 && segs[2] == apiGWSegGatewayResponses && method == http.MethodDelete:
		return "DeleteGatewayResponse", map[string]string{"restApiId": segs[1], "responseType": segs[3]}, true
	// GET /restapis/{id}/models/{modelName}/default_template → GetModelTemplate
	case n == 5 && segs[2] == apiGWSegModels && segs[4] == "default_template" && method == http.MethodGet:
		return "GetModelTemplate", map[string]string{"restApiId": segs[1], "modelName": segs[3]}, true
	// POST /restapis/{id}/authorizers/{authId}/invocations → TestInvokeAuthorizer
	case n == 5 && segs[2] == apiGWSegAuthorizers && segs[4] == "invocations" && method == http.MethodPost:
		return "TestInvokeAuthorizer", map[string]string{"restApiId": segs[1], "authorizerId": segs[3]}, true
	}

	return apiGWUnknownOp, nil, false
}

// parseAPIGWMethodPath handles paths under /restapis/{id}/resources/{resId}/methods/{httpMethod}.
//
//nolint:cyclop,gocognit,nestif,gocyclo,funlen // method path routing table is inherently a multi-branch switch
func parseAPIGWMethodPath(method string, segs []string) (string, map[string]string, bool) {
	// segs: [restapis, {id}, resources, {resId}, methods, {httpMethod}, ...]
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
		"restApiId":  apiID,
		"resourceId": resID,
		"httpMethod": httpMethod,
	}

	// /restapis/{id}/resources/{resId}/methods/{httpMethod}/integration[/responses/{statusCode}]
	if len(segs) > idxIntegSeg && segs[idxIntegSeg] == apiGWSegInteg {
		// /restapis/{id}/resources/{resId}/methods/{httpMethod}/integration/responses/{statusCode}
		if len(segs) > idxRespSeg && segs[idxRespSeg] == apiGWSegResponses {
			if len(segs) <= idxRespSeg+1 {
				return apiGWUnknownOp, nil, false
			}
			params := map[string]string{
				"restApiId":  apiID,
				"resourceId": resID,
				"httpMethod": httpMethod,
				"statusCode": segs[idxRespSeg+1],
			}
			switch method {
			case http.MethodPut:
				return "PutIntegrationResponse", params, true
			case http.MethodGet:
				return "GetIntegrationResponse", params, true
			case http.MethodDelete:
				return "DeleteIntegrationResponse", params, true
			}

			return apiGWUnknownOp, nil, false
		}

		switch method {
		case http.MethodPut:
			return "PutIntegration", baseParams, true
		case http.MethodGet:
			return "GetIntegration", baseParams, true
		case http.MethodDelete:
			return "DeleteIntegration", baseParams, true
		}

		return apiGWUnknownOp, nil, false
	}

	// /restapis/{id}/resources/{resId}/methods/{httpMethod}/responses/{statusCode}
	if len(segs) > idxIntegSeg && segs[idxIntegSeg] == apiGWSegResponses {
		if len(segs) <= idxIntegSeg+1 {
			return apiGWUnknownOp, nil, false
		}
		params := map[string]string{
			"restApiId":  apiID,
			"resourceId": resID,
			"httpMethod": httpMethod,
			"statusCode": segs[idxIntegSeg+1],
		}
		switch method {
		case http.MethodPut:
			return "PutMethodResponse", params, true
		case http.MethodGet:
			return "GetMethodResponse", params, true
		case http.MethodDelete:
			return "DeleteMethodResponse", params, true
		}

		return apiGWUnknownOp, nil, false
	}

	// /restapis/{id}/resources/{resId}/methods/{httpMethod}
	switch method {
	case http.MethodPut:
		return "PutMethod", baseParams, true
	case http.MethodGet:
		return "GetMethod", baseParams, true
	case http.MethodDelete:
		return "DeleteMethod", baseParams, true
	case http.MethodPost:
		return "TestInvokeMethod", baseParams, true
	case http.MethodPatch:
		return "UpdateMethod", baseParams, true
	}

	// Integration PATCH
	if len(segs) > idxIntegSeg && segs[idxIntegSeg] == apiGWSegInteg {
		if method == http.MethodPatch {
			if len(segs) > idxRespSeg && segs[idxRespSeg] == apiGWSegResponses {
				if len(segs) > idxRespSeg+1 {
					params := map[string]string{
						"restApiId":  apiID,
						"resourceId": resID,
						"httpMethod": httpMethod,
						"statusCode": segs[idxRespSeg+1],
					}

					return "UpdateIntegrationResponse", params, true
				}
			} else {
				return "UpdateIntegration", baseParams, true
			}
		}
	}

	// MethodResponse PATCH
	if len(segs) > idxIntegSeg && segs[idxIntegSeg] == apiGWSegResponses {
		if method == http.MethodPatch && len(segs) > idxIntegSeg+1 {
			params := map[string]string{
				"restApiId":  apiID,
				"resourceId": resID,
				"httpMethod": httpMethod,
				"statusCode": segs[idxIntegSeg+1],
			}

			return "UpdateMethodResponse", params, true
		}
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

func (h *Handler) restAPIActions() map[string]actionFn {
	return map[string]actionFn{
		"CreateRestApi": func(b []byte) (int, any, error) {
			var input createRestAPIInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			api, err := h.Backend.CreateRestAPI(input.Name, input.Description, input.Tags)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusCreated, api, nil
		},
		"DeleteRestApi": func(b []byte) (int, any, error) {
			var input deleteRestAPIInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			if err := h.Backend.DeleteRestAPI(input.RestAPIID); err != nil {
				return 0, nil, err
			}

			return http.StatusAccepted, map[string]any{}, nil
		},
		"GetRestApi": func(b []byte) (int, any, error) {
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
		"GetRestApis": func(b []byte) (int, any, error) {
			var input getRestApisInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			apis, position, err := h.Backend.GetRestAPIs(input.Limit, input.Position)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, map[string]any{"item": apis, "position": position}, nil
		},
	}
}

func (h *Handler) resourceActions() map[string]actionFn {
	return map[string]actionFn{
		"GetResources": func(b []byte) (int, any, error) {
			var input getResourcesInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			resources, position, err := h.Backend.GetResources(input.RestAPIID, input.Position, input.Limit)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, map[string]any{"item": resources, "position": position}, nil
		},
		"GetResource": func(b []byte) (int, any, error) {
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
		"CreateResource": func(b []byte) (int, any, error) {
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
		"DeleteResource": func(b []byte) (int, any, error) {
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
		"PutMethod": func(b []byte) (int, any, error) {
			var input putMethodInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			m, err := h.Backend.PutMethod(
				input.RestAPIID,
				input.ResourceID,
				input.HTTPMethod,
				input.AuthorizationType,
				input.AuthorizerID,
				input.RequestValidatorID,
				input.APIKeyRequired,
			)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusCreated, m, nil
		},
		"GetMethod": func(b []byte) (int, any, error) {
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
		"DeleteMethod": func(b []byte) (int, any, error) {
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
		"PutMethodResponse": func(b []byte) (int, any, error) {
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
		"GetMethodResponse": func(b []byte) (int, any, error) {
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
		"DeleteMethodResponse": func(b []byte) (int, any, error) {
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
		"PutIntegrationResponse": func(b []byte) (int, any, error) {
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
		"GetIntegrationResponse": func(b []byte) (int, any, error) {
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
		"DeleteIntegrationResponse": func(b []byte) (int, any, error) {
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
		"CreateAuthorizer": func(b []byte) (int, any, error) {
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
		"GetAuthorizer": func(b []byte) (int, any, error) {
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
		"GetAuthorizers": func(b []byte) (int, any, error) {
			var input getAuthorizersInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			auths, err := h.Backend.GetAuthorizers(input.RestAPIID)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, map[string]any{"item": auths}, nil
		},
		"UpdateAuthorizer": func(b []byte) (int, any, error) {
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
		"DeleteAuthorizer": func(b []byte) (int, any, error) {
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
		"CreateRequestValidator": func(b []byte) (int, any, error) {
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
		"GetRequestValidator": func(b []byte) (int, any, error) {
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
		"GetRequestValidators": func(b []byte) (int, any, error) {
			var input getRequestValidatorsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			rvs, err := h.Backend.GetRequestValidators(input.RestAPIID)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, map[string]any{"item": rvs}, nil
		},
		"UpdateRequestValidator": func(b []byte) (int, any, error) {
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
		"DeleteRequestValidator": func(b []byte) (int, any, error) {
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
		"PutIntegration": func(b []byte) (int, any, error) {
			var input putIntegrationInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
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
		"GetIntegration": func(b []byte) (int, any, error) {
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
		"DeleteIntegration": func(b []byte) (int, any, error) {
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

//nolint:gocognit // deployment action map is explicit by design.
func (h *Handler) deploymentActions() map[string]actionFn {
	return map[string]actionFn{
		"CreateDeployment": func(b []byte) (int, any, error) {
			var input createDeploymentInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			depl, err := h.Backend.CreateDeployment(input.RestAPIID, input.StageName, input.Description)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusCreated, depl, nil
		},
		"GetDeployment": func(b []byte) (int, any, error) {
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
		"GetDeployments": func(b []byte) (int, any, error) {
			var input getDeploymentsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			depls, err := h.Backend.GetDeployments(input.RestAPIID)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, map[string]any{"item": depls}, nil
		},
		"DeleteDeployment": func(b []byte) (int, any, error) {
			var input deleteDeploymentInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			if err := h.Backend.DeleteDeployment(input.RestAPIID, input.DeploymentID); err != nil {
				return 0, nil, err
			}

			return http.StatusNoContent, map[string]any{}, nil
		},
		"GetStages": func(b []byte) (int, any, error) {
			var input getStagesInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			stages, err := h.Backend.GetStages(input.RestAPIID)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, map[string]any{"item": stages}, nil
		},
		"GetStage": func(b []byte) (int, any, error) {
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
		"DeleteStage": func(b []byte) (int, any, error) {
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

	return table
}

//nolint:cyclop,funlen,gocognit // action table - one closure per op; complexity unavoidable
func (h *Handler) newResourceActions() map[string]actionFn {
	return map[string]actionFn{
		"CreateApiKey": func(b []byte) (int, any, error) {
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
		"CreateBasePathMapping": func(b []byte) (int, any, error) {
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
		"CreateDocumentationPart": func(b []byte) (int, any, error) {
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
		"CreateDocumentationVersion": func(b []byte) (int, any, error) {
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
		"CreateDomainName": func(b []byte) (int, any, error) {
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
		"CreateDomainNameAccessAssociation": func(b []byte) (int, any, error) {
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
		"CreateModel": func(b []byte) (int, any, error) {
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
		"CreateStage": func(b []byte) (int, any, error) {
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
		"CreateUsagePlan": func(b []byte) (int, any, error) {
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
		"CreateUsagePlanKey": func(b []byte) (int, any, error) {
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
	c.Response().Header().Set("Content-Type", "application/x-amz-json-1.1")

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
		errors.Is(reqErr, ErrUsagePlanKeyNotFound):
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

//nolint:cyclop,funlen,gocognit,gocyclo // action table - one closure per op; complexity unavoidable
func (h *Handler) getDeleteUpdateActions() map[string]actionFn {
	return map[string]actionFn{
		"GetApiKey": func(b []byte) (int, any, error) {
			var input getAPIKeyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			key, err := h.Backend.GetAPIKey(input.APIKeyID)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, key, nil
		},
		"GetApiKeys": func(b []byte) (int, any, error) {
			var input getAPIKeysPageInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			if input.Limit == 0 && input.Position == "" {
				keys, err := h.Backend.GetAPIKeys()
				if err != nil {
					return 0, nil, err
				}

				return http.StatusOK, map[string]any{"item": keys}, nil
			}
			keys, position, err := h.Backend.GetAPIKeysPage(input.Limit, input.Position)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, map[string]any{"item": keys, "position": position}, nil
		},
		"DeleteApiKey": func(b []byte) (int, any, error) {
			var input deleteAPIKeyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			if err := h.Backend.DeleteAPIKey(input.APIKeyID); err != nil {
				return 0, nil, err
			}

			return http.StatusAccepted, map[string]any{}, nil
		},
		"UpdateApiKey": func(b []byte) (int, any, error) {
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
		"GetDomainName": func(b []byte) (int, any, error) {
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
		"GetDomainNames": func(b []byte) (int, any, error) {
			var input getDomainNamesPageInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			if input.Limit == 0 && input.Position == "" {
				dns, err := h.Backend.GetDomainNames()
				if err != nil {
					return 0, nil, err
				}

				return http.StatusOK, map[string]any{"item": dns}, nil
			}
			dns, position, err := h.Backend.GetDomainNamesPage(input.Limit, input.Position)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, map[string]any{"item": dns, "position": position}, nil
		},
		"DeleteDomainName": func(b []byte) (int, any, error) {
			var input deleteDomainNameInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			if err := h.Backend.DeleteDomainName(input.DomainName); err != nil {
				return 0, nil, err
			}

			return http.StatusAccepted, map[string]any{}, nil
		},
		"GetBasePathMapping": func(b []byte) (int, any, error) {
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
		"GetBasePathMappings": func(b []byte) (int, any, error) {
			var input getBasePathMappingsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			bpms, err := h.Backend.GetBasePathMappings(input.DomainName)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, map[string]any{"item": bpms}, nil
		},
		"DeleteBasePathMapping": func(b []byte) (int, any, error) {
			var input deleteBasePathMappingInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			if err := h.Backend.DeleteBasePathMapping(input.DomainName, input.BasePath); err != nil {
				return 0, nil, err
			}

			return http.StatusAccepted, map[string]any{}, nil
		},
		"GetModel": func(b []byte) (int, any, error) {
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
		"GetModels": func(b []byte) (int, any, error) {
			var input getModelsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			ms, err := h.Backend.GetModels(input.RestAPIID)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, map[string]any{"item": ms}, nil
		},
		"DeleteModel": func(b []byte) (int, any, error) {
			var input deleteModelInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			if err := h.Backend.DeleteModel(input.RestAPIID, input.ModelName); err != nil {
				return 0, nil, err
			}

			return http.StatusAccepted, map[string]any{}, nil
		},
		"UpdateModel": func(b []byte) (int, any, error) {
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
		"UpdateStage": func(b []byte) (int, any, error) {
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
		"FlushStageCache": func(b []byte) (int, any, error) {
			var input flushStageCacheInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}

			return http.StatusAccepted, map[string]any{}, nil
		},
		"FlushStageAuthorizersCache": func(b []byte) (int, any, error) {
			var input flushStageCacheInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			h.authCache.flush()

			return http.StatusAccepted, map[string]any{}, nil
		},
		"GetUsagePlan": func(b []byte) (int, any, error) {
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
		"GetUsagePlans": func(b []byte) (int, any, error) {
			var input getUsagePlansPageInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			if input.Limit == 0 && input.Position == "" {
				ps, err := h.Backend.GetUsagePlans()
				if err != nil {
					return 0, nil, err
				}

				return http.StatusOK, map[string]any{"item": ps}, nil
			}
			ps, position, err := h.Backend.GetUsagePlansPage(input.Limit, input.Position)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, map[string]any{"item": ps, "position": position}, nil
		},
		"DeleteUsagePlan": func(b []byte) (int, any, error) {
			var input deleteUsagePlanInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			if err := h.Backend.DeleteUsagePlan(input.UsagePlanID); err != nil {
				return 0, nil, err
			}

			return http.StatusAccepted, map[string]any{}, nil
		},
		"GetUsagePlanKey": func(b []byte) (int, any, error) {
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
		"GetUsagePlanKeys": func(b []byte) (int, any, error) {
			var input getUsagePlanKeysInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			ks, err := h.Backend.GetUsagePlanKeys(input.UsagePlanID)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, map[string]any{"item": ks}, nil
		},
		"DeleteUsagePlanKey": func(b []byte) (int, any, error) {
			var input deleteUsagePlanKeyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			if err := h.Backend.DeleteUsagePlanKey(input.UsagePlanID, input.KeyID); err != nil {
				return 0, nil, err
			}

			return http.StatusAccepted, map[string]any{}, nil
		},
		"GetDocumentationPart": func(b []byte) (int, any, error) {
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
		"GetDocumentationParts": func(b []byte) (int, any, error) {
			var input getDocumentationPartsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			ps, err := h.Backend.GetDocumentationParts(input.RestAPIID)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, map[string]any{"item": ps}, nil
		},
		"DeleteDocumentationPart": func(b []byte) (int, any, error) {
			var input deleteDocumentationPartInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			if err := h.Backend.DeleteDocumentationPart(input.RestAPIID, input.DocPartID); err != nil {
				return 0, nil, err
			}

			return http.StatusAccepted, map[string]any{}, nil
		},
		"GetDocumentationVersion": func(b []byte) (int, any, error) {
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
		"GetDocumentationVersions": func(b []byte) (int, any, error) {
			var input getDocumentationVersionsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			vs, err := h.Backend.GetDocumentationVersions(input.RestAPIID)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, map[string]any{"item": vs}, nil
		},
		"DeleteDocumentationVersion": func(b []byte) (int, any, error) {
			var input deleteDocumentationVersionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			if err := h.Backend.DeleteDocumentationVersion(input.RestAPIID, input.DocVersion); err != nil {
				return 0, nil, err
			}

			return http.StatusAccepted, map[string]any{}, nil
		},
	}
}

//nolint:cyclop,gocognit,gocyclo,funlen // action table - one closure per op; complexity unavoidable
func (h *Handler) updatePatchActions() map[string]actionFn {
	return map[string]actionFn{
		"UpdateRestApi": func(b []byte) (int, any, error) {
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
		"UpdateDeployment": func(b []byte) (int, any, error) {
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
		"UpdateResource": func(b []byte) (int, any, error) {
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
		"GetAccount": func(_ []byte) (int, any, error) {
			acct, err := h.Backend.GetAccount()
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, acct, nil
		},
		"GetTags": func(b []byte) (int, any, error) {
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
		"TagResource": func(b []byte) (int, any, error) {
			var input tagResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			if err := h.Backend.TagResource(input.ResourceARN, input.Tags); err != nil {
				return 0, nil, err
			}

			return http.StatusNoContent, map[string]any{}, nil
		},
		"UntagResource": func(b []byte) (int, any, error) {
			var input untagResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			if err := h.Backend.UntagResource(input.ResourceARN, input.TagKeys); err != nil {
				return 0, nil, err
			}

			return http.StatusNoContent, map[string]any{}, nil
		},
		"TestInvokeMethod": func(b []byte) (int, any, error) {
			var input testInvokeMethodHandlerInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			out, err := h.Backend.TestInvokeMethod(input.TestInvokeMethodInput)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, out, nil
		},
		"UpdateUsagePlan": func(b []byte) (int, any, error) {
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
		"UpdateDomainName": func(b []byte) (int, any, error) {
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
		"UpdateBasePathMapping": func(b []byte) (int, any, error) {
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
		"UpdateDocumentationPart": func(b []byte) (int, any, error) {
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
		"UpdateDocumentationVersion": func(b []byte) (int, any, error) {
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
		"UpdateMethod": func(b []byte) (int, any, error) {
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
		"UpdateIntegration": func(b []byte) (int, any, error) {
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
		"UpdateIntegrationResponse": func(b []byte) (int, any, error) {
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
		"UpdateMethodResponse": func(b []byte) (int, any, error) {
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
		"UpdateAccount": func(b []byte) (int, any, error) {
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
		"TestInvokeAuthorizer": func(b []byte) (int, any, error) {
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
		"GetModelTemplate": func(b []byte) (int, any, error) {
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
		"GetGatewayResponse": func(b []byte) (int, any, error) {
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
		"GetGatewayResponses": func(b []byte) (int, any, error) {
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

			return http.StatusOK, map[string]any{"item": out}, nil
		},
		"PutGatewayResponse": func(b []byte) (int, any, error) {
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
		"DeleteGatewayResponse": func(b []byte) (int, any, error) {
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

			return http.StatusAccepted, nil, nil
		},
		"GenerateClientCertificate": func(b []byte) (int, any, error) {
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
		"GetClientCertificate": func(b []byte) (int, any, error) {
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
		"GetClientCertificates": func(_ []byte) (int, any, error) {
			out, err := h.Backend.GetClientCertificates()
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, map[string]any{"item": out}, nil
		},
		"DeleteClientCertificate": func(b []byte) (int, any, error) {
			var params struct {
				ClientCertificateID string `json:"clientCertificateId"`
			}
			if err := json.Unmarshal(b, &params); err != nil {
				return 0, nil, err
			}
			if err := h.Backend.DeleteClientCertificate(params.ClientCertificateID); err != nil {
				return 0, nil, err
			}

			return http.StatusAccepted, nil, nil
		},
		"GetUsage": func(b []byte) (int, any, error) {
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
