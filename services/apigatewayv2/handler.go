package apigatewayv2

import (
	"context"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/apigatewaymanagementapi"
)

const (
	apigwV2MatchPriority = service.PriorityPathVersioned
	apisPathPrefix       = "/v2/apis"
	domainNamesPrefix    = "/v2/domainnames"
	portalsPrefix        = "/v2/portals"
	portalProductsPrefix = "/v2/portalproducts"
	vpcLinksPrefix       = "/v2/vpclinks"

	// path segment count constants.
	segCountAPIs     = 0
	segCountAPIByID  = 1
	segCountSubColl  = 2
	segCountSubRes   = 3
	segCountDeepColl = 4
	segCountDeepRes  = 5

	// maxPathParts is the max number of parts when splitting a non-apis path suffix.
	maxPathParts = 3

	tagsPrefix = "/v2/tags"

	// non-apis path segment count constants.
	pathPartOne = 1
	pathPartTwo = 2

	// collection name constants.
	collStages               = "stages"
	collRoutes               = "routes"
	collIntegrations         = "integrations"
	collDeployments          = "deployments"
	collAuthorizers          = "authorizers"
	collModels               = "models"
	collIntegrationResponses = "integrationresponses"
	collRouteResponses       = "routeresponses"
	collAPIMappings          = "apimappings"
	collRoutingRules         = "routingrules"
	collAccessLogSettings    = "accesslogsettings"
	collRouteSettings        = "routesettings"
	collRequestParameters    = "requestparameters"
	collCors                 = "cors"
	collExports              = "exports"
	collProductPages         = "productpages"
	collProductREPages       = "productrestendpointpages"
	collSharingPolicy        = "sharingpolicy"
	collTemplate             = "template"
	collPreview              = "preview"
	collPublish              = "publish"
	collCache                = "cache"

	// error messages.
	msgNotFound         = "Not Found"
	msgMethodNotAllowed = "Method Not Allowed"
	msgInvalidBody      = "invalid request body"
	emptyModelTemplate  = "{}"

	// opUnknown is returned when no matching operation is found.
	opUnknown = "Unknown"
)

// LambdaInvoker is the interface for invoking Lambda functions.
type LambdaInvoker interface {
	InvokeFunction(ctx context.Context, name, invocationType string, payload []byte) ([]byte, int, error)
}

// JWKSProvider resolves RSA public keys for JWT signature verification.
// Implementations return an error when the issuer or key is unknown.
type JWKSProvider interface {
	GetJWTPublicKey(issuerURL, kid string) (*rsa.PublicKey, error)
}

type Handler struct {
	Backend               StorageBackend
	jwksProvider          JWKSProvider
	lambdaInvoker         LambdaInvoker
	managementAPI         apigatewaymanagementapi.StorageBackend
	authCache             *authorizerCache
	subCollectionDispatch map[subDispatchKey]func(*Handler, *echo.Context, string) error
	subResourceDispatch   map[subDispatchKey]func(*Handler, *echo.Context, string, string) error
	httpClient            *http.Client
}

// NewHandler creates a new API Gateway v2 Handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{
		Backend:               backend,
		authCache:             newAuthorizerCache(),
		subCollectionDispatch: newSubCollectionDispatch(),
		subResourceDispatch:   newSubResourceDispatch(),
		httpClient:            &http.Client{Timeout: httpClientTimeout},
	}
}

// SetHTTPClient configures the HTTP client used for HTTP_PROXY integration calls.
func (h *Handler) SetHTTPClient(c *http.Client) {
	h.httpClient = c
}

func (h *Handler) getHTTPClient() *http.Client {
	if h.httpClient != nil {
		return h.httpClient
	}

	return &http.Client{Timeout: httpClientTimeout}
}

// SetLambdaInvoker configures the Lambda invoker for AWS_PROXY integrations.
func (h *Handler) SetLambdaInvoker(lambda LambdaInvoker) {
	h.lambdaInvoker = lambda
}

// SetJWKSProvider configures the JWKS provider used to verify JWT authorizer signatures.
func (h *Handler) SetJWKSProvider(p JWKSProvider) {
	h.jwksProvider = p
}

// SetManagementAPIBackend configures the Management API backend for WebSocket connections.
func (h *Handler) SetManagementAPIBackend(managementAPI apigatewaymanagementapi.StorageBackend) {
	h.managementAPI = managementAPI
}

// Name returns the service name.
func (h *Handler) Name() string { return "APIGatewayV2" }

// GetSupportedOperations returns all supported API operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateApi", "GetApi", "GetApis", "DeleteApi", "UpdateApi",
		"CreateStage", "GetStage", "GetStages", "DeleteStage", "UpdateStage",
		"CreateRoute", "GetRoute", "GetRoutes", "DeleteRoute", "UpdateRoute",
		"CreateIntegration", "GetIntegration", "GetIntegrations", "DeleteIntegration", "UpdateIntegration",
		"CreateDeployment", "GetDeployment", "GetDeployments", "DeleteDeployment",
		"CreateAuthorizer", "GetAuthorizer", "GetAuthorizers", "DeleteAuthorizer", "UpdateAuthorizer",
		"CreateApiMapping", "GetApiMapping", "GetApiMappings", "DeleteApiMapping",
		"CreateDomainName", "GetDomainName", "GetDomainNames", "DeleteDomainName",
		"CreateVpcLink", "DeleteVpcLink", "GetVpcLink", "GetVpcLinks", "UpdateVpcLink",
		"CreateRoutingRule", "DeleteRoutingRule", "GetRoutingRule", "ListRoutingRules", "PutRoutingRule",
		"CreateIntegrationResponse", "GetIntegrationResponse", "GetIntegrationResponses", "DeleteIntegrationResponse",
		"CreateModel", "GetModel", "GetModels", "DeleteModel",
		"CreatePortal", "GetPortal", "ListPortals",
		"CreatePortalProduct", "GetPortalProduct", "ListPortalProducts",
		"CreateProductPage", "ListProductPages",
		"CreateProductRestEndpointPage", "ListProductRestEndpointPages",
		"GetPortalProductSharingPolicy", "PutPortalProductSharingPolicy", "DeletePortalProductSharingPolicy",
		"CreateRouteResponse", "GetRouteResponse", "GetRouteResponses", "DeleteRouteResponse",
		"GetTags", "TagResource", "UntagResource",
		"UpdateApiMapping", "UpdateDeployment", "UpdateDomainName", "UpdateIntegrationResponse",
		"UpdateModel", "UpdatePortal", "UpdatePortalProduct", "UpdateRouteResponse",
		"UpdateProductPage", "UpdateProductRestEndpointPage",
		"DeletePortal", "DeletePortalProduct",
		"DeleteProductPage", "DeleteProductRestEndpointPage",
		"GetProductPage", "GetProductRestEndpointPage",
		"DeleteAccessLogSettings", "DeleteCorsConfiguration",
		"DeleteRouteRequestParameter", "DeleteRouteSettings",
		"DisablePortal", "PreviewPortal", "PublishPortal",
		"ExportApi", "ImportApi", "ReimportApi", "GetModelTemplate",
		"ResetAuthorizersCache",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "apigatewayv2" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler instance handles.
func (h *Handler) ChaosRegions() []string { return []string{config.DefaultRegion} }

// RouteMatcher returns a function matching API Gateway v2 requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path

		return path == apisPathPrefix || strings.HasPrefix(path, apisPathPrefix+"/") ||
			path == domainNamesPrefix || strings.HasPrefix(path, domainNamesPrefix+"/") ||
			path == portalsPrefix || strings.HasPrefix(path, portalsPrefix+"/") ||
			path == portalProductsPrefix || strings.HasPrefix(path, portalProductsPrefix+"/") ||
			path == vpcLinksPrefix || strings.HasPrefix(path, vpcLinksPrefix+"/") ||
			strings.HasPrefix(path, tagsPrefix+"/") ||
			strings.HasPrefix(path, "/v2proxy/") ||
			isUserRequestPath(path)
	}
}

// isUserRequestPath reports whether the path follows the data-plane format:
// /restapis/{apiId}/{stageName}/_user_request_/{resourcePath...}.
func isUserRequestPath(path string) bool {
	segs := strings.Split(strings.TrimPrefix(path, "/"), "/")
	const minSegs = 4 // restapis, apiId, stageName, _user_request_

	return len(segs) >= minSegs && segs[0] == "restapis" && segs[3] == "_user_request_"
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return apigwV2MatchPriority }

// operationKey uniquely identifies a routing case for ExtractOperation.
type operationKey struct {
	seg1   string
	method string
	segs   int
}

// onceOpTable lazily initialises the operation lookup table exactly once.
//
//nolint:gochecknoglobals // read-only package-level lookup table
var onceOpTable = sync.OnceValue(func() map[operationKey]string {
	return map[operationKey]string{
		// /v2/apis
		{segs: segCountAPIs, method: http.MethodPost}: "CreateApi",
		{segs: segCountAPIs, method: http.MethodGet}:  "GetApis",
		{segs: segCountAPIs, method: http.MethodPut}:  "ImportApi",
		// /v2/apis/{apiId}
		{segs: segCountAPIByID, method: http.MethodGet}:    "GetApi",
		{segs: segCountAPIByID, method: http.MethodDelete}: "DeleteApi",
		{segs: segCountAPIByID, method: http.MethodPatch}:  "UpdateApi",
		{segs: segCountAPIByID, method: http.MethodPut}:    "ReimportApi",
		// /v2/apis/{apiId}/cors
		{segs: segCountSubColl, seg1: collCors, method: http.MethodDelete}: "DeleteCorsConfiguration",
		// /v2/apis/{apiId}/stages
		{segs: segCountSubColl, seg1: collStages, method: http.MethodPost}: "CreateStage",
		{segs: segCountSubColl, seg1: collStages, method: http.MethodGet}:  "GetStages",
		// /v2/apis/{apiId}/routes
		{segs: segCountSubColl, seg1: collRoutes, method: http.MethodPost}: "CreateRoute",
		{segs: segCountSubColl, seg1: collRoutes, method: http.MethodGet}:  "GetRoutes",
		// /v2/apis/{apiId}/integrations
		{segs: segCountSubColl, seg1: collIntegrations, method: http.MethodPost}: "CreateIntegration",
		{segs: segCountSubColl, seg1: collIntegrations, method: http.MethodGet}:  "GetIntegrations",
		// /v2/apis/{apiId}/deployments
		{segs: segCountSubColl, seg1: collDeployments, method: http.MethodPost}: "CreateDeployment",
		{segs: segCountSubColl, seg1: collDeployments, method: http.MethodGet}:  "GetDeployments",
		// /v2/apis/{apiId}/authorizers
		{segs: segCountSubColl, seg1: collAuthorizers, method: http.MethodPost}: "CreateAuthorizer",
		{segs: segCountSubColl, seg1: collAuthorizers, method: http.MethodGet}:  "GetAuthorizers",
		// /v2/apis/{apiId}/models
		{segs: segCountSubColl, seg1: collModels, method: http.MethodPost}: "CreateModel",
		{segs: segCountSubColl, seg1: collModels, method: http.MethodGet}:  "GetModels",
		// /v2/apis/{apiId}/stages/{stageName}
		{segs: segCountSubRes, seg1: collStages, method: http.MethodGet}:    "GetStage",
		{segs: segCountSubRes, seg1: collStages, method: http.MethodDelete}: "DeleteStage",
		{segs: segCountSubRes, seg1: collStages, method: http.MethodPatch}:  "UpdateStage",
		// /v2/apis/{apiId}/routes/{routeId}
		{segs: segCountSubRes, seg1: collRoutes, method: http.MethodGet}:    "GetRoute",
		{segs: segCountSubRes, seg1: collRoutes, method: http.MethodDelete}: "DeleteRoute",
		{segs: segCountSubRes, seg1: collRoutes, method: http.MethodPatch}:  "UpdateRoute",
		// /v2/apis/{apiId}/integrations/{integrationId}
		{segs: segCountSubRes, seg1: collIntegrations, method: http.MethodGet}:    "GetIntegration",
		{segs: segCountSubRes, seg1: collIntegrations, method: http.MethodDelete}: "DeleteIntegration",
		{segs: segCountSubRes, seg1: collIntegrations, method: http.MethodPatch}:  "UpdateIntegration",
		// /v2/apis/{apiId}/deployments/{deploymentId}
		{segs: segCountSubRes, seg1: collDeployments, method: http.MethodGet}:    "GetDeployment",
		{segs: segCountSubRes, seg1: collDeployments, method: http.MethodDelete}: "DeleteDeployment",
		// /v2/apis/{apiId}/deployments/{deploymentId} PATCH
		{segs: segCountSubRes, seg1: collDeployments, method: http.MethodPatch}: "UpdateDeployment",
		// /v2/apis/{apiId}/authorizers/{authorizerId}
		{segs: segCountSubRes, seg1: collAuthorizers, method: http.MethodGet}:    "GetAuthorizer",
		{segs: segCountSubRes, seg1: collAuthorizers, method: http.MethodDelete}: "DeleteAuthorizer",
		{segs: segCountSubRes, seg1: collAuthorizers, method: http.MethodPatch}:  "UpdateAuthorizer",
		// /v2/apis/{apiId}/models/{modelId}
		{segs: segCountSubRes, seg1: collModels, method: http.MethodGet}:    "GetModel",
		{segs: segCountSubRes, seg1: collModels, method: http.MethodDelete}: "DeleteModel",
		// /v2/apis/{apiId}/exports/{specification}
		{segs: segCountSubRes, seg1: collExports, method: http.MethodGet}: "ExportApi",
		// /v2/apis/{apiId}/models/{modelId} PATCH
		{segs: segCountSubRes, seg1: collModels, method: http.MethodPatch}: "UpdateModel",
		// /v2/apis/{apiId}/models/{modelId}/template
		{segs: segCountDeepColl, seg1: collTemplate, method: http.MethodGet}: "GetModelTemplate",
		// /v2/apis/{apiId}/integrations/{integrationId}/integrationresponses
		{segs: segCountDeepColl, seg1: collIntegrationResponses, method: http.MethodPost}: "CreateIntegrationResponse",
		{segs: segCountDeepColl, seg1: collIntegrationResponses, method: http.MethodGet}:  "GetIntegrationResponses",
		// /v2/apis/{apiId}/routes/{routeId}/routeresponses
		{segs: segCountDeepColl, seg1: collRouteResponses, method: http.MethodPost}: "CreateRouteResponse",
		{segs: segCountDeepColl, seg1: collRouteResponses, method: http.MethodGet}:  "GetRouteResponses",
		// /v2/apis/{apiId}/integrations/{integrationId}/integrationresponses/{id}
		{segs: segCountDeepRes, seg1: collIntegrationResponses, method: http.MethodGet}:    "GetIntegrationResponse",
		{segs: segCountDeepRes, seg1: collIntegrationResponses, method: http.MethodDelete}: "DeleteIntegrationResponse",
		// /v2/apis/{apiId}/routes/{routeId}/routeresponses/{id}
		{segs: segCountDeepRes, seg1: collRouteResponses, method: http.MethodGet}:    "GetRouteResponse",
		{segs: segCountDeepRes, seg1: collRouteResponses, method: http.MethodDelete}: "DeleteRouteResponse",
		// /v2/apis/{apiId}/stages/{stageName}/accesslogsettings DELETE
		{segs: segCountDeepColl, seg1: collAccessLogSettings, method: http.MethodDelete}: "DeleteAccessLogSettings",
		// /v2/apis/{apiId}/stages/{stageName}/routesettings/{routeKey} DELETE
		{segs: segCountDeepRes, seg1: collRouteSettings, method: http.MethodDelete}: "DeleteRouteSettings",
		// /v2/apis/{apiId}/routes/{routeId}/requestparameters/{requestParameterKey} DELETE
		{segs: segCountDeepRes, seg1: collRequestParameters, method: http.MethodDelete}: "DeleteRouteRequestParameter",
		// /v2/apis/{apiId}/stages/{stageName}/cache/authorizers DELETE
		{segs: segCountDeepRes, seg1: collCache, method: http.MethodDelete}: "ResetAuthorizersCache",
		// /v2/apis/{apiId}/integrations/{integrationId}/integrationresponses/{id} PATCH
		{segs: segCountDeepRes, seg1: collIntegrationResponses, method: http.MethodPatch}: "UpdateIntegrationResponse",
		// /v2/apis/{apiId}/routes/{routeId}/routeresponses/{id} PATCH
		{segs: segCountDeepRes, seg1: collRouteResponses, method: http.MethodPatch}: "UpdateRouteResponse",
	}
})

// ExtractOperation returns the operation name from the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	path := c.Request().URL.Path
	method := c.Request().Method

	// Handle non-/v2/apis paths.
	switch {
	case path == domainNamesPrefix || strings.HasPrefix(path, domainNamesPrefix+"/"):
		return extractDomainNamesOp(path, method)
	case path == portalsPrefix || strings.HasPrefix(path, portalsPrefix+"/"):
		return extractPortalsOp(path, method)
	case path == portalProductsPrefix || strings.HasPrefix(path, portalProductsPrefix+"/"):
		return extractPortalProductsOp(path, method)
	case path == vpcLinksPrefix || strings.HasPrefix(path, vpcLinksPrefix+"/"):
		return extractVpcLinksOp(path, method)
	case strings.HasPrefix(path, tagsPrefix+"/"):
		return extractTagsOp(path, method)
	}

	return extractAPIsOp(path, method)
}

// ExtractResource extracts the API ID from the URL path for metrics.
func (h *Handler) ExtractResource(c *echo.Context) string {
	segs := pathSegments(c.Request().URL.Path)
	if len(segs) > segCountAPIs {
		return segs[segCountAPIs]
	}

	return ""
}

// Handler returns the Echo handler function for API Gateway v2 operations.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		path := c.Request().URL.Path
		method := c.Request().Method

		switch {
		case strings.HasPrefix(path, "/v2proxy/"):
			return h.handleStageProxyEcho(c)
		case isUserRequestPath(path):
			return h.handleUserRequestEcho(c)
		case path == domainNamesPrefix || strings.HasPrefix(path, domainNamesPrefix+"/"):
			return h.handleDomainNamesPath(c, method, path)
		case path == portalsPrefix || strings.HasPrefix(path, portalsPrefix+"/"):
			return h.handlePortalsPath(c, method, path)
		case path == portalProductsPrefix || strings.HasPrefix(path, portalProductsPrefix+"/"):
			return h.handlePortalProductsPath(c, method, path)
		case path == vpcLinksPrefix || strings.HasPrefix(path, vpcLinksPrefix+"/"):
			return h.handleVpcLinksPath(c, method, path)
		case path == apisPathPrefix || strings.HasPrefix(path, apisPathPrefix+"/"):
			return h.handleAPIsPath(c, method, path)
		case strings.HasPrefix(path, tagsPrefix+"/"):
			return h.handleTagsPath(c, method, path)
		default:
			return writeErr(c, http.StatusNotFound, msgNotFound)
		}
	}
}

// subDispatchKey is used to route sub-collection and sub-resource requests.
type subDispatchKey struct {
	method     string
	collection string
}

// newSubCollectionDispatch builds the (method, collection) → handler table for
// /v2/apis/{apiId}/{collection}. It is built once per Handler (in NewHandler)
// using method expressions, so it is not reallocated on every request.
func newSubCollectionDispatch() map[subDispatchKey]func(*Handler, *echo.Context, string) error {
	return map[subDispatchKey]func(*Handler, *echo.Context, string) error{
		{http.MethodPost, collStages}:       (*Handler).handleCreateStage,
		{http.MethodGet, collStages}:        (*Handler).handleGetStages,
		{http.MethodPost, collRoutes}:       (*Handler).handleCreateRoute,
		{http.MethodGet, collRoutes}:        (*Handler).handleGetRoutes,
		{http.MethodPost, collIntegrations}: (*Handler).handleCreateIntegration,
		{http.MethodGet, collIntegrations}:  (*Handler).handleGetIntegrations,
		{http.MethodPost, collDeployments}:  (*Handler).handleCreateDeployment,
		{http.MethodGet, collDeployments}:   (*Handler).handleGetDeployments,
		{http.MethodPost, collAuthorizers}:  (*Handler).handleCreateAuthorizer,
		{http.MethodGet, collAuthorizers}:   (*Handler).handleGetAuthorizers,
		{http.MethodPost, collModels}:       (*Handler).handleCreateModel,
		{http.MethodGet, collModels}:        (*Handler).handleGetModels,
		{http.MethodDelete, collCors}:       (*Handler).handleDeleteCorsConfiguration,
	}
}

// newSubResourceDispatch builds the (method, collection) → handler table for
// /v2/apis/{apiId}/{collection}/{resourceId}. Built once per Handler.
func newSubResourceDispatch() map[subDispatchKey]func(*Handler, *echo.Context, string, string) error {
	return map[subDispatchKey]func(*Handler, *echo.Context, string, string) error{
		{http.MethodGet, collStages}:          (*Handler).handleGetStage,
		{http.MethodDelete, collStages}:       (*Handler).handleDeleteStage,
		{http.MethodPatch, collStages}:        (*Handler).handleUpdateStage,
		{http.MethodGet, collRoutes}:          (*Handler).handleGetRoute,
		{http.MethodDelete, collRoutes}:       (*Handler).handleDeleteRoute,
		{http.MethodPatch, collRoutes}:        (*Handler).handleUpdateRoute,
		{http.MethodGet, collIntegrations}:    (*Handler).handleGetIntegration,
		{http.MethodDelete, collIntegrations}: (*Handler).handleDeleteIntegration,
		{http.MethodPatch, collIntegrations}:  (*Handler).handleUpdateIntegration,
		{http.MethodGet, collDeployments}:     (*Handler).handleGetDeployment,
		{http.MethodDelete, collDeployments}:  (*Handler).handleDeleteDeployment,
		{http.MethodPatch, collDeployments}:   (*Handler).handleUpdateDeployment,
		{http.MethodGet, collAuthorizers}:     (*Handler).handleGetAuthorizer,
		{http.MethodDelete, collAuthorizers}:  (*Handler).handleDeleteAuthorizer,
		{http.MethodPatch, collAuthorizers}:   (*Handler).handleUpdateAuthorizer,
		{http.MethodGet, collModels}:          (*Handler).handleGetModel,
		{http.MethodDelete, collModels}:       (*Handler).handleDeleteModel,
		{http.MethodPatch, collModels}:        (*Handler).handleUpdateModel,
		{http.MethodGet, collExports}:         (*Handler).handleExportAPI,
	}
}

// handleSubCollection handles POST/GET on /v2/apis/{apiId}/{collection}.
func (h *Handler) handleSubCollection(c *echo.Context, method, apiID, collection string) error {
	if fn, ok := h.subCollectionDispatch[subDispatchKey{method, collection}]; ok {
		return fn(h, c, apiID)
	}

	return writeErr(c, http.StatusNotFound, msgNotFound)
}

// handleSubResource handles GET/DELETE/PATCH on /v2/apis/{apiId}/{collection}/{resourceId}.
func (h *Handler) handleSubResource(c *echo.Context, method, apiID, collection, resourceID string) error {
	if fn, ok := h.subResourceDispatch[subDispatchKey{method, collection}]; ok {
		return fn(h, c, apiID, resourceID)
	}

	return writeErr(c, http.StatusNotFound, msgNotFound)
}

// handleDeepCollection handles POST on /v2/apis/{apiId}/{collection}/{resourceId}/{subCollection}.
// This supports integration responses (/integrations/{id}/integrationresponses)
// and route responses (/routes/{id}/routeresponses).
func (h *Handler) handleDeepCollection(
	c *echo.Context,
	method, apiID, _ /* collection */, resourceID, subCollection string,
) error {
	type nestedResourceHandler func(*echo.Context, string, string) error

	dispatch := map[subDispatchKey]nestedResourceHandler{
		{http.MethodPost, collIntegrationResponses}: func(c *echo.Context, apiID, resourceID string) error {
			return handleCreateMulti(c, apiID, "integration response",
				func(input CreateIntegrationResponseInput) (*IntegrationResponse, error) {
					return h.Backend.CreateIntegrationResponse(apiID, resourceID, input)
				},
				ErrAPINotFound, ErrIntegrationNotFound)
		},
		{http.MethodGet, collIntegrationResponses}: func(c *echo.Context, apiID, resourceID string) error {
			return h.integrationResponseOps().handleList(c, apiID, resourceID)
		},
		{http.MethodPost, collRouteResponses}: func(c *echo.Context, apiID, resourceID string) error {
			return handleCreateMulti(c, apiID, "route response",
				func(input CreateRouteResponseInput) (*RouteResponse, error) {
					return h.Backend.CreateRouteResponse(apiID, resourceID, input)
				},
				ErrAPINotFound, ErrRouteNotFound)
		},
		{http.MethodGet, collRouteResponses}: func(c *echo.Context, apiID, resourceID string) error {
			return h.routeResponseOps().handleList(c, apiID, resourceID)
		},
		{http.MethodDelete, collAccessLogSettings}: func(c *echo.Context, apiID, resourceID string) error {
			return h.handleDeleteAccessLogSettings(c, apiID, resourceID)
		},
		{http.MethodGet, collTemplate}: func(c *echo.Context, apiID, resourceID string) error {
			return h.handleGetModelTemplate(c, apiID, resourceID)
		},
	}

	if fn, ok := dispatch[subDispatchKey{method, subCollection}]; ok {
		return fn(c, apiID, resourceID)
	}

	return writeErr(c, http.StatusNotFound, msgNotFound)
}

// handleCreate is a generic helper for Create* handlers that decode a body,
// call a backend function, and return 201 Created on success.
func handleCreate[I, O any](
	c *echo.Context,
	apiID, resourceName string,
	notFoundErr error,
	backendFn func(I) (*O, error),
) error {
	return handleCreateMulti(c, apiID, resourceName, backendFn, notFoundErr)
}

// handleCreateMulti is a generic helper for Create* handlers that supports
// multiple not-found errors.
func handleCreateMulti[I, O any](
	c *echo.Context,
	apiID, resourceName string,
	backendFn func(I) (*O, error),
	notFoundErrs ...error,
) error {
	log := logger.Load(c.Request().Context())

	var input I
	if err := json.NewDecoder(c.Request().Body).Decode(&input); err != nil {
		return writeErr(c, http.StatusBadRequest, msgInvalidBody)
	}

	result, err := backendFn(input)
	if err != nil {
		log.Error("apigatewayv2: create "+resourceName+" failed", logKeyAPIID, apiID, "error", err)

		if errors.Is(err, awserr.ErrAlreadyExists) {
			return writeErr(c, http.StatusConflict, err.Error())
		}

		if errors.Is(err, ErrBadRequest) {
			return writeErr(c, http.StatusBadRequest, err.Error())
		}

		for _, nfe := range notFoundErrs {
			if errors.Is(err, nfe) {
				return writeErr(c, http.StatusNotFound, msgNotFound)
			}
		}

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusCreated, result)
}

// handleUpdate is a generic helper for Update* (PATCH) handlers that decode a body,
// call a backend function, and return 200 OK on success.
func handleUpdate[I, O any](
	c *echo.Context,
	apiID, resourceID, resourceName string,
	backendFn func(I) (*O, error),
	notFoundErrs ...error,
) error {
	log := logger.Load(c.Request().Context())

	var input I
	if err := json.NewDecoder(c.Request().Body).Decode(&input); err != nil {
		return writeErr(c, http.StatusBadRequest, msgInvalidBody)
	}

	result, err := backendFn(input)
	if err != nil {
		log.Error("apigatewayv2: update "+resourceName+" failed",
			logKeyAPIID, apiID, "resourceId", resourceID, "error", err)

		if errors.Is(err, ErrBadRequest) {
			return writeErr(c, http.StatusBadRequest, err.Error())
		}

		for _, nfe := range notFoundErrs {
			if errors.Is(err, nfe) {
				return writeErr(c, http.StatusNotFound, msgNotFound)
			}
		}

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, result)
}

// apigwDefaultPageSize is the default page size for API Gateway v2 list operations.
const apigwDefaultPageSize = 500

// apigwPaginationParams extracts maxResults and nextToken from query parameters.
func apigwPaginationParams(c *echo.Context) (int, string) {
	q := c.Request().URL.Query()
	maxResults := 0

	if s := q.Get("maxResults"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			maxResults = n
		}
	}

	return maxResults, q.Get("nextToken")
}

// handleGetList is a generic helper for list (GET collection) handlers.
func handleGetList[T any](
	c *echo.Context,
	apiID, resourceName string,
	backendFn func() ([]T, error),
	wrapFn func([]T, string) any,
) error {
	log := logger.Load(c.Request().Context())

	items, err := backendFn()
	if err != nil {
		log.Error("apigatewayv2: get "+resourceName+" failed", logKeyAPIID, apiID, "error", err)

		if errors.Is(err, ErrAPINotFound) {
			return writeErr(c, http.StatusNotFound, msgNotFound)
		}

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	maxResults, nextToken := apigwPaginationParams(c)
	p := page.New(items, nextToken, maxResults, apigwDefaultPageSize)

	return c.JSON(http.StatusOK, wrapFn(p.Data, p.Next))
}

// handleCreateNoParent is a generic helper for top-level Create* handlers (no parent resource).
func handleCreateNoParent[I, O any](
	c *echo.Context,
	resourceName string,
	backendFn func(I) (*O, error),
) error {
	log := logger.Load(c.Request().Context())

	var input I
	if err := json.NewDecoder(c.Request().Body).Decode(&input); err != nil {
		return writeErr(c, http.StatusBadRequest, msgInvalidBody)
	}

	result, err := backendFn(input)
	if err != nil {
		log.Error("apigatewayv2: create "+resourceName+" failed", "error", err)

		if errors.Is(err, awserr.ErrAlreadyExists) {
			return writeErr(c, http.StatusConflict, err.Error())
		}

		if errors.Is(err, ErrBadRequest) {
			return writeErr(c, http.StatusBadRequest, err.Error())
		}

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusCreated, result)
}

// handleGetChildList is a generic helper for GET-collection handlers on a
// resource nested two levels deep (e.g. GetIntegrationResponses,
// GetRouteResponses) that do not paginate.
func handleGetChildList[T any](
	c *echo.Context,
	logMsg string,
	logArgs []any,
	backendFn func() ([]T, error),
	wrapFn func([]T) any,
	notFoundErrs ...error,
) error {
	log := logger.Load(c.Request().Context())

	items, err := backendFn()
	if err != nil {
		log.Error(logMsg, append(logArgs, "error", err)...)

		for _, nfe := range notFoundErrs {
			if errors.Is(err, nfe) {
				return writeErr(c, http.StatusNotFound, msgNotFound)
			}
		}

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, wrapFn(items))
}

// handleGetChild is a generic helper for GET-single handlers on a resource
// nested two levels deep.
func handleGetChild[T any](
	c *echo.Context,
	logMsg string,
	logArgs []any,
	backendFn func() (*T, error),
	notFoundErrs ...error,
) error {
	log := logger.Load(c.Request().Context())

	item, err := backendFn()
	if err != nil {
		log.Error(logMsg, append(logArgs, "error", err)...)

		for _, nfe := range notFoundErrs {
			if errors.Is(err, nfe) {
				return writeErr(c, http.StatusNotFound, msgNotFound)
			}
		}

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, item)
}

// handleDeleteChild is a generic helper for DELETE handlers on a resource
// nested two levels deep.
func handleDeleteChild(
	c *echo.Context,
	logMsg string,
	logArgs []any,
	backendFn func() error,
	notFoundErrs ...error,
) error {
	log := logger.Load(c.Request().Context())

	if err := backendFn(); err != nil {
		log.Error(logMsg, append(logArgs, "error", err)...)

		for _, nfe := range notFoundErrs {
			if errors.Is(err, nfe) {
				return writeErr(c, http.StatusNotFound, msgNotFound)
			}
		}

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	return c.NoContent(http.StatusNoContent)
}

// nestedResponseOps bundles the backend calls and error classification needed
// to implement the four handlers (list/get/delete/update) for a "response"
// resource nested two levels under an API. IntegrationResponse (under an
// integration) and RouteResponse (under a route) share this exact shape, so
// this single generic implementation replaces what would otherwise be two
// hand-duplicated copies of the same four handlers.
type nestedResponseOps[T, U any] struct {
	selfNotFound error
	list         func(apiID, parentID string) ([]T, error)
	wrapList     func([]T) any
	get          func(apiID, parentID, id string) (*T, error)
	del          func(apiID, parentID, id string) error
	update       func(apiID, parentID, id string, input U) (*T, error)
	kind         string
	parentIDKey  string
	notFoundErrs []error
}

func (ops nestedResponseOps[T, U]) handleList(c *echo.Context, apiID, parentID string) error {
	return handleGetChildList(c,
		"apigatewayv2: get "+ops.kind+"s failed", []any{logKeyAPIID, apiID, ops.parentIDKey, parentID},
		func() ([]T, error) { return ops.list(apiID, parentID) },
		ops.wrapList,
		ops.notFoundErrs...)
}

func (ops nestedResponseOps[T, U]) handleGet(c *echo.Context, apiID, parentID, id string) error {
	return handleGetChild(c,
		"apigatewayv2: get "+ops.kind+" failed",
		[]any{logKeyAPIID, apiID, ops.parentIDKey, parentID, "responseId", id},
		func() (*T, error) { return ops.get(apiID, parentID, id) },
		append(slices.Clone(ops.notFoundErrs), ops.selfNotFound)...)
}

func (ops nestedResponseOps[T, U]) handleDelete(c *echo.Context, apiID, parentID, id string) error {
	return handleDeleteChild(c,
		"apigatewayv2: delete "+ops.kind+" failed",
		[]any{logKeyAPIID, apiID, ops.parentIDKey, parentID, "responseId", id},
		func() error { return ops.del(apiID, parentID, id) },
		append(slices.Clone(ops.notFoundErrs), ops.selfNotFound)...)
}

func (ops nestedResponseOps[T, U]) handleUpdate(c *echo.Context, apiID, parentID, id string) error {
	return handleUpdate(c, apiID, id, ops.kind,
		func(input U) (*T, error) { return ops.update(apiID, parentID, id, input) },
		append(slices.Clone(ops.notFoundErrs), ops.selfNotFound)...)
}

// integrationResponseOps returns the nestedResponseOps binding for
// IntegrationResponse, nested under an integration.
func (h *Handler) integrationResponseOps() nestedResponseOps[IntegrationResponse, UpdateIntegrationResponseInput] {
	return nestedResponseOps[IntegrationResponse, UpdateIntegrationResponseInput]{
		kind:         "integration response",
		parentIDKey:  "integrationId",
		list:         h.Backend.GetIntegrationResponses,
		wrapList:     func(items []IntegrationResponse) any { return listIntegrationResponsesOutput{Items: items} },
		get:          h.Backend.GetIntegrationResponse,
		del:          h.Backend.DeleteIntegrationResponse,
		update:       h.Backend.UpdateIntegrationResponse,
		notFoundErrs: []error{ErrAPINotFound, ErrIntegrationNotFound},
		selfNotFound: ErrIntegrationResponseNotFound,
	}
}

// routeResponseOps returns the nestedResponseOps binding for RouteResponse,
// nested under a route.
func (h *Handler) routeResponseOps() nestedResponseOps[RouteResponse, UpdateRouteResponseInput] {
	return nestedResponseOps[RouteResponse, UpdateRouteResponseInput]{
		kind:         "route response",
		parentIDKey:  "routeId",
		list:         h.Backend.GetRouteResponses,
		wrapList:     func(items []RouteResponse) any { return listRouteResponsesOutput{Items: items} },
		get:          h.Backend.GetRouteResponse,
		del:          h.Backend.DeleteRouteResponse,
		update:       h.Backend.UpdateRouteResponse,
		notFoundErrs: []error{ErrAPINotFound, ErrRouteNotFound},
		selfNotFound: ErrRouteResponseNotFound,
	}
}

// pathSegments strips the /v2/apis prefix and returns the remaining path segments.
// For example: /v2/apis/abc123/stages/prod → ["abc123", "stages", "prod"].
func pathSegments(path string) []string {
	trimmed := strings.TrimPrefix(path, apisPathPrefix)
	trimmed = strings.Trim(trimmed, "/")

	if trimmed == "" {
		return []string{}
	}

	return strings.Split(trimmed, "/")
}

// handleDeepResource handles GET/DELETE on /v2/apis/{apiId}/{coll}/{resourceId}/{subColl}/{subResourceId}.
func (h *Handler) handleDeepResource(
	c *echo.Context,
	method, apiID, parentCollection, resourceID, subCollection, subResourceID string,
) error {
	if method == http.MethodDelete && subCollection == collRouteSettings && parentCollection == collStages {
		return h.handleDeleteRouteSettings(c, apiID, resourceID, subResourceID)
	}
	if method == http.MethodDelete && subCollection == collRequestParameters && parentCollection == collRoutes {
		return h.handleDeleteRouteRequestParameter(c, apiID, resourceID, subResourceID)
	}
	if method == http.MethodDelete && subCollection == collCache && subResourceID == "authorizers" {
		return h.handleResetAuthorizersCache(c, apiID, resourceID)
	}

	type threeArgHandler func(*echo.Context, string, string, string) error

	dispatch := map[subDispatchKey]threeArgHandler{
		{http.MethodGet, collIntegrationResponses}: func(c *echo.Context, apiID, integrationID, responseID string) error {
			return h.integrationResponseOps().handleGet(c, apiID, integrationID, responseID)
		},
		{http.MethodDelete, collIntegrationResponses}: func(c *echo.Context, apiID, integrationID, responseID string) error {
			return h.integrationResponseOps().handleDelete(c, apiID, integrationID, responseID)
		},
		{http.MethodPatch, collIntegrationResponses}: func(c *echo.Context, apiID, integrationID, responseID string) error {
			return h.integrationResponseOps().handleUpdate(c, apiID, integrationID, responseID)
		},
		{http.MethodGet, collRouteResponses}: func(c *echo.Context, apiID, routeID, responseID string) error {
			return h.routeResponseOps().handleGet(c, apiID, routeID, responseID)
		},
		{http.MethodDelete, collRouteResponses}: func(c *echo.Context, apiID, routeID, responseID string) error {
			return h.routeResponseOps().handleDelete(c, apiID, routeID, responseID)
		},
		{http.MethodPatch, collRouteResponses}: func(c *echo.Context, apiID, routeID, responseID string) error {
			return h.routeResponseOps().handleUpdate(c, apiID, routeID, responseID)
		},
	}

	if fn, ok := dispatch[subDispatchKey{method, subCollection}]; ok {
		return fn(c, apiID, resourceID, subResourceID)
	}

	return writeErr(c, http.StatusNotFound, msgNotFound)
}
