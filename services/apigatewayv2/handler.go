package apigatewayv2

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	apigwV2MatchPriority = service.PriorityPathVersioned
	apisPathPrefix       = "/v2/apis"
	domainNamesPrefix    = "/v2/domainnames"
	portalsPrefix        = "/v2/portals"
	portalProductsPrefix = "/v2/portalproducts"

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
	collProductPages         = "productpages"
	collProductREPages       = "productrestendpointpages"

	// error messages.
	msgNotFound         = "Not Found"
	msgMethodNotAllowed = "Method Not Allowed"
	msgInvalidBody      = "invalid request body"

	// opUnknown is returned when no matching operation is found.
	opUnknown = "Unknown"
)

// Handler is the Echo HTTP handler for API Gateway v2 (HTTP API) operations.
type Handler struct {
	Backend StorageBackend
}

// NewHandler creates a new API Gateway v2 Handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{Backend: backend}
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
		"CreateIntegrationResponse", "GetIntegrationResponse", "GetIntegrationResponses", "DeleteIntegrationResponse",
		"CreateModel", "GetModel", "GetModels", "DeleteModel",
		"CreatePortal", "GetPortal", "ListPortals",
		"CreatePortalProduct", "GetPortalProduct", "ListPortalProducts",
		"CreateProductPage", "ListProductPages",
		"CreateProductRestEndpointPage", "ListProductRestEndpointPages",
		"CreateRouteResponse", "GetRouteResponse", "GetRouteResponses", "DeleteRouteResponse",
		"GetTags", "TagResource", "UntagResource",
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
			strings.HasPrefix(path, tagsPrefix+"/")
	}
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
		// /v2/apis/{apiId}
		{segs: segCountAPIByID, method: http.MethodGet}:    "GetApi",
		{segs: segCountAPIByID, method: http.MethodDelete}: "DeleteApi",
		{segs: segCountAPIByID, method: http.MethodPatch}:  "UpdateApi",
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
		// /v2/apis/{apiId}/authorizers/{authorizerId}
		{segs: segCountSubRes, seg1: collAuthorizers, method: http.MethodGet}:    "GetAuthorizer",
		{segs: segCountSubRes, seg1: collAuthorizers, method: http.MethodDelete}: "DeleteAuthorizer",
		{segs: segCountSubRes, seg1: collAuthorizers, method: http.MethodPatch}:  "UpdateAuthorizer",
		// /v2/apis/{apiId}/models/{modelId}
		{segs: segCountSubRes, seg1: collModels, method: http.MethodGet}:    "GetModel",
		{segs: segCountSubRes, seg1: collModels, method: http.MethodDelete}: "DeleteModel",
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
	case strings.HasPrefix(path, tagsPrefix+"/"):
		return extractTagsOp(path, method)
	}

	return extractAPIsOp(path, method)
}

func extractDomainNamesOp(path, method string) string {
	suffix := strings.Trim(strings.TrimPrefix(path, domainNamesPrefix), "/")

	if suffix == "" {
		switch method {
		case http.MethodPost:
			return "CreateDomainName"
		case http.MethodGet:
			return "GetDomainNames"
		}

		return opUnknown
	}

	parts := strings.SplitN(suffix, "/", maxPathParts)

	switch len(parts) {
	case pathPartOne:
		switch method {
		case http.MethodGet:
			return "GetDomainName"
		case http.MethodDelete:
			return "DeleteDomainName"
		}
	case pathPartTwo:
		if parts[1] == collAPIMappings {
			switch method {
			case http.MethodPost:
				return "CreateApiMapping"
			case http.MethodGet:
				return "GetApiMappings"
			}
		}
	case maxPathParts:
		if parts[1] == collAPIMappings {
			switch method {
			case http.MethodGet:
				return "GetApiMapping"
			case http.MethodDelete:
				return "DeleteApiMapping"
			}
		}
	}

	return opUnknown
}

func extractPortalsOp(path, method string) string {
	suffix := strings.Trim(strings.TrimPrefix(path, portalsPrefix), "/")

	if suffix == "" {
		switch method {
		case http.MethodPost:
			return "CreatePortal"
		case http.MethodGet:
			return "ListPortals"
		}

		return opUnknown
	}

	parts := strings.SplitN(suffix, "/", maxPathParts)
	if len(parts) == 1 && method == http.MethodGet {
		return "GetPortal"
	}

	return opUnknown
}

func extractPortalProductsOp(path, method string) string {
	suffix := strings.Trim(strings.TrimPrefix(path, portalProductsPrefix), "/")

	if suffix == "" {
		switch method {
		case http.MethodPost:
			return "CreatePortalProduct"
		case http.MethodGet:
			return "ListPortalProducts"
		}

		return opUnknown
	}

	parts := strings.SplitN(suffix, "/", maxPathParts)

	switch len(parts) {
	case pathPartOne:
		if method == http.MethodGet {
			return "GetPortalProduct"
		}
	case pathPartTwo:
		switch parts[1] {
		case collProductPages:
			switch method {
			case http.MethodPost:
				return "CreateProductPage"
			case http.MethodGet:
				return "ListProductPages"
			}
		case collProductREPages:
			switch method {
			case http.MethodPost:
				return "CreateProductRestEndpointPage"
			case http.MethodGet:
				return "ListProductRestEndpointPages"
			}
		}
	}

	return opUnknown
}

func extractAPIsOp(path, method string) string {
	segs := pathSegments(path)
	nsegs := len(segs)

	var seg1 string

	switch {
	case nsegs == segCountDeepRes:
		// e.g. ["{apiId}", "integrations", "{id}", "integrationresponses", "{responseId}"]
		seg1 = segs[segCountDeepColl-1]
	case nsegs == segCountDeepColl:
		// pathSegments strips the /v2/apis prefix, so segs[3] is the 4th element
		// (0-indexed last) of e.g. ["{apiId}", "integrations", "{id}", "integrationresponses"].
		seg1 = segs[segCountDeepColl-1]
	case nsegs >= segCountSubColl:
		seg1 = segs[segCountSubColl-1]
	}

	key := operationKey{segs: nsegs, seg1: seg1, method: method}

	if op, ok := onceOpTable()[key]; ok {
		return op
	}

	return opUnknown
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
		case path == domainNamesPrefix || strings.HasPrefix(path, domainNamesPrefix+"/"):
			return h.handleDomainNamesPath(c, method, path)
		case path == portalsPrefix || strings.HasPrefix(path, portalsPrefix+"/"):
			return h.handlePortalsPath(c, method, path)
		case path == portalProductsPrefix || strings.HasPrefix(path, portalProductsPrefix+"/"):
			return h.handlePortalProductsPath(c, method, path)
		case path == apisPathPrefix || strings.HasPrefix(path, apisPathPrefix+"/"):
			return h.handleAPIsPath(c, method, path)
		case strings.HasPrefix(path, tagsPrefix+"/"):
			return h.handleTagsPath(c, method, path)
		default:
			return c.JSON(http.StatusNotFound, notFoundResponse{Message: msgNotFound})
		}
	}
}

// handleAPIsPath dispatches requests under the /v2/apis prefix.
func (h *Handler) handleAPIsPath(c *echo.Context, method, path string) error {
	segs := pathSegments(path)

	switch len(segs) {
	case segCountAPIs:
		return h.handleAPIs(c, method)
	case segCountAPIByID:
		return h.handleAPI(c, method, segs[0])
	case segCountSubColl:
		return h.handleSubCollection(c, method, segs[0], segs[1])
	case segCountSubRes:
		return h.handleSubResource(c, method, segs[0], segs[1], segs[2])
	case segCountDeepColl:
		return h.handleDeepCollection(c, method, segs[0], segs[1], segs[2], segs[3])
	case segCountDeepRes:
		return h.handleDeepResource(c, method, segs[0], segs[1], segs[2], segs[3], segs[4])
	default:
		logger.Load(c.Request().Context()).Warn("apigatewayv2: unhandled path", "path", path)

		return c.JSON(http.StatusNotFound, notFoundResponse{Message: msgNotFound})
	}
}

// handleAPIs handles POST /v2/apis and GET /v2/apis.
func (h *Handler) handleAPIs(c *echo.Context, method string) error {
	switch method {
	case http.MethodPost:
		return h.handleCreateAPI(c)
	case http.MethodGet:
		return h.handleGetAPIs(c)
	default:
		return c.JSON(http.StatusMethodNotAllowed, notFoundResponse{Message: msgMethodNotAllowed})
	}
}

// handleAPI handles /v2/apis/{apiId}.
func (h *Handler) handleAPI(c *echo.Context, method, apiID string) error {
	switch method {
	case http.MethodGet:
		return h.handleGetAPI(c, apiID)
	case http.MethodDelete:
		return h.handleDeleteAPI(c, apiID)
	case http.MethodPatch:
		return h.handleUpdateAPI(c, apiID)
	default:
		return c.JSON(http.StatusMethodNotAllowed, notFoundResponse{Message: msgMethodNotAllowed})
	}
}

// subDispatchKey is used to route sub-collection and sub-resource requests.
type subDispatchKey struct {
	method     string
	collection string
}

// handleSubCollection handles POST/GET on /v2/apis/{apiId}/{collection}.
func (h *Handler) handleSubCollection(c *echo.Context, method, apiID, collection string) error {
	dispatch := map[subDispatchKey]func(*echo.Context, string) error{
		{http.MethodPost, collStages}:       h.handleCreateStage,
		{http.MethodGet, collStages}:        h.handleGetStages,
		{http.MethodPost, collRoutes}:       h.handleCreateRoute,
		{http.MethodGet, collRoutes}:        h.handleGetRoutes,
		{http.MethodPost, collIntegrations}: h.handleCreateIntegration,
		{http.MethodGet, collIntegrations}:  h.handleGetIntegrations,
		{http.MethodPost, collDeployments}:  h.handleCreateDeployment,
		{http.MethodGet, collDeployments}:   h.handleGetDeployments,
		{http.MethodPost, collAuthorizers}:  h.handleCreateAuthorizer,
		{http.MethodGet, collAuthorizers}:   h.handleGetAuthorizers,
		{http.MethodPost, collModels}:       h.handleCreateModel,
		{http.MethodGet, collModels}:        h.handleGetModels,
	}

	if fn, ok := dispatch[subDispatchKey{method, collection}]; ok {
		return fn(c, apiID)
	}

	return c.JSON(http.StatusNotFound, notFoundResponse{Message: msgNotFound})
}

// handleSubResource handles GET/DELETE/PATCH on /v2/apis/{apiId}/{collection}/{resourceId}.
func (h *Handler) handleSubResource(c *echo.Context, method, apiID, collection, resourceID string) error {
	type twoArgHandler func(*echo.Context, string, string) error

	dispatch := map[subDispatchKey]twoArgHandler{
		{http.MethodGet, collStages}:          h.handleGetStage,
		{http.MethodDelete, collStages}:       h.handleDeleteStage,
		{http.MethodPatch, collStages}:        h.handleUpdateStage,
		{http.MethodGet, collRoutes}:          h.handleGetRoute,
		{http.MethodDelete, collRoutes}:       h.handleDeleteRoute,
		{http.MethodPatch, collRoutes}:        h.handleUpdateRoute,
		{http.MethodGet, collIntegrations}:    h.handleGetIntegration,
		{http.MethodDelete, collIntegrations}: h.handleDeleteIntegration,
		{http.MethodPatch, collIntegrations}:  h.handleUpdateIntegration,
		{http.MethodGet, collDeployments}:     h.handleGetDeployment,
		{http.MethodDelete, collDeployments}:  h.handleDeleteDeployment,
		{http.MethodGet, collAuthorizers}:     h.handleGetAuthorizer,
		{http.MethodDelete, collAuthorizers}:  h.handleDeleteAuthorizer,
		{http.MethodPatch, collAuthorizers}:   h.handleUpdateAuthorizer,
		{http.MethodGet, collModels}:          h.handleGetModel,
		{http.MethodDelete, collModels}:       h.handleDeleteModel,
	}

	if fn, ok := dispatch[subDispatchKey{method, collection}]; ok {
		return fn(c, apiID, resourceID)
	}

	return c.JSON(http.StatusNotFound, notFoundResponse{Message: msgNotFound})
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
			return h.handleGetIntegrationResponses(c, apiID, resourceID)
		},
		{http.MethodPost, collRouteResponses}: func(c *echo.Context, apiID, resourceID string) error {
			return handleCreateMulti(c, apiID, "route response",
				func(input CreateRouteResponseInput) (*RouteResponse, error) {
					return h.Backend.CreateRouteResponse(apiID, resourceID, input)
				},
				ErrAPINotFound, ErrRouteNotFound)
		},
		{http.MethodGet, collRouteResponses}: func(c *echo.Context, apiID, resourceID string) error {
			return h.handleGetRouteResponses(c, apiID, resourceID)
		},
	}

	if fn, ok := dispatch[subDispatchKey{method, subCollection}]; ok {
		return fn(c, apiID, resourceID)
	}

	return c.JSON(http.StatusNotFound, notFoundResponse{Message: msgNotFound})
}

// handleDomainNamesPath handles requests for /v2/domainnames and /v2/domainnames/{domainName}/...
func (h *Handler) handleDomainNamesPath(c *echo.Context, method, path string) error {
	suffix := strings.TrimPrefix(path, domainNamesPrefix)
	suffix = strings.Trim(suffix, "/")

	if suffix == "" {
		switch method {
		case http.MethodPost:
			return handleCreateNoParent(c, "domain name", func(input CreateDomainNameInput) (*DomainName, error) {
				return h.Backend.CreateDomainName(input)
			})
		case http.MethodGet:
			return h.handleGetDomainNames(c)
		default:
			return c.JSON(http.StatusMethodNotAllowed, notFoundResponse{Message: msgMethodNotAllowed})
		}
	}

	parts := strings.Split(suffix, "/")

	switch len(parts) {
	case pathPartOne:
		domainName := parts[0]
		switch method {
		case http.MethodGet:
			return h.handleGetDomainName(c, domainName)
		case http.MethodDelete:
			return h.handleDeleteDomainName(c, domainName)
		}
	case pathPartTwo:
		if parts[1] == collAPIMappings {
			return h.handleAPIMappingsCollection(c, method, parts[0])
		}
	case maxPathParts:
		if parts[1] == collAPIMappings {
			return h.handleAPIMappingResource(c, method, parts[0], parts[pathPartTwo])
		}
	}

	return c.JSON(http.StatusNotFound, notFoundResponse{Message: msgNotFound})
}

func (h *Handler) handleAPIMappingsCollection(c *echo.Context, method, domainName string) error {
	switch method {
	case http.MethodPost:
		return handleCreateMulti(c, domainName, "api mapping",
			func(input CreateAPIMappingInput) (*APIMapping, error) {
				return h.Backend.CreateAPIMapping(domainName, input)
			},
			ErrDomainNameNotFound, ErrAPINotFound, ErrStageNotFound)
	case http.MethodGet:
		return h.handleGetAPIMappings(c, domainName)
	}

	return c.JSON(http.StatusNotFound, notFoundResponse{Message: msgNotFound})
}

func (h *Handler) handleAPIMappingResource(c *echo.Context, method, domainName, mappingID string) error {
	switch method {
	case http.MethodGet:
		return h.handleGetAPIMapping(c, domainName, mappingID)
	case http.MethodDelete:
		return h.handleDeleteAPIMapping(c, domainName, mappingID)
	}

	return c.JSON(http.StatusNotFound, notFoundResponse{Message: msgNotFound})
}

// handlePortalsPath handles requests for /v2/portals and /v2/portals/{portalId}/...
func (h *Handler) handlePortalsPath(c *echo.Context, method, path string) error {
	suffix := strings.TrimPrefix(path, portalsPrefix)
	suffix = strings.Trim(suffix, "/")

	if suffix == "" {
		switch method {
		case http.MethodPost:
			return handleCreateNoParent(c, "portal", func(input CreatePortalInput) (*Portal, error) {
				return h.Backend.CreatePortal(input)
			})
		case http.MethodGet:
			return h.handleListPortals(c)
		default:
			return c.JSON(http.StatusMethodNotAllowed, notFoundResponse{Message: msgMethodNotAllowed})
		}
	}

	parts := strings.Split(suffix, "/")
	if len(parts) == 1 && method == http.MethodGet {
		return h.handleGetPortal(c, parts[0])
	}

	return c.JSON(http.StatusNotFound, notFoundResponse{Message: msgNotFound})
}

// handlePortalProductsPath handles requests for /v2/portalproducts and nested paths.
func (h *Handler) handlePortalProductsPath(c *echo.Context, method, path string) error {
	suffix := strings.TrimPrefix(path, portalProductsPrefix)
	suffix = strings.Trim(suffix, "/")

	if suffix == "" {
		switch method {
		case http.MethodPost:
			return handleCreateNoParent(
				c,
				"portal product",
				func(input CreatePortalProductInput) (*PortalProduct, error) {
					return h.Backend.CreatePortalProduct(input)
				},
			)
		case http.MethodGet:
			return h.handleListPortalProducts(c)
		default:
			return c.JSON(http.StatusMethodNotAllowed, notFoundResponse{Message: msgMethodNotAllowed})
		}
	}

	parts := strings.Split(suffix, "/")

	switch len(parts) {
	case pathPartOne:
		if method == http.MethodGet {
			return h.handleGetPortalProduct(c, parts[0])
		}
	case pathPartTwo:
		portalProductID := parts[0]
		subColl := parts[1]

		switch method {
		case http.MethodPost:
			switch subColl {
			case collProductPages:
				return handleCreate(c, portalProductID, "product page", ErrPortalProductNotFound,
					func(input CreateProductPageInput) (*ProductPage, error) {
						return h.Backend.CreateProductPage(portalProductID, input)
					})
			case collProductREPages:
				return handleCreate(c, portalProductID, "product rest endpoint page", ErrPortalProductNotFound,
					func(input CreateProductRestEndpointPageInput) (*ProductRestEndpointPage, error) {
						return h.Backend.CreateProductRestEndpointPage(portalProductID, input)
					})
			}
		case http.MethodGet:
			switch subColl {
			case collProductPages:
				return h.handleListProductPages(c, portalProductID)
			case collProductREPages:
				return h.handleListProductRestEndpointPages(c, portalProductID)
			}
		}
	}

	return c.JSON(http.StatusNotFound, notFoundResponse{Message: msgNotFound})
}

func (h *Handler) handleCreateAPI(c *echo.Context) error {
	log := logger.Load(c.Request().Context())

	var input CreateAPIInput
	if err := json.NewDecoder(c.Request().Body).Decode(&input); err != nil {
		return c.JSON(http.StatusBadRequest, notFoundResponse{Message: msgInvalidBody})
	}

	api, err := h.Backend.CreateAPI(input)
	if err != nil {
		log.Error("apigatewayv2: create api failed", "error", err)

		return c.JSON(http.StatusInternalServerError, notFoundResponse{Message: err.Error()})
	}

	return c.JSON(http.StatusCreated, api)
}

func (h *Handler) handleGetAPIs(c *echo.Context) error {
	log := logger.Load(c.Request().Context())

	apis, err := h.Backend.GetAPIs()
	if err != nil {
		log.Error("apigatewayv2: get apis failed", "error", err)

		return c.JSON(http.StatusInternalServerError, notFoundResponse{Message: err.Error()})
	}

	return c.JSON(http.StatusOK, listApisOutput{Items: apis})
}

func (h *Handler) handleGetAPI(c *echo.Context, apiID string) error {
	log := logger.Load(c.Request().Context())

	api, err := h.Backend.GetAPI(apiID)
	if err != nil {
		log.Error("apigatewayv2: get api failed", "apiId", apiID, "error", err)

		if errors.Is(err, ErrAPINotFound) {
			return c.JSON(http.StatusNotFound, notFoundResponse{Message: msgNotFound})
		}

		return c.JSON(http.StatusInternalServerError, notFoundResponse{Message: err.Error()})
	}

	return c.JSON(http.StatusOK, api)
}

func (h *Handler) handleDeleteAPI(c *echo.Context, apiID string) error {
	log := logger.Load(c.Request().Context())

	if err := h.Backend.DeleteAPI(apiID); err != nil {
		log.Error("apigatewayv2: delete api failed", "apiId", apiID, "error", err)

		if errors.Is(err, ErrAPINotFound) {
			return c.JSON(http.StatusNotFound, notFoundResponse{Message: msgNotFound})
		}

		return c.JSON(http.StatusInternalServerError, notFoundResponse{Message: err.Error()})
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleUpdateAPI(c *echo.Context, apiID string) error {
	log := logger.Load(c.Request().Context())

	var input UpdateAPIInput
	if err := json.NewDecoder(c.Request().Body).Decode(&input); err != nil {
		return c.JSON(http.StatusBadRequest, notFoundResponse{Message: msgInvalidBody})
	}

	api, err := h.Backend.UpdateAPI(apiID, input)
	if err != nil {
		log.Error("apigatewayv2: update api failed", "apiId", apiID, "error", err)

		if errors.Is(err, ErrAPINotFound) {
			return c.JSON(http.StatusNotFound, notFoundResponse{Message: msgNotFound})
		}

		return c.JSON(http.StatusInternalServerError, notFoundResponse{Message: err.Error()})
	}

	return c.JSON(http.StatusOK, api)
}

// --- Stage handlers ---

func (h *Handler) handleCreateStage(c *echo.Context, apiID string) error {
	return handleCreate(c, apiID, "stage", ErrAPINotFound, func(input CreateStageInput) (*Stage, error) {
		return h.Backend.CreateStage(apiID, input)
	})
}

func (h *Handler) handleGetStages(c *echo.Context, apiID string) error {
	return handleGetList(c, apiID, "stages", func() ([]Stage, error) {
		return h.Backend.GetStages(apiID)
	}, func(items []Stage) any { return listStagesOutput{Items: items} })
}

func (h *Handler) handleGetStage(c *echo.Context, apiID, stageName string) error {
	log := logger.Load(c.Request().Context())

	stage, err := h.Backend.GetStage(apiID, stageName)
	if err != nil {
		log.Error("apigatewayv2: get stage failed", "apiId", apiID, "stageName", stageName, "error", err)

		if errors.Is(err, ErrAPINotFound) || errors.Is(err, ErrStageNotFound) {
			return c.JSON(http.StatusNotFound, notFoundResponse{Message: msgNotFound})
		}

		return c.JSON(http.StatusInternalServerError, notFoundResponse{Message: err.Error()})
	}

	return c.JSON(http.StatusOK, stage)
}

func (h *Handler) handleDeleteStage(c *echo.Context, apiID, stageName string) error {
	log := logger.Load(c.Request().Context())

	if err := h.Backend.DeleteStage(apiID, stageName); err != nil {
		log.Error("apigatewayv2: delete stage failed", "apiId", apiID, "stageName", stageName, "error", err)

		if errors.Is(err, ErrAPINotFound) || errors.Is(err, ErrStageNotFound) {
			return c.JSON(http.StatusNotFound, notFoundResponse{Message: msgNotFound})
		}

		return c.JSON(http.StatusInternalServerError, notFoundResponse{Message: err.Error()})
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleUpdateStage(c *echo.Context, apiID, stageName string) error {
	return handleUpdate(c, apiID, stageName, "stage",
		func(input UpdateStageInput) (*Stage, error) {
			return h.Backend.UpdateStage(apiID, stageName, input)
		},
		ErrAPINotFound, ErrStageNotFound)
}

// --- Route handlers ---

func (h *Handler) handleCreateRoute(c *echo.Context, apiID string) error {
	return handleCreate(c, apiID, "route", ErrAPINotFound, func(input CreateRouteInput) (*Route, error) {
		return h.Backend.CreateRoute(apiID, input)
	})
}

func (h *Handler) handleGetRoutes(c *echo.Context, apiID string) error {
	return handleGetList(c, apiID, "routes", func() ([]Route, error) {
		return h.Backend.GetRoutes(apiID)
	}, func(items []Route) any { return listRoutesOutput{Items: items} })
}

func (h *Handler) handleGetRoute(c *echo.Context, apiID, routeID string) error {
	log := logger.Load(c.Request().Context())

	route, err := h.Backend.GetRoute(apiID, routeID)
	if err != nil {
		log.Error("apigatewayv2: get route failed", "apiId", apiID, "routeId", routeID, "error", err)

		if errors.Is(err, ErrAPINotFound) || errors.Is(err, ErrRouteNotFound) {
			return c.JSON(http.StatusNotFound, notFoundResponse{Message: msgNotFound})
		}

		return c.JSON(http.StatusInternalServerError, notFoundResponse{Message: err.Error()})
	}

	return c.JSON(http.StatusOK, route)
}

func (h *Handler) handleDeleteRoute(c *echo.Context, apiID, routeID string) error {
	log := logger.Load(c.Request().Context())

	if err := h.Backend.DeleteRoute(apiID, routeID); err != nil {
		log.Error("apigatewayv2: delete route failed", "apiId", apiID, "routeId", routeID, "error", err)

		if errors.Is(err, ErrAPINotFound) || errors.Is(err, ErrRouteNotFound) {
			return c.JSON(http.StatusNotFound, notFoundResponse{Message: msgNotFound})
		}

		return c.JSON(http.StatusInternalServerError, notFoundResponse{Message: err.Error()})
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleUpdateRoute(c *echo.Context, apiID, routeID string) error {
	return handleUpdate(c, apiID, routeID, "route",
		func(input UpdateRouteInput) (*Route, error) {
			return h.Backend.UpdateRoute(apiID, routeID, input)
		},
		ErrAPINotFound, ErrRouteNotFound)
}

// --- Integration handlers ---

func (h *Handler) handleCreateIntegration(c *echo.Context, apiID string) error {
	return handleCreate(c, apiID, "integration", ErrAPINotFound,
		func(input CreateIntegrationInput) (*Integration, error) {
			return h.Backend.CreateIntegration(apiID, input)
		})
}

func (h *Handler) handleGetIntegrations(c *echo.Context, apiID string) error {
	return handleGetList(c, apiID, "integrations", func() ([]Integration, error) {
		return h.Backend.GetIntegrations(apiID)
	}, func(items []Integration) any { return listIntegrationsOutput{Items: items} })
}

func (h *Handler) handleGetIntegration(c *echo.Context, apiID, integrationID string) error {
	log := logger.Load(c.Request().Context())

	integration, err := h.Backend.GetIntegration(apiID, integrationID)
	if err != nil {
		log.Error("apigatewayv2: get integration failed", "apiId", apiID, "integrationId", integrationID, "error", err)

		if errors.Is(err, ErrAPINotFound) || errors.Is(err, ErrIntegrationNotFound) {
			return c.JSON(http.StatusNotFound, notFoundResponse{Message: msgNotFound})
		}

		return c.JSON(http.StatusInternalServerError, notFoundResponse{Message: err.Error()})
	}

	return c.JSON(http.StatusOK, integration)
}

func (h *Handler) handleDeleteIntegration(c *echo.Context, apiID, integrationID string) error {
	log := logger.Load(c.Request().Context())

	if err := h.Backend.DeleteIntegration(apiID, integrationID); err != nil {
		log.Error("apigatewayv2: delete integration failed",
			"apiId", apiID, "integrationId", integrationID, "error", err)

		if errors.Is(err, ErrAPINotFound) || errors.Is(err, ErrIntegrationNotFound) {
			return c.JSON(http.StatusNotFound, notFoundResponse{Message: msgNotFound})
		}

		return c.JSON(http.StatusInternalServerError, notFoundResponse{Message: err.Error()})
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleUpdateIntegration(c *echo.Context, apiID, integrationID string) error {
	return handleUpdate(c, apiID, integrationID, "integration",
		func(input UpdateIntegrationInput) (*Integration, error) {
			return h.Backend.UpdateIntegration(apiID, integrationID, input)
		},
		ErrAPINotFound, ErrIntegrationNotFound)
}

// --- Deployment handlers ---

func (h *Handler) handleCreateDeployment(c *echo.Context, apiID string) error {
	return handleCreate(c, apiID, "deployment", ErrAPINotFound, func(input CreateDeploymentInput) (*Deployment, error) {
		return h.Backend.CreateDeployment(apiID, input)
	})
}

func (h *Handler) handleGetDeployments(c *echo.Context, apiID string) error {
	return handleGetList(c, apiID, "deployments", func() ([]Deployment, error) {
		return h.Backend.GetDeployments(apiID)
	}, func(items []Deployment) any { return listDeploymentsOutput{Items: items} })
}

func (h *Handler) handleGetDeployment(c *echo.Context, apiID, deploymentID string) error {
	log := logger.Load(c.Request().Context())

	deployment, err := h.Backend.GetDeployment(apiID, deploymentID)
	if err != nil {
		log.Error("apigatewayv2: get deployment failed", "apiId", apiID, "deploymentId", deploymentID, "error", err)

		if errors.Is(err, ErrAPINotFound) || errors.Is(err, ErrDeploymentNotFound) {
			return c.JSON(http.StatusNotFound, notFoundResponse{Message: msgNotFound})
		}

		return c.JSON(http.StatusInternalServerError, notFoundResponse{Message: err.Error()})
	}

	return c.JSON(http.StatusOK, deployment)
}

func (h *Handler) handleDeleteDeployment(c *echo.Context, apiID, deploymentID string) error {
	log := logger.Load(c.Request().Context())

	if err := h.Backend.DeleteDeployment(apiID, deploymentID); err != nil {
		log.Error("apigatewayv2: delete deployment failed", "apiId", apiID, "deploymentId", deploymentID, "error", err)

		if errors.Is(err, ErrAPINotFound) || errors.Is(err, ErrDeploymentNotFound) {
			return c.JSON(http.StatusNotFound, notFoundResponse{Message: msgNotFound})
		}

		return c.JSON(http.StatusInternalServerError, notFoundResponse{Message: err.Error()})
	}

	return c.NoContent(http.StatusNoContent)
}

// --- Authorizer handlers ---

func (h *Handler) handleCreateAuthorizer(c *echo.Context, apiID string) error {
	return handleCreate(c, apiID, "authorizer", ErrAPINotFound, func(input CreateAuthorizerInput) (*Authorizer, error) {
		return h.Backend.CreateAuthorizer(apiID, input)
	})
}

func (h *Handler) handleGetAuthorizers(c *echo.Context, apiID string) error {
	return handleGetList(c, apiID, "authorizers", func() ([]Authorizer, error) {
		return h.Backend.GetAuthorizers(apiID)
	}, func(items []Authorizer) any { return listAuthorizersOutput{Items: items} })
}

func (h *Handler) handleGetAuthorizer(c *echo.Context, apiID, authorizerID string) error {
	log := logger.Load(c.Request().Context())

	authorizer, err := h.Backend.GetAuthorizer(apiID, authorizerID)
	if err != nil {
		log.Error("apigatewayv2: get authorizer failed", "apiId", apiID, "authorizerId", authorizerID, "error", err)

		if errors.Is(err, ErrAPINotFound) || errors.Is(err, ErrAuthorizerNotFound) {
			return c.JSON(http.StatusNotFound, notFoundResponse{Message: msgNotFound})
		}

		return c.JSON(http.StatusInternalServerError, notFoundResponse{Message: err.Error()})
	}

	return c.JSON(http.StatusOK, authorizer)
}

func (h *Handler) handleDeleteAuthorizer(c *echo.Context, apiID, authorizerID string) error {
	log := logger.Load(c.Request().Context())

	if err := h.Backend.DeleteAuthorizer(apiID, authorizerID); err != nil {
		log.Error("apigatewayv2: delete authorizer failed", "apiId", apiID, "authorizerId", authorizerID, "error", err)

		if errors.Is(err, ErrAPINotFound) || errors.Is(err, ErrAuthorizerNotFound) {
			return c.JSON(http.StatusNotFound, notFoundResponse{Message: msgNotFound})
		}

		return c.JSON(http.StatusInternalServerError, notFoundResponse{Message: err.Error()})
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleUpdateAuthorizer(c *echo.Context, apiID, authorizerID string) error {
	return handleUpdate(c, apiID, authorizerID, "authorizer",
		func(input UpdateAuthorizerInput) (*Authorizer, error) {
			return h.Backend.UpdateAuthorizer(apiID, authorizerID, input)
		},
		ErrAPINotFound, ErrAuthorizerNotFound)
}

// --- Model handlers ---

func (h *Handler) handleCreateModel(c *echo.Context, apiID string) error {
	return handleCreate(c, apiID, "model", ErrAPINotFound, func(input CreateModelInput) (*Model, error) {
		return h.Backend.CreateModel(apiID, input)
	})
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
		return c.JSON(http.StatusBadRequest, notFoundResponse{Message: msgInvalidBody})
	}

	result, err := backendFn(input)
	if err != nil {
		log.Error("apigatewayv2: create "+resourceName+" failed", "apiId", apiID, "error", err)

		if errors.Is(err, awserr.ErrAlreadyExists) {
			return c.JSON(http.StatusConflict, notFoundResponse{Message: err.Error()})
		}

		if errors.Is(err, ErrBadRequest) {
			return c.JSON(http.StatusBadRequest, notFoundResponse{Message: err.Error()})
		}

		for _, nfe := range notFoundErrs {
			if errors.Is(err, nfe) {
				return c.JSON(http.StatusNotFound, notFoundResponse{Message: msgNotFound})
			}
		}

		return c.JSON(http.StatusInternalServerError, notFoundResponse{Message: err.Error()})
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
		return c.JSON(http.StatusBadRequest, notFoundResponse{Message: msgInvalidBody})
	}

	result, err := backendFn(input)
	if err != nil {
		log.Error("apigatewayv2: update "+resourceName+" failed",
			"apiId", apiID, "resourceId", resourceID, "error", err)

		for _, nfe := range notFoundErrs {
			if errors.Is(err, nfe) {
				return c.JSON(http.StatusNotFound, notFoundResponse{Message: msgNotFound})
			}
		}

		return c.JSON(http.StatusInternalServerError, notFoundResponse{Message: err.Error()})
	}

	return c.JSON(http.StatusOK, result)
}

// handleGetList is a generic helper for list (GET collection) handlers.
func handleGetList[T any](
	c *echo.Context,
	apiID, resourceName string,
	backendFn func() ([]T, error),
	wrapFn func([]T) any,
) error {
	log := logger.Load(c.Request().Context())

	items, err := backendFn()
	if err != nil {
		log.Error("apigatewayv2: get "+resourceName+" failed", "apiId", apiID, "error", err)

		if errors.Is(err, ErrAPINotFound) {
			return c.JSON(http.StatusNotFound, notFoundResponse{Message: msgNotFound})
		}

		return c.JSON(http.StatusInternalServerError, notFoundResponse{Message: err.Error()})
	}

	return c.JSON(http.StatusOK, wrapFn(items))
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
		return c.JSON(http.StatusBadRequest, notFoundResponse{Message: msgInvalidBody})
	}

	result, err := backendFn(input)
	if err != nil {
		log.Error("apigatewayv2: create "+resourceName+" failed", "error", err)

		if errors.Is(err, awserr.ErrAlreadyExists) {
			return c.JSON(http.StatusConflict, notFoundResponse{Message: err.Error()})
		}

		if errors.Is(err, ErrBadRequest) {
			return c.JSON(http.StatusBadRequest, notFoundResponse{Message: err.Error()})
		}

		return c.JSON(http.StatusInternalServerError, notFoundResponse{Message: err.Error()})
	}

	return c.JSON(http.StatusCreated, result)
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

func (h *Handler) handleGetModels(c *echo.Context, apiID string) error {
	return handleGetList(c, apiID, "models", func() ([]Model, error) {
		return h.Backend.GetModels(apiID)
	}, func(items []Model) any { return listModelsOutput{Items: items} })
}

func (h *Handler) handleGetModel(c *echo.Context, apiID, modelID string) error {
	log := logger.Load(c.Request().Context())

	model, err := h.Backend.GetModel(apiID, modelID)
	if err != nil {
		log.Error("apigatewayv2: get model failed", "apiId", apiID, "modelId", modelID, "error", err)

		if errors.Is(err, ErrAPINotFound) || errors.Is(err, ErrModelNotFound) {
			return c.JSON(http.StatusNotFound, notFoundResponse{Message: msgNotFound})
		}

		return c.JSON(http.StatusInternalServerError, notFoundResponse{Message: err.Error()})
	}

	return c.JSON(http.StatusOK, model)
}

func (h *Handler) handleDeleteModel(c *echo.Context, apiID, modelID string) error {
	log := logger.Load(c.Request().Context())

	if err := h.Backend.DeleteModel(apiID, modelID); err != nil {
		log.Error("apigatewayv2: delete model failed", "apiId", apiID, "modelId", modelID, "error", err)

		if errors.Is(err, ErrAPINotFound) || errors.Is(err, ErrModelNotFound) {
			return c.JSON(http.StatusNotFound, notFoundResponse{Message: msgNotFound})
		}

		return c.JSON(http.StatusInternalServerError, notFoundResponse{Message: err.Error()})
	}

	return c.NoContent(http.StatusNoContent)
}

// --- Integration Response handlers ---

func (h *Handler) handleGetIntegrationResponses(c *echo.Context, apiID, integrationID string) error {
	log := logger.Load(c.Request().Context())

	items, err := h.Backend.GetIntegrationResponses(apiID, integrationID)
	if err != nil {
		log.Error("apigatewayv2: get integration responses failed",
			"apiId", apiID, "integrationId", integrationID, "error", err)

		if errors.Is(err, ErrAPINotFound) || errors.Is(err, ErrIntegrationNotFound) {
			return c.JSON(http.StatusNotFound, notFoundResponse{Message: msgNotFound})
		}

		return c.JSON(http.StatusInternalServerError, notFoundResponse{Message: err.Error()})
	}

	return c.JSON(http.StatusOK, listIntegrationResponsesOutput{Items: items})
}

func (h *Handler) handleGetIntegrationResponse(c *echo.Context, apiID, integrationID, responseID string) error {
	log := logger.Load(c.Request().Context())

	ir, err := h.Backend.GetIntegrationResponse(apiID, integrationID, responseID)
	if err != nil {
		log.Error("apigatewayv2: get integration response failed",
			"apiId", apiID, "integrationId", integrationID, "responseId", responseID, "error", err)

		if errors.Is(err, ErrAPINotFound) || errors.Is(err, ErrIntegrationNotFound) ||
			errors.Is(err, ErrIntegrationResponseNotFound) {
			return c.JSON(http.StatusNotFound, notFoundResponse{Message: msgNotFound})
		}

		return c.JSON(http.StatusInternalServerError, notFoundResponse{Message: err.Error()})
	}

	return c.JSON(http.StatusOK, ir)
}

func (h *Handler) handleDeleteIntegrationResponse(c *echo.Context, apiID, integrationID, responseID string) error {
	log := logger.Load(c.Request().Context())

	if err := h.Backend.DeleteIntegrationResponse(apiID, integrationID, responseID); err != nil {
		log.Error("apigatewayv2: delete integration response failed",
			"apiId", apiID, "integrationId", integrationID, "responseId", responseID, "error", err)

		if errors.Is(err, ErrAPINotFound) || errors.Is(err, ErrIntegrationNotFound) ||
			errors.Is(err, ErrIntegrationResponseNotFound) {
			return c.JSON(http.StatusNotFound, notFoundResponse{Message: msgNotFound})
		}

		return c.JSON(http.StatusInternalServerError, notFoundResponse{Message: err.Error()})
	}

	return c.NoContent(http.StatusNoContent)
}

// --- Route Response handlers ---

func (h *Handler) handleGetRouteResponses(c *echo.Context, apiID, routeID string) error {
	log := logger.Load(c.Request().Context())

	items, err := h.Backend.GetRouteResponses(apiID, routeID)
	if err != nil {
		log.Error("apigatewayv2: get route responses failed",
			"apiId", apiID, "routeId", routeID, "error", err)

		if errors.Is(err, ErrAPINotFound) || errors.Is(err, ErrRouteNotFound) {
			return c.JSON(http.StatusNotFound, notFoundResponse{Message: msgNotFound})
		}

		return c.JSON(http.StatusInternalServerError, notFoundResponse{Message: err.Error()})
	}

	return c.JSON(http.StatusOK, listRouteResponsesOutput{Items: items})
}

func (h *Handler) handleGetRouteResponse(c *echo.Context, apiID, routeID, responseID string) error {
	log := logger.Load(c.Request().Context())

	rr, err := h.Backend.GetRouteResponse(apiID, routeID, responseID)
	if err != nil {
		log.Error("apigatewayv2: get route response failed",
			"apiId", apiID, "routeId", routeID, "responseId", responseID, "error", err)

		if errors.Is(err, ErrAPINotFound) || errors.Is(err, ErrRouteNotFound) ||
			errors.Is(err, ErrRouteResponseNotFound) {
			return c.JSON(http.StatusNotFound, notFoundResponse{Message: msgNotFound})
		}

		return c.JSON(http.StatusInternalServerError, notFoundResponse{Message: err.Error()})
	}

	return c.JSON(http.StatusOK, rr)
}

func (h *Handler) handleDeleteRouteResponse(c *echo.Context, apiID, routeID, responseID string) error {
	log := logger.Load(c.Request().Context())

	if err := h.Backend.DeleteRouteResponse(apiID, routeID, responseID); err != nil {
		log.Error("apigatewayv2: delete route response failed",
			"apiId", apiID, "routeId", routeID, "responseId", responseID, "error", err)

		if errors.Is(err, ErrAPINotFound) || errors.Is(err, ErrRouteNotFound) ||
			errors.Is(err, ErrRouteResponseNotFound) {
			return c.JSON(http.StatusNotFound, notFoundResponse{Message: msgNotFound})
		}

		return c.JSON(http.StatusInternalServerError, notFoundResponse{Message: err.Error()})
	}

	return c.NoContent(http.StatusNoContent)
}

// handleDeepResource handles GET/DELETE on /v2/apis/{apiId}/{coll}/{resourceId}/{subColl}/{subResourceId}.
func (h *Handler) handleDeepResource(
	c *echo.Context,
	method, apiID, _ /* collection */, resourceID, subCollection, subResourceID string,
) error {
	type threeArgHandler func(*echo.Context, string, string, string) error

	dispatch := map[subDispatchKey]threeArgHandler{
		{http.MethodGet, collIntegrationResponses}:    h.handleGetIntegrationResponse,
		{http.MethodDelete, collIntegrationResponses}: h.handleDeleteIntegrationResponse,
		{http.MethodGet, collRouteResponses}:          h.handleGetRouteResponse,
		{http.MethodDelete, collRouteResponses}:       h.handleDeleteRouteResponse,
	}

	if fn, ok := dispatch[subDispatchKey{method, subCollection}]; ok {
		return fn(c, apiID, resourceID, subResourceID)
	}

	return c.JSON(http.StatusNotFound, notFoundResponse{Message: msgNotFound})
}

// --- Domain Name handlers ---

func (h *Handler) handleGetDomainNames(c *echo.Context) error {
	log := logger.Load(c.Request().Context())

	items, err := h.Backend.GetDomainNames()
	if err != nil {
		log.Error("apigatewayv2: get domain names failed", "error", err)

		return c.JSON(http.StatusInternalServerError, notFoundResponse{Message: err.Error()})
	}

	return c.JSON(http.StatusOK, listDomainNamesOutput{Items: items})
}

func (h *Handler) handleGetDomainName(c *echo.Context, domainName string) error {
	log := logger.Load(c.Request().Context())

	dn, err := h.Backend.GetDomainName(domainName)
	if err != nil {
		log.Error("apigatewayv2: get domain name failed", "domainName", domainName, "error", err)

		if errors.Is(err, ErrDomainNameNotFound) {
			return c.JSON(http.StatusNotFound, notFoundResponse{Message: msgNotFound})
		}

		return c.JSON(http.StatusInternalServerError, notFoundResponse{Message: err.Error()})
	}

	return c.JSON(http.StatusOK, dn)
}

func (h *Handler) handleDeleteDomainName(c *echo.Context, domainName string) error {
	log := logger.Load(c.Request().Context())

	if err := h.Backend.DeleteDomainName(domainName); err != nil {
		log.Error("apigatewayv2: delete domain name failed", "domainName", domainName, "error", err)

		if errors.Is(err, ErrDomainNameNotFound) {
			return c.JSON(http.StatusNotFound, notFoundResponse{Message: msgNotFound})
		}

		return c.JSON(http.StatusInternalServerError, notFoundResponse{Message: err.Error()})
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleGetAPIMappings(c *echo.Context, domainName string) error {
	log := logger.Load(c.Request().Context())

	items, err := h.Backend.GetAPIMappings(domainName)
	if err != nil {
		log.Error("apigatewayv2: get api mappings failed", "domainName", domainName, "error", err)

		if errors.Is(err, ErrDomainNameNotFound) {
			return c.JSON(http.StatusNotFound, notFoundResponse{Message: msgNotFound})
		}

		return c.JSON(http.StatusInternalServerError, notFoundResponse{Message: err.Error()})
	}

	return c.JSON(http.StatusOK, listAPIMappingsOutput{Items: items})
}

func (h *Handler) handleGetAPIMapping(c *echo.Context, domainName, mappingID string) error {
	log := logger.Load(c.Request().Context())

	m, err := h.Backend.GetAPIMapping(domainName, mappingID)
	if err != nil {
		log.Error("apigatewayv2: get api mapping failed",
			"domainName", domainName, "mappingId", mappingID, "error", err)

		if errors.Is(err, ErrDomainNameNotFound) || errors.Is(err, ErrAPIMappingNotFound) {
			return c.JSON(http.StatusNotFound, notFoundResponse{Message: msgNotFound})
		}

		return c.JSON(http.StatusInternalServerError, notFoundResponse{Message: err.Error()})
	}

	return c.JSON(http.StatusOK, m)
}

func (h *Handler) handleDeleteAPIMapping(c *echo.Context, domainName, mappingID string) error {
	log := logger.Load(c.Request().Context())

	if err := h.Backend.DeleteAPIMapping(domainName, mappingID); err != nil {
		log.Error("apigatewayv2: delete api mapping failed",
			"domainName", domainName, "mappingId", mappingID, "error", err)

		if errors.Is(err, ErrDomainNameNotFound) || errors.Is(err, ErrAPIMappingNotFound) {
			return c.JSON(http.StatusNotFound, notFoundResponse{Message: msgNotFound})
		}

		return c.JSON(http.StatusInternalServerError, notFoundResponse{Message: err.Error()})
	}

	return c.NoContent(http.StatusNoContent)
}

// --- Portal handlers ---

func (h *Handler) handleListPortals(c *echo.Context) error {
	log := logger.Load(c.Request().Context())

	items, err := h.Backend.ListPortals()
	if err != nil {
		log.Error("apigatewayv2: list portals failed", "error", err)

		return c.JSON(http.StatusInternalServerError, notFoundResponse{Message: err.Error()})
	}

	return c.JSON(http.StatusOK, listPortalsOutput{Items: items})
}

func (h *Handler) handleGetPortal(c *echo.Context, portalID string) error {
	log := logger.Load(c.Request().Context())

	p, err := h.Backend.GetPortal(portalID)
	if err != nil {
		log.Error("apigatewayv2: get portal failed", "portalId", portalID, "error", err)

		if errors.Is(err, ErrPortalNotFound) {
			return c.JSON(http.StatusNotFound, notFoundResponse{Message: msgNotFound})
		}

		return c.JSON(http.StatusInternalServerError, notFoundResponse{Message: err.Error()})
	}

	return c.JSON(http.StatusOK, p)
}

// --- Portal Product handlers ---

func (h *Handler) handleListPortalProducts(c *echo.Context) error {
	log := logger.Load(c.Request().Context())

	items, err := h.Backend.ListPortalProducts()
	if err != nil {
		log.Error("apigatewayv2: list portal products failed", "error", err)

		return c.JSON(http.StatusInternalServerError, notFoundResponse{Message: err.Error()})
	}

	return c.JSON(http.StatusOK, listPortalProductsOutput{Items: items})
}

func (h *Handler) handleGetPortalProduct(c *echo.Context, portalProductID string) error {
	log := logger.Load(c.Request().Context())

	pp, err := h.Backend.GetPortalProduct(portalProductID)
	if err != nil {
		log.Error("apigatewayv2: get portal product failed", "portalProductId", portalProductID, "error", err)

		if errors.Is(err, ErrPortalProductNotFound) {
			return c.JSON(http.StatusNotFound, notFoundResponse{Message: msgNotFound})
		}

		return c.JSON(http.StatusInternalServerError, notFoundResponse{Message: err.Error()})
	}

	return c.JSON(http.StatusOK, pp)
}

func (h *Handler) handleListProductPages(c *echo.Context, portalProductID string) error {
	log := logger.Load(c.Request().Context())

	items, err := h.Backend.ListProductPages(portalProductID)
	if err != nil {
		log.Error("apigatewayv2: list product pages failed", "portalProductId", portalProductID, "error", err)

		if errors.Is(err, ErrPortalProductNotFound) {
			return c.JSON(http.StatusNotFound, notFoundResponse{Message: msgNotFound})
		}

		return c.JSON(http.StatusInternalServerError, notFoundResponse{Message: err.Error()})
	}

	return c.JSON(http.StatusOK, listProductPagesOutput{Items: items})
}

func (h *Handler) handleListProductRestEndpointPages(c *echo.Context, portalProductID string) error {
	log := logger.Load(c.Request().Context())

	items, err := h.Backend.ListProductRestEndpointPages(portalProductID)
	if err != nil {
		log.Error("apigatewayv2: list product rest endpoint pages failed",
			"portalProductId", portalProductID, "error", err)

		if errors.Is(err, ErrPortalProductNotFound) {
			return c.JSON(http.StatusNotFound, notFoundResponse{Message: msgNotFound})
		}

		return c.JSON(http.StatusInternalServerError, notFoundResponse{Message: err.Error()})
	}

	return c.JSON(http.StatusOK, listProductREPagesOutput{Items: items})
}

// --- Tags handlers ---

func extractTagsOp(path, method string) string {
	suffix := strings.Trim(strings.TrimPrefix(path, tagsPrefix), "/")
	if suffix == "" {
		return opUnknown
	}

	switch method {
	case http.MethodGet:
		return "GetTags"
	case http.MethodPut:
		return "TagResource"
	case http.MethodDelete:
		return "UntagResource"
	}

	return opUnknown
}

func (h *Handler) handleTagsPath(c *echo.Context, method, path string) error {
	resourceARN := strings.TrimPrefix(path, tagsPrefix+"/")
	if resourceARN == "" {
		return c.JSON(http.StatusNotFound, notFoundResponse{Message: msgNotFound})
	}

	switch method {
	case http.MethodGet:
		return h.handleGetTags(c, resourceARN)
	case http.MethodPut:
		return h.handleTagResource(c, resourceARN)
	case http.MethodDelete:
		return h.handleUntagResource(c, resourceARN)
	default:
		return c.JSON(http.StatusMethodNotAllowed, notFoundResponse{Message: msgMethodNotAllowed})
	}
}

func (h *Handler) handleGetTags(c *echo.Context, resourceARN string) error {
	log := logger.Load(c.Request().Context())

	tags, err := h.Backend.GetTags(resourceARN)
	if err != nil {
		log.Error("apigatewayv2: get tags failed", "resourceArn", resourceARN, "error", err)

		if errors.Is(err, ErrAPINotFound) {
			return c.JSON(http.StatusNotFound, notFoundResponse{Message: msgNotFound})
		}

		return c.JSON(http.StatusInternalServerError, notFoundResponse{Message: err.Error()})
	}

	if tags == nil {
		tags = map[string]string{}
	}

	return c.JSON(http.StatusOK, getTagsOutput{Tags: tags})
}

func (h *Handler) handleTagResource(c *echo.Context, resourceARN string) error {
	log := logger.Load(c.Request().Context())

	var input tagResourceInput
	if err := json.NewDecoder(c.Request().Body).Decode(&input); err != nil {
		return c.JSON(http.StatusBadRequest, notFoundResponse{Message: msgInvalidBody})
	}

	if err := h.Backend.TagResource(resourceARN, input.Tags); err != nil {
		log.Error("apigatewayv2: tag resource failed", "resourceArn", resourceARN, "error", err)

		if errors.Is(err, ErrAPINotFound) {
			return c.JSON(http.StatusNotFound, notFoundResponse{Message: msgNotFound})
		}

		return c.JSON(http.StatusInternalServerError, notFoundResponse{Message: err.Error()})
	}

	return c.NoContent(http.StatusCreated)
}

func (h *Handler) handleUntagResource(c *echo.Context, resourceARN string) error {
	log := logger.Load(c.Request().Context())

	tagKeysRaw := c.Request().URL.Query().Get("tagKeys")
	tagKeys := strings.Split(tagKeysRaw, ",")

	// Filter out empty strings.
	filtered := tagKeys[:0]
	for _, k := range tagKeys {
		if k != "" {
			filtered = append(filtered, k)
		}
	}

	if err := h.Backend.UntagResource(resourceARN, filtered); err != nil {
		log.Error("apigatewayv2: untag resource failed", "resourceArn", resourceARN, "error", err)

		if errors.Is(err, ErrAPINotFound) {
			return c.JSON(http.StatusNotFound, notFoundResponse{Message: msgNotFound})
		}

		return c.JSON(http.StatusInternalServerError, notFoundResponse{Message: err.Error()})
	}

	return c.NoContent(http.StatusNoContent)
}
