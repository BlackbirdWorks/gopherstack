package appsync

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	keySourceAPIAssociations = "sourceApiAssociations"
	keySourceAPIAssociation  = "sourceApiAssociation"
	keyEnvironmentVariables  = "environmentVariables"
	keyGraphqlAPI            = "graphqlApi"
	keyDataSource            = "dataSource"
	keyResolver              = "resolver"
	keyAPICache              = "apiCache"
	keyFunctionConfiguration = "functionConfiguration"
	keyType                  = "type"
	keyDomainNameConfig      = "domainNameConfig"
	keyAPI                   = "api"
	keyChannelNamespace      = "channelNamespace"
	keyStatus                = "status"
	keyCode                  = "code"
)

const (
	appsyncPathPrefix       = "/v1/apis"
	appsyncV2PathPrefix     = "/v2/apis"
	appsyncDomainPrefix     = "/v1/domainnames"
	appsyncSourcePrefix     = "/v1/sourceApis"
	appsyncMergedPrefix     = "/v1/mergedApis"
	appsyncIntrospectPrefix = "/v1/dataSource-introspections"
	appsyncEvalPrefix       = "/v1/dataplane-evaluations"

	// Path segment names.
	pathSegV1          = "v1"
	pathSegV2          = "v2"
	pathSegAPIs        = "apis"
	pathSegDomainNames = "domainnames"

	// Path segment counts for AppSync routes.
	pathSegsAPIs           = 2
	pathSegsAPIID          = 3
	pathSegsAPISubresource = 4
	pathSegsNamedResource  = 5
	pathSegsTypeResolvers  = 6
	pathSegsResolver       = 7

	// Resource segment names.
	pathSegDatasources       = "datasources"
	pathSegTypes             = "types"
	pathSegResolvers         = "resolvers"
	pathSegAPIKeys           = "apikeys"
	pathSegAPICaches         = "ApiCaches"
	pathSegTags              = "tags"
	pathSegFunctions         = "functions"
	pathSegChannelNamespaces = "channelNamespaces"

	// opUnknown is the operation name for unrecognized paths.
	opUnknown = "Unknown"
)

// Handler is the Echo HTTP handler for AppSync operations.
type Handler struct {
	Backend       StorageBackend
	DefaultRegion string
	AccountID     string
}

// NewHandler creates a new AppSync handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{Backend: backend}
}

// StartWorker starts the AppSync background workers.
func (h *Handler) StartWorker(ctx context.Context) error {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		go NewJanitor(b).Run(ctx)
	}

	return nil
}

// Name returns the service name.
func (h *Handler) Name() string { return "AppSync" }

// GetSupportedOperations returns the list of supported AppSync operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateGraphqlApi",
		"GetGraphqlApi",
		"UpdateGraphqlApi",
		"ListGraphqlApis",
		"DeleteGraphqlApi",
		"StartSchemaCreation",
		"GetSchemaCreationStatus",
		"GetIntrospectionSchema",
		"CreateDataSource",
		"GetDataSource",
		"UpdateDataSource",
		"ListDataSources",
		"DeleteDataSource",
		"CreateResolver",
		"GetResolver",
		"UpdateResolver",
		"ListResolvers",
		"DeleteResolver",
		"ListResolversByFunction",
		"ExecuteGraphQL",
		"AssociateApi",
		"DisassociateApi",
		"AssociateMergedGraphqlApi",
		"AssociateSourceGraphqlApi",
		"DisassociateMergedGraphqlApi",
		"DisassociateSourceGraphqlApi",
		"GetSourceApiAssociation",
		"ListSourceApiAssociations",
		"CreateApi",
		"GetApi",
		"ListApis",
		"UpdateApi",
		"DeleteApi",
		"CreateApiCache",
		"DeleteApiCache",
		"FlushApiCache",
		"GetApiCache",
		"UpdateApiCache",
		"CreateApiKey",
		"DeleteApiKey",
		"ListApiKeys",
		"UpdateApiKey",
		"CreateChannelNamespace",
		"DeleteChannelNamespace",
		"GetChannelNamespace",
		"ListChannelNamespaces",
		"UpdateChannelNamespace",
		"CreateDomainName",
		"DeleteDomainName",
		"GetApiAssociation",
		"GetDomainName",
		"ListDomainNames",
		"UpdateDomainName",
		"CreateFunction",
		"DeleteFunction",
		"GetFunction",
		"ListFunctions",
		"UpdateFunction",
		"CreateType",
		"DeleteType",
		"GetType",
		"ListTypes",
		"UpdateType",
		"GetGraphqlApiEnvironmentVariables",
		"PutGraphqlApiEnvironmentVariables",
		"ListTagsForResource",
		"TagResource",
		"UntagResource",
		"EvaluateCode",
		"EvaluateMappingTemplate",
		"GetDataSourceIntrospection",
		"ListTypesByAssociation",
		"StartDataSourceIntrospection",
		"StartSchemaMerge",
		"UpdateSourceApiAssociation",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "appsync" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this AppSync instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.DefaultRegion} }

// RouteMatcher returns a function that matches AppSync management API and GraphQL requests.
//
// The /v2/apis path prefix is shared with API Gateway V2. Both services use the same
// URL path but send distinct User-Agent values: the AppSync SDK includes "api/appsync/"
// while the API Gateway V2 SDK includes "api/apigatewayv2/". When the path matches
// /v2/apis, we only claim the request if the User-Agent indicates AppSync.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path

		if strings.HasPrefix(path, appsyncV2PathPrefix) {
			ua := c.Request().Header.Get("User-Agent")

			return strings.Contains(ua, "api/appsync")
		}

		return strings.HasPrefix(path, appsyncPathPrefix) ||
			strings.HasPrefix(path, appsyncDomainPrefix) ||
			strings.HasPrefix(path, appsyncSourcePrefix) ||
			strings.HasPrefix(path, appsyncMergedPrefix) ||
			strings.HasPrefix(path, appsyncIntrospectPrefix) ||
			strings.HasPrefix(path, appsyncEvalPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityPathVersioned }

// ExtractOperation extracts the AppSync operation from the request path.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	return parseOperation(c.Request().Method, c.Request().URL.Path)
}

// ExtractResource extracts the API ID from the request path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	segs := splitPath(c.Request().URL.Path)
	// Path: /v1/apis/{apiId}/... or /v2/apis/{apiId}/...
	const apiIDIndex = 2
	if len(segs) > apiIDIndex && (segs[0] == pathSegV1 || segs[0] == pathSegV2) && segs[1] == pathSegAPIs {
		return segs[apiIDIndex]
	}

	// Path: /v1/domainnames/{domainName}/...
	if len(segs) > apiIDIndex && segs[0] == pathSegV1 && segs[1] == pathSegDomainNames {
		return segs[apiIDIndex]
	}

	return ""
}

// parseOperation derives an operation name from the HTTP method and path.
func parseOperation(method, path string) string {
	segs := splitPath(path)

	if len(segs) == 0 {
		return opUnknown
	}

	version := segs[0]

	if len(segs) < pathSegsAPIs {
		return opUnknown
	}

	switch segs[1] {
	case pathSegDomainNames:
		return parseOperationDomainNames(method, segs)
	case "sourceApis":
		return parseOperationSourceAPIs(method, segs)
	case "mergedApis":
		return parseOperationMergedAPIs(method, segs)
	case "dataSource-introspections":
		return parseOperationDataSourceIntrospections(method, segs)
	case "dataplane-evaluations":
		return parseOperationDataplaneEvaluations(segs)
	case pathSegAPIs:
		if version == pathSegV2 {
			return parseOperationV2APIs(method, segs)
		}

		return parseOperationV1APIs(method, segs)
	}

	return opUnknown
}

func parseOperationDomainNames(method string, segs []string) string {
	switch len(segs) {
	case pathSegsAPIs:
		// /v1/domainnames
		switch method {
		case http.MethodPost:
			return "CreateDomainName"
		case http.MethodGet:
			return "ListDomainNames"
		}

		return opUnknown
	case pathSegsAPIID:
		// /v1/domainnames/{domainName}
		switch method {
		case http.MethodGet:
			return "GetDomainName"
		case http.MethodDelete:
			return "DeleteDomainName"
		case http.MethodPut, http.MethodPatch:
			return "UpdateDomainName"
		}

		return opUnknown
	case pathSegsAPISubresource:
		// /v1/domainnames/{domainName}/apiassociation
		if segs[3] == "apiassociation" {
			switch method {
			case http.MethodPost:
				return "AssociateApi"
			case http.MethodGet:
				return "GetApiAssociation"
			case http.MethodDelete:
				return "DisassociateApi"
			}
		}

		return opUnknown
	}

	return opUnknown
}

func parseOperationSourceAPIs(method string, segs []string) string {
	// /v1/sourceApis/{sourceApiIdentifier}/mergedApiAssociations
	if segs[3] != "mergedApiAssociations" {
		return opUnknown
	}

	switch len(segs) {
	case pathSegsAPISubresource:
		if method == http.MethodPost {
			return "AssociateMergedGraphqlApi"
		}
	case pathSegsNamedResource:
		if method == http.MethodDelete {
			return "DisassociateMergedGraphqlApi"
		}
	}

	return opUnknown
}

func parseOperationMergedAPIs(method string, segs []string) string {
	// /v1/mergedApis/{mergedApiIdentifier}/sourceApiAssociations
	if len(segs) < pathSegsAPISubresource || segs[3] != keySourceAPIAssociations {
		return opUnknown
	}

	switch len(segs) {
	case pathSegsAPISubresource:
		switch method {
		case http.MethodPost:
			return "AssociateSourceGraphqlApi"
		case http.MethodGet:
			return "ListSourceApiAssociations"
		}
	case pathSegsNamedResource:
		switch method {
		case http.MethodGet:
			return "GetSourceApiAssociation"
		case http.MethodDelete:
			return "DisassociateSourceGraphqlApi"
		case http.MethodPut, http.MethodPatch:
			return "UpdateSourceApiAssociation"
		}
	case pathSegsTypeResolvers:
		// /v1/mergedApis/{id}/sourceApiAssociations/{assocId}/types
		if segs[5] == pathSegTypes {
			return "ListTypesByAssociation"
		}
	}

	return opUnknown
}

func parseOperationDataSourceIntrospections(method string, segs []string) string {
	switch len(segs) {
	case pathSegsAPIs:
		// /v1/dataSource-introspections
		if method == http.MethodPost {
			return "StartDataSourceIntrospection"
		}
	case pathSegsAPIID:
		// /v1/dataSource-introspections/{introspectionId}
		if method == http.MethodGet {
			return "GetDataSourceIntrospection"
		}
	}

	return opUnknown
}

func parseOperationDataplaneEvaluations(segs []string) string {
	if len(segs) != pathSegsAPIID {
		return opUnknown
	}

	// /v1/dataplane-evaluations/template or /v1/dataplane-evaluations/code
	switch segs[2] {
	case "template":
		return "EvaluateMappingTemplate"
	case keyCode:
		return "EvaluateCode"
	}

	return opUnknown
}

func parseOperationV2APIs(method string, segs []string) string {
	switch len(segs) {
	case pathSegsAPIs:
		return parseOpV2APIsCollection(method)
	case pathSegsAPIID:
		return parseOpV2APIsItem(method)
	case pathSegsAPISubresource:
		return parseOpV2APIsSubresource(method, segs)
	case pathSegsNamedResource:
		return parseOpV2APIsNamedResource(method, segs)
	}

	return opUnknown
}

func parseOpV2APIsCollection(method string) string {
	switch method {
	case http.MethodPost:
		return "CreateApi"
	case http.MethodGet:
		return "ListApis"
	}

	return opUnknown
}

func parseOpV2APIsItem(method string) string {
	switch method {
	case http.MethodGet:
		return "GetApi"
	case http.MethodDelete:
		return "DeleteApi"
	case http.MethodPut, http.MethodPatch:
		return "UpdateApi"
	}

	return opUnknown
}

func parseOpV2APIsSubresource(method string, segs []string) string {
	// /v2/apis/{apiId}/channelNamespaces
	if segs[3] != pathSegChannelNamespaces {
		return opUnknown
	}

	switch method {
	case http.MethodPost:
		return "CreateChannelNamespace"
	case http.MethodGet:
		return "ListChannelNamespaces"
	}

	return opUnknown
}

func parseOpV2APIsNamedResource(method string, segs []string) string {
	// /v2/apis/{apiId}/channelNamespaces/{name}
	if segs[3] != pathSegChannelNamespaces {
		return opUnknown
	}

	switch method {
	case http.MethodGet:
		return "GetChannelNamespace"
	case http.MethodDelete:
		return "DeleteChannelNamespace"
	case http.MethodPut, http.MethodPatch:
		return "UpdateChannelNamespace"
	}

	return opUnknown
}

func parseOperationV1APIs(method string, segs []string) string {
	// segs[0] = "v1", segs[1] = "apis", segs[2] = apiId (if present)
	switch len(segs) {
	case pathSegsAPIs:
		return parseOperationAPIs(method)
	case pathSegsAPIID:
		return parseOperationAPIID(method)
	case pathSegsAPISubresource:
		return parseOperationSub(method, segs[3])
	case pathSegsNamedResource:
		return parseOperationNamed(method, segs[3])
	case pathSegsTypeResolvers:
		return parseOperationTypeResolvers(method, segs[3], segs[5])
	case pathSegsResolver:
		return parseOperationResolver(method, segs[3], segs[5])
	}

	return opUnknown
}

func parseOperationAPIs(method string) string {
	switch method {
	case http.MethodPost:
		return "CreateGraphqlApi"
	case http.MethodGet:
		return "ListGraphqlApis"
	default:
		return opUnknown
	}
}

func parseOperationAPIID(method string) string {
	switch method {
	case http.MethodDelete:
		return "DeleteGraphqlApi"
	case http.MethodPatch, http.MethodPut:
		return "UpdateGraphqlApi"
	default:
		return "GetGraphqlApi"
	}
}

func parseOperationSub(method, seg string) string {
	switch seg {
	case "schemacreation":
		if method == http.MethodPost {
			return "StartSchemaCreation"
		}

		return "GetSchemaCreationStatus"
	case "schema":
		return "GetIntrospectionSchema"
	case pathSegDatasources:
		return parseOperationSubDatasources(method)
	case pathSegAPIKeys:
		return parseOperationSubAPIKeys(method)
	case pathSegAPICaches:
		return parseOperationSubAPICaches(method)
	case pathSegFunctions:
		return parseOperationSubFunctions(method)
	case pathSegTypes:
		return parseOperationSubTypes(method)
	case keyEnvironmentVariables:
		if method == http.MethodPut {
			return "PutGraphqlApiEnvironmentVariables"
		}

		return "GetGraphqlApiEnvironmentVariables"
	case "graphql":
		return "ExecuteGraphQL"
	case "schemaMerge":
		return "StartSchemaMerge"
	case pathSegTags:
		return parseOperationSubTags(method)
	}

	return opUnknown
}

func parseOperationSubDatasources(method string) string {
	if method == http.MethodPost {
		return "CreateDataSource"
	}

	return "ListDataSources"
}

func parseOperationSubAPIKeys(method string) string {
	if method == http.MethodPost {
		return "CreateApiKey"
	}

	return "ListApiKeys"
}

func parseOperationSubAPICaches(method string) string {
	switch method {
	case http.MethodPost:
		return "CreateApiCache"
	case http.MethodPut:
		return "UpdateApiCache"
	case http.MethodDelete:
		return "DeleteApiCache"
	}

	return "GetApiCache"
}

func parseOperationSubFunctions(method string) string {
	if method == http.MethodPost {
		return "CreateFunction"
	}

	return "ListFunctions"
}

func parseOperationSubTypes(method string) string {
	if method == http.MethodPost {
		return "CreateType"
	}

	return "ListTypes"
}

func parseOperationSubTags(method string) string {
	switch method {
	case http.MethodPost:
		return "TagResource"
	case http.MethodGet:
		return "ListTagsForResource"
	case http.MethodDelete:
		return "UntagResource"
	}

	return opUnknown
}

func parseOperationNamed(method, seg3 string) string {
	switch seg3 {
	case pathSegDatasources:
		switch method {
		case http.MethodDelete:
			return "DeleteDataSource"
		case http.MethodPut:
			return "UpdateDataSource"
		}

		return "GetDataSource"
	case pathSegAPIKeys:
		switch method {
		case http.MethodDelete:
			return "DeleteApiKey"
		case http.MethodPut, http.MethodPatch:
			return "UpdateApiKey"
		}

		return opUnknown
	case pathSegFunctions:
		switch method {
		case http.MethodDelete:
			return "DeleteFunction"
		case http.MethodPut:
			return "UpdateFunction"
		}

		return "GetFunction"
	case pathSegAPICaches:
		// /v1/apis/{id}/ApiCaches/entries → FlushApiCache
		return "FlushApiCache"
	case pathSegTypes:
		switch method {
		case http.MethodDelete:
			return "DeleteType"
		case http.MethodPut:
			return "UpdateType"
		}

		return "GetType"
	}

	return opUnknown
}

func parseOperationResolver(method, seg3, seg5 string) string {
	if seg3 != pathSegTypes || seg5 != pathSegResolvers {
		return opUnknown
	}

	switch method {
	case http.MethodDelete:
		return "DeleteResolver"
	case http.MethodPut, http.MethodPatch:
		return "UpdateResolver"
	}

	return "GetResolver"
}

func parseOperationTypeResolvers(method, seg3, seg5 string) string {
	if seg5 != pathSegResolvers {
		return opUnknown
	}

	if seg3 == pathSegTypes {
		if method == http.MethodPost {
			return "CreateResolver"
		}

		return "ListResolvers"
	}

	if seg3 == pathSegFunctions {
		return "ListResolversByFunction"
	}

	return opUnknown
}

// splitPath splits a URL path into non-empty segments.
func splitPath(path string) []string {
	var segs []string

	for s := range strings.SplitSeq(path, "/") {
		if s != "" {
			segs = append(segs, s)
		}
	}

	return segs
}

// Handler returns the Echo handler function for AppSync requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		method := c.Request().Method
		path := c.Request().URL.Path
		segs := splitPath(path)
		log := logger.Load(ctx)

		log.DebugContext(ctx, "AppSync request", "method", method, "path", path)

		if len(segs) < pathSegsAPIs {
			return c.JSON(http.StatusNotFound, errorResponse("NotFoundException", "Not found"))
		}

		switch segs[1] {
		case pathSegDomainNames:
			return h.handleDomainNames(ctx, c, segs)
		case "sourceApis":
			return h.handleSourceAPIs(ctx, c, segs)
		case "mergedApis":
			return h.handleMergedAPIs(ctx, c, segs)
		case "dataSource-introspections":
			return h.handleDataSourceIntrospections(ctx, c, segs)
		case "dataplane-evaluations":
			return h.handleDataplaneEvaluations(ctx, c, segs)
		}

		switch {
		case segs[0] == pathSegV2 && segs[1] == pathSegAPIs:
			return h.handleV2APIs(ctx, c, segs)
		case len(segs) == pathSegsAPIs && segs[1] == pathSegAPIs:
			return h.handleAPIs(ctx, c)
		case len(segs) >= pathSegsAPIID && segs[1] == pathSegAPIs:
			return h.handleAPIResource(ctx, c, segs)
		default:
			return c.JSON(http.StatusNotFound, errorResponse("NotFoundException", "Not found"))
		}
	}
}

// handleAPIs handles /v1/apis.
func (h *Handler) handleAPIs(ctx context.Context, c *echo.Context) error {
	switch c.Request().Method {
	case http.MethodPost:
		return h.createGraphqlAPI(ctx, c)
	case http.MethodGet:
		return h.listGraphqlAPIs(ctx, c)
	default:
		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}
}

// handleAPIResource handles /v1/apis/{apiId}/...
func (h *Handler) handleAPIResource(ctx context.Context, c *echo.Context, segs []string) error {
	apiID := segs[2]

	if len(segs) == pathSegsAPIID {
		return h.handleAPIByID(ctx, c, apiID)
	}

	if len(segs) < pathSegsAPISubresource {
		return c.JSON(http.StatusNotFound, errorResponse("NotFoundException", "Not found"))
	}

	switch segs[3] {
	case "schemacreation":
		return h.handleSchemaCreation(ctx, c, apiID)
	case "schema":
		return h.getIntrospectionSchema(ctx, c, apiID)
	case pathSegDatasources:
		return h.handleDataSources(ctx, c, apiID, segs)
	case pathSegTypes:
		return h.handleTypes(ctx, c, apiID, segs)
	case "graphql":
		return h.handleGraphQL(ctx, c, apiID)
	case pathSegAPIKeys:
		return h.handleAPIKeys(ctx, c, apiID, segs)
	case pathSegAPICaches:
		return h.handleAPICaches(ctx, c, apiID, segs)
	case pathSegFunctions:
		return h.handleFunctions(ctx, c, apiID, segs)
	case pathSegTags:
		return h.handleTags(ctx, c, apiID)
	case keyEnvironmentVariables:
		return h.handleEnvironmentVariables(ctx, c, apiID)
	case "schemaMerge":
		return h.handleSchemaMerge(ctx, c, apiID)
	default:
		return c.JSON(http.StatusNotFound, errorResponse("NotFoundException", "Not found"))
	}
}

// handleAPIByID handles GET/DELETE/PATCH on /v1/apis/{apiId}.
func (h *Handler) handleAPIByID(ctx context.Context, c *echo.Context, apiID string) error {
	switch c.Request().Method {
	case http.MethodGet:
		return h.getGraphqlAPI(ctx, c, apiID)
	case http.MethodDelete:
		return h.deleteGraphqlAPI(ctx, c, apiID)
	case http.MethodPatch, http.MethodPut:
		return h.updateGraphqlAPI(ctx, c, apiID)
	default:
		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}
}

// createGraphqlAPI handles POST /v1/apis.
func (h *Handler) createGraphqlAPI(ctx context.Context, c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var input struct {
		Tags                              map[string]string                  `json:"tags"`
		Name                              string                             `json:"name"`
		AuthenticationType                string                             `json:"authenticationType"`
		APIType                           string                             `json:"apiType"`
		Visibility                        string                             `json:"visibility"`
		AdditionalAuthenticationProviders []AdditionalAuthenticationProvider `json:"additionalAuthenticationProviders"`
		XrayEnabled                       bool                               `json:"xrayEnabled"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	if input.Name == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "name is required"))
	}

	authType := AuthenticationType(input.AuthenticationType)
	if authType == "" {
		authType = AuthTypeAPIKey
	}

	api, createErr := h.Backend.CreateGraphqlAPI(
		input.Name,
		authType,
		input.XrayEnabled,
		input.APIType,
		input.Visibility,
		input.AdditionalAuthenticationProviders,
		input.Tags,
	)
	if createErr != nil {
		return h.handleError(ctx, c, "CreateGraphqlApi", createErr)
	}

	return c.JSON(http.StatusCreated, map[string]any{keyGraphqlAPI: api})
}

// listGraphqlAPIs handles GET /v1/apis.
func (h *Handler) listGraphqlAPIs(ctx context.Context, c *echo.Context) error {
	apiType := c.Request().URL.Query().Get("apiType")

	apis, err := h.Backend.ListGraphqlAPIs(apiType)
	if err != nil {
		return h.handleError(ctx, c, "ListGraphqlApis", err)
	}

	return c.JSON(http.StatusOK, map[string]any{"graphqlApis": apis})
}

// getGraphqlAPI handles GET /v1/apis/{apiId}.
func (h *Handler) getGraphqlAPI(ctx context.Context, c *echo.Context, apiID string) error {
	api, err := h.Backend.GetGraphqlAPI(apiID)
	if err != nil {
		return h.handleError(ctx, c, "GetGraphqlApi", err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyGraphqlAPI: api})
}

// deleteGraphqlAPI handles DELETE /v1/apis/{apiId}.
func (h *Handler) deleteGraphqlAPI(ctx context.Context, c *echo.Context, apiID string) error {
	if err := h.Backend.DeleteGraphqlAPI(apiID); err != nil {
		return h.handleError(ctx, c, "DeleteGraphqlApi", err)
	}

	return c.NoContent(http.StatusNoContent)
}

// handleSchemaCreation handles /v1/apis/{apiId}/schemacreation.
func (h *Handler) handleSchemaCreation(ctx context.Context, c *echo.Context, apiID string) error {
	switch c.Request().Method {
	case http.MethodPost:
		return h.startSchemaCreation(ctx, c, apiID)
	case http.MethodGet:
		return h.getSchemaCreationStatus(ctx, c, apiID)
	default:
		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}
}

// startSchemaCreation handles POST /v1/apis/{apiId}/schemacreation.
func (h *Handler) startSchemaCreation(ctx context.Context, c *echo.Context, apiID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var input struct {
		Definition string `json:"definition"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	// AWS SDK sends the definition as base64-encoded bytes.
	sdl := input.Definition
	if decoded, decErr := base64.StdEncoding.DecodeString(sdl); decErr == nil {
		sdl = string(decoded)
	}

	schema, schemaErr := h.Backend.StartSchemaCreation(apiID, sdl)
	if schemaErr != nil {
		return h.handleError(ctx, c, "StartSchemaCreation", schemaErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyStatus: schema.Status,
		"details": schema.Details,
	})
}

// getSchemaCreationStatus handles GET /v1/apis/{apiId}/schemacreation.
func (h *Handler) getSchemaCreationStatus(ctx context.Context, c *echo.Context, apiID string) error {
	schema, err := h.Backend.GetSchemaCreationStatus(apiID)
	if err != nil {
		return h.handleError(ctx, c, "GetSchemaCreationStatus", err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyStatus: schema.Status,
		"details": schema.Details,
	})
}

// getIntrospectionSchema handles GET /v1/apis/{apiId}/schema.
func (h *Handler) getIntrospectionSchema(ctx context.Context, c *echo.Context, apiID string) error {
	format := c.Request().URL.Query().Get("format")
	if format == "" {
		format = "SDL"
	}

	sdl, err := h.Backend.GetIntrospectionSchema(apiID, format)
	if err != nil {
		return h.handleError(ctx, c, "GetIntrospectionSchema", err)
	}

	c.Response().Header().Set("Content-Type", "application/octet-stream")

	return c.Blob(http.StatusOK, "application/octet-stream", sdl)
}

// handleDataSources handles /v1/apis/{apiId}/datasources[/{name}].
func (h *Handler) handleDataSources(ctx context.Context, c *echo.Context, apiID string, segs []string) error {
	method := c.Request().Method

	if len(segs) == pathSegsAPISubresource {
		// /v1/apis/{apiId}/datasources
		switch method {
		case http.MethodPost:
			return h.createDataSource(ctx, c, apiID)
		case http.MethodGet:
			return h.listDataSources(ctx, c, apiID)
		default:
			return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
		}
	}

	// /v1/apis/{apiId}/datasources/{name}
	dsName := segs[4]

	switch method {
	case http.MethodGet:
		return h.getDataSource(ctx, c, apiID, dsName)
	case http.MethodDelete:
		return h.deleteDataSource(ctx, c, apiID, dsName)
	case http.MethodPut:
		return h.updateDataSource(ctx, c, apiID, dsName)
	default:
		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}
}

// createDataSource handles POST /v1/apis/{apiId}/datasources.
func (h *Handler) createDataSource(ctx context.Context, c *echo.Context, apiID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var ds DataSource
	if jsonErr := json.Unmarshal(body, &ds); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	created, createErr := h.Backend.CreateDataSource(apiID, &ds)
	if createErr != nil {
		return h.handleError(ctx, c, "CreateDataSource", createErr)
	}

	return c.JSON(http.StatusCreated, map[string]any{keyDataSource: created})
}

// getDataSource handles GET /v1/apis/{apiId}/datasources/{name}.
func (h *Handler) getDataSource(ctx context.Context, c *echo.Context, apiID, name string) error {
	ds, err := h.Backend.GetDataSource(apiID, name)
	if err != nil {
		return h.handleError(ctx, c, "GetDataSource", err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyDataSource: ds})
}

// listDataSources handles GET /v1/apis/{apiId}/datasources.
func (h *Handler) listDataSources(ctx context.Context, c *echo.Context, apiID string) error {
	dss, err := h.Backend.ListDataSources(apiID)
	if err != nil {
		return h.handleError(ctx, c, "ListDataSources", err)
	}

	return c.JSON(http.StatusOK, map[string]any{"dataSources": dss})
}

// deleteDataSource handles DELETE /v1/apis/{apiId}/datasources/{name}.
func (h *Handler) deleteDataSource(ctx context.Context, c *echo.Context, apiID, name string) error {
	if err := h.Backend.DeleteDataSource(apiID, name); err != nil {
		return h.handleError(ctx, c, "DeleteDataSource", err)
	}

	return c.NoContent(http.StatusNoContent)
}

// handleTypes handles /v1/apis/{apiId}/types[/{typeName}[/resolvers[/{fieldName}]]].
func (h *Handler) handleTypes(ctx context.Context, c *echo.Context, apiID string, segs []string) error {
	if len(segs) == pathSegsAPISubresource {
		return h.handleTypeCollection(ctx, c, apiID)
	}

	if len(segs) < pathSegsNamedResource {
		return c.JSON(http.StatusNotFound, errorResponse("NotFoundException", "Not found"))
	}

	typeName := segs[4]

	if len(segs) == pathSegsNamedResource {
		return h.handleNamedType(ctx, c, apiID, typeName)
	}

	return h.handleTypeResolvers(ctx, c, apiID, typeName, segs)
}

// handleTypeCollection handles GET/POST /v1/apis/{apiId}/types.
func (h *Handler) handleTypeCollection(ctx context.Context, c *echo.Context, apiID string) error {
	switch c.Request().Method {
	case http.MethodPost:
		return h.createTypeHandler(ctx, c, apiID)
	case http.MethodGet:
		return h.listTypes(ctx, c, apiID)
	default:
		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}
}

// handleNamedType handles GET/DELETE/PUT /v1/apis/{apiId}/types/{typeName}.
func (h *Handler) handleNamedType(ctx context.Context, c *echo.Context, apiID, typeName string) error {
	switch c.Request().Method {
	case http.MethodGet:
		return h.getType(ctx, c, apiID, typeName)
	case http.MethodDelete:
		return h.deleteType(ctx, c, apiID, typeName)
	case http.MethodPut:
		return h.updateType(ctx, c, apiID, typeName)
	default:
		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}
}

// handleTypeResolvers handles /v1/apis/{apiId}/types/{typeName}/resolvers[/{fieldName}].
func (h *Handler) handleTypeResolvers(
	ctx context.Context, c *echo.Context, apiID, typeName string, segs []string,
) error {
	if len(segs) < pathSegsTypeResolvers || segs[5] != pathSegResolvers {
		return c.JSON(http.StatusNotFound, errorResponse("NotFoundException", "Not found"))
	}

	if len(segs) == pathSegsTypeResolvers {
		// /v1/apis/{apiId}/types/{typeName}/resolvers
		switch c.Request().Method {
		case http.MethodPost:
			return h.createResolver(ctx, c, apiID, typeName)
		case http.MethodGet:
			return h.listResolvers(ctx, c, apiID, typeName)
		default:
			return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
		}
	}

	// /v1/apis/{apiId}/types/{typeName}/resolvers/{fieldName}
	fieldName := segs[6]

	switch c.Request().Method {
	case http.MethodGet:
		return h.getResolver(ctx, c, apiID, typeName, fieldName)
	case http.MethodDelete:
		return h.deleteResolver(ctx, c, apiID, typeName, fieldName)
	case http.MethodPut, http.MethodPatch:
		return h.updateResolver(ctx, c, apiID, typeName, fieldName)
	default:
		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}
}

// createResolver handles POST /v1/apis/{apiId}/types/{typeName}/resolvers.
func (h *Handler) createResolver(ctx context.Context, c *echo.Context, apiID, typeName string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var r Resolver
	if jsonErr := json.Unmarshal(body, &r); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	created, createErr := h.Backend.CreateResolver(apiID, typeName, &r)
	if createErr != nil {
		return h.handleError(ctx, c, "CreateResolver", createErr)
	}

	return c.JSON(http.StatusCreated, map[string]any{keyResolver: created})
}

// getResolver handles GET /v1/apis/{apiId}/types/{typeName}/resolvers/{fieldName}.
func (h *Handler) getResolver(ctx context.Context, c *echo.Context, apiID, typeName, fieldName string) error {
	r, err := h.Backend.GetResolver(apiID, typeName, fieldName)
	if err != nil {
		return h.handleError(ctx, c, "GetResolver", err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyResolver: r})
}

// listResolvers handles GET /v1/apis/{apiId}/types/{typeName}/resolvers.
func (h *Handler) listResolvers(ctx context.Context, c *echo.Context, apiID, typeName string) error {
	resolvers, err := h.Backend.ListResolvers(apiID, typeName)
	if err != nil {
		return h.handleError(ctx, c, "ListResolvers", err)
	}

	return c.JSON(http.StatusOK, map[string]any{"resolvers": resolvers})
}

// deleteResolver handles DELETE /v1/apis/{apiId}/types/{typeName}/resolvers/{fieldName}.
func (h *Handler) deleteResolver(ctx context.Context, c *echo.Context, apiID, typeName, fieldName string) error {
	if err := h.Backend.DeleteResolver(apiID, typeName, fieldName); err != nil {
		return h.handleError(ctx, c, "DeleteResolver", err)
	}

	return c.NoContent(http.StatusNoContent)
}

// handleGraphQL handles POST /v1/apis/{apiId}/graphql — the GraphQL execution endpoint.
func (h *Handler) handleGraphQL(ctx context.Context, c *echo.Context, apiID string) error {
	if c.Request().Method != http.MethodPost {
		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	req, parseErr := parseGraphQLRequest(body)
	if parseErr != nil {
		return c.JSON(http.StatusBadRequest, graphqlResponse{
			Errors: []graphqlError{{Message: parseErr.Error()}},
		})
	}

	result, execErr := h.Backend.ExecuteGraphQL(ctx, apiID, req.Query, req.OperationName, req.Variables)
	if execErr != nil {
		return c.JSON(http.StatusOK, graphqlResponse{
			Errors: []graphqlError{{Message: execErr.Error()}},
		})
	}

	return c.JSON(http.StatusOK, graphqlResponse{Data: result})
}

// handleError maps backend errors to appropriate HTTP responses.
func (h *Handler) handleError(ctx context.Context, c *echo.Context, op string, err error) error {
	log := logger.Load(ctx)
	log.ErrorContext(ctx, "AppSync operation failed", "operation", op, "error", err)

	if errors.Is(err, awserr.ErrNotFound) {
		return c.JSON(http.StatusNotFound, errorResponse("NotFoundException", err.Error()))
	}

	if errors.Is(err, awserr.ErrAlreadyExists) || errors.Is(err, awserr.ErrConflict) {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", err.Error()))
	}

	if errors.Is(err, awserr.ErrInvalidParameter) {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", err.Error()))
	}

	if errors.Is(err, ErrInvalidSchema) {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", err.Error()))
	}

	if errors.Is(err, ErrValidation) {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", err.Error()))
	}

	return c.JSON(
		http.StatusInternalServerError,
		errorResponse("InternalFailure", "internal error: "+err.Error()),
	)
}

// errorResponse builds a standard AppSync error response body.
func errorResponse(code, message string) map[string]any {
	return map[string]any{
		"message": message,
		keyCode:   code,
	}
}

// handleAPIKeys handles /v1/apis/{apiId}/apikeys[/{keyId}].
func (h *Handler) handleAPIKeys(ctx context.Context, c *echo.Context, apiID string, segs []string) error {
	method := c.Request().Method

	if len(segs) == pathSegsNamedResource {
		// /v1/apis/{apiId}/apikeys/{keyId}
		keyID := segs[4]

		switch method {
		case http.MethodDelete:
			return h.deleteAPIKey(ctx, c, apiID, keyID)
		case http.MethodPut, http.MethodPatch:
			return h.updateAPIKey(ctx, c, apiID, keyID)
		default:
			return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
		}
	}

	// /v1/apis/{apiId}/apikeys
	switch method {
	case http.MethodPost:
		return h.createAPIKey(ctx, c, apiID)
	case http.MethodGet:
		return h.listAPIKeys(ctx, c, apiID)
	default:
		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}
}

// createAPIKey handles POST /v1/apis/{apiId}/apikeys.
func (h *Handler) createAPIKey(ctx context.Context, c *echo.Context, apiID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var input struct {
		Description string `json:"description"`
		Expires     int64  `json:"expires"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	key, createErr := h.Backend.CreateAPIKey(apiID, input.Description, input.Expires)
	if createErr != nil {
		return h.handleError(ctx, c, "CreateApiKey", createErr)
	}

	return c.JSON(http.StatusCreated, map[string]any{"apiKey": key})
}

// handleAPICaches handles /v1/apis/{apiId}/ApiCaches[/entries].
func (h *Handler) handleAPICaches(ctx context.Context, c *echo.Context, apiID string, segs []string) error {
	// /v1/apis/{apiId}/ApiCaches/entries → FlushApiCache
	if len(segs) == pathSegsNamedResource && segs[4] == "entries" {
		if c.Request().Method == http.MethodDelete {
			return h.flushAPICache(ctx, c, apiID)
		}

		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}

	switch c.Request().Method {
	case http.MethodPost:
		return h.createAPICache(ctx, c, apiID)
	case http.MethodGet:
		return h.getAPICache(ctx, c, apiID)
	case http.MethodPut:
		return h.updateAPICache(ctx, c, apiID)
	case http.MethodDelete:
		return h.deleteAPICache(ctx, c, apiID)
	default:
		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}
}

// createAPICache handles POST /v1/apis/{apiId}/ApiCaches.
func (h *Handler) createAPICache(ctx context.Context, c *echo.Context, apiID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var cache APICache
	if jsonErr := json.Unmarshal(body, &cache); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	created, createErr := h.Backend.CreateAPICache(apiID, &cache)
	if createErr != nil {
		return h.handleError(ctx, c, "CreateApiCache", createErr)
	}

	return c.JSON(http.StatusCreated, map[string]any{keyAPICache: created})
}

// handleFunctions handles /v1/apis/{apiId}/functions[/{functionId}[/resolvers]].
func (h *Handler) handleFunctions(ctx context.Context, c *echo.Context, apiID string, segs []string) error {
	method := c.Request().Method

	// /v1/apis/{apiId}/functions/{functionId}/resolvers (6 segs)
	if len(segs) == pathSegsTypeResolvers && segs[5] == pathSegResolvers {
		funcID := segs[4]

		if method == http.MethodGet {
			return h.listResolversByFunction(ctx, c, apiID, funcID)
		}

		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}

	if len(segs) == pathSegsNamedResource {
		// /v1/apis/{apiId}/functions/{functionId}
		funcID := segs[4]

		switch method {
		case http.MethodGet:
			return h.getFunction(ctx, c, apiID, funcID)
		case http.MethodDelete:
			return h.deleteFunction(ctx, c, apiID, funcID)
		case http.MethodPut:
			return h.updateFunction(ctx, c, apiID, funcID)
		default:
			return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
		}
	}

	// /v1/apis/{apiId}/functions
	switch method {
	case http.MethodPost:
		return h.createFunction(ctx, c, apiID)
	case http.MethodGet:
		return h.listFunctions(ctx, c, apiID)
	default:
		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}
}

// createFunction handles POST /v1/apis/{apiId}/functions.
func (h *Handler) createFunction(ctx context.Context, c *echo.Context, apiID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var f Function
	if jsonErr := json.Unmarshal(body, &f); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	if f.DataSourceName == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "dataSourceName is required"))
	}

	created, createErr := h.Backend.CreateFunction(apiID, &f)
	if createErr != nil {
		return h.handleError(ctx, c, "CreateFunction", createErr)
	}

	return c.JSON(http.StatusCreated, map[string]any{keyFunctionConfiguration: created})
}

// createTypeHandler handles POST /v1/apis/{apiId}/types within handleTypes.
func (h *Handler) createTypeHandler(ctx context.Context, c *echo.Context, apiID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var input struct {
		Definition string `json:"definition"`
		Format     string `json:"format"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	if input.Definition == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "definition is required"))
	}

	format := TypeDefinitionFormat(input.Format)
	if format == "" {
		format = TypeFormatSDL
	}

	created, createErr := h.Backend.CreateType(apiID, input.Definition, format)
	if createErr != nil {
		return h.handleError(ctx, c, "CreateType", createErr)
	}

	return c.JSON(http.StatusCreated, map[string]any{keyType: created})
}

// handleDomainNames handles /v1/domainnames[/{domainName}[/apiassociation]].
func (h *Handler) handleDomainNames(ctx context.Context, c *echo.Context, segs []string) error {
	switch len(segs) {
	case pathSegsAPIs:
		return h.handleDomainNamesCollection(ctx, c)
	case pathSegsAPIID:
		return h.handleDomainName(ctx, c, segs[2])
	case pathSegsAPISubresource:
		if segs[3] == "apiassociation" {
			return h.handleAPIAssociation(ctx, c, segs[2])
		}
	}

	return c.JSON(http.StatusNotFound, errorResponse("NotFoundException", "Not found"))
}

// handleDomainNamesCollection handles /v1/domainnames.
func (h *Handler) handleDomainNamesCollection(ctx context.Context, c *echo.Context) error {
	switch c.Request().Method {
	case http.MethodPost:
		return h.createDomainName(ctx, c)
	case http.MethodGet:
		return h.listDomainNames(ctx, c)
	default:
		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}
}

// handleDomainName handles /v1/domainnames/{domainName}.
func (h *Handler) handleDomainName(ctx context.Context, c *echo.Context, domainName string) error {
	switch c.Request().Method {
	case http.MethodGet:
		return h.getDomainName(ctx, c, domainName)
	case http.MethodDelete:
		return h.deleteDomainName(ctx, c, domainName)
	case http.MethodPut, http.MethodPatch:
		return h.updateDomainName(ctx, c, domainName)
	default:
		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}
}

// handleAPIAssociation handles /v1/domainnames/{domainName}/apiassociation.
func (h *Handler) handleAPIAssociation(ctx context.Context, c *echo.Context, domainName string) error {
	switch c.Request().Method {
	case http.MethodPost:
		return h.associateAPI(ctx, c, domainName)
	case http.MethodGet:
		return h.getAPIAssociation(ctx, c, domainName)
	case http.MethodDelete:
		return h.disassociateAPI(ctx, c, domainName)
	default:
		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}
}

// createDomainName handles POST /v1/domainnames.
func (h *Handler) createDomainName(ctx context.Context, c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var input struct {
		Tags           map[string]string `json:"tags"`
		DomainName     string            `json:"domainName"`
		CertificateArn string            `json:"certificateArn"`
		Description    string            `json:"description"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	if input.DomainName == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "domainName is required"))
	}

	if input.CertificateArn == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "certificateArn is required"))
	}

	dn, createErr := h.Backend.CreateDomainName(input.DomainName, input.CertificateArn, input.Description, input.Tags)
	if createErr != nil {
		return h.handleError(ctx, c, "CreateDomainName", createErr)
	}

	return c.JSON(http.StatusCreated, map[string]any{keyDomainNameConfig: dn})
}

// associateAPI handles POST /v1/domainnames/{domainName}/apiassociation.
func (h *Handler) associateAPI(ctx context.Context, c *echo.Context, domainName string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var input struct {
		APIID string `json:"apiId"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	if input.APIID == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "apiId is required"))
	}

	assoc, createErr := h.Backend.AssociateAPI(domainName, input.APIID)
	if createErr != nil {
		return h.handleError(ctx, c, "AssociateApi", createErr)
	}

	return c.JSON(http.StatusCreated, map[string]any{"apiAssociation": assoc})
}

// handleSourceAPIs handles /v1/sourceApis/{sourceApiIdentifier}/mergedApiAssociations[/{assocId}].
func (h *Handler) handleSourceAPIs(ctx context.Context, c *echo.Context, segs []string) error {
	if segs[3] != "mergedApiAssociations" {
		return c.JSON(http.StatusNotFound, errorResponse("NotFoundException", "Not found"))
	}

	sourceAPIID := segs[2]

	if len(segs) == pathSegsAPISubresource {
		switch c.Request().Method {
		case http.MethodPost:
			return h.associateMergedGraphqlAPI(ctx, c, sourceAPIID)
		default:
			return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
		}
	}

	if len(segs) == pathSegsNamedResource {
		assocID := segs[4]

		if c.Request().Method == http.MethodDelete {
			if err := h.Backend.DisassociateMergedGraphqlAPI(sourceAPIID, assocID); err != nil {
				return h.handleError(ctx, c, "DisassociateMergedGraphqlApi", err)
			}

			return c.NoContent(http.StatusNoContent)
		}

		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}

	return c.JSON(http.StatusNotFound, errorResponse("NotFoundException", "Not found"))
}

// associateMergedGraphqlAPI handles POST /v1/sourceApis/{sourceApiIdentifier}/mergedApiAssociations.
// associateAPIInput holds the common JSON fields for API association requests.
type associateAPIInput struct {
	MergedAPIIdentifier string `json:"mergedApiIdentifier"`
	SourceAPIIdentifier string `json:"sourceApiIdentifier"`
	Description         string `json:"description"`
}

// doSourceAPIAssociation is the shared implementation for both Merged/Source GraphQL API association.
func (h *Handler) doSourceAPIAssociation(
	ctx context.Context,
	c *echo.Context,
	primaryAPIID, secondaryAPIID, requiredField, opName string,
	backendFn func(firstID, secondID, description string) (*SourceAPIAssociation, error),
	input associateAPIInput,
) error {
	if secondaryAPIID == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", requiredField+" is required"))
	}

	assoc, createErr := backendFn(primaryAPIID, secondaryAPIID, input.Description)
	if createErr != nil {
		return h.handleError(ctx, c, opName, createErr)
	}

	return c.JSON(http.StatusCreated, map[string]any{keySourceAPIAssociation: assoc})
}

func (h *Handler) associateMergedGraphqlAPI(ctx context.Context, c *echo.Context, sourceAPIID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var input associateAPIInput
	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	return h.doSourceAPIAssociation(ctx, c, sourceAPIID, input.MergedAPIIdentifier,
		"mergedApiIdentifier", "AssociateMergedGraphqlApi",
		h.Backend.AssociateMergedGraphqlAPI, input)
}

// handleMergedAPIs handles /v1/mergedApis/{mergedApiIdentifier}/sourceApiAssociations[/{assocId}].
func (h *Handler) handleMergedAPIs(ctx context.Context, c *echo.Context, segs []string) error {
	if len(segs) < pathSegsAPISubresource || segs[3] != keySourceAPIAssociations {
		return c.JSON(http.StatusNotFound, errorResponse("NotFoundException", "Not found"))
	}

	mergedAPIID := segs[2]

	if len(segs) == pathSegsAPISubresource {
		switch c.Request().Method {
		case http.MethodPost:
			return h.associateSourceGraphqlAPI(ctx, c, mergedAPIID)
		case http.MethodGet:
			return h.listSourceAPIAssociations(ctx, c, mergedAPIID)
		default:
			return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
		}
	}

	if len(segs) == pathSegsNamedResource {
		assocID := segs[4]

		switch c.Request().Method {
		case http.MethodGet:
			return h.getSourceAPIAssociation(ctx, c, mergedAPIID, assocID)
		case http.MethodDelete:
			if err := h.Backend.DisassociateSourceGraphqlAPI(mergedAPIID, assocID); err != nil {
				return h.handleError(ctx, c, "DisassociateSourceGraphqlApi", err)
			}

			return c.NoContent(http.StatusNoContent)
		case http.MethodPut, http.MethodPatch:
			return h.updateSourceAPIAssociation(ctx, c, mergedAPIID, assocID)
		default:
			return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
		}
	}

	// /v1/mergedApis/{mergedApiId}/sourceApiAssociations/{assocId}/types
	if len(segs) == pathSegsTypeResolvers && segs[5] == pathSegTypes {
		assocID := segs[4]

		return h.listTypesByAssociation(ctx, c, mergedAPIID, assocID)
	}

	return c.JSON(http.StatusNotFound, errorResponse("NotFoundException", "Not found"))
}

// associateSourceGraphqlAPI handles POST /v1/mergedApis/{mergedApiIdentifier}/sourceApiAssociations.
func (h *Handler) associateSourceGraphqlAPI(ctx context.Context, c *echo.Context, mergedAPIID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var input associateAPIInput
	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	return h.doSourceAPIAssociation(ctx, c, mergedAPIID, input.SourceAPIIdentifier,
		"sourceApiIdentifier", "AssociateSourceGraphqlApi",
		h.Backend.AssociateSourceGraphqlAPI, input)
}

// handleV2APIs handles /v2/apis[/{apiId}[/channelNamespaces[/{name}]]].
func (h *Handler) handleV2APIs(ctx context.Context, c *echo.Context, segs []string) error {
	switch len(segs) {
	case pathSegsAPIs:
		return h.handleV2APIsCollection(ctx, c)
	case pathSegsAPIID:
		return h.handleV2APIsItem(ctx, c, segs[2])
	case pathSegsAPISubresource:
		if segs[3] == pathSegChannelNamespaces {
			return h.handleChannelNamespacesCollection(ctx, c, segs[2])
		}
	case pathSegsNamedResource:
		if segs[3] == pathSegChannelNamespaces {
			return h.handleChannelNamespaceItem(ctx, c, segs[2], segs[4])
		}
	}

	return c.JSON(http.StatusNotFound, errorResponse("NotFoundException", "Not found"))
}

func (h *Handler) handleV2APIsCollection(ctx context.Context, c *echo.Context) error {
	switch c.Request().Method {
	case http.MethodPost:
		return h.createAPI(ctx, c)
	case http.MethodGet:
		return h.listAPIs(ctx, c)
	default:
		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}
}

func (h *Handler) handleV2APIsItem(ctx context.Context, c *echo.Context, apiID string) error {
	switch c.Request().Method {
	case http.MethodGet:
		return h.getAPI(ctx, c, apiID)
	case http.MethodDelete:
		return h.deleteAPI(ctx, c, apiID)
	case http.MethodPut, http.MethodPatch:
		return h.updateAPI(ctx, c, apiID)
	default:
		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}
}

func (h *Handler) handleChannelNamespacesCollection(ctx context.Context, c *echo.Context, apiID string) error {
	switch c.Request().Method {
	case http.MethodPost:
		return h.createChannelNamespace(ctx, c, apiID)
	case http.MethodGet:
		return h.listChannelNamespaces(ctx, c, apiID)
	default:
		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}
}

func (h *Handler) handleChannelNamespaceItem(ctx context.Context, c *echo.Context, apiID, nsName string) error {
	switch c.Request().Method {
	case http.MethodGet:
		return h.getChannelNamespace(ctx, c, apiID, nsName)
	case http.MethodDelete:
		return h.deleteChannelNamespace(ctx, c, apiID, nsName)
	case http.MethodPut, http.MethodPatch:
		return h.updateChannelNamespace(ctx, c, apiID, nsName)
	default:
		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}
}

// createAPI handles POST /v2/apis.
func (h *Handler) createAPI(ctx context.Context, c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var input struct {
		Tags         map[string]string `json:"tags"`
		Name         string            `json:"name"`
		OwnerContact string            `json:"ownerContact"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	if input.Name == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "name is required"))
	}

	api, createErr := h.Backend.CreateAPI(input.Name, input.OwnerContact, input.Tags)
	if createErr != nil {
		return h.handleError(ctx, c, "CreateApi", createErr)
	}

	return c.JSON(http.StatusCreated, map[string]any{keyAPI: api})
}

// createChannelNamespace handles POST /v2/apis/{apiId}/channelNamespaces.
func (h *Handler) createChannelNamespace(ctx context.Context, c *echo.Context, apiID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var input struct {
		Tags map[string]string `json:"tags"`
		Name string            `json:"name"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	if input.Name == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "name is required"))
	}

	ns, createErr := h.Backend.CreateChannelNamespace(apiID, input.Name, input.Tags)
	if createErr != nil {
		return h.handleError(ctx, c, "CreateChannelNamespace", createErr)
	}

	return c.JSON(http.StatusCreated, map[string]any{keyChannelNamespace: ns})
}

// updateGraphqlAPI handles PATCH /v1/apis/{apiId}.
func (h *Handler) updateGraphqlAPI(ctx context.Context, c *echo.Context, apiID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var input struct {
		XrayEnabled                       *bool                              `json:"xrayEnabled"`
		Name                              string                             `json:"name"`
		AuthenticationType                string                             `json:"authenticationType"`
		Visibility                        string                             `json:"visibility"`
		AdditionalAuthenticationProviders []AdditionalAuthenticationProvider `json:"additionalAuthenticationProviders"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	api, updateErr := h.Backend.UpdateGraphqlAPI(
		apiID,
		input.Name,
		AuthenticationType(input.AuthenticationType),
		input.XrayEnabled,
		input.Visibility,
		input.AdditionalAuthenticationProviders,
	)
	if updateErr != nil {
		return h.handleError(ctx, c, "UpdateGraphqlApi", updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyGraphqlAPI: api})
}

// listAPIKeys handles GET /v1/apis/{apiId}/apikeys.
func (h *Handler) listAPIKeys(ctx context.Context, c *echo.Context, apiID string) error {
	keys, err := h.Backend.ListAPIKeys(apiID)
	if err != nil {
		return h.handleError(ctx, c, "ListApiKeys", err)
	}

	return c.JSON(http.StatusOK, map[string]any{"apiKeys": keys})
}

// deleteAPIKey handles DELETE /v1/apis/{apiId}/apikeys/{keyId}.
func (h *Handler) deleteAPIKey(ctx context.Context, c *echo.Context, apiID, keyID string) error {
	if err := h.Backend.DeleteAPIKey(apiID, keyID); err != nil {
		return h.handleError(ctx, c, "DeleteApiKey", err)
	}

	return c.NoContent(http.StatusNoContent)
}

// getAPICache handles GET /v1/apis/{apiId}/ApiCaches.
func (h *Handler) getAPICache(ctx context.Context, c *echo.Context, apiID string) error {
	cache, err := h.Backend.GetAPICache(apiID)
	if err != nil {
		return h.handleError(ctx, c, "GetApiCache", err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyAPICache: cache})
}

// deleteAPICache handles DELETE /v1/apis/{apiId}/ApiCaches.
func (h *Handler) deleteAPICache(ctx context.Context, c *echo.Context, apiID string) error {
	if err := h.Backend.DeleteAPICache(apiID); err != nil {
		return h.handleError(ctx, c, "DeleteApiCache", err)
	}

	return c.NoContent(http.StatusNoContent)
}

// getFunction handles GET /v1/apis/{apiId}/functions/{functionId}.
func (h *Handler) getFunction(ctx context.Context, c *echo.Context, apiID, functionID string) error {
	fn, err := h.Backend.GetFunction(apiID, functionID)
	if err != nil {
		return h.handleError(ctx, c, "GetFunction", err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyFunctionConfiguration: fn})
}

// listFunctions handles GET /v1/apis/{apiId}/functions.
func (h *Handler) listFunctions(ctx context.Context, c *echo.Context, apiID string) error {
	fns, err := h.Backend.ListFunctions(apiID)
	if err != nil {
		return h.handleError(ctx, c, "ListFunctions", err)
	}

	return c.JSON(http.StatusOK, map[string]any{"functions": fns})
}

// deleteFunction handles DELETE /v1/apis/{apiId}/functions/{functionId}.
func (h *Handler) deleteFunction(ctx context.Context, c *echo.Context, apiID, functionID string) error {
	if err := h.Backend.DeleteFunction(apiID, functionID); err != nil {
		return h.handleError(ctx, c, "DeleteFunction", err)
	}

	return c.NoContent(http.StatusNoContent)
}

// getType handles GET /v1/apis/{apiId}/types/{typeName}.
func (h *Handler) getType(ctx context.Context, c *echo.Context, apiID, typeName string) error {
	// The format query parameter (SDL or JSON) is accepted for AWS SDK compatibility.
	// The definition is returned in the format it was stored in.
	t, err := h.Backend.GetType(apiID, typeName)
	if err != nil {
		return h.handleError(ctx, c, "GetType", err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyType: t})
}

// listTypes handles GET /v1/apis/{apiId}/types.
func (h *Handler) listTypes(ctx context.Context, c *echo.Context, apiID string) error {
	// The format query parameter (SDL or JSON) is accepted for AWS SDK compatibility.
	// Each type is returned in the format it was stored in.
	types, err := h.Backend.ListTypes(apiID)
	if err != nil {
		return h.handleError(ctx, c, "ListTypes", err)
	}

	return c.JSON(http.StatusOK, map[string]any{"types": types})
}

// deleteType handles DELETE /v1/apis/{apiId}/types/{typeName}.
func (h *Handler) deleteType(ctx context.Context, c *echo.Context, apiID, typeName string) error {
	if err := h.Backend.DeleteType(apiID, typeName); err != nil {
		return h.handleError(ctx, c, "DeleteType", err)
	}

	return c.NoContent(http.StatusNoContent)
}

// getDomainName handles GET /v1/domainnames/{domainName}.
func (h *Handler) getDomainName(ctx context.Context, c *echo.Context, domainName string) error {
	dn, err := h.Backend.GetDomainName(domainName)
	if err != nil {
		return h.handleError(ctx, c, "GetDomainName", err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyDomainNameConfig: dn})
}

// listDomainNames handles GET /v1/domainnames.
func (h *Handler) listDomainNames(ctx context.Context, c *echo.Context) error {
	dns, err := h.Backend.ListDomainNames()
	if err != nil {
		return h.handleError(ctx, c, "ListDomainNames", err)
	}

	return c.JSON(http.StatusOK, map[string]any{"domainNameConfigs": dns})
}

// deleteDomainName handles DELETE /v1/domainnames/{domainName}.
func (h *Handler) deleteDomainName(ctx context.Context, c *echo.Context, domainName string) error {
	if err := h.Backend.DeleteDomainName(domainName); err != nil {
		return h.handleError(ctx, c, "DeleteDomainName", err)
	}

	return c.NoContent(http.StatusNoContent)
}

// getAPIAssociation handles GET /v1/domainnames/{domainName}/apiassociation.
func (h *Handler) getAPIAssociation(ctx context.Context, c *echo.Context, domainName string) error {
	assoc, err := h.Backend.GetAPIAssociation(domainName)
	if err != nil {
		return h.handleError(ctx, c, "GetApiAssociation", err)
	}

	return c.JSON(http.StatusOK, map[string]any{"apiAssociation": assoc})
}

// updateAPIKey handles PUT/PATCH /v1/apis/{apiId}/apikeys/{keyId}.
func (h *Handler) updateAPIKey(ctx context.Context, c *echo.Context, apiID, keyID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var input struct {
		Description string `json:"description"`
		Expires     int64  `json:"expires"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	key, updateErr := h.Backend.UpdateAPIKey(apiID, keyID, input.Description, input.Expires)
	if updateErr != nil {
		return h.handleError(ctx, c, "UpdateApiKey", updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{"apiKey": key})
}

// updateAPICache handles PUT /v1/apis/{apiId}/ApiCaches.
func (h *Handler) updateAPICache(ctx context.Context, c *echo.Context, apiID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var cache APICache
	if jsonErr := json.Unmarshal(body, &cache); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	updated, updateErr := h.Backend.UpdateAPICache(apiID, &cache)
	if updateErr != nil {
		return h.handleError(ctx, c, "UpdateApiCache", updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyAPICache: updated})
}

// flushAPICache handles DELETE /v1/apis/{apiId}/ApiCaches/entries.
func (h *Handler) flushAPICache(ctx context.Context, c *echo.Context, apiID string) error {
	if err := h.Backend.FlushAPICache(apiID); err != nil {
		return h.handleError(ctx, c, "FlushApiCache", err)
	}

	return c.NoContent(http.StatusNoContent)
}

// updateDataSource handles PUT /v1/apis/{apiId}/datasources/{name}.
func (h *Handler) updateDataSource(ctx context.Context, c *echo.Context, apiID, name string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var ds DataSource
	if jsonErr := json.Unmarshal(body, &ds); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	updated, updateErr := h.Backend.UpdateDataSource(apiID, name, &ds)
	if updateErr != nil {
		return h.handleError(ctx, c, "UpdateDataSource", updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyDataSource: updated})
}

// updateFunction handles PUT /v1/apis/{apiId}/functions/{functionId}.
func (h *Handler) updateFunction(ctx context.Context, c *echo.Context, apiID, functionID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var f Function
	if jsonErr := json.Unmarshal(body, &f); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	updated, updateErr := h.Backend.UpdateFunction(apiID, functionID, &f)
	if updateErr != nil {
		return h.handleError(ctx, c, "UpdateFunction", updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyFunctionConfiguration: updated})
}

// updateResolver handles PUT/PATCH /v1/apis/{apiId}/types/{typeName}/resolvers/{fieldName}.
func (h *Handler) updateResolver(ctx context.Context, c *echo.Context, apiID, typeName, fieldName string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var r Resolver
	if jsonErr := json.Unmarshal(body, &r); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	r.FieldName = fieldName

	updated, updateErr := h.Backend.UpdateResolver(apiID, typeName, &r)
	if updateErr != nil {
		return h.handleError(ctx, c, "UpdateResolver", updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyResolver: updated})
}

// updateType handles PUT /v1/apis/{apiId}/types/{typeName}.
func (h *Handler) updateType(ctx context.Context, c *echo.Context, apiID, typeName string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var input struct {
		Definition string `json:"definition"`
		Format     string `json:"format"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	updated, updateErr := h.Backend.UpdateType(apiID, typeName, input.Definition, TypeDefinitionFormat(input.Format))
	if updateErr != nil {
		return h.handleError(ctx, c, "UpdateType", updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyType: updated})
}

// handleTags handles /v1/apis/{apiId}/tags.
func (h *Handler) handleTags(ctx context.Context, c *echo.Context, apiID string) error {
	switch c.Request().Method {
	case http.MethodPost:
		return h.tagResource(ctx, c, apiID)
	case http.MethodGet:
		return h.listTagsForResource(ctx, c, apiID)
	case http.MethodDelete:
		return h.untagResource(ctx, c, apiID)
	default:
		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}
}

// tagResource handles POST /v1/apis/{apiId}/tags.
func (h *Handler) tagResource(ctx context.Context, c *echo.Context, apiID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var input struct {
		Tags map[string]string `json:"tags"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	if tagErr := h.Backend.TagResource(apiID, input.Tags); tagErr != nil {
		return h.handleError(ctx, c, "TagResource", tagErr)
	}

	return c.NoContent(http.StatusNoContent)
}

// untagResource handles DELETE /v1/apis/{apiId}/tags.
func (h *Handler) untagResource(ctx context.Context, c *echo.Context, apiID string) error {
	tagKeys := c.Request().URL.Query()["tagKeys"]
	if len(tagKeys) == 0 {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("BadRequestException", "tagKeys query parameter is required"),
		)
	}

	if tagErr := h.Backend.UntagResource(apiID, tagKeys); tagErr != nil {
		return h.handleError(ctx, c, "UntagResource", tagErr)
	}

	return c.NoContent(http.StatusNoContent)
}

// listTagsForResource handles GET /v1/apis/{apiId}/tags.
func (h *Handler) listTagsForResource(ctx context.Context, c *echo.Context, apiID string) error {
	tagMap, err := h.Backend.ListTagsForResource(apiID)
	if err != nil {
		return h.handleError(ctx, c, "ListTagsForResource", err)
	}

	return c.JSON(http.StatusOK, map[string]any{"tags": tagMap})
}

// updateDomainName handles PUT/PATCH /v1/domainnames/{domainName}.
func (h *Handler) updateDomainName(ctx context.Context, c *echo.Context, domainName string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var input struct {
		Description    string `json:"description"`
		CertificateARN string `json:"certificateArn"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	dn, updateErr := h.Backend.UpdateDomainName(domainName, input.Description, input.CertificateARN)
	if updateErr != nil {
		return h.handleError(ctx, c, "UpdateDomainName", updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyDomainNameConfig: dn})
}

// disassociateAPI handles DELETE /v1/domainnames/{domainName}/apiassociation.
func (h *Handler) disassociateAPI(ctx context.Context, c *echo.Context, domainName string) error {
	if err := h.Backend.DisassociateAPI(domainName); err != nil {
		return h.handleError(ctx, c, "DisassociateApi", err)
	}

	return c.NoContent(http.StatusNoContent)
}

// getAPI handles GET /v2/apis/{apiId}.
func (h *Handler) getAPI(ctx context.Context, c *echo.Context, apiID string) error {
	api, err := h.Backend.GetAPI(apiID)
	if err != nil {
		return h.handleError(ctx, c, "GetApi", err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyAPI: api})
}

// listAPIs handles GET /v2/apis.
func (h *Handler) listAPIs(ctx context.Context, c *echo.Context) error {
	apis, err := h.Backend.ListAPIs()
	if err != nil {
		return h.handleError(ctx, c, "ListApis", err)
	}

	return c.JSON(http.StatusOK, map[string]any{"items": apis})
}

// deleteAPI handles DELETE /v2/apis/{apiId}.
func (h *Handler) deleteAPI(ctx context.Context, c *echo.Context, apiID string) error {
	if err := h.Backend.DeleteAPI(apiID); err != nil {
		return h.handleError(ctx, c, "DeleteApi", err)
	}

	return c.NoContent(http.StatusNoContent)
}

// updateAPI handles PUT/PATCH /v2/apis/{apiId}.
func (h *Handler) updateAPI(ctx context.Context, c *echo.Context, apiID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var input struct {
		Name         string `json:"name"`
		OwnerContact string `json:"ownerContact"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	api, updateErr := h.Backend.UpdateAPI(apiID, input.Name, input.OwnerContact)
	if updateErr != nil {
		return h.handleError(ctx, c, "UpdateApi", updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyAPI: api})
}

// getChannelNamespace handles GET /v2/apis/{apiId}/channelNamespaces/{name}.
func (h *Handler) getChannelNamespace(ctx context.Context, c *echo.Context, apiID, name string) error {
	ns, err := h.Backend.GetChannelNamespace(apiID, name)
	if err != nil {
		return h.handleError(ctx, c, "GetChannelNamespace", err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyChannelNamespace: ns})
}

// listChannelNamespaces handles GET /v2/apis/{apiId}/channelNamespaces.
func (h *Handler) listChannelNamespaces(ctx context.Context, c *echo.Context, apiID string) error {
	nss, err := h.Backend.ListChannelNamespaces(apiID)
	if err != nil {
		return h.handleError(ctx, c, "ListChannelNamespaces", err)
	}

	return c.JSON(http.StatusOK, map[string]any{"channelNamespaces": nss})
}

// updateChannelNamespace handles PUT/PATCH /v2/apis/{apiId}/channelNamespaces/{name}.
func (h *Handler) updateChannelNamespace(ctx context.Context, c *echo.Context, apiID, name string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var input struct {
		CodeHandlers string `json:"codeHandlers"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	updated, updateErr := h.Backend.UpdateChannelNamespace(apiID, name, input.CodeHandlers)
	if updateErr != nil {
		return h.handleError(ctx, c, "UpdateChannelNamespace", updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyChannelNamespace: updated})
}

// deleteChannelNamespace handles DELETE /v2/apis/{apiId}/channelNamespaces/{name}.
func (h *Handler) deleteChannelNamespace(ctx context.Context, c *echo.Context, apiID, name string) error {
	if err := h.Backend.DeleteChannelNamespace(apiID, name); err != nil {
		return h.handleError(ctx, c, "DeleteChannelNamespace", err)
	}

	return c.NoContent(http.StatusNoContent)
}

// getSourceAPIAssociation handles GET /v1/mergedApis/{mergedApiId}/sourceApiAssociations/{assocId}.
func (h *Handler) getSourceAPIAssociation(ctx context.Context, c *echo.Context, mergedAPIID, assocID string) error {
	assoc, err := h.Backend.GetSourceAPIAssociation(mergedAPIID, assocID)
	if err != nil {
		return h.handleError(ctx, c, "GetSourceApiAssociation", err)
	}

	return c.JSON(http.StatusOK, map[string]any{keySourceAPIAssociation: assoc})
}

// listSourceAPIAssociations handles GET /v1/mergedApis/{mergedApiId}/sourceApiAssociations.
func (h *Handler) listSourceAPIAssociations(ctx context.Context, c *echo.Context, mergedAPIID string) error {
	assocs, err := h.Backend.ListSourceAPIAssociations(mergedAPIID)
	if err != nil {
		return h.handleError(ctx, c, "ListSourceApiAssociations", err)
	}

	return c.JSON(http.StatusOK, map[string]any{keySourceAPIAssociations: assocs})
}

// listResolversByFunction handles GET /v1/apis/{apiId}/functions/{functionId}/resolvers.
func (h *Handler) listResolversByFunction(ctx context.Context, c *echo.Context, apiID, functionID string) error {
	resolvers, err := h.Backend.ListResolversByFunction(apiID, functionID)
	if err != nil {
		return h.handleError(ctx, c, "ListResolversByFunction", err)
	}

	return c.JSON(http.StatusOK, map[string]any{"resolvers": resolvers})
}

// handleEnvironmentVariables handles GET and PUT /v1/apis/{apiId}/environmentVariables.
func (h *Handler) handleEnvironmentVariables(ctx context.Context, c *echo.Context, apiID string) error {
	switch c.Request().Method {
	case http.MethodGet:
		envVars, err := h.Backend.GetGraphqlAPIEnvironmentVariables(apiID)
		if err != nil {
			return h.handleError(ctx, c, "GetGraphqlApiEnvironmentVariables", err)
		}

		return c.JSON(http.StatusOK, map[string]any{keyEnvironmentVariables: envVars})
	case http.MethodPut:
		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
		}

		var input struct {
			EnvironmentVariables map[string]string `json:"environmentVariables"`
		}

		if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
			return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
		}

		envVars, putErr := h.Backend.PutGraphqlAPIEnvironmentVariables(apiID, input.EnvironmentVariables)
		if putErr != nil {
			return h.handleError(ctx, c, "PutGraphqlApiEnvironmentVariables", putErr)
		}

		return c.JSON(http.StatusOK, map[string]any{keyEnvironmentVariables: envVars, "apiId": apiID})
	default:
		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}
}

// handleDataSourceIntrospections handles /v1/dataSource-introspections[/{introspectionId}].
func (h *Handler) handleDataSourceIntrospections(ctx context.Context, c *echo.Context, segs []string) error {
	switch len(segs) {
	case pathSegsAPIs:
		// POST /v1/dataSource-introspections → StartDataSourceIntrospection
		if c.Request().Method != http.MethodPost {
			return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
		}

		return h.startDataSourceIntrospection(ctx, c)
	case pathSegsAPIID:
		// GET /v1/dataSource-introspections/{introspectionId}
		if c.Request().Method != http.MethodGet {
			return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
		}

		return h.getDataSourceIntrospection(ctx, c, segs[2])
	}

	return c.JSON(http.StatusNotFound, errorResponse("NotFoundException", "Not found"))
}

// handleDataplaneEvaluations handles /v1/dataplane-evaluations/{template|code}.
func (h *Handler) handleDataplaneEvaluations(ctx context.Context, c *echo.Context, segs []string) error {
	if len(segs) != pathSegsAPIID {
		return c.JSON(http.StatusNotFound, errorResponse("NotFoundException", "Not found"))
	}

	if c.Request().Method != http.MethodPost {
		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}

	switch segs[2] {
	case "template":
		return h.evaluateMappingTemplate(ctx, c)
	case keyCode:
		return h.evaluateCode(ctx, c)
	}

	return c.JSON(http.StatusNotFound, errorResponse("NotFoundException", "Not found"))
}

// startDataSourceIntrospection handles POST /v1/dataSource-introspections.
func (h *Handler) startDataSourceIntrospection(ctx context.Context, c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var input struct {
		APIID          string `json:"apiId"`
		DataSourceName string `json:"dataSourceName"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	id, startErr := h.Backend.StartDataSourceIntrospection(input.APIID, input.DataSourceName)
	if startErr != nil {
		return h.handleError(ctx, c, "StartDataSourceIntrospection", startErr)
	}

	return c.JSON(http.StatusCreated, map[string]any{"introspectionId": id})
}

// getDataSourceIntrospection handles GET /v1/dataSource-introspections/{introspectionId}.
func (h *Handler) getDataSourceIntrospection(ctx context.Context, c *echo.Context, introspectionID string) error {
	result, err := h.Backend.GetDataSourceIntrospection(introspectionID)
	if err != nil {
		return h.handleError(ctx, c, "GetDataSourceIntrospection", err)
	}

	return c.JSON(http.StatusOK, map[string]any{"introspectionResult": result})
}

// evaluateMappingTemplate handles POST /v1/dataplane-evaluations/template.
func (h *Handler) evaluateMappingTemplate(ctx context.Context, c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var input struct {
		Template string `json:"template"`
		Context  string `json:"context"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	out, evalErr := h.Backend.EvaluateMappingTemplate(input.Template, input.Context)
	if evalErr != nil {
		return h.handleError(ctx, c, "EvaluateMappingTemplate", evalErr)
	}

	return c.JSON(http.StatusOK, map[string]any{"evaluationResult": out})
}

// evaluateCode handles POST /v1/dataplane-evaluations/code.
func (h *Handler) evaluateCode(ctx context.Context, c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var input struct {
		Code     string `json:"code"`
		Context  string `json:"context"`
		Function string `json:"function"`
		Runtime  struct {
			Name string `json:"name"`
		} `json:"runtime"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	out, evalErr := h.Backend.EvaluateCode(input.Code, input.Context, input.Function, input.Runtime.Name)
	if evalErr != nil {
		return h.handleError(ctx, c, "EvaluateCode", evalErr)
	}

	return c.JSON(http.StatusOK, map[string]any{"evaluationResult": out, "logs": []string{}})
}

// updateSourceAPIAssociation handles PUT /v1/mergedApis/{mergedApiId}/sourceApiAssociations/{assocId}.
func (h *Handler) updateSourceAPIAssociation(
	ctx context.Context,
	c *echo.Context,
	mergedAPIID, assocID string,
) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var input struct {
		Description string `json:"description"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	assoc, updateErr := h.Backend.UpdateSourceAPIAssociation(mergedAPIID, assocID, input.Description)
	if updateErr != nil {
		return h.handleError(ctx, c, "UpdateSourceApiAssociation", updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keySourceAPIAssociation: assoc})
}

// listTypesByAssociation handles GET /v1/mergedApis/{mergedApiId}/sourceApiAssociations/{assocId}/types.
func (h *Handler) listTypesByAssociation(
	ctx context.Context,
	c *echo.Context,
	mergedAPIID, assocID string,
) error {
	format := c.Request().URL.Query().Get("format")
	if format == "" {
		format = string(TypeFormatSDL)
	}

	types, err := h.Backend.ListTypesByAssociation(mergedAPIID, assocID, format)
	if err != nil {
		return h.handleError(ctx, c, "ListTypesByAssociation", err)
	}

	return c.JSON(http.StatusOK, map[string]any{pathSegTypes: types})
}

// handleDataplaneEvalsSchemaMerge handles POST /v1/apis/{apiId}/schemaMerge.
func (h *Handler) handleSchemaMerge(ctx context.Context, c *echo.Context, apiID string) error {
	if c.Request().Method != http.MethodPost {
		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}

	status, err := h.Backend.StartSchemaMerge(apiID)
	if err != nil {
		return h.handleError(ctx, c, "StartSchemaMerge", err)
	}

	return c.JSON(http.StatusOK, map[string]any{"sourceApiSchemaMetadata": []any{}, keyStatus: status})
}
