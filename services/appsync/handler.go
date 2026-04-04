package appsync

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	appsyncPathPrefix   = "/v1/apis"
	appsyncV2PathPrefix = "/v2/apis"
	appsyncDomainPrefix = "/v1/domainnames"
	appsyncSourcePrefix = "/v1/sourceApis"
	appsyncMergedPrefix = "/v1/mergedApis"

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

// Name returns the service name.
func (h *Handler) Name() string { return "AppSync" }

// GetSupportedOperations returns the list of supported AppSync operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateGraphqlApi",
		"GetGraphqlApi",
		"ListGraphqlApis",
		"DeleteGraphqlApi",
		"StartSchemaCreation",
		"GetSchemaCreationStatus",
		"GetIntrospectionSchema",
		"CreateDataSource",
		"GetDataSource",
		"ListDataSources",
		"DeleteDataSource",
		"CreateResolver",
		"GetResolver",
		"ListResolvers",
		"DeleteResolver",
		"ExecuteGraphQL",
		"AssociateApi",
		"AssociateMergedGraphqlApi",
		"AssociateSourceGraphqlApi",
		"CreateApi",
		"CreateApiCache",
		"CreateApiKey",
		"CreateChannelNamespace",
		"CreateDomainName",
		"CreateFunction",
		"CreateType",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "appsync" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this AppSync instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.DefaultRegion} }

// RouteMatcher returns a function that matches AppSync management API and GraphQL requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path

		return strings.HasPrefix(path, appsyncPathPrefix) ||
			strings.HasPrefix(path, appsyncV2PathPrefix) ||
			strings.HasPrefix(path, appsyncDomainPrefix) ||
			strings.HasPrefix(path, appsyncSourcePrefix) ||
			strings.HasPrefix(path, appsyncMergedPrefix)
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
		if method == http.MethodPost {
			return "CreateDomainName"
		}

		return opUnknown
	case pathSegsAPISubresource:
		// /v1/domainnames/{domainName}/apiassociation
		if segs[3] == "apiassociation" && method == http.MethodPost {
			return "AssociateApi"
		}

		return opUnknown
	}

	return opUnknown
}

func parseOperationSourceAPIs(method string, segs []string) string {
	// /v1/sourceApis/{sourceApiIdentifier}/mergedApiAssociations
	if len(segs) == pathSegsAPISubresource && segs[3] == "mergedApiAssociations" {
		if method == http.MethodPost {
			return "AssociateMergedGraphqlApi"
		}
	}

	return opUnknown
}

func parseOperationMergedAPIs(method string, segs []string) string {
	// /v1/mergedApis/{mergedApiIdentifier}/sourceApiAssociations
	if len(segs) == pathSegsAPISubresource && segs[3] == "sourceApiAssociations" {
		if method == http.MethodPost {
			return "AssociateSourceGraphqlApi"
		}
	}

	return opUnknown
}

func parseOperationV2APIs(method string, segs []string) string {
	switch len(segs) {
	case pathSegsAPIs:
		// /v2/apis
		if method == http.MethodPost {
			return "CreateApi"
		}

		return "ListAPIs"
	case pathSegsAPISubresource:
		// /v2/apis/{apiId}/{resource}
		if segs[3] == pathSegChannelNamespaces && method == http.MethodPost {
			return "CreateChannelNamespace"
		}

		return opUnknown
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
	if method == http.MethodDelete {
		return "DeleteGraphqlApi"
	}

	return "GetGraphqlApi"
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
		if method == http.MethodPost {
			return "CreateDataSource"
		}

		return "ListDataSources"
	case pathSegAPIKeys:
		if method == http.MethodPost {
			return "CreateApiKey"
		}

		return "ListApiKeys"
	case pathSegAPICaches:
		if method == http.MethodPost {
			return "CreateApiCache"
		}

		return "GetApiCache"
	case pathSegFunctions:
		if method == http.MethodPost {
			return "CreateFunction"
		}

		return "ListFunctions"
	case pathSegTypes:
		if method == http.MethodPost {
			return "CreateType"
		}

		return "ListTypes"
	case "graphql":
		return "ExecuteGraphQL"
	}

	return opUnknown
}

func parseOperationNamed(method, seg3 string) string {
	if seg3 == pathSegDatasources {
		if method == http.MethodDelete {
			return "DeleteDataSource"
		}

		return "GetDataSource"
	}

	return opUnknown
}

func parseOperationTypeResolvers(method, seg3, seg5 string) string {
	if seg3 != pathSegTypes || seg5 != pathSegResolvers {
		return opUnknown
	}

	if method == http.MethodPost {
		return "CreateResolver"
	}

	return "ListResolvers"
}

func parseOperationResolver(method, seg3, seg5 string) string {
	if seg3 != pathSegTypes || seg5 != pathSegResolvers {
		return opUnknown
	}

	if method == http.MethodDelete {
		return "DeleteResolver"
	}

	return "GetResolver"
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
	method := c.Request().Method

	if len(segs) == pathSegsAPIID {
		switch method {
		case http.MethodGet:
			return h.getGraphqlAPI(ctx, c, apiID)
		case http.MethodDelete:
			return h.deleteGraphqlAPI(ctx, c, apiID)
		default:
			return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
		}
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
		return h.handleAPIKeys(ctx, c, apiID)
	case pathSegAPICaches:
		return h.handleAPICaches(ctx, c, apiID)
	case pathSegFunctions:
		return h.handleFunctions(ctx, c, apiID)
	default:
		return c.JSON(http.StatusNotFound, errorResponse("NotFoundException", "Not found"))
	}
}

// createGraphqlAPI handles POST /v1/apis.
func (h *Handler) createGraphqlAPI(ctx context.Context, c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var input struct {
		Tags               map[string]string `json:"tags"`
		Name               string            `json:"name"`
		AuthenticationType string            `json:"authenticationType"`
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

	api, createErr := h.Backend.CreateGraphqlAPI(input.Name, authType, input.Tags)
	if createErr != nil {
		return h.handleError(ctx, c, "CreateGraphqlApi", createErr)
	}

	return c.JSON(http.StatusCreated, map[string]any{"graphqlApi": api})
}

// listGraphqlAPIs handles GET /v1/apis.
func (h *Handler) listGraphqlAPIs(ctx context.Context, c *echo.Context) error {
	apis, err := h.Backend.ListGraphqlAPIs()
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

	return c.JSON(http.StatusOK, map[string]any{"graphqlApi": api})
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
		"status":  schema.Status,
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
		"status":  schema.Status,
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

	return c.JSON(http.StatusCreated, map[string]any{"dataSource": created})
}

// getDataSource handles GET /v1/apis/{apiId}/datasources/{name}.
func (h *Handler) getDataSource(ctx context.Context, c *echo.Context, apiID, name string) error {
	ds, err := h.Backend.GetDataSource(apiID, name)
	if err != nil {
		return h.handleError(ctx, c, "GetDataSource", err)
	}

	return c.JSON(http.StatusOK, map[string]any{"dataSource": ds})
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

// handleTypes handles /v1/apis/{apiId}/types[/{typeName}/resolvers[/{fieldName}]].
func (h *Handler) handleTypes(ctx context.Context, c *echo.Context, apiID string, segs []string) error {
	method := c.Request().Method

	// /v1/apis/{apiId}/types — CreateType or ListTypes
	if len(segs) == pathSegsAPISubresource {
		switch method {
		case http.MethodPost:
			return h.createTypeHandler(ctx, c, apiID)
		default:
			return c.JSON(http.StatusNotFound, errorResponse("NotFoundException", "Not found"))
		}
	}

	if len(segs) < pathSegsNamedResource {
		return c.JSON(http.StatusNotFound, errorResponse("NotFoundException", "Not found"))
	}

	typeName := segs[4]

	if len(segs) < pathSegsTypeResolvers || segs[5] != pathSegResolvers {
		return c.JSON(http.StatusNotFound, errorResponse("NotFoundException", "Not found"))
	}

	if len(segs) == pathSegsTypeResolvers {
		// /v1/apis/{apiId}/types/{typeName}/resolvers
		switch method {
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

	switch method {
	case http.MethodGet:
		return h.getResolver(ctx, c, apiID, typeName, fieldName)
	case http.MethodDelete:
		return h.deleteResolver(ctx, c, apiID, typeName, fieldName)
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

	return c.JSON(http.StatusCreated, map[string]any{"resolver": created})
}

// getResolver handles GET /v1/apis/{apiId}/types/{typeName}/resolvers/{fieldName}.
func (h *Handler) getResolver(ctx context.Context, c *echo.Context, apiID, typeName, fieldName string) error {
	r, err := h.Backend.GetResolver(apiID, typeName, fieldName)
	if err != nil {
		return h.handleError(ctx, c, "GetResolver", err)
	}

	return c.JSON(http.StatusOK, map[string]any{"resolver": r})
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

	if errors.Is(err, ErrInvalidSchema) {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", err.Error()))
	}

	return c.JSON(
		http.StatusInternalServerError,
		errorResponse("InternalFailure", fmt.Sprintf("internal error: %s", err.Error())),
	)
}

// errorResponse builds a standard AppSync error response body.
func errorResponse(code, message string) map[string]any {
	return map[string]any{
		"message": message,
		"code":    code,
	}
}

// handleAPIKeys handles /v1/apis/{apiId}/apikeys.
func (h *Handler) handleAPIKeys(ctx context.Context, c *echo.Context, apiID string) error {
	if c.Request().Method != http.MethodPost {
		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}

	return h.createAPIKey(ctx, c, apiID)
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

// handleAPICaches handles /v1/apis/{apiId}/ApiCaches.
func (h *Handler) handleAPICaches(ctx context.Context, c *echo.Context, apiID string) error {
	if c.Request().Method != http.MethodPost {
		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}

	return h.createAPICache(ctx, c, apiID)
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

	return c.JSON(http.StatusCreated, map[string]any{"apiCache": created})
}

// handleFunctions handles /v1/apis/{apiId}/functions.
func (h *Handler) handleFunctions(ctx context.Context, c *echo.Context, apiID string) error {
	if c.Request().Method != http.MethodPost {
		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}

	return h.createFunction(ctx, c, apiID)
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

	if f.Name == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "name is required"))
	}

	if f.DataSourceName == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "dataSourceName is required"))
	}

	created, createErr := h.Backend.CreateFunction(apiID, &f)
	if createErr != nil {
		return h.handleError(ctx, c, "CreateFunction", createErr)
	}

	return c.JSON(http.StatusCreated, map[string]any{"functionConfiguration": created})
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

	return c.JSON(http.StatusCreated, map[string]any{"type": created})
}

// handleDomainNames handles /v1/domainnames[/{domainName}/apiassociation].
func (h *Handler) handleDomainNames(ctx context.Context, c *echo.Context, segs []string) error {
	if len(segs) == pathSegsAPIs {
		// /v1/domainnames
		if c.Request().Method == http.MethodPost {
			return h.createDomainName(ctx, c)
		}

		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}

	if len(segs) == pathSegsAPISubresource && segs[3] == "apiassociation" {
		// /v1/domainnames/{domainName}/apiassociation
		if c.Request().Method == http.MethodPost {
			return h.associateAPI(ctx, c, segs[2])
		}

		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}

	return c.JSON(http.StatusNotFound, errorResponse("NotFoundException", "Not found"))
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

	return c.JSON(http.StatusCreated, map[string]any{"domainNameConfig": dn})
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

// handleSourceAPIs handles /v1/sourceApis/{sourceApiIdentifier}/mergedApiAssociations.
func (h *Handler) handleSourceAPIs(ctx context.Context, c *echo.Context, segs []string) error {
	if len(segs) == pathSegsAPISubresource && segs[3] == "mergedApiAssociations" {
		if c.Request().Method == http.MethodPost {
			return h.associateMergedGraphqlAPI(ctx, c, segs[2])
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

	return c.JSON(http.StatusCreated, map[string]any{"sourceApiAssociation": assoc})
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

// handleMergedAPIs handles /v1/mergedApis/{mergedApiIdentifier}/sourceApiAssociations.
func (h *Handler) handleMergedAPIs(ctx context.Context, c *echo.Context, segs []string) error {
	if len(segs) == pathSegsAPISubresource && segs[3] == "sourceApiAssociations" {
		if c.Request().Method == http.MethodPost {
			return h.associateSourceGraphqlAPI(ctx, c, segs[2])
		}

		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
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

// handleV2APIs handles /v2/apis[/{apiId}/channelNamespaces].
func (h *Handler) handleV2APIs(ctx context.Context, c *echo.Context, segs []string) error {
	if len(segs) == pathSegsAPIs {
		if c.Request().Method == http.MethodPost {
			return h.createAPI(ctx, c)
		}

		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}

	if len(segs) == pathSegsAPISubresource {
		apiID := segs[2]

		if segs[3] == pathSegChannelNamespaces {
			if c.Request().Method == http.MethodPost {
				return h.createChannelNamespace(ctx, c, apiID)
			}

			return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
		}
	}

	return c.JSON(http.StatusNotFound, errorResponse("NotFoundException", "Not found"))
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

	api, createErr := h.Backend.CreateAPI(input.Name, input.Tags)
	if createErr != nil {
		return h.handleError(ctx, c, "CreateApi", createErr)
	}

	return c.JSON(http.StatusCreated, map[string]any{"api": api})
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

	return c.JSON(http.StatusCreated, map[string]any{"channelNamespace": ns})
}
