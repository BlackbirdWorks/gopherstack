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
	appsyncPathPrefix = "/v1/apis"

	// Path segment counts for AppSync routes.
	pathSegsAPIs           = 2
	pathSegsAPIID          = 3
	pathSegsAPISubresource = 4
	pathSegsNamedResource  = 5
	pathSegsTypeResolvers  = 6
	pathSegsResolver       = 7

	// Resource segment names.
	pathSegDatasources = "datasources"
	pathSegTypes       = "types"
	pathSegResolvers   = "resolvers"

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
	}
}

// RouteMatcher returns a function that matches AppSync management API and GraphQL requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().URL.Path, appsyncPathPrefix)
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
	// Path: /v1/apis/{apiId}/...
	const apiIDIndex = 2
	if len(segs) > apiIDIndex {
		return segs[apiIDIndex]
	}

	return ""
}

// parseOperation derives an operation name from the HTTP method and path.
func parseOperation(method, path string) string {
	segs := splitPath(path)

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
	if method == http.MethodPost {
		return "CreateGraphqlApi"
	}

	return "ListGraphqlApis"
}

func parseOperationAPIID(method string) string {
	if method == http.MethodDelete {
		return "DeleteGraphqlApi"
	}

	return "GetGraphqlApi"
}

func parseOperationSub(method, seg string) string {
	switch seg {
	case "schemacreations":
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
	case "graphql":
		return "ExecuteGraphQL"
	}

	return opUnknown
}

func parseOperationNamed(method, seg3 string) string {
	switch seg3 {
	case pathSegDatasources:
		if method == http.MethodDelete {
			return "DeleteDataSource"
		}

		return "GetDataSource"
	case pathSegTypes:
		return "GetType"
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

		switch {
		case len(segs) == pathSegsAPIs && segs[1] == "apis":
			return h.handleAPIs(ctx, c)

		case len(segs) >= pathSegsAPIID && segs[1] == "apis":
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
	case "schemacreations":
		return h.handleSchemaCreations(ctx, c, apiID)
	case "schema":
		return h.getIntrospectionSchema(ctx, c, apiID)
	case pathSegDatasources:
		return h.handleDataSources(ctx, c, apiID, segs)
	case pathSegTypes:
		return h.handleTypes(ctx, c, apiID, segs)
	case "graphql":
		return h.handleGraphQL(ctx, c, apiID)
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

// handleSchemaCreations handles /v1/apis/{apiId}/schemacreations.
func (h *Handler) handleSchemaCreations(ctx context.Context, c *echo.Context, apiID string) error {
	switch c.Request().Method {
	case http.MethodPost:
		return h.startSchemaCreation(ctx, c, apiID)
	case http.MethodGet:
		return h.getSchemaCreationStatus(ctx, c, apiID)
	default:
		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}
}

// startSchemaCreation handles POST /v1/apis/{apiId}/schemacreations.
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

// getSchemaCreationStatus handles GET /v1/apis/{apiId}/schemacreations.
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

// handleTypes handles /v1/apis/{apiId}/types/{typeName}/resolvers[/{fieldName}].
func (h *Handler) handleTypes(ctx context.Context, c *echo.Context, apiID string, segs []string) error {
	if len(segs) < pathSegsNamedResource {
		return c.JSON(http.StatusNotFound, errorResponse("NotFoundException", "Not found"))
	}

	typeName := segs[4]
	method := c.Request().Method

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
