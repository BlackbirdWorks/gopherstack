package apigatewayv2

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

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

		return writeErr(c, http.StatusNotFound, msgNotFound)
	}
}

// handleAPIs handles POST /v2/apis and GET /v2/apis.
func (h *Handler) handleAPIs(c *echo.Context, method string) error {
	switch method {
	case http.MethodPost:
		return h.handleCreateAPI(c)
	case http.MethodGet:
		return h.handleGetAPIs(c)
	case http.MethodPut:
		return h.handleImportAPI(c)
	default:
		return writeErr(c, http.StatusMethodNotAllowed, msgMethodNotAllowed)
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
	case http.MethodPut:
		return h.handleReimportAPI(c, apiID)
	default:
		return writeErr(c, http.StatusMethodNotAllowed, msgMethodNotAllowed)
	}
}

func (h *Handler) handleCreateAPI(c *echo.Context) error {
	log := logger.Load(c.Request().Context())

	var input CreateAPIInput
	if err := json.NewDecoder(c.Request().Body).Decode(&input); err != nil {
		return writeErr(c, http.StatusBadRequest, msgInvalidBody)
	}

	api, err := h.Backend.CreateAPI(c.Request().Context(), input)
	if err != nil {
		log.Error("apigatewayv2: create api failed", "error", err)

		if errors.Is(err, ErrBadRequest) {
			return writeErr(c, http.StatusBadRequest, err.Error())
		}

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusCreated, api)
}

func (h *Handler) handleGetAPIs(c *echo.Context) error {
	log := logger.Load(c.Request().Context())

	apis, err := h.Backend.GetAPIs()
	if err != nil {
		log.Error("apigatewayv2: get apis failed", "error", err)

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	maxResults, nextToken := apigwPaginationParams(c)
	p := page.New(apis, nextToken, maxResults, apigwDefaultPageSize)

	return c.JSON(http.StatusOK, listApisOutput{Items: p.Data, NextToken: p.Next})
}

func (h *Handler) handleGetAPI(c *echo.Context, apiID string) error {
	log := logger.Load(c.Request().Context())

	api, err := h.Backend.GetAPI(apiID)
	if err != nil {
		log.Error("apigatewayv2: get api failed", logKeyAPIID, apiID, "error", err)

		if errors.Is(err, ErrAPINotFound) {
			return writeErr(c, http.StatusNotFound, msgNotFound)
		}

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, api)
}

func (h *Handler) handleDeleteAPI(c *echo.Context, apiID string) error {
	log := logger.Load(c.Request().Context())

	// Snapshot the API's authorizer IDs before the cascade delete removes
	// them, so any cached decisions for those authorizers can be purged
	// afterward (bd: gopherstack-wmh). A lookup failure here just means
	// nothing to purge -- DeleteAPI below still enforces ErrAPINotFound.
	authorizers, _ := h.Backend.GetAuthorizers(apiID)

	if err := h.Backend.DeleteAPI(apiID); err != nil {
		log.Error("apigatewayv2: delete api failed", logKeyAPIID, apiID, "error", err)

		if errors.Is(err, ErrAPINotFound) {
			return writeErr(c, http.StatusNotFound, msgNotFound)
		}

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	if h.authCache != nil {
		for _, a := range authorizers {
			h.authCache.purge(a.AuthorizerID)
		}
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleUpdateAPI(c *echo.Context, apiID string) error {
	log := logger.Load(c.Request().Context())

	var input UpdateAPIInput
	if err := json.NewDecoder(c.Request().Body).Decode(&input); err != nil {
		return writeErr(c, http.StatusBadRequest, msgInvalidBody)
	}

	api, err := h.Backend.UpdateAPI(apiID, input)
	if err != nil {
		log.Error("apigatewayv2: update api failed", logKeyAPIID, apiID, "error", err)

		if errors.Is(err, ErrAPINotFound) {
			return writeErr(c, http.StatusNotFound, msgNotFound)
		}

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, api)
}

// openAPISpec is a minimal representation of an OpenAPI 3 / Swagger 2 document.
// Only the fields needed to derive an API name, routes, and integrations are
// modeled; unknown fields are ignored so that minimal specs are tolerated.
type openAPISpec struct {
	Paths map[string]map[string]openAPIOperation `json:"paths"`
	Info  openAPIInfo                            `json:"info"`
}

type openAPIInfo struct {
	Title string `json:"title"`
}

type openAPIOperation struct {
	Integration *openAPIIntegration `json:"x-amazon-apigateway-integration"`
}

// openAPIIntegration models the x-amazon-apigateway-integration extension.
type openAPIIntegration struct {
	Type                 string `json:"type"`
	HTTPMethod           string `json:"httpMethod"`
	URI                  string `json:"uri"`
	PayloadFormatVersion string `json:"payloadFormatVersion"`
	ConnectionType       string `json:"connectionType"`
	TimeoutInMillis      int32  `json:"timeoutInMillis"`
}

// parseOpenAPISpec decodes an OpenAPI body into an openAPISpec, tolerating
// minimal/partial documents.
func parseOpenAPISpec(body string) (*openAPISpec, error) {
	var spec openAPISpec
	if strings.TrimSpace(body) == "" {
		return &spec, nil
	}

	if err := json.Unmarshal([]byte(body), &spec); err != nil {
		return nil, err
	}

	return &spec, nil
}

// applyOpenAPIToAPI creates a route (and integration, when defined) for each
// path+method in the spec. Entries that are not valid HTTP route keys are
// skipped gracefully.
func (h *Handler) applyOpenAPIToAPI(apiID string, spec *openAPISpec) {
	for path, methods := range spec.Paths {
		if !strings.HasPrefix(path, "/") {
			path = "/" + path
		}

		for method, op := range methods {
			h.applyOpenAPIOperation(apiID, path, method, op)
		}
	}
}

// applyOpenAPIOperation creates the route (and integration, when defined) for
// a single path+method entry from an imported OpenAPI spec. Route keys the
// backend would reject (e.g. spec-level fields like "parameters" that share
// the path map) are skipped gracefully.
func (h *Handler) applyOpenAPIOperation(apiID, path, method string, op openAPIOperation) {
	routeKey := strings.ToUpper(method) + " " + path
	if err := validateHTTPRouteKey(routeKey); err != nil {
		return
	}

	var target string

	if op.Integration != nil {
		integ, err := h.Backend.CreateIntegration(apiID, CreateIntegrationInput{
			IntegrationType:      openAPIIntegrationType(op.Integration.Type),
			IntegrationMethod:    op.Integration.HTTPMethod,
			IntegrationURI:       op.Integration.URI,
			PayloadFormatVersion: op.Integration.PayloadFormatVersion,
			ConnectionType:       op.Integration.ConnectionType,
			TimeoutInMillis:      op.Integration.TimeoutInMillis,
		})
		if err == nil && integ != nil {
			target = "integrations/" + integ.IntegrationID
		}
	}

	if _, err := h.Backend.CreateRoute(apiID, CreateRouteInput{
		RouteKey: routeKey,
		Target:   target,
	}); err != nil {
		return
	}
}

// openAPIIntegrationType maps an OpenAPI x-amazon-apigateway-integration
// "type" extension value to the API Gateway v2 IntegrationType wire value.
func openAPIIntegrationType(t string) string {
	switch t {
	case "http", "http_proxy":
		return integrationTypeHTTPProxy
	case "aws", "aws_proxy":
		return IntegrationTypeAWSProxy
	case "mock":
		return "MOCK"
	case "":
		return IntegrationTypeAWSProxy
	default:
		return strings.ToUpper(t)
	}
}

func (h *Handler) handleImportAPI(c *echo.Context) error {
	var input struct {
		Body string `json:"body"`
	}
	if err := json.NewDecoder(c.Request().Body).Decode(&input); err != nil {
		return writeErr(c, http.StatusBadRequest, msgInvalidBody)
	}

	spec, err := parseOpenAPISpec(input.Body)
	if err != nil {
		return writeErr(c, http.StatusBadRequest, msgInvalidBody)
	}

	name := spec.Info.Title
	if name == "" {
		name = "imported-api"
	}

	api, err := h.Backend.CreateAPI(c.Request().Context(), CreateAPIInput{
		Name:         name,
		ProtocolType: protocolTypeHTTP,
	})
	if err != nil {
		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	h.applyOpenAPIToAPI(api.APIID, spec)

	return c.JSON(http.StatusCreated, api)
}

func (h *Handler) handleReimportAPI(c *echo.Context, apiID string) error {
	var input struct {
		Body string `json:"body"`
	}
	if err := json.NewDecoder(c.Request().Body).Decode(&input); err != nil {
		return writeErr(c, http.StatusBadRequest, msgInvalidBody)
	}

	spec, err := parseOpenAPISpec(input.Body)
	if err != nil {
		return writeErr(c, http.StatusBadRequest, msgInvalidBody)
	}

	// Replace existing routes and integrations from the new spec.
	if routes, rErr := h.Backend.GetRoutes(apiID); rErr == nil {
		for _, r := range routes {
			_ = h.Backend.DeleteRoute(apiID, r.RouteID)
		}
	} else if errors.Is(rErr, ErrAPINotFound) {
		return writeErr(c, http.StatusNotFound, msgNotFound)
	}

	if integrations, iErr := h.Backend.GetIntegrations(apiID); iErr == nil {
		for _, i := range integrations {
			_ = h.Backend.DeleteIntegration(apiID, i.IntegrationID)
		}
	}

	update := UpdateAPIInput{}
	if spec.Info.Title != "" {
		update.Name = spec.Info.Title
	}

	api, err := h.Backend.UpdateAPI(apiID, update)
	if err != nil {
		if errors.Is(err, ErrAPINotFound) {
			return writeErr(c, http.StatusNotFound, msgNotFound)
		}

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	h.applyOpenAPIToAPI(apiID, spec)

	return c.JSON(http.StatusCreated, api)
}

func (h *Handler) handleDeleteCorsConfiguration(c *echo.Context, apiID string) error {
	log := logger.Load(c.Request().Context())

	if err := h.Backend.DeleteCorsConfiguration(apiID); err != nil {
		log.Error("apigatewayv2: delete cors configuration failed", logKeyAPIID, apiID, "error", err)

		if errors.Is(err, ErrAPINotFound) {
			return writeErr(c, http.StatusNotFound, msgNotFound)
		}

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleExportAPI(c *echo.Context, apiID, specification string) error {
	// API Gateway v2 only supports the OAS30 specification for exports.
	if specification != "" && specification != "OAS30" {
		return writeErr(c, http.StatusBadRequest, "specification must be OAS30")
	}

	spec, err := h.Backend.ExportAPI(apiID)
	if err != nil {
		if errors.Is(err, ErrAPINotFound) {
			return writeErr(c, http.StatusNotFound, msgNotFound)
		}

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	// AWS returns the raw OpenAPI document as the HTTP response body (the SDK's
	// ExportApi `Body` blob), not a wrapper object.
	blob, mErr := json.Marshal(spec)
	if mErr != nil {
		return writeErr(c, http.StatusInternalServerError, mErr.Error())
	}

	return c.JSONBlob(http.StatusOK, blob)
}
