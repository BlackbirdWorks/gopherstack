package apigatewayv2

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"
	"gopkg.in/yaml.v3"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
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

	input.PinnedID, input.Tags = tags.PinnedIDFromMap(input.Tags)

	api, err := h.Backend.CreateAPI(c.Request().Context(), input)
	if err != nil {
		log.Error("apigatewayv2: create api failed", "error", err)

		if errors.Is(err, ErrAlreadyExists) {
			return writeErr(c, http.StatusConflict, err.Error())
		}

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

	if pageErr := validateAPIGWPaging(c); pageErr != nil {
		return writeErr(c, http.StatusBadRequest, pageErr.Error())
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

// validateBasepath returns ErrBadRequest unless basepath is empty (defaults
// to ignore) or one of the three values ImportApiInput/ReimportApiInput
// document (api_op_ImportApi.go:37-41, aws-sdk-go-v2/service/apigatewayv2@v1.37.4:
// "Valid values are ignore, prepend, and split. The default value is ignore.").
func validateBasepath(basepath string) error {
	switch basepath {
	case "", basepathIgnore, basepathPrepend, basepathSplit:
		return nil
	default:
		return fmt.Errorf("%w: basepath must be one of ignore, prepend, split", ErrBadRequest)
	}
}

// failOnWarnings reads the failOnWarnings query param (ImportApiInput/ReimportApiInput).
func failOnWarnings(c *echo.Context) (bool, error) {
	raw := c.Request().URL.Query().Get("failOnWarnings")
	if raw == "" {
		return false, nil
	}

	v, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("%w: failOnWarnings must be a boolean", ErrBadRequest)
	}

	return v, nil
}

// checkFailOnWarnings rejects the import with BadRequestException when failOnWarnings is set and spec warns.
func checkFailOnWarnings(c *echo.Context, spec *openAPISpec) error {
	fail, err := failOnWarnings(c)
	if err != nil || !fail {
		return err
	}

	if w := importWarnings(spec); len(w) > 0 {
		return fmt.Errorf("%w: warnings found during import: %s", ErrBadRequest, strings.Join(w, "; "))
	}

	return nil
}

func (h *Handler) handleImportAPI(c *echo.Context) error {
	// basepath and failOnWarnings are HTTP query-string params on the real
	// ImportApiInput, not JSON body fields (api_op_ImportApi.go:37-45,
	// aws-sdk-go-v2/service/apigatewayv2@v1.37.4).
	basepath := c.Request().URL.Query().Get("basepath")
	if err := validateBasepath(basepath); err != nil {
		return writeErr(c, http.StatusBadRequest, err.Error())
	}

	if _, err := failOnWarnings(c); err != nil {
		return writeErr(c, http.StatusBadRequest, err.Error())
	}

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

	if err = checkFailOnWarnings(c, spec); err != nil {
		return writeErr(c, http.StatusBadRequest, err.Error())
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

	if err = applyOpenAPISpec(h.Backend, api.APIID, spec, basepath); err != nil {
		return writeErr(c, http.StatusBadRequest, err.Error())
	}

	return c.JSON(http.StatusCreated, api)
}

func (h *Handler) handleReimportAPI(c *echo.Context, apiID string) error {
	basepath := c.Request().URL.Query().Get("basepath")
	if err := validateBasepath(basepath); err != nil {
		return writeErr(c, http.StatusBadRequest, err.Error())
	}

	if _, err := failOnWarnings(c); err != nil {
		return writeErr(c, http.StatusBadRequest, err.Error())
	}

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

	if err = checkFailOnWarnings(c, spec); err != nil {
		return writeErr(c, http.StatusBadRequest, err.Error())
	}

	// Replace existing routes and integrations from the new spec.
	if routes, rErr := h.Backend.GetRoutes(apiID); rErr == nil {
		for _, r := range routes {
			_ = h.Backend.DeleteRoute(apiID, r.RouteID)
		}
	} else if errors.Is(rErr, ErrAPINotFound) {
		return writeErr(c, http.StatusNotFound, msgNotFound)
	}

	_ = h.Backend.PurgeIntegrations(apiID)

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

	if err = applyOpenAPISpec(h.Backend, apiID, spec, basepath); err != nil {
		return writeErr(c, http.StatusBadRequest, err.Error())
	}

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

// apigwExtensionPrefix is the key prefix for AWS API Gateway extensions in an
// exported OpenAPI document (e.g. x-amazon-apigateway-authtype).
const apigwExtensionPrefix = "x-amazon-apigateway-"

// includeExtensions reads ExportApiInput's includeExtensions query param
// (api_op_ExportApi.go:52, "*bool ... included by default"), defaulting to
// true (AWS's documented default) when absent or unparseable.
func includeExtensions(c *echo.Context) bool {
	raw := c.QueryParam("includeExtensions")
	if raw == "" {
		return true
	}

	v, err := strconv.ParseBool(raw)
	if err != nil {
		return true
	}

	return v
}

// stripAPIGatewayExtensions recursively removes x-amazon-apigateway-* keys
// from an exported OpenAPI document, for includeExtensions=false.
func stripAPIGatewayExtensions(v any) map[string]any {
	m, ok := v.(map[string]any)
	if !ok {
		return nil
	}

	stripMapExtensions(m)

	return m
}

func stripMapExtensions(m map[string]any) {
	for k, v := range m {
		if strings.HasPrefix(k, apigwExtensionPrefix) {
			delete(m, k)

			continue
		}

		stripExtensionsIn(v)
	}
}

func stripExtensionsIn(v any) {
	switch t := v.(type) {
	case map[string]any:
		stripMapExtensions(t)
	case []any:
		for _, item := range t {
			stripExtensionsIn(item)
		}
	}
}

func (h *Handler) handleExportAPI(c *echo.Context, apiID, specification string) error {
	// API Gateway v2 only supports the OAS30 specification for exports.
	if specification != "" && specification != "OAS30" {
		return writeErr(c, http.StatusBadRequest, "specification must be OAS30")
	}

	// outputType is a required query param on the real ExportApiInput (verified
	// against aws-sdk-go-v2/service/apigatewayv2's validateOpExportApiInput);
	// valid values are JSON and YAML.
	outputType := c.QueryParam("outputType")

	switch {
	case outputType == "":
		return writeErr(c, http.StatusBadRequest, "outputType is required")
	case strings.EqualFold(outputType, "JSON"):
	case strings.EqualFold(outputType, "YAML"):
	default:
		return writeErr(c, http.StatusBadRequest, "outputType must be JSON or YAML")
	}

	// exportVersion is a query param (api_op_ExportApi.go doc: "Currently, the
	// only supported version is 1.0"); reject anything else rather than
	// silently ignoring an unsupported version.
	if exportVersion := c.QueryParam("exportVersion"); exportVersion != "" && exportVersion != "1.0" {
		return writeErr(c, http.StatusBadRequest, "exportVersion must be 1.0")
	}

	spec, err := h.Backend.ExportAPI(apiID)
	if err != nil {
		if errors.Is(err, ErrAPINotFound) {
			return writeErr(c, http.StatusNotFound, msgNotFound)
		}

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	if !includeExtensions(c) {
		spec = stripAPIGatewayExtensions(spec)
	}

	// AWS returns the raw OpenAPI document as the HTTP response body (the SDK's
	// ExportApi `Body` blob), not a wrapper object.
	if strings.EqualFold(outputType, "YAML") {
		blob, mErr := yaml.Marshal(spec)
		if mErr != nil {
			return writeErr(c, http.StatusInternalServerError, mErr.Error())
		}

		return c.Blob(http.StatusOK, "application/x-yaml", blob)
	}

	blob, mErr := json.Marshal(spec)
	if mErr != nil {
		return writeErr(c, http.StatusInternalServerError, mErr.Error())
	}

	return c.JSONBlob(http.StatusOK, blob)
}
