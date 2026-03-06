package lambda

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/httputil"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// lambdaPathPrefix is the path prefix for Lambda REST API v1 endpoints.
const lambdaPathPrefix = "/2015-03-31/functions"

// lambda2020PathPrefix is the path prefix for Lambda REST API v2 endpoints (e.g. code signing configs).
const lambda2020PathPrefix = "/2020-06-30/functions"

// esmPathPrefix is the path prefix for Lambda event source mapping endpoints.
const esmPathPrefix = "/2015-03-31/event-source-mappings"

// lambdaTagsPathPrefix is the path prefix for Lambda resource tag endpoints.
const lambdaTagsPathPrefix = "/2015-03-31/tags"

// lambdaLayersPathPrefix is the path prefix for Lambda Layers endpoints.
// The Lambda Layers API uses the 2018-10-31 date version.
const lambdaLayersPathPrefix = "/2018-10-31/layers"

type lambdaTagsInput struct {
	Tags *tags.Tags `json:"Tags"`
}

type lambdaEmptyOutput struct{}

type getTagsOutput struct {
	Tags map[string]string `json:"Tags"`
}

type publishVersionInput struct {
	Description string `json:"Description"`
}

// routeSpec binds an HTTP method and path predicate to an operation name or handler.
type routeSpec struct {
	method string
	match  func(rest string) bool
	op     string
}

// lambdaOpRoutes maps HTTP method + path predicates to operation names.
//
//nolint:gochecknoglobals // intentional package-level route table
var lambdaOpRoutes = []routeSpec{
	{http.MethodPost, isEmptyRest, "CreateFunction"},
	{http.MethodGet, isEmptyRest, "ListFunctions"},
	{http.MethodGet, isNameOnly, "GetFunction"},
	{http.MethodDelete, isNameOnly, "DeleteFunction"},
	{http.MethodPut, hasSuffixCode, "UpdateFunctionCode"},
	{http.MethodPut, hasSuffixConfiguration, "UpdateFunctionConfiguration"},
	{http.MethodPost, hasSuffixInvocations, "InvokeFunction"},
	{http.MethodPost, hasSuffixURL, "CreateFunctionURLConfig"},
	{http.MethodGet, hasSuffixURL, "GetFunctionURLConfig"},
	{http.MethodDelete, hasSuffixURL, "DeleteFunctionURLConfig"},
}

func isEmptyRest(rest string) bool            { return rest == "" }
func hasSuffixCode(rest string) bool          { return strings.HasSuffix(rest, "/code") }
func hasSuffixConfiguration(rest string) bool { return strings.HasSuffix(rest, "/configuration") }
func hasSuffixInvocations(rest string) bool   { return strings.HasSuffix(rest, "/invocations") }
func hasSuffixURL(rest string) bool           { return strings.HasSuffix(rest, "/url") }
func hasSuffixCodeSigningConfig(rest string) bool {
	return strings.HasSuffix(rest, "/code-signing-config")
}
func hasSuffixVersions(rest string) bool { return strings.HasSuffix(rest, "/versions") }
func hasSuffixAliasPath(rest string) bool {
	trimmed := strings.TrimPrefix(rest, "/")
	parts := strings.SplitN(trimmed, "/", 3) //nolint:mnd // split into name + "aliases" + optional alias name

	return len(parts) >= 2 && parts[1] == "aliases"
}

// Handler is the Echo HTTP handler for Lambda operations.
type Handler struct {
	Backend       StorageBackend
	tags          map[string]*tags.Tags
	tagsMu        *lockmetrics.RWMutex
	DefaultRegion string
	AccountID     string
}

// NewHandler creates a new Lambda handler with the given backend.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{
		Backend: backend,
		tags:    make(map[string]*tags.Tags),
		tagsMu:  lockmetrics.New("lambda.tags"),
	}
}

func (h *Handler) setTags(resourceID string, kv map[string]string) {
	h.tagsMu.Lock("setTags")
	defer h.tagsMu.Unlock()
	if h.tags[resourceID] == nil {
		h.tags[resourceID] = tags.New("lambda." + resourceID + ".tags")
	}
	h.tags[resourceID].Merge(kv)
}

func (h *Handler) removeTags(resourceID string, keys []string) {
	h.tagsMu.RLock("removeTags")
	t := h.tags[resourceID]
	h.tagsMu.RUnlock()
	if t != nil {
		t.DeleteKeys(keys)
	}
}

func (h *Handler) getTags(resourceID string) map[string]string {
	h.tagsMu.RLock("getTags")
	t := h.tags[resourceID]
	h.tagsMu.RUnlock()
	if t == nil {
		return map[string]string{}
	}

	return t.Clone()
}

// Name returns the service name.
func (h *Handler) Name() string { return "Lambda" }

// StartWorker starts the Kinesis event source poller background goroutine, if one is configured.
// It implements service.BackgroundWorker.
func (h *Handler) StartWorker(ctx context.Context) error {
	if lambdaBk, ok := h.Backend.(*InMemoryBackend); ok {
		lambdaBk.StartKinesisPoller(ctx)
	}

	return nil
}

// GetSupportedOperations returns the list of supported Lambda operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateFunction",
		"GetFunction",
		"ListFunctions",
		"DeleteFunction",
		"UpdateFunctionCode",
		"UpdateFunctionConfiguration",
		"InvokeFunction",
		"CreateEventSourceMapping",
		"GetEventSourceMapping",
		"ListEventSourceMappings",
		"DeleteEventSourceMapping",
		"CreateFunctionURLConfig",
		"GetFunctionURLConfig",
		"DeleteFunctionURLConfig",
		"PublishVersion",
		"ListVersionsByFunction",
		"CreateAlias",
		"GetAlias",
		"ListAliases",
		"UpdateAlias",
		"DeleteAlias",
		"ListTags",
		"TagResource",
		"UntagResource",
		"PublishLayerVersion",
		"GetLayerVersion",
		"ListLayers",
		"ListLayerVersions",
		"DeleteLayerVersion",
		"GetLayerVersionPolicy",
		"AddLayerVersionPermission",
		"RemoveLayerVersionPermission",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "lambda" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this Lambda instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.DefaultRegion} }

// RouteMatcher returns a function that identifies Lambda requests by path prefix.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path
		target := c.Request().Header.Get("X-Amz-Target")

		return strings.HasPrefix(path, lambdaPathPrefix) ||
			strings.HasPrefix(path, lambda2020PathPrefix) ||
			strings.HasPrefix(path, esmPathPrefix) ||
			strings.HasPrefix(path, lambdaTagsPathPrefix) ||
			strings.HasPrefix(path, lambdaLayersPathPrefix) ||
			strings.HasPrefix(target, "AWSLambda")
	}
}

// MatchPriority returns the routing priority for the Lambda handler.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderPartial }

// layerVersionsPath is the segment name for layer version sub-paths.
const layerVersionsPath = "versions"

// layerPolicyPath is the segment name for layer policy sub-paths.
const layerPolicyPath = "policy"

// layerOpKey is the lookup key used to map a layer route to an operation name.
type layerOpKey struct {
	method   string
	lastSeg  string
	numParts int
}

// Layer path part count constants for lookup table.
const (
	// layerRootParts is the number of path parts for the root layers path (no segments after prefix).
	layerRootParts = 0
	// layerVersionListParts is the number of path parts for /{layerName}/versions.
	layerVersionListParts = 2
	// layerVersionItemParts is the number of path parts for /{layerName}/versions/{version}.
	layerVersionItemParts = 3
	// layerPolicyParts is the number of path parts when the policy segment is present.
	layerPolicyParts = 4
)

// layerOpTable maps (method, numParts, lastSegment) to an operation name.
//
//nolint:gochecknoglobals // intentional package-level lookup table
var layerOpTable = map[layerOpKey]string{
	{method: http.MethodGet, numParts: layerRootParts}:                                     "ListLayers",
	{method: http.MethodGet, numParts: layerVersionListParts, lastSeg: layerVersionsPath}:  "ListLayerVersions",
	{method: http.MethodPost, numParts: layerVersionListParts, lastSeg: layerVersionsPath}: "PublishLayerVersion",
	{method: http.MethodGet, numParts: layerVersionItemParts}:                              "GetLayerVersion",
	{method: http.MethodDelete, numParts: layerVersionItemParts}:                           "DeleteLayerVersion",
	{method: http.MethodGet, numParts: layerPolicyParts, lastSeg: layerPolicyPath}:         "GetLayerVersionPolicy",
	{method: http.MethodPost, numParts: layerPolicyParts, lastSeg: layerPolicyPath}:        "AddLayerVersionPermission",
	{method: http.MethodDelete, numParts: layerPathMaxParts, lastSeg: layerPolicyPath}:     "RemoveLayerVersionPermission",
}

// extractLayerOperation returns the operation name for a layer path, or "" if not matched.
func extractLayerOperation(rest, method string) string {
	if rest == "" {
		return layerOpTable[layerOpKey{method: method, numParts: layerRootParts}]
	}

	parts := strings.SplitN(rest, "/", layerPathMaxParts)
	if len(parts) < layerVersionListParts || parts[1] != layerVersionsPath {
		return ""
	}

	n := len(parts)

	// For versioned routes (n>=layerPolicyParts), the relevant discriminating segment is
	// parts[3] (the "policy" marker); for shorter paths the version number in parts[2] is
	// not a meaningful key so lastSeg stays empty.
	lastSeg := ""
	if n >= layerPolicyParts {
		lastSeg = parts[layerVersionItemParts]
	}

	return layerOpTable[layerOpKey{method: method, numParts: n, lastSeg: lastSeg}]
}

// ExtractOperation returns the Lambda operation name derived from the request method and path.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	path := c.Request().URL.Path
	method := c.Request().Method

	// Identify layer operations first (different path prefix).
	if after, ok := strings.CutPrefix(path, lambdaLayersPathPrefix); ok {
		rest := strings.TrimPrefix(after, "/")
		if op := extractLayerOperation(rest, method); op != "" {
			return op
		}
	}

	rest := strings.TrimPrefix(path, lambdaPathPrefix)

	for _, route := range lambdaOpRoutes {
		if route.method == method && route.match(rest) {
			return route.op
		}
	}

	return "Unknown"
}

// ExtractResource returns the function name from the request path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	rest := strings.TrimPrefix(c.Request().URL.Path, lambdaPathPrefix+"/")
	parts := strings.SplitN(rest, "/", 2) //nolint:mnd // split into at most name + rest

	if len(parts) > 0 && parts[0] != "" {
		return parts[0]
	}

	return ""
}

// IAMAction returns the IAM action for a Lambda HTTP request.
// It implements iam.ActionExtractor, providing per-service action extraction
// for Lambda REST API paths that are not covered by the global action mapper.
func (h *Handler) IAMAction(r *http.Request) string {
	path := r.URL.Path
	method := r.Method

	switch {
	case strings.HasPrefix(path, lambdaLayersPathPrefix):
		rest := strings.TrimPrefix(path, lambdaLayersPathPrefix)

		return "lambda:" + extractLayerOperation(strings.TrimPrefix(rest, "/"), method)
	case strings.HasPrefix(path, lambdaPathPrefix) || strings.HasPrefix(path, lambda2020PathPrefix):
		prefix := lambdaPathPrefix
		if strings.HasPrefix(path, lambda2020PathPrefix) {
			prefix = lambda2020PathPrefix
		}

		rest := strings.TrimPrefix(path, prefix)

		for _, route := range lambdaOpRoutes {
			if route.method == method && route.match(rest) {
				return "lambda:" + route.op
			}
		}

		return ""
	case strings.HasPrefix(path, esmPathPrefix):
		rest := strings.TrimPrefix(path, esmPathPrefix)

		return esmIAMAction(method, strings.TrimPrefix(rest, "/"))
	case strings.HasPrefix(path, lambdaTagsPathPrefix):
		if method == http.MethodGet {
			return "lambda:ListTags"
		}

		return "lambda:TagResource"
	}

	return ""
}

// esmIAMAction returns the IAM action for an event source mapping request.
// rest is the path after the ESM prefix with the leading slash stripped.
func esmIAMAction(method, rest string) string {
	switch method {
	case http.MethodPost:
		return "lambda:CreateEventSourceMapping"
	case http.MethodGet:
		if rest == "" {
			return "lambda:ListEventSourceMappings"
		}

		return "lambda:GetEventSourceMapping"
	case http.MethodDelete:
		return "lambda:DeleteEventSourceMapping"
	case http.MethodPut:
		return "lambda:UpdateEventSourceMapping"
	}

	return ""
}

// handlerEntry binds a route to a handler function.
type handlerEntry struct {
	execute func(c *echo.Context, rest string) error
	match   func(rest string) bool
	method  string
}

func (h *Handler) buildRouteHandlers() []handlerEntry {
	return append(h.buildCoreRoutes(), h.buildVersionAliasRoutes()...)
}

// buildCoreRoutes returns the core function CRUD + invoke + URL routes.
func (h *Handler) buildCoreRoutes() []handlerEntry {
	return []handlerEntry{
		{
			method:  http.MethodPost,
			match:   isEmptyRest,
			execute: func(c *echo.Context, _ string) error { return h.handleCreateFunction(c) },
		},
		{
			method:  http.MethodGet,
			match:   isEmptyRest,
			execute: func(c *echo.Context, _ string) error { return h.handleListFunctions(c) },
		},
		{
			method:  http.MethodGet,
			match:   isNameOnly,
			execute: func(c *echo.Context, rest string) error { return h.handleGetFunction(c, nameFromRest(rest)) },
		},
		{
			method:  http.MethodDelete,
			match:   isNameOnly,
			execute: func(c *echo.Context, rest string) error { return h.handleDeleteFunction(c, nameFromRest(rest)) },
		},
		{
			method: http.MethodPut,
			match:  hasSuffixCode,
			execute: func(c *echo.Context, rest string) error {
				name := strings.TrimSuffix(strings.TrimPrefix(rest, "/"), "/code")

				return h.handleUpdateFunctionCode(c, name)
			},
		},
		{
			method: http.MethodPut,
			match:  hasSuffixConfiguration,
			execute: func(c *echo.Context, rest string) error {
				name := strings.TrimSuffix(strings.TrimPrefix(rest, "/"), "/configuration")

				return h.handleUpdateFunctionConfiguration(c, name)
			},
		},
		{
			method: http.MethodPost,
			match:  hasSuffixInvocations,
			execute: func(c *echo.Context, rest string) error {
				name := strings.TrimSuffix(strings.TrimPrefix(rest, "/"), "/invocations")

				return h.handleInvoke(c, name)
			},
		},
		{
			method: http.MethodPost,
			match:  hasSuffixURL,
			execute: func(c *echo.Context, rest string) error {
				name := strings.TrimSuffix(strings.TrimPrefix(rest, "/"), "/url")

				return h.handleCreateFunctionURLConfig(c, name)
			},
		},
		{
			method: http.MethodGet,
			match:  hasSuffixURL,
			execute: func(c *echo.Context, rest string) error {
				name := strings.TrimSuffix(strings.TrimPrefix(rest, "/"), "/url")

				return h.handleGetFunctionURLConfig(c, name)
			},
		},
		{
			method: http.MethodDelete,
			match:  hasSuffixURL,
			execute: func(c *echo.Context, rest string) error {
				name := strings.TrimSuffix(strings.TrimPrefix(rest, "/"), "/url")

				return h.handleDeleteFunctionURLConfig(c, name)
			},
		},
		{
			method: http.MethodGet,
			match:  hasSuffixCodeSigningConfig,
			execute: func(c *echo.Context, _ string) error {
				// Stub: no code signing config → empty 200 response.
				return c.JSON(http.StatusOK, &lambdaEmptyOutput{})
			},
		},
	}
}

// buildVersionAliasRoutes returns routes for Lambda versions and aliases.
func (h *Handler) buildVersionAliasRoutes() []handlerEntry {
	return []handlerEntry{
		// Versions: POST and GET /2015-03-31/functions/{name}/versions
		{
			method: http.MethodPost,
			match:  hasSuffixVersions,
			execute: func(c *echo.Context, rest string) error {
				name := strings.TrimSuffix(strings.TrimPrefix(rest, "/"), "/versions")

				return h.handlePublishVersion(c, name)
			},
		},
		{
			method: http.MethodGet,
			match:  hasSuffixVersions,
			execute: func(c *echo.Context, rest string) error {
				name := strings.TrimSuffix(strings.TrimPrefix(rest, "/"), "/versions")

				return h.handleListVersionsByFunction(c, name)
			},
		},
		// Aliases: POST, GET, PUT, DELETE /2015-03-31/functions/{name}/aliases[/{aliasName}]
		{
			method: http.MethodPost,
			match:  hasSuffixAliasPath,
			execute: func(c *echo.Context, rest string) error {
				name := extractNameFromAliasPath(rest)

				return h.handleCreateAlias(c, name)
			},
		},
		{
			method: http.MethodGet,
			match:  hasSuffixAliasPath,
			execute: func(c *echo.Context, rest string) error {
				name, aliasName := extractNameAndAlias(rest)
				if aliasName != "" {
					return h.handleGetAlias(c, name, aliasName)
				}

				return h.handleListAliases(c, name)
			},
		},
		{
			method: http.MethodPut,
			match:  hasSuffixAliasPath,
			execute: func(c *echo.Context, rest string) error {
				name, aliasName := extractNameAndAlias(rest)

				return h.handleUpdateAlias(c, name, aliasName)
			},
		},
		{
			method: http.MethodDelete,
			match:  hasSuffixAliasPath,
			execute: func(c *echo.Context, rest string) error {
				name, aliasName := extractNameAndAlias(rest)

				return h.handleDeleteAlias(c, name, aliasName)
			},
		},
	}
}

// Handler returns the Echo handler function for Lambda operations.
func (h *Handler) Handler() echo.HandlerFunc {
	routes := h.buildRouteHandlers()

	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)
		path := c.Request().URL.Path
		method := c.Request().Method

		// Handle event-source-mappings routes
		if strings.HasPrefix(path, esmPathPrefix) {
			return h.handleESMRoute(c, path, method)
		}

		// Handle tags routes
		if strings.HasPrefix(path, lambdaTagsPathPrefix) {
			return h.handleTagsRoute(c, method)
		}

		// Handle layers routes
		if strings.HasPrefix(path, lambdaLayersPathPrefix) {
			return h.handleLayersRoute(c, path, method)
		}

		// Handle 2020-06-30 API routes (e.g. GetFunctionCodeSigningConfig)
		if rest2020, ok := strings.CutPrefix(path, lambda2020PathPrefix); ok {
			if method == http.MethodGet && hasSuffixCodeSigningConfig(rest2020) {
				return c.JSON(http.StatusOK, &lambdaEmptyOutput{})
			}

			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "route not found")
		}

		rest := strings.TrimPrefix(path, lambdaPathPrefix)

		for _, route := range routes {
			if route.method == method && route.match(rest) {
				return route.execute(c, rest)
			}
		}

		log.DebugContext(ctx, "lambda: unknown route", "method", method, "path", path)

		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}
}

// handleTagsRoute handles GET/POST/DELETE /2015-03-31/tags/{arn}.
func (h *Handler) handleTagsRoute(c *echo.Context, method string) error {
	arn := strings.TrimPrefix(c.Request().URL.Path, lambdaTagsPathPrefix+"/")

	switch method {
	case http.MethodGet:
		return c.JSON(http.StatusOK, &getTagsOutput{Tags: h.getTags(arn)})
	case http.MethodPost:
		body, err := httputil.ReadBody(c.Request())
		if err != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "failed to read body")
		}
		var input lambdaTagsInput
		if unmarshalErr := json.Unmarshal(body, &input); unmarshalErr != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid body")
		}
		var kv map[string]string
		if input.Tags != nil {
			kv = input.Tags.Clone()
		}
		h.setTags(arn, kv)
		c.Response().WriteHeader(http.StatusNoContent)

		return nil
	case http.MethodDelete:
		keys := c.Request().URL.Query()["tagKeys"]
		h.removeTags(arn, keys)
		c.Response().WriteHeader(http.StatusNoContent)

		return nil
	default:
		return h.writeError(c, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
	}
}

// handleESMRoute dispatches event-source-mapping REST API requests.
func (h *Handler) handleESMRoute(c *echo.Context, path, method string) error {
	rest := strings.TrimPrefix(path, esmPathPrefix)
	// Remove leading slash
	rest = strings.TrimPrefix(rest, "/")

	switch {
	case method == http.MethodPost && rest == "":
		return h.handleCreateESM(c)
	case method == http.MethodGet && rest == "":
		return h.handleListESMs(c)
	case method == http.MethodGet && rest != "":
		return h.handleGetESM(c, rest)
	case method == http.MethodDelete && rest != "":
		return h.handleDeleteESM(c, rest)
	default:
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}
}

type handleCreateESMInput struct {
	Enabled          *bool  `json:"Enabled"`
	EventSourceARN   string `json:"EventSourceArn"`
	FunctionName     string `json:"FunctionName"`
	StartingPosition string `json:"StartingPosition"`
	BatchSize        int    `json:"BatchSize"`
}

// handleCreateESM handles POST /2015-03-31/event-source-mappings/.
func (h *Handler) handleCreateESM(c *echo.Context) error {
	if lambdaBk, ok := h.Backend.(*InMemoryBackend); ok {
		body, err := httputil.ReadBody(c.Request())
		if err != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "failed to read body")
		}

		var req handleCreateESMInput

		if err = json.Unmarshal(body, &req); err != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid JSON")
		}

		enabled := req.Enabled == nil || *req.Enabled // default enabled=true

		m, err := lambdaBk.CreateEventSourceMapping(&CreateEventSourceMappingInput{
			EventSourceARN:   req.EventSourceARN,
			FunctionName:     req.FunctionName,
			StartingPosition: req.StartingPosition,
			BatchSize:        req.BatchSize,
			Enabled:          enabled,
		})
		if err != nil {
			return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
		}

		return c.JSON(http.StatusCreated, toJSONESMResponse(m))
	}

	return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
}

// handleListESMs handles GET /2015-03-31/event-source-mappings/.
func (h *Handler) handleListESMs(c *echo.Context) error {
	if lambdaBk, ok := h.Backend.(*InMemoryBackend); ok {
		functionName := c.Request().URL.Query().Get("FunctionName")
		mappings := lambdaBk.ListEventSourceMappings(functionName)
		resp := make([]jsonESMResponse, len(mappings))
		for i, m := range mappings {
			resp[i] = toJSONESMResponse(m)
		}

		return c.JSON(http.StatusOK, jsonListESMResponse{EventSourceMappings: resp})
	}

	return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
}

// handleGetESM handles GET /2015-03-31/event-source-mappings/{UUID}.
func (h *Handler) handleGetESM(c *echo.Context, id string) error {
	if lambdaBk, ok := h.Backend.(*InMemoryBackend); ok {
		m, err := lambdaBk.GetEventSourceMapping(id)
		if err != nil {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "event source mapping not found")
		}

		return c.JSON(http.StatusOK, toJSONESMResponse(m))
	}

	return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
}

// handleDeleteESM handles DELETE /2015-03-31/event-source-mappings/{UUID}.
func (h *Handler) handleDeleteESM(c *echo.Context, id string) error {
	if lambdaBk, ok := h.Backend.(*InMemoryBackend); ok {
		m, err := lambdaBk.DeleteEventSourceMapping(id)
		if err != nil {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "event source mapping not found")
		}

		return c.JSON(http.StatusOK, toJSONESMResponse(m))
	}

	return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
}

// validateCreateFunctionInput checks required fields and package-type-specific constraints.
// It normalizes PackageType to Image when omitted. Returns true if validation passes.
// If validation fails, it writes the HTTP error response and returns false.
func (h *Handler) validateCreateFunctionInput(c *echo.Context, input *CreateFunctionInput) bool {
	if input.FunctionName == "" {
		_ = h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "FunctionName is required")

		return false
	}

	if input.PackageType == "" {
		input.PackageType = PackageTypeImage
	}

	if input.PackageType != PackageTypeImage && input.PackageType != PackageTypeZip {
		_ = h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException",
			"PackageType must be Image or Zip")

		return false
	}

	if input.Code == nil {
		_ = h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "Code is required")

		return false
	}

	if input.PackageType == PackageTypeImage && input.Code.ImageURI == "" {
		_ = h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException",
			"Code.ImageUri is required for Image package type")

		return false
	}

	if input.PackageType == PackageTypeZip {
		if input.Runtime == "" {
			_ = h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException",
				"Runtime is required for Zip package type")

			return false
		}

		if input.Code.ZipFile == nil && (input.Code.S3Bucket == "" || input.Code.S3Key == "") {
			_ = h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException",
				"Code.ZipFile or Code.S3Bucket+Code.S3Key is required for Zip package type")

			return false
		}
	}

	return true
}

func (h *Handler) handleCreateFunction(c *echo.Context) error {
	body, err := httputil.ReadBody(c.Request())
	if err != nil {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "failed to read request")
	}

	var input CreateFunctionInput
	if unmarshalErr := json.Unmarshal(body, &input); unmarshalErr != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid request body")
	}

	if !h.validateCreateFunctionInput(c, &input) {
		return nil
	}

	memorySize := input.MemorySize
	if memorySize <= 0 {
		memorySize = defaultMemorySize
	}

	timeout := input.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}

	now := time.Now().UTC()
	fn := &FunctionConfiguration{
		FunctionName:     input.FunctionName,
		FunctionArn:      buildARN(h.DefaultRegion, h.AccountID, input.FunctionName),
		Description:      input.Description,
		ImageURI:         input.Code.ImageURI,
		PackageType:      input.PackageType,
		Runtime:          input.Runtime,
		Handler:          input.Handler,
		Role:             input.Role,
		MemorySize:       memorySize,
		Timeout:          timeout,
		Environment:      input.Environment,
		Layers:           layerARNsToFunctionLayers(input.Layers),
		State:            FunctionStateActive,
		LastUpdateStatus: LastUpdateStatusSuccessful,
		CreatedAt:        now,
		LastModified:     now.Format(time.RFC3339),
		RevisionID:       uuid.New().String(),
		ZipData:          input.Code.ZipFile,
		S3BucketCode:     input.Code.S3Bucket,
		S3KeyCode:        input.Code.S3Key,
	}

	if len(fn.ZipData) > 0 {
		fn.CodeSize = int64(len(fn.ZipData))
	}

	if createErr := h.Backend.CreateFunction(fn); createErr != nil {
		if errors.Is(createErr, ErrFunctionAlreadyExists) {
			return h.writeError(c, http.StatusConflict, "ResourceConflictException", createErr.Error())
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", createErr.Error())
	}

	return c.JSON(http.StatusCreated, fn)
}

func (h *Handler) handleGetFunction(c *echo.Context, name string) error {
	fn, err := h.Backend.GetFunction(name)
	if err != nil {
		if errors.Is(err, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				fmt.Sprintf("Function not found: %s", name))
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.JSON(http.StatusOK, &GetFunctionOutput{
		Configuration: fn,
		Code:          buildCodeLocation(fn),
	})
}

func (h *Handler) handleListFunctions(c *echo.Context) error {
	fns := h.Backend.ListFunctions()

	return c.JSON(http.StatusOK, &ListFunctionsOutput{
		Functions: fns,
	})
}

func (h *Handler) handleDeleteFunction(c *echo.Context, name string) error {
	if err := h.Backend.DeleteFunction(name); err != nil {
		if errors.Is(err, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				fmt.Sprintf("Function not found: %s", name))
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleUpdateFunctionCode(c *echo.Context, name string) error {
	body, err := httputil.ReadBody(c.Request())
	if err != nil {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "failed to read request")
	}

	var input UpdateFunctionCodeInput
	if unmarshalErr := json.Unmarshal(body, &input); unmarshalErr != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid request body")
	}

	fn, getFnErr := h.Backend.GetFunction(name)
	if getFnErr != nil {
		if errors.Is(getFnErr, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				fmt.Sprintf("Function not found: %s", name))
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", getFnErr.Error())
	}

	if fn.PackageType == PackageTypeImage || fn.PackageType == "" {
		if input.ImageURI == "" {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException",
				"ImageUri is required for Image package type")
		}

		fn.ImageURI = input.ImageURI
	} else {
		// Zip package type: update zip data or S3 reference
		if input.ZipFile == nil && (input.S3Bucket == "" || input.S3Key == "") {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException",
				"ZipFile or S3Bucket+S3Key is required for Zip package type")
		}

		fn.ZipData = input.ZipFile
		fn.S3BucketCode = input.S3Bucket
		fn.S3KeyCode = input.S3Key

		if len(fn.ZipData) > 0 {
			fn.CodeSize = int64(len(fn.ZipData))
		}
	}

	fn.LastModified = time.Now().UTC().Format(time.RFC3339)
	fn.RevisionID = uuid.New().String()
	fn.LastUpdateStatus = LastUpdateStatusSuccessful

	if updateErr := h.Backend.UpdateFunction(fn); updateErr != nil {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", updateErr.Error())
	}

	return c.JSON(http.StatusOK, fn)
}

func (h *Handler) handleUpdateFunctionConfiguration(c *echo.Context, name string) error {
	body, err := httputil.ReadBody(c.Request())
	if err != nil {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "failed to read request")
	}

	var input UpdateFunctionConfigurationInput
	if unmarshalErr := json.Unmarshal(body, &input); unmarshalErr != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid request body")
	}

	fn, getFnErr := h.Backend.GetFunction(name)
	if getFnErr != nil {
		if errors.Is(getFnErr, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				fmt.Sprintf("Function not found: %s", name))
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", getFnErr.Error())
	}

	if input.Description != "" {
		fn.Description = input.Description
	}

	if input.MemorySize > 0 {
		fn.MemorySize = input.MemorySize
	}

	if input.Timeout > 0 {
		fn.Timeout = input.Timeout
	}

	if input.Environment != nil {
		fn.Environment = input.Environment
	}

	if input.Role != "" {
		fn.Role = input.Role
	}

	if input.Runtime != "" {
		fn.Runtime = input.Runtime
	}

	if input.Handler != "" {
		fn.Handler = input.Handler
	}

	if input.Layers != nil {
		fn.Layers = layerARNsToFunctionLayers(input.Layers)
	}

	fn.LastModified = time.Now().UTC().Format(time.RFC3339)
	fn.RevisionID = uuid.New().String()
	fn.LastUpdateStatus = LastUpdateStatusSuccessful

	if updateErr := h.Backend.UpdateFunction(fn); updateErr != nil {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", updateErr.Error())
	}

	return c.JSON(http.StatusOK, fn)
}

func (h *Handler) handleInvoke(c *echo.Context, name string) error {
	ctx := c.Request().Context()

	invType := c.Request().Header.Get("X-Amz-Invocation-Type")
	if invType == "" {
		invType = InvocationTypeRequestResponse
	}

	qualifier := c.Request().URL.Query().Get("Qualifier")

	body, err := httputil.ReadBody(c.Request())
	if err != nil {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "failed to read request")
	}

	if body == nil {
		body = []byte("{}")
	}

	var result []byte
	var statusCode int
	var invokeErr error

	if qi, ok := h.Backend.(QualifierInvoker); ok && qualifier != "" {
		result, statusCode, invokeErr = qi.InvokeFunctionWithQualifier(ctx, name, qualifier, invType, body)
	} else {
		result, statusCode, invokeErr = h.Backend.InvokeFunction(ctx, name, invType, body)
	}

	if invokeErr != nil {
		if errors.Is(invokeErr, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				fmt.Sprintf("Function not found: %s", name))
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", invokeErr.Error())
	}

	if statusCode == http.StatusNoContent {
		return c.NoContent(http.StatusNoContent)
	}

	if statusCode == http.StatusAccepted {
		return c.NoContent(http.StatusAccepted)
	}

	if len(result) > 0 {
		return c.JSONBlob(http.StatusOK, result)
	}

	return c.NoContent(http.StatusOK)
}

// writeError writes a Lambda-formatted JSON error response.
func (h *Handler) writeError(c *echo.Context, status int, errType, message string) error {
	return c.JSON(status, &Error{
		Type:    errType,
		Message: message,
	})
}

func (h *Handler) handleCreateFunctionURLConfig(c *echo.Context, name string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	body, err := httputil.ReadBody(c.Request())
	if err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "failed to read body")
	}

	var input CreateFunctionURLConfigInput
	if len(body) > 0 {
		if unmarshalErr := json.Unmarshal(body, &input); unmarshalErr != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid JSON")
		}
	}

	if input.AuthType == "" {
		input.AuthType = "NONE"
	}

	cfg, createErr := lambdaBk.CreateFunctionURLConfig(name, input.AuthType)
	if createErr != nil {
		if errors.Is(createErr, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				fmt.Sprintf("Function not found: %s", name))
		}

		if errors.Is(createErr, ErrFunctionAlreadyExists) {
			return h.writeError(c, http.StatusConflict, "ResourceConflictException",
				fmt.Sprintf("Function URL config already exists for: %s", name))
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", createErr.Error())
	}

	return c.JSON(http.StatusCreated, cfg)
}

func (h *Handler) handleGetFunctionURLConfig(c *echo.Context, name string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	cfg, err := lambdaBk.GetFunctionURLConfig(name)
	if err != nil {
		if errors.Is(err, ErrFunctionURLNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				fmt.Sprintf("Function URL config not found: %s", name))
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.JSON(http.StatusOK, cfg)
}

func (h *Handler) handleDeleteFunctionURLConfig(c *echo.Context, name string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	if err := lambdaBk.DeleteFunctionURLConfig(name); err != nil {
		if errors.Is(err, ErrFunctionURLNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				fmt.Sprintf("Function URL config not found: %s", name))
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.NoContent(http.StatusNoContent)
}

// isNameOnly returns true when rest is a single path segment (/{name} with no sub-paths).
func isNameOnly(rest string) bool {
	trimmed := strings.TrimPrefix(rest, "/")

	return trimmed != "" && !strings.Contains(trimmed, "/")
}

// nameFromRest strips the leading slash from a single-segment path like /{name}.
func nameFromRest(rest string) string {
	return strings.TrimPrefix(rest, "/")
}

// buildCodeLocation constructs the FunctionCodeLocation response for a function.
func buildCodeLocation(fn *FunctionConfiguration) *FunctionCodeLocation {
	if fn.PackageType == PackageTypeZip {
		loc := &FunctionCodeLocation{RepositoryType: "S3"}
		if fn.S3BucketCode != "" && fn.S3KeyCode != "" {
			loc.Location = fmt.Sprintf("s3://%s/%s", fn.S3BucketCode, fn.S3KeyCode)
		}

		return loc
	}

	return &FunctionCodeLocation{
		ImageURI:       fn.ImageURI,
		RepositoryType: "ECR",
	}
}

// buildARN constructs a Lambda function ARN.
func buildARN(region, accountID, functionName string) string {
	return arn.Build("lambda", region, accountID, "function:"+functionName)
}

// layerARNsToFunctionLayers converts a list of layer ARN strings to FunctionLayer structs.
func layerARNsToFunctionLayers(arns []string) []*FunctionLayer {
	if len(arns) == 0 {
		return nil
	}

	layers := make([]*FunctionLayer, len(arns))
	for i, a := range arns {
		layers[i] = &FunctionLayer{Arn: a}
	}

	return layers
}

// defaultMemorySize is the default Lambda function memory in MB.
const defaultMemorySize = 128

// defaultTimeout is the default Lambda function timeout in seconds.
const defaultTimeout = 3

// extractNameFromAliasPath extracts the function name from a rest path like /{name}/aliases
// or /{name}/aliases/{aliasName}.
func extractNameFromAliasPath(rest string) string {
	trimmed := strings.TrimPrefix(rest, "/")
	parts := strings.SplitN(trimmed, "/", 3) //nolint:mnd // at most: name, aliases, aliasName
	if len(parts) >= 1 {
		return parts[0]
	}

	return ""
}

// extractNameAndAlias extracts both the function name and optional alias name from rest path.
func extractNameAndAlias(rest string) (string, string) {
	trimmed := strings.TrimPrefix(rest, "/")
	parts := strings.SplitN(trimmed, "/", 3) //nolint:mnd // at most: name, aliases, aliasName

	var fnName, aliasName string

	if len(parts) >= 1 {
		fnName = parts[0]
	}

	if len(parts) >= 3 { //nolint:mnd // parts: name, "aliases", aliasName
		aliasName = parts[2]
	}

	return fnName, aliasName
}

// handlePublishVersion handles POST /2015-03-31/functions/{name}/versions.
func (h *Handler) handlePublishVersion(c *echo.Context, name string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	body, err := httputil.ReadBody(c.Request())
	if err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "failed to read body")
	}

	var input publishVersionInput

	if len(body) > 0 {
		if unmarshalErr := json.Unmarshal(body, &input); unmarshalErr != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid JSON")
		}
	}

	ver, publishErr := lambdaBk.PublishVersion(name, input.Description)
	if publishErr != nil {
		if errors.Is(publishErr, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				fmt.Sprintf("Function not found: %s", name))
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", publishErr.Error())
	}

	return c.JSON(http.StatusCreated, ver)
}

// handleListVersionsByFunction handles GET /2015-03-31/functions/{name}/versions.
func (h *Handler) handleListVersionsByFunction(c *echo.Context, name string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	versions, err := lambdaBk.ListVersionsByFunction(name)
	if err != nil {
		if errors.Is(err, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				fmt.Sprintf("Function not found: %s", name))
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.JSON(http.StatusOK, &ListVersionsByFunctionOutput{Versions: versions})
}

// handleCreateAlias handles POST /2015-03-31/functions/{name}/aliases.
func (h *Handler) handleCreateAlias(c *echo.Context, name string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	body, err := httputil.ReadBody(c.Request())
	if err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "failed to read body")
	}

	var input CreateAliasInput
	if unmarshalErr := json.Unmarshal(body, &input); unmarshalErr != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid JSON")
	}

	if input.Name == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "Name is required")
	}

	if input.FunctionVersion == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "FunctionVersion is required")
	}

	alias, createErr := lambdaBk.CreateAlias(name, &input)
	if createErr != nil {
		if errors.Is(createErr, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				fmt.Sprintf("Function not found: %s", name))
		}

		if errors.Is(createErr, ErrAliasAlreadyExists) {
			return h.writeError(c, http.StatusConflict, "ResourceConflictException",
				fmt.Sprintf("Alias already exists: %s", input.Name))
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", createErr.Error())
	}

	return c.JSON(http.StatusCreated, alias)
}

// handleGetAlias handles GET /2015-03-31/functions/{name}/aliases/{aliasName}.
func (h *Handler) handleGetAlias(c *echo.Context, name, aliasName string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	alias, err := lambdaBk.GetAlias(name, aliasName)
	if err != nil {
		if errors.Is(err, ErrAliasNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				fmt.Sprintf("Alias not found: %s", aliasName))
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.JSON(http.StatusOK, alias)
}

// handleListAliases handles GET /2015-03-31/functions/{name}/aliases.
func (h *Handler) handleListAliases(c *echo.Context, name string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	aliases, err := lambdaBk.ListAliases(name)
	if err != nil {
		if errors.Is(err, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				fmt.Sprintf("Function not found: %s", name))
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.JSON(http.StatusOK, &ListAliasesOutput{Aliases: aliases})
}

// handleUpdateAlias handles PUT /2015-03-31/functions/{name}/aliases/{aliasName}.
func (h *Handler) handleUpdateAlias(c *echo.Context, name, aliasName string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	body, err := httputil.ReadBody(c.Request())
	if err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "failed to read body")
	}

	var input UpdateAliasInput
	if unmarshalErr := json.Unmarshal(body, &input); unmarshalErr != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid JSON")
	}

	alias, updateErr := lambdaBk.UpdateAlias(name, aliasName, &input)
	if updateErr != nil {
		if errors.Is(updateErr, ErrAliasNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				fmt.Sprintf("Alias not found: %s", aliasName))
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", updateErr.Error())
	}

	return c.JSON(http.StatusOK, alias)
}

// handleDeleteAlias handles DELETE /2015-03-31/functions/{name}/aliases/{aliasName}.
func (h *Handler) handleDeleteAlias(c *echo.Context, name, aliasName string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	if err := lambdaBk.DeleteAlias(name, aliasName); err != nil {
		if errors.Is(err, ErrAliasNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				fmt.Sprintf("Alias not found: %s", aliasName))
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.NoContent(http.StatusNoContent)
}

// TaggedFunctionInfo contains a Lambda function's ARN and tag snapshot.
// Used by the Resource Groups Tagging API cross-service listing.
type TaggedFunctionInfo struct {
	Tags map[string]string
	ARN  string
}

// TaggedFunctions returns a snapshot of all Lambda functions with their ARNs and tags.
// Intended for use by the Resource Groups Tagging API provider.
func (h *Handler) TaggedFunctions() []TaggedFunctionInfo {
	fns := h.Backend.ListFunctions()

	h.tagsMu.RLock("TaggedFunctions")
	defer h.tagsMu.RUnlock()

	result := make([]TaggedFunctionInfo, 0, len(fns))

	for _, fn := range fns {
		var tagMap map[string]string
		if t := h.tags[fn.FunctionArn]; t != nil {
			tagMap = t.Clone()
		}

		result = append(result, TaggedFunctionInfo{ARN: fn.FunctionArn, Tags: tagMap})
	}

	return result
}

// TagFunctionByARN applies tags to the Lambda function identified by its ARN.
func (h *Handler) TagFunctionByARN(fnARN string, newTags map[string]string) error {
	fns := h.Backend.ListFunctions()

	for _, fn := range fns {
		if fn.FunctionArn == fnARN {
			h.setTags(fn.FunctionArn, newTags)

			return nil
		}
	}

	return fmt.Errorf("%w: %s", ErrFunctionNotFound, fnARN)
}

// UntagFunctionByARN removes the specified tag keys from the Lambda function identified by its ARN.
func (h *Handler) UntagFunctionByARN(fnARN string, tagKeys []string) error {
	fns := h.Backend.ListFunctions()

	for _, fn := range fns {
		if fn.FunctionArn == fnARN {
			h.removeTags(fn.FunctionArn, tagKeys)

			return nil
		}
	}

	return fmt.Errorf("%w: %s", ErrFunctionNotFound, fnARN)
}

// layerPathMaxParts is the maximum number of path segments to split when parsing a layer route.
// Format: layerName / "versions" / versionNumber / "policy" / statementId.
const layerPathMaxParts = 5

// handleLayersRoute dispatches Lambda Layers REST API requests.
// Path format: /2018-10-31/layers[/{layerName}[/versions[/{versionNumber}[/policy[/{statementId}]]]]].
func (h *Handler) handleLayersRoute(c *echo.Context, path, method string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	rest := strings.TrimPrefix(path, lambdaLayersPathPrefix)
	rest = strings.TrimPrefix(rest, "/")

	// GET /2018-10-31/layers → ListLayers
	if rest == "" && method == http.MethodGet {
		return h.handleListLayers(c, lambdaBk)
	}

	// Parse: {layerName}[/versions[/{versionNumber}[/policy[/{statementId}]]]]
	parts := strings.SplitN(rest, "/", layerPathMaxParts)
	layerName := parts[0]

	if len(parts) == 1 || parts[1] != layerVersionsPath {
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}

	// GET /2018-10-31/layers/{layerName}/versions → ListLayerVersions
	if len(parts) == 2 && method == http.MethodGet {
		return h.handleListLayerVersions(c, lambdaBk, layerName)
	}

	// POST /2018-10-31/layers/{layerName}/versions → PublishLayerVersion
	if len(parts) == 2 && method == http.MethodPost {
		return h.handlePublishLayerVersion(c, lambdaBk, layerName)
	}

	if len(parts) < 3 { //nolint:mnd // minimum parts for versioned sub-routes: layerName, "versions", versionNum
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}

	version, parseErr := parseLayerVersion(parts[2])
	if parseErr != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid version number")
	}

	return h.handleLayerVersionedRoutes(c, lambdaBk, layerName, version, parts, method)
}

// handleLayerVersionedRoutes dispatches routes that require a specific layer version number.
func (h *Handler) handleLayerVersionedRoutes(
	c *echo.Context, bk *InMemoryBackend, layerName string, version int64, parts []string, method string,
) error {
	// GET/DELETE /2018-10-31/layers/{layerName}/versions/{versionNumber}
	if len(parts) == 3 { //nolint:mnd // parts: layerName, "versions", versionNum
		switch method {
		case http.MethodGet:
			return h.handleGetLayerVersion(c, bk, layerName, version)
		case http.MethodDelete:
			return h.handleDeleteLayerVersion(c, bk, layerName, version)
		}
	}

	if len(parts) < 4 || parts[3] != layerPolicyPath {
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}

	// GET/POST /2018-10-31/layers/{layerName}/versions/{versionNumber}/policy
	if len(parts) == 4 { //nolint:mnd // parts: layerName, "versions", versionNum, "policy"
		switch method {
		case http.MethodGet:
			return h.handleGetLayerVersionPolicy(c, bk, layerName, version)
		case http.MethodPost:
			return h.handleAddLayerVersionPermission(c, bk, layerName, version)
		}
	}

	// DELETE /2018-10-31/layers/{layerName}/versions/{versionNumber}/policy/{statementId}
	if len(parts) == layerPathMaxParts && method == http.MethodDelete {
		return h.handleRemoveLayerVersionPermission(c, bk, layerName, version, parts[4])
	}

	return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "route not found")
}

// parseLayerVersion parses a layer version string to int64.
func parseLayerVersion(s string) (int64, error) {
	var v int64

	_, err := fmt.Sscanf(s, "%d", &v)
	if err != nil {
		return 0, err
	}

	return v, nil
}

func (h *Handler) handleListLayers(c *echo.Context, bk *InMemoryBackend) error {
	layers := bk.ListLayers()

	return c.JSON(http.StatusOK, &ListLayersOutput{Layers: layers})
}

func (h *Handler) handleListLayerVersions(c *echo.Context, bk *InMemoryBackend, layerName string) error {
	versions, err := bk.ListLayerVersions(layerName)
	if err != nil {
		if errors.Is(err, ErrLayerNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				fmt.Sprintf("Layer not found: %s", layerName))
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.JSON(http.StatusOK, &ListLayerVersionsOutput{LayerVersions: versions})
}

func (h *Handler) handlePublishLayerVersion(c *echo.Context, bk *InMemoryBackend, layerName string) error {
	body, err := httputil.ReadBody(c.Request())
	if err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "failed to read body")
	}

	var input PublishLayerVersionInput
	if len(body) > 0 {
		if unmarshalErr := json.Unmarshal(body, &input); unmarshalErr != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid JSON")
		}
	}

	input.LayerName = layerName

	if input.Content == nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "Content is required")
	}

	out, publishErr := bk.PublishLayerVersion(&input)
	if publishErr != nil {
		if errors.Is(publishErr, ErrInvalidParameterValue) {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", publishErr.Error())
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", publishErr.Error())
	}

	return c.JSON(http.StatusCreated, out)
}

func (h *Handler) handleGetLayerVersion(c *echo.Context, bk *InMemoryBackend, layerName string, version int64) error {
	out, err := bk.GetLayerVersion(layerName, version)
	if err != nil {
		if errors.Is(err, ErrLayerNotFound) || errors.Is(err, ErrLayerVersionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				fmt.Sprintf("Layer version not found: %s:%d", layerName, version))
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.JSON(http.StatusOK, out)
}

func (h *Handler) handleDeleteLayerVersion(
	c *echo.Context,
	bk *InMemoryBackend,
	layerName string,
	version int64,
) error {
	if err := bk.DeleteLayerVersion(layerName, version); err != nil {
		if errors.Is(err, ErrLayerNotFound) || errors.Is(err, ErrLayerVersionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				fmt.Sprintf("Layer version not found: %s:%d", layerName, version))
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleGetLayerVersionPolicy(
	c *echo.Context,
	bk *InMemoryBackend,
	layerName string,
	version int64,
) error {
	policy, err := bk.GetLayerVersionPolicy(layerName, version)
	if err != nil {
		if errors.Is(err, ErrLayerNotFound) || errors.Is(err, ErrLayerVersionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				fmt.Sprintf("Layer version not found: %s:%d", layerName, version))
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.JSON(http.StatusOK, policy)
}

func (h *Handler) handleAddLayerVersionPermission(
	c *echo.Context, bk *InMemoryBackend, layerName string, version int64,
) error {
	body, err := httputil.ReadBody(c.Request())
	if err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "failed to read body")
	}

	var input AddLayerVersionPermissionInput
	if len(body) > 0 {
		if unmarshalErr := json.Unmarshal(body, &input); unmarshalErr != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid JSON")
		}
	}

	if input.StatementID == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "StatementId is required")
	}

	if input.Action == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "Action is required")
	}

	if input.Principal == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "Principal is required")
	}

	out, addErr := bk.AddLayerVersionPermission(layerName, version, &input)
	if addErr != nil {
		if errors.Is(addErr, ErrLayerNotFound) || errors.Is(addErr, ErrLayerVersionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				fmt.Sprintf("Layer version not found: %s:%d", layerName, version))
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", addErr.Error())
	}

	return c.JSON(http.StatusCreated, out)
}

func (h *Handler) handleRemoveLayerVersionPermission(
	c *echo.Context, bk *InMemoryBackend, layerName string, version int64, statementID string,
) error {
	if err := bk.RemoveLayerVersionPermission(layerName, version, statementID); err != nil {
		if errors.Is(err, ErrLayerNotFound) || errors.Is(err, ErrLayerVersionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				fmt.Sprintf("Layer version not found: %s:%d", layerName, version))
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.NoContent(http.StatusNoContent)
}
