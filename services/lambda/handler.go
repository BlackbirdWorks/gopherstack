package lambda

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	opListProvisionedConcurrencyConfigs = "ListProvisionedConcurrencyConfigs"
)

// lambdaPathPrefix is the path prefix for Lambda REST API v1 endpoints.
const lambdaPathPrefix = "/2015-03-31/functions"

// lambda2017PathPrefix is the API date prefix used by the AWS SDK v2 for
// reserved concurrency operations (PutFunctionConcurrency, DeleteFunctionConcurrency).
const lambda2017PathPrefix = "/2017-10-31/functions"

// lambda2019PathPrefix is the API date prefix used by the AWS SDK v2 for
// provisioned concurrency and GetFunctionConcurrency operations.
const lambda2019PathPrefix = "/2019-09-30/functions"

// lambda2020PathPrefix is the path prefix for Lambda REST API v2 endpoints (e.g. code signing configs).
const lambda2020PathPrefix = "/2020-06-30/functions"

// lambda2021PathPrefix is the path prefix for Lambda function URL config endpoints (SDK "Url" casing).
const lambda2021PathPrefix = "/2021-10-31/functions"

// lambda2021RuntimeMgmtPathPrefix is the path prefix for runtime management config endpoints.
const lambda2021RuntimeMgmtPathPrefix = "/2021-07-20/functions"

// lambda2024RecursionPathPrefix is the path prefix for function recursion config endpoints.
const lambda2024RecursionPathPrefix = "/2024-08-28/functions"

// lambda2023ScalingPathPrefix is the path prefix for function scaling config endpoints.
const lambda2023ScalingPathPrefix = "/2023-10-26/functions"

// lambda2014AsyncPathPrefix is the path prefix for the legacy InvokeAsync endpoint.
const lambda2014AsyncPathPrefix = "/2014-11-13/functions"

// lambda2021StreamingPathPrefix is the path prefix for InvokeWithResponseStream.
const lambda2021StreamingPathPrefix = "/2021-11-15/functions"

// lambdaFunctionPrefixes holds all the date-versioned /functions path prefixes that
// Gopherstack normalises to lambdaPathPrefix for route matching.
//
//nolint:gochecknoglobals // intentional package-level prefix table
var lambdaFunctionPrefixes = []string{
	lambdaPathPrefix,
	lambda2017PathPrefix,
	lambda2019PathPrefix,
	lambda2020PathPrefix,
	lambda2014AsyncPathPrefix,
	lambda2021StreamingPathPrefix,
}

// esmPathPrefix is the path prefix for Lambda event source mapping endpoints.
const esmPathPrefix = "/2015-03-31/event-source-mappings"

// lambdaTagsPathPrefix is the path prefix for Lambda resource tag endpoints.
const lambdaTagsPathPrefix = "/2015-03-31/tags"

// lambdaLayersPathPrefix is the path prefix for Lambda Layers endpoints.
// The Lambda Layers API uses the 2018-10-31 date version.
const lambdaLayersPathPrefix = "/2018-10-31/layers"

// lambdaCodeSigningPathPrefix is the path prefix for Lambda code signing config endpoints.
const lambdaCodeSigningPathPrefix = "/2020-04-22/code-signing-configs"

// lambdaCapacityPathPrefix is the path prefix for Lambda capacity provider endpoints.
const lambdaCapacityPathPrefix = "/2025-11-30/capacity-providers"

// lambdaDurableExecPathPrefix is the path prefix for Lambda durable execution endpoints.
const lambdaDurableExecPathPrefix = "/2025-12-01/durable-executions"

// lambdaAccountSettingsPath is the exact path for the GetAccountSettings endpoint.
const lambdaAccountSettingsPath = "/2016-08-19/account-settings"

// lambdaLayersByArnPath is the path prefix for GetLayerVersionByArn (query-param based).
const lambdaLayersByArnPath = "/2018-10-31/layers-by-arn"

type lambdaTagsInput struct {
	Tags *tags.Tags `json:"Tags"`
}

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
	{http.MethodGet, hasSuffixConfiguration, "GetFunctionConfiguration"},
	{http.MethodPut, hasSuffixConfiguration, "UpdateFunctionConfiguration"},
	{http.MethodPost, hasSuffixInvocations, "InvokeFunction"},
	{http.MethodPost, hasSuffixURL, "CreateFunctionURLConfig"},
	{http.MethodGet, hasSuffixURL, "GetFunctionURLConfig"},
	{http.MethodDelete, hasSuffixURL, "DeleteFunctionURLConfig"},
	{http.MethodPost, hasSuffixPolicy, "AddPermission"},
	{http.MethodGet, hasSuffixPolicy, "GetPolicy"},
	{http.MethodDelete, hasSuffixPolicy, "RemovePermission"},
	{http.MethodPut, hasSuffixEventInvokeConfig, "PutFunctionEventInvokeConfig"},
	{http.MethodGet, hasSuffixEventInvokeConfig, "GetFunctionEventInvokeConfig"},
	{http.MethodPost, hasSuffixEventInvokeConfig, "UpdateFunctionEventInvokeConfig"},
	{http.MethodDelete, hasSuffixEventInvokeConfig, "DeleteFunctionEventInvokeConfig"},
	{http.MethodGet, hasSuffixEventInvokeConfigs, "ListFunctionEventInvokeConfigs"},
	{http.MethodPut, hasSuffixConcurrency, "PutFunctionConcurrency"},
	{http.MethodGet, hasSuffixConcurrency, "GetFunctionConcurrency"},
	{http.MethodDelete, hasSuffixConcurrency, "DeleteFunctionConcurrency"},
	{http.MethodPut, hasSuffixProvisionedConcurrency, "PutProvisionedConcurrencyConfig"},
	{http.MethodGet, hasSuffixProvisionedConcurrency, opListProvisionedConcurrencyConfigs},
	{http.MethodDelete, hasSuffixProvisionedConcurrency, "DeleteProvisionedConcurrencyConfig"},
	{http.MethodGet, hasSuffixCodeSigningConfig, "GetFunctionCodeSigningConfig"},
	{http.MethodPut, hasSuffixCodeSigningConfig, "PutFunctionCodeSigningConfig"},
	{http.MethodDelete, hasSuffixCodeSigningConfig, "DeleteFunctionCodeSigningConfig"},
	// SDK "Invoke" alias routes (same endpoint as InvokeFunction).
	{http.MethodPost, hasSuffixInvocations, "Invoke"},
	// InvokeAsync: POST /2014-11-13/functions/{name}/invoke-async/
	{http.MethodPost, hasSuffixInvokeAsync, "InvokeAsync"},
	// InvokeWithResponseStream: POST /2021-11-15/functions/{name}/response-streaming-invocations
	{http.MethodPost, hasSuffixResponseStream, "InvokeWithResponseStream"},
}

func isEmptyRest(rest string) bool            { return rest == "" }
func hasSuffixCode(rest string) bool          { return strings.HasSuffix(rest, "/code") }
func hasSuffixConfiguration(rest string) bool { return strings.HasSuffix(rest, "/configuration") }
func hasSuffixInvocations(rest string) bool   { return strings.HasSuffix(rest, "/invocations") }
func hasSuffixURL(rest string) bool           { return strings.HasSuffix(rest, "/url") }
func hasSuffixConcurrency(rest string) bool   { return strings.HasSuffix(rest, "/concurrency") }
func hasSuffixProvisionedConcurrency(rest string) bool {
	return strings.HasSuffix(rest, "/provisioned-concurrency")
}

func hasSuffixEventInvokeConfig(rest string) bool {
	return strings.HasSuffix(rest, "/event-invoke-config")
}

func hasSuffixEventInvokeConfigs(rest string) bool {
	return strings.HasSuffix(rest, "/event-invoke-configs")
}

func hasSuffixCodeSigningConfig(rest string) bool {
	return strings.HasSuffix(rest, "/code-signing-config")
}
func hasSuffixPolicy(rest string) bool      { return strings.HasSuffix(rest, "/policy") }
func hasSuffixVersions(rest string) bool    { return strings.HasSuffix(rest, "/versions") }
func hasSuffixInvokeAsync(rest string) bool { return strings.HasSuffix(rest, "/invoke-async/") }
func hasSuffixResponseStream(rest string) bool {
	return strings.HasSuffix(rest, "/response-streaming-invocations")
}

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

// deleteTags removes the tags entry for a resource and releases its resources.
func (h *Handler) deleteTags(resourceID string) {
	h.tagsMu.Lock("deleteTags")
	t := h.tags[resourceID]
	delete(h.tags, resourceID)
	h.tagsMu.Unlock()

	if t != nil {
		t.Close()
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

// StartWorker starts the Kinesis event source poller and the resource janitor.
// It implements service.BackgroundWorker.
func (h *Handler) StartWorker(ctx context.Context) error {
	if lambdaBk, ok := h.Backend.(*InMemoryBackend); ok {
		lambdaBk.StartKinesisPoller(ctx)
		janitor := NewJanitor(lambdaBk, lambdaBk.settings)
		go janitor.Run(ctx)
	}

	return nil
}

// GetSupportedOperations returns the list of supported Lambda operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"AddPermission",
		"CheckpointDurableExecution",
		"CreateCapacityProvider",
		"CreateCodeSigningConfig",
		"CreateFunction",
		"CreateFunctionUrlConfig",
		"CreateEventSourceMapping",
		"CreateAlias",
		"DeleteAlias",
		"DeleteCapacityProvider",
		"DeleteCodeSigningConfig",
		"DeleteFunction",
		"DeleteFunctionCodeSigningConfig",
		"DeleteFunctionConcurrency",
		"DeleteFunctionEventInvokeConfig",
		"DeleteFunctionUrlConfig",
		"DeleteEventSourceMapping",
		"DeleteLayerVersion",
		"DeleteProvisionedConcurrencyConfig",
		"GetAccountSettings",
		"GetAlias",
		"GetCapacityProvider",
		"GetCodeSigningConfig",
		"GetEventSourceMapping",
		"GetFunction",
		"GetFunctionCodeSigningConfig",
		"GetFunctionConcurrency",
		"GetFunctionConfiguration",
		"GetFunctionEventInvokeConfig",
		"GetFunctionRecursionConfig",
		"GetFunctionScalingConfig",
		"GetFunctionUrlConfig",
		"GetLayerVersion",
		"GetLayerVersionByArn",
		"GetLayerVersionPolicy",
		"GetPolicy",
		"GetProvisionedConcurrencyConfig",
		"GetRuntimeManagementConfig",
		"InvokeFunction",
		"ListAliases",
		"ListCapacityProviders",
		"ListCodeSigningConfigs",
		"ListEventSourceMappings",
		"ListFunctionEventInvokeConfigs",
		"ListFunctions",
		"ListFunctionsByCodeSigningConfig",
		"ListFunctionUrlConfigs",
		"ListLayerVersions",
		"ListLayers",
		opListProvisionedConcurrencyConfigs,
		"ListTags",
		"ListVersionsByFunction",
		"AddLayerVersionPermission",
		"PublishLayerVersion",
		"PublishVersion",
		"PutFunctionCodeSigningConfig",
		"PutFunctionConcurrency",
		"PutFunctionEventInvokeConfig",
		"PutFunctionRecursionConfig",
		"PutFunctionScalingConfig",
		"PutProvisionedConcurrencyConfig",
		"PutRuntimeManagementConfig",
		"RemoveLayerVersionPermission",
		"RemovePermission",
		"TagResource",
		"UntagResource",
		"UpdateAlias",
		"UpdateCapacityProvider",
		"UpdateCodeSigningConfig",
		"UpdateEventSourceMapping",
		"UpdateFunctionCode",
		"UpdateFunctionConfiguration",
		"UpdateFunctionEventInvokeConfig",
		"UpdateFunctionUrlConfig",
		// Durable execution stubs (Lambda Workflows).
		"GetDurableExecution",
		"GetDurableExecutionHistory",
		"GetDurableExecutionState",
		"ListDurableExecutionsByFunction",
		"SendDurableExecutionCallbackFailure",
		"SendDurableExecutionCallbackHeartbeat",
		"SendDurableExecutionCallbackSuccess",
		"StopDurableExecution",
		// Capacity provider — function version listing.
		"ListFunctionVersionsByCapacityProvider",
		// SDK "Invoke" alias + async/streaming variants.
		"Invoke",
		"InvokeAsync",
		"InvokeWithResponseStream",
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

		return isLambdaPath(path) || strings.HasPrefix(target, "AWSLambda")
	}
}

// lambdaPathPrefixes holds all path prefixes handled by the Lambda service.
//
//nolint:gochecknoglobals // intentional package-level prefix table
var lambdaPathPrefixes = []string{
	lambdaPathPrefix,
	lambda2017PathPrefix,
	lambda2019PathPrefix,
	lambda2020PathPrefix,
	lambda2021PathPrefix,
	lambda2021RuntimeMgmtPathPrefix,
	lambda2023ScalingPathPrefix,
	lambda2024RecursionPathPrefix,
	lambda2014AsyncPathPrefix,
	lambda2021StreamingPathPrefix,
	esmPathPrefix,
	lambdaTagsPathPrefix,
	lambdaLayersPathPrefix,
	lambdaCodeSigningPathPrefix,
	lambdaCapacityPathPrefix,
	lambdaDurableExecPathPrefix,
}

// isLambdaPath returns true when the given path belongs to the Lambda service.
func isLambdaPath(path string) bool {
	if path == lambdaAccountSettingsPath || path == lambdaLayersByArnPath {
		return true
	}

	for _, prefix := range lambdaPathPrefixes {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}

	return false
}

// normalizeFunctionPath strips the date-versioned /functions prefix from path and
// returns the remainder (including the leading slash before the function name).
// It falls back to stripping lambdaPathPrefix when no other prefix matches.
func normalizeFunctionPath(path string) string {
	for _, prefix := range lambdaFunctionPrefixes {
		if after, ok := strings.CutPrefix(path, prefix); ok {
			return after
		}
	}

	return strings.TrimPrefix(path, lambdaPathPrefix)
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

	rest := normalizeFunctionPath(path)

	// Special case: GET /provisioned-concurrency dispatches to Get vs List based on Qualifier.
	if op := resolveProvisionedConcurrencyOp(method, rest, c.Request().URL.Query().Get("Qualifier")); op != "" {
		return op
	}

	for _, route := range lambdaOpRoutes {
		if route.method == method && route.match(rest) {
			return route.op
		}
	}

	return "Unknown"
}

// ExtractResource returns the function name from the request path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	rest := strings.TrimPrefix(normalizeFunctionPath(c.Request().URL.Path), "/")
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
	case strings.HasPrefix(path, lambdaPathPrefix) ||
		strings.HasPrefix(path, lambda2017PathPrefix) ||
		strings.HasPrefix(path, lambda2019PathPrefix) ||
		strings.HasPrefix(path, lambda2020PathPrefix):
		rest := normalizeFunctionPath(path)

		// Special case: GET /provisioned-concurrency dispatches to Get vs List based on Qualifier.
		if op := resolveProvisionedConcurrencyOp(method, rest, r.URL.Query().Get("Qualifier")); op != "" {
			return "lambda:" + op
		}

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

// resolveProvisionedConcurrencyOp returns the operation name for a GET
// /provisioned-concurrency request, disambiguating between Get (Qualifier present)
// and List (Qualifier absent). Returns "" for non-matching requests.
func resolveProvisionedConcurrencyOp(method, rest, qualifier string) string {
	if method != http.MethodGet || !hasSuffixProvisionedConcurrency(rest) {
		return ""
	}

	if qualifier != "" {
		return "GetProvisionedConcurrencyConfig"
	}

	return opListProvisionedConcurrencyConfigs
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
	return append(
		append(h.buildCoreRoutes(), h.buildEventInvokeRoutes()...),
		append(h.buildConcurrencyRoutes(), h.buildVersionAliasRoutes()...)...,
	)
}

// buildCoreRoutes returns the core function CRUD + invoke + URL + policy routes.
func (h *Handler) buildCoreRoutes() []handlerEntry {
	return append(h.buildFunctionCRUDRoutes(), h.buildFunctionURLPolicyRoutes()...)
}

// buildFunctionCRUDRoutes returns the basic function CRUD and invoke routes.
func (h *Handler) buildFunctionCRUDRoutes() []handlerEntry {
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
			method: http.MethodGet,
			match:  hasSuffixConfiguration,
			execute: func(c *echo.Context, rest string) error {
				name := strings.TrimSuffix(strings.TrimPrefix(rest, "/"), "/configuration")

				return h.handleGetFunctionConfiguration(c, name)
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
			match:  hasSuffixInvokeAsync,
			execute: func(c *echo.Context, rest string) error {
				name := strings.TrimSuffix(strings.TrimPrefix(rest, "/"), "/invoke-async/")

				return h.handleInvokeAsync(c, name)
			},
		},
		{
			method: http.MethodPost,
			match:  hasSuffixResponseStream,
			execute: func(c *echo.Context, rest string) error {
				name := strings.TrimSuffix(strings.TrimPrefix(rest, "/"), "/response-streaming-invocations")

				return h.handleInvokeWithResponseStream(c, name)
			},
		},
	}
}

// buildFunctionURLPolicyRoutes returns the function URL and resource policy routes.
func (h *Handler) buildFunctionURLPolicyRoutes() []handlerEntry {
	return []handlerEntry{
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
			method: http.MethodPost,
			match:  hasSuffixPolicy,
			execute: func(c *echo.Context, rest string) error {
				name := strings.TrimSuffix(strings.TrimPrefix(rest, "/"), "/policy")

				return h.handleAddPermission(c, name)
			},
		},
		{
			method: http.MethodGet,
			match:  hasSuffixPolicy,
			execute: func(c *echo.Context, rest string) error {
				name := strings.TrimSuffix(strings.TrimPrefix(rest, "/"), "/policy")

				return h.handleGetPolicy(c, name)
			},
		},
		{
			method: http.MethodDelete,
			match:  hasSuffixPolicy,
			execute: func(c *echo.Context, rest string) error {
				name := strings.TrimSuffix(strings.TrimPrefix(rest, "/"), "/policy")

				return h.handleRemovePermission(c, name)
			},
		},
	}
}

// buildEventInvokeRoutes returns the event invoke config routes.
func (h *Handler) buildEventInvokeRoutes() []handlerEntry {
	return []handlerEntry{
		{
			method: http.MethodPut,
			match:  hasSuffixEventInvokeConfig,
			execute: func(c *echo.Context, rest string) error {
				name := strings.TrimSuffix(strings.TrimPrefix(rest, "/"), "/event-invoke-config")

				return h.handlePutFunctionEventInvokeConfig(c, name)
			},
		},
		{
			method: http.MethodGet,
			match:  hasSuffixEventInvokeConfig,
			execute: func(c *echo.Context, rest string) error {
				name := strings.TrimSuffix(strings.TrimPrefix(rest, "/"), "/event-invoke-config")

				return h.handleGetFunctionEventInvokeConfig(c, name)
			},
		},
		{
			method: http.MethodPost,
			match:  hasSuffixEventInvokeConfig,
			execute: func(c *echo.Context, rest string) error {
				name := strings.TrimSuffix(strings.TrimPrefix(rest, "/"), "/event-invoke-config")

				return h.handleUpdateFunctionEventInvokeConfig(c, name)
			},
		},
		{
			method: http.MethodDelete,
			match:  hasSuffixEventInvokeConfig,
			execute: func(c *echo.Context, rest string) error {
				name := strings.TrimSuffix(strings.TrimPrefix(rest, "/"), "/event-invoke-config")

				return h.handleDeleteFunctionEventInvokeConfig(c, name)
			},
		},
		{
			method: http.MethodGet,
			match:  hasSuffixEventInvokeConfigs,
			execute: func(c *echo.Context, rest string) error {
				name := strings.TrimSuffix(strings.TrimPrefix(rest, "/"), "/event-invoke-configs")

				return h.handleListFunctionEventInvokeConfigs(c, name)
			},
		},
	}
}

// buildConcurrencyRoutes returns the function concurrency management routes.
func (h *Handler) buildConcurrencyRoutes() []handlerEntry {
	return []handlerEntry{
		{
			method: http.MethodPut,
			match:  hasSuffixConcurrency,
			execute: func(c *echo.Context, rest string) error {
				name := strings.TrimSuffix(strings.TrimPrefix(rest, "/"), "/concurrency")

				return h.handlePutFunctionConcurrency(c, name)
			},
		},
		{
			method: http.MethodGet,
			match:  hasSuffixConcurrency,
			execute: func(c *echo.Context, rest string) error {
				name := strings.TrimSuffix(strings.TrimPrefix(rest, "/"), "/concurrency")

				return h.handleGetFunctionConcurrency(c, name)
			},
		},
		{
			method: http.MethodDelete,
			match:  hasSuffixConcurrency,
			execute: func(c *echo.Context, rest string) error {
				name := strings.TrimSuffix(strings.TrimPrefix(rest, "/"), "/concurrency")

				return h.handleDeleteFunctionConcurrency(c, name)
			},
		},
		{
			method: http.MethodPut,
			match:  hasSuffixProvisionedConcurrency,
			execute: func(c *echo.Context, rest string) error {
				name := strings.TrimSuffix(strings.TrimPrefix(rest, "/"), "/provisioned-concurrency")

				return h.handlePutProvisionedConcurrencyConfig(c, name)
			},
		},
		{
			method: http.MethodGet,
			match:  hasSuffixProvisionedConcurrency,
			execute: func(c *echo.Context, rest string) error {
				name := strings.TrimSuffix(strings.TrimPrefix(rest, "/"), "/provisioned-concurrency")
				qualifier := c.Request().URL.Query().Get("Qualifier")
				if qualifier != "" {
					return h.handleGetProvisionedConcurrencyConfig(c, name, qualifier)
				}

				return h.handleListProvisionedConcurrencyConfigs(c, name)
			},
		},
		{
			method: http.MethodDelete,
			match:  hasSuffixProvisionedConcurrency,
			execute: func(c *echo.Context, rest string) error {
				name := strings.TrimSuffix(strings.TrimPrefix(rest, "/"), "/provisioned-concurrency")

				return h.handleDeleteProvisionedConcurrencyConfig(c, name)
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

		if handled, err := h.dispatchSpecialRoutes(c, path, method); handled {
			return err
		}

		rest := normalizeFunctionPath(path)

		for _, route := range routes {
			if route.method == method && route.match(rest) {
				return route.execute(c, rest)
			}
		}

		log.DebugContext(ctx, "lambda: unknown route", "method", method, "path", path)

		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}
}

// dispatchSpecialRoutes handles non-function-path routes. It returns (true, err) when it
// matched and dispatched the request, or (false, nil) when the caller should continue.
func (h *Handler) dispatchSpecialRoutes(c *echo.Context, path, method string) (bool, error) {
	switch {
	case strings.HasPrefix(path, esmPathPrefix):
		return true, h.handleESMRoute(c, path, method)
	case strings.HasPrefix(path, lambdaTagsPathPrefix):
		return true, h.handleTagsRoute(c, method)
	// layers-by-arn must be checked before lambdaLayersPathPrefix (it's a prefix match)
	case path == lambdaLayersByArnPath:
		return true, h.handleGetLayerVersionByArn(c)
	case strings.HasPrefix(path, lambdaLayersPathPrefix):
		return true, h.handleLayersRoute(c, path, method)
	case path == lambdaAccountSettingsPath:
		return true, h.handleGetAccountSettings(c)
	case strings.HasPrefix(path, lambdaCodeSigningPathPrefix):
		return true, h.handleCodeSigningRoute(c, path, method)
	case strings.HasPrefix(path, lambdaCapacityPathPrefix):
		return true, h.handleCapacityProviderRoute(c, path, method)
	case strings.HasPrefix(path, lambdaDurableExecPathPrefix):
		return true, h.handleDurableExecRoute(c, path, method)
	case strings.HasPrefix(path, lambda2021PathPrefix):
		return true, h.handleFunctionURLRoute2021(c, path, method)
	case strings.HasPrefix(path, lambda2021RuntimeMgmtPathPrefix):
		return true, h.handleRuntimeMgmtRoute(c, path, method)
	case strings.HasPrefix(path, lambda2024RecursionPathPrefix):
		return true, h.handleRecursionConfigRoute(c, path, method)
	case strings.HasPrefix(path, lambda2023ScalingPathPrefix):
		return true, h.handleScalingConfigRoute(c, path, method)
	}

	if rest2020, ok := strings.CutPrefix(path, lambda2020PathPrefix); ok {
		return true, h.handle2020FunctionRoute(c, rest2020, method)
	}

	return false, nil
}

// handleTagsRoute handles GET/POST/DELETE /2015-03-31/tags/{arn}.
func (h *Handler) handleTagsRoute(c *echo.Context, method string) error {
	resourceARN := strings.TrimPrefix(c.Request().URL.Path, lambdaTagsPathPrefix+"/")
	fnName := functionNameFromARN(resourceARN)

	switch method {
	case http.MethodGet:
		if lambdaBk, ok := h.Backend.(*InMemoryBackend); ok && fnName != "" {
			fn, err := lambdaBk.GetFunction(fnName)
			if err == nil {
				return c.JSON(http.StatusOK, &getTagsOutput{Tags: fn.Tags})
			}
		}

		return c.JSON(http.StatusOK, &getTagsOutput{Tags: h.getTags(resourceARN)})
	case http.MethodPost:
		body, err := httputils.ReadBody(c.Request())
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
		if lambdaBk, ok := h.Backend.(*InMemoryBackend); ok && fnName != "" {
			_ = lambdaBk.TagResource(fnName, kv)
		}
		h.setTags(resourceARN, kv)
		c.Response().WriteHeader(http.StatusNoContent)

		return nil
	case http.MethodDelete:
		keys := c.Request().URL.Query()["tagKeys"]
		if lambdaBk, ok := h.Backend.(*InMemoryBackend); ok && fnName != "" {
			_ = lambdaBk.UntagResource(fnName, keys)
		}
		h.removeTags(resourceARN, keys)
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
	case method == http.MethodPut && rest != "":
		return h.handleUpdateESM(c, rest)
	case method == http.MethodDelete && rest != "":
		return h.handleDeleteESM(c, rest)
	default:
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}
}

type handleCreateESMInput struct {
	Enabled                             *bool                                `json:"Enabled"`
	FilterCriteria                      *FilterCriteria                      `json:"FilterCriteria"`
	DestinationConfig                   *ESMDestinationConfig                `json:"DestinationConfig"`
	AmazonManagedKafkaEventSourceConfig *AmazonManagedKafkaEventSourceConfig `json:"AmazonManagedKafkaEventSourceConfig"`
	SelfManagedKafkaEventSourceConfig   *SelfManagedKafkaEventSourceConfig   `json:"SelfManagedKafkaEventSourceConfig"`
	SelfManagedEventSource              *SelfManagedEventSource              `json:"SelfManagedEventSource"`
	DocumentDBEventSourceConfig         *DocumentDBEventSourceConfig         `json:"DocumentDBEventSourceConfig"`
	EventSourceARN                      string                               `json:"EventSourceArn"`
	FunctionName                        string                               `json:"FunctionName"`
	StartingPosition                    string                               `json:"StartingPosition"`
	SourceAccessConfigurations          []SourceAccessConfiguration          `json:"SourceAccessConfigurations"`
	Topics                              []string                             `json:"Topics"`
	Queues                              []string                             `json:"Queues"`
	FunctionResponseTypes               []string                             `json:"FunctionResponseTypes"`
	BatchSize                           int                                  `json:"BatchSize"`
	MaximumBatchingWindowInSeconds      int                                  `json:"MaximumBatchingWindowInSeconds"`
	TumblingWindowInSeconds             int                                  `json:"TumblingWindowInSeconds"`
	MaximumRecordAgeInSeconds           int                                  `json:"MaximumRecordAgeInSeconds"`
	MaximumRetryAttempts                int                                  `json:"MaximumRetryAttempts"`
	ParallelizationFactor               int                                  `json:"ParallelizationFactor"`
	BisectBatchOnFunctionError          bool                                 `json:"BisectBatchOnFunctionError"`
}

// handleCreateESM handles POST /2015-03-31/event-source-mappings/.
func (h *Handler) handleCreateESM(c *echo.Context) error {
	if lambdaBk, ok := h.Backend.(*InMemoryBackend); ok {
		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "failed to read body")
		}

		var req handleCreateESMInput

		if err = json.Unmarshal(body, &req); err != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid JSON")
		}

		enabled := req.Enabled == nil || *req.Enabled // default enabled=true

		m, err := lambdaBk.CreateEventSourceMapping(&CreateEventSourceMappingInput{
			EventSourceARN:                      req.EventSourceARN,
			FunctionName:                        req.FunctionName,
			StartingPosition:                    req.StartingPosition,
			BatchSize:                           req.BatchSize,
			Enabled:                             enabled,
			FilterCriteria:                      req.FilterCriteria,
			DestinationConfig:                   req.DestinationConfig,
			AmazonManagedKafkaEventSourceConfig: req.AmazonManagedKafkaEventSourceConfig,
			SelfManagedKafkaEventSourceConfig:   req.SelfManagedKafkaEventSourceConfig,
			SelfManagedEventSource:              req.SelfManagedEventSource,
			DocumentDBEventSourceConfig:         req.DocumentDBEventSourceConfig,
			SourceAccessConfigurations:          req.SourceAccessConfigurations,
			Topics:                              req.Topics,
			Queues:                              req.Queues,
			FunctionResponseTypes:               req.FunctionResponseTypes,
			MaximumBatchingWindowInSeconds:      req.MaximumBatchingWindowInSeconds,
			TumblingWindowInSeconds:             req.TumblingWindowInSeconds,
			MaximumRecordAgeInSeconds:           req.MaximumRecordAgeInSeconds,
			MaximumRetryAttempts:                req.MaximumRetryAttempts,
			ParallelizationFactor:               req.ParallelizationFactor,
			BisectBatchOnFunctionError:          req.BisectBatchOnFunctionError,
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
		marker, maxItems := parsePaginationParams(c.Request())
		p := lambdaBk.ListEventSourceMappings(functionName, marker, maxItems)
		resp := make([]jsonESMResponse, len(p.Data))
		for i, m := range p.Data {
			resp[i] = toJSONESMResponse(m)
		}

		return c.JSON(http.StatusOK, jsonListESMResponse{EventSourceMappings: resp, NextMarker: p.Next})
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

// handleUpdateESMInput is the request body for UpdateEventSourceMapping.
type handleUpdateESMInput struct {
	Enabled                        *bool                       `json:"Enabled"`
	FilterCriteria                 *FilterCriteria             `json:"FilterCriteria"`
	DestinationConfig              *ESMDestinationConfig       `json:"DestinationConfig"`
	BisectBatchOnFunctionError     *bool                       `json:"BisectBatchOnFunctionError"`
	SourceAccessConfigurations     []SourceAccessConfiguration `json:"SourceAccessConfigurations"`
	Topics                         []string                    `json:"Topics"`
	Queues                         []string                    `json:"Queues"`
	FunctionResponseTypes          []string                    `json:"FunctionResponseTypes"`
	BatchSize                      int                         `json:"BatchSize"`
	MaximumBatchingWindowInSeconds int                         `json:"MaximumBatchingWindowInSeconds"`
	TumblingWindowInSeconds        int                         `json:"TumblingWindowInSeconds"`
	MaximumRecordAgeInSeconds      int                         `json:"MaximumRecordAgeInSeconds"`
	MaximumRetryAttempts           int                         `json:"MaximumRetryAttempts"`
	ParallelizationFactor          int                         `json:"ParallelizationFactor"`
}

// handleUpdateESM handles PUT /2015-03-31/event-source-mappings/{UUID}.
func (h *Handler) handleUpdateESM(c *echo.Context, id string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "failed to read body")
	}

	var req handleUpdateESMInput
	if err = json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid JSON")
	}

	m, err := lambdaBk.UpdateEventSourceMapping(id, &UpdateEventSourceMappingInput{
		Enabled:                        req.Enabled,
		BatchSize:                      req.BatchSize,
		FilterCriteria:                 req.FilterCriteria,
		DestinationConfig:              req.DestinationConfig,
		SourceAccessConfigurations:     req.SourceAccessConfigurations,
		Topics:                         req.Topics,
		Queues:                         req.Queues,
		FunctionResponseTypes:          req.FunctionResponseTypes,
		MaximumBatchingWindowInSeconds: req.MaximumBatchingWindowInSeconds,
		TumblingWindowInSeconds:        req.TumblingWindowInSeconds,
		MaximumRecordAgeInSeconds:      req.MaximumRecordAgeInSeconds,
		MaximumRetryAttempts:           req.MaximumRetryAttempts,
		ParallelizationFactor:          req.ParallelizationFactor,
		BisectBatchOnFunctionError:     req.BisectBatchOnFunctionError,
	})
	if err != nil {
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "event source mapping not found")
	}

	return c.JSON(http.StatusOK, toJSONESMResponse(m))
}

// isValidAliasName reports whether s is a valid Lambda alias name
// (letters, digits, hyphens, and underscores only).
func isValidAliasName(s string) bool {
	for _, ch := range s {
		if (ch < 'a' || ch > 'z') && (ch < 'A' || ch > 'Z') && (ch < '0' || ch > '9') && ch != '-' && ch != '_' {
			return false
		}
	}

	return true
}

// validateQualifier validates a function qualifier. A valid qualifier is either
// "$LATEST", an alias name (letters, digits, hyphens, underscores), or a version
// number (digits only). Returns true if valid; writes an error response and returns
// false if the qualifier is non-empty but malformed.
func (h *Handler) validateQualifier(c *echo.Context, qualifier string) bool {
	if qualifier == "" || qualifier == versionLatest {
		return true
	}

	// Version number: digits only.
	isVersion := true
	for _, ch := range qualifier {
		if ch < '0' || ch > '9' {
			isVersion = false

			break
		}
	}

	if isVersion {
		return true
	}

	// Alias name: letters, digits, hyphens, underscores.
	if !isValidAliasName(qualifier) {
		_ = h.writeError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValueException",
			fmt.Sprintf(
				"invalid qualifier %q: must be $LATEST, a version number, or a valid alias name",
				qualifier,
			),
		)

		return false
	}

	return true
}

// validateCreateFunctionInput checks required fields and package-type-specific constraints.
// It normalizes PackageType to Image when omitted. Returns true if validation passes.
// If validation fails, it writes the HTTP error response and returns false.
func (h *Handler) validateCreateFunctionInput(c *echo.Context, input *CreateFunctionInput) bool {
	if input.FunctionName == "" {
		_ = h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "FunctionName is required")

		return false
	}

	if !h.validateMemoryAndTimeout(c, input.MemorySize, input.Timeout) {
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

	if !h.validateCreateFunctionCode(c, input) {
		return false
	}

	if !h.validateSnapStartInput(c, input.SnapStart) {
		return false
	}

	return h.validateEphemeralStorageInput(c, input.EphemeralStorage)
}

// validateSnapStartInput checks the optional SnapStart.ApplyOn value. AWS only
// accepts "None" or "PublishedVersions"; anything else is rejected with
// InvalidParameterValueException. A nil config (omitted) is valid.
func (h *Handler) validateSnapStartInput(c *echo.Context, s *SnapStart) bool {
	if s == nil || s.ApplyOn == "" {
		return true
	}

	if s.ApplyOn != "None" && s.ApplyOn != "PublishedVersions" {
		_ = h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException",
			"SnapStart.ApplyOn must be one of [PublishedVersions, None]")

		return false
	}

	return true
}

// validateEphemeralStorageInput checks the optional EphemeralStorage field and writes an error
// response when the supplied size is outside the allowed range. Returns true when valid.
func (h *Handler) validateEphemeralStorageInput(c *echo.Context, es *EphemeralStorageConfig) bool {
	if es == nil {
		return true
	}

	if es.Size < minEphemeralStorageSize || es.Size > maxEphemeralStorageSize {
		_ = h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException",
			fmt.Sprintf("EphemeralStorage.Size must be between %d and %d MB",
				minEphemeralStorageSize, maxEphemeralStorageSize))

		return false
	}

	return true
}

// validateMemoryAndTimeout validates MemorySize and Timeout values (both 0 means use defaults).
func (h *Handler) validateMemoryAndTimeout(c *echo.Context, memorySize, timeout int) bool {
	if memorySize != 0 && (memorySize < minMemorySize || memorySize > maxMemorySize) {
		_ = h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException",
			fmt.Sprintf("MemorySize must be between %d and %d MB", minMemorySize, maxMemorySize))

		return false
	}

	if timeout != 0 && (timeout < minTimeout || timeout > maxTimeout) {
		_ = h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException",
			fmt.Sprintf("Timeout must be between %d and %d seconds", minTimeout, maxTimeout))

		return false
	}

	return true
}

// validateCreateFunctionCode validates the Code field based on PackageType.
func (h *Handler) validateCreateFunctionCode(c *echo.Context, input *CreateFunctionInput) bool {
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

// applyImageConfig sets fn.ImageConfig from input when the package type is Image.
func applyImageConfig(fn *FunctionConfiguration, input *CreateFunctionInput) {
	if input.PackageType == PackageTypeImage && input.ImageConfig != nil {
		fn.ImageConfig = input.ImageConfig
	}
}

// applyZipDigest computes CodeSize and CodeSha256 from fn.ZipData when present.
func applyZipDigest(fn *FunctionConfiguration) {
	if len(fn.ZipData) > 0 {
		fn.CodeSize = int64(len(fn.ZipData))
		sum := sha256.Sum256(fn.ZipData)
		fn.CodeSha256 = base64.StdEncoding.EncodeToString(sum[:])
	}
}

func (h *Handler) handleCreateFunction(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
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
		FunctionName:      input.FunctionName,
		FunctionArn:       buildARN(h.DefaultRegion, h.AccountID, input.FunctionName),
		Description:       input.Description,
		ImageURI:          input.Code.ImageURI,
		PackageType:       input.PackageType,
		Runtime:           input.Runtime,
		Handler:           input.Handler,
		Role:              input.Role,
		MemorySize:        memorySize,
		Timeout:           timeout,
		Environment:       input.Environment,
		VpcConfig:         input.VpcConfig,
		TracingConfig:     input.TracingConfig,
		FileSystemConfigs: input.FileSystemConfigs,
		DeadLetterConfig:  input.DeadLetterConfig,
		EphemeralStorage:  input.EphemeralStorage,
		Layers:            layerARNsToFunctionLayers(input.Layers),
		Tags:              input.Tags,
		State:             FunctionStateActive,
		LastUpdateStatus:  LastUpdateStatusSuccessful,
		CreatedAt:         now,
		LastModified:      now.Format(time.RFC3339),
		RevisionID:        uuid.New().String(),
		ZipData:           input.Code.ZipFile,
		S3BucketCode:      input.Code.S3Bucket,
		S3KeyCode:         input.Code.S3Key,
	}

	applyImageConfig(fn, &input)
	applyZipDigest(fn)
	applySnapStart(fn, input.SnapStart)

	if len(input.Architectures) > 0 {
		fn.Architectures = input.Architectures
	}

	if createErr := h.Backend.CreateFunction(fn); createErr != nil {
		if errors.Is(createErr, ErrFunctionAlreadyExists) {
			return h.writeError(c, http.StatusConflict, "ResourceConflictException", createErr.Error())
		}

		if errors.Is(createErr, ErrInvalidParameterValue) {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", createErr.Error())
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", createErr.Error())
	}

	// When Publish: true, immediately publish version 1 so that the caller can
	// reference aws_lambda_function.this.version (used by provisioned concurrency etc.).
	// Return a response copy with the numbered version; the stored live fn keeps "$LATEST".
	if input.Publish {
		if publishedVersion := h.maybePublishVersion(
			c.Request().Context(), fn, "CreateFunction",
		); publishedVersion != "" {
			resp := *fn
			resp.Version = publishedVersion

			return c.JSON(http.StatusCreated, &resp)
		}
	}

	return c.JSON(http.StatusCreated, fn)
}

func (h *Handler) handleGetFunction(c *echo.Context, name string) error {
	fn, err := h.Backend.GetFunction(name)
	if err != nil {
		if errors.Is(err, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Function not found: "+name)
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.JSON(http.StatusOK, &GetFunctionOutput{
		Configuration: fn,
		Code:          buildCodeLocation(fn),
	})
}

// parsePaginationParams extracts Marker and MaxItems from the request query string.
func parsePaginationParams(r *http.Request) (string, int) {
	marker := r.URL.Query().Get("Marker")
	maxItems := 0

	if v := r.URL.Query().Get("MaxItems"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxItems = n
		}
	}

	return marker, maxItems
}

func (h *Handler) handleListFunctions(c *echo.Context) error {
	marker, maxItems := parsePaginationParams(c.Request())
	p := h.Backend.ListFunctions(marker, maxItems)

	return c.JSON(http.StatusOK, &ListFunctionsOutput{
		Functions:  p.Data,
		NextMarker: p.Next,
	})
}

func (h *Handler) handleDeleteFunction(c *echo.Context, name string) error {
	if err := h.Backend.DeleteFunction(name); err != nil {
		if errors.Is(err, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Function not found: "+name)
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	// Release the tags entry for this function's ARN.
	h.deleteTags(buildARN(h.DefaultRegion, h.AccountID, name))

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleUpdateFunctionCode(c *echo.Context, name string) error {
	body, err := httputils.ReadBody(c.Request())
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
				"Function not found: "+name)
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", getFnErr.Error())
	}

	if applyErr := h.applyFunctionCodeUpdate(c, fn, &input); applyErr != nil {
		return applyErr
	}

	fn.LastModified = time.Now().UTC().Format(time.RFC3339)
	fn.RevisionID = uuid.New().String()
	fn.LastUpdateStatus = LastUpdateStatusSuccessful

	if updateErr := h.Backend.UpdateFunction(fn); updateErr != nil {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", updateErr.Error())
	}

	// Return a response copy with the numbered version when Publish=true;
	// the stored live fn keeps Version="$LATEST".
	if input.Publish {
		if publishedVersion := h.maybePublishVersion(
			c.Request().Context(), fn, "UpdateFunctionCode",
		); publishedVersion != "" {
			resp := *fn
			resp.Version = publishedVersion

			return c.JSON(http.StatusOK, &resp)
		}
	}

	return c.JSON(http.StatusOK, fn)
}

// maybePublishVersion publishes a new numbered version for fn when the InMemoryBackend is
// active and returns the published version string (e.g. "1"). Returns "" on failure or when
// the backend does not support versioning. The caller must NOT store the returned version on
// the live fn — the live function always keeps Version="$LATEST".
func (h *Handler) maybePublishVersion(ctx context.Context, fn *FunctionConfiguration, op string) string {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return ""
	}

	v, pubErr := lambdaBk.PublishVersion(fn.FunctionName, "")
	if pubErr != nil {
		logger.Load(ctx).WarnContext(
			ctx,
			"lambda: Publish=true but PublishVersion failed",
			"op", op, "function", fn.FunctionName, "error", pubErr,
		)

		return ""
	}

	return v.Version
}

// applyFunctionCodeUpdate applies the code fields from input onto fn, validating package type constraints.
func (h *Handler) applyFunctionCodeUpdate(
	c *echo.Context,
	fn *FunctionConfiguration,
	input *UpdateFunctionCodeInput,
) error {
	if fn.PackageType == PackageTypeImage || fn.PackageType == "" {
		if input.ImageURI == "" {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException",
				"ImageUri is required for Image package type")
		}

		fn.ImageURI = input.ImageURI
	} else {
		if input.ZipFile == nil && (input.S3Bucket == "" || input.S3Key == "") {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException",
				"ZipFile or S3Bucket+S3Key is required for Zip package type")
		}

		fn.ZipData = input.ZipFile
		fn.S3BucketCode = input.S3Bucket
		fn.S3KeyCode = input.S3Key

		if len(fn.ZipData) > 0 {
			fn.CodeSize = int64(len(fn.ZipData))
			sum := sha256.Sum256(fn.ZipData)
			fn.CodeSha256 = base64.StdEncoding.EncodeToString(sum[:])
		}
	}

	if len(input.Architectures) > 0 {
		fn.Architectures = input.Architectures
	} else if len(fn.Architectures) == 0 {
		fn.Architectures = []string{"x86_64"}
	}

	return nil
}

func (h *Handler) handleUpdateFunctionConfiguration(c *echo.Context, name string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "failed to read request")
	}

	var input UpdateFunctionConfigurationInput
	if unmarshalErr := json.Unmarshal(body, &input); unmarshalErr != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid request body")
	}

	if !h.validateMemoryAndTimeout(c, input.MemorySize, input.Timeout) {
		return nil
	}

	if input.EphemeralStorage != nil {
		if input.EphemeralStorage.Size < minEphemeralStorageSize ||
			input.EphemeralStorage.Size > maxEphemeralStorageSize {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException",
				fmt.Sprintf("EphemeralStorage.Size must be between %d and %d MB",
					minEphemeralStorageSize, maxEphemeralStorageSize))
		}
	}

	fn, getFnErr := h.Backend.GetFunction(name)
	if getFnErr != nil {
		if errors.Is(getFnErr, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Function not found: "+name)
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", getFnErr.Error())
	}

	applyFunctionConfigurationUpdate(fn, &input)

	fn.LastModified = time.Now().UTC().Format(time.RFC3339)
	fn.RevisionID = uuid.New().String()
	fn.LastUpdateStatus = LastUpdateStatusSuccessful

	if updateErr := h.Backend.UpdateFunction(fn); updateErr != nil {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", updateErr.Error())
	}

	return c.JSON(http.StatusOK, fn)
}

// applySnapStart sets the SnapStart field on fn based on the input.
func applySnapStart(fn *FunctionConfiguration, s *SnapStart) {
	if s == nil {
		return
	}

	applyOn := s.ApplyOn
	if applyOn == "" {
		applyOn = "None"
	}

	optimizationStatus := "Off"
	if applyOn == "PublishedVersions" {
		optimizationStatus = "On"
	}

	fn.SnapStart = &SnapStartResponse{
		ApplyOn:            applyOn,
		OptimizationStatus: optimizationStatus,
	}
}

// applyFunctionConfigurationUpdate applies non-zero fields from input onto fn.
func applyFunctionConfigurationUpdate(fn *FunctionConfiguration, input *UpdateFunctionConfigurationInput) {
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

	if input.VpcConfig != nil {
		fn.VpcConfig = input.VpcConfig
	}

	if input.TracingConfig != nil {
		fn.TracingConfig = input.TracingConfig
	}

	if input.FileSystemConfigs != nil {
		fn.FileSystemConfigs = input.FileSystemConfigs
	}

	if input.DeadLetterConfig != nil {
		fn.DeadLetterConfig = input.DeadLetterConfig
	}

	if input.EphemeralStorage != nil {
		fn.EphemeralStorage = input.EphemeralStorage
	}

	if input.SnapStart != nil {
		applySnapStart(fn, input.SnapStart)
	}
}

func (h *Handler) handleInvoke(c *echo.Context, name string) error {
	ctx := c.Request().Context()

	invType := c.Request().Header.Get("X-Amz-Invocation-Type")
	if invType == "" {
		invType = InvocationTypeRequestResponse
	}

	qualifier := c.Request().URL.Query().Get("Qualifier")

	if !h.validateQualifier(c, qualifier) {
		return nil
	}

	body, err := httputils.ReadBody(c.Request())
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
				"Function not found: "+name)
		}

		if errors.Is(invokeErr, ErrTooManyRequests) {
			return h.writeError(c, http.StatusTooManyRequests, "TooManyRequestsException", invokeErr.Error())
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
		// AWS Lambda signals an unhandled function error to the SDK via the
		// X-Amz-Function-Error response header (with the body still HTTP 200
		// containing the errorMessage/errorType JSON payload). Detect the
		// payload shape and set the header so SDK clients can distinguish
		// function errors from successful invocations.
		if isLambdaFunctionErrorPayload(result) {
			c.Response().Header().Set("X-Amz-Function-Error", "Unhandled")
		}

		return c.JSONBlob(http.StatusOK, result)
	}

	return c.NoContent(http.StatusOK)
}

// isLambdaFunctionErrorPayload reports whether result looks like a Lambda
// function-error payload, i.e. a JSON object with a top-level errorMessage
// field (typically alongside errorType / stackTrace / trace).
func isLambdaFunctionErrorPayload(result []byte) bool {
	trimmed := bytes.TrimSpace(result)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return false
	}

	var probe struct {
		ErrorMessage *json.RawMessage `json:"errorMessage"`
		ErrorType    *json.RawMessage `json:"errorType"`
	}
	if err := json.Unmarshal(trimmed, &probe); err != nil {
		return false
	}

	return probe.ErrorMessage != nil || probe.ErrorType != nil
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

	body, err := httputils.ReadBody(c.Request())
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

	cfg, createErr := lambdaBk.CreateFunctionURLConfig(name, input.AuthType, input.Cors, input.InvokeMode)
	if createErr != nil {
		if errors.Is(createErr, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Function not found: "+name)
		}

		if errors.Is(createErr, ErrFunctionAlreadyExists) {
			return h.writeError(c, http.StatusConflict, "ResourceConflictException",
				"Function URL config already exists for: "+name)
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
				"Function URL config not found: "+name)
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
				"Function URL config not found: "+name)
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

// minMemorySize is the minimum allowed Lambda function memory in MB.
const minMemorySize = 128

// maxMemorySize is the maximum allowed Lambda function memory in MB.
const maxMemorySize = 10240

// minTimeout is the minimum allowed Lambda function timeout in seconds.
const minTimeout = 1

// maxTimeout is the maximum allowed Lambda function timeout in seconds.
const maxTimeout = 900

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

	body, err := httputils.ReadBody(c.Request())
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
				"Function not found: "+name)
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

	marker, maxItems := parsePaginationParams(c.Request())

	p, err := lambdaBk.ListVersionsByFunction(name, marker, maxItems)
	if err != nil {
		if errors.Is(err, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Function not found: "+name)
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.JSON(http.StatusOK, &ListVersionsByFunctionOutput{Versions: p.Data, NextMarker: p.Next})
}

// handleCreateAlias handles POST /2015-03-31/functions/{name}/aliases.
func (h *Handler) handleCreateAlias(c *echo.Context, name string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	body, err := httputils.ReadBody(c.Request())
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
				"Function not found: "+name)
		}

		if errors.Is(createErr, ErrAliasAlreadyExists) {
			return h.writeError(c, http.StatusConflict, "ResourceConflictException",
				"Alias already exists: "+input.Name)
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
				"Alias not found: "+aliasName)
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

	marker, maxItems := parsePaginationParams(c.Request())
	functionVersion := c.Request().URL.Query().Get("FunctionVersion")

	p, err := lambdaBk.ListAliases(name, functionVersion, marker, maxItems)
	if err != nil {
		if errors.Is(err, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Function not found: "+name)
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.JSON(http.StatusOK, &ListAliasesOutput{Aliases: p.Data, NextMarker: p.Next})
}

// handleUpdateAlias handles PUT /2015-03-31/functions/{name}/aliases/{aliasName}.
func (h *Handler) handleUpdateAlias(c *echo.Context, name, aliasName string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	body, err := httputils.ReadBody(c.Request())
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
				"Alias not found: "+aliasName)
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
				"Alias not found: "+aliasName)
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
	p := h.Backend.ListFunctions("", 0)
	fns := p.Data

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
	p := h.Backend.ListFunctions("", 0)
	fns := p.Data

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
	p := h.Backend.ListFunctions("", 0)
	fns := p.Data

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
	marker, maxItems := parsePaginationParams(c.Request())
	p := bk.ListLayers(marker, maxItems)

	return c.JSON(http.StatusOK, &ListLayersOutput{Layers: p.Data, NextMarker: p.Next})
}

func (h *Handler) handleListLayerVersions(c *echo.Context, bk *InMemoryBackend, layerName string) error {
	versions, err := bk.ListLayerVersions(layerName)
	if err != nil {
		if errors.Is(err, ErrLayerNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Layer not found: "+layerName)
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.JSON(http.StatusOK, &ListLayerVersionsOutput{LayerVersions: versions})
}

func (h *Handler) handlePublishLayerVersion(c *echo.Context, bk *InMemoryBackend, layerName string) error {
	body, err := httputils.ReadBody(c.Request())
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
	body, err := httputils.ReadBody(c.Request())
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

// handlePutFunctionEventInvokeConfig handles PUT /2015-03-31/functions/{name}/event-invoke-config.
func (h *Handler) handlePutFunctionEventInvokeConfig(c *echo.Context, name string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "failed to read body")
	}

	var input PutFunctionEventInvokeConfigInput
	if len(body) > 0 {
		if unmarshalErr := json.Unmarshal(body, &input); unmarshalErr != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid JSON")
		}
	}

	cfg, putErr := lambdaBk.PutFunctionEventInvokeConfig(name, &input)
	if putErr != nil {
		return h.eventInvokeConfigError(c, putErr, name)
	}

	return c.JSON(http.StatusOK, cfg)
}

// handleGetFunctionEventInvokeConfig handles GET /2015-03-31/functions/{name}/event-invoke-config.
func (h *Handler) handleGetFunctionEventInvokeConfig(c *echo.Context, name string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	cfg, err := lambdaBk.GetFunctionEventInvokeConfig(name)
	if err != nil {
		return h.eventInvokeConfigError(c, err, name)
	}

	return c.JSON(http.StatusOK, cfg)
}

// handleUpdateFunctionEventInvokeConfig handles POST /2015-03-31/functions/{name}/event-invoke-config.
func (h *Handler) handleUpdateFunctionEventInvokeConfig(c *echo.Context, name string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "failed to read body")
	}

	var input PutFunctionEventInvokeConfigInput
	if len(body) > 0 {
		if unmarshalErr := json.Unmarshal(body, &input); unmarshalErr != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid JSON")
		}
	}

	cfg, updateErr := lambdaBk.UpdateFunctionEventInvokeConfig(name, &input)
	if updateErr != nil {
		return h.eventInvokeConfigError(c, updateErr, name)
	}

	return c.JSON(http.StatusOK, cfg)
}

// handleDeleteFunctionEventInvokeConfig handles DELETE /2015-03-31/functions/{name}/event-invoke-config.
func (h *Handler) handleDeleteFunctionEventInvokeConfig(c *echo.Context, name string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	if err := lambdaBk.DeleteFunctionEventInvokeConfig(name); err != nil {
		return h.eventInvokeConfigError(c, err, name)
	}

	return c.NoContent(http.StatusNoContent)
}

// handleListFunctionEventInvokeConfigs handles GET /2015-03-31/functions/{name}/event-invoke-configs.
func (h *Handler) handleListFunctionEventInvokeConfigs(c *echo.Context, name string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	marker, maxItems := parsePaginationParams(c.Request())

	configs, next, err := lambdaBk.ListFunctionEventInvokeConfigs(name, marker, maxItems)
	if err != nil {
		if errors.Is(err, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Function not found: "+name)
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.JSON(http.StatusOK, &ListFunctionEventInvokeConfigsOutput{
		FunctionEventInvokeConfigs: configs,
		NextMarker:                 next,
	})
}

// eventInvokeConfigError maps event invoke config errors to HTTP responses.
func (h *Handler) eventInvokeConfigError(c *echo.Context, err error, name string) error {
	switch {
	case errors.Is(err, ErrFunctionNotFound):
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
			"Function not found: "+name)
	case errors.Is(err, ErrEventInvokeConfigNotFound):
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
			fmt.Sprintf("The function %s doesn't have an event invoke config", name))
	case errors.Is(err, ErrInvalidParameterValue):
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", err.Error())
	default:
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}
}

// handlePutFunctionConcurrency handles PUT /2015-03-31/functions/{name}/concurrency.
func (h *Handler) handlePutFunctionConcurrency(c *echo.Context, name string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "failed to read body")
	}

	if len(body) == 0 {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException",
			"ReservedConcurrentExecutions is required")
	}

	var raw map[string]json.RawMessage
	if unmarshalErr := json.Unmarshal(body, &raw); unmarshalErr != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid JSON")
	}

	if _, present := raw["ReservedConcurrentExecutions"]; !present {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException",
			"ReservedConcurrentExecutions is required")
	}

	var input PutFunctionConcurrencyInput
	if unmarshalErr := json.Unmarshal(body, &input); unmarshalErr != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid JSON")
	}

	concurrency, putErr := lambdaBk.PutFunctionConcurrency(name, input.ReservedConcurrentExecutions)
	if putErr != nil {
		if errors.Is(putErr, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Function not found: "+name)
		}

		if errors.Is(putErr, ErrInvalidParameterValue) {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", putErr.Error())
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", putErr.Error())
	}

	return c.JSON(http.StatusOK, concurrency)
}

// handleGetFunctionConcurrency handles GET /2015-03-31/functions/{name}/concurrency.
func (h *Handler) handleGetFunctionConcurrency(c *echo.Context, name string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	concurrency, err := lambdaBk.GetFunctionConcurrency(name)
	if err != nil {
		if errors.Is(err, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Function not found: "+name)
		}

		if errors.Is(err, ErrFunctionConcurrencyNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				fmt.Sprintf("Function %s has no reserved concurrency configured", name))
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.JSON(http.StatusOK, concurrency)
}

// handleDeleteFunctionConcurrency handles DELETE /2015-03-31/functions/{name}/concurrency.
func (h *Handler) handleDeleteFunctionConcurrency(c *echo.Context, name string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	if err := lambdaBk.DeleteFunctionConcurrency(name); err != nil {
		if errors.Is(err, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Function not found: "+name)
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.NoContent(http.StatusNoContent)
}

// handlePutProvisionedConcurrencyConfig handles
// PUT /2015-03-31/functions/{name}/provisioned-concurrency?Qualifier={qualifier}.
func (h *Handler) handlePutProvisionedConcurrencyConfig(c *echo.Context, name string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	qualifier := c.Request().URL.Query().Get("Qualifier")
	if qualifier == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException",
			"Qualifier is required for PutProvisionedConcurrencyConfig")
	}

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "failed to read body")
	}

	var input PutProvisionedConcurrencyConfigInput
	if unmarshalErr := json.Unmarshal(body, &input); unmarshalErr != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid JSON")
	}

	cfg, putErr := lambdaBk.PutProvisionedConcurrencyConfig(name, qualifier, input.ProvisionedConcurrentExecutions)
	if putErr != nil {
		if errors.Is(putErr, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Function not found: "+name)
		}

		if errors.Is(putErr, ErrInvalidParameterValue) {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", putErr.Error())
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", putErr.Error())
	}

	return c.JSON(http.StatusCreated, cfg)
}

// handleGetProvisionedConcurrencyConfig handles
// GET /2015-03-31/functions/{name}/provisioned-concurrency?Qualifier={qualifier}.
func (h *Handler) handleGetProvisionedConcurrencyConfig(c *echo.Context, name, qualifier string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	cfg, err := lambdaBk.GetProvisionedConcurrencyConfig(name, qualifier)
	if err != nil {
		if errors.Is(err, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Function not found: "+name)
		}

		if errors.Is(err, ErrProvisionedConcurrencyConfigNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"No provisioned concurrency config found for qualifier: "+qualifier)
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.JSON(http.StatusOK, cfg)
}

// handleDeleteProvisionedConcurrencyConfig handles
// DELETE /2015-03-31/functions/{name}/provisioned-concurrency?Qualifier={qualifier}.
func (h *Handler) handleDeleteProvisionedConcurrencyConfig(c *echo.Context, name string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	qualifier := c.Request().URL.Query().Get("Qualifier")
	if qualifier == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException",
			"Qualifier is required for DeleteProvisionedConcurrencyConfig")
	}

	if err := lambdaBk.DeleteProvisionedConcurrencyConfig(name, qualifier); err != nil {
		if errors.Is(err, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Function not found: "+name)
		}

		if errors.Is(err, ErrProvisionedConcurrencyConfigNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"No provisioned concurrency config found for qualifier: "+qualifier)
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.NoContent(http.StatusNoContent)
}

// handleListProvisionedConcurrencyConfigs handles
// GET /2015-03-31/functions/{name}/provisioned-concurrency (no Qualifier).
func (h *Handler) handleListProvisionedConcurrencyConfigs(c *echo.Context, name string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	configs, err := lambdaBk.ListProvisionedConcurrencyConfigs(name)
	if err != nil {
		if errors.Is(err, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Function not found: "+name)
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.JSON(http.StatusOK, &ListProvisionedConcurrencyConfigsOutput{
		ProvisionedConcurrencyConfigs: configs,
	})
}

// Purge removes all resources older than the given cutoff time.
func (h *Handler) Purge(ctx context.Context, cutoff time.Time) {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		b.Purge(ctx, cutoff)
	}

	// Build the set of function ARNs that still exist in the backend after purge.
	remaining := make(map[string]struct{})
	pg := h.Backend.ListFunctions("", -1)
	for _, fn := range pg.Data {
		remaining[fn.FunctionArn] = struct{}{}
	}

	// Remove tags for any function that no longer exists.
	h.tagsMu.Lock("Purge")
	defer h.tagsMu.Unlock()

	for arn, t := range h.tags {
		if _, ok := remaining[arn]; !ok {
			if t != nil {
				t.Close()
			}
			delete(h.tags, arn)
		}
	}
}

// Reset clears all in-memory state from the backend. It is used by the
// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
func (h *Handler) Reset() {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		b.Reset()
	}

	// Close and clear the handler-level tag store.
	h.tagsMu.Lock("Reset")
	defer h.tagsMu.Unlock()

	for _, t := range h.tags {
		if t != nil {
			t.Close()
		}
	}

	h.tags = make(map[string]*tags.Tags)
}

// --- AddPermission handler ---

// handleAddPermission handles POST /2015-03-31/functions/{name}/policy.
func (h *Handler) handleAddPermission(c *echo.Context, name string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "failed to read body")
	}

	var input AddPermissionInput
	if unmarshalErr := json.Unmarshal(body, &input); unmarshalErr != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid JSON")
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

	out, addErr := lambdaBk.AddPermission(name, &input)
	if addErr != nil {
		if errors.Is(addErr, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Function not found: "+name)
		}

		if errors.Is(addErr, ErrFunctionAlreadyExists) {
			return h.writeError(c, http.StatusConflict, "ResourceConflictException",
				"Permission already exists: "+input.StatementID)
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", addErr.Error())
	}

	return c.JSON(http.StatusCreated, out)
}

// handleGetPolicy handles GET /2015-03-31/functions/{name}/policy.
func (h *Handler) handleGetPolicy(c *echo.Context, name string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	out, err := lambdaBk.GetPolicy(name)
	if err != nil {
		if errors.Is(err, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Function not found: "+name)
		}

		if errors.Is(err, ErrNoPolicyFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"No policy is associated with the given resource: "+name)
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.JSON(http.StatusOK, out)
}

// handleRemovePermission handles DELETE /2015-03-31/functions/{name}/policy?StatementId=xxx.
func (h *Handler) handleRemovePermission(c *echo.Context, name string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	statementID := c.Request().URL.Query().Get("StatementId")
	if statementID == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "StatementId is required")
	}

	if err := lambdaBk.RemovePermission(name, statementID); err != nil {
		if errors.Is(err, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Function not found: "+name)
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.NoContent(http.StatusNoContent)
}

// handleGetFunctionConfiguration handles GET /2015-03-31/functions/{name}/configuration.
// Real AWS returns the function configuration without the code location.
func (h *Handler) handleGetFunctionConfiguration(c *echo.Context, name string) error {
	fn, err := h.Backend.GetFunction(name)
	if err != nil {
		if errors.Is(err, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Function not found: "+name)
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	// GetFunctionConfiguration returns the configuration only (no code location).
	return c.JSON(http.StatusOK, fn)
}

// handle2020FunctionRoute handles routes under /2020-06-30/functions/{name}/...
func (h *Handler) handle2020FunctionRoute(c *echo.Context, rest2020, method string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	if !hasSuffixCodeSigningConfig(rest2020) {
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}

	name := strings.TrimSuffix(strings.TrimPrefix(rest2020, "/"), "/code-signing-config")

	switch method {
	case http.MethodGet:
		return h.handleGetFunctionCodeSigningConfig(c, lambdaBk, name)
	case http.MethodPut:
		return h.handlePutFunctionCodeSigningConfig(c, lambdaBk, name)
	case http.MethodDelete:
		return h.handleDeleteFunctionCodeSigningConfig(c, lambdaBk, name)
	default:
		return h.writeError(c, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
	}
}

// handleGetFunctionCodeSigningConfig handles GET /2020-06-30/functions/{name}/code-signing-config.
// Real AWS returns HTTP 200 with an empty CodeSigningConfigArn when the function exists but
// has no code signing config associated (not a 404).
func (h *Handler) handleGetFunctionCodeSigningConfig(c *echo.Context, bk *InMemoryBackend, name string) error {
	cscARN, err := bk.GetFunctionCodeSigningConfig(name)
	if err != nil {
		if errors.Is(err, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Function not found: "+name)
		}

		if errors.Is(err, ErrCodeSigningConfigNotFound) {
			// Real AWS returns 200 with empty ARN when no code signing config is associated.
			return c.JSON(http.StatusOK, &GetFunctionCodeSigningConfigOutput{
				CodeSigningConfigArn: "",
				FunctionName:         name,
			})
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.JSON(http.StatusOK, &GetFunctionCodeSigningConfigOutput{
		CodeSigningConfigArn: cscARN,
		FunctionName:         name,
	})
}

// handlePutFunctionCodeSigningConfig handles PUT /2020-06-30/functions/{name}/code-signing-config.
func (h *Handler) handlePutFunctionCodeSigningConfig(c *echo.Context, bk *InMemoryBackend, name string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "failed to read body")
	}

	var input PutFunctionCodeSigningConfigInput
	if unmarshalErr := json.Unmarshal(body, &input); unmarshalErr != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid JSON")
	}

	if input.CodeSigningConfigArn == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException",
			"CodeSigningConfigArn is required")
	}

	if putErr := bk.PutFunctionCodeSigningConfig(name, input.CodeSigningConfigArn); putErr != nil {
		if errors.Is(putErr, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Function or code signing config not found: "+name)
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", putErr.Error())
	}

	return c.JSON(http.StatusOK, &PutFunctionCodeSigningConfigOutput{
		CodeSigningConfigArn: input.CodeSigningConfigArn,
		FunctionName:         name,
	})
}

// handleDeleteFunctionCodeSigningConfig handles DELETE /2020-06-30/functions/{name}/code-signing-config.
func (h *Handler) handleDeleteFunctionCodeSigningConfig(c *echo.Context, bk *InMemoryBackend, name string) error {
	if err := bk.DeleteFunctionCodeSigningConfig(name); err != nil {
		if errors.Is(err, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Function not found: "+name)
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.NoContent(http.StatusNoContent)
}

// --- Code signing config handlers ---

// handleCodeSigningRoute dispatches /2020-04-22/code-signing-configs routes.
func (h *Handler) handleCodeSigningRoute(c *echo.Context, path, method string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	rest := strings.TrimPrefix(path, lambdaCodeSigningPathPrefix)
	rest = strings.TrimPrefix(rest, "/")

	// /2020-04-22/code-signing-configs → Create / List
	if rest == "" {
		switch method {
		case http.MethodPost:
			return h.handleCreateCodeSigningConfig(c, lambdaBk)
		case http.MethodGet:
			return h.handleListCodeSigningConfigs(c, lambdaBk)
		default:
			return h.writeError(c, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
		}
	}

	// /2020-04-22/code-signing-configs/{cscArn}/functions → ListFunctionsByCodeSigningConfig
	if before, ok0 := strings.CutSuffix(rest, "/functions"); ok0 {
		cscARN := before
		if method == http.MethodGet {
			return h.handleListFunctionsByCodeSigningConfig(c, lambdaBk, cscARN)
		}

		return h.writeError(c, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
	}

	// /2020-04-22/code-signing-configs/{cscArn} → Get / Delete / Update
	cscARN := rest

	switch method {
	case http.MethodGet:
		return h.handleGetCodeSigningConfig(c, lambdaBk, cscARN)
	case http.MethodDelete:
		return h.handleDeleteCodeSigningConfig(c, lambdaBk, cscARN)
	case http.MethodPut:
		return h.handleUpdateCodeSigningConfig(c, lambdaBk, cscARN)
	default:
		return h.writeError(c, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
	}
}

// handleCreateCodeSigningConfig handles POST /2020-04-22/code-signing-configs.
func (h *Handler) handleCreateCodeSigningConfig(c *echo.Context, bk *InMemoryBackend) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "failed to read body")
	}

	var input CreateCodeSigningConfigInput
	if len(body) > 0 {
		if unmarshalErr := json.Unmarshal(body, &input); unmarshalErr != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid JSON")
		}
	}

	if input.AllowedPublishers == nil {
		input.AllowedPublishers = &AllowedPublishers{SigningProfileVersionArns: []string{}}
	}

	cfg, createErr := bk.CreateCodeSigningConfig(&input)
	if createErr != nil {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", createErr.Error())
	}

	return c.JSON(http.StatusCreated, &CreateCodeSigningConfigOutput{CodeSigningConfig: cfg})
}

// handleGetCodeSigningConfig handles GET /2020-04-22/code-signing-configs/{cscArn}.
func (h *Handler) handleGetCodeSigningConfig(c *echo.Context, bk *InMemoryBackend, cscARN string) error {
	cfg, err := bk.GetCodeSigningConfig(cscARN)
	if err != nil {
		if errors.Is(err, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Code signing config not found: "+cscARN)
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.JSON(http.StatusOK, &CreateCodeSigningConfigOutput{CodeSigningConfig: cfg})
}

// handleDeleteCodeSigningConfig handles DELETE /2020-04-22/code-signing-configs/{cscArn}.
func (h *Handler) handleDeleteCodeSigningConfig(c *echo.Context, bk *InMemoryBackend, cscARN string) error {
	if err := bk.DeleteCodeSigningConfig(cscARN); err != nil {
		if errors.Is(err, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Code signing config not found: "+cscARN)
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.NoContent(http.StatusNoContent)
}

// handleUpdateCodeSigningConfig handles PUT /2020-04-22/code-signing-configs/{cscArn}.
//
//nolint:dupl // similar update-handler structure shared with handleUpdateCapacityProvider by design
func (h *Handler) handleUpdateCodeSigningConfig(c *echo.Context, bk *InMemoryBackend, cscARN string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "failed to read body")
	}

	var input UpdateCodeSigningConfigInput
	if len(body) > 0 {
		if unmarshalErr := json.Unmarshal(body, &input); unmarshalErr != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid JSON")
		}
	}

	cfg, updateErr := bk.UpdateCodeSigningConfig(cscARN, &input)
	if updateErr != nil {
		if errors.Is(updateErr, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Code signing config not found: "+cscARN)
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", updateErr.Error())
	}

	return c.JSON(http.StatusOK, &UpdateCodeSigningConfigOutput{CodeSigningConfig: cfg})
}

// handleListCodeSigningConfigs handles GET /2020-04-22/code-signing-configs.
func (h *Handler) handleListCodeSigningConfigs(c *echo.Context, bk *InMemoryBackend) error {
	cfgs := bk.ListCodeSigningConfigs()

	return c.JSON(http.StatusOK, &ListCodeSigningConfigsOutput{CodeSigningConfigs: cfgs})
}

// handleListFunctionsByCodeSigningConfig handles GET /2020-04-22/code-signing-configs/{cscArn}/functions.
func (h *Handler) handleListFunctionsByCodeSigningConfig(c *echo.Context, bk *InMemoryBackend, cscARN string) error {
	arns, err := bk.ListFunctionsByCodeSigningConfig(cscARN)
	if err != nil {
		if errors.Is(err, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Code signing config not found: "+cscARN)
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.JSON(http.StatusOK, &ListFunctionsByCodeSigningConfigOutput{FunctionArns: arns})
}

// --- Capacity provider handlers ---

// handleCapacityProviderRoute dispatches /2025-11-30/capacity-providers routes.
func (h *Handler) handleCapacityProviderRoute(c *echo.Context, path, method string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	rest := strings.TrimPrefix(path, lambdaCapacityPathPrefix)
	rest = strings.TrimPrefix(rest, "/")

	// /2025-11-30/capacity-providers → Create / List
	if rest == "" {
		switch method {
		case http.MethodPost:
			return h.handleCreateCapacityProvider(c, lambdaBk)
		case http.MethodGet:
			return h.handleListCapacityProviders(c, lambdaBk)
		default:
			return h.writeError(c, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
		}
	}

	// /2025-11-30/capacity-providers/{name}/function-versions → ListFunctionVersionsByCapacityProvider
	if strings.HasSuffix(rest, "/function-versions") && method == http.MethodGet {
		cpName := strings.TrimSuffix(rest, "/function-versions")

		return h.handleListFunctionVersionsByCapacityProvider(c, lambdaBk, cpName)
	}

	// /2025-11-30/capacity-providers/{name} → Get / Delete / Update
	name := strings.SplitN(rest, "/", 2)[0] //nolint:mnd // split name from sub-path

	switch method {
	case http.MethodGet:
		return h.handleGetCapacityProvider(c, lambdaBk, name)
	case http.MethodDelete:
		return h.handleDeleteCapacityProvider(c, lambdaBk, name)
	case http.MethodPut:
		return h.handleUpdateCapacityProvider(c, lambdaBk, name)
	default:
		return h.writeError(c, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
	}
}

// handleCreateCapacityProvider handles POST /2025-11-30/capacity-providers.
func (h *Handler) handleCreateCapacityProvider(c *echo.Context, bk *InMemoryBackend) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "failed to read body")
	}

	var input CreateCapacityProviderInput
	if len(body) > 0 {
		if unmarshalErr := json.Unmarshal(body, &input); unmarshalErr != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid JSON")
		}
	}

	if input.Name == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "Name is required")
	}

	cp, createErr := bk.CreateCapacityProvider(&input)
	if createErr != nil {
		if errors.Is(createErr, ErrFunctionAlreadyExists) {
			return h.writeError(c, http.StatusConflict, "ResourceConflictException",
				"Capacity provider already exists: "+input.Name)
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", createErr.Error())
	}

	return c.JSON(http.StatusCreated, &CreateCapacityProviderOutput{CapacityProvider: cp})
}

// handleGetCapacityProvider handles GET /2025-11-30/capacity-providers/{name}.
func (h *Handler) handleGetCapacityProvider(c *echo.Context, bk *InMemoryBackend, name string) error {
	cp, err := bk.GetCapacityProvider(name)
	if err != nil {
		if errors.Is(err, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Capacity provider not found: "+name)
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.JSON(http.StatusOK, &CreateCapacityProviderOutput{CapacityProvider: cp})
}

// handleDeleteCapacityProvider handles DELETE /2025-11-30/capacity-providers/{name}.
func (h *Handler) handleDeleteCapacityProvider(c *echo.Context, bk *InMemoryBackend, name string) error {
	if err := bk.DeleteCapacityProvider(name); err != nil {
		if errors.Is(err, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Capacity provider not found: "+name)
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.NoContent(http.StatusNoContent)
}

// handleUpdateCapacityProvider handles PUT /2025-11-30/capacity-providers/{name}.
//
//nolint:dupl // similar update-handler structure shared with handleUpdateCodeSigningConfig by design
func (h *Handler) handleUpdateCapacityProvider(c *echo.Context, bk *InMemoryBackend, name string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "failed to read body")
	}

	var input UpdateCapacityProviderInput
	if len(body) > 0 {
		if unmarshalErr := json.Unmarshal(body, &input); unmarshalErr != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid JSON")
		}
	}

	cp, updateErr := bk.UpdateCapacityProvider(name, &input)
	if updateErr != nil {
		if errors.Is(updateErr, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Capacity provider not found: "+name)
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", updateErr.Error())
	}

	return c.JSON(http.StatusOK, &UpdateCapacityProviderOutput{CapacityProvider: cp})
}

// handleListCapacityProviders handles GET /2025-11-30/capacity-providers.
func (h *Handler) handleListCapacityProviders(c *echo.Context, bk *InMemoryBackend) error {
	cps := bk.ListCapacityProviders()

	return c.JSON(http.StatusOK, &ListCapacityProvidersOutput{CapacityProviders: cps})
}

// --- Durable execution handler (stub) ---

// handleDurableExecRoute handles routes under /2025-12-01/durable-executions/...
func (h *Handler) handleDurableExecRoute(c *echo.Context, path, method string) error {
	// CheckpointDurableExecution: POST /2025-12-01/durable-executions/{arn}/checkpoint
	if method == http.MethodPost && strings.HasSuffix(path, "/checkpoint") {
		return h.handleCheckpointDurableExecution(c, path)
	}

	// StopDurableExecution: DELETE /2025-12-01/durable-executions/{arn}
	if method == http.MethodDelete {
		return h.handleStopDurableExecution(c)
	}

	// GET routes.
	if method == http.MethodGet {
		switch {
		case path == lambdaDurableExecPathPrefix || path == lambdaDurableExecPathPrefix+"/":
			return h.handleListDurableExecutionsByFunction(c)
		case strings.HasSuffix(path, "/history"):
			return h.handleGetDurableExecutionHistory(c)
		case strings.HasSuffix(path, "/state"):
			return h.handleGetDurableExecutionState(c)
		default:
			return h.handleGetDurableExecution(c)
		}
	}

	// POST sub-routes (callbacks).
	if method == http.MethodPost {
		switch {
		case strings.HasSuffix(path, "/callback/failure"):
			return h.handleSendDurableExecutionCallbackFailure(c)
		case strings.HasSuffix(path, "/callback/heartbeat"):
			return h.handleSendDurableExecutionCallbackHeartbeat(c)
		case strings.HasSuffix(path, "/callback/success"):
			return h.handleSendDurableExecutionCallbackSuccess(c)
		}
	}

	return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "route not found")
}

// handleCheckpointDurableExecution handles POST /2025-12-01/durable-executions/{arn}/checkpoint.
func (h *Handler) handleCheckpointDurableExecution(c *echo.Context, path string) error {
	store := durableExecFromBackend(h)
	if store == nil {
		return c.JSON(http.StatusOK, &CheckpointDurableExecutionOutput{})
	}

	executionARN := extractDurableExecARN(path)

	var data map[string]any
	if body, err := httputils.ReadBody(c.Request()); err == nil && len(body) > 0 {
		_ = json.Unmarshal(body, &data)
	}

	_ = store.checkpoint(executionARN, data)

	return c.JSON(http.StatusOK, &CheckpointDurableExecutionOutput{})
}

// --- Account settings handler ---

// handleGetAccountSettings handles GET /2016-08-19/account-settings.
func (h *Handler) handleGetAccountSettings(c *echo.Context) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	return c.JSON(http.StatusOK, lambdaBk.GetAccountSettings())
}

// --- Function URL config 2021-10-31 route handler ---

// handleFunctionURLRoute2021 dispatches /2021-10-31/functions/{name}/url routes (SDK "Url" casing).
func (h *Handler) handleFunctionURLRoute2021(c *echo.Context, path, method string) error {
	rest := strings.TrimPrefix(path, lambda2021PathPrefix)

	// /2021-10-31/functions/{name}/urls → ListFunctionUrlConfigs
	if strings.HasSuffix(rest, "/urls") {
		name := strings.TrimSuffix(strings.TrimPrefix(rest, "/"), "/urls")
		if method == http.MethodGet {
			return h.handleListFunctionURLConfigs(c, name)
		}

		return h.writeError(c, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
	}

	// /2021-10-31/functions/{name}/url → Create / Get / Delete / Update
	if strings.HasSuffix(rest, "/url") {
		name := strings.TrimSuffix(strings.TrimPrefix(rest, "/"), "/url")

		switch method {
		case http.MethodPost:
			return h.handleCreateFunctionURLConfig(c, name)
		case http.MethodGet:
			return h.handleGetFunctionURLConfig(c, name)
		case http.MethodDelete:
			return h.handleDeleteFunctionURLConfig(c, name)
		case http.MethodPut:
			return h.handleUpdateFunctionURLConfig(c, name)
		default:
			return h.writeError(c, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
		}
	}

	return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "route not found")
}

// handleListFunctionURLConfigs handles GET /2021-10-31/functions/{name}/urls.
func (h *Handler) handleListFunctionURLConfigs(c *echo.Context, name string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	// If name is provided, filter to that function only.
	if name != "" {
		cfg, err := lambdaBk.GetFunctionURLConfig(name)
		if err != nil {
			// Return empty list rather than 404 for a missing URL config.
			return c.JSON(http.StatusOK, &ListFunctionURLConfigsOutput{FunctionURLConfigs: []*FunctionURLConfig{}})
		}

		return c.JSON(http.StatusOK, &ListFunctionURLConfigsOutput{
			FunctionURLConfigs: []*FunctionURLConfig{cfg},
		})
	}

	cfgs := lambdaBk.ListFunctionURLConfigs()

	return c.JSON(http.StatusOK, &ListFunctionURLConfigsOutput{FunctionURLConfigs: cfgs})
}

// handleUpdateFunctionURLConfig handles PUT /2021-10-31/functions/{name}/url.
func (h *Handler) handleUpdateFunctionURLConfig(c *echo.Context, name string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "failed to read body")
	}

	var input UpdateFunctionURLConfigInput
	if len(body) > 0 {
		if unmarshalErr := json.Unmarshal(body, &input); unmarshalErr != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid JSON")
		}
	}

	cfg, updateErr := lambdaBk.UpdateFunctionURLConfig(name, input.AuthType, input.Cors)
	if updateErr != nil {
		if errors.Is(updateErr, ErrFunctionURLNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Function URL config not found: "+name)
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", updateErr.Error())
	}

	return c.JSON(http.StatusOK, cfg)
}

// handleRuntimeMgmtRoute handles /2021-07-20/functions/{name}/runtime-management-config routes.
//
//nolint:dupl // similar get/put pattern shared with handleRecursionConfigRoute by design
func (h *Handler) handleRuntimeMgmtRoute(c *echo.Context, path, method string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	rest, found := strings.CutPrefix(path, lambda2021RuntimeMgmtPathPrefix)
	if !found {
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}

	rest = strings.TrimPrefix(rest, "/")
	parts := strings.SplitN(rest, "/", 2) //nolint:mnd // split name + suffix

	if len(parts) < 2 || parts[1] != "runtime-management-config" {
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}

	name := parts[0]

	switch method {
	case http.MethodGet:
		cfg, err := lambdaBk.GetRuntimeManagementConfig(name)
		if err != nil {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Function not found: "+name)
		}

		return c.JSON(http.StatusOK, cfg)
	case http.MethodPut:
		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "failed to read body")
		}

		var input PutRuntimeManagementConfigInput
		if len(body) > 0 {
			if unmarshalErr := json.Unmarshal(body, &input); unmarshalErr != nil {
				return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid JSON")
			}
		}

		cfg, putErr := lambdaBk.PutRuntimeManagementConfig(name, &input)
		if putErr != nil {
			if errors.Is(putErr, ErrFunctionNotFound) {
				return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
					"Function not found: "+name)
			}

			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", putErr.Error())
		}

		return c.JSON(http.StatusOK, cfg)
	default:
		return h.writeError(c, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
	}
}

// handleRecursionConfigRoute handles /2024-08-28/functions/{name}/recursion-config routes.
//
//nolint:dupl // similar get/put pattern shared with handleRuntimeMgmtRoute and handleScalingConfigRoute by design
func (h *Handler) handleRecursionConfigRoute(c *echo.Context, path, method string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	rest, found := strings.CutPrefix(path, lambda2024RecursionPathPrefix)
	if !found {
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}

	rest = strings.TrimPrefix(rest, "/")
	parts := strings.SplitN(rest, "/", 2) //nolint:mnd // split name + suffix

	if len(parts) < 2 || parts[1] != "recursion-config" {
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}

	name := parts[0]

	switch method {
	case http.MethodGet:
		cfg, err := lambdaBk.GetFunctionRecursionConfig(name)
		if err != nil {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Function not found: "+name)
		}

		return c.JSON(http.StatusOK, cfg)
	case http.MethodPut:
		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "failed to read body")
		}

		var input PutFunctionRecursionConfigInput
		if len(body) > 0 {
			if unmarshalErr := json.Unmarshal(body, &input); unmarshalErr != nil {
				return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid JSON")
			}
		}

		cfg, putErr := lambdaBk.PutFunctionRecursionConfig(name, &input)
		if putErr != nil {
			if errors.Is(putErr, ErrFunctionNotFound) {
				return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
					"Function not found: "+name)
			}

			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", putErr.Error())
		}

		return c.JSON(http.StatusOK, cfg)
	default:
		return h.writeError(c, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
	}
}

// handleScalingConfigRoute handles /2023-10-26/functions/{name}/scaling-config routes.
//
//nolint:dupl // similar get/put pattern shared with handleRuntimeMgmtRoute by design
func (h *Handler) handleScalingConfigRoute(c *echo.Context, path, method string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	rest, found := strings.CutPrefix(path, lambda2023ScalingPathPrefix)
	if !found {
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}

	rest = strings.TrimPrefix(rest, "/")
	parts := strings.SplitN(rest, "/", 2) //nolint:mnd // split name + suffix

	if len(parts) < 2 || parts[1] != "scaling-config" {
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}

	name := parts[0]

	switch method {
	case http.MethodGet:
		cfg, err := lambdaBk.GetFunctionScalingConfig(name)
		if err != nil {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Function not found: "+name)
		}

		return c.JSON(http.StatusOK, cfg)
	case http.MethodPut:
		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "failed to read body")
		}

		var input PutFunctionScalingConfigInput
		if len(body) > 0 {
			if unmarshalErr := json.Unmarshal(body, &input); unmarshalErr != nil {
				return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid JSON")
			}
		}

		cfg, putErr := lambdaBk.PutFunctionScalingConfig(name, &input)
		if putErr != nil {
			if errors.Is(putErr, ErrFunctionNotFound) {
				return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
					"Function not found: "+name)
			}

			return h.writeError(c, http.StatusInternalServerError, "ServiceException", putErr.Error())
		}

		return c.JSON(http.StatusOK, cfg)
	default:
		return h.writeError(c, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
	}
}

// handleGetLayerVersionByArn handles GET /2018-10-31/layers-by-arn?Arn={arn}.
func (h *Handler) handleGetLayerVersionByArn(c *echo.Context) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	arn := c.Request().URL.Query().Get("Arn")
	if arn == "" {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValueException",
			"Arn query parameter is required",
		)
	}

	out, err := lambdaBk.GetLayerVersionByArn(arn)
	if err != nil {
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
			"Layer version not found: "+arn)
	}

	return c.JSON(http.StatusOK, out)
}
