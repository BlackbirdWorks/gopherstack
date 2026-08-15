package lambda

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

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
	{http.MethodDelete, hasPolicyStatementSuffix, "RemovePermission"},
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
	{http.MethodGet, hasSuffixCodeSigningConfig, opGetFunctionCodeSigningConfig},
	{http.MethodPut, hasSuffixCodeSigningConfig, opPutFunctionCodeSigningConfig},
	{http.MethodDelete, hasSuffixCodeSigningConfig, opDeleteFunctionCodeSigningConfig},
	// Invoke (real SDK op name) alias route -- same endpoint as InvokeFunction above.
	{http.MethodPost, hasSuffixInvocations, opInvoke},
	// InvokeAsync: POST /2014-11-13/functions/{name}/invoke-async/
	{http.MethodPost, hasSuffixInvokeAsync, "InvokeAsync"},
	// InvokeWithResponseStream: POST /2021-11-15/functions/{name}/response-streaming-invocations
	{http.MethodPost, hasSuffixResponseStream, "InvokeWithResponseStream"},
}

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

// layerRootParts is the number of path parts for the root layers path (no segments after prefix).
const layerRootParts = 0

// layerVersionListParts is the number of path parts for /{layerName}/versions.
const layerVersionListParts = 2

// layerVersionItemParts is the number of path parts for /{layerName}/versions/{version}.
const layerVersionItemParts = 3

// layerPolicyParts is the number of path parts when the policy segment is present.
const layerPolicyParts = 4

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

	// lastSeg is the discriminating segment layerOpTable keys on: "versions"
	// itself for the /{layerName}/versions collection route (n==2, already
	// confirmed above), "policy" for the policy sub-routes (n>=layerPolicyParts,
	// parts[3]). For n==3 (a bare version-number route) there is no such
	// marker, so lastSeg stays empty -- gopherstack-l5ir: the n==2 case was
	// previously left empty too, so ListLayerVersions/PublishLayerVersion
	// never resolved (always "Unknown").
	lastSeg := ""

	switch {
	case n == layerVersionListParts:
		lastSeg = layerVersionsPath
	case n >= layerPolicyParts:
		lastSeg = parts[layerVersionItemParts]
	}

	return layerOpTable[layerOpKey{method: method, numParts: n, lastSeg: lastSeg}]
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
				name := strings.TrimSuffix(strings.TrimPrefix(rest, "/"), "/invoke-async")

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
			match:  hasPolicyStatementSuffix,
			execute: func(c *echo.Context, rest string) error {
				name, statementID := extractNameAndPolicyStatement(rest)

				return h.handleRemovePermission(c, name, statementID)
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
				name := strings.TrimSuffix(strings.TrimPrefix(rest, "/"), "/event-invoke-config/list")

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

// dispatchSpecialRoutes handles non-function-path routes. It returns (true, err) when it
// matched and dispatched the request, or (false, nil) when the caller should continue.
func (h *Handler) dispatchSpecialRoutes(c *echo.Context, path, method string) (bool, error) {
	switch {
	case strings.HasPrefix(path, esmPathPrefix):
		return true, h.handleESMRoute(c, path, method)
	case strings.HasPrefix(path, lambdaTagsPathPrefix):
		return true, h.handleTagsRoute(c, method)
	case strings.HasPrefix(path, lambdaLayersPathPrefix):
		return true, h.handleLayersRoute(c, path, method)
	case path == lambdaAccountSettingsPath:
		return true, h.handleGetAccountSettings(c)
	case strings.HasPrefix(path, lambdaCodeSigningPathPrefix):
		return true, h.handleCodeSigningRoute(c, path, method)
	case strings.HasPrefix(path, lambdaCapacityPathPrefix):
		return true, h.handleCapacityProviderRoute(c, path, method)
	case isDurableExecPath(path):
		return true, h.dispatchDurableExecRoutes(c, path, method)
	case strings.HasPrefix(path, lambda2021PathPrefix):
		return true, h.handleFunctionURLRoute2021(c, path, method)
	case strings.HasPrefix(path, lambda2021RuntimeMgmtPathPrefix):
		return true, h.handleRuntimeMgmtRoute(c, path, method)
	case strings.HasPrefix(path, lambda2024RecursionPathPrefix):
		return true, h.handleRecursionConfigRoute(c, path, method)
	case strings.HasPrefix(path, lambda2025ScalingPathPrefix):
		return true, h.handleScalingConfigRoute(c, path, method)
	}

	if rest2020, ok := strings.CutPrefix(path, lambda2020PathPrefix); ok {
		return true, h.handle2020FunctionRoute(c, rest2020, method)
	}

	return false, nil
}

// layerPathMaxParts is the maximum number of path segments to split when parsing a layer route.
// Format: layerName / "versions" / versionNumber / "policy" / statementId.
const layerPathMaxParts = 5
