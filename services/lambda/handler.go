package lambda

import (
	"context"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

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
		opDeleteFunctionCodeSigningConfig,
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
		opGetFunctionCodeSigningConfig,
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
		opPutFunctionCodeSigningConfig,
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
		// Invoke (SDK op name) + async/streaming variants.
		opInvoke,
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

// MatchPriority returns the routing priority for the Lambda handler.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderPartial }

// ExtractOperation returns the Lambda operation name derived from the request
// method and path. It mirrors dispatchSpecialRoutes/lambdaOpRoutes/
// layerOpTable's real dispatch tree op-for-op (gopherstack-l5ir) so that
// TestExtractOperation_SDKRouteTable in handler_paths_sdk_diff_test.go can
// exercise it directly against every real op's authoritative method+path.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	path := c.Request().URL.Path
	method := c.Request().Method

	// Identify layer operations first (different path prefix).
	if after, ok := strings.CutPrefix(path, lambdaLayersPathPrefix); ok {
		rest := strings.TrimPrefix(after, "/")
		if rest == "" && method == http.MethodGet &&
			c.Request().URL.Query().Get(lambdaFindQueryParam) == lambdaFindLayerVersion {
			return "GetLayerVersionByArn"
		}

		if op := extractLayerOperation(rest, method); op != "" {
			return op
		}
	}

	// Durable-execution family spans three independent path prefixes that
	// normalizeFunctionPath/lambdaOpRoutes below don't cover — see handler_paths.go.
	if op := extractDurableExecOperation(path, method); op != "" {
		return op
	}

	if op := extractSpecialFamilyOperation(path, method); op != "" {
		return op
	}

	rest := normalizeFunctionPath(path)

	// Special case: GET /provisioned-concurrency dispatches to Get vs List based on Qualifier.
	if op := resolveProvisionedConcurrencyOp(method, rest, c.Request().URL.Query().Get("Qualifier")); op != "" {
		return op
	}

	// Special case: the real SDK operation name is opInvoke (lambdaOpRoutes also
	// carries an "InvokeFunction" entry for the same path -- that is the real
	// IAM *action* name, a documented AWS naming quirk where the IAM action
	// differs from the API operation name; IAMAction relies on it via the
	// shared table below, but ExtractOperation must report the real op name).
	if method == http.MethodPost && hasSuffixInvocations(rest) {
		return opInvoke
	}

	for _, route := range lambdaOpRoutes {
		if route.method == method && route.match(rest) {
			return route.op
		}
	}

	return "Unknown"
}

// extractSpecialFamilyOperation covers every non-function-path family
// dispatchSpecialRoutes routes: ESM, tags, account settings, code signing,
// capacity providers, function URLs, runtime management, recursion config,
// scaling config, and function-scoped code-signing-config, plus aliases/
// versions (handled through buildVersionAliasRoutes, not dispatchSpecialRoutes,
// but likewise outside lambdaOpRoutes' scope). Returns "" when path doesn't
// belong to any of these families.
func extractSpecialFamilyOperation(path, method string) string {
	for _, fn := range []func(string, string) string{
		extractESMOp,
		extractTagsOp,
		extractCodeSigningOp,
		extractCapacityProviderOp,
		extractFunctionURLOp,
		extractRuntimeMgmtOp,
		extractRecursionConfigOp,
		extractScalingConfigOp,
		extractFunctionCodeSigningConfigOp,
		extractAliasVersionOp,
	} {
		if op := fn(path, method); op != "" {
			return op
		}
	}

	if path == lambdaAccountSettingsPath && method == http.MethodGet {
		return "GetAccountSettings"
	}

	return ""
}

// extractESMOp handles the event-source-mapping family.
func extractESMOp(path, method string) string {
	rest, ok := strings.CutPrefix(path, esmPathPrefix)
	if !ok {
		return ""
	}

	rest = strings.TrimPrefix(rest, "/")

	switch {
	case method == http.MethodPost && rest == "":
		return "CreateEventSourceMapping"
	case method == http.MethodGet && rest == "":
		return "ListEventSourceMappings"
	case method == http.MethodGet && rest != "":
		return "GetEventSourceMapping"
	case method == http.MethodPut && rest != "":
		return "UpdateEventSourceMapping"
	case method == http.MethodDelete && rest != "":
		return "DeleteEventSourceMapping"
	}

	return ""
}

// extractTagsOp handles the ListTags/TagResource/UntagResource trio, all
// sharing /2017-03-31/tags/{Resource}, disambiguated by method.
func extractTagsOp(path, method string) string {
	if !strings.HasPrefix(path, lambdaTagsPathPrefix+"/") {
		return ""
	}

	switch method {
	case http.MethodGet:
		return "ListTags"
	case http.MethodPost:
		return "TagResource"
	case http.MethodDelete:
		return "UntagResource"
	}

	return ""
}

// extractCodeSigningOp handles the CodeSigningConfig resource family.
func extractCodeSigningOp(path, method string) string {
	rest, ok := strings.CutPrefix(path, lambdaCodeSigningPathPrefix)
	if !ok {
		return ""
	}

	rest = strings.TrimPrefix(rest, "/")

	if rest == "" {
		return extractCodeSigningRootOp(method)
	}

	if strings.HasSuffix(rest, "/functions") && method == http.MethodGet {
		return "ListFunctionsByCodeSigningConfig"
	}

	if strings.Contains(rest, "/") {
		return ""
	}

	return extractCodeSigningItemOp(method)
}

func extractCodeSigningRootOp(method string) string {
	switch method {
	case http.MethodPost:
		return "CreateCodeSigningConfig"
	case http.MethodGet:
		return "ListCodeSigningConfigs"
	}

	return ""
}

func extractCodeSigningItemOp(method string) string {
	switch method {
	case http.MethodGet:
		return "GetCodeSigningConfig"
	case http.MethodDelete:
		return "DeleteCodeSigningConfig"
	case http.MethodPut:
		return "UpdateCodeSigningConfig"
	}

	return ""
}

// extractCapacityProviderOp handles the CapacityProvider resource family.
func extractCapacityProviderOp(path, method string) string {
	rest, ok := strings.CutPrefix(path, lambdaCapacityPathPrefix)
	if !ok {
		return ""
	}

	rest = strings.TrimPrefix(rest, "/")

	if rest == "" {
		return extractCapacityProviderRootOp(method)
	}

	if strings.HasSuffix(rest, "/function-versions") && method == http.MethodGet {
		return "ListFunctionVersionsByCapacityProvider"
	}

	if strings.Contains(rest, "/") {
		return ""
	}

	return extractCapacityProviderItemOp(method)
}

func extractCapacityProviderRootOp(method string) string {
	switch method {
	case http.MethodPost:
		return "CreateCapacityProvider"
	case http.MethodGet:
		return "ListCapacityProviders"
	}

	return ""
}

func extractCapacityProviderItemOp(method string) string {
	switch method {
	case http.MethodGet:
		return "GetCapacityProvider"
	case http.MethodDelete:
		return "DeleteCapacityProvider"
	case http.MethodPut:
		return "UpdateCapacityProvider"
	}

	return ""
}

// extractFunctionURLOp handles the 2021-10-31 FunctionUrlConfig family.
func extractFunctionURLOp(path, method string) string {
	rest, ok := strings.CutPrefix(path, lambda2021PathPrefix)
	if !ok {
		return ""
	}

	switch {
	case strings.HasSuffix(rest, "/urls") && method == http.MethodGet:
		return "ListFunctionUrlConfigs"
	case strings.HasSuffix(rest, "/url") && method == http.MethodPost:
		return "CreateFunctionUrlConfig"
	case strings.HasSuffix(rest, "/url") && method == http.MethodGet:
		return "GetFunctionUrlConfig"
	case strings.HasSuffix(rest, "/url") && method == http.MethodDelete:
		return "DeleteFunctionUrlConfig"
	case strings.HasSuffix(rest, "/url") && method == http.MethodPut:
		return "UpdateFunctionUrlConfig"
	}

	return ""
}

// extractRuntimeMgmtOp handles RuntimeManagementConfig.
func extractRuntimeMgmtOp(path, method string) string {
	hasPrefix := strings.HasPrefix(path, lambda2021RuntimeMgmtPathPrefix)
	hasSuffix := strings.HasSuffix(path, "/runtime-management-config")

	if !hasPrefix || !hasSuffix {
		return ""
	}

	switch method {
	case http.MethodGet:
		return "GetRuntimeManagementConfig"
	case http.MethodPut:
		return "PutRuntimeManagementConfig"
	}

	return ""
}

// extractRecursionConfigOp handles FunctionRecursionConfig.
func extractRecursionConfigOp(path, method string) string {
	if !strings.HasPrefix(path, lambda2024RecursionPathPrefix) || !strings.HasSuffix(path, "/recursion-config") {
		return ""
	}

	switch method {
	case http.MethodGet:
		return "GetFunctionRecursionConfig"
	case http.MethodPut:
		return "PutFunctionRecursionConfig"
	}

	return ""
}

// extractScalingConfigOp handles FunctionScalingConfig.
func extractScalingConfigOp(path, method string) string {
	if !strings.HasPrefix(path, lambda2025ScalingPathPrefix) || !strings.HasSuffix(path, "/function-scaling-config") {
		return ""
	}

	switch method {
	case http.MethodGet:
		return "GetFunctionScalingConfig"
	case http.MethodPut:
		return "PutFunctionScalingConfig"
	}

	return ""
}

// extractFunctionCodeSigningConfigOp handles the 2020-06-30
// function-scoped code-signing-config sub-resource (distinct from the
// 2020-04-22 CodeSigningConfig resource family above).
func extractFunctionCodeSigningConfigOp(path, method string) string {
	rest, ok := strings.CutPrefix(path, lambda2020PathPrefix)
	if !ok || !hasSuffixCodeSigningConfig(rest) {
		return ""
	}

	switch method {
	case http.MethodGet:
		return opGetFunctionCodeSigningConfig
	case http.MethodPut:
		return opPutFunctionCodeSigningConfig
	case http.MethodDelete:
		return opDeleteFunctionCodeSigningConfig
	}

	return ""
}

// extractAliasVersionOp handles the versions/aliases family under the
// 2015-03-31 functions prefix.
func extractAliasVersionOp(path, method string) string {
	rest, ok := strings.CutPrefix(path, lambdaPathPrefix)
	if !ok {
		return ""
	}

	switch {
	case hasSuffixVersions(rest) && method == http.MethodPost:
		return "PublishVersion"
	case hasSuffixVersions(rest) && method == http.MethodGet:
		return "ListVersionsByFunction"
	case hasSuffixAliasPath(rest):
		return extractAliasOp(rest, method)
	}

	return ""
}

// extractAliasOp disambiguates the alias sub-family: POST always creates;
// GET is Get-by-name when an alias name segment follows, List otherwise.
func extractAliasOp(rest, method string) string {
	switch method {
	case http.MethodPost:
		return "CreateAlias"
	case http.MethodGet:
		_, aliasName := extractNameAndAlias(rest)
		if aliasName != "" {
			return "GetAlias"
		}

		return "ListAliases"
	case http.MethodPut:
		return "UpdateAlias"
	case http.MethodDelete:
		return "DeleteAlias"
	}

	return ""
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

// writeError writes a Lambda-formatted JSON error response.
func (h *Handler) writeError(c *echo.Context, status int, errType, message string) error {
	c.Response().Header().Set("X-Amzn-Errortype", errType)

	return c.JSON(status, &Error{
		Type:    errType,
		Message: message,
	})
}

// checkRevisionID enforces the optimistic-concurrency RevisionId check used by
// UpdateFunctionCode and UpdateFunctionConfiguration (the paths that fetch a
// resource, mutate it in place, then call Backend.UpdateFunction — see
// handler_functions.go): when the caller supplies a RevisionId it must match
// the resource's current one, or the request fails with
// PreconditionFailedException without mutating anything. An empty
// providedRevisionID (the common case — most callers never pass one) always
// passes.
//
// Returns true when the check passes and the caller should continue; false
// when it wrote a PreconditionFailedException response and the caller must
// stop and return nil immediately, matching validateMemoryAndTimeout's and
// applyFunctionCodeUpdate's bool-return convention. This is deliberately NOT
// modeled as "return writeError's own error return value" — c.JSON (and so
// writeError) returns nil on any successful write, error or not, so a
// `!= nil` check on that return value can never detect a written error
// response and would silently fall through to a second, conflicting write
// (exactly the bug this function and applyFunctionCodeUpdate both avoid).
func (h *Handler) checkRevisionID(c *echo.Context, currentRevisionID, providedRevisionID string) bool {
	if providedRevisionID == "" || providedRevisionID == currentRevisionID {
		return true
	}

	_ = h.writeError(c, http.StatusPreconditionFailed, "PreconditionFailedException",
		"The RevisionId provided does not match the latest RevisionId. Fetch the latest version "+
			"and try again.")

	return false
}
