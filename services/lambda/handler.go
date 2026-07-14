package lambda

import (
	"context"
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
	"github.com/labstack/echo/v5"
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

// MatchPriority returns the routing priority for the Lambda handler.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderPartial }

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
