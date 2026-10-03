package kafkaconnect

import (
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	kafkaConnectService       = "kafkaconnect"
	kafkaConnectMatchPriority = service.PriorityPathVersioned
)

// Operation names, matching the AWS API exactly.
const (
	opCreateConnector            = "CreateConnector"
	opDescribeConnector          = "DescribeConnector"
	opListConnectors             = "ListConnectors"
	opUpdateConnector            = "UpdateConnector"
	opDeleteConnector            = "DeleteConnector"
	opRestartConnector           = "RestartConnector"
	opDescribeConnectorOperation = "DescribeConnectorOperation"
	opListConnectorOperations    = "ListConnectorOperations"

	opCreateCustomPlugin   = "CreateCustomPlugin"
	opDescribeCustomPlugin = "DescribeCustomPlugin"
	opListCustomPlugins    = "ListCustomPlugins"
	opDeleteCustomPlugin   = "DeleteCustomPlugin"

	opCreateWorkerConfiguration   = "CreateWorkerConfiguration"
	opDescribeWorkerConfiguration = "DescribeWorkerConfiguration"
	opListWorkerConfigurations    = "ListWorkerConfigurations"
	opDeleteWorkerConfiguration   = "DeleteWorkerConfiguration"

	opTagResource         = "TagResource"
	opUntagResource       = "UntagResource"
	opListTagsForResource = "ListTagsForResource"
)

// opFunc is the uniform signature for all dispatch operations; resource is
// the ARN parsed from the path (empty when the operation has none), and body
// is the raw JSON request body (empty for bodyless GET/DELETE requests).
type opFunc func(c *echo.Context, resource string, body []byte) error

// Handler is the HTTP handler for the MSK Connect REST API.
type Handler struct {
	Backend       StorageBackend
	ops           map[string]opFunc
	AccountID     string
	DefaultRegion string
}

// NewHandler creates a new MSK Connect handler.
func NewHandler(backend StorageBackend) *Handler {
	h := &Handler{Backend: backend}
	h.ops = h.buildOps()

	return h
}

func (h *Handler) buildOps() map[string]opFunc {
	ops := make(map[string]opFunc, len(h.GetSupportedOperations()))

	maps.Copy(ops, h.buildConnectorOps())
	maps.Copy(ops, h.buildCustomPluginOps())
	maps.Copy(ops, h.buildWorkerConfigurationOps())
	maps.Copy(ops, h.buildTagOps())

	return ops
}

// Reset clears all backend state.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// Name returns the service name.
func (h *Handler) Name() string { return "KafkaConnect" }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		opCreateConnector,
		opDescribeConnector,
		opListConnectors,
		opUpdateConnector,
		opDeleteConnector,
		opRestartConnector,
		opDescribeConnectorOperation,
		opListConnectorOperations,
		opCreateCustomPlugin,
		opDescribeCustomPlugin,
		opListCustomPlugins,
		opDeleteCustomPlugin,
		opCreateWorkerConfiguration,
		opDescribeWorkerConfiguration,
		opListWorkerConfigurations,
		opDeleteWorkerConfiguration,
		opTagResource,
		opUntagResource,
		opListTagsForResource,
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return kafkaConnectService }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler handles.
func (h *Handler) ChaosRegions() []string { return []string{h.DefaultRegion} }

// RouteMatcher returns a function that matches MSK Connect REST API requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path

		switch {
		case strings.HasPrefix(path, connectorsPath):
			return true
		case strings.HasPrefix(path, connectorOperationsPath):
			return true
		case strings.HasPrefix(path, customPluginsPath):
			return true
		case strings.HasPrefix(path, workerConfigurationsPath):
			return true
		case strings.HasPrefix(path, tagsPrefix):
			return isKafkaConnectTagsPath(path)
		}

		return false
	}
}

// isKafkaConnectTagsPath reports whether path is a /v1/tags/{arn} path for a
// kafkaconnect ARN. Several restjson1 services (kafka, batch, appsync, ...)
// claim this same "/v1/tags/{arn}" prefix, so this is guarded by the ARN's
// own service field rather than claimed unconditionally, per
// .claude/memories -- route-matcher-prefix-collision.
func isKafkaConnectTagsPath(path string) bool {
	encodedARN := strings.TrimPrefix(path, tagsPrefix)
	if encodedARN == "" {
		return false
	}

	decodedARN, err := decodeResourceARN(encodedARN)
	if err != nil {
		return false
	}

	parts := strings.SplitN(decodedARN, ":", arnMaxParts)

	return len(parts) >= arnServiceFieldIndex+1 && parts[arnServiceFieldIndex] == kafkaConnectService
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return kafkaConnectMatchPriority }

// ExtractOperation extracts the MSK Connect operation name from the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	op, _ := parseKafkaConnectPath(c.Request().Method, c.Request().URL.Path)

	return op
}

// ExtractResource extracts the resource ARN from the request path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	_, resource := parseKafkaConnectPath(c.Request().Method, c.Request().URL.Path)

	return resource
}

// Handler returns the Echo handler function for MSK Connect requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		op, resource := parseKafkaConnectPath(c.Request().Method, c.Request().URL.Path)
		if op == "" {
			return h.writeError(c, http.StatusBadRequest, "BadRequestException", "unknown operation")
		}

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "kafkaconnect: failed to read request body", "error", err)

			return h.writeError(
				c,
				http.StatusInternalServerError,
				"InternalServerErrorException",
				"failed to read request body",
			)
		}

		log.DebugContext(ctx, "kafkaconnect request", "op", op, "resource", resource)

		return h.dispatch(c, op, resource, body)
	}
}

func (h *Handler) dispatch(c *echo.Context, op, resource string, body []byte) error {
	fn, ok := h.ops[op]
	if !ok {
		return h.writeError(c, http.StatusBadRequest, "BadRequestException", "unknown operation")
	}

	return fn(c, resource, body)
}

func decodeBody[T any](body []byte, out *T) error {
	if len(body) == 0 {
		return nil
	}

	return json.Unmarshal(body, out)
}

// writeJSON writes a 200 JSON response.
func (h *Handler) writeJSON(c *echo.Context, v any) error {
	return c.JSON(http.StatusOK, v)
}

// writeError writes a kafkaconnect JSON error response with the AWS __type field.
func (h *Handler) writeError(c *echo.Context, status int, errType, message string) error {
	return c.JSON(status, errorResponse{Type: errType, Message: message})
}

// writeBackendError maps a backend error to an HTTP error response with the appropriate AWS error type.
func (h *Handler) writeBackendError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, awserr.ErrNotFound):
		return h.writeError(c, http.StatusNotFound, "NotFoundException", err.Error())
	case errors.Is(err, awserr.ErrAlreadyExists), errors.Is(err, awserr.ErrConflict):
		return h.writeError(c, http.StatusConflict, "ConflictException", err.Error())
	case errors.Is(err, awserr.ErrInvalidParameter):
		return h.writeError(c, http.StatusBadRequest, "BadRequestException", err.Error())
	default:
		return h.writeError(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}
}
