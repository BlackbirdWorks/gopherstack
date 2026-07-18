package mwaa

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/labstack/echo/v5"
)

const (
	mwaaService       = "airflow"
	mwaaMatchPriority = 87
	opUnknown         = "Unknown"
	keyArn            = "Arn"

	// Path prefix constants.
	pathEnvironments   = "/environments"
	pathTagsPrefix     = "/tags/"
	pathCliTokenPrefix = "/clitoken/"
	pathWebTokenPrefix = "/webtoken/"
	pathRestAPIPrefix  = "/restapi/"
	pathMetricsPrefix  = "/metrics/environments/"
)

// Handler is the HTTP handler for the AWS MWAA REST API.
type Handler struct {
	Backend       StorageBackend
	AccountID     string
	DefaultRegion string
}

// NewHandler creates a new MWAA handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{Backend: backend}
}

// Reset resets the handler's backend state.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// Name returns the service name.
func (h *Handler) Name() string { return "MWAA" }

// GetSupportedOperations returns the list of supported MWAA operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateEnvironment",
		"GetEnvironment",
		"DeleteEnvironment",
		"UpdateEnvironment",
		"ListEnvironments",
		"ListTagsForResource",
		"TagResource",
		"UntagResource",
		"CreateCliToken",
		"CreateWebLoginToken",
		"InvokeRestApi",
		"PublishMetrics",
		"GetMetrics",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return mwaaService }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler handles.
func (h *Handler) ChaosRegions() []string { return []string{h.DefaultRegion} }

// RouteMatcher returns a function that matches MWAA API requests.
// All path-based matches are gated on the SigV4 service name to prevent
// routing conflicts with other services that share similar REST paths.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		if httputils.ExtractServiceFromRequest(c.Request()) != mwaaService {
			return false
		}

		path := c.Request().URL.Path

		mwaaPathPrefixes := []string{
			pathEnvironments, pathTagsPrefix, pathCliTokenPrefix,
			pathWebTokenPrefix, pathRestAPIPrefix, pathMetricsPrefix,
		}
		for _, prefix := range mwaaPathPrefixes {
			if strings.HasPrefix(path, prefix) {
				return true
			}
		}

		return false
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return mwaaMatchPriority }

// ExtractOperation extracts the operation name from the request path and method.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	method := c.Request().Method
	path := c.Request().URL.Path

	switch {
	case strings.HasPrefix(path, pathCliTokenPrefix):
		return "CreateCliToken"
	case strings.HasPrefix(path, pathWebTokenPrefix):
		return "CreateWebLoginToken"
	case strings.HasPrefix(path, pathRestAPIPrefix):
		return "InvokeRestApi"
	case strings.HasPrefix(path, pathMetricsPrefix):
		return extractMetricsOperation(method)
	case strings.HasPrefix(path, pathTagsPrefix):
		return extractTagOperation(method)
	case path == pathEnvironments || path == pathEnvironments+"/":
		return extractEnvironmentListOperation(method)
	case strings.HasPrefix(path, pathEnvironments+"/"):
		return extractEnvironmentOperation(method)
	}

	return opUnknown
}

// extractMetricsOperation returns the operation name for a /metrics/environments/ path.
func extractMetricsOperation(method string) string {
	switch method {
	case http.MethodPost:
		return "PublishMetrics"
	case http.MethodGet:
		return "GetMetrics"
	}

	return opUnknown
}

// extractTagOperation returns the operation name for a /tags/ path.
func extractTagOperation(method string) string {
	switch method {
	case http.MethodGet:
		return "ListTagsForResource"
	case http.MethodPost:
		return "TagResource"
	case http.MethodDelete:
		return "UntagResource"
	}

	return opUnknown
}

// extractEnvironmentListOperation returns the operation name for the /environments list path.
func extractEnvironmentListOperation(method string) string {
	if method == http.MethodGet {
		return "ListEnvironments"
	}

	return opUnknown
}

// extractEnvironmentOperation returns the operation name for a /environments/{name} path.
func extractEnvironmentOperation(method string) string {
	switch method {
	case http.MethodGet:
		return "GetEnvironment"
	case http.MethodPut:
		return "CreateEnvironment"
	case http.MethodDelete:
		return "DeleteEnvironment"
	case http.MethodPatch:
		return "UpdateEnvironment"
	}

	return opUnknown
}

// ExtractResource extracts the environment name or ARN from the request path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	path := c.Request().URL.Path

	switch {
	case strings.HasPrefix(path, pathEnvironments+"/"):
		return strings.TrimPrefix(path, pathEnvironments+"/")
	case strings.HasPrefix(path, pathCliTokenPrefix):
		return strings.TrimPrefix(path, pathCliTokenPrefix)
	case strings.HasPrefix(path, pathWebTokenPrefix):
		return strings.TrimPrefix(path, pathWebTokenPrefix)
	case strings.HasPrefix(path, pathRestAPIPrefix):
		return strings.TrimPrefix(path, pathRestAPIPrefix)
	case strings.HasPrefix(path, pathMetricsPrefix):
		return strings.TrimPrefix(path, pathMetricsPrefix)
	case strings.HasPrefix(path, pathTagsPrefix):
		return strings.TrimPrefix(path, pathTagsPrefix)
	}

	return ""
}

// Handler returns the echo.HandlerFunc for this service.
func (h *Handler) Handler() echo.HandlerFunc {
	return h.ServeHTTP
}

// contextWithRegion returns the request context with the resolved AWS region attached
// under regionContextKey so that backend operations are routed to the correct region.
// The region is extracted from the request's SigV4 credential scope, falling back to
// the handler's default region.
func (h *Handler) contextWithRegion(c *echo.Context) context.Context {
	region := httputils.ExtractRegionFromRequest(c.Request(), h.DefaultRegion)

	return context.WithValue(c.Request().Context(), regionContextKey{}, region)
}

// ServeHTTP dispatches MWAA API requests.
func (h *Handler) ServeHTTP(c *echo.Context) error {
	path := c.Request().URL.Path

	switch {
	case strings.HasPrefix(path, pathCliTokenPrefix):
		return h.dispatchCliToken(c, path)
	case strings.HasPrefix(path, pathWebTokenPrefix):
		return h.dispatchWebToken(c, path)
	case strings.HasPrefix(path, pathRestAPIPrefix):
		return h.dispatchRestAPI(c, path)
	case strings.HasPrefix(path, pathMetricsPrefix):
		return h.dispatchMetrics(c, path)
	case strings.HasPrefix(path, pathTagsPrefix):
		return h.dispatchTags(c, path)
	case path == pathEnvironments || path == pathEnvironments+"/":
		return h.dispatchEnvironmentList(c)
	case strings.HasPrefix(path, pathEnvironments+"/"):
		return h.dispatchEnvironment(c, path)
	}

	ctx := c.Request().Context()
	log := logger.Load(ctx)
	log.WarnContext(ctx, "mwaa: unhandled request", "method", c.Request().Method, "path", path)

	return writeErrorResponse(c, http.StatusNotFound, "ResourceNotFoundException", "resource not found")
}

func (h *Handler) dispatchCliToken(c *echo.Context, path string) error {
	name := strings.TrimPrefix(path, pathCliTokenPrefix)
	if c.Request().Method == http.MethodPost {
		return h.handleCreateCliToken(c, name)
	}

	return writeErrorResponse(c, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
}

func (h *Handler) dispatchWebToken(c *echo.Context, path string) error {
	name := strings.TrimPrefix(path, pathWebTokenPrefix)
	if c.Request().Method == http.MethodPost {
		return h.handleCreateWebLoginToken(c, name)
	}

	return writeErrorResponse(c, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
}

func (h *Handler) dispatchTags(c *echo.Context, path string) error {
	resourceARN := strings.TrimPrefix(path, pathTagsPrefix)

	switch c.Request().Method {
	case http.MethodGet:
		return h.handleListTagsForResource(c, resourceARN)
	case http.MethodPost:
		return h.handleTagResource(c, resourceARN)
	case http.MethodDelete:
		return h.handleUntagResource(c, resourceARN)
	}

	return writeErrorResponse(c, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
}

func (h *Handler) dispatchEnvironmentList(c *echo.Context) error {
	if c.Request().Method == http.MethodGet {
		return h.handleListEnvironments(c)
	}

	return writeErrorResponse(c, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
}

func (h *Handler) dispatchEnvironment(c *echo.Context, path string) error {
	name := strings.TrimPrefix(path, pathEnvironments+"/")

	switch c.Request().Method {
	case http.MethodGet:
		return h.handleGetEnvironment(c, name)
	case http.MethodPut:
		return h.handleCreateEnvironment(c, name)
	case http.MethodDelete:
		return h.handleDeleteEnvironment(c, name)
	case http.MethodPatch:
		return h.handleUpdateEnvironment(c, name)
	}

	return writeErrorResponse(c, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
}

// decodeJSONBody reads the request body and unmarshals it into target. On
// failure it writes the appropriate MWAA error response and returns false so
// the caller can return immediately.
func decodeJSONBody(c *echo.Context, target any) bool {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		_ = writeErrorResponse(c, http.StatusBadRequest, "ValidationException", "failed to read request body")

		return false
	}

	if jsonErr := json.Unmarshal(body, target); jsonErr != nil {
		_ = writeErrorResponse(c, http.StatusBadRequest, "ValidationException", "invalid request body")

		return false
	}

	return true
}

// writeEnvironmentResult maps a backend environment error to an MWAA error
// response, or writes the environment ARN on success. MWAA has no
// AlreadyExistsException in its API model at all (CreateEnvironment's only
// documented errors are InternalServerException, ServiceUnavailableException,
// and ValidationException), so a duplicate-name create is surfaced the same
// way AWS does: a 400 ValidationException, not a fabricated 409 Conflict.
func writeEnvironmentResult(c *echo.Context, env *Environment, err error) error {
	if err != nil {
		switch {
		case errors.Is(err, awserr.ErrAlreadyExists):
			return writeErrorResponse(c, http.StatusBadRequest, "ValidationException", err.Error())
		case errors.Is(err, awserr.ErrNotFound):
			return writeErrorResponse(c, http.StatusNotFound, "ResourceNotFoundException", err.Error())
		case errors.Is(err, awserr.ErrInvalidParameter):
			return writeErrorResponse(c, http.StatusBadRequest, "ValidationException", err.Error())
		default:
			return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerException", err.Error())
		}
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, map[string]string{
		keyArn: env.ARN,
	})

	return nil
}

// writeEnvironmentVoidResult maps a backend environment error to an MWAA error
// response, or writes an empty success body. DeleteEnvironment's response
// shape (unlike Create/Update) carries no members at all -- AWS returns HTTP
// 200 with an empty body, so the ARN must not be echoed back here.
func writeEnvironmentVoidResult(c *echo.Context, _ *Environment, err error) error {
	if err != nil {
		switch {
		case errors.Is(err, awserr.ErrNotFound):
			return writeErrorResponse(c, http.StatusNotFound, "ResourceNotFoundException", err.Error())
		case errors.Is(err, awserr.ErrInvalidParameter):
			return writeErrorResponse(c, http.StatusBadRequest, "ValidationException", err.Error())
		default:
			return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerException", err.Error())
		}
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, map[string]any{})

	return nil
}

func (h *Handler) dispatchRestAPI(c *echo.Context, path string) error {
	name := strings.TrimPrefix(path, pathRestAPIPrefix)
	if c.Request().Method == http.MethodPost {
		return h.handleInvokeRestAPI(c, name)
	}

	return writeErrorResponse(c, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
}

func (h *Handler) dispatchMetrics(c *echo.Context, path string) error {
	name := strings.TrimPrefix(path, pathMetricsPrefix)

	switch c.Request().Method {
	case http.MethodPost:
		return h.handlePublishMetrics(c, name)
	case http.MethodGet:
		return h.handleGetMetrics(c, name)
	}

	return writeErrorResponse(c, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
}

// writeErrorResponse writes a JSON error response in the MWAA REST API format.
func writeErrorResponse(c *echo.Context, statusCode int, errorType, message string) error {
	httputils.WriteJSON(c.Request().Context(), c.Response(), statusCode, map[string]string{
		"message": message,
		"__type":  errorType,
	})

	return nil
}
