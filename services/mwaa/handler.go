package mwaa

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
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
		_ = writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "failed to read request body")

		return false
	}

	if jsonErr := json.Unmarshal(body, target); jsonErr != nil {
		_ = writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "invalid request body")

		return false
	}

	return true
}

// writeEnvironmentResult maps a backend environment error to an MWAA error
// response, or writes the environment ARN on success. It mirrors AWS, treating
// ErrAlreadyExists as a 409 Conflict (only produced by CreateEnvironment).
func writeEnvironmentResult(c *echo.Context, env *Environment, err error) error {
	if err != nil {
		switch {
		case errors.Is(err, awserr.ErrAlreadyExists):
			return writeErrorResponse(c, http.StatusConflict, "AlreadyExistsException", err.Error())
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

func (h *Handler) handleCreateEnvironment(c *echo.Context, name string) error {
	var req createEnvironmentRequest
	if !decodeJSONBody(c, &req) {
		return nil
	}

	env, err := h.Backend.CreateEnvironment(h.contextWithRegion(c), name, &req)

	return writeEnvironmentResult(c, env, err)
}

func (h *Handler) handleGetEnvironment(c *echo.Context, name string) error {
	env, err := h.Backend.GetEnvironment(h.contextWithRegion(c), name)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "ResourceNotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerException", err.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, map[string]any{
		"Environment": env,
	})

	return nil
}

func (h *Handler) handleDeleteEnvironment(c *echo.Context, name string) error {
	env, err := h.Backend.DeleteEnvironment(h.contextWithRegion(c), name)

	return writeEnvironmentResult(c, env, err)
}

func (h *Handler) handleUpdateEnvironment(c *echo.Context, name string) error {
	var req updateEnvironmentRequest
	if !decodeJSONBody(c, &req) {
		return nil
	}

	env, err := h.Backend.UpdateEnvironment(h.contextWithRegion(c), name, &req)

	return writeEnvironmentResult(c, env, err)
}

func (h *Handler) handleListEnvironments(c *echo.Context) error {
	q := c.Request().URL.Query()
	nextToken := q.Get("NextToken")

	pageSize := 0
	if v := q.Get("MaxResults"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > listEnvMaxPageSize {
			return writeErrorResponse(c, http.StatusBadRequest, "ValidationException",
				"MaxResults must be between 1 and 100")
		}

		pageSize = n
	}

	names, outToken, err := h.Backend.ListEnvironmentsPage(h.contextWithRegion(c), nextToken, pageSize)
	if err != nil {
		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerException", err.Error())
	}

	if names == nil {
		names = []string{}
	}

	resp := map[string]any{"Environments": names}
	if outToken != "" {
		resp["NextToken"] = outToken
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, resp)

	return nil
}

func (h *Handler) handleListTagsForResource(c *echo.Context, resourceARN string) error {
	tags, err := h.Backend.ListTagsForResource(h.contextWithRegion(c), resourceARN)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "ResourceNotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerException", err.Error())
	}

	if tags == nil {
		tags = map[string]string{}
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, map[string]any{
		"Tags": tags,
	})

	return nil
}

func (h *Handler) handleTagResource(c *echo.Context, resourceARN string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "failed to read request body")
	}

	var req struct {
		Tags map[string]string `json:"Tags"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "invalid request body")
	}

	if tagErr := h.Backend.TagResource(h.contextWithRegion(c), resourceARN, req.Tags); tagErr != nil {
		if errors.Is(tagErr, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "ResourceNotFoundException", tagErr.Error())
		}

		if errors.Is(tagErr, awserr.ErrInvalidParameter) {
			return writeErrorResponse(c, http.StatusBadRequest, "ValidationException", tagErr.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerException", tagErr.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, map[string]any{})

	return nil
}

func (h *Handler) handleUntagResource(c *echo.Context, resourceARN string) error {
	tagKeys := c.Request().URL.Query()["tagKeys"]

	if err := h.Backend.UntagResource(h.contextWithRegion(c), resourceARN, tagKeys); err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "ResourceNotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerException", err.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, map[string]any{})

	return nil
}

func (h *Handler) handleCreateCliToken(c *echo.Context, name string) error {
	token, err := h.Backend.CreateCliToken(h.contextWithRegion(c), name)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "ResourceNotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerException", err.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, map[string]string{
		"CliToken":          token,
		"WebServerHostname": name + ".airflow." + h.DefaultRegion + ".amazonaws.com",
	})

	return nil
}

func (h *Handler) handleCreateWebLoginToken(c *echo.Context, name string) error {
	token, err := h.Backend.CreateWebLoginToken(h.contextWithRegion(c), name)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "ResourceNotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerException", err.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, map[string]string{
		"WebToken":          token,
		"WebServerHostname": name + ".airflow." + h.DefaultRegion + ".amazonaws.com",
	})

	return nil
}

func (h *Handler) dispatchRestAPI(c *echo.Context, path string) error {
	name := strings.TrimPrefix(path, pathRestAPIPrefix)
	if c.Request().Method == http.MethodPost {
		return h.handleInvokeRestAPI(c, name)
	}

	return writeErrorResponse(c, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
}

func (h *Handler) handleInvokeRestAPI(c *echo.Context, name string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "failed to read request body")
	}

	var req invokeRestAPIRequest

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "invalid request body")
	}

	resp, err := h.Backend.InvokeRestAPI(h.contextWithRegion(c), name, &req)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "ResourceNotFoundException", err.Error())
		}

		if errors.Is(err, awserr.ErrInvalidParameter) {
			return writeErrorResponse(c, http.StatusBadRequest, "ValidationException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerException", err.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, resp)

	return nil
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

func (h *Handler) handleGetMetrics(c *echo.Context, name string) error {
	metrics, err := h.Backend.GetMetrics(h.contextWithRegion(c), name)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "ResourceNotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerException", err.Error())
	}

	if metrics == nil {
		metrics = []MetricDatum{}
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, map[string]any{
		"MetricData": metrics,
	})

	return nil
}

func (h *Handler) handlePublishMetrics(c *echo.Context, name string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "failed to read request body")
	}

	var req publishMetricsRequest

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "invalid request body")
	}

	if pubErr := h.Backend.PublishMetrics(h.contextWithRegion(c), name, &req); pubErr != nil {
		if errors.Is(pubErr, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "ResourceNotFoundException", pubErr.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerException", pubErr.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, map[string]any{})

	return nil
}

// writeErrorResponse writes a JSON error response in the MWAA REST API format.
func writeErrorResponse(c *echo.Context, statusCode int, errorType, message string) error {
	httputils.WriteJSON(c.Request().Context(), c.Response(), statusCode, map[string]string{
		"message": message,
		"__type":  errorType,
	})

	return nil
}
