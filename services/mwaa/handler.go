package mwaa

import (
	"encoding/json"
	"errors"
	"net/http"
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

		for _, prefix := range []string{"/environments", "/tags/", "/clitoken/", "/webtoken/"} {
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
	case strings.HasPrefix(path, "/clitoken/"):
		return "CreateCliToken"
	case strings.HasPrefix(path, "/webtoken/"):
		return "CreateWebLoginToken"
	case strings.HasPrefix(path, "/tags/"):
		switch method {
		case http.MethodGet:
			return "ListTagsForResource"
		case http.MethodPost:
			return "TagResource"
		case http.MethodDelete:
			return "UntagResource"
		}
	case path == "/environments" || path == "/environments/":
		if method == http.MethodGet {
			return "ListEnvironments"
		}
	case strings.HasPrefix(path, "/environments/"):
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
	}

	return "Unknown"
}

// ExtractResource extracts the environment name from the request path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	path := c.Request().URL.Path

	switch {
	case strings.HasPrefix(path, "/environments/"):
		return strings.TrimPrefix(path, "/environments/")
	case strings.HasPrefix(path, "/clitoken/"):
		return strings.TrimPrefix(path, "/clitoken/")
	case strings.HasPrefix(path, "/webtoken/"):
		return strings.TrimPrefix(path, "/webtoken/")
	}

	return ""
}

// Handler returns the echo.HandlerFunc for this service.
func (h *Handler) Handler() echo.HandlerFunc {
	return h.ServeHTTP
}

// ServeHTTP dispatches MWAA API requests.
func (h *Handler) ServeHTTP(c *echo.Context) error {
	path := c.Request().URL.Path

	switch {
	case strings.HasPrefix(path, "/clitoken/"):
		return h.dispatchCliToken(c, path)
	case strings.HasPrefix(path, "/webtoken/"):
		return h.dispatchWebToken(c, path)
	case strings.HasPrefix(path, "/tags/"):
		return h.dispatchTags(c, path)
	case path == "/environments" || path == "/environments/":
		return h.dispatchEnvironmentList(c)
	case strings.HasPrefix(path, "/environments/"):
		return h.dispatchEnvironment(c, path)
	}

	ctx := c.Request().Context()
	log := logger.Load(ctx)
	log.WarnContext(ctx, "mwaa: unhandled request", "method", c.Request().Method, "path", path)

	return writeErrorResponse(c, http.StatusNotFound, "ResourceNotFoundException", "resource not found")
}

func (h *Handler) dispatchCliToken(c *echo.Context, path string) error {
	name := strings.TrimPrefix(path, "/clitoken/")
	if c.Request().Method == http.MethodPost {
		return h.handleCreateCliToken(c, name)
	}

	return writeErrorResponse(c, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
}

func (h *Handler) dispatchWebToken(c *echo.Context, path string) error {
	name := strings.TrimPrefix(path, "/webtoken/")
	if c.Request().Method == http.MethodPost {
		return h.handleCreateWebLoginToken(c, name)
	}

	return writeErrorResponse(c, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
}

func (h *Handler) dispatchTags(c *echo.Context, path string) error {
	resourceARN := strings.TrimPrefix(path, "/tags/")

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
	name := strings.TrimPrefix(path, "/environments/")

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

func (h *Handler) handleCreateEnvironment(c *echo.Context, name string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "failed to read request body")
	}

	var req createEnvironmentRequest

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "invalid request body")
	}

	region := httputils.ExtractRegionFromRequest(c.Request(), h.DefaultRegion)

	env, err := h.Backend.CreateEnvironment(region, h.AccountID, name, &req)
	if err != nil {
		if errors.Is(err, awserr.ErrAlreadyExists) {
			return writeErrorResponse(c, http.StatusConflict, "AlreadyExistsException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerException", err.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, map[string]string{
		"Arn": env.ARN,
	})

	return nil
}

func (h *Handler) handleGetEnvironment(c *echo.Context, name string) error {
	env, err := h.Backend.GetEnvironment(name)
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
	env, err := h.Backend.DeleteEnvironment(name)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "ResourceNotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerException", err.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, map[string]string{
		"Arn": env.ARN,
	})

	return nil
}

func (h *Handler) handleUpdateEnvironment(c *echo.Context, name string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "failed to read request body")
	}

	var req updateEnvironmentRequest

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "invalid request body")
	}

	env, err := h.Backend.UpdateEnvironment(name, &req)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "ResourceNotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerException", err.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, map[string]string{
		"Arn": env.ARN,
	})

	return nil
}

func (h *Handler) handleListEnvironments(c *echo.Context) error {
	names, err := h.Backend.ListEnvironments()
	if err != nil {
		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerException", err.Error())
	}

	if names == nil {
		names = []string{}
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, map[string]any{
		"Environments": names,
	})

	return nil
}

func (h *Handler) handleListTagsForResource(c *echo.Context, resourceARN string) error {
	tags, err := h.Backend.ListTagsForResource(resourceARN)
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

	if tagErr := h.Backend.TagResource(resourceARN, req.Tags); tagErr != nil {
		if errors.Is(tagErr, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "ResourceNotFoundException", tagErr.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerException", tagErr.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, map[string]any{})

	return nil
}

func (h *Handler) handleUntagResource(c *echo.Context, resourceARN string) error {
	tagKeys := c.Request().URL.Query()["tagKeys"]

	if err := h.Backend.UntagResource(resourceARN, tagKeys); err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "ResourceNotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerException", err.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, map[string]any{})

	return nil
}

func (h *Handler) handleCreateCliToken(c *echo.Context, name string) error {
	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, map[string]string{
		"CliToken":          "stub-cli-token-" + name,
		"WebServerHostname": name + ".airflow." + h.DefaultRegion + ".amazonaws.com",
	})

	return nil
}

func (h *Handler) handleCreateWebLoginToken(c *echo.Context, name string) error {
	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, map[string]string{
		"WebToken":          "stub-web-token-" + name,
		"WebServerHostname": name + ".airflow." + h.DefaultRegion + ".amazonaws.com",
	})

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
