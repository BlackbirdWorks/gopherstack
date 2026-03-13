package pinpoint

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
	pinpointService       = "mobiletargeting"
	pinpointMatchPriority = 87
)

// Handler is the HTTP handler for the Amazon Pinpoint REST API.
type Handler struct {
	Backend       StorageBackend
	AccountID     string
	DefaultRegion string
}

// NewHandler creates a new Pinpoint handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "Pinpoint" }

// GetSupportedOperations returns the list of supported Pinpoint operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateApp",
		"GetApp",
		"DeleteApp",
		"GetApps",
		"TagResource",
		"UntagResource",
		"ListTagsForResource",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return pinpointService }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler handles.
func (h *Handler) ChaosRegions() []string { return []string{h.DefaultRegion} }

// RouteMatcher returns a function that matches Pinpoint API requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		if httputils.ExtractServiceFromRequest(c.Request()) != pinpointService {
			return false
		}

		path := c.Request().URL.Path

		return strings.HasPrefix(path, "/v1/apps") ||
			strings.HasPrefix(path, "/v1/tags/")
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return pinpointMatchPriority }

// ExtractOperation extracts the operation name from the request path and method.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	method := c.Request().Method
	path := c.Request().URL.Path

	switch {
	case strings.HasPrefix(path, "/v1/tags/"):
		switch method {
		case http.MethodGet:
			return "ListTagsForResource"
		case http.MethodPost:
			return "TagResource"
		case http.MethodDelete:
			return "UntagResource"
		}
	case path == "/v1/apps" || path == "/v1/apps/":
		if method == http.MethodPost {
			return "CreateApp"
		}

		if method == http.MethodGet {
			return "GetApps"
		}
	case strings.HasPrefix(path, "/v1/apps/"):
		switch method {
		case http.MethodGet:
			return "GetApp"
		case http.MethodDelete:
			return "DeleteApp"
		}
	}

	return "Unknown"
}

// ExtractResource extracts the app ID from the request path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	path := c.Request().URL.Path

	switch {
	case strings.HasPrefix(path, "/v1/apps/"):
		return strings.TrimPrefix(path, "/v1/apps/")
	case strings.HasPrefix(path, "/v1/tags/"):
		return strings.TrimPrefix(path, "/v1/tags/")
	}

	return ""
}

// Handler returns the echo.HandlerFunc for this service.
func (h *Handler) Handler() echo.HandlerFunc {
	return h.ServeHTTP
}

// ServeHTTP dispatches Pinpoint API requests.
func (h *Handler) ServeHTTP(c *echo.Context) error {
	path := c.Request().URL.Path

	switch {
	case strings.HasPrefix(path, "/v1/tags/"):
		return h.dispatchTags(c, path)
	case path == "/v1/apps" || path == "/v1/apps/":
		return h.dispatchApps(c)
	case strings.HasPrefix(path, "/v1/apps/"):
		return h.dispatchApp(c, path)
	}

	ctx := c.Request().Context()
	log := logger.Load(ctx)
	log.WarnContext(ctx, "pinpoint: unhandled request", "method", c.Request().Method, "path", path)

	return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", "resource not found")
}

func (h *Handler) dispatchTags(c *echo.Context, path string) error {
	resourceARN := strings.TrimPrefix(path, "/v1/tags/")

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

func (h *Handler) dispatchApps(c *echo.Context) error {
	switch c.Request().Method {
	case http.MethodPost:
		return h.handleCreateApp(c)
	case http.MethodGet:
		return h.handleGetApps(c)
	}

	return writeErrorResponse(c, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
}

func (h *Handler) dispatchApp(c *echo.Context, path string) error {
	appID := strings.TrimPrefix(path, "/v1/apps/")

	switch c.Request().Method {
	case http.MethodGet:
		return h.handleGetApp(c, appID)
	case http.MethodDelete:
		return h.handleDeleteApp(c, appID)
	}

	return writeErrorResponse(c, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
}

func (h *Handler) handleCreateApp(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "failed to read request body")
	}

	var req createAppRequest

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "invalid request body")
	}

	region := httputils.ExtractRegionFromRequest(c.Request(), h.DefaultRegion)

	app, err := h.Backend.CreateApp(region, h.AccountID, req.Name, req.Tags)
	if err != nil {
		if errors.Is(err, awserr.ErrAlreadyExists) {
			return writeErrorResponse(c, http.StatusConflict, "ConflictException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusCreated, toAppResponse(app))

	return nil
}

func (h *Handler) handleGetApp(c *echo.Context, appID string) error {
	app, err := h.Backend.GetApp(appID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, toAppResponse(app))

	return nil
}

func (h *Handler) handleDeleteApp(c *echo.Context, appID string) error {
	app, err := h.Backend.DeleteApp(appID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, toAppResponse(app))

	return nil
}

func (h *Handler) handleGetApps(c *echo.Context) error {
	apps, err := h.Backend.GetApps()
	if err != nil {
		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	items := make([]appResponse, 0, len(apps))

	for _, app := range apps {
		items = append(items, toAppResponse(app))
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, appsResponse{Item: items})

	return nil
}

func (h *Handler) handleTagResource(c *echo.Context, resourceARN string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "failed to read request body")
	}

	var req tagResourceRequest

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "invalid request body")
	}

	tagErr := h.Backend.TagResource(resourceARN, req.Tags)
	if tagErr != nil {
		if errors.Is(tagErr, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", tagErr.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", tagErr.Error())
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) handleUntagResource(c *echo.Context, resourceARN string) error {
	tagKeys := c.Request().URL.Query()["tagKeys"]

	if err := h.Backend.UntagResource(resourceARN, tagKeys); err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) handleListTagsForResource(c *echo.Context, resourceARN string) error {
	tags, err := h.Backend.ListTagsForResource(resourceARN)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, tagsModel{Tags: tags})

	return nil
}

// toAppResponse converts an App to the JSON wire format.
func toAppResponse(app *App) appResponse {
	return appResponse{
		ARN:          app.ARN,
		ID:           app.ID,
		Name:         app.Name,
		CreationDate: app.CreationDate,
		Tags:         app.Tags,
	}
}

// writeErrorResponse writes a JSON error response in the Pinpoint REST API format.
func writeErrorResponse(c *echo.Context, statusCode int, errorType, message string) error {
	httputils.WriteJSON(c.Request().Context(), c.Response(), statusCode, map[string]string{
		"message": message,
		"__type":  errorType,
	})

	return nil
}
