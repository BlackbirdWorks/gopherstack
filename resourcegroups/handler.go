package resourcegroups

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputil"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	resourceGroupsTargetPrefix  = "ResourceGroups."
	resourceGroupsMatchPriority = 100
)

type groupNameInput struct {
	GroupName string `json:"GroupName"`
}

// Handler is the Echo HTTP handler for Resource Groups operations.
type Handler struct {
	Backend *InMemoryBackend
	Logger  *slog.Logger
}

// NewHandler creates a new Resource Groups handler.
func NewHandler(backend *InMemoryBackend, log *slog.Logger) *Handler {
	return &Handler{Backend: backend, Logger: log}
}

// Name returns the service name.
func (h *Handler) Name() string { return "ResourceGroups" }

// GetSupportedOperations returns the list of supported Resource Groups operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateGroup",
		"DeleteGroup",
		"ListGroups",
		"GetGroup",
	}
}

// RouteMatcher returns a function that matches Resource Groups requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), resourceGroupsTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return resourceGroupsMatchPriority }

// ExtractOperation extracts the Resource Groups action from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, resourceGroupsTargetPrefix)
	if action == "" || action == target {
		return "Unknown"
	}

	return action
}

type extractResourceGroupInput struct {
	Name      string `json:"Name"`
	GroupName string `json:"GroupName"`
}

// ExtractResource extracts the group name from the request body, checking both
// the Name (CreateGroup) and GroupName (GetGroup/DeleteGroup) fields.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputil.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var req extractResourceGroupInput
	_ = json.Unmarshal(body, &req)

	if req.Name != "" {
		return req.Name
	}

	return req.GroupName
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		body, err := httputil.ReadBody(c.Request())
		if err != nil {
			return c.JSON(http.StatusInternalServerError, map[string]string{"message": "failed to read body"})
		}

		action := strings.TrimPrefix(c.Request().Header.Get("X-Amz-Target"), resourceGroupsTargetPrefix)
		switch action {
		case "CreateGroup":
			return h.handleCreateGroup(c, body)
		case "DeleteGroup":
			return h.handleDeleteGroup(c, body)
		case "ListGroups":
			return h.handleListGroups(c)
		case "GetGroup":
			return h.handleGetGroup(c, body)
		default:
			return c.JSON(http.StatusBadRequest, map[string]string{"message": "unknown action: " + action})
		}
	}
}

type handleCreateGroupInput struct {
	Tags        map[string]string `json:"Tags"`
	Name        string            `json:"Name"`
	Description string            `json:"Description"`
}

func (h *Handler) handleCreateGroup(c *echo.Context, body []byte) error {
	var req handleCreateGroupInput
	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"message": "invalid request"})
	}

	g, err := h.Backend.CreateGroup(req.Name, req.Description, req.Tags)
	if err != nil {
		if errors.Is(err, ErrAlreadyExists) {
			return c.JSON(http.StatusBadRequest, map[string]string{"message": err.Error()})
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"Group": g,
	})
}

func (h *Handler) handleDeleteGroup(c *echo.Context, body []byte) error {
	var req groupNameInput
	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"message": "invalid request"})
	}

	if err := h.Backend.DeleteGroup(req.GroupName); err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.JSON(http.StatusNotFound, map[string]string{"message": err.Error()})
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]string{})
}

func (h *Handler) handleListGroups(c *echo.Context) error {
	groups := h.Backend.ListGroups()

	return c.JSON(http.StatusOK, map[string]any{
		"GroupIdentifiers": groups,
	})
}

func (h *Handler) handleGetGroup(c *echo.Context, body []byte) error {
	var req groupNameInput
	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"message": "invalid request"})
	}

	g, err := h.Backend.GetGroup(req.GroupName)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.JSON(http.StatusNotFound, map[string]string{"message": err.Error()})
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"Group": g,
	})
}
