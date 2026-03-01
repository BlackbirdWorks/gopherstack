package resourcegroups

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputil"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

var (
	// ErrUnknownOperation is returned when the requested Resource Groups operation is not supported.
	ErrUnknownOperation = errors.New("UnknownOperationException")
	errInvalidRequest   = errors.New("invalid request")
)

const resourceGroupsTargetPrefix = "ResourceGroups."

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
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

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
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"ResourceGroups", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

func (h *Handler) dispatch(_ context.Context, action string, body []byte) ([]byte, error) {
	var result any
	var err error

	switch action {
	case "CreateGroup":
		result, err = h.handleCreateGroup(body)
	case "DeleteGroup":
		result, err = h.handleDeleteGroup(body)
	case "ListGroups":
		result, err = h.handleListGroups()
	case "GetGroup":
		result, err = h.handleGetGroup(body)
	default:
		return nil, ErrUnknownOperation
	}

	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleError(_ context.Context, c *echo.Context, _ string, err error) error {
	code := http.StatusInternalServerError

	switch {
	case errors.Is(err, errInvalidRequest), errors.Is(err, ErrUnknownOperation):
		code = http.StatusBadRequest
	case errors.Is(err, ErrAlreadyExists):
		code = http.StatusBadRequest
	case errors.Is(err, ErrNotFound):
		code = http.StatusNotFound
	}

	return c.JSON(code, map[string]string{"message": err.Error()})
}

type handleCreateGroupInput struct {
	Tags        *tags.Tags `json:"Tags"`
	Name        string     `json:"Name"`
	Description string     `json:"Description"`
}

func (h *Handler) handleCreateGroup(body []byte) (any, error) {
	var req handleCreateGroupInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, errInvalidRequest
	}

	g, err := h.Backend.CreateGroup(req.Name, req.Description, req.Tags)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"Group": g,
	}, nil
}

func (h *Handler) handleDeleteGroup(body []byte) (any, error) {
	var req groupNameInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, errInvalidRequest
	}

	if err := h.Backend.DeleteGroup(req.GroupName); err != nil {
		return nil, err
	}

	return map[string]string{}, nil
}

//nolint:unparam // error returned for consistent dispatch signature
func (h *Handler) handleListGroups() (any, error) {
	groups := h.Backend.ListGroups()

	return map[string]any{
		"GroupIdentifiers": groups,
	}, nil
}

func (h *Handler) handleGetGroup(body []byte) (any, error) {
	var req groupNameInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, errInvalidRequest
	}

	g, err := h.Backend.GetGroup(req.GroupName)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"Group": g,
	}, nil
}
