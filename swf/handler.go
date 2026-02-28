package swf

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputil"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	swfTargetPrefix  = "SimpleWorkflowService."
	swfMatchPriority = 100
)

// Handler is the Echo HTTP handler for SWF operations.
type Handler struct {
	Backend *InMemoryBackend
	Logger  *slog.Logger
}

// NewHandler creates a new SWF handler.
func NewHandler(backend *InMemoryBackend, log *slog.Logger) *Handler {
	return &Handler{Backend: backend, Logger: log}
}

// Name returns the service name.
func (h *Handler) Name() string { return "SWF" }

// GetSupportedOperations returns the list of supported SWF operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"RegisterDomain",
		"ListDomains",
		"DeprecateDomain",
		"RegisterWorkflowType",
		"ListWorkflowTypes",
		"StartWorkflowExecution",
		"DescribeWorkflowExecution",
	}
}

// RouteMatcher returns a function that matches SWF requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), swfTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return swfMatchPriority }

// ExtractOperation extracts the SWF action from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, swfTargetPrefix)
	if action == "" || action == target {
		return "Unknown"
	}

	return action
}

type extractSWFResourceInput struct {
	Name   string `json:"name"`
	Domain string `json:"domain"`
}

// ExtractResource extracts the domain name from the request body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputil.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var req extractSWFResourceInput
	_ = json.Unmarshal(body, &req)

	if req.Name != "" {
		return req.Name
	}

	return req.Domain
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		body, err := httputil.ReadBody(c.Request())
		if err != nil {
			return c.JSON(http.StatusInternalServerError, map[string]string{"message": "failed to read body"})
		}

		action := strings.TrimPrefix(c.Request().Header.Get("X-Amz-Target"), swfTargetPrefix)
		switch action {
		case "RegisterDomain":
			return h.handleRegisterDomain(c, body)
		case "ListDomains":
			return h.handleListDomains(c, body)
		case "DeprecateDomain":
			return h.handleDeprecateDomain(c, body)
		case "RegisterWorkflowType":
			return h.handleRegisterWorkflowType(c, body)
		case "ListWorkflowTypes":
			return h.handleListWorkflowTypes(c, body)
		case "StartWorkflowExecution":
			return h.handleStartWorkflowExecution(c, body)
		case "DescribeWorkflowExecution":
			return h.handleDescribeWorkflowExecution(c, body)
		default:
			return c.JSON(http.StatusBadRequest, map[string]string{"message": "unknown action: " + action})
		}
	}
}

type handleRegisterDomainInput struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

func (h *Handler) handleRegisterDomain(c *echo.Context, body []byte) error {
	var req handleRegisterDomainInput
	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"message": "invalid request"})
	}

	if err := h.Backend.RegisterDomain(req.Name, req.Description); err != nil {
		if errors.Is(err, ErrAlreadyExists) || errors.Is(err, ErrDeprecated) {
			return c.JSON(http.StatusBadRequest, map[string]string{"message": err.Error()})
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]string{})
}

type handleListDomainsInput struct {
	RegistrationStatus string `json:"registrationStatus"`
}

func (h *Handler) handleListDomains(c *echo.Context, body []byte) error {
	var req handleListDomainsInput
	_ = json.Unmarshal(body, &req)

	domains := h.Backend.ListDomains(req.RegistrationStatus)

	return c.JSON(http.StatusOK, map[string]any{
		"domainInfos": domains,
	})
}

type handleDeprecateDomainInput struct {
	Name string `json:"name"`
}

func (h *Handler) handleDeprecateDomain(c *echo.Context, body []byte) error {
	var req handleDeprecateDomainInput
	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"message": "invalid request"})
	}

	if err := h.Backend.DeprecateDomain(req.Name); err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.JSON(http.StatusNotFound, map[string]string{"message": err.Error()})
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]string{})
}

type handleRegisterWorkflowTypeInput struct {
	Domain  string `json:"domain"`
	Name    string `json:"name"`
	Version string `json:"version"`
}

func (h *Handler) handleRegisterWorkflowType(c *echo.Context, body []byte) error {
	var req handleRegisterWorkflowTypeInput
	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"message": "invalid request"})
	}

	if err := h.Backend.RegisterWorkflowType(req.Domain, req.Name, req.Version); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"message": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]string{})
}

type handleListWorkflowTypesInput struct {
	Domain string `json:"domain"`
}

func (h *Handler) handleListWorkflowTypes(c *echo.Context, body []byte) error {
	var req handleListWorkflowTypesInput
	_ = json.Unmarshal(body, &req)

	wts := h.Backend.ListWorkflowTypes(req.Domain)

	return c.JSON(http.StatusOK, map[string]any{
		"typeInfos": wts,
	})
}

type handleStartWorkflowExecutionInput struct {
	Domain     string `json:"domain"`
	WorkflowID string `json:"workflowId"`
}

func (h *Handler) handleStartWorkflowExecution(c *echo.Context, body []byte) error {
	var req handleStartWorkflowExecutionInput
	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"message": "invalid request"})
	}

	runID := uuid.New().String()
	exec, err := h.Backend.StartWorkflowExecution(req.Domain, req.WorkflowID, runID)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]string{
		"runId": exec.RunID,
	})
}

type handleDescribeWorkflowExecutionInput struct {
	Domain    string `json:"domain"`
	Execution struct {
		WorkflowID string `json:"workflowId"`
		RunID      string `json:"runId"`
	} `json:"execution"`
}

func (h *Handler) handleDescribeWorkflowExecution(c *echo.Context, body []byte) error {
	var req handleDescribeWorkflowExecutionInput
	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"message": "invalid request"})
	}

	exec, err := h.Backend.DescribeWorkflowExecution(req.Domain, req.Execution.WorkflowID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.JSON(http.StatusNotFound, map[string]string{"message": err.Error()})
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"executionInfo": exec,
	})
}
