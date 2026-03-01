package swf

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputil"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

var (
	// ErrUnknownOperation is returned when the requested SWF operation is not supported.
	ErrUnknownOperation = errors.New("UnknownOperationException")
	errInvalidRequest   = errors.New("invalid request")
)

const swfTargetPrefix = "SimpleWorkflowService."

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
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

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
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"SWF", "application/x-amz-json-1.1",
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
	case "RegisterDomain":
		result, err = h.handleRegisterDomain(body)
	case "ListDomains":
		result, err = h.handleListDomains(body)
	case "DeprecateDomain":
		result, err = h.handleDeprecateDomain(body)
	case "RegisterWorkflowType":
		result, err = h.handleRegisterWorkflowType(body)
	case "ListWorkflowTypes":
		result, err = h.handleListWorkflowTypes(body)
	case "StartWorkflowExecution":
		result, err = h.handleStartWorkflowExecution(body)
	case "DescribeWorkflowExecution":
		result, err = h.handleDescribeWorkflowExecution(body)
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
	case errors.Is(err, ErrAlreadyExists), errors.Is(err, ErrDeprecated), errors.Is(err, ErrTypeAlreadyExists):
		code = http.StatusBadRequest
	case errors.Is(err, ErrNotFound):
		code = http.StatusNotFound
	}

	return c.JSON(code, map[string]string{"message": err.Error()})
}

type handleRegisterDomainInput struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

func (h *Handler) handleRegisterDomain(body []byte) (any, error) {
	var req handleRegisterDomainInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, errInvalidRequest
	}

	if err := h.Backend.RegisterDomain(req.Name, req.Description); err != nil {
		return nil, err
	}

	return map[string]string{}, nil
}

type handleListDomainsInput struct {
	RegistrationStatus string `json:"registrationStatus"`
}

//nolint:unparam // error returned for consistent dispatch signature
func (h *Handler) handleListDomains(body []byte) (any, error) {
	var req handleListDomainsInput
	_ = json.Unmarshal(body, &req)

	domains := h.Backend.ListDomains(req.RegistrationStatus)

	return map[string]any{
		"domainInfos": domains,
	}, nil
}

type handleDeprecateDomainInput struct {
	Name string `json:"name"`
}

func (h *Handler) handleDeprecateDomain(body []byte) (any, error) {
	var req handleDeprecateDomainInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, errInvalidRequest
	}

	if err := h.Backend.DeprecateDomain(req.Name); err != nil {
		return nil, err
	}

	return map[string]string{}, nil
}

type handleRegisterWorkflowTypeInput struct {
	Domain  string `json:"domain"`
	Name    string `json:"name"`
	Version string `json:"version"`
}

func (h *Handler) handleRegisterWorkflowType(body []byte) (any, error) {
	var req handleRegisterWorkflowTypeInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, errInvalidRequest
	}

	if err := h.Backend.RegisterWorkflowType(req.Domain, req.Name, req.Version); err != nil {
		return nil, err
	}

	return map[string]string{}, nil
}

type handleListWorkflowTypesInput struct {
	Domain string `json:"domain"`
}

//nolint:unparam // error returned for consistent dispatch signature
func (h *Handler) handleListWorkflowTypes(body []byte) (any, error) {
	var req handleListWorkflowTypesInput
	_ = json.Unmarshal(body, &req)

	wts := h.Backend.ListWorkflowTypes(req.Domain)

	return map[string]any{
		"typeInfos": wts,
	}, nil
}

type handleStartWorkflowExecutionInput struct {
	Domain     string `json:"domain"`
	WorkflowID string `json:"workflowId"`
}

func (h *Handler) handleStartWorkflowExecution(body []byte) (any, error) {
	var req handleStartWorkflowExecutionInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, errInvalidRequest
	}

	runID := uuid.New().String()

	exec, err := h.Backend.StartWorkflowExecution(req.Domain, req.WorkflowID, runID)
	if err != nil {
		return nil, err
	}

	return map[string]string{
		"runId": exec.RunID,
	}, nil
}

type handleDescribeWorkflowExecutionInput struct {
	Domain    string `json:"domain"`
	Execution struct {
		WorkflowID string `json:"workflowId"`
		RunID      string `json:"runId"`
	} `json:"execution"`
}

func (h *Handler) handleDescribeWorkflowExecution(body []byte) (any, error) {
	var req handleDescribeWorkflowExecutionInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, errInvalidRequest
	}

	exec, err := h.Backend.DescribeWorkflowExecution(req.Domain, req.Execution.WorkflowID)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"executionInfo": exec,
	}, nil
}
