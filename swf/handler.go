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

func (h *Handler) dispatchTable() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"RegisterDomain":            service.WrapOp(h.handleRegisterDomain),
		"ListDomains":               service.WrapOp(h.handleListDomains),
		"DeprecateDomain":           service.WrapOp(h.handleDeprecateDomain),
		"RegisterWorkflowType":      service.WrapOp(h.handleRegisterWorkflowType),
		"ListWorkflowTypes":         service.WrapOp(h.handleListWorkflowTypes),
		"StartWorkflowExecution":    service.WrapOp(h.handleStartWorkflowExecution),
		"DescribeWorkflowExecution": service.WrapOp(h.handleDescribeWorkflowExecution),
	}
}

func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.dispatchTable()[action]
	if !ok {
		return nil, ErrUnknownOperation
	}

	result, err := fn(ctx, body)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleError(_ context.Context, c *echo.Context, _ string, err error) error {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError

	code := http.StatusInternalServerError

	switch {
	case errors.Is(err, errInvalidRequest), errors.Is(err, ErrUnknownOperation),
		errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		code = http.StatusBadRequest
	case errors.Is(err, ErrAlreadyExists), errors.Is(err, ErrDeprecated), errors.Is(err, ErrTypeAlreadyExists):
		code = http.StatusBadRequest
	case errors.Is(err, ErrNotFound):
		code = http.StatusNotFound
	}

	return c.JSON(code, map[string]string{"message": err.Error()})
}

type registerDomainOutput struct{}

type listDomainsOutput struct {
	DomainInfos []Domain `json:"domainInfos"`
}

type deprecateDomainOutput struct{}

type registerWorkflowTypeOutput struct{}

type listWorkflowTypesOutput struct {
	TypeInfos []WorkflowType `json:"typeInfos"`
}

type startWorkflowExecutionOutput struct {
	RunID string `json:"runId"`
}

type describeWorkflowExecutionOutput struct {
	ExecutionInfo *WorkflowExecution `json:"executionInfo"`
}

type handleRegisterDomainInput struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

func (h *Handler) handleRegisterDomain(
	_ context.Context,
	in *handleRegisterDomainInput,
) (*registerDomainOutput, error) {
	if err := h.Backend.RegisterDomain(in.Name, in.Description); err != nil {
		return nil, err
	}

	return &registerDomainOutput{}, nil
}

type handleListDomainsInput struct {
	RegistrationStatus string `json:"registrationStatus"`
}

func (h *Handler) handleListDomains(_ context.Context, in *handleListDomainsInput) (*listDomainsOutput, error) {
	domains := h.Backend.ListDomains(in.RegistrationStatus)

	return &listDomainsOutput{DomainInfos: domains}, nil
}

type handleDeprecateDomainInput struct {
	Name string `json:"name"`
}

func (h *Handler) handleDeprecateDomain(
	_ context.Context,
	in *handleDeprecateDomainInput,
) (*deprecateDomainOutput, error) {
	if err := h.Backend.DeprecateDomain(in.Name); err != nil {
		return nil, err
	}

	return &deprecateDomainOutput{}, nil
}

type handleRegisterWorkflowTypeInput struct {
	Domain  string `json:"domain"`
	Name    string `json:"name"`
	Version string `json:"version"`
}

func (h *Handler) handleRegisterWorkflowType(
	_ context.Context,
	in *handleRegisterWorkflowTypeInput,
) (*registerWorkflowTypeOutput, error) {
	if err := h.Backend.RegisterWorkflowType(in.Domain, in.Name, in.Version); err != nil {
		return nil, err
	}

	return &registerWorkflowTypeOutput{}, nil
}

type handleListWorkflowTypesInput struct {
	Domain string `json:"domain"`
}

func (h *Handler) handleListWorkflowTypes(
	_ context.Context,
	in *handleListWorkflowTypesInput,
) (*listWorkflowTypesOutput, error) {
	wts := h.Backend.ListWorkflowTypes(in.Domain)

	return &listWorkflowTypesOutput{TypeInfos: wts}, nil
}

type handleStartWorkflowExecutionInput struct {
	Domain     string `json:"domain"`
	WorkflowID string `json:"workflowId"`
}

func (h *Handler) handleStartWorkflowExecution(
	_ context.Context,
	in *handleStartWorkflowExecutionInput,
) (*startWorkflowExecutionOutput, error) {
	runID := uuid.New().String()

	exec, err := h.Backend.StartWorkflowExecution(in.Domain, in.WorkflowID, runID)
	if err != nil {
		return nil, err
	}

	return &startWorkflowExecutionOutput{RunID: exec.RunID}, nil
}

type handleDescribeWorkflowExecutionInput struct {
	Domain    string `json:"domain"`
	Execution struct {
		WorkflowID string `json:"workflowId"`
		RunID      string `json:"runId"`
	} `json:"execution"`
}

func (h *Handler) handleDescribeWorkflowExecution(
	_ context.Context,
	in *handleDescribeWorkflowExecutionInput,
) (*describeWorkflowExecutionOutput, error) {
	exec, err := h.Backend.DescribeWorkflowExecution(in.Domain, in.Execution.WorkflowID)
	if err != nil {
		return nil, err
	}

	return &describeWorkflowExecutionOutput{ExecutionInfo: exec}, nil
}
