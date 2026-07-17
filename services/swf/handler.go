package swf

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
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
	Backend StorageBackend
	ops     map[string]service.JSONOpFunc
}

// NewHandler creates a new SWF handler with a cached dispatch table.
func NewHandler(backend StorageBackend) *Handler {
	h := &Handler{Backend: backend}
	h.ops = h.buildOps()

	return h
}

// Reset clears all backend state.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// Name returns the service name.
func (h *Handler) Name() string { return "SWF" }

// GetSupportedOperations returns the list of supported SWF operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CountClosedWorkflowExecutions",
		"CountOpenWorkflowExecutions",
		"CountPendingActivityTasks",
		"CountPendingDecisionTasks",
		"DeleteActivityType",
		"DeleteWorkflowType",
		"DeprecateActivityType",
		"DeprecateDomain",
		"DeprecateWorkflowType",
		"DescribeActivityType",
		"DescribeDomain",
		"DescribeWorkflowExecution",
		"DescribeWorkflowType",
		"GetWorkflowExecutionHistory",
		"ListActivityTypes",
		"ListClosedWorkflowExecutions",
		"ListDomains",
		"ListOpenWorkflowExecutions",
		"ListTagsForResource",
		"ListWorkflowTypes",
		"PollForActivityTask",
		"PollForDecisionTask",
		"RecordActivityTaskHeartbeat",
		"RegisterActivityType",
		"RegisterDomain",
		"RegisterWorkflowType",
		"RequestCancelWorkflowExecution",
		"RespondActivityTaskCanceled",
		"RespondActivityTaskCompleted",
		"RespondActivityTaskFailed",
		"RespondDecisionTaskCompleted",
		"SignalWorkflowExecution",
		"StartWorkflowExecution",
		"TagResource",
		"TerminateWorkflowExecution",
		"UndeprecateActivityType",
		"UndeprecateDomain",
		"UndeprecateWorkflowType",
		"UntagResource",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "swf" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this SWF instance handles.
func (h *Handler) ChaosRegions() []string { return []string{config.DefaultRegion} }

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
	body, err := httputils.ReadBody(c.Request())
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
			"SWF", "application/x-amz-json-1.0",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

func (h *Handler) buildOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"RegisterDomain":                 service.WrapOp(h.handleRegisterDomain),
		"DescribeDomain":                 service.WrapOp(h.handleDescribeDomain),
		"ListDomains":                    service.WrapOp(h.handleListDomains),
		"DeprecateDomain":                service.WrapOp(h.handleDeprecateDomain),
		"UndeprecateDomain":              service.WrapOp(h.handleUndeprecateDomain),
		"RegisterWorkflowType":           service.WrapOp(h.handleRegisterWorkflowType),
		"ListWorkflowTypes":              service.WrapOp(h.handleListWorkflowTypes),
		"DescribeWorkflowType":           service.WrapOp(h.handleDescribeWorkflowType),
		"DeprecateWorkflowType":          service.WrapOp(h.handleDeprecateWorkflowType),
		"UndeprecateWorkflowType":        service.WrapOp(h.handleUndeprecateWorkflowType),
		"DeleteWorkflowType":             service.WrapOp(h.handleDeleteWorkflowType),
		"RegisterActivityType":           service.WrapOp(h.handleRegisterActivityType),
		"ListActivityTypes":              service.WrapOp(h.handleListActivityTypes),
		"DescribeActivityType":           service.WrapOp(h.handleDescribeActivityType),
		"DeprecateActivityType":          service.WrapOp(h.handleDeprecateActivityType),
		"UndeprecateActivityType":        service.WrapOp(h.handleUndeprecateActivityType),
		"DeleteActivityType":             service.WrapOp(h.handleDeleteActivityType),
		"CountOpenWorkflowExecutions":    service.WrapOp(h.handleCountOpenWorkflowExecutions),
		"CountClosedWorkflowExecutions":  service.WrapOp(h.handleCountClosedWorkflowExecutions),
		"CountPendingActivityTasks":      service.WrapOp(h.handleCountPendingActivityTasks),
		"CountPendingDecisionTasks":      service.WrapOp(h.handleCountPendingDecisionTasks),
		"StartWorkflowExecution":         service.WrapOp(h.handleStartWorkflowExecution),
		"TerminateWorkflowExecution":     service.WrapOp(h.handleTerminateWorkflowExecution),
		"DescribeWorkflowExecution":      service.WrapOp(h.handleDescribeWorkflowExecution),
		"GetWorkflowExecutionHistory":    service.WrapOp(h.handleGetWorkflowExecutionHistory),
		"ListOpenWorkflowExecutions":     service.WrapOp(h.handleListOpenWorkflowExecutions),
		"ListClosedWorkflowExecutions":   service.WrapOp(h.handleListClosedWorkflowExecutions),
		"ListTagsForResource":            service.WrapOp(h.handleListTagsForResource),
		"TagResource":                    service.WrapOp(h.handleTagResource),
		"UntagResource":                  service.WrapOp(h.handleUntagResource),
		"PollForActivityTask":            service.WrapOp(h.handlePollForActivityTask),
		"PollForDecisionTask":            service.WrapOp(h.handlePollForDecisionTask),
		"RecordActivityTaskHeartbeat":    service.WrapOp(h.handleRecordActivityTaskHeartbeat),
		"RequestCancelWorkflowExecution": service.WrapOp(h.handleRequestCancelWorkflowExecution),
		"RespondActivityTaskCanceled":    service.WrapOp(h.handleRespondActivityTaskCanceled),
		"RespondActivityTaskCompleted":   service.WrapOp(h.handleRespondActivityTaskCompleted),
		"RespondActivityTaskFailed":      service.WrapOp(h.handleRespondActivityTaskFailed),
		"RespondDecisionTaskCompleted":   service.WrapOp(h.handleRespondDecisionTaskCompleted),
		"SignalWorkflowExecution":        service.WrapOp(h.handleSignalWorkflowExecution),
	}
}

func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.ops[action]
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
	var errType string

	switch {
	case errors.Is(err, ErrWorkflowAlreadyStarted):
		code = http.StatusBadRequest
		errType = "WorkflowExecutionAlreadyStartedFault"
	case errors.Is(err, ErrAlreadyExists):
		code = http.StatusBadRequest
		errType = "DomainAlreadyExistsFault"
	case errors.Is(err, ErrDeprecated):
		code = http.StatusBadRequest
		errType = "DomainDeprecatedFault"
	case errors.Is(err, ErrTypeAlreadyExists):
		code = http.StatusBadRequest
		errType = "TypeAlreadyExistsFault"
	case errors.Is(err, ErrTypeDeprecated):
		code = http.StatusBadRequest
		errType = "TypeDeprecatedFault"
	case errors.Is(err, ErrTypeNotDeprecated):
		code = http.StatusBadRequest
		errType = "TypeNotDeprecatedFault"
	case errors.Is(err, ErrTooManyTags):
		code = http.StatusBadRequest
		errType = "TooManyTagsFault"
	case errors.Is(err, ErrValidation):
		code = http.StatusBadRequest
		errType = "ValidationException"
	case errors.Is(err, ErrNotFound):
		code = http.StatusNotFound
		errType = "UnknownResourceFault"
	case errors.Is(err, ErrOperationNotPermitted):
		code = http.StatusForbidden
		errType = "OperationNotPermittedFault"
	case errors.Is(err, errInvalidRequest), errors.Is(err, ErrUnknownOperation),
		errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		code = http.StatusBadRequest
	}

	resp := map[string]string{"message": err.Error()}
	if errType != "" {
		resp["__type"] = errType
	}

	return c.JSON(code, resp)
}

const defaultSWFMaxPageSize = 1000

// applyPageTokenSlice applies nextPageToken-based pagination using the shared
// pkgs/page opaque token format.
func applyPageTokenSlice[T any](items []T, nextPageToken string, maximumPageSize int) ([]T, string) {
	p := page.New(items, nextPageToken, maximumPageSize, defaultSWFMaxPageSize)

	return p.Data, p.Next
}
