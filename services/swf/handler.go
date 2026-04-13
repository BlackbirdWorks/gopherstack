package swf

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strings"

	"github.com/google/uuid"
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
	Backend *InMemoryBackend
}

// NewHandler creates a new SWF handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
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
		"ListDomains",
		"ListWorkflowTypes",
		"RegisterDomain",
		"RegisterWorkflowType",
		"StartWorkflowExecution",
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
			"SWF", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

func (h *Handler) dispatchTable() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"RegisterDomain":                service.WrapOp(h.handleRegisterDomain),
		"DescribeDomain":                service.WrapOp(h.handleDescribeDomain),
		"ListDomains":                   service.WrapOp(h.handleListDomains),
		"DeprecateDomain":               service.WrapOp(h.handleDeprecateDomain),
		"RegisterWorkflowType":          service.WrapOp(h.handleRegisterWorkflowType),
		"ListWorkflowTypes":             service.WrapOp(h.handleListWorkflowTypes),
		"DescribeWorkflowType":          service.WrapOp(h.handleDescribeWorkflowType),
		"DeprecateWorkflowType":         service.WrapOp(h.handleDeprecateWorkflowType),
		"DeleteWorkflowType":            service.WrapOp(h.handleDeleteWorkflowType),
		"DescribeActivityType":          service.WrapOp(h.handleDescribeActivityType),
		"DeprecateActivityType":         service.WrapOp(h.handleDeprecateActivityType),
		"DeleteActivityType":            service.WrapOp(h.handleDeleteActivityType),
		"CountOpenWorkflowExecutions":   service.WrapOp(h.handleCountOpenWorkflowExecutions),
		"CountClosedWorkflowExecutions": service.WrapOp(h.handleCountClosedWorkflowExecutions),
		"CountPendingActivityTasks":     service.WrapOp(h.handleCountPendingActivityTasks),
		"CountPendingDecisionTasks":     service.WrapOp(h.handleCountPendingDecisionTasks),
		"StartWorkflowExecution":        service.WrapOp(h.handleStartWorkflowExecution),
		"DescribeWorkflowExecution":     service.WrapOp(h.handleDescribeWorkflowExecution),
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
	var errType string

	switch {
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
	case errors.Is(err, ErrNotFound):
		code = http.StatusNotFound
		errType = "UnknownResourceFault"
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

type registerDomainOutput struct{}

type describeDomainOutput struct {
	DomainInfo    *Domain            `json:"domainInfo"`
	Configuration domainConfigOutput `json:"configuration"`
}

type domainConfigOutput struct {
	WorkflowExecutionRetentionPeriodInDays string `json:"workflowExecutionRetentionPeriodInDays"`
}

type listDomainsOutput struct {
	NextPageToken string   `json:"nextPageToken,omitempty"`
	DomainInfos   []Domain `json:"domainInfos"`
}

type deprecateDomainOutput struct{}

type registerWorkflowTypeOutput struct{}

type listWorkflowTypesOutput struct {
	NextPageToken string         `json:"nextPageToken,omitempty"`
	TypeInfos     []WorkflowType `json:"typeInfos"`
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

type handleDescribeDomainInput struct {
	Name string `json:"name"`
}

func (h *Handler) handleDescribeDomain(
	_ context.Context,
	in *handleDescribeDomainInput,
) (*describeDomainOutput, error) {
	d, err := h.Backend.DescribeDomain(in.Name)
	if err != nil {
		return nil, err
	}

	return &describeDomainOutput{
		DomainInfo:    d,
		Configuration: domainConfigOutput{WorkflowExecutionRetentionPeriodInDays: "NONE"},
	}, nil
}

type handleListDomainsInput struct {
	RegistrationStatus string `json:"registrationStatus"`
	NextPageToken      string `json:"nextPageToken,omitempty"`
	MaximumPageSize    int    `json:"maximumPageSize,omitempty"`
}

func (h *Handler) handleListDomains(_ context.Context, in *handleListDomainsInput) (*listDomainsOutput, error) {
	domains := h.Backend.ListDomains(in.RegistrationStatus)

	sort.Slice(domains, func(i, j int) bool { return domains[i].Name < domains[j].Name })

	domains, nextPageToken := applyPageTokenSlice(domains, in.NextPageToken, in.MaximumPageSize)

	return &listDomainsOutput{DomainInfos: domains, NextPageToken: nextPageToken}, nil
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
	Domain          string `json:"domain"`
	NextPageToken   string `json:"nextPageToken,omitempty"`
	MaximumPageSize int    `json:"maximumPageSize,omitempty"`
}

func (h *Handler) handleListWorkflowTypes(
	_ context.Context,
	in *handleListWorkflowTypesInput,
) (*listWorkflowTypesOutput, error) {
	wts := h.Backend.ListWorkflowTypes(in.Domain)

	sort.Slice(wts, func(i, j int) bool { return wts[i].Name < wts[j].Name })

	wts, nextPageToken := applyPageTokenSlice(wts, in.NextPageToken, in.MaximumPageSize)

	return &listWorkflowTypesOutput{TypeInfos: wts, NextPageToken: nextPageToken}, nil
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

// workflowExecutionRef identifies a specific workflow execution by ID and run ID.
type workflowExecutionRef struct {
	WorkflowID string `json:"workflowId"`
	RunID      string `json:"runId"`
}

type handleDescribeWorkflowExecutionInput struct {
	Domain    string               `json:"domain"`
	Execution workflowExecutionRef `json:"execution"`
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

// workflowTypeRef identifies a workflow type by name and version.
type workflowTypeRef struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// activityTypeRef identifies an activity type by name and version.
type activityTypeRef struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

type typeInfoOutput struct {
	WorkflowType *workflowTypeRef `json:"workflowType,omitempty"`
	ActivityType *activityTypeRef `json:"activityType,omitempty"`
	Status       string           `json:"status"`
	Description  string           `json:"description,omitempty"`
}

type typeConfigOutput struct{}

type handleDescribeWorkflowTypeInput struct {
	Domain       string          `json:"domain"`
	WorkflowType workflowTypeRef `json:"workflowType"`
}

type describeWorkflowTypeOutput struct {
	Configuration typeConfigOutput `json:"configuration"`
	TypeInfo      typeInfoOutput   `json:"typeInfo"`
}

func (h *Handler) handleDescribeWorkflowType(
	_ context.Context,
	in *handleDescribeWorkflowTypeInput,
) (*describeWorkflowTypeOutput, error) {
	wt, err := h.Backend.DescribeWorkflowType(in.Domain, in.WorkflowType.Name, in.WorkflowType.Version)
	if err != nil {
		return nil, err
	}

	return &describeWorkflowTypeOutput{
		TypeInfo: typeInfoOutput{
			WorkflowType: &workflowTypeRef{Name: wt.Name, Version: wt.Version},
			Status:       wt.Status,
			Description:  wt.Description,
		},
	}, nil
}

type handleDeprecateWorkflowTypeInput struct {
	Domain       string          `json:"domain"`
	WorkflowType workflowTypeRef `json:"workflowType"`
}

type deprecateWorkflowTypeOutput struct{}

func (h *Handler) handleDeprecateWorkflowType(
	_ context.Context,
	in *handleDeprecateWorkflowTypeInput,
) (*deprecateWorkflowTypeOutput, error) {
	if err := h.Backend.DeprecateWorkflowType(in.Domain, in.WorkflowType.Name, in.WorkflowType.Version); err != nil {
		return nil, err
	}

	return &deprecateWorkflowTypeOutput{}, nil
}

type handleDeleteWorkflowTypeInput struct {
	Domain       string          `json:"domain"`
	WorkflowType workflowTypeRef `json:"workflowType"`
}

type deleteWorkflowTypeOutput struct{}

func (h *Handler) handleDeleteWorkflowType(
	_ context.Context,
	in *handleDeleteWorkflowTypeInput,
) (*deleteWorkflowTypeOutput, error) {
	if err := h.Backend.DeleteWorkflowType(in.Domain, in.WorkflowType.Name, in.WorkflowType.Version); err != nil {
		return nil, err
	}

	return &deleteWorkflowTypeOutput{}, nil
}

type handleDescribeActivityTypeInput struct {
	Domain       string          `json:"domain"`
	ActivityType activityTypeRef `json:"activityType"`
}

type describeActivityTypeOutput struct {
	Configuration typeConfigOutput `json:"configuration"`
	TypeInfo      typeInfoOutput   `json:"typeInfo"`
}

func (h *Handler) handleDescribeActivityType(
	_ context.Context,
	in *handleDescribeActivityTypeInput,
) (*describeActivityTypeOutput, error) {
	at, err := h.Backend.DescribeActivityType(in.Domain, in.ActivityType.Name, in.ActivityType.Version)
	if err != nil {
		return nil, err
	}

	return &describeActivityTypeOutput{
		TypeInfo: typeInfoOutput{
			ActivityType: &activityTypeRef{Name: at.Name, Version: at.Version},
			Status:       at.Status,
			Description:  at.Description,
		},
	}, nil
}

type handleDeprecateActivityTypeInput struct {
	Domain       string          `json:"domain"`
	ActivityType activityTypeRef `json:"activityType"`
}

type deprecateActivityTypeOutput struct{}

func (h *Handler) handleDeprecateActivityType(
	_ context.Context,
	in *handleDeprecateActivityTypeInput,
) (*deprecateActivityTypeOutput, error) {
	if err := h.Backend.DeprecateActivityType(in.Domain, in.ActivityType.Name, in.ActivityType.Version); err != nil {
		return nil, err
	}

	return &deprecateActivityTypeOutput{}, nil
}

type handleDeleteActivityTypeInput struct {
	Domain       string          `json:"domain"`
	ActivityType activityTypeRef `json:"activityType"`
}

type deleteActivityTypeOutput struct{}

func (h *Handler) handleDeleteActivityType(
	_ context.Context,
	in *handleDeleteActivityTypeInput,
) (*deleteActivityTypeOutput, error) {
	if err := h.Backend.DeleteActivityType(in.Domain, in.ActivityType.Name, in.ActivityType.Version); err != nil {
		return nil, err
	}

	return &deleteActivityTypeOutput{}, nil
}

type taskListRef struct {
	Name string `json:"name"`
}

type workflowExecutionCountOutput struct {
	Count     int  `json:"count"`
	Truncated bool `json:"truncated"`
}

type handleCountOpenWorkflowExecutionsInput struct {
	Domain string `json:"domain"`
}

func (h *Handler) handleCountOpenWorkflowExecutions(
	_ context.Context,
	in *handleCountOpenWorkflowExecutionsInput,
) (*workflowExecutionCountOutput, error) {
	count := h.Backend.CountOpenWorkflowExecutions(in.Domain)

	return &workflowExecutionCountOutput{Count: count}, nil
}

type handleCountClosedWorkflowExecutionsInput struct {
	Domain string `json:"domain"`
}

func (h *Handler) handleCountClosedWorkflowExecutions(
	_ context.Context,
	in *handleCountClosedWorkflowExecutionsInput,
) (*workflowExecutionCountOutput, error) {
	count := h.Backend.CountClosedWorkflowExecutions(in.Domain)

	return &workflowExecutionCountOutput{Count: count}, nil
}

type pendingTaskCountOutput struct {
	Count     int  `json:"count"`
	Truncated bool `json:"truncated"`
}

type handleCountPendingActivityTasksInput struct {
	Domain   string      `json:"domain"`
	TaskList taskListRef `json:"taskList"`
}

func (h *Handler) handleCountPendingActivityTasks(
	_ context.Context,
	in *handleCountPendingActivityTasksInput,
) (*pendingTaskCountOutput, error) {
	count := h.Backend.CountPendingActivityTasks(in.TaskList.Name)

	return &pendingTaskCountOutput{Count: count}, nil
}

type handleCountPendingDecisionTasksInput struct {
	Domain   string      `json:"domain"`
	TaskList taskListRef `json:"taskList"`
}

func (h *Handler) handleCountPendingDecisionTasks(
	_ context.Context,
	in *handleCountPendingDecisionTasksInput,
) (*pendingTaskCountOutput, error) {
	count := h.Backend.CountPendingDecisionTasks(in.TaskList.Name)

	return &pendingTaskCountOutput{Count: count}, nil
}

const defaultSWFMaxPageSize = 1000

// applyPageTokenSlice applies nextPageToken-based pagination using the shared
// pkgs/page opaque token format.
func applyPageTokenSlice[T any](items []T, nextPageToken string, maximumPageSize int) ([]T, string) {
	p := page.New(items, nextPageToken, maximumPageSize, defaultSWFMaxPageSize)

	return p.Data, p.Next
}
