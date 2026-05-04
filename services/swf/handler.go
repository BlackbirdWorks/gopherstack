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
			"SWF", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

func (h *Handler) buildOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"RegisterDomain":                  service.WrapOp(h.handleRegisterDomain),
		"DescribeDomain":                  service.WrapOp(h.handleDescribeDomain),
		"ListDomains":                     service.WrapOp(h.handleListDomains),
		"DeprecateDomain":                 service.WrapOp(h.handleDeprecateDomain),
		"UndeprecateDomain":               service.WrapOp(h.handleUndeprecateDomain),
		"RegisterWorkflowType":            service.WrapOp(h.handleRegisterWorkflowType),
		"ListWorkflowTypes":               service.WrapOp(h.handleListWorkflowTypes),
		"DescribeWorkflowType":            service.WrapOp(h.handleDescribeWorkflowType),
		"DeprecateWorkflowType":           service.WrapOp(h.handleDeprecateWorkflowType),
		"UndeprecateWorkflowType":         service.WrapOp(h.handleUndeprecateWorkflowType),
		"DeleteWorkflowType":              service.WrapOp(h.handleDeleteWorkflowType),
		"RegisterActivityType":            service.WrapOp(h.handleRegisterActivityType),
		"ListActivityTypes":               service.WrapOp(h.handleListActivityTypes),
		"DescribeActivityType":            service.WrapOp(h.handleDescribeActivityType),
		"DeprecateActivityType":           service.WrapOp(h.handleDeprecateActivityType),
		"UndeprecateActivityType":         service.WrapOp(h.handleUndeprecateActivityType),
		"DeleteActivityType":              service.WrapOp(h.handleDeleteActivityType),
		"CountOpenWorkflowExecutions":     service.WrapOp(h.handleCountOpenWorkflowExecutions),
		"CountClosedWorkflowExecutions":   service.WrapOp(h.handleCountClosedWorkflowExecutions),
		"CountPendingActivityTasks":       service.WrapOp(h.handleCountPendingActivityTasks),
		"CountPendingDecisionTasks":       service.WrapOp(h.handleCountPendingDecisionTasks),
		"StartWorkflowExecution":          service.WrapOp(h.handleStartWorkflowExecution),
		"TerminateWorkflowExecution":      service.WrapOp(h.handleTerminateWorkflowExecution),
		"DescribeWorkflowExecution":       service.WrapOp(h.handleDescribeWorkflowExecution),
		"GetWorkflowExecutionHistory":     service.WrapOp(h.handleGetWorkflowExecutionHistory),
		"ListOpenWorkflowExecutions":      service.WrapOp(h.handleListOpenWorkflowExecutions),
		"ListClosedWorkflowExecutions":    service.WrapOp(h.handleListClosedWorkflowExecutions),
		"ListTagsForResource":             service.WrapOp(h.handleListTagsForResource),
		"TagResource":                     service.WrapOp(h.handleTagResource),
		"UntagResource":                   service.WrapOp(h.handleUntagResource),
		"PollForActivityTask":             service.WrapOp(h.handlePollForActivityTask),
		"PollForDecisionTask":             service.WrapOp(h.handlePollForDecisionTask),
		"RecordActivityTaskHeartbeat":     service.WrapOp(h.handleRecordActivityTaskHeartbeat),
		"RequestCancelWorkflowExecution":  service.WrapOp(h.handleRequestCancelWorkflowExecution),
		"RespondActivityTaskCanceled":     service.WrapOp(h.handleRespondActivityTaskCanceled),
		"RespondActivityTaskCompleted":    service.WrapOp(h.handleRespondActivityTaskCompleted),
		"RespondActivityTaskFailed":       service.WrapOp(h.handleRespondActivityTaskFailed),
		"RespondDecisionTaskCompleted":    service.WrapOp(h.handleRespondDecisionTaskCompleted),
		"SignalWorkflowExecution":         service.WrapOp(h.handleSignalWorkflowExecution),
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
	case errors.Is(err, ErrValidation):
		code = http.StatusBadRequest
		errType = "ValidationException"
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

type handleUndeprecateDomainInput struct {
	Name string `json:"name"`
}

type undeprecateDomainOutput struct{}

func (h *Handler) handleUndeprecateDomain(
	_ context.Context,
	in *handleUndeprecateDomainInput,
) (*undeprecateDomainOutput, error) {
	if err := h.Backend.UndeprecateDomain(in.Name); err != nil {
		return nil, err
	}

	return &undeprecateDomainOutput{}, nil
}

type handleRegisterWorkflowTypeInput struct {
	Domain      string `json:"domain"`
	Name        string `json:"name"`
	Version     string `json:"version"`
	Description string `json:"description,omitempty"`
}

func (h *Handler) handleRegisterWorkflowType(
	_ context.Context,
	in *handleRegisterWorkflowTypeInput,
) (*registerWorkflowTypeOutput, error) {
	if err := h.Backend.RegisterWorkflowType(in.Domain, in.Name, in.Version, in.Description); err != nil {
		return nil, err
	}

	return &registerWorkflowTypeOutput{}, nil
}

type handleListWorkflowTypesInput struct {
	Domain             string `json:"domain"`
	RegistrationStatus string `json:"registrationStatus,omitempty"`
	NextPageToken      string `json:"nextPageToken,omitempty"`
	MaximumPageSize    int    `json:"maximumPageSize,omitempty"`
}

func (h *Handler) handleListWorkflowTypes(
	_ context.Context,
	in *handleListWorkflowTypesInput,
) (*listWorkflowTypesOutput, error) {
	wts := h.Backend.ListWorkflowTypes(in.Domain, in.RegistrationStatus)

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

type handleTerminateWorkflowExecutionInput struct {
	Domain     string `json:"domain"`
	WorkflowID string `json:"workflowId"`
	Reason     string `json:"reason,omitempty"`
}

type terminateWorkflowExecutionOutput struct{}

func (h *Handler) handleTerminateWorkflowExecution(
	_ context.Context,
	in *handleTerminateWorkflowExecutionInput,
) (*terminateWorkflowExecutionOutput, error) {
	if err := h.Backend.TerminateWorkflowExecution(in.Domain, in.WorkflowID); err != nil {
		return nil, err
	}

	return &terminateWorkflowExecutionOutput{}, nil
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

type handleUndeprecateWorkflowTypeInput struct {
	Domain       string          `json:"domain"`
	WorkflowType workflowTypeRef `json:"workflowType"`
}

type undeprecateWorkflowTypeOutput struct{}

func (h *Handler) handleUndeprecateWorkflowType(
	_ context.Context,
	in *handleUndeprecateWorkflowTypeInput,
) (*undeprecateWorkflowTypeOutput, error) {
	if err := h.Backend.UndeprecateWorkflowType(in.Domain, in.WorkflowType.Name, in.WorkflowType.Version); err != nil {
		return nil, err
	}

	return &undeprecateWorkflowTypeOutput{}, nil
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

type registerActivityTypeOutput struct{}

type handleRegisterActivityTypeInput struct {
	Domain      string `json:"domain"`
	Name        string `json:"name"`
	Version     string `json:"version"`
	Description string `json:"description,omitempty"`
}

func (h *Handler) handleRegisterActivityType(
	_ context.Context,
	in *handleRegisterActivityTypeInput,
) (*registerActivityTypeOutput, error) {
	if err := h.Backend.RegisterActivityType(in.Domain, in.Name, in.Version, in.Description); err != nil {
		return nil, err
	}

	return &registerActivityTypeOutput{}, nil
}

type listActivityTypesOutput struct {
	NextPageToken string         `json:"nextPageToken,omitempty"`
	TypeInfos     []ActivityType `json:"typeInfos"`
}

type handleListActivityTypesInput struct {
	Domain             string `json:"domain"`
	RegistrationStatus string `json:"registrationStatus,omitempty"`
	NextPageToken      string `json:"nextPageToken,omitempty"`
	MaximumPageSize    int    `json:"maximumPageSize,omitempty"`
}

func (h *Handler) handleListActivityTypes(
	_ context.Context,
	in *handleListActivityTypesInput,
) (*listActivityTypesOutput, error) {
	ats := h.Backend.ListActivityTypes(in.Domain, in.RegistrationStatus)

	sort.Slice(ats, func(i, j int) bool { return ats[i].Name < ats[j].Name })

	ats, nextPageToken := applyPageTokenSlice(ats, in.NextPageToken, in.MaximumPageSize)

	return &listActivityTypesOutput{TypeInfos: ats, NextPageToken: nextPageToken}, nil
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

type handleUndeprecateActivityTypeInput struct {
	Domain       string          `json:"domain"`
	ActivityType activityTypeRef `json:"activityType"`
}

type undeprecateActivityTypeOutput struct{}

func (h *Handler) handleUndeprecateActivityType(
	_ context.Context,
	in *handleUndeprecateActivityTypeInput,
) (*undeprecateActivityTypeOutput, error) {
	if err := h.Backend.UndeprecateActivityType(in.Domain, in.ActivityType.Name, in.ActivityType.Version); err != nil {
		return nil, err
	}

	return &undeprecateActivityTypeOutput{}, nil
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

// --- GetWorkflowExecutionHistory ---

type handleGetWorkflowExecutionHistoryInput struct {
	Domain    string               `json:"domain"`
	Execution workflowExecutionRef `json:"execution"`
	// MaximumPageSize and ReverseOrder ignored in this emulator.
}

type getWorkflowExecutionHistoryOutput struct {
	Events        []HistoryEvent `json:"events"`
	NextPageToken string         `json:"nextPageToken,omitempty"`
}

func (h *Handler) handleGetWorkflowExecutionHistory(
	_ context.Context,
	in *handleGetWorkflowExecutionHistoryInput,
) (*getWorkflowExecutionHistoryOutput, error) {
	events := h.Backend.GetWorkflowExecutionHistory(in.Domain, in.Execution.WorkflowID)

	return &getWorkflowExecutionHistoryOutput{Events: events}, nil
}

// --- ListOpenWorkflowExecutions ---

type executionFilter struct {
	WorkflowID string `json:"workflowId,omitempty"`
}

type handleListOpenWorkflowExecutionsInput struct {
	Domain          string          `json:"domain"`
	ExecutionFilter *executionFilter `json:"executionFilter,omitempty"`
	NextPageToken   string          `json:"nextPageToken,omitempty"`
	MaximumPageSize int             `json:"maximumPageSize,omitempty"`
}

type listWorkflowExecutionsOutput struct {
	ExecutionInfos []WorkflowExecution `json:"executionInfos"`
	NextPageToken  string              `json:"nextPageToken,omitempty"`
}

func (h *Handler) handleListOpenWorkflowExecutions(
	_ context.Context,
	in *handleListOpenWorkflowExecutionsInput,
) (*listWorkflowExecutionsOutput, error) {
	execs := h.Backend.ListOpenWorkflowExecutions(in.Domain)

	execs, nextPageToken := applyPageTokenSlice(execs, in.NextPageToken, in.MaximumPageSize)

	return &listWorkflowExecutionsOutput{ExecutionInfos: execs, NextPageToken: nextPageToken}, nil
}

// --- ListClosedWorkflowExecutions ---

type handleListClosedWorkflowExecutionsInput struct {
	Domain          string          `json:"domain"`
	ExecutionFilter *executionFilter `json:"executionFilter,omitempty"`
	NextPageToken   string          `json:"nextPageToken,omitempty"`
	MaximumPageSize int             `json:"maximumPageSize,omitempty"`
}

func (h *Handler) handleListClosedWorkflowExecutions(
	_ context.Context,
	in *handleListClosedWorkflowExecutionsInput,
) (*listWorkflowExecutionsOutput, error) {
	execs := h.Backend.ListClosedWorkflowExecutions(in.Domain)

	execs, nextPageToken := applyPageTokenSlice(execs, in.NextPageToken, in.MaximumPageSize)

	return &listWorkflowExecutionsOutput{ExecutionInfos: execs, NextPageToken: nextPageToken}, nil
}

// --- ListTagsForResource ---

type handleListTagsForResourceInput struct {
	ResourceArn string `json:"resourceArn"`
}

type resourceTag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type listTagsForResourceOutput struct {
	Tags []resourceTag `json:"tags"`
}

func (h *Handler) handleListTagsForResource(
	_ context.Context,
	in *handleListTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	tagMap := h.Backend.ListTagsForResource(in.ResourceArn)

	tags := make([]resourceTag, 0, len(tagMap))
	for k, v := range tagMap {
		tags = append(tags, resourceTag{Key: k, Value: v})
	}

	return &listTagsForResourceOutput{Tags: tags}, nil
}

// --- TagResource ---

type handleTagResourceInput struct {
	ResourceArn string        `json:"resourceArn"`
	Tags        []resourceTag `json:"tags"`
}

type tagResourceOutput struct{}

func (h *Handler) handleTagResource(
	_ context.Context,
	in *handleTagResourceInput,
) (*tagResourceOutput, error) {
	tagMap := make(map[string]string, len(in.Tags))
	for _, t := range in.Tags {
		tagMap[t.Key] = t.Value
	}

	if err := h.Backend.TagResource(in.ResourceArn, tagMap); err != nil {
		return nil, err
	}

	return &tagResourceOutput{}, nil
}

// --- UntagResource ---

type handleUntagResourceInput struct {
	ResourceArn string   `json:"resourceArn"`
	TagKeys     []string `json:"tagKeys"`
}

type untagResourceOutput struct{}

func (h *Handler) handleUntagResource(
	_ context.Context,
	in *handleUntagResourceInput,
) (*untagResourceOutput, error) {
	if err := h.Backend.UntagResource(in.ResourceArn, in.TagKeys); err != nil {
		return nil, err
	}

	return &untagResourceOutput{}, nil
}

// --- PollForActivityTask ---

type handlePollForActivityTaskInput struct {
	Domain   string      `json:"domain"`
	TaskList taskListRef `json:"taskList"`
}

func (h *Handler) handlePollForActivityTask(
	_ context.Context,
	in *handlePollForActivityTaskInput,
) (*ActivityTask, error) {
	task := h.Backend.PollForActivityTask(in.Domain, in.TaskList.Name)
	if task == nil {
		return &ActivityTask{}, nil
	}

	return task, nil
}

// --- PollForDecisionTask ---

type handlePollForDecisionTaskInput struct {
	Domain          string      `json:"domain"`
	TaskList        taskListRef `json:"taskList"`
	NextPageToken   string      `json:"nextPageToken,omitempty"`
	MaximumPageSize int         `json:"maximumPageSize,omitempty"`
}

func (h *Handler) handlePollForDecisionTask(
	_ context.Context,
	in *handlePollForDecisionTaskInput,
) (*DecisionTask, error) {
	task := h.Backend.PollForDecisionTask(in.Domain, in.TaskList.Name)
	if task == nil {
		return &DecisionTask{}, nil
	}

	return task, nil
}

// --- RecordActivityTaskHeartbeat ---

type handleRecordActivityTaskHeartbeatInput struct {
	TaskToken string `json:"taskToken"`
}

type recordActivityTaskHeartbeatOutput struct {
	CancelRequested bool `json:"cancelRequested"`
}

func (h *Handler) handleRecordActivityTaskHeartbeat(
	_ context.Context,
	in *handleRecordActivityTaskHeartbeatInput,
) (*recordActivityTaskHeartbeatOutput, error) {
	cancelRequested := h.Backend.RecordActivityTaskHeartbeat(in.TaskToken)

	return &recordActivityTaskHeartbeatOutput{CancelRequested: cancelRequested}, nil
}

// --- RequestCancelWorkflowExecution ---

type handleRequestCancelWorkflowExecutionInput struct {
	Domain     string `json:"domain"`
	WorkflowID string `json:"workflowId"`
	RunID      string `json:"runId,omitempty"`
}

type requestCancelWorkflowExecutionOutput struct{}

func (h *Handler) handleRequestCancelWorkflowExecution(
	_ context.Context,
	in *handleRequestCancelWorkflowExecutionInput,
) (*requestCancelWorkflowExecutionOutput, error) {
	if err := h.Backend.RequestCancelWorkflowExecution(in.Domain, in.WorkflowID); err != nil {
		return nil, err
	}

	return &requestCancelWorkflowExecutionOutput{}, nil
}

// --- RespondActivityTaskCanceled ---

type handleRespondActivityTaskCanceledInput struct {
	TaskToken string `json:"taskToken"`
	Details   string `json:"details,omitempty"`
}

type respondActivityTaskCanceledOutput struct{}

func (h *Handler) handleRespondActivityTaskCanceled(
	_ context.Context,
	in *handleRespondActivityTaskCanceledInput,
) (*respondActivityTaskCanceledOutput, error) {
	if err := h.Backend.RespondActivityTaskCanceled(in.TaskToken); err != nil {
		return nil, err
	}

	return &respondActivityTaskCanceledOutput{}, nil
}

// --- RespondActivityTaskCompleted ---

type handleRespondActivityTaskCompletedInput struct {
	TaskToken string `json:"taskToken"`
	Result    string `json:"result,omitempty"`
}

type respondActivityTaskCompletedOutput struct{}

func (h *Handler) handleRespondActivityTaskCompleted(
	_ context.Context,
	in *handleRespondActivityTaskCompletedInput,
) (*respondActivityTaskCompletedOutput, error) {
	if err := h.Backend.RespondActivityTaskCompleted(in.TaskToken); err != nil {
		return nil, err
	}

	return &respondActivityTaskCompletedOutput{}, nil
}

// --- RespondActivityTaskFailed ---

type handleRespondActivityTaskFailedInput struct {
	TaskToken string `json:"taskToken"`
	Reason    string `json:"reason,omitempty"`
	Details   string `json:"details,omitempty"`
}

type respondActivityTaskFailedOutput struct{}

func (h *Handler) handleRespondActivityTaskFailed(
	_ context.Context,
	in *handleRespondActivityTaskFailedInput,
) (*respondActivityTaskFailedOutput, error) {
	if err := h.Backend.RespondActivityTaskFailed(in.TaskToken); err != nil {
		return nil, err
	}

	return &respondActivityTaskFailedOutput{}, nil
}

// --- RespondDecisionTaskCompleted ---

type handleRespondDecisionTaskCompletedInput struct {
	TaskToken string `json:"taskToken"`
	// Decisions slice ignored in this emulator.
}

type respondDecisionTaskCompletedOutput struct{}

func (h *Handler) handleRespondDecisionTaskCompleted(
	_ context.Context,
	in *handleRespondDecisionTaskCompletedInput,
) (*respondDecisionTaskCompletedOutput, error) {
	if err := h.Backend.RespondDecisionTaskCompleted(in.TaskToken); err != nil {
		return nil, err
	}

	return &respondDecisionTaskCompletedOutput{}, nil
}

// --- SignalWorkflowExecution ---

type handleSignalWorkflowExecutionInput struct {
	Domain     string `json:"domain"`
	WorkflowID string `json:"workflowId"`
	RunID      string `json:"runId,omitempty"`
	SignalName string `json:"signalName"`
	Input      string `json:"input,omitempty"`
}

type signalWorkflowExecutionOutput struct{}

func (h *Handler) handleSignalWorkflowExecution(
	_ context.Context,
	in *handleSignalWorkflowExecutionInput,
) (*signalWorkflowExecutionOutput, error) {
	if err := h.Backend.SignalWorkflowExecution(in.Domain, in.WorkflowID, in.SignalName, in.Input); err != nil {
		return nil, err
	}

	return &signalWorkflowExecutionOutput{}, nil
}
