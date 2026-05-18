package swf

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strings"
	"time"

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

// --- RegisterDomain ---

type registerDomainOutput struct{}

type resourceTagInput struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type handleRegisterDomainInput struct {
	Name                                   string             `json:"name"`
	Description                            string             `json:"description"`
	WorkflowExecutionRetentionPeriodInDays string             `json:"workflowExecutionRetentionPeriodInDays"`
	Tags                                   []resourceTagInput `json:"tags,omitempty"`
}

func (h *Handler) handleRegisterDomain(
	_ context.Context,
	in *handleRegisterDomainInput,
) (*registerDomainOutput, error) {
	retention := in.WorkflowExecutionRetentionPeriodInDays
	if err := h.Backend.RegisterDomain(in.Name, in.Description, retention); err != nil {
		return nil, err
	}
	// Apply tags via TagResource if provided.
	if len(in.Tags) > 0 {
		arn := domainARN(config.DefaultRegion, defaultAccountID, in.Name)
		tagMap := make(map[string]string, len(in.Tags))
		for _, t := range in.Tags {
			tagMap[t.Key] = t.Value
		}
		if err := h.Backend.TagResource(arn, tagMap); err != nil {
			return nil, err
		}
	}

	return &registerDomainOutput{}, nil
}

// --- DescribeDomain ---

type domainConfigOutput struct {
	WorkflowExecutionRetentionPeriodInDays string `json:"workflowExecutionRetentionPeriodInDays"`
}

type describeDomainOutput struct {
	DomainInfo    *Domain            `json:"domainInfo"`
	Configuration domainConfigOutput `json:"configuration"`
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
	retention := d.WorkflowExecutionRetentionPeriodInDays
	if retention == "" {
		retention = retentionNone
	}

	return &describeDomainOutput{
		DomainInfo:    d,
		Configuration: domainConfigOutput{WorkflowExecutionRetentionPeriodInDays: retention},
	}, nil
}

// --- ListDomains ---

type listDomainsOutput struct {
	NextPageToken string   `json:"nextPageToken,omitempty"`
	DomainInfos   []Domain `json:"domainInfos"`
}

type handleListDomainsInput struct {
	RegistrationStatus string `json:"registrationStatus"`
	NextPageToken      string `json:"nextPageToken,omitempty"`
	MaximumPageSize    int    `json:"maximumPageSize,omitempty"`
}

func (h *Handler) handleListDomains(_ context.Context, in *handleListDomainsInput) (*listDomainsOutput, error) {
	domains, err := h.Backend.ListDomains(in.RegistrationStatus)
	if err != nil {
		return nil, err
	}
	sort.Slice(domains, func(i, j int) bool { return domains[i].Name < domains[j].Name })
	domains, nextPageToken := applyPageTokenSlice(domains, in.NextPageToken, in.MaximumPageSize)

	return &listDomainsOutput{DomainInfos: domains, NextPageToken: nextPageToken}, nil
}

// --- DeprecateDomain ---

type deprecateDomainOutput struct{}

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

// --- UndeprecateDomain ---

type undeprecateDomainOutput struct{}

type handleUndeprecateDomainInput struct {
	Name string `json:"name"`
}

func (h *Handler) handleUndeprecateDomain(
	_ context.Context,
	in *handleUndeprecateDomainInput,
) (*undeprecateDomainOutput, error) {
	if err := h.Backend.UndeprecateDomain(in.Name); err != nil {
		return nil, err
	}

	return &undeprecateDomainOutput{}, nil
}

// --- RegisterWorkflowType ---

type registerWorkflowTypeOutput struct{}

type taskListInput struct {
	Name string `json:"name"`
}

type handleRegisterWorkflowTypeInput struct {
	Domain                              string         `json:"domain"`
	Name                                string         `json:"name"`
	Version                             string         `json:"version"`
	Description                         string         `json:"description,omitempty"`
	DefaultTaskList                     *taskListInput `json:"defaultTaskList,omitempty"`
	DefaultTaskPriority                 string         `json:"defaultTaskPriority,omitempty"`
	DefaultTaskStartToCloseTimeout      string         `json:"defaultTaskStartToCloseTimeout,omitempty"`
	DefaultExecutionStartToCloseTimeout string         `json:"defaultExecutionStartToCloseTimeout,omitempty"`
	DefaultChildPolicy                  string         `json:"defaultChildPolicy,omitempty"`
	DefaultLambdaRole                   string         `json:"defaultLambdaRole,omitempty"`
}

func (h *Handler) handleRegisterWorkflowType(
	_ context.Context,
	in *handleRegisterWorkflowTypeInput,
) (*registerWorkflowTypeOutput, error) {
	defaults := WorkflowTypeDefaults{
		DefaultTaskPriority:                 in.DefaultTaskPriority,
		DefaultTaskStartToCloseTimeout:      in.DefaultTaskStartToCloseTimeout,
		DefaultExecutionStartToCloseTimeout: in.DefaultExecutionStartToCloseTimeout,
		DefaultChildPolicy:                  in.DefaultChildPolicy,
		DefaultLambdaRole:                   in.DefaultLambdaRole,
	}
	if in.DefaultTaskList != nil {
		defaults.DefaultTaskList = in.DefaultTaskList.Name
	}
	if err := h.Backend.RegisterWorkflowType(in.Domain, in.Name, in.Version, in.Description, defaults); err != nil {
		return nil, err
	}

	return &registerWorkflowTypeOutput{}, nil
}

// --- ListWorkflowTypes ---

type workflowTypeRef struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

type workflowTypeInfoOutput struct {
	WorkflowType *workflowTypeRef `json:"workflowType"`
	Status       string           `json:"status"`
	Description  string           `json:"description,omitempty"`
	CreationDate float64          `json:"creationDate,omitempty"`
}

type listWorkflowTypesOutput struct {
	NextPageToken string                   `json:"nextPageToken,omitempty"`
	TypeInfos     []workflowTypeInfoOutput `json:"typeInfos"`
}

type handleListWorkflowTypesInput struct {
	Domain             string `json:"domain"`
	RegistrationStatus string `json:"registrationStatus,omitempty"`
	NextPageToken      string `json:"nextPageToken,omitempty"`
	MaximumPageSize    int    `json:"maximumPageSize,omitempty"`
}

//nolint:dupl // WorkflowType and ActivityType have parallel list structure
func (h *Handler) handleListWorkflowTypes(
	_ context.Context,
	in *handleListWorkflowTypesInput,
) (*listWorkflowTypesOutput, error) {
	wts, err := h.Backend.ListWorkflowTypes(in.Domain, in.RegistrationStatus)
	if err != nil {
		return nil, err
	}
	infos := make([]workflowTypeInfoOutput, len(wts))
	for i, wt := range wts {
		infos[i] = workflowTypeInfoOutput{
			WorkflowType: &workflowTypeRef{Name: wt.Name, Version: wt.Version},
			Status:       wt.Status,
			Description:  wt.Description,
			CreationDate: wt.CreationDate,
		}
	}
	sort.Slice(infos, func(i, j int) bool {
		return infos[i].WorkflowType.Name < infos[j].WorkflowType.Name
	})
	infos, nextPageToken := applyPageTokenSlice(infos, in.NextPageToken, in.MaximumPageSize)

	return &listWorkflowTypesOutput{TypeInfos: infos, NextPageToken: nextPageToken}, nil
}

// --- DescribeWorkflowType ---

type workflowTypeConfigOutput struct {
	DefaultTaskList                     *taskListRef `json:"defaultTaskList,omitempty"`
	DefaultTaskPriority                 string       `json:"defaultTaskPriority,omitempty"`
	DefaultTaskStartToCloseTimeout      string       `json:"defaultTaskStartToCloseTimeout,omitempty"`
	DefaultExecutionStartToCloseTimeout string       `json:"defaultExecutionStartToCloseTimeout,omitempty"`
	DefaultChildPolicy                  string       `json:"defaultChildPolicy,omitempty"`
	DefaultLambdaRole                   string       `json:"defaultLambdaRole,omitempty"`
}

type taskListRef struct {
	Name string `json:"name"`
}

type describeWorkflowTypeOutput struct {
	Configuration workflowTypeConfigOutput `json:"configuration"`
	TypeInfo      workflowTypeInfoOutput   `json:"typeInfo"`
}

type handleDescribeWorkflowTypeInput struct {
	Domain       string          `json:"domain"`
	WorkflowType workflowTypeRef `json:"workflowType"`
}

//nolint:dupl // mirrors ActivityType describe structure
func (h *Handler) handleDescribeWorkflowType(
	_ context.Context,
	in *handleDescribeWorkflowTypeInput,
) (*describeWorkflowTypeOutput, error) {
	wt, err := h.Backend.DescribeWorkflowType(in.Domain, in.WorkflowType.Name, in.WorkflowType.Version)
	if err != nil {
		return nil, err
	}
	cfg := workflowTypeConfigOutput{
		DefaultTaskPriority:                 wt.Defaults.DefaultTaskPriority,
		DefaultTaskStartToCloseTimeout:      wt.Defaults.DefaultTaskStartToCloseTimeout,
		DefaultExecutionStartToCloseTimeout: wt.Defaults.DefaultExecutionStartToCloseTimeout,
		DefaultChildPolicy:                  wt.Defaults.DefaultChildPolicy,
		DefaultLambdaRole:                   wt.Defaults.DefaultLambdaRole,
	}
	if wt.Defaults.DefaultTaskList != "" {
		cfg.DefaultTaskList = &taskListRef{Name: wt.Defaults.DefaultTaskList}
	}

	return &describeWorkflowTypeOutput{
		TypeInfo: workflowTypeInfoOutput{
			WorkflowType: &workflowTypeRef{Name: wt.Name, Version: wt.Version},
			Status:       wt.Status,
			Description:  wt.Description,
			CreationDate: wt.CreationDate,
		},
		Configuration: cfg,
	}, nil
}

// --- DeprecateWorkflowType ---

type deprecateWorkflowTypeOutput struct{}

type handleDeprecateWorkflowTypeInput struct {
	Domain       string          `json:"domain"`
	WorkflowType workflowTypeRef `json:"workflowType"`
}

func (h *Handler) handleDeprecateWorkflowType(
	_ context.Context,
	in *handleDeprecateWorkflowTypeInput,
) (*deprecateWorkflowTypeOutput, error) {
	if err := h.Backend.DeprecateWorkflowType(in.Domain, in.WorkflowType.Name, in.WorkflowType.Version); err != nil {
		return nil, err
	}

	return &deprecateWorkflowTypeOutput{}, nil
}

// --- UndeprecateWorkflowType ---

type undeprecateWorkflowTypeOutput struct{}

type handleUndeprecateWorkflowTypeInput struct {
	Domain       string          `json:"domain"`
	WorkflowType workflowTypeRef `json:"workflowType"`
}

func (h *Handler) handleUndeprecateWorkflowType(
	_ context.Context,
	in *handleUndeprecateWorkflowTypeInput,
) (*undeprecateWorkflowTypeOutput, error) {
	if err := h.Backend.UndeprecateWorkflowType(in.Domain, in.WorkflowType.Name, in.WorkflowType.Version); err != nil {
		return nil, err
	}

	return &undeprecateWorkflowTypeOutput{}, nil
}

// --- DeleteWorkflowType ---

type deleteWorkflowTypeOutput struct{}

type handleDeleteWorkflowTypeInput struct {
	Domain       string          `json:"domain"`
	WorkflowType workflowTypeRef `json:"workflowType"`
}

func (h *Handler) handleDeleteWorkflowType(
	_ context.Context,
	in *handleDeleteWorkflowTypeInput,
) (*deleteWorkflowTypeOutput, error) {
	if err := h.Backend.DeleteWorkflowType(in.Domain, in.WorkflowType.Name, in.WorkflowType.Version); err != nil {
		return nil, err
	}

	return &deleteWorkflowTypeOutput{}, nil
}

// --- RegisterActivityType ---

type registerActivityTypeOutput struct{}

type handleRegisterActivityTypeInput struct {
	Domain                            string         `json:"domain"`
	Name                              string         `json:"name"`
	Version                           string         `json:"version"`
	Description                       string         `json:"description,omitempty"`
	DefaultTaskList                   *taskListInput `json:"defaultTaskList,omitempty"`
	DefaultTaskPriority               string         `json:"defaultTaskPriority,omitempty"`
	DefaultTaskHeartbeatTimeout       string         `json:"defaultTaskHeartbeatTimeout,omitempty"`
	DefaultTaskScheduleToCloseTimeout string         `json:"defaultTaskScheduleToCloseTimeout,omitempty"`
	DefaultTaskScheduleToStartTimeout string         `json:"defaultTaskScheduleToStartTimeout,omitempty"`
	DefaultTaskStartToCloseTimeout    string         `json:"defaultTaskStartToCloseTimeout,omitempty"`
}

func (h *Handler) handleRegisterActivityType(
	_ context.Context,
	in *handleRegisterActivityTypeInput,
) (*registerActivityTypeOutput, error) {
	defaults := ActivityTypeDefaults{
		DefaultTaskPriority:               in.DefaultTaskPriority,
		DefaultTaskHeartbeatTimeout:       in.DefaultTaskHeartbeatTimeout,
		DefaultTaskScheduleToCloseTimeout: in.DefaultTaskScheduleToCloseTimeout,
		DefaultTaskScheduleToStartTimeout: in.DefaultTaskScheduleToStartTimeout,
		DefaultTaskStartToCloseTimeout:    in.DefaultTaskStartToCloseTimeout,
	}
	if in.DefaultTaskList != nil {
		defaults.DefaultTaskList = in.DefaultTaskList.Name
	}
	if err := h.Backend.RegisterActivityType(in.Domain, in.Name, in.Version, in.Description, defaults); err != nil {
		return nil, err
	}

	return &registerActivityTypeOutput{}, nil
}

// --- ListActivityTypes ---

type activityTypeRef struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

type activityTypeInfoOutput struct {
	ActivityType *activityTypeRef `json:"activityType"`
	Status       string           `json:"status"`
	Description  string           `json:"description,omitempty"`
	CreationDate float64          `json:"creationDate,omitempty"`
}

type listActivityTypesOutput struct {
	NextPageToken string                   `json:"nextPageToken,omitempty"`
	TypeInfos     []activityTypeInfoOutput `json:"typeInfos"`
}

type handleListActivityTypesInput struct {
	Domain             string `json:"domain"`
	RegistrationStatus string `json:"registrationStatus,omitempty"`
	NextPageToken      string `json:"nextPageToken,omitempty"`
	MaximumPageSize    int    `json:"maximumPageSize,omitempty"`
}

//nolint:dupl // ActivityType list mirrors WorkflowType list structure
func (h *Handler) handleListActivityTypes(
	_ context.Context,
	in *handleListActivityTypesInput,
) (*listActivityTypesOutput, error) {
	ats, err := h.Backend.ListActivityTypes(in.Domain, in.RegistrationStatus)
	if err != nil {
		return nil, err
	}
	infos := make([]activityTypeInfoOutput, len(ats))
	for i, at := range ats {
		infos[i] = activityTypeInfoOutput{
			ActivityType: &activityTypeRef{Name: at.Name, Version: at.Version},
			Status:       at.Status,
			Description:  at.Description,
			CreationDate: at.CreationDate,
		}
	}
	sort.Slice(infos, func(i, j int) bool {
		return infos[i].ActivityType.Name < infos[j].ActivityType.Name
	})
	infos, nextPageToken := applyPageTokenSlice(infos, in.NextPageToken, in.MaximumPageSize)

	return &listActivityTypesOutput{TypeInfos: infos, NextPageToken: nextPageToken}, nil
}

// --- DescribeActivityType ---

type activityTypeConfigOutput struct {
	DefaultTaskList                   *taskListRef `json:"defaultTaskList,omitempty"`
	DefaultTaskPriority               string       `json:"defaultTaskPriority,omitempty"`
	DefaultTaskHeartbeatTimeout       string       `json:"defaultTaskHeartbeatTimeout,omitempty"`
	DefaultTaskScheduleToCloseTimeout string       `json:"defaultTaskScheduleToCloseTimeout,omitempty"`
	DefaultTaskScheduleToStartTimeout string       `json:"defaultTaskScheduleToStartTimeout,omitempty"`
	DefaultTaskStartToCloseTimeout    string       `json:"defaultTaskStartToCloseTimeout,omitempty"`
}

type describeActivityTypeOutput struct {
	Configuration activityTypeConfigOutput `json:"configuration"`
	TypeInfo      activityTypeInfoOutput   `json:"typeInfo"`
}

type handleDescribeActivityTypeInput struct {
	Domain       string          `json:"domain"`
	ActivityType activityTypeRef `json:"activityType"`
}

//nolint:dupl // ActivityType describe mirrors WorkflowType describe structure
func (h *Handler) handleDescribeActivityType(
	_ context.Context,
	in *handleDescribeActivityTypeInput,
) (*describeActivityTypeOutput, error) {
	at, err := h.Backend.DescribeActivityType(in.Domain, in.ActivityType.Name, in.ActivityType.Version)
	if err != nil {
		return nil, err
	}
	cfg := activityTypeConfigOutput{
		DefaultTaskPriority:               at.Defaults.DefaultTaskPriority,
		DefaultTaskHeartbeatTimeout:       at.Defaults.DefaultTaskHeartbeatTimeout,
		DefaultTaskScheduleToCloseTimeout: at.Defaults.DefaultTaskScheduleToCloseTimeout,
		DefaultTaskScheduleToStartTimeout: at.Defaults.DefaultTaskScheduleToStartTimeout,
		DefaultTaskStartToCloseTimeout:    at.Defaults.DefaultTaskStartToCloseTimeout,
	}
	if at.Defaults.DefaultTaskList != "" {
		cfg.DefaultTaskList = &taskListRef{Name: at.Defaults.DefaultTaskList}
	}

	return &describeActivityTypeOutput{
		TypeInfo: activityTypeInfoOutput{
			ActivityType: &activityTypeRef{Name: at.Name, Version: at.Version},
			Status:       at.Status,
			Description:  at.Description,
			CreationDate: at.CreationDate,
		},
		Configuration: cfg,
	}, nil
}

// --- DeprecateActivityType ---

type deprecateActivityTypeOutput struct{}

type handleDeprecateActivityTypeInput struct {
	Domain       string          `json:"domain"`
	ActivityType activityTypeRef `json:"activityType"`
}

func (h *Handler) handleDeprecateActivityType(
	_ context.Context,
	in *handleDeprecateActivityTypeInput,
) (*deprecateActivityTypeOutput, error) {
	if err := h.Backend.DeprecateActivityType(in.Domain, in.ActivityType.Name, in.ActivityType.Version); err != nil {
		return nil, err
	}

	return &deprecateActivityTypeOutput{}, nil
}

// --- UndeprecateActivityType ---

type undeprecateActivityTypeOutput struct{}

type handleUndeprecateActivityTypeInput struct {
	Domain       string          `json:"domain"`
	ActivityType activityTypeRef `json:"activityType"`
}

func (h *Handler) handleUndeprecateActivityType(
	_ context.Context,
	in *handleUndeprecateActivityTypeInput,
) (*undeprecateActivityTypeOutput, error) {
	if err := h.Backend.UndeprecateActivityType(in.Domain, in.ActivityType.Name, in.ActivityType.Version); err != nil {
		return nil, err
	}

	return &undeprecateActivityTypeOutput{}, nil
}

// --- DeleteActivityType ---

type deleteActivityTypeOutput struct{}

type handleDeleteActivityTypeInput struct {
	Domain       string          `json:"domain"`
	ActivityType activityTypeRef `json:"activityType"`
}

func (h *Handler) handleDeleteActivityType(
	_ context.Context,
	in *handleDeleteActivityTypeInput,
) (*deleteActivityTypeOutput, error) {
	if err := h.Backend.DeleteActivityType(in.Domain, in.ActivityType.Name, in.ActivityType.Version); err != nil {
		return nil, err
	}

	return &deleteActivityTypeOutput{}, nil
}

// --- Execution counts ---

type workflowExecutionCountOutput struct {
	Count     int  `json:"count"`
	Truncated bool `json:"truncated"`
}

type timeFilter struct {
	OldestDate *float64 `json:"oldestDate,omitempty"`
	LatestDate *float64 `json:"latestDate,omitempty"`
}

type executionFilterInput struct {
	WorkflowID string `json:"workflowId,omitempty"`
}

type typeFilterInput struct {
	Name    string `json:"name,omitempty"`
	Version string `json:"version,omitempty"`
}

type closeStatusFilterInput struct {
	Status string `json:"status,omitempty"`
}

type tagFilterInput struct {
	Tag string `json:"tag,omitempty"`
}

func buildExecutionFilter(
	execFilter *executionFilterInput,
	typeFilter *typeFilterInput,
	tagFilter *tagFilterInput,
	closeStatusFilter *closeStatusFilterInput,
	startTimeFilter *timeFilter,
	closeTimeFilter *timeFilter,
) ExecutionFilter {
	f := ExecutionFilter{}
	if execFilter != nil {
		f.WorkflowID = execFilter.WorkflowID
	}
	if typeFilter != nil {
		f.WorkflowTypeName = typeFilter.Name
		f.WorkflowTypeVersion = typeFilter.Version
	}
	if tagFilter != nil {
		f.Tag = tagFilter.Tag
	}
	if closeStatusFilter != nil {
		f.CloseStatus = closeStatusFilter.Status
	}
	if startTimeFilter != nil {
		if startTimeFilter.OldestDate != nil {
			t := time.Unix(int64(*startTimeFilter.OldestDate), 0)
			f.OldestDate = &t
		}
		if startTimeFilter.LatestDate != nil {
			t := time.Unix(int64(*startTimeFilter.LatestDate), 0)
			f.LatestDate = &t
		}
	}
	if closeTimeFilter != nil {
		if closeTimeFilter.OldestDate != nil {
			t := time.Unix(int64(*closeTimeFilter.OldestDate), 0)
			f.CloseOldestDate = &t
		}
		if closeTimeFilter.LatestDate != nil {
			t := time.Unix(int64(*closeTimeFilter.LatestDate), 0)
			f.CloseLatestDate = &t
		}
	}

	return f
}

type handleCountOpenWorkflowExecutionsInput struct {
	StartTimeFilter *timeFilter           `json:"startTimeFilter,omitempty"`
	ExecutionFilter *executionFilterInput `json:"executionFilter,omitempty"`
	TypeFilter      *typeFilterInput      `json:"typeFilter,omitempty"`
	TagFilter       *tagFilterInput       `json:"tagFilter,omitempty"`
	Domain          string                `json:"domain"`
}

func (h *Handler) handleCountOpenWorkflowExecutions(
	_ context.Context,
	in *handleCountOpenWorkflowExecutionsInput,
) (*workflowExecutionCountOutput, error) {
	f := buildExecutionFilter(in.ExecutionFilter, in.TypeFilter, in.TagFilter, nil, in.StartTimeFilter, nil)
	count := h.Backend.CountOpenWorkflowExecutions(in.Domain, f)

	return &workflowExecutionCountOutput{Count: count}, nil
}

type handleCountClosedWorkflowExecutionsInput struct {
	StartTimeFilter   *timeFilter             `json:"startTimeFilter,omitempty"`
	CloseTimeFilter   *timeFilter             `json:"closeTimeFilter,omitempty"`
	ExecutionFilter   *executionFilterInput   `json:"executionFilter,omitempty"`
	TypeFilter        *typeFilterInput        `json:"typeFilter,omitempty"`
	TagFilter         *tagFilterInput         `json:"tagFilter,omitempty"`
	CloseStatusFilter *closeStatusFilterInput `json:"closeStatusFilter,omitempty"`
	Domain            string                  `json:"domain"`
}

func (h *Handler) handleCountClosedWorkflowExecutions(
	_ context.Context,
	in *handleCountClosedWorkflowExecutionsInput,
) (*workflowExecutionCountOutput, error) {
	f := buildExecutionFilter(
		in.ExecutionFilter,
		in.TypeFilter,
		in.TagFilter,
		in.CloseStatusFilter,
		in.StartTimeFilter,
		in.CloseTimeFilter,
	)
	count := h.Backend.CountClosedWorkflowExecutions(in.Domain, f)

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
	count := h.Backend.CountPendingActivityTasks(in.Domain, in.TaskList.Name)

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
	count := h.Backend.CountPendingDecisionTasks(in.Domain, in.TaskList.Name)

	return &pendingTaskCountOutput{Count: count}, nil
}

// --- StartWorkflowExecution ---

type startWorkflowExecutionOutput struct {
	RunID string `json:"runId"`
}

type workflowTypeRefInput struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

type handleStartWorkflowExecutionInput struct {
	TaskList                     *taskListInput       `json:"taskList,omitempty"`
	WorkflowType                 workflowTypeRefInput `json:"workflowType"`
	Domain                       string               `json:"domain"`
	WorkflowID                   string               `json:"workflowId"`
	Input                        string               `json:"input,omitempty"`
	ChildPolicy                  string               `json:"childPolicy,omitempty"`
	LambdaRole                   string               `json:"lambdaRole,omitempty"`
	ExecutionStartToCloseTimeout string               `json:"executionStartToCloseTimeout,omitempty"`
	TaskStartToCloseTimeout      string               `json:"taskStartToCloseTimeout,omitempty"`
	TaskPriority                 string               `json:"taskPriority,omitempty"`
	TagList                      []string             `json:"tagList,omitempty"`
}

func (h *Handler) handleStartWorkflowExecution(
	_ context.Context,
	in *handleStartWorkflowExecutionInput,
) (*startWorkflowExecutionOutput, error) {
	taskList := ""
	if in.TaskList != nil {
		taskList = in.TaskList.Name
	}
	input := StartWorkflowExecutionInput{
		Domain:                       in.Domain,
		WorkflowID:                   in.WorkflowID,
		RunID:                        uuid.New().String(),
		WorkflowTypeName:             in.WorkflowType.Name,
		WorkflowTypeVersion:          in.WorkflowType.Version,
		TaskList:                     taskList,
		Input:                        in.Input,
		TagList:                      in.TagList,
		ChildPolicy:                  in.ChildPolicy,
		LambdaRole:                   in.LambdaRole,
		ExecutionStartToCloseTimeout: in.ExecutionStartToCloseTimeout,
		TaskStartToCloseTimeout:      in.TaskStartToCloseTimeout,
		TaskPriority:                 in.TaskPriority,
	}
	exec, err := h.Backend.StartWorkflowExecution(input)
	if err != nil {
		return nil, err
	}

	return &startWorkflowExecutionOutput{RunID: exec.RunID}, nil
}

// --- DescribeWorkflowExecution ---

type workflowExecutionRef struct {
	WorkflowID string `json:"workflowId"`
	RunID      string `json:"runId"`
}

type executionInfoOutput struct {
	WorkflowType    *workflowTypeRef     `json:"workflowType,omitempty"`
	Execution       workflowExecutionRef `json:"execution"`
	ExecutionStatus string               `json:"executionStatus"`
	CloseStatus     string               `json:"closeStatus,omitempty"`
	TagList         []string             `json:"tagList,omitempty"`
	StartTimestamp  float64              `json:"startTimestamp"`
	CloseTimestamp  float64              `json:"closeTimestamp,omitempty"`
	CancelRequested bool                 `json:"cancelRequested,omitempty"`
}

type executionConfigOutput struct {
	TaskList                     *taskListRef `json:"taskList,omitempty"`
	ExecutionStartToCloseTimeout string       `json:"executionStartToCloseTimeout,omitempty"`
	TaskStartToCloseTimeout      string       `json:"taskStartToCloseTimeout,omitempty"`
	ChildPolicy                  string       `json:"childPolicy,omitempty"`
	LambdaRole                   string       `json:"lambdaRole,omitempty"`
	TaskPriority                 string       `json:"taskPriority,omitempty"`
}

type openCountsOutput struct {
	OpenActivityTasks           int `json:"openActivityTasks"`
	OpenDecisionTasks           int `json:"openDecisionTasks"`
	OpenTimers                  int `json:"openTimers"`
	OpenChildWorkflowExecutions int `json:"openChildWorkflowExecutions"`
}

type describeWorkflowExecutionOutput struct {
	ExecutionConfiguration executionConfigOutput `json:"executionConfiguration"`
	LatestExecutionContext string                `json:"latestExecutionContext,omitempty"`
	ExecutionInfo          executionInfoOutput   `json:"executionInfo"`
	OpenCounts             openCountsOutput      `json:"openCounts"`
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

	info := executionInfoOutput{
		Execution:       workflowExecutionRef{WorkflowID: exec.WorkflowID, RunID: exec.RunID},
		StartTimestamp:  exec.StartTimestamp,
		CloseTimestamp:  exec.CloseTimestamp,
		ExecutionStatus: exec.Status,
		CloseStatus:     exec.CloseStatus,
		TagList:         exec.TagList,
		CancelRequested: exec.CancelRequested,
	}
	if exec.WorkflowTypeName != "" {
		info.WorkflowType = &workflowTypeRef{Name: exec.WorkflowTypeName, Version: exec.WorkflowTypeVersion}
	}

	cfg := executionConfigOutput{
		ExecutionStartToCloseTimeout: exec.ExecutionStartToCloseTimeout,
		TaskStartToCloseTimeout:      exec.TaskStartToCloseTimeout,
		ChildPolicy:                  exec.ChildPolicy,
		LambdaRole:                   exec.LambdaRole,
		TaskPriority:                 exec.TaskPriority,
	}
	if exec.TaskList != "" {
		cfg.TaskList = &taskListRef{Name: exec.TaskList}
	}

	// Retrieve open counts from the backend.
	b, ok := h.Backend.(*InMemoryBackend)
	var counts openCountsOutput
	if ok {
		b.mu.RLock("openCounts")
		c := b.openCountsLocked(in.Domain, in.Execution.WorkflowID)
		b.mu.RUnlock()
		counts = openCountsOutput{
			OpenActivityTasks:           c["openActivityTasks"],
			OpenDecisionTasks:           c["openDecisionTasks"],
			OpenTimers:                  c["openTimers"],
			OpenChildWorkflowExecutions: c["openChildWorkflowExecutions"],
		}
	}

	return &describeWorkflowExecutionOutput{
		ExecutionInfo:          info,
		ExecutionConfiguration: cfg,
		OpenCounts:             counts,
		LatestExecutionContext: exec.LatestExecutionContext,
	}, nil
}

// --- TerminateWorkflowExecution ---

type terminateWorkflowExecutionOutput struct{}

type handleTerminateWorkflowExecutionInput struct {
	Domain      string `json:"domain"`
	WorkflowID  string `json:"workflowId"`
	RunID       string `json:"runId,omitempty"`
	Reason      string `json:"reason,omitempty"`
	Details     string `json:"details,omitempty"`
	ChildPolicy string `json:"childPolicy,omitempty"`
}

func (h *Handler) handleTerminateWorkflowExecution(
	_ context.Context,
	in *handleTerminateWorkflowExecutionInput,
) (*terminateWorkflowExecutionOutput, error) {
	if err := h.Backend.TerminateWorkflowExecution(
		in.Domain,
		in.WorkflowID,
		in.RunID,
		in.Reason,
		in.Details,
	); err != nil {
		return nil, err
	}

	return &terminateWorkflowExecutionOutput{}, nil
}

// --- GetWorkflowExecutionHistory ---

type handleGetWorkflowExecutionHistoryInput struct {
	Execution       workflowExecutionRef `json:"execution"`
	Domain          string               `json:"domain"`
	NextPageToken   string               `json:"nextPageToken,omitempty"`
	MaximumPageSize int                  `json:"maximumPageSize,omitempty"`
	ReverseOrder    bool                 `json:"reverseOrder,omitempty"`
}

type getWorkflowExecutionHistoryOutput struct {
	NextPageToken string         `json:"nextPageToken,omitempty"`
	Events        []HistoryEvent `json:"events"`
}

func (h *Handler) handleGetWorkflowExecutionHistory(
	_ context.Context,
	in *handleGetWorkflowExecutionHistoryInput,
) (*getWorkflowExecutionHistoryOutput, error) {
	events, nextPageToken := h.Backend.GetWorkflowExecutionHistory(
		in.Domain, in.Execution.WorkflowID,
		in.MaximumPageSize, in.NextPageToken, in.ReverseOrder,
	)

	return &getWorkflowExecutionHistoryOutput{Events: events, NextPageToken: nextPageToken}, nil
}

// --- ListOpenWorkflowExecutions ---

type listWorkflowExecutionsOutput struct {
	NextPageToken  string                `json:"nextPageToken,omitempty"`
	ExecutionInfos []executionInfoOutput `json:"executionInfos"`
}

type handleListOpenWorkflowExecutionsInput struct {
	Domain          string                `json:"domain"`
	StartTimeFilter *timeFilter           `json:"startTimeFilter,omitempty"`
	ExecutionFilter *executionFilterInput `json:"executionFilter,omitempty"`
	TypeFilter      *typeFilterInput      `json:"typeFilter,omitempty"`
	TagFilter       *tagFilterInput       `json:"tagFilter,omitempty"`
	NextPageToken   string                `json:"nextPageToken,omitempty"`
	MaximumPageSize int                   `json:"maximumPageSize,omitempty"`
}

func executionToInfo(e WorkflowExecution) executionInfoOutput {
	info := executionInfoOutput{
		Execution:       workflowExecutionRef{WorkflowID: e.WorkflowID, RunID: e.RunID},
		StartTimestamp:  e.StartTimestamp,
		CloseTimestamp:  e.CloseTimestamp,
		ExecutionStatus: e.Status,
		CloseStatus:     e.CloseStatus,
		TagList:         e.TagList,
		CancelRequested: e.CancelRequested,
	}
	if e.WorkflowTypeName != "" {
		info.WorkflowType = &workflowTypeRef{Name: e.WorkflowTypeName, Version: e.WorkflowTypeVersion}
	}

	return info
}

func (h *Handler) handleListOpenWorkflowExecutions(
	_ context.Context,
	in *handleListOpenWorkflowExecutionsInput,
) (*listWorkflowExecutionsOutput, error) {
	f := buildExecutionFilter(in.ExecutionFilter, in.TypeFilter, in.TagFilter, nil, in.StartTimeFilter, nil)
	execs := h.Backend.ListOpenWorkflowExecutions(in.Domain, f)
	infos := make([]executionInfoOutput, len(execs))
	for i, e := range execs {
		infos[i] = executionToInfo(e)
	}
	infos, nextPageToken := applyPageTokenSlice(infos, in.NextPageToken, in.MaximumPageSize)

	return &listWorkflowExecutionsOutput{ExecutionInfos: infos, NextPageToken: nextPageToken}, nil
}

// --- ListClosedWorkflowExecutions ---

type handleListClosedWorkflowExecutionsInput struct {
	Domain            string                  `json:"domain"`
	StartTimeFilter   *timeFilter             `json:"startTimeFilter,omitempty"`
	CloseTimeFilter   *timeFilter             `json:"closeTimeFilter,omitempty"`
	ExecutionFilter   *executionFilterInput   `json:"executionFilter,omitempty"`
	TypeFilter        *typeFilterInput        `json:"typeFilter,omitempty"`
	TagFilter         *tagFilterInput         `json:"tagFilter,omitempty"`
	CloseStatusFilter *closeStatusFilterInput `json:"closeStatusFilter,omitempty"`
	NextPageToken     string                  `json:"nextPageToken,omitempty"`
	MaximumPageSize   int                     `json:"maximumPageSize,omitempty"`
}

func (h *Handler) handleListClosedWorkflowExecutions(
	_ context.Context,
	in *handleListClosedWorkflowExecutionsInput,
) (*listWorkflowExecutionsOutput, error) {
	f := buildExecutionFilter(
		in.ExecutionFilter,
		in.TypeFilter,
		in.TagFilter,
		in.CloseStatusFilter,
		in.StartTimeFilter,
		in.CloseTimeFilter,
	)
	execs := h.Backend.ListClosedWorkflowExecutions(in.Domain, f)
	infos := make([]executionInfoOutput, len(execs))
	for i, e := range execs {
		infos[i] = executionToInfo(e)
	}
	infos, nextPageToken := applyPageTokenSlice(infos, in.NextPageToken, in.MaximumPageSize)

	return &listWorkflowExecutionsOutput{ExecutionInfos: infos, NextPageToken: nextPageToken}, nil
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
	tagMap, err := h.Backend.ListTagsForResource(in.ResourceArn)
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(tagMap))
	for k := range tagMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	tags := make([]resourceTag, 0, len(keys))
	for _, k := range keys {
		tags = append(tags, resourceTag{Key: k, Value: tagMap[k]})
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
	Identity string      `json:"identity,omitempty"`
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

type decisionTaskWorkflowTypeOutput struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

type decisionTaskOutput struct {
	WorkflowType           *decisionTaskWorkflowTypeOutput `json:"workflowType,omitempty"`
	WorkflowExecution      *workflowExecutionRef           `json:"workflowExecution,omitempty"`
	TaskToken              string                          `json:"taskToken"`
	WorkflowID             string                          `json:"workflowId"`
	RunID                  string                          `json:"runId"`
	NextPageToken          string                          `json:"nextPageToken,omitempty"`
	Events                 []HistoryEvent                  `json:"events"`
	StartedEventID         int64                           `json:"startedEventId"`
	PreviousStartedEventID int64                           `json:"previousStartedEventId"`
}

type handlePollForDecisionTaskInput struct {
	Domain          string      `json:"domain"`
	TaskList        taskListRef `json:"taskList"`
	NextPageToken   string      `json:"nextPageToken,omitempty"`
	Identity        string      `json:"identity,omitempty"`
	MaximumPageSize int         `json:"maximumPageSize,omitempty"`
	ReverseOrder    bool        `json:"reverseOrder,omitempty"`
}

func (h *Handler) handlePollForDecisionTask(
	_ context.Context,
	in *handlePollForDecisionTaskInput,
) (*decisionTaskOutput, error) {
	task := h.Backend.PollForDecisionTask(in.Domain, in.TaskList.Name, in.MaximumPageSize, in.NextPageToken)
	if task == nil {
		return &decisionTaskOutput{}, nil
	}
	out := &decisionTaskOutput{
		TaskToken:              task.TaskToken,
		WorkflowID:             task.WorkflowID,
		RunID:                  task.RunID,
		Events:                 task.Events,
		StartedEventID:         task.StartedEventID,
		PreviousStartedEventID: task.PreviousStartedEventID,
		NextPageToken:          task.NextPageToken,
	}
	if task.WorkflowTypeName != "" {
		out.WorkflowType = &decisionTaskWorkflowTypeOutput{
			Name:    task.WorkflowTypeName,
			Version: task.WorkflowTypeVersion,
		}
	}
	if task.WorkflowID != "" {
		out.WorkflowExecution = &workflowExecutionRef{WorkflowID: task.WorkflowID, RunID: task.RunID}
	}

	return out, nil
}

// --- RecordActivityTaskHeartbeat ---

type handleRecordActivityTaskHeartbeatInput struct {
	TaskToken string `json:"taskToken"`
	Details   string `json:"details,omitempty"`
}

type recordActivityTaskHeartbeatOutput struct {
	CancelRequested bool `json:"cancelRequested"`
}

func (h *Handler) handleRecordActivityTaskHeartbeat(
	_ context.Context,
	in *handleRecordActivityTaskHeartbeatInput,
) (*recordActivityTaskHeartbeatOutput, error) {
	cancelRequested, err := h.Backend.RecordActivityTaskHeartbeat(in.TaskToken)
	if err != nil {
		// Unknown token: per AWS, return cancelRequested:false rather than error
		// for heartbeats on tokens that may have expired.
		//nolint:nilerr // AWS returns false for unknown tokens, not an error
		return &recordActivityTaskHeartbeatOutput{CancelRequested: false}, nil
	}

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
	if err := h.Backend.RequestCancelWorkflowExecution(in.Domain, in.WorkflowID, in.RunID); err != nil {
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
	if err := h.Backend.RespondActivityTaskCanceled(in.TaskToken, in.Details); err != nil {
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
	if err := h.Backend.RespondActivityTaskCompleted(in.TaskToken, in.Result); err != nil {
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
	if err := h.Backend.RespondActivityTaskFailed(in.TaskToken, in.Reason, in.Details); err != nil {
		return nil, err
	}

	return &respondActivityTaskFailedOutput{}, nil
}

// --- RespondDecisionTaskCompleted ---

type handleRespondDecisionTaskCompletedInput struct {
	TaskToken        string `json:"taskToken"`
	ExecutionContext string `json:"executionContext,omitempty"`
}

type respondDecisionTaskCompletedOutput struct{}

func (h *Handler) handleRespondDecisionTaskCompleted(
	_ context.Context,
	in *handleRespondDecisionTaskCompletedInput,
) (*respondDecisionTaskCompletedOutput, error) {
	if err := h.Backend.RespondDecisionTaskCompleted(in.TaskToken, in.ExecutionContext); err != nil {
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
	if err := h.Backend.SignalWorkflowExecution(
		in.Domain,
		in.WorkflowID,
		in.RunID,
		in.SignalName,
		in.Input,
	); err != nil {
		return nil, err
	}

	return &signalWorkflowExecutionOutput{}, nil
}

const defaultSWFMaxPageSize = 1000

// applyPageTokenSlice applies nextPageToken-based pagination using the shared
// pkgs/page opaque token format.
func applyPageTokenSlice[T any](items []T, nextPageToken string, maximumPageSize int) ([]T, string) {
	p := page.New(items, nextPageToken, maximumPageSize, defaultSWFMaxPageSize)

	return p.Data, p.Next
}
