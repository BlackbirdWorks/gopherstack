package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	keyNamePrefix = "NamePrefix"
	keyNextToken  = "NextToken"
	keyMaxResults = "MaxResults"
)

const (
	opCreateSchedule      = "CreateSchedule"
	opCreateScheduleGroup = "CreateScheduleGroup"
	opDeleteSchedule      = "DeleteSchedule"
	opDeleteScheduleGroup = "DeleteScheduleGroup"
	opGetSchedule         = "GetSchedule"
	opGetScheduleGroup    = "GetScheduleGroup"
	opListScheduleGroups  = "ListScheduleGroups"
	opListSchedules       = "ListSchedules"
	opListTagsForResource = "ListTagsForResource"
	opTagResource         = "TagResource"
	opUpdateSchedule      = "UpdateSchedule"
)

const (
	schedulerTargetPrefix    = "AWSScheduler."
	schedulerPathSegment     = "schedules"
	scheduleGroupPathSegment = "schedule-groups"
	schedulerTagsPathSegment = "tags"
	// schedulesPathMinSegments is the minimum number of URL path segments in a
	// /schedules/{name} REST path: ["schedules", "{name}"].
	schedulesPathMinSegments = 2
	// restOpUnknown is returned by parseSchedulerRESTPath when no pattern matches.
	restOpUnknown = "Unknown"
	// opUntagResource is the operation name for removing tags from a resource.
	opUntagResource = "UntagResource"
)

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
)

type scheduleNameInput struct {
	Name      string `json:"Name"`
	GroupName string `json:"GroupName"`
}

// scheduleTargetRetryPolicy mirrors RetryPolicy for handler input/output.
type scheduleTargetRetryPolicy struct {
	MaximumEventAgeInSeconds int `json:"MaximumEventAgeInSeconds,omitempty"`
	MaximumRetryAttempts     int `json:"MaximumRetryAttempts,omitempty"`
}

// scheduleTargetDeadLetterConfig mirrors DeadLetterConfig for handler input/output.
type scheduleTargetDeadLetterConfig struct {
	Arn string `json:"Arn,omitempty"`
}

// scheduleTargetInputTransformer mirrors InputTransformer for handler input/output.
type scheduleTargetInputTransformer struct {
	InputPathsMap map[string]string `json:"InputPathsMap,omitempty"`
	InputTemplate string            `json:"InputTemplate,omitempty"`
}

// scheduleTargetEventBridgeParameters mirrors EventBridgeParameters for handler input/output.
type scheduleTargetEventBridgeParameters struct {
	DetailType string `json:"DetailType,omitempty"`
	Source     string `json:"Source,omitempty"`
}

// scheduleTargetKinesisParameters mirrors KinesisParameters for handler input/output.
type scheduleTargetKinesisParameters struct {
	PartitionKey string `json:"PartitionKey,omitempty"`
}

// scheduleTargetSqsParameters mirrors SqsParameters for handler input/output.
type scheduleTargetSqsParameters struct {
	MessageGroupId string `json:"MessageGroupId,omitempty"`
}

// scheduleTargetSageMakerPipelineParam mirrors a pipeline parameter name/value.
type scheduleTargetSageMakerPipelineParam struct {
	Name  string `json:"Name"`
	Value string `json:"Value"`
}

// scheduleTargetSageMakerPipelineParameters mirrors SageMakerPipelineParameters for handler.
type scheduleTargetSageMakerPipelineParameters struct {
	PipelineParameterList []scheduleTargetSageMakerPipelineParam `json:"PipelineParameterList,omitempty"`
}

// scheduleTargetEcsParameters mirrors EcsParameters for handler input/output.
type scheduleTargetEcsParameters struct {
	TaskDefinitionArn    string `json:"TaskDefinitionArn,omitempty"`
	LaunchType           string `json:"LaunchType,omitempty"`
	TaskCount            int    `json:"TaskCount,omitempty"`
	PlatformVersion      string `json:"PlatformVersion,omitempty"`
	Group                string `json:"Group,omitempty"`
	PropagateTags        string `json:"PropagateTags,omitempty"`
	ReferenceId          string `json:"ReferenceId,omitempty"`
	EnableECSManagedTags bool   `json:"EnableECSManagedTags,omitempty"`
	EnableExecuteCommand bool   `json:"EnableExecuteCommand,omitempty"`
}

// scheduleTarget holds the ARN, IAM role, and optional custom input for a schedule target.
type scheduleTarget struct {
	Arn                         string                                     `json:"Arn"`
	RoleArn                     string                                     `json:"RoleArn"`
	Input                       string                                     `json:"Input,omitempty"`
	RetryPolicy                 *scheduleTargetRetryPolicy                 `json:"RetryPolicy,omitempty"`
	DeadLetterConfig            *scheduleTargetDeadLetterConfig            `json:"DeadLetterConfig,omitempty"`
	InputTransformer            *scheduleTargetInputTransformer            `json:"InputTransformer,omitempty"`
	EventBridgeParameters       *scheduleTargetEventBridgeParameters       `json:"EventBridgeParameters,omitempty"`
	KinesisParameters           *scheduleTargetKinesisParameters           `json:"KinesisParameters,omitempty"`
	SqsParameters               *scheduleTargetSqsParameters               `json:"SqsParameters,omitempty"`
	SageMakerPipelineParameters *scheduleTargetSageMakerPipelineParameters `json:"SageMakerPipelineParameters,omitempty"`
	EcsParameters               *scheduleTargetEcsParameters               `json:"EcsParameters,omitempty"`
}

// scheduleFlexibleTimeWindow holds the flexible time window configuration for a schedule.
type scheduleFlexibleTimeWindow struct {
	Mode                   string `json:"Mode"`
	MaximumWindowInMinutes int    `json:"MaximumWindowInMinutes"`
}

type scheduleInput struct {
	EndDate                    *float64                   `json:"EndDate,omitempty"`
	StartDate                  *float64                   `json:"StartDate,omitempty"`
	Target                     scheduleTarget             `json:"Target"`
	ScheduleExpressionTimezone string                     `json:"ScheduleExpressionTimezone"`
	Description                string                     `json:"Description"`
	Name                       string                     `json:"Name"`
	State                      string                     `json:"State"`
	ScheduleExpression         string                     `json:"ScheduleExpression"`
	GroupName                  string                     `json:"GroupName"`
	ActionAfterCompletion      string                     `json:"ActionAfterCompletion,omitempty"`
	KmsKeyArn                  string                     `json:"KmsKeyArn,omitempty"`
	ClientToken                string                     `json:"ClientToken,omitempty"`
	FlexibleTimeWindow         scheduleFlexibleTimeWindow `json:"FlexibleTimeWindow"`
}

// Handler is the Echo HTTP handler for EventBridge Scheduler operations.
type Handler struct {
	Backend StorageBackend
	ops     map[string]service.JSONOpFunc
	runner  *Runner
	cancel  context.CancelFunc
}

// NewHandler creates a new Scheduler handler.
func NewHandler(backend StorageBackend) *Handler {
	h := &Handler{
		Backend: backend,
		runner:  NewRunner(backend),
	}
	h.ops = h.buildOps()

	return h
}

// SetRunner replaces the default runner with the given one.
// Useful for wiring target invokers before StartWorker is called.
func (h *Handler) SetRunner(r *Runner) {
	h.runner = r
}

// GetRunner returns the handler's Runner so callers can configure target invokers.
func (h *Handler) GetRunner() *Runner {
	return h.runner
}

// Name returns the service name.
func (h *Handler) Name() string { return "Scheduler" }

// StartWorker implements service.BackgroundWorker.
// It starts the schedule runner as a background goroutine.
func (h *Handler) StartWorker(ctx context.Context) error {
	if h.runner == nil {
		return nil
	}

	runCtx, cancel := context.WithCancel(ctx)
	h.cancel = cancel
	h.runner.Start(runCtx)

	return nil
}

// Shutdown implements service.Shutdowner.
// It stops the schedule runner goroutine.
func (h *Handler) Shutdown(_ context.Context) {
	if h.cancel != nil {
		h.cancel()
	}
}

// Ensure Handler implements service.BackgroundWorker and service.Shutdowner at compile time.
var _ service.BackgroundWorker = (*Handler)(nil)
var _ service.Shutdowner = (*Handler)(nil)

// GetSupportedOperations returns the list of supported Scheduler operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		opCreateSchedule,
		opCreateScheduleGroup,
		opDeleteSchedule,
		opDeleteScheduleGroup,
		opGetSchedule,
		opGetScheduleGroup,
		opListScheduleGroups,
		opListSchedules,
		opListTagsForResource,
		opTagResource,
		opUntagResource,
		opUpdateSchedule,
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "scheduler" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this Scheduler instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches Scheduler requests.
// Matches both X-Amz-Target (JSON protocol) and REST API paths (/schedules/... , /schedule-groups/...).
// For /tags/{ResourceArn} paths, only matches when the ARN belongs to the Scheduler service
// (i.e. contains ":scheduler:") to avoid intercepting tag requests destined for other services.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		if strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), schedulerTargetPrefix) {
			return true
		}

		path := c.Request().URL.Path

		if strings.HasPrefix(path, "/"+schedulerPathSegment) ||
			strings.HasPrefix(path, "/"+scheduleGroupPathSegment) {
			return true
		}

		// Only claim /tags/{ResourceArn} when the ARN is a scheduler-owned resource.
		if after, ok := strings.CutPrefix(path, "/"+schedulerTagsPathSegment+"/"); ok {
			return strings.Contains(after, ":scheduler:")
		}

		return false
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the Scheduler action from the X-Amz-Target header or REST path.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, schedulerTargetPrefix)
	if action != "" && action != target {
		return action
	}

	op, _ := parseSchedulerRESTPath(c.Request().Method, c.Request().URL.Path)

	return op
}

// ExtractResource extracts the schedule or schedule group name from the request body or REST path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	// For REST paths extract name from the URL path segment.
	if !strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), schedulerTargetPrefix) {
		path := c.Request().URL.Path
		// /tags/{ResourceArn} paths - return the ARN as resource.
		if after, ok := strings.CutPrefix(path, "/"+schedulerTagsPathSegment+"/"); ok {
			return after
		}

		parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
		if len(parts) >= schedulesPathMinSegments {
			return parts[1]
		}
	}

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}
	var req scheduleNameInput
	_ = json.Unmarshal(body, &req)

	return req.Name
}

// parseSchedulerRESTPath maps an HTTP method + path to a Scheduler operation name and
// extracts the resource name (if present in the path).
// Returns ("Unknown", "") when no pattern matches.
func parseSchedulerRESTPath(method, path string) (string, string) {
	// Strip leading slash and split into segments.
	segments := strings.Split(strings.TrimPrefix(path, "/"), "/")

	if len(segments) == 0 {
		return restOpUnknown, ""
	}

	switch segments[0] {
	case schedulerPathSegment:
		return parseScheduleRESTPath(method, segments)
	case scheduleGroupPathSegment:
		return parseScheduleGroupRESTPath(method, segments)
	case schedulerTagsPathSegment:
		return parseTagsRESTPath(method, segments)
	}

	return restOpUnknown, ""
}

// parseTagsRESTPath handles REST routing for /tags/{ResourceArn} paths.
func parseTagsRESTPath(method string, segments []string) (string, string) {
	if len(segments) < schedulesPathMinSegments || segments[1] == "" {
		return restOpUnknown, ""
	}

	resourceARN := strings.Join(segments[1:], "/")

	switch method {
	case http.MethodGet:
		return opListTagsForResource, resourceARN
	case http.MethodPost:
		return opTagResource, resourceARN
	case http.MethodDelete:
		return opUntagResource, resourceARN
	}

	return restOpUnknown, ""
}

// parseScheduleRESTPath handles REST routing for /schedules/... paths.
func parseScheduleRESTPath(method string, segments []string) (string, string) {
	switch {
	// GET /schedules or GET /schedules/ → ListSchedules
	case method == http.MethodGet &&
		(len(segments) == 1 || (len(segments) == schedulesPathMinSegments && segments[1] == "")):
		return opListSchedules, ""
	// POST /schedules/{name} → CreateSchedule
	case method == http.MethodPost && len(segments) == schedulesPathMinSegments:
		return opCreateSchedule, segments[1]
	// GET /schedules/{name} → GetSchedule
	case method == http.MethodGet && len(segments) == schedulesPathMinSegments:
		return opGetSchedule, segments[1]
	// DELETE /schedules/{name} → DeleteSchedule
	case method == http.MethodDelete && len(segments) == schedulesPathMinSegments:
		return opDeleteSchedule, segments[1]
	// PUT /schedules/{name} → UpdateSchedule
	case method == http.MethodPut && len(segments) == schedulesPathMinSegments:
		return opUpdateSchedule, segments[1]
	}

	return restOpUnknown, ""
}

// parseScheduleGroupRESTPath handles REST routing for /schedule-groups/... paths.
func parseScheduleGroupRESTPath(method string, segments []string) (string, string) {
	switch {
	// GET /schedule-groups or GET /schedule-groups/ → ListScheduleGroups
	case method == http.MethodGet &&
		(len(segments) == 1 || (len(segments) == schedulesPathMinSegments && segments[1] == "")):
		return opListScheduleGroups, ""
	// POST /schedule-groups/{name} → CreateScheduleGroup
	case method == http.MethodPost && len(segments) == schedulesPathMinSegments:
		return opCreateScheduleGroup, segments[1]
	// GET /schedule-groups/{name} → GetScheduleGroup
	case method == http.MethodGet && len(segments) == schedulesPathMinSegments:
		return opGetScheduleGroup, segments[1]
	// DELETE /schedule-groups/{name} → DeleteScheduleGroup
	case method == http.MethodDelete && len(segments) == schedulesPathMinSegments:
		return opDeleteScheduleGroup, segments[1]
	}

	return restOpUnknown, ""
}

// Handler returns the Echo handler function for Scheduler requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		path := c.Request().URL.Path
		isREST := !strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), schedulerTargetPrefix)

		// REST API paths: /schedules/..., /schedule-groups/..., /tags/...
		if isREST && (strings.HasPrefix(path, "/"+schedulerPathSegment) ||
			strings.HasPrefix(path, "/"+scheduleGroupPathSegment) ||
			strings.HasPrefix(path, "/"+schedulerTagsPathSegment+"/")) {
			return h.handleREST(c)
		}

		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"Scheduler", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

// handleREST handles Scheduler REST API calls.
// It extracts path parameters from the URL, injects them into the request body,
// and dispatches to the existing handler logic.
func (h *Handler) handleREST(c *echo.Context) error {
	ctx := c.Request().Context()

	action, name := parseSchedulerRESTPath(c.Request().Method, c.Request().URL.Path)
	if action == restOpUnknown {
		return c.String(http.StatusNotFound, "not found")
	}

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		logger.Load(ctx).ErrorContext(ctx, "failed to read request body", "error", err)

		return c.String(http.StatusInternalServerError, "internal server error")
	}

	q := c.Request().URL.Query()
	body = h.enrichRESTBody(action, name, body, q)

	response, dispErr := h.dispatch(ctx, action, body)
	if dispErr != nil {
		return h.handleError(ctx, c, action, dispErr)
	}

	return c.JSONBlob(http.StatusOK, response)
}

// enrichRESTBody injects path parameters and query parameters into the JSON body
// so that the existing dispatch handlers can read them uniformly.
func (h *Handler) enrichRESTBody(action, name string, body []byte, q interface{ Get(string) string }) []byte {
	m := unmarshalOrEmpty(body)

	switch action {
	case opGetSchedule, opDeleteSchedule, opUpdateSchedule:
		enrichScheduleByPath(m, name, q)
	case opCreateSchedule, opCreateScheduleGroup, opGetScheduleGroup, opDeleteScheduleGroup:
		if name != "" {
			setJSONString(m, "Name", name)
		}
	case opListSchedules:
		enrichListSchedulesQuery(m, q)
	case opListScheduleGroups:
		enrichListScheduleGroupsQuery(m, q)
	case opListTagsForResource, opTagResource, opUntagResource:
		enrichTagsPath(m, name, action, q)
	}

	result, _ := json.Marshal(m)

	return result
}

// enrichScheduleByPath injects Name from the URL path and GroupName from the query string.
func enrichScheduleByPath(m map[string]json.RawMessage, name string, q interface{ Get(string) string }) {
	if name != "" {
		setJSONString(m, "Name", name)
	}

	if gn := q.Get("groupName"); gn != "" {
		setJSONString(m, "GroupName", gn)
	}
}

// enrichListSchedulesQuery injects ListSchedules query parameters into the JSON map.
func enrichListSchedulesQuery(m map[string]json.RawMessage, q interface{ Get(string) string }) {
	if sg := q.Get("ScheduleGroup"); sg != "" {
		setJSONString(m, "GroupName", sg)
	}

	for _, qk := range []struct{ query, json string }{
		{keyNamePrefix, keyNamePrefix},
		{"State", "State"},
		{keyNextToken, keyNextToken},
		{keyMaxResults, keyMaxResults},
	} {
		if v := q.Get(qk.query); v != "" {
			setJSONString(m, qk.json, v)
		}
	}
}

// enrichListScheduleGroupsQuery injects ListScheduleGroups query parameters into the JSON map.
func enrichListScheduleGroupsQuery(m map[string]json.RawMessage, q interface{ Get(string) string }) {
	for _, qk := range []struct{ query, json string }{
		{keyNamePrefix, keyNamePrefix},
		{keyNextToken, keyNextToken},
		{keyMaxResults, keyMaxResults},
	} {
		if v := q.Get(qk.query); v != "" {
			setJSONString(m, qk.json, v)
		}
	}
}

// enrichTagsPath injects ResourceArn from the URL path and tag keys from the query string.
func enrichTagsPath(m map[string]json.RawMessage, name, action string, q interface{ Get(string) string }) {
	if name != "" {
		setJSONString(m, "ResourceArn", name)
	}

	if tagKeys := q.Get("tagKeys"); tagKeys != "" && action == opUntagResource {
		keys := strings.Split(tagKeys, ",")
		keysJSON, _ := json.Marshal(keys)
		m["TagKeys"] = json.RawMessage(keysJSON)
	}
}

// unmarshalOrEmpty parses body as a JSON object, returning an empty map on failure.
func unmarshalOrEmpty(body []byte) map[string]json.RawMessage {
	var m map[string]json.RawMessage
	if len(body) > 0 {
		if err := json.Unmarshal(body, &m); err != nil {
			return make(map[string]json.RawMessage)
		}

		return m
	}

	return make(map[string]json.RawMessage)
}

// setJSONString encodes value as a JSON string and sets it on key in the map,
// unless key is already present.
func setJSONString(m map[string]json.RawMessage, key, value string) {
	if _, exists := m[key]; exists {
		return
	}

	quoted, _ := json.Marshal(value)
	m[key] = json.RawMessage(quoted)
}

// buildOps constructs the dispatch map once at handler creation time.
func (h *Handler) buildOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		opCreateSchedule:      service.WrapOp(h.handleCreateSchedule),
		opCreateScheduleGroup: service.WrapOp(h.handleCreateScheduleGroup),
		opDeleteSchedule:      service.WrapOp(h.handleDeleteSchedule),
		opDeleteScheduleGroup: service.WrapOp(h.handleDeleteScheduleGroup),
		opGetSchedule:         service.WrapOp(h.handleGetSchedule),
		opGetScheduleGroup:    service.WrapOp(h.handleGetScheduleGroup),
		opListScheduleGroups:  service.WrapOp(h.handleListScheduleGroups),
		opListSchedules:       service.WrapOp(h.handleListSchedules),
		opListTagsForResource: service.WrapOp(h.handleListTagsForResource),
		opTagResource:         service.WrapOp(h.handleTagResource),
		opUntagResource:       service.WrapOp(h.handleUntagResource),
		opUpdateSchedule:      service.WrapOp(h.handleUpdateSchedule),
	}
}

func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.ops[action]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errUnknownAction, action)
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

	switch {
	case errors.Is(err, ErrNotFound):
		payload, _ := json.Marshal(service.JSONErrorResponse{
			Type:    "ResourceNotFoundException",
			Message: err.Error(),
		})

		return c.JSONBlob(http.StatusNotFound, payload)
	case errors.Is(err, ErrAlreadyExists):
		payload, _ := json.Marshal(service.JSONErrorResponse{
			Type:    "ConflictException",
			Message: err.Error(),
		})

		return c.JSONBlob(http.StatusConflict, payload)
	case errors.Is(err, ErrValidation):
		payload, _ := json.Marshal(service.JSONErrorResponse{
			Type:    "ValidationException",
			Message: err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, errInvalidRequest), errors.Is(err, errUnknownAction),
		errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		return c.JSON(http.StatusBadRequest, map[string]string{"message": err.Error()})
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}
}

type createScheduleOutput struct {
	ScheduleArn string `json:"ScheduleArn"`
}

func (h *Handler) handleCreateSchedule(_ context.Context, in *scheduleInput) (*createScheduleOutput, error) {
	state := in.State
	if state == "" {
		state = scheduleStateEnabled
	}

	var opts []ScheduleOption
	if in.StartDate != nil {
		opts = append(opts, WithStartDate(epochSecondsToTime(*in.StartDate)))
	}

	if in.EndDate != nil {
		opts = append(opts, WithEndDate(epochSecondsToTime(*in.EndDate)))
	}

	if in.ActionAfterCompletion != "" {
		opts = append(opts, WithActionAfterCompletion(in.ActionAfterCompletion))
	}

	if in.KmsKeyArn != "" {
		opts = append(opts, WithKmsKeyArn(in.KmsKeyArn))
	}

	s, err := h.Backend.CreateSchedule(
		in.Name,
		in.GroupName,
		in.ScheduleExpression,
		in.Description,
		in.ScheduleExpressionTimezone,
		targetFromInput(in.Target),
		state,
		FlexibleTimeWindow{
			Mode:                   in.FlexibleTimeWindow.Mode,
			MaximumWindowInMinutes: in.FlexibleTimeWindow.MaximumWindowInMinutes,
		},
		opts...,
	)
	if err != nil {
		return nil, err
	}

	return &createScheduleOutput{ScheduleArn: s.ARN}, nil
}

// targetFromInput converts a handler scheduleTarget into the backend Target type.
func targetFromInput(in scheduleTarget) Target {
	t := Target{
		ARN:     in.Arn,
		RoleARN: in.RoleArn,
		Input:   in.Input,
	}

	if in.RetryPolicy != nil {
		t.RetryPolicy = &RetryPolicy{
			MaximumEventAgeInSeconds: in.RetryPolicy.MaximumEventAgeInSeconds,
			MaximumRetryAttempts:     in.RetryPolicy.MaximumRetryAttempts,
		}
	}

	if in.DeadLetterConfig != nil {
		t.DeadLetterConfig = &DeadLetterConfig{Arn: in.DeadLetterConfig.Arn}
	}

	if in.InputTransformer != nil {
		t.InputTransformer = &InputTransformer{
			InputPathsMap: in.InputTransformer.InputPathsMap,
			InputTemplate: in.InputTransformer.InputTemplate,
		}
	}

	if in.EventBridgeParameters != nil {
		t.EventBridgeParameters = &EventBridgeParameters{
			DetailType: in.EventBridgeParameters.DetailType,
			Source:     in.EventBridgeParameters.Source,
		}
	}

	if in.KinesisParameters != nil {
		t.KinesisParameters = &KinesisParameters{PartitionKey: in.KinesisParameters.PartitionKey}
	}

	if in.SqsParameters != nil {
		t.SqsParameters = &SqsParameters{MessageGroupId: in.SqsParameters.MessageGroupId}
	}

	if in.SageMakerPipelineParameters != nil {
		params := make([]SageMakerPipelineParameter, len(in.SageMakerPipelineParameters.PipelineParameterList))
		for i, p := range in.SageMakerPipelineParameters.PipelineParameterList {
			params[i] = SageMakerPipelineParameter{Name: p.Name, Value: p.Value}
		}

		t.SageMakerPipelineParameters = &SageMakerPipelineParameters{PipelineParameterList: params}
	}

	if in.EcsParameters != nil {
		t.EcsParameters = &EcsParameters{
			TaskDefinitionArn:    in.EcsParameters.TaskDefinitionArn,
			LaunchType:           in.EcsParameters.LaunchType,
			TaskCount:            in.EcsParameters.TaskCount,
			PlatformVersion:      in.EcsParameters.PlatformVersion,
			Group:                in.EcsParameters.Group,
			PropagateTags:        in.EcsParameters.PropagateTags,
			ReferenceId:          in.EcsParameters.ReferenceId,
			EnableECSManagedTags: in.EcsParameters.EnableECSManagedTags,
			EnableExecuteCommand: in.EcsParameters.EnableExecuteCommand,
		}
	}

	return t
}

// targetToOutput converts a backend Target into the handler output type.
func targetToOutput(t Target) scheduleTargetOutput {
	out := scheduleTargetOutput{
		Arn:     t.ARN,
		RoleArn: t.RoleARN,
		Input:   t.Input,
	}

	if t.RetryPolicy != nil {
		out.RetryPolicy = &scheduleTargetRetryPolicy{
			MaximumEventAgeInSeconds: t.RetryPolicy.MaximumEventAgeInSeconds,
			MaximumRetryAttempts:     t.RetryPolicy.MaximumRetryAttempts,
		}
	}

	if t.DeadLetterConfig != nil {
		out.DeadLetterConfig = &scheduleTargetDeadLetterConfig{Arn: t.DeadLetterConfig.Arn}
	}

	if t.InputTransformer != nil {
		out.InputTransformer = &scheduleTargetInputTransformer{
			InputPathsMap: t.InputTransformer.InputPathsMap,
			InputTemplate: t.InputTransformer.InputTemplate,
		}
	}

	if t.EventBridgeParameters != nil {
		out.EventBridgeParameters = &scheduleTargetEventBridgeParameters{
			DetailType: t.EventBridgeParameters.DetailType,
			Source:     t.EventBridgeParameters.Source,
		}
	}

	if t.KinesisParameters != nil {
		out.KinesisParameters = &scheduleTargetKinesisParameters{PartitionKey: t.KinesisParameters.PartitionKey}
	}

	if t.SqsParameters != nil {
		out.SqsParameters = &scheduleTargetSqsParameters{MessageGroupId: t.SqsParameters.MessageGroupId}
	}

	if t.SageMakerPipelineParameters != nil {
		params := make([]scheduleTargetSageMakerPipelineParam, len(t.SageMakerPipelineParameters.PipelineParameterList))
		for i, p := range t.SageMakerPipelineParameters.PipelineParameterList {
			params[i] = scheduleTargetSageMakerPipelineParam{Name: p.Name, Value: p.Value}
		}

		out.SageMakerPipelineParameters = &scheduleTargetSageMakerPipelineParameters{PipelineParameterList: params}
	}

	if t.EcsParameters != nil {
		out.EcsParameters = &scheduleTargetEcsParameters{
			TaskDefinitionArn:    t.EcsParameters.TaskDefinitionArn,
			LaunchType:           t.EcsParameters.LaunchType,
			TaskCount:            t.EcsParameters.TaskCount,
			PlatformVersion:      t.EcsParameters.PlatformVersion,
			Group:                t.EcsParameters.Group,
			PropagateTags:        t.EcsParameters.PropagateTags,
			ReferenceId:          t.EcsParameters.ReferenceId,
			EnableECSManagedTags: t.EcsParameters.EnableECSManagedTags,
			EnableExecuteCommand: t.EcsParameters.EnableExecuteCommand,
		}
	}

	return out
}

type scheduleTargetOutput struct {
	Arn                         string                                     `json:"Arn"`
	RoleArn                     string                                     `json:"RoleArn"`
	Input                       string                                     `json:"Input,omitempty"`
	RetryPolicy                 *scheduleTargetRetryPolicy                 `json:"RetryPolicy,omitempty"`
	DeadLetterConfig            *scheduleTargetDeadLetterConfig            `json:"DeadLetterConfig,omitempty"`
	InputTransformer            *scheduleTargetInputTransformer            `json:"InputTransformer,omitempty"`
	EventBridgeParameters       *scheduleTargetEventBridgeParameters       `json:"EventBridgeParameters,omitempty"`
	KinesisParameters           *scheduleTargetKinesisParameters           `json:"KinesisParameters,omitempty"`
	SqsParameters               *scheduleTargetSqsParameters               `json:"SqsParameters,omitempty"`
	SageMakerPipelineParameters *scheduleTargetSageMakerPipelineParameters `json:"SageMakerPipelineParameters,omitempty"`
	EcsParameters               *scheduleTargetEcsParameters               `json:"EcsParameters,omitempty"`
}

type flexibleTimeWindowOutput struct {
	Mode                   string `json:"Mode"`
	MaximumWindowInMinutes int    `json:"MaximumWindowInMinutes,omitempty"`
}

type getScheduleOutput struct {
	EndDate                    *float64                 `json:"EndDate,omitempty"`
	Tags                       map[string]string        `json:"Tags,omitempty"`
	StartDate                  *float64                 `json:"StartDate,omitempty"`
	Target                     scheduleTargetOutput     `json:"Target"`
	ScheduleExpression         string                   `json:"ScheduleExpression"`
	Name                       string                   `json:"Name"`
	Arn                        string                   `json:"Arn"`
	GroupName                  string                   `json:"GroupName"`
	ScheduleExpressionTimezone string                   `json:"ScheduleExpressionTimezone,omitempty"`
	Description                string                   `json:"Description,omitempty"`
	State                      string                   `json:"State"`
	ActionAfterCompletion      string                   `json:"ActionAfterCompletion,omitempty"`
	KmsKeyArn                  string                   `json:"KmsKeyArn,omitempty"`
	FlexibleTimeWindow         flexibleTimeWindowOutput `json:"FlexibleTimeWindow"`
	LastModificationDate       float64                  `json:"LastModificationDate"`
	CreationDate               float64                  `json:"CreationDate"`
}

func (h *Handler) handleGetSchedule(_ context.Context, in *scheduleNameInput) (*getScheduleOutput, error) {
	s, err := h.Backend.GetSchedule(in.Name, in.GroupName)
	if err != nil {
		return nil, err
	}

	var tagMap map[string]string
	if s.Tags != nil {
		tagMap = s.Tags.Clone()
	}

	out := &getScheduleOutput{
		Name:                       s.Name,
		Arn:                        s.ARN,
		GroupName:                  s.GroupName,
		ScheduleExpression:         s.ScheduleExpression,
		ScheduleExpressionTimezone: s.ScheduleExpressionTimezone,
		Description:                s.Description,
		State:                      s.State,
		ActionAfterCompletion:      s.ActionAfterCompletion,
		KmsKeyArn:                  s.KmsKeyArn,
		CreationDate:               float64(s.CreationDate.Unix()),
		LastModificationDate:       float64(s.LastModificationDate.Unix()),
		Tags:                       tagMap,
		Target:                     targetToOutput(s.Target),
		FlexibleTimeWindow: flexibleTimeWindowOutput{
			Mode:                   s.FlexibleTimeWindow.Mode,
			MaximumWindowInMinutes: s.FlexibleTimeWindow.MaximumWindowInMinutes,
		},
	}

	if s.StartDate != nil {
		v := float64(s.StartDate.Unix())
		out.StartDate = &v
	}

	if s.EndDate != nil {
		v := float64(s.EndDate.Unix())
		out.EndDate = &v
	}

	return out, nil
}

type listSchedulesInput struct {
	GroupName  string `json:"GroupName"`
	NamePrefix string `json:"NamePrefix"`
	State      string `json:"State"`
	NextToken  string `json:"NextToken"`
	MaxResults string `json:"MaxResults"`
}

type scheduleSummary struct {
	Name                 string  `json:"Name"`
	Arn                  string  `json:"Arn"`
	GroupName            string  `json:"GroupName"`
	ScheduleExpression   string  `json:"ScheduleExpression"`
	State                string  `json:"State"`
	CreationDate         float64 `json:"CreationDate"`
	LastModificationDate float64 `json:"LastModificationDate"`
}

type listSchedulesOutput struct {
	NextToken string            `json:"NextToken,omitempty"`
	Schedules []scheduleSummary `json:"Schedules"`
}

func (h *Handler) handleListSchedules(_ context.Context, in *listSchedulesInput) (*listSchedulesOutput, error) {
	maxResults := parseMaxResults(in.MaxResults)
	schedules, nextToken := h.Backend.ListSchedules(in.GroupName, in.NamePrefix, in.State, in.NextToken, maxResults)
	items := make([]scheduleSummary, 0, len(schedules))

	for _, s := range schedules {
		items = append(items, scheduleSummary{
			Name:                 s.Name,
			Arn:                  s.ARN,
			GroupName:            s.GroupName,
			ScheduleExpression:   s.ScheduleExpression,
			State:                s.State,
			CreationDate:         float64(s.CreationDate.Unix()),
			LastModificationDate: float64(s.LastModificationDate.Unix()),
		})
	}

	return &listSchedulesOutput{Schedules: items, NextToken: nextToken}, nil
}

type deleteScheduleOutput struct{}

func (h *Handler) handleDeleteSchedule(_ context.Context, in *scheduleNameInput) (*deleteScheduleOutput, error) {
	if err := h.Backend.DeleteSchedule(in.Name, in.GroupName); err != nil {
		return nil, err
	}

	return &deleteScheduleOutput{}, nil
}

type updateScheduleOutput struct {
	ScheduleArn string `json:"ScheduleArn"`
}

func (h *Handler) handleUpdateSchedule(_ context.Context, in *scheduleInput) (*updateScheduleOutput, error) {
	var opts []ScheduleOption
	if in.StartDate != nil {
		opts = append(opts, WithStartDate(epochSecondsToTime(*in.StartDate)))
	}

	if in.EndDate != nil {
		opts = append(opts, WithEndDate(epochSecondsToTime(*in.EndDate)))
	}

	if in.ActionAfterCompletion != "" {
		opts = append(opts, WithActionAfterCompletion(in.ActionAfterCompletion))
	}

	if in.KmsKeyArn != "" {
		opts = append(opts, WithKmsKeyArn(in.KmsKeyArn))
	}

	s, err := h.Backend.UpdateSchedule(
		in.Name,
		in.GroupName,
		in.ScheduleExpression,
		in.Description,
		in.ScheduleExpressionTimezone,
		targetFromInput(in.Target),
		in.State,
		FlexibleTimeWindow{
			Mode:                   in.FlexibleTimeWindow.Mode,
			MaximumWindowInMinutes: in.FlexibleTimeWindow.MaximumWindowInMinutes,
		},
		opts...,
	)
	if err != nil {
		return nil, err
	}

	return &updateScheduleOutput{ScheduleArn: s.ARN}, nil
}

type handleTagResourceInput struct {
	Tags        map[string]string `json:"Tags"`
	ResourceArn string            `json:"ResourceArn"`
}

type tagResourceOutput struct{}

func (h *Handler) handleTagResource(_ context.Context, in *handleTagResourceInput) (*tagResourceOutput, error) {
	if err := h.Backend.TagResource(in.ResourceArn, in.Tags); err != nil {
		return nil, err
	}

	return &tagResourceOutput{}, nil
}

type handleListTagsForResourceInput struct {
	ResourceArn string `json:"ResourceArn"`
}

type listTagsForResourceOutput struct {
	Tags map[string]string `json:"Tags"`
}

func (h *Handler) handleListTagsForResource(
	_ context.Context,
	in *handleListTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	kv, err := h.Backend.ListTagsForResource(in.ResourceArn)
	if err != nil {
		return nil, err
	}

	return &listTagsForResourceOutput{Tags: kv}, nil
}

// handleUntagResource removes the specified tag keys from a resource.
type handleUntagResourceInput struct {
	ResourceArn string   `json:"ResourceArn"`
	TagKeys     []string `json:"TagKeys"`
}

type untagResourceOutput struct{}

func (h *Handler) handleUntagResource(_ context.Context, in *handleUntagResourceInput) (*untagResourceOutput, error) {
	if err := h.Backend.UntagResource(in.ResourceArn, in.TagKeys); err != nil {
		return nil, err
	}

	return &untagResourceOutput{}, nil
}

// Schedule group handlers.

type createScheduleGroupInput struct {
	Tags        map[string]string `json:"Tags"`
	Name        string            `json:"Name"`
	Description string            `json:"Description"`
}

type createScheduleGroupOutput struct {
	ScheduleGroupArn string `json:"ScheduleGroupArn"`
}

func (h *Handler) handleCreateScheduleGroup(
	_ context.Context,
	in *createScheduleGroupInput,
) (*createScheduleGroupOutput, error) {
	g, err := h.Backend.CreateScheduleGroup(in.Name, in.Description, in.Tags)
	if err != nil {
		return nil, err
	}

	return &createScheduleGroupOutput{ScheduleGroupArn: g.ARN}, nil
}

type scheduleGroupNameInput struct {
	Name string `json:"Name"`
}

type deleteScheduleGroupOutput struct{}

func (h *Handler) handleDeleteScheduleGroup(
	_ context.Context,
	in *scheduleGroupNameInput,
) (*deleteScheduleGroupOutput, error) {
	if err := h.Backend.DeleteScheduleGroup(in.Name); err != nil {
		return nil, err
	}

	return &deleteScheduleGroupOutput{}, nil
}

type getScheduleGroupOutput struct {
	Tags                 map[string]string `json:"Tags,omitempty"`
	Arn                  string            `json:"Arn"`
	Description          string            `json:"Description,omitempty"`
	Name                 string            `json:"Name"`
	State                string            `json:"State"`
	CreationDate         float64           `json:"CreationDate"`
	LastModificationDate float64           `json:"LastModificationDate"`
}

func (h *Handler) handleGetScheduleGroup(
	_ context.Context,
	in *scheduleGroupNameInput,
) (*getScheduleGroupOutput, error) {
	g, err := h.Backend.GetScheduleGroup(in.Name)
	if err != nil {
		return nil, err
	}

	var tagMap map[string]string
	if g.Tags != nil {
		tagMap = g.Tags.Clone()
	}

	return &getScheduleGroupOutput{
		Arn:                  g.ARN,
		CreationDate:         float64(g.CreationDate.Unix()),
		LastModificationDate: float64(g.LastModificationDate.Unix()),
		Description:          g.Description,
		Tags:                 tagMap,
		Name:                 g.Name,
		State:                g.State,
	}, nil
}

type listScheduleGroupsInput struct {
	NamePrefix string `json:"NamePrefix"`
	NextToken  string `json:"NextToken"`
	MaxResults string `json:"MaxResults"`
}

type scheduleGroupSummary struct {
	Tags                 map[string]string `json:"Tags,omitempty"`
	Arn                  string            `json:"Arn"`
	Name                 string            `json:"Name"`
	State                string            `json:"State"`
	CreationDate         float64           `json:"CreationDate"`
	LastModificationDate float64           `json:"LastModificationDate"`
}

type listScheduleGroupsOutput struct {
	NextToken      string                 `json:"NextToken,omitempty"`
	ScheduleGroups []scheduleGroupSummary `json:"ScheduleGroups"`
}

func (h *Handler) handleListScheduleGroups(
	_ context.Context,
	in *listScheduleGroupsInput,
) (*listScheduleGroupsOutput, error) {
	maxResults := parseMaxResults(in.MaxResults)
	groups, nextToken := h.Backend.ListScheduleGroups(in.NamePrefix, in.NextToken, maxResults)
	items := make([]scheduleGroupSummary, 0, len(groups))

	for _, g := range groups {
		var tagMap map[string]string
		if g.Tags != nil {
			tagMap = g.Tags.Clone()
		}

		items = append(items, scheduleGroupSummary{
			Arn:                  g.ARN,
			CreationDate:         float64(g.CreationDate.Unix()),
			LastModificationDate: float64(g.LastModificationDate.Unix()),
			Tags:                 tagMap,
			Name:                 g.Name,
			State:                g.State,
		})
	}

	return &listScheduleGroupsOutput{ScheduleGroups: items, NextToken: nextToken}, nil
}

// parseMaxResults converts the MaxResults string field to an integer.
// Returns 0 when the field is empty or non-numeric.
func parseMaxResults(s string) int {
	if s == "" {
		return 0
	}

	n := 0

	for _, c := range s {
		if c < '0' || c > '9' {
			return 0
		}

		n = n*10 + int(c-'0')
	}

	return n
}

// nanosPerSecond is the number of nanoseconds in a second.
const nanosPerSecond = 1e9

// epochSecondsToTime converts a float64 Unix epoch seconds value to [time.Time].
func epochSecondsToTime(epoch float64) time.Time {
	sec := int64(epoch)
	nsec := int64((epoch - float64(sec)) * nanosPerSecond)

	return time.Unix(sec, nsec).UTC()
}
