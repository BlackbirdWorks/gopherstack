package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
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

// resourceTag mirrors the wire shape EventBridge Scheduler uses for resource-level
// tags: CreateScheduleGroup.Tags, TagResource.Tags, and ListTagsForResource.Tags are
// all a JSON array of {"Key":..., "Value":...} objects, NOT a JSON map (unlike the
// per-target EcsParameters.Tags, which is already list-shaped, or many other AWS
// services' resource tags). See aws-sdk-go-v2/service/scheduler's
// awsRestjson1_(de)serializeDocumentTagList.
type resourceTag struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// tagsFromWire converts the wire []resourceTag list into the map[string]string the
// backend stores tags as. Duplicate keys keep the last value, matching map semantics.
func tagsFromWire(in []resourceTag) map[string]string {
	if len(in) == 0 {
		return nil
	}

	m := make(map[string]string, len(in))
	for _, t := range in {
		m[t.Key] = t.Value
	}

	return m
}

// tagsToWire converts a backend tag map into the wire []resourceTag list, sorted by
// key for deterministic output.
func tagsToWire(in map[string]string) []resourceTag {
	if len(in) == 0 {
		return nil
	}

	out := make([]resourceTag, 0, len(in))
	for k, v := range in {
		out = append(out, resourceTag{Key: k, Value: v})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })

	return out
}

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
	MessageGroupID string `json:"MessageGroupId,omitempty"`
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

// scheduleTargetEcsAwsvpcConfiguration mirrors EcsAwsvpcConfiguration for handler input/output.
type scheduleTargetEcsAwsvpcConfiguration struct {
	AssignPublicIP string   `json:"AssignPublicIp,omitempty"`
	SecurityGroups []string `json:"SecurityGroups,omitempty"`
	Subnets        []string `json:"Subnets,omitempty"`
}

// scheduleTargetEcsNetworkConfiguration mirrors EcsNetworkConfiguration for handler input/output.
type scheduleTargetEcsNetworkConfiguration struct {
	AwsvpcConfiguration *scheduleTargetEcsAwsvpcConfiguration `json:"AwsvpcConfiguration,omitempty"`
}

// scheduleTargetEcsCapacityProviderStrategyItem mirrors EcsCapacityProviderStrategyItem.
type scheduleTargetEcsCapacityProviderStrategyItem struct {
	CapacityProvider string `json:"CapacityProvider"`
	Base             int    `json:"Base,omitempty"`
	Weight           int    `json:"Weight,omitempty"`
}

// scheduleTargetEcsPlacementConstraint mirrors EcsPlacementConstraint for handler input/output.
type scheduleTargetEcsPlacementConstraint struct {
	Expression string `json:"Expression,omitempty"`
	Type       string `json:"Type,omitempty"`
}

// scheduleTargetEcsPlacementStrategy mirrors EcsPlacementStrategy for handler input/output.
type scheduleTargetEcsPlacementStrategy struct {
	Field string `json:"Field,omitempty"`
	Type  string `json:"Type,omitempty"`
}

// scheduleTargetEcsTag mirrors EcsTag for handler input/output.
type scheduleTargetEcsTag struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// scheduleTargetEcsParameters mirrors EcsParameters for handler input/output.
type scheduleTargetEcsParameters struct {
	NetworkConfiguration     *scheduleTargetEcsNetworkConfiguration          `json:"NetworkConfiguration,omitempty"`
	PropagateTags            string                                          `json:"PropagateTags,omitempty"`
	TaskDefinitionArn        string                                          `json:"TaskDefinitionArn,omitempty"`
	LaunchType               string                                          `json:"LaunchType,omitempty"`
	PlatformVersion          string                                          `json:"PlatformVersion,omitempty"`
	Group                    string                                          `json:"Group,omitempty"`
	ReferenceID              string                                          `json:"ReferenceId,omitempty"`
	PlacementConstraints     []scheduleTargetEcsPlacementConstraint          `json:"PlacementConstraints,omitempty"`
	PlacementStrategy        []scheduleTargetEcsPlacementStrategy            `json:"PlacementStrategy,omitempty"`
	Tags                     []scheduleTargetEcsTag                          `json:"Tags,omitempty"`
	CapacityProviderStrategy []scheduleTargetEcsCapacityProviderStrategyItem `json:"CapacityProviderStrategy,omitempty"`
	TaskCount                int                                             `json:"TaskCount,omitempty"`
	EnableECSManagedTags     bool                                            `json:"EnableECSManagedTags,omitempty"`
	EnableExecuteCommand     bool                                            `json:"EnableExecuteCommand,omitempty"`
}

// scheduleTarget holds the ARN, IAM role, and optional custom input for a schedule target.
type scheduleTarget struct {
	RetryPolicy                 *scheduleTargetRetryPolicy                 `json:"RetryPolicy,omitempty"`
	DeadLetterConfig            *scheduleTargetDeadLetterConfig            `json:"DeadLetterConfig,omitempty"`
	InputTransformer            *scheduleTargetInputTransformer            `json:"InputTransformer,omitempty"`
	EventBridgeParameters       *scheduleTargetEventBridgeParameters       `json:"EventBridgeParameters,omitempty"`
	KinesisParameters           *scheduleTargetKinesisParameters           `json:"KinesisParameters,omitempty"`
	SqsParameters               *scheduleTargetSqsParameters               `json:"SqsParameters,omitempty"`
	SageMakerPipelineParameters *scheduleTargetSageMakerPipelineParameters `json:"SageMakerPipelineParameters,omitempty"`
	EcsParameters               *scheduleTargetEcsParameters               `json:"EcsParameters,omitempty"`
	Arn                         string                                     `json:"Arn"`
	RoleArn                     string                                     `json:"RoleArn"`
	Input                       string                                     `json:"Input,omitempty"`
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

// Runner returns the internal runner for cross-service wiring.
func (h *Handler) Runner() *Runner {
	return h.runner
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
var (
	_ service.BackgroundWorker = (*Handler)(nil)
	_ service.Shutdowner       = (*Handler)(nil)
)

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

		ctx := h.contextWithRegion(c)

		return service.HandleTarget(
			c, logger.Load(ctx),
			"Scheduler", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			func(_ context.Context, action string, body []byte) ([]byte, error) {
				return h.dispatch(ctx, action, body)
			},
			h.handleError,
		)
	}
}

// contextWithRegion returns the request context with the resolved AWS region attached
// under regionContextKey so that backend operations are routed to the correct region.
func (h *Handler) contextWithRegion(c *echo.Context) context.Context {
	region := httputils.ExtractRegionFromRequest(c.Request(), h.Backend.Region())

	return context.WithValue(c.Request().Context(), regionContextKey{}, region)
}

// handleREST handles Scheduler REST API calls.
// It extracts path parameters from the URL, injects them into the request body,
// and dispatches to the existing handler logic.
func (h *Handler) handleREST(c *echo.Context) error {
	ctx := h.contextWithRegion(c)

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
func (h *Handler) enrichRESTBody(action, name string, body []byte, q url.Values) []byte {
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
func enrichScheduleByPath(m map[string]json.RawMessage, name string, q url.Values) {
	if name != "" {
		setJSONString(m, "Name", name)
	}

	if gn := q.Get("groupName"); gn != "" {
		setJSONString(m, "GroupName", gn)
	}
}

// enrichListSchedulesQuery injects ListSchedules query parameters into the JSON map.
func enrichListSchedulesQuery(m map[string]json.RawMessage, q url.Values) {
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
func enrichListScheduleGroupsQuery(m map[string]json.RawMessage, q url.Values) {
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

// enrichTagsPath injects ResourceArn from the URL path and tag keys from the query
// string. UntagResource sends TagKeys as a repeated query parameter
// (?TagKeys=a&TagKeys=b, see awsRestjson1_serializeOpHttpBindingsUntagResourceInput's
// encoder.AddQuery("TagKeys", ...)), not a single comma-separated value.
func enrichTagsPath(m map[string]json.RawMessage, name, action string, q url.Values) {
	if name != "" {
		setJSONString(m, "ResourceArn", name)
	}

	if action != opUntagResource {
		return
	}

	if keys := q["TagKeys"]; len(keys) > 0 {
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

func (h *Handler) handleCreateSchedule(ctx context.Context, in *scheduleInput) (*createScheduleOutput, error) {
	state := in.State
	if state == "" {
		state = scheduleStateEnabled
	}

	if err := validateActionAfterCompletion(in.ActionAfterCompletion); err != nil {
		return nil, err
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
		ctx,
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

// retryPolicyFromInput converts a handler retry policy to the backend type.
func retryPolicyFromInput(in *scheduleTargetRetryPolicy) *RetryPolicy {
	if in == nil {
		return nil
	}

	return &RetryPolicy{
		MaximumEventAgeInSeconds: in.MaximumEventAgeInSeconds,
		MaximumRetryAttempts:     in.MaximumRetryAttempts,
	}
}

// deadLetterConfigFromInput converts a handler DLQ config to the backend type.
func deadLetterConfigFromInput(in *scheduleTargetDeadLetterConfig) *DeadLetterConfig {
	if in == nil {
		return nil
	}

	return &DeadLetterConfig{Arn: in.Arn}
}

// inputTransformerFromInput converts a handler input transformer to the backend type.
func inputTransformerFromInput(in *scheduleTargetInputTransformer) *InputTransformer {
	if in == nil {
		return nil
	}

	return &InputTransformer{InputPathsMap: in.InputPathsMap, InputTemplate: in.InputTemplate}
}

// eventBridgeParamsFromInput converts handler EventBridge parameters to the backend type.
func eventBridgeParamsFromInput(in *scheduleTargetEventBridgeParameters) *EventBridgeParameters {
	if in == nil {
		return nil
	}

	return &EventBridgeParameters{DetailType: in.DetailType, Source: in.Source}
}

// kinesisParamsFromInput converts handler Kinesis parameters to the backend type.
func kinesisParamsFromInput(in *scheduleTargetKinesisParameters) *KinesisParameters {
	if in == nil {
		return nil
	}

	return &KinesisParameters{PartitionKey: in.PartitionKey}
}

// sqsParamsFromInput converts handler SQS parameters to the backend type.
func sqsParamsFromInput(in *scheduleTargetSqsParameters) *SqsParameters {
	if in == nil {
		return nil
	}

	return &SqsParameters{MessageGroupID: in.MessageGroupID}
}

// sageMakerParamsFromInput converts handler SageMaker parameters to the backend type.
func sageMakerParamsFromInput(in *scheduleTargetSageMakerPipelineParameters) *SageMakerPipelineParameters {
	if in == nil {
		return nil
	}

	params := make([]SageMakerPipelineParameter, len(in.PipelineParameterList))
	for i, p := range in.PipelineParameterList {
		params[i] = SageMakerPipelineParameter(p)
	}

	return &SageMakerPipelineParameters{PipelineParameterList: params}
}

// ecsNetworkConfigFromInput converts handler network configuration to the backend type.
func ecsNetworkConfigFromInput(in *scheduleTargetEcsNetworkConfiguration) *EcsNetworkConfiguration {
	if in == nil {
		return nil
	}

	out := &EcsNetworkConfiguration{}

	if in.AwsvpcConfiguration != nil {
		out.AwsvpcConfiguration = &EcsAwsvpcConfiguration{
			Subnets:        in.AwsvpcConfiguration.Subnets,
			SecurityGroups: in.AwsvpcConfiguration.SecurityGroups,
			AssignPublicIP: in.AwsvpcConfiguration.AssignPublicIP,
		}
	}

	return out
}

// ecsCapacityStrategyFromInput converts handler capacity provider strategy to the backend type.
func ecsCapacityStrategyFromInput(
	in []scheduleTargetEcsCapacityProviderStrategyItem,
) []EcsCapacityProviderStrategyItem {
	if len(in) == 0 {
		return nil
	}

	out := make([]EcsCapacityProviderStrategyItem, len(in))
	for i, item := range in {
		out[i] = EcsCapacityProviderStrategyItem(item)
	}

	return out
}

// ecsPlacementConstraintsFromInput converts handler placement constraints to the backend type.
func ecsPlacementConstraintsFromInput(in []scheduleTargetEcsPlacementConstraint) []EcsPlacementConstraint {
	if len(in) == 0 {
		return nil
	}

	out := make([]EcsPlacementConstraint, len(in))
	for i, c := range in {
		out[i] = EcsPlacementConstraint(c)
	}

	return out
}

// ecsPlacementStrategyFromInput converts handler placement strategy to the backend type.
func ecsPlacementStrategyFromInput(in []scheduleTargetEcsPlacementStrategy) []EcsPlacementStrategy {
	if len(in) == 0 {
		return nil
	}

	out := make([]EcsPlacementStrategy, len(in))
	for i, s := range in {
		out[i] = EcsPlacementStrategy(s)
	}

	return out
}

// ecsTagsFromInput converts handler ECS tags to the backend type.
func ecsTagsFromInput(in []scheduleTargetEcsTag) []EcsTag {
	if len(in) == 0 {
		return nil
	}

	out := make([]EcsTag, len(in))
	for i, t := range in {
		out[i] = EcsTag(t)
	}

	return out
}

// ecsParamsFromInput converts handler ECS parameters to the backend type.
func ecsParamsFromInput(in *scheduleTargetEcsParameters) *EcsParameters {
	if in == nil {
		return nil
	}

	return &EcsParameters{
		TaskDefinitionArn:        in.TaskDefinitionArn,
		LaunchType:               in.LaunchType,
		TaskCount:                in.TaskCount,
		PlatformVersion:          in.PlatformVersion,
		Group:                    in.Group,
		PropagateTags:            in.PropagateTags,
		ReferenceID:              in.ReferenceID,
		EnableECSManagedTags:     in.EnableECSManagedTags,
		EnableExecuteCommand:     in.EnableExecuteCommand,
		NetworkConfiguration:     ecsNetworkConfigFromInput(in.NetworkConfiguration),
		CapacityProviderStrategy: ecsCapacityStrategyFromInput(in.CapacityProviderStrategy),
		PlacementConstraints:     ecsPlacementConstraintsFromInput(in.PlacementConstraints),
		PlacementStrategy:        ecsPlacementStrategyFromInput(in.PlacementStrategy),
		Tags:                     ecsTagsFromInput(in.Tags),
	}
}

// targetFromInput converts a handler scheduleTarget into the backend Target type.
func targetFromInput(in scheduleTarget) Target {
	return Target{
		ARN:                         in.Arn,
		RoleARN:                     in.RoleArn,
		Input:                       in.Input,
		RetryPolicy:                 retryPolicyFromInput(in.RetryPolicy),
		DeadLetterConfig:            deadLetterConfigFromInput(in.DeadLetterConfig),
		InputTransformer:            inputTransformerFromInput(in.InputTransformer),
		EventBridgeParameters:       eventBridgeParamsFromInput(in.EventBridgeParameters),
		KinesisParameters:           kinesisParamsFromInput(in.KinesisParameters),
		SqsParameters:               sqsParamsFromInput(in.SqsParameters),
		SageMakerPipelineParameters: sageMakerParamsFromInput(in.SageMakerPipelineParameters),
		EcsParameters:               ecsParamsFromInput(in.EcsParameters),
	}
}

// retryPolicyToOutput converts a backend retry policy to the handler output type.
func retryPolicyToOutput(r *RetryPolicy) *scheduleTargetRetryPolicy {
	if r == nil {
		return nil
	}

	return &scheduleTargetRetryPolicy{
		MaximumEventAgeInSeconds: r.MaximumEventAgeInSeconds,
		MaximumRetryAttempts:     r.MaximumRetryAttempts,
	}
}

// deadLetterConfigToOutput converts a backend DLQ config to the handler output type.
func deadLetterConfigToOutput(d *DeadLetterConfig) *scheduleTargetDeadLetterConfig {
	if d == nil {
		return nil
	}

	return &scheduleTargetDeadLetterConfig{Arn: d.Arn}
}

// inputTransformerToOutput converts a backend input transformer to the handler output type.
func inputTransformerToOutput(t *InputTransformer) *scheduleTargetInputTransformer {
	if t == nil {
		return nil
	}

	return &scheduleTargetInputTransformer{InputPathsMap: t.InputPathsMap, InputTemplate: t.InputTemplate}
}

// eventBridgeParamsToOutput converts backend EventBridge parameters to the handler output type.
func eventBridgeParamsToOutput(e *EventBridgeParameters) *scheduleTargetEventBridgeParameters {
	if e == nil {
		return nil
	}

	return &scheduleTargetEventBridgeParameters{DetailType: e.DetailType, Source: e.Source}
}

// kinesisParamsToOutput converts backend Kinesis parameters to the handler output type.
func kinesisParamsToOutput(k *KinesisParameters) *scheduleTargetKinesisParameters {
	if k == nil {
		return nil
	}

	return &scheduleTargetKinesisParameters{PartitionKey: k.PartitionKey}
}

// sqsParamsToOutput converts backend SQS parameters to the handler output type.
func sqsParamsToOutput(s *SqsParameters) *scheduleTargetSqsParameters {
	if s == nil {
		return nil
	}

	return &scheduleTargetSqsParameters{MessageGroupID: s.MessageGroupID}
}

// sageMakerParamsToOutput converts backend SageMaker parameters to the handler output type.
func sageMakerParamsToOutput(s *SageMakerPipelineParameters) *scheduleTargetSageMakerPipelineParameters {
	if s == nil {
		return nil
	}

	params := make([]scheduleTargetSageMakerPipelineParam, len(s.PipelineParameterList))
	for i, p := range s.PipelineParameterList {
		params[i] = scheduleTargetSageMakerPipelineParam(p)
	}

	return &scheduleTargetSageMakerPipelineParameters{PipelineParameterList: params}
}

// ecsNetworkConfigToOutput converts backend network configuration to the handler output type.
func ecsNetworkConfigToOutput(in *EcsNetworkConfiguration) *scheduleTargetEcsNetworkConfiguration {
	if in == nil {
		return nil
	}

	out := &scheduleTargetEcsNetworkConfiguration{}

	if in.AwsvpcConfiguration != nil {
		out.AwsvpcConfiguration = &scheduleTargetEcsAwsvpcConfiguration{
			Subnets:        in.AwsvpcConfiguration.Subnets,
			SecurityGroups: in.AwsvpcConfiguration.SecurityGroups,
			AssignPublicIP: in.AwsvpcConfiguration.AssignPublicIP,
		}
	}

	return out
}

// ecsCapacityStrategyToOutput converts backend capacity provider strategy to the handler output type.
func ecsCapacityStrategyToOutput(in []EcsCapacityProviderStrategyItem) []scheduleTargetEcsCapacityProviderStrategyItem {
	if len(in) == 0 {
		return nil
	}

	out := make([]scheduleTargetEcsCapacityProviderStrategyItem, len(in))
	for i, item := range in {
		out[i] = scheduleTargetEcsCapacityProviderStrategyItem(item)
	}

	return out
}

// ecsPlacementConstraintsToOutput converts backend placement constraints to the handler output type.
func ecsPlacementConstraintsToOutput(in []EcsPlacementConstraint) []scheduleTargetEcsPlacementConstraint {
	if len(in) == 0 {
		return nil
	}

	out := make([]scheduleTargetEcsPlacementConstraint, len(in))
	for i, c := range in {
		out[i] = scheduleTargetEcsPlacementConstraint(c)
	}

	return out
}

// ecsPlacementStrategyToOutput converts backend placement strategy to the handler output type.
func ecsPlacementStrategyToOutput(in []EcsPlacementStrategy) []scheduleTargetEcsPlacementStrategy {
	if len(in) == 0 {
		return nil
	}

	out := make([]scheduleTargetEcsPlacementStrategy, len(in))
	for i, s := range in {
		out[i] = scheduleTargetEcsPlacementStrategy(s)
	}

	return out
}

// ecsTagsToOutput converts backend ECS tags to the handler output type.
func ecsTagsToOutput(in []EcsTag) []scheduleTargetEcsTag {
	if len(in) == 0 {
		return nil
	}

	out := make([]scheduleTargetEcsTag, len(in))
	for i, t := range in {
		out[i] = scheduleTargetEcsTag(t)
	}

	return out
}

// ecsParamsToOutput converts backend ECS parameters to the handler output type.
func ecsParamsToOutput(e *EcsParameters) *scheduleTargetEcsParameters {
	if e == nil {
		return nil
	}

	return &scheduleTargetEcsParameters{
		TaskDefinitionArn:        e.TaskDefinitionArn,
		LaunchType:               e.LaunchType,
		TaskCount:                e.TaskCount,
		PlatformVersion:          e.PlatformVersion,
		Group:                    e.Group,
		PropagateTags:            e.PropagateTags,
		ReferenceID:              e.ReferenceID,
		EnableECSManagedTags:     e.EnableECSManagedTags,
		EnableExecuteCommand:     e.EnableExecuteCommand,
		NetworkConfiguration:     ecsNetworkConfigToOutput(e.NetworkConfiguration),
		CapacityProviderStrategy: ecsCapacityStrategyToOutput(e.CapacityProviderStrategy),
		PlacementConstraints:     ecsPlacementConstraintsToOutput(e.PlacementConstraints),
		PlacementStrategy:        ecsPlacementStrategyToOutput(e.PlacementStrategy),
		Tags:                     ecsTagsToOutput(e.Tags),
	}
}

// targetToOutput converts a backend Target into the handler output type.
func targetToOutput(t Target) scheduleTargetOutput {
	return scheduleTargetOutput{
		Arn:                         t.ARN,
		RoleArn:                     t.RoleARN,
		Input:                       t.Input,
		RetryPolicy:                 retryPolicyToOutput(t.RetryPolicy),
		DeadLetterConfig:            deadLetterConfigToOutput(t.DeadLetterConfig),
		InputTransformer:            inputTransformerToOutput(t.InputTransformer),
		EventBridgeParameters:       eventBridgeParamsToOutput(t.EventBridgeParameters),
		KinesisParameters:           kinesisParamsToOutput(t.KinesisParameters),
		SqsParameters:               sqsParamsToOutput(t.SqsParameters),
		SageMakerPipelineParameters: sageMakerParamsToOutput(t.SageMakerPipelineParameters),
		EcsParameters:               ecsParamsToOutput(t.EcsParameters),
	}
}

type scheduleTargetOutput struct {
	RetryPolicy                 *scheduleTargetRetryPolicy                 `json:"RetryPolicy,omitempty"`
	DeadLetterConfig            *scheduleTargetDeadLetterConfig            `json:"DeadLetterConfig,omitempty"`
	InputTransformer            *scheduleTargetInputTransformer            `json:"InputTransformer,omitempty"`
	EventBridgeParameters       *scheduleTargetEventBridgeParameters       `json:"EventBridgeParameters,omitempty"`
	KinesisParameters           *scheduleTargetKinesisParameters           `json:"KinesisParameters,omitempty"`
	SqsParameters               *scheduleTargetSqsParameters               `json:"SqsParameters,omitempty"`
	SageMakerPipelineParameters *scheduleTargetSageMakerPipelineParameters `json:"SageMakerPipelineParameters,omitempty"`
	EcsParameters               *scheduleTargetEcsParameters               `json:"EcsParameters,omitempty"`
	Arn                         string                                     `json:"Arn"`
	RoleArn                     string                                     `json:"RoleArn"`
	Input                       string                                     `json:"Input,omitempty"`
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

func (h *Handler) handleGetSchedule(ctx context.Context, in *scheduleNameInput) (*getScheduleOutput, error) {
	s, err := h.Backend.GetSchedule(ctx, in.Name, in.GroupName)
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

// scheduleSummaryTarget holds the target summary included in ListSchedules items.
type scheduleSummaryTarget struct {
	Arn     string `json:"Arn"`
	RoleArn string `json:"RoleArn"`
}

type scheduleSummary struct {
	Target               scheduleSummaryTarget `json:"Target"`
	Name                 string                `json:"Name"`
	Arn                  string                `json:"Arn"`
	GroupName            string                `json:"GroupName"`
	ScheduleExpression   string                `json:"ScheduleExpression"`
	State                string                `json:"State"`
	CreationDate         float64               `json:"CreationDate"`
	LastModificationDate float64               `json:"LastModificationDate"`
}

type listSchedulesOutput struct {
	NextToken string            `json:"NextToken,omitempty"`
	Schedules []scheduleSummary `json:"Schedules"`
}

func (h *Handler) handleListSchedules(ctx context.Context, in *listSchedulesInput) (*listSchedulesOutput, error) {
	maxResults := parseMaxResults(in.MaxResults)
	schedules, nextToken := h.Backend.ListSchedules(
		ctx,
		in.GroupName,
		in.NamePrefix,
		in.State,
		in.NextToken,
		maxResults,
	)
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
			Target: scheduleSummaryTarget{
				Arn:     s.Target.ARN,
				RoleArn: s.Target.RoleARN,
			},
		})
	}

	return &listSchedulesOutput{Schedules: items, NextToken: nextToken}, nil
}

type emptyOutput struct{}

func voidOp(fn func() error) (*emptyOutput, error) {
	if err := fn(); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

func (h *Handler) handleDeleteSchedule(ctx context.Context, in *scheduleNameInput) (*emptyOutput, error) {
	return voidOp(func() error { return h.Backend.DeleteSchedule(ctx, in.Name, in.GroupName) })
}

type updateScheduleOutput struct {
	ScheduleArn string `json:"ScheduleArn"`
}

func (h *Handler) handleUpdateSchedule(ctx context.Context, in *scheduleInput) (*updateScheduleOutput, error) {
	if err := validateActionAfterCompletion(in.ActionAfterCompletion); err != nil {
		return nil, err
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

	s, err := h.Backend.UpdateSchedule(
		ctx,
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
	ResourceArn string        `json:"ResourceArn"`
	Tags        []resourceTag `json:"Tags"`
}

func (h *Handler) handleTagResource(ctx context.Context, in *handleTagResourceInput) (*emptyOutput, error) {
	return voidOp(func() error { return h.Backend.TagResource(ctx, in.ResourceArn, tagsFromWire(in.Tags)) })
}

type handleListTagsForResourceInput struct {
	ResourceArn string `json:"ResourceArn"`
}

type listTagsForResourceOutput struct {
	Tags []resourceTag `json:"Tags"`
}

func (h *Handler) handleListTagsForResource(
	ctx context.Context,
	in *handleListTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	kv, err := h.Backend.ListTagsForResource(ctx, in.ResourceArn)
	if err != nil {
		return nil, err
	}

	return &listTagsForResourceOutput{Tags: tagsToWire(kv)}, nil
}

// handleUntagResource removes the specified tag keys from a resource.
type handleUntagResourceInput struct {
	ResourceArn string   `json:"ResourceArn"`
	TagKeys     []string `json:"TagKeys"`
}

func (h *Handler) handleUntagResource(ctx context.Context, in *handleUntagResourceInput) (*emptyOutput, error) {
	return voidOp(func() error { return h.Backend.UntagResource(ctx, in.ResourceArn, in.TagKeys) })
}

// Schedule group handlers.

type createScheduleGroupInput struct {
	Name        string        `json:"Name"`
	Description string        `json:"Description"`
	Tags        []resourceTag `json:"Tags"`
}

type createScheduleGroupOutput struct {
	ScheduleGroupArn string `json:"ScheduleGroupArn"`
}

func (h *Handler) handleCreateScheduleGroup(
	ctx context.Context,
	in *createScheduleGroupInput,
) (*createScheduleGroupOutput, error) {
	g, err := h.Backend.CreateScheduleGroup(ctx, in.Name, in.Description, tagsFromWire(in.Tags))
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
	ctx context.Context,
	in *scheduleGroupNameInput,
) (*deleteScheduleGroupOutput, error) {
	if err := h.Backend.DeleteScheduleGroup(ctx, in.Name); err != nil {
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
	ctx context.Context,
	in *scheduleGroupNameInput,
) (*getScheduleGroupOutput, error) {
	g, err := h.Backend.GetScheduleGroup(ctx, in.Name)
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
	ctx context.Context,
	in *listScheduleGroupsInput,
) (*listScheduleGroupsOutput, error) {
	maxResults := parseMaxResults(in.MaxResults)
	groups, nextToken := h.Backend.ListScheduleGroups(ctx, in.NamePrefix, in.NextToken, maxResults)
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

// decimalBase is the base for decimal integer parsing.
const decimalBase = 10

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

		n = n*decimalBase + int(c-'0')
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
