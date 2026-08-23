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
	"github.com/blackbirdworks/gopherstack/pkgs/safemap"
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

// Handler is the Echo HTTP handler for EventBridge Scheduler operations.
type Handler struct {
	Backend StorageBackend
	ops     map[string]service.JSONOpFunc
	runner  *Runner
	cancel  context.CancelFunc
	// idempotency caches successful CreateSchedule/CreateScheduleGroup ARNs by
	// ClientToken so a lost-response retry replays the original result instead of
	// failing with ConflictException on the now-existing name. See idempotency.go.
	idempotency *safemap.Map[string, idempotentResult]
}

// Runner returns the internal runner for cross-service wiring.
func (h *Handler) Runner() *Runner {
	return h.runner
}

// NewHandler creates a new Scheduler handler.
func NewHandler(backend StorageBackend) *Handler {
	h := &Handler{
		Backend:     backend,
		runner:      NewRunner(backend),
		idempotency: safemap.New[string, idempotentResult]("scheduler.idempotency"),
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
		return h.handleError(ctx, c, action, ErrNotFound)
	}

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		logger.Load(ctx).ErrorContext(ctx, "failed to read request body", "error", err)

		return h.handleError(ctx, c, action, err)
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
	// scheduler@v1.20.4 deserializers.go models ValidationException and
	// InternalServerException on every one of its 12 operations (verified by
	// scanning every awsRestjson1_deserializeOpError* switch), so both are safe
	// blanket fallbacks regardless of which operation reached this path.
	case errors.Is(err, errInvalidRequest), errors.Is(err, errUnknownAction),
		errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		payload, _ := json.Marshal(service.JSONErrorResponse{
			Type:    "ValidationException",
			Message: err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	default:
		payload, _ := json.Marshal(service.JSONErrorResponse{
			Type:    "InternalServerException",
			Message: err.Error(),
		})

		return c.JSONBlob(http.StatusInternalServerError, payload)
	}
}

type emptyOutput struct{}

func voidOp(fn func() error) (*emptyOutput, error) {
	if err := fn(); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
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
