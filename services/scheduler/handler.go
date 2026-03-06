package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputil"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	schedulerTargetPrefix = "AWSScheduler."
	schedulerPathSegment  = "schedules"
	// schedulesPathMinSegments is the minimum number of URL path segments in a
	// /schedules/{name} REST path: ["schedules", "{name}"].
	schedulesPathMinSegments = 2
)

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
)

type scheduleNameInput struct {
	Name string `json:"Name"`
}

// scheduleTarget holds the ARN and IAM role for a schedule target.
type scheduleTarget struct {
	Arn     string `json:"Arn"`
	RoleArn string `json:"RoleArn"`
}

// scheduleFlexibleTimeWindow holds the flexible time window configuration for a schedule.
type scheduleFlexibleTimeWindow struct {
	Mode                   string `json:"Mode"`
	MaximumWindowInMinutes int    `json:"MaximumWindowInMinutes"`
}

type scheduleInput struct {
	Name               string                     `json:"Name"`
	ScheduleExpression string                     `json:"ScheduleExpression"`
	Target             scheduleTarget             `json:"Target"`
	State              string                     `json:"State"`
	FlexibleTimeWindow scheduleFlexibleTimeWindow `json:"FlexibleTimeWindow"`
}

// Handler is the Echo HTTP handler for EventBridge Scheduler operations.
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates a new Scheduler handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "Scheduler" }

// GetSupportedOperations returns the list of supported Scheduler operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateSchedule",
		"GetSchedule",
		"ListSchedules",
		"DeleteSchedule",
		"UpdateSchedule",
		"TagResource",
		"ListTagsForResource",
	}
}

// RouteMatcher returns a function that matches Scheduler requests.
// Matches both X-Amz-Target (JSON protocol) and REST API paths (/schedules/...).
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		if strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), schedulerTargetPrefix) {
			return true
		}

		path := c.Request().URL.Path

		return strings.HasPrefix(path, "/"+schedulerPathSegment)
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

// ExtractResource extracts the schedule name from the request body or REST path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	// For REST paths extract name from the URL path segment.
	if !strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), schedulerTargetPrefix) {
		parts := strings.Split(strings.TrimPrefix(c.Request().URL.Path, "/"), "/")
		if len(parts) >= schedulesPathMinSegments {
			return parts[1]
		}
	}

	body, err := httputil.ReadBody(c.Request())
	if err != nil {
		return ""
	}
	var req scheduleNameInput
	_ = json.Unmarshal(body, &req)

	return req.Name
}

// parseSchedulerRESTPath maps an HTTP method + path to a Scheduler operation name and
// extracts the schedule name (if present in the path).
// Returns ("Unknown", "") when no pattern matches.
//
//nolint:cyclop // path routing table has necessary branches for each HTTP method + resource combination
func parseSchedulerRESTPath(method, path string) (string, string) {
	// Strip leading slash and split into segments.
	segments := strings.Split(strings.TrimPrefix(path, "/"), "/")
	switch {
	// GET /schedules or GET /schedules/ → ListSchedules
	case method == http.MethodGet && len(segments) >= 1 && segments[0] == schedulerPathSegment &&
		(len(segments) == 1 || (len(segments) == 2 && segments[1] == "")):
		return "ListSchedules", ""
	// POST /schedules/{name} → CreateSchedule
	case method == http.MethodPost && len(segments) == schedulesPathMinSegments && segments[0] == schedulerPathSegment:
		return "CreateSchedule", segments[1]
	// GET /schedules/{name} → GetSchedule
	case method == http.MethodGet && len(segments) == schedulesPathMinSegments && segments[0] == schedulerPathSegment:
		return "GetSchedule", segments[1]
	// DELETE /schedules/{name} → DeleteSchedule
	case method == http.MethodDelete && len(segments) == schedulesPathMinSegments && segments[0] == schedulerPathSegment:
		return "DeleteSchedule", segments[1]
	// PUT /schedules/{name} → UpdateSchedule
	case method == http.MethodPut && len(segments) == schedulesPathMinSegments && segments[0] == schedulerPathSegment:
		return "UpdateSchedule", segments[1]
	}

	return "Unknown", ""
}

// Handler returns the Echo handler function for Scheduler requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		// REST API path: /schedules or /schedules/{name}
		if strings.HasPrefix(c.Request().URL.Path, "/"+schedulerPathSegment) &&
			!strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), schedulerTargetPrefix) {
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

// handleREST handles Scheduler REST API calls (/schedules/{name}).
// It extracts the schedule name from the URL path, injects it into the request body,
// and dispatches to the existing handler logic.
func (h *Handler) handleREST(c *echo.Context) error {
	ctx := c.Request().Context()

	action, name := parseSchedulerRESTPath(c.Request().Method, c.Request().URL.Path)
	if action == "Unknown" {
		return c.String(http.StatusNotFound, "not found")
	}

	body, err := httputil.ReadBody(c.Request())
	if err != nil {
		logger.Load(ctx).ErrorContext(ctx, "failed to read request body", "error", err)

		return c.String(http.StatusInternalServerError, "internal server error")
	}

	// For operations that identify a schedule by URL path (not request body),
	// inject the name into the JSON body so existing handlers can read it.
	if name != "" {
		body = injectJSONField(body, "Name", name)
	}

	response, dispErr := h.dispatch(ctx, action, body)
	if dispErr != nil {
		return h.handleError(ctx, c, action, dispErr)
	}

	return c.JSONBlob(http.StatusOK, response)
}

// injectJSONField merges a key/value string pair into a JSON object body.
// If body is empty or not a valid JSON object, it returns {"key":"value"}.
func injectJSONField(body []byte, key, value string) []byte {
	var m map[string]json.RawMessage
	if len(body) > 0 {
		if err := json.Unmarshal(body, &m); err != nil {
			m = make(map[string]json.RawMessage)
		}
	} else {
		m = make(map[string]json.RawMessage)
	}

	quoted, _ := json.Marshal(value)
	m[key] = json.RawMessage(quoted)

	result, _ := json.Marshal(m)

	return result
}

func (h *Handler) dispatchTable() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateSchedule":      service.WrapOp(h.handleCreateSchedule),
		"GetSchedule":         service.WrapOp(h.handleGetSchedule),
		"ListSchedules":       service.WrapOp(h.handleListSchedules),
		"DeleteSchedule":      service.WrapOp(h.handleDeleteSchedule),
		"UpdateSchedule":      service.WrapOp(h.handleUpdateSchedule),
		"TagResource":         service.WrapOp(h.handleTagResource),
		"ListTagsForResource": service.WrapOp(h.handleListTagsForResource),
	}
}

func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.dispatchTable()[action]
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
		state = "ENABLED"
	}

	s, err := h.Backend.CreateSchedule(
		in.Name,
		in.ScheduleExpression,
		Target{ARN: in.Target.Arn, RoleARN: in.Target.RoleArn},
		state,
		FlexibleTimeWindow{
			Mode:                   in.FlexibleTimeWindow.Mode,
			MaximumWindowInMinutes: in.FlexibleTimeWindow.MaximumWindowInMinutes,
		},
	)
	if err != nil {
		return nil, err
	}

	return &createScheduleOutput{ScheduleArn: s.ARN}, nil
}

type scheduleTargetOutput struct {
	Arn     string `json:"Arn"`
	RoleArn string `json:"RoleArn"`
}

type flexibleTimeWindowOutput struct {
	Mode                   string `json:"Mode"`
	MaximumWindowInMinutes int    `json:"MaximumWindowInMinutes"`
}

type getScheduleOutput struct {
	Name               string                   `json:"Name"`
	Arn                string                   `json:"Arn"`
	ScheduleExpression string                   `json:"ScheduleExpression"`
	State              string                   `json:"State"`
	Target             scheduleTargetOutput     `json:"Target"`
	FlexibleTimeWindow flexibleTimeWindowOutput `json:"FlexibleTimeWindow"`
}

func (h *Handler) handleGetSchedule(_ context.Context, in *scheduleNameInput) (*getScheduleOutput, error) {
	s, err := h.Backend.GetSchedule(in.Name)
	if err != nil {
		return nil, err
	}

	return &getScheduleOutput{
		Name:               s.Name,
		Arn:                s.ARN,
		ScheduleExpression: s.ScheduleExpression,
		State:              s.State,
		Target:             scheduleTargetOutput{Arn: s.Target.ARN, RoleArn: s.Target.RoleARN},
		FlexibleTimeWindow: flexibleTimeWindowOutput{
			Mode:                   s.FlexibleTimeWindow.Mode,
			MaximumWindowInMinutes: s.FlexibleTimeWindow.MaximumWindowInMinutes,
		},
	}, nil
}

type listSchedulesInput struct{}

type scheduleSummary struct {
	Name               string `json:"Name"`
	Arn                string `json:"Arn"`
	ScheduleExpression string `json:"ScheduleExpression"`
	State              string `json:"State"`
}

type listSchedulesOutput struct {
	Schedules []scheduleSummary `json:"Schedules"`
}

func (h *Handler) handleListSchedules(_ context.Context, _ *listSchedulesInput) (*listSchedulesOutput, error) {
	schedules := h.Backend.ListSchedules()
	items := make([]scheduleSummary, 0, len(schedules))
	for _, s := range schedules {
		items = append(items, scheduleSummary{
			Name:               s.Name,
			Arn:                s.ARN,
			ScheduleExpression: s.ScheduleExpression,
			State:              s.State,
		})
	}

	return &listSchedulesOutput{Schedules: items}, nil
}

type deleteScheduleOutput struct{}

func (h *Handler) handleDeleteSchedule(_ context.Context, in *scheduleNameInput) (*deleteScheduleOutput, error) {
	if err := h.Backend.DeleteSchedule(in.Name); err != nil {
		return nil, err
	}

	return &deleteScheduleOutput{}, nil
}

type updateScheduleOutput struct {
	ScheduleArn string `json:"ScheduleArn"`
}

func (h *Handler) handleUpdateSchedule(_ context.Context, in *scheduleInput) (*updateScheduleOutput, error) {
	s, err := h.Backend.UpdateSchedule(
		in.Name,
		in.ScheduleExpression,
		Target{ARN: in.Target.Arn, RoleARN: in.Target.RoleArn},
		in.State,
		FlexibleTimeWindow{
			Mode:                   in.FlexibleTimeWindow.Mode,
			MaximumWindowInMinutes: in.FlexibleTimeWindow.MaximumWindowInMinutes,
		},
	)
	if err != nil {
		return nil, err
	}

	return &updateScheduleOutput{ScheduleArn: s.ARN}, nil
}

type handleTagResourceInput struct {
	Tags        *tags.Tags `json:"Tags"`
	ResourceArn string     `json:"ResourceArn"`
}

type tagResourceOutput struct{}

func (h *Handler) handleTagResource(_ context.Context, in *handleTagResourceInput) (*tagResourceOutput, error) {
	var kv map[string]string
	if in.Tags != nil {
		kv = in.Tags.Clone()
	}

	if err := h.Backend.TagResource(in.ResourceArn, kv); err != nil {
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
	tags, err := h.Backend.ListTagsForResource(in.ResourceArn)
	if err != nil {
		return nil, err
	}

	return &listTagsForResourceOutput{Tags: tags}, nil
}
