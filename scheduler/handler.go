package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputil"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const schedulerTargetPrefix = "AWSScheduler."

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
)

type scheduleNameInput struct {
	Name string `json:"Name"`
}

type scheduleInput struct {
	Name               string `json:"Name"`
	ScheduleExpression string `json:"ScheduleExpression"`
	Target             struct {
		Arn     string `json:"Arn"`
		RoleArn string `json:"RoleArn"`
	} `json:"Target"`
	State              string `json:"State"`
	FlexibleTimeWindow struct {
		Mode                   string `json:"Mode"`
		MaximumWindowInMinutes int    `json:"MaximumWindowInMinutes"`
	} `json:"FlexibleTimeWindow"`
}

// Handler is the Echo HTTP handler for EventBridge Scheduler operations.
type Handler struct {
	Backend *InMemoryBackend
	Logger  *slog.Logger
}

// NewHandler creates a new Scheduler handler.
func NewHandler(backend *InMemoryBackend, log *slog.Logger) *Handler {
	return &Handler{Backend: backend, Logger: log}
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
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), schedulerTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the Scheduler action from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, schedulerTargetPrefix)
	if action == "" || action == target {
		return "Unknown"
	}

	return action
}

// ExtractResource extracts the schedule name from the request body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputil.ReadBody(c.Request())
	if err != nil {
		return ""
	}
	var req scheduleNameInput
	_ = json.Unmarshal(body, &req)

	return req.Name
}

// Handler returns the Echo handler function for Scheduler requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"Scheduler", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	var result any
	var err error

	switch action {
	case "CreateSchedule":
		result, err = service.HandleJSON(ctx, body, h.handleCreateSchedule)
	case "GetSchedule":
		result, err = service.HandleJSON(ctx, body, h.handleGetSchedule)
	case "ListSchedules":
		result, err = service.HandleJSON(ctx, body, h.handleListSchedules)
	case "DeleteSchedule":
		result, err = service.HandleJSON(ctx, body, h.handleDeleteSchedule)
	case "UpdateSchedule":
		result, err = service.HandleJSON(ctx, body, h.handleUpdateSchedule)
	case "TagResource":
		result, err = service.HandleJSON(ctx, body, h.handleTagResource)
	case "ListTagsForResource":
		result, err = service.HandleJSON(ctx, body, h.handleListTagsForResource)
	default:
		return nil, fmt.Errorf("%w: %s", errUnknownAction, action)
	}

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
		return c.JSON(http.StatusNotFound, map[string]string{"message": err.Error()})
	case errors.Is(err, ErrAlreadyExists):
		return c.JSON(http.StatusConflict, map[string]string{"message": err.Error()})
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

func (h *Handler) handleListTagsForResource(_ context.Context, in *handleListTagsForResourceInput) (*listTagsForResourceOutput, error) {
	tags, err := h.Backend.ListTagsForResource(in.ResourceArn)
	if err != nil {
		return nil, err
	}

	return &listTagsForResourceOutput{Tags: tags}, nil
}
