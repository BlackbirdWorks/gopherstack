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

func (h *Handler) dispatch(_ context.Context, action string, body []byte) ([]byte, error) {
	var result any
	var err error

	switch action {
	case "CreateSchedule":
		result, err = h.handleCreateSchedule(body)
	case "GetSchedule":
		result, err = h.handleGetSchedule(body)
	case "ListSchedules":
		result, err = h.handleListSchedules()
	case "DeleteSchedule":
		result, err = h.handleDeleteSchedule(body)
	case "UpdateSchedule":
		result, err = h.handleUpdateSchedule(body)
	case "TagResource":
		result, err = h.handleTagResource(body)
	case "ListTagsForResource":
		result, err = h.handleListTagsForResource(body)
	default:
		return nil, fmt.Errorf("%w: %s", errUnknownAction, action)
	}

	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleError(_ context.Context, c *echo.Context, _ string, err error) error {
	switch {
	case errors.Is(err, ErrNotFound):
		return c.JSON(http.StatusNotFound, map[string]string{"message": err.Error()})
	case errors.Is(err, ErrAlreadyExists):
		return c.JSON(http.StatusConflict, map[string]string{"message": err.Error()})
	case errors.Is(err, errInvalidRequest), errors.Is(err, errUnknownAction):
		return c.JSON(http.StatusBadRequest, map[string]string{"message": err.Error()})
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}
}

func (h *Handler) handleCreateSchedule(body []byte) (any, error) {
	var req scheduleInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, errInvalidRequest
	}

	state := req.State
	if state == "" {
		state = "ENABLED"
	}

	s, err := h.Backend.CreateSchedule(
		req.Name,
		req.ScheduleExpression,
		Target{ARN: req.Target.Arn, RoleARN: req.Target.RoleArn},
		state,
		FlexibleTimeWindow{
			Mode:                   req.FlexibleTimeWindow.Mode,
			MaximumWindowInMinutes: req.FlexibleTimeWindow.MaximumWindowInMinutes,
		},
	)
	if err != nil {
		return nil, err
	}

	return map[string]string{"ScheduleArn": s.ARN}, nil
}

func (h *Handler) handleGetSchedule(body []byte) (any, error) {
	var req scheduleNameInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, errInvalidRequest
	}

	s, err := h.Backend.GetSchedule(req.Name)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"Name":               s.Name,
		"Arn":                s.ARN,
		"ScheduleExpression": s.ScheduleExpression,
		"State":              s.State,
		"Target": map[string]string{
			"Arn":     s.Target.ARN,
			"RoleArn": s.Target.RoleARN,
		},
		"FlexibleTimeWindow": map[string]any{
			"Mode":                   s.FlexibleTimeWindow.Mode,
			"MaximumWindowInMinutes": s.FlexibleTimeWindow.MaximumWindowInMinutes,
		},
	}, nil
}

//nolint:unparam // error returned for consistent dispatch signature
func (h *Handler) handleListSchedules() (any, error) {
	schedules := h.Backend.ListSchedules()
	items := make([]map[string]any, 0, len(schedules))
	for _, s := range schedules {
		items = append(items, map[string]any{
			"Name":               s.Name,
			"Arn":                s.ARN,
			"ScheduleExpression": s.ScheduleExpression,
			"State":              s.State,
		})
	}

	return map[string]any{
		"Schedules": items,
	}, nil
}

func (h *Handler) handleDeleteSchedule(body []byte) (any, error) {
	var req scheduleNameInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, errInvalidRequest
	}

	if err := h.Backend.DeleteSchedule(req.Name); err != nil {
		return nil, err
	}

	return map[string]string{}, nil
}

func (h *Handler) handleUpdateSchedule(body []byte) (any, error) {
	var req scheduleInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, errInvalidRequest
	}

	s, err := h.Backend.UpdateSchedule(
		req.Name,
		req.ScheduleExpression,
		Target{ARN: req.Target.Arn, RoleARN: req.Target.RoleArn},
		req.State,
		FlexibleTimeWindow{
			Mode:                   req.FlexibleTimeWindow.Mode,
			MaximumWindowInMinutes: req.FlexibleTimeWindow.MaximumWindowInMinutes,
		},
	)
	if err != nil {
		return nil, err
	}

	return map[string]string{"ScheduleArn": s.ARN}, nil
}

type handleTagResourceInput struct {
	Tags        map[string]string `json:"Tags"`
	ResourceArn string            `json:"ResourceArn"`
}

func (h *Handler) handleTagResource(body []byte) (any, error) {
	var req handleTagResourceInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, errInvalidRequest
	}

	if err := h.Backend.TagResource(req.ResourceArn, req.Tags); err != nil {
		return nil, err
	}

	return map[string]string{}, nil
}

type handleListTagsForResourceInput struct {
	ResourceArn string `json:"ResourceArn"`
}

func (h *Handler) handleListTagsForResource(body []byte) (any, error) {
	var req handleListTagsForResourceInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, errInvalidRequest
	}

	tags, err := h.Backend.ListTagsForResource(req.ResourceArn)
	if err != nil {
		return nil, err
	}

	return map[string]any{"Tags": tags}, nil
}
