package scheduler

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputil"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	schedulerTargetPrefix  = "AWSScheduler."
	schedulerMatchPriority = 100
)

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
func (h *Handler) MatchPriority() int { return schedulerMatchPriority }

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
	var req struct {
		Name string `json:"Name"`
	}
	_ = json.Unmarshal(body, &req)
	return req.Name
}

// Handler returns the Echo handler function for Scheduler requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		body, err := httputil.ReadBody(c.Request())
		if err != nil {
			return c.JSON(http.StatusInternalServerError, map[string]string{"message": "failed to read body"})
		}

		action := strings.TrimPrefix(c.Request().Header.Get("X-Amz-Target"), schedulerTargetPrefix)
		switch action {
		case "CreateSchedule":
			return h.handleCreateSchedule(c, body)
		case "GetSchedule":
			return h.handleGetSchedule(c, body)
		case "ListSchedules":
			return h.handleListSchedules(c)
		case "DeleteSchedule":
			return h.handleDeleteSchedule(c, body)
		case "UpdateSchedule":
			return h.handleUpdateSchedule(c, body)
		case "TagResource":
			return h.handleTagResource(c, body)
		case "ListTagsForResource":
			return h.handleListTagsForResource(c, body)
		default:
			return c.JSON(http.StatusBadRequest, map[string]string{"message": "unknown action: " + action})
		}
	}
}

func (h *Handler) handleCreateSchedule(c *echo.Context, body []byte) error {
	var req struct {
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
	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"message": "invalid request"})
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
		FlexibleTimeWindow{Mode: req.FlexibleTimeWindow.Mode, MaximumWindowInMinutes: req.FlexibleTimeWindow.MaximumWindowInMinutes},
	)
	if err != nil {
		if errors.Is(err, ErrAlreadyExists) {
			return c.JSON(http.StatusConflict, map[string]string{"message": err.Error()})
		}
		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]string{"ScheduleArn": s.ARN})
}

func (h *Handler) handleGetSchedule(c *echo.Context, body []byte) error {
	var req struct {
		Name string `json:"Name"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"message": "invalid request"})
	}

	s, err := h.Backend.GetSchedule(req.Name)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.JSON(http.StatusNotFound, map[string]string{"message": err.Error()})
		}
		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{
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
	})
}

func (h *Handler) handleListSchedules(c *echo.Context) error {
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
	return c.JSON(http.StatusOK, map[string]any{
		"Schedules": items,
	})
}

func (h *Handler) handleDeleteSchedule(c *echo.Context, body []byte) error {
	var req struct {
		Name string `json:"Name"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"message": "invalid request"})
	}

	if err := h.Backend.DeleteSchedule(req.Name); err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.JSON(http.StatusNotFound, map[string]string{"message": err.Error()})
		}
		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]string{})
}

func (h *Handler) handleUpdateSchedule(c *echo.Context, body []byte) error {
	var req struct {
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
	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"message": "invalid request"})
	}

	s, err := h.Backend.UpdateSchedule(
		req.Name,
		req.ScheduleExpression,
		Target{ARN: req.Target.Arn, RoleARN: req.Target.RoleArn},
		req.State,
		FlexibleTimeWindow{Mode: req.FlexibleTimeWindow.Mode, MaximumWindowInMinutes: req.FlexibleTimeWindow.MaximumWindowInMinutes},
	)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.JSON(http.StatusNotFound, map[string]string{"message": err.Error()})
		}
		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]string{"ScheduleArn": s.ARN})
}

func (h *Handler) handleTagResource(c *echo.Context, body []byte) error {
	var req struct {
		ResourceArn string            `json:"ResourceArn"`
		Tags        map[string]string `json:"Tags"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"message": "invalid request"})
	}

	if err := h.Backend.TagResource(req.ResourceArn, req.Tags); err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.JSON(http.StatusNotFound, map[string]string{"message": err.Error()})
		}
		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]string{})
}

func (h *Handler) handleListTagsForResource(c *echo.Context, body []byte) error {
	var req struct {
		ResourceArn string `json:"ResourceArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"message": "invalid request"})
	}

	tags, err := h.Backend.ListTagsForResource(req.ResourceArn)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.JSON(http.StatusNotFound, map[string]string{"message": err.Error()})
		}
		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{"Tags": tags})
}
