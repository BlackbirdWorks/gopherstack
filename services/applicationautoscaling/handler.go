package applicationautoscaling

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	autoscalingTargetPrefix = "AnyScaleFrontendService."
)

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
)

// Handler is the Echo HTTP handler for Application Auto Scaling operations.
type Handler struct {
	Backend       *InMemoryBackend
	dispatchTable map[string]service.JSONOpFunc
}

// NewHandler creates a new Application Auto Scaling handler backed by backend.
// backend must not be nil.
func NewHandler(backend *InMemoryBackend) *Handler {
	h := &Handler{Backend: backend}
	h.dispatchTable = h.buildDispatchTable()

	return h
}

// Name returns the service name.
func (h *Handler) Name() string { return "ApplicationAutoscaling" }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"RegisterScalableTarget",
		"DeregisterScalableTarget",
		"DescribeScalableTargets",
		"PutScalingPolicy",
		"DeleteScalingPolicy",
		"DescribeScalingPolicies",
		"DescribeScalingActivities",
		"PutScheduledAction",
		"DeleteScheduledAction",
		"DescribeScheduledActions",
		"ListTagsForResource",
		"TagResource",
		"UntagResource",
		"GetPredictiveScalingForecast",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "applicationautoscaling" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches Application Auto Scaling requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), autoscalingTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the Application Auto Scaling action from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")

	return strings.TrimPrefix(target, autoscalingTargetPrefix)
}

// ExtractResource extracts the resource identifier from the request body.
func (h *Handler) ExtractResource(_ *echo.Context) string {
	return ""
}

// Handler returns the Echo handler function for Application Auto Scaling requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"ApplicationAutoscaling", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

func (h *Handler) buildDispatchTable() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"RegisterScalableTarget":       service.WrapOp(h.handleRegisterScalableTarget),
		"DeregisterScalableTarget":     service.WrapOp(h.handleDeregisterScalableTarget),
		"DescribeScalableTargets":      service.WrapOp(h.handleDescribeScalableTargets),
		"PutScalingPolicy":             service.WrapOp(h.handlePutScalingPolicy),
		"DeleteScalingPolicy":          service.WrapOp(h.handleDeleteScalingPolicy),
		"DescribeScalingPolicies":      service.WrapOp(h.handleDescribeScalingPolicies),
		"DescribeScalingActivities":    service.WrapOp(h.handleDescribeScalingActivities),
		"PutScheduledAction":           service.WrapOp(h.handlePutScheduledAction),
		"DeleteScheduledAction":        service.WrapOp(h.handleDeleteScheduledAction),
		"DescribeScheduledActions":     service.WrapOp(h.handleDescribeScheduledActions),
		"ListTagsForResource":          service.WrapOp(h.handleListTagsForResource),
		"TagResource":                  service.WrapOp(h.handleTagResource),
		"UntagResource":                service.WrapOp(h.handleUntagResource),
		"GetPredictiveScalingForecast": service.WrapOp(h.handleGetPredictiveScalingForecast),
	}
}

func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.dispatchTable[action]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errUnknownAction, action)
	}

	result, err := fn(ctx, body)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

// marshalError serialises a JSONErrorResponse into bytes.
// Marshaling a struct with only string fields cannot fail; error is intentionally ignored.
func marshalError(errType, message string) []byte {
	payload, _ := json.Marshal(service.JSONErrorResponse{Type: errType, Message: message})

	return payload
}

func (h *Handler) handleError(_ context.Context, c *echo.Context, _ string, err error) error {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError

	switch {
	case errors.Is(err, ErrNotFound):
		return c.JSONBlob(http.StatusNotFound, marshalError("ObjectNotFoundException", err.Error()))
	case errors.Is(err, ErrAlreadyExists):
		return c.JSONBlob(http.StatusConflict, marshalError("ValidationException", err.Error()))
	case errors.Is(err, ErrValidation):
		return c.JSONBlob(http.StatusBadRequest, marshalError("ValidationException", err.Error()))
	case errors.Is(err, errInvalidRequest), errors.Is(err, errUnknownAction),
		errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		return c.JSON(http.StatusBadRequest, map[string]string{"message": err.Error()})
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}
}

// epochSecondsPtr converts a non-zero [time.Time] to a pointer to its Unix epoch
// seconds value (float64), as required by the AWS JSON protocol for timestamp
// fields. Returns nil for zero-value times so omitempty omits the field.
func epochSecondsPtr(t time.Time) *float64 {
	if t.IsZero() {
		return nil
	}

	v := awstime.Epoch(t)

	return &v
}

// parseEpochSeconds converts an AWS JSON-protocol epoch-seconds timestamp
// (a JSON number, decoded here as *float64) to a UTC [time.Time]. Returns nil
// when v is nil, so callers can distinguish "field absent" from "field zero".
func parseEpochSeconds(v *float64) *time.Time {
	if v == nil {
		return nil
	}

	t := time.Unix(int64(*v), 0).UTC()

	return &t
}

// Reset clears all backend state.
func (h *Handler) Reset() {
	h.Backend.Reset()
}
