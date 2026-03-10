package applicationautoscaling

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

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
	Backend *InMemoryBackend
}

// NewHandler creates a new Application Auto Scaling handler backed by backend.
// backend must not be nil.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
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

func (h *Handler) dispatchTable() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"RegisterScalableTarget":    service.WrapOp(h.handleRegisterScalableTarget),
		"DeregisterScalableTarget":  service.WrapOp(h.handleDeregisterScalableTarget),
		"DescribeScalableTargets":   service.WrapOp(h.handleDescribeScalableTargets),
		"PutScalingPolicy":          service.WrapOp(h.handlePutScalingPolicy),
		"DeleteScalingPolicy":       service.WrapOp(h.handleDeleteScalingPolicy),
		"DescribeScalingPolicies":   service.WrapOp(h.handleDescribeScalingPolicies),
		"DescribeScalingActivities": service.WrapOp(h.handleDescribeScalingActivities),
		"PutScheduledAction":        service.WrapOp(h.handlePutScheduledAction),
		"DeleteScheduledAction":     service.WrapOp(h.handleDeleteScheduledAction),
		"DescribeScheduledActions":  service.WrapOp(h.handleDescribeScheduledActions),
		"ListTagsForResource":       service.WrapOp(h.handleListTagsForResource),
		"TagResource":               service.WrapOp(h.handleTagResource),
		"UntagResource":             service.WrapOp(h.handleUntagResource),
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
			Type:    "ObjectNotFoundException",
			Message: err.Error(),
		})

		return c.JSONBlob(http.StatusNotFound, payload)
	case errors.Is(err, ErrAlreadyExists):
		payload, _ := json.Marshal(service.JSONErrorResponse{
			Type:    "ValidationException",
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

// --- Input/Output types ---

type registerScalableTargetInput struct {
	ServiceNamespace  string `json:"ServiceNamespace"`
	ResourceID        string `json:"ResourceId"`
	ScalableDimension string `json:"ScalableDimension"`
	MinCapacity       int32  `json:"MinCapacity"`
	MaxCapacity       int32  `json:"MaxCapacity"`
}

type registerScalableTargetOutput struct {
	ScalableTargetARN string `json:"ScalableTargetARN"`
}

func (h *Handler) handleRegisterScalableTarget(
	_ context.Context,
	in *registerScalableTargetInput,
) (*registerScalableTargetOutput, error) {
	t, err := h.Backend.RegisterScalableTarget(
		in.ServiceNamespace, in.ResourceID, in.ScalableDimension,
		in.MinCapacity, in.MaxCapacity,
	)
	if err != nil {
		return nil, err
	}

	return &registerScalableTargetOutput{ScalableTargetARN: t.ARN}, nil
}

type deregisterScalableTargetInput struct {
	ServiceNamespace  string `json:"ServiceNamespace"`
	ResourceID        string `json:"ResourceId"`
	ScalableDimension string `json:"ScalableDimension"`
}

type deregisterScalableTargetOutput struct{}

func (h *Handler) handleDeregisterScalableTarget(
	_ context.Context,
	in *deregisterScalableTargetInput,
) (*deregisterScalableTargetOutput, error) {
	if err := h.Backend.DeregisterScalableTarget(in.ServiceNamespace, in.ResourceID, in.ScalableDimension); err != nil {
		return nil, err
	}

	return &deregisterScalableTargetOutput{}, nil
}

type describeScalableTargetsInput struct {
	ServiceNamespace string `json:"ServiceNamespace"`
}

type scalableTargetSummary struct {
	ServiceNamespace  string `json:"ServiceNamespace"`
	ResourceID        string `json:"ResourceId"`
	ScalableDimension string `json:"ScalableDimension"`
	ScalableTargetARN string `json:"ScalableTargetARN"`
	MinCapacity       int32  `json:"MinCapacity"`
	MaxCapacity       int32  `json:"MaxCapacity"`
}

type describeScalableTargetsOutput struct {
	ScalableTargets []scalableTargetSummary `json:"ScalableTargets"`
}

func (h *Handler) handleDescribeScalableTargets(
	_ context.Context,
	in *describeScalableTargetsInput,
) (*describeScalableTargetsOutput, error) {
	targets := h.Backend.DescribeScalableTargets(in.ServiceNamespace)
	items := make([]scalableTargetSummary, 0, len(targets))
	for _, t := range targets {
		items = append(items, scalableTargetSummary{
			ServiceNamespace:  t.ServiceNamespace,
			ResourceID:        t.ResourceID,
			ScalableDimension: t.ScalableDimension,
			MinCapacity:       t.MinCapacity,
			MaxCapacity:       t.MaxCapacity,
			ScalableTargetARN: t.ARN,
		})
	}

	return &describeScalableTargetsOutput{ScalableTargets: items}, nil
}

type putScalingPolicyInput struct {
	ServiceNamespace  string `json:"ServiceNamespace"`
	ResourceID        string `json:"ResourceId"`
	ScalableDimension string `json:"ScalableDimension"`
	PolicyName        string `json:"PolicyName"`
	PolicyType        string `json:"PolicyType"`
}

type putScalingPolicyOutput struct {
	PolicyARN string `json:"PolicyARN"`
}

func (h *Handler) handlePutScalingPolicy(
	_ context.Context,
	in *putScalingPolicyInput,
) (*putScalingPolicyOutput, error) {
	p, err := h.Backend.PutScalingPolicy(
		in.ServiceNamespace, in.ResourceID, in.ScalableDimension,
		in.PolicyName, in.PolicyType,
	)
	if err != nil {
		return nil, err
	}

	return &putScalingPolicyOutput{PolicyARN: p.ARN}, nil
}

type deleteScalingPolicyInput struct {
	ServiceNamespace  string `json:"ServiceNamespace"`
	ResourceID        string `json:"ResourceId"`
	ScalableDimension string `json:"ScalableDimension"`
	PolicyName        string `json:"PolicyName"`
}

type deleteScalingPolicyOutput struct{}

func (h *Handler) handleDeleteScalingPolicy(
	_ context.Context,
	in *deleteScalingPolicyInput,
) (*deleteScalingPolicyOutput, error) {
	if err := h.Backend.DeleteScalingPolicy(
		in.ServiceNamespace,
		in.ResourceID,
		in.ScalableDimension,
		in.PolicyName,
	); err != nil {
		return nil, err
	}

	return &deleteScalingPolicyOutput{}, nil
}

type describeScalingPoliciesInput struct {
	ServiceNamespace string `json:"ServiceNamespace"`
}

type scalingPolicySummary struct {
	ServiceNamespace  string `json:"ServiceNamespace"`
	ResourceID        string `json:"ResourceId"`
	ScalableDimension string `json:"ScalableDimension"`
	PolicyName        string `json:"PolicyName"`
	PolicyType        string `json:"PolicyType"`
	PolicyARN         string `json:"PolicyARN"`
}

type describeScalingPoliciesOutput struct {
	ScalingPolicies []scalingPolicySummary `json:"ScalingPolicies"`
}

func (h *Handler) handleDescribeScalingPolicies(
	_ context.Context,
	in *describeScalingPoliciesInput,
) (*describeScalingPoliciesOutput, error) {
	policies := h.Backend.DescribeScalingPolicies(in.ServiceNamespace)
	items := make([]scalingPolicySummary, 0, len(policies))
	for _, p := range policies {
		items = append(items, scalingPolicySummary{
			ServiceNamespace:  p.ServiceNamespace,
			ResourceID:        p.ResourceID,
			ScalableDimension: p.ScalableDimension,
			PolicyName:        p.PolicyName,
			PolicyType:        p.PolicyType,
			PolicyARN:         p.ARN,
		})
	}

	return &describeScalingPoliciesOutput{ScalingPolicies: items}, nil
}

type describeScalingActivitiesInput struct {
	ServiceNamespace string `json:"ServiceNamespace"`
}

type describeScalingActivitiesOutput struct {
	ScalingActivities []any `json:"ScalingActivities"`
}

func (h *Handler) handleDescribeScalingActivities(
	_ context.Context,
	_ *describeScalingActivitiesInput,
) (*describeScalingActivitiesOutput, error) {
	return &describeScalingActivitiesOutput{ScalingActivities: []any{}}, nil
}

type putScheduledActionInput struct {
	ServiceNamespace    string `json:"ServiceNamespace"`
	ResourceID          string `json:"ResourceId"`
	ScalableDimension   string `json:"ScalableDimension"`
	ScheduledActionName string `json:"ScheduledActionName"`
	Schedule            string `json:"Schedule"`
}

type putScheduledActionOutput struct {
	ScheduledActionARN string `json:"ScheduledActionARN"`
}

func (h *Handler) handlePutScheduledAction(
	_ context.Context,
	in *putScheduledActionInput,
) (*putScheduledActionOutput, error) {
	a, err := h.Backend.PutScheduledAction(
		in.ServiceNamespace, in.ResourceID, in.ScalableDimension,
		in.ScheduledActionName, in.Schedule,
	)
	if err != nil {
		return nil, err
	}

	return &putScheduledActionOutput{ScheduledActionARN: a.ARN}, nil
}

type deleteScheduledActionInput struct {
	ServiceNamespace    string `json:"ServiceNamespace"`
	ResourceID          string `json:"ResourceId"`
	ScalableDimension   string `json:"ScalableDimension"`
	ScheduledActionName string `json:"ScheduledActionName"`
}

type deleteScheduledActionOutput struct{}

func (h *Handler) handleDeleteScheduledAction(
	_ context.Context,
	in *deleteScheduledActionInput,
) (*deleteScheduledActionOutput, error) {
	if err := h.Backend.DeleteScheduledAction(
		in.ServiceNamespace,
		in.ResourceID,
		in.ScalableDimension,
		in.ScheduledActionName,
	); err != nil {
		return nil, err
	}

	return &deleteScheduledActionOutput{}, nil
}

type describeScheduledActionsInput struct {
	ServiceNamespace string `json:"ServiceNamespace"`
}

type scheduledActionSummary struct {
	ServiceNamespace    string `json:"ServiceNamespace"`
	ResourceID          string `json:"ResourceId"`
	ScalableDimension   string `json:"ScalableDimension"`
	ScheduledActionName string `json:"ScheduledActionName"`
	Schedule            string `json:"Schedule"`
	ScheduledActionARN  string `json:"ScheduledActionARN"`
}

type describeScheduledActionsOutput struct {
	ScheduledActions []scheduledActionSummary `json:"ScheduledActions"`
}

func (h *Handler) handleDescribeScheduledActions(
	_ context.Context,
	in *describeScheduledActionsInput,
) (*describeScheduledActionsOutput, error) {
	actions := h.Backend.DescribeScheduledActions(in.ServiceNamespace)
	items := make([]scheduledActionSummary, 0, len(actions))
	for _, a := range actions {
		items = append(items, scheduledActionSummary{
			ServiceNamespace:    a.ServiceNamespace,
			ResourceID:          a.ResourceID,
			ScalableDimension:   a.ScalableDimension,
			ScheduledActionName: a.ScheduledActionName,
			Schedule:            a.Schedule,
			ScheduledActionARN:  a.ARN,
		})
	}

	return &describeScheduledActionsOutput{ScheduledActions: items}, nil
}

type listTagsForResourceInput struct {
	ResourceARN string `json:"ResourceARN"`
}

type listTagsForResourceOutput struct {
	Tags map[string]string `json:"Tags"`
}

func (h *Handler) handleListTagsForResource(
	_ context.Context,
	in *listTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	tags, err := h.Backend.ListTagsForResource(in.ResourceARN)
	if err != nil {
		return nil, err
	}

	return &listTagsForResourceOutput{Tags: tags}, nil
}

type tagResourceInput struct {
	Tags        map[string]string `json:"Tags"`
	ResourceARN string            `json:"ResourceARN"`
}

type tagResourceOutput struct{}

func (h *Handler) handleTagResource(_ context.Context, in *tagResourceInput) (*tagResourceOutput, error) {
	if err := h.Backend.TagResource(in.ResourceARN, in.Tags); err != nil {
		return nil, err
	}

	return &tagResourceOutput{}, nil
}

type untagResourceInput struct {
	ResourceARN string   `json:"ResourceARN"`
	TagKeys     []string `json:"TagKeys"`
}

type untagResourceOutput struct{}

func (h *Handler) handleUntagResource(_ context.Context, in *untagResourceInput) (*untagResourceOutput, error) {
	if err := h.Backend.UntagResource(in.ResourceARN, in.TagKeys); err != nil {
		return nil, err
	}

	return &untagResourceOutput{}, nil
}
