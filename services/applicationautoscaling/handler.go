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

	v := float64(t.Unix())

	return &v
}

// --- Input/Output types ---

type suspendedStateInput struct {
	DynamicScalingInSuspended  bool `json:"DynamicScalingInSuspended"`
	DynamicScalingOutSuspended bool `json:"DynamicScalingOutSuspended"`
	ScheduledScalingSuspended  bool `json:"ScheduledScalingSuspended"`
}

type registerScalableTargetInput struct {
	SuspendedState    *suspendedStateInput `json:"SuspendedState,omitempty"`
	Tags              map[string]string    `json:"Tags,omitempty"`
	ServiceNamespace  string               `json:"ServiceNamespace"`
	ResourceID        string               `json:"ResourceId"`
	ScalableDimension string               `json:"ScalableDimension"`
	RoleARN           string               `json:"RoleARN,omitempty"`
	MinCapacity       int32                `json:"MinCapacity"`
	MaxCapacity       int32                `json:"MaxCapacity"`
}

type registerScalableTargetOutput struct {
	ScalableTargetARN string `json:"ScalableTargetARN"`
}

func (h *Handler) handleRegisterScalableTarget(
	_ context.Context,
	in *registerScalableTargetInput,
) (*registerScalableTargetOutput, error) {
	var ss *SuspendedState
	if in.SuspendedState != nil {
		ss = &SuspendedState{
			DynamicScalingInSuspended:  in.SuspendedState.DynamicScalingInSuspended,
			DynamicScalingOutSuspended: in.SuspendedState.DynamicScalingOutSuspended,
			ScheduledScalingSuspended:  in.SuspendedState.ScheduledScalingSuspended,
		}
	}

	t, err := h.Backend.RegisterScalableTarget(
		in.ServiceNamespace, in.ResourceID, in.ScalableDimension,
		in.MinCapacity, in.MaxCapacity,
		in.Tags, in.RoleARN, ss,
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
	ServiceNamespace  string   `json:"ServiceNamespace"`
	ScalableDimension string   `json:"ScalableDimension,omitempty"`
	NextToken         string   `json:"NextToken,omitempty"`
	ResourceIDs       []string `json:"ResourceIds,omitempty"`
	MaxResults        int32    `json:"MaxResults,omitempty"`
}

type suspendedStateSummary struct {
	DynamicScalingInSuspended  bool `json:"DynamicScalingInSuspended"`
	DynamicScalingOutSuspended bool `json:"DynamicScalingOutSuspended"`
	ScheduledScalingSuspended  bool `json:"ScheduledScalingSuspended"`
}

type scalableTargetSummary struct {
	SuspendedState    *suspendedStateSummary `json:"SuspendedState,omitempty"`
	Tags              map[string]string      `json:"Tags,omitempty"`
	CreationTime      *float64               `json:"CreationTime,omitempty"`
	LastModifiedTime  *float64               `json:"LastModifiedTime,omitempty"`
	ServiceNamespace  string                 `json:"ServiceNamespace"`
	ResourceID        string                 `json:"ResourceId"`
	ScalableDimension string                 `json:"ScalableDimension"`
	ScalableTargetARN string                 `json:"ScalableTargetARN"`
	RoleARN           string                 `json:"RoleARN,omitempty"`
	MinCapacity       int32                  `json:"MinCapacity"`
	MaxCapacity       int32                  `json:"MaxCapacity"`
}

type describeScalableTargetsOutput struct {
	NextToken       string                  `json:"NextToken,omitempty"`
	ScalableTargets []scalableTargetSummary `json:"ScalableTargets"`
}

func (h *Handler) handleDescribeScalableTargets(
	_ context.Context,
	in *describeScalableTargetsInput,
) (*describeScalableTargetsOutput, error) {
	targets, nextToken := h.Backend.DescribeScalableTargets(DescribeScalableTargetsFilter{
		ServiceNamespace:  in.ServiceNamespace,
		ResourceIDs:       in.ResourceIDs,
		ScalableDimension: in.ScalableDimension,
		MaxResults:        in.MaxResults,
		NextToken:         in.NextToken,
	})
	items := make([]scalableTargetSummary, 0, len(targets))
	for _, t := range targets {
		item := scalableTargetSummary{
			ServiceNamespace:  t.ServiceNamespace,
			ResourceID:        t.ResourceID,
			ScalableDimension: t.ScalableDimension,
			MinCapacity:       t.MinCapacity,
			MaxCapacity:       t.MaxCapacity,
			ScalableTargetARN: t.ARN,
			RoleARN:           t.RoleARN,
			Tags:              t.Tags,
			CreationTime:      epochSecondsPtr(t.CreationTime),
			LastModifiedTime:  epochSecondsPtr(t.LastModifiedTime),
		}
		if t.SuspendedState != nil {
			item.SuspendedState = &suspendedStateSummary{
				DynamicScalingInSuspended:  t.SuspendedState.DynamicScalingInSuspended,
				DynamicScalingOutSuspended: t.SuspendedState.DynamicScalingOutSuspended,
				ScheduledScalingSuspended:  t.SuspendedState.ScheduledScalingSuspended,
			}
		}

		items = append(items, item)
	}

	return &describeScalableTargetsOutput{ScalableTargets: items, NextToken: nextToken}, nil
}

type putScalingPolicyInput struct {
	TargetTrackingScalingPolicyConfiguration map[string]any `json:"TargetTrackingScalingPolicyConfiguration,omitempty"`
	StepScalingPolicyConfiguration           map[string]any `json:"StepScalingPolicyConfiguration,omitempty"`
	ServiceNamespace                         string         `json:"ServiceNamespace"`
	ResourceID                               string         `json:"ResourceId"`
	ScalableDimension                        string         `json:"ScalableDimension"`
	PolicyName                               string         `json:"PolicyName"`
	PolicyType                               string         `json:"PolicyType"`
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
		in.TargetTrackingScalingPolicyConfiguration,
		in.StepScalingPolicyConfiguration,
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
	ServiceNamespace  string   `json:"ServiceNamespace"`
	ResourceID        string   `json:"ResourceId,omitempty"`
	ScalableDimension string   `json:"ScalableDimension,omitempty"`
	NextToken         string   `json:"NextToken,omitempty"`
	PolicyNames       []string `json:"PolicyNames,omitempty"`
	PolicyARNs        []string `json:"PolicyARNs,omitempty"`
	MaxResults        int32    `json:"MaxResults,omitempty"`
}

type scalingPolicySummary struct {
	TargetTrackingScalingPolicyConfiguration map[string]any `json:"TargetTrackingScalingPolicyConfiguration,omitempty"`
	StepScalingPolicyConfiguration           map[string]any `json:"StepScalingPolicyConfiguration,omitempty"`
	CreationTime                             *float64       `json:"CreationTime,omitempty"`
	LastModifiedTime                         *float64       `json:"LastModifiedTime,omitempty"`
	ServiceNamespace                         string         `json:"ServiceNamespace"`
	ResourceID                               string         `json:"ResourceId"`
	ScalableDimension                        string         `json:"ScalableDimension"`
	PolicyName                               string         `json:"PolicyName"`
	PolicyType                               string         `json:"PolicyType"`
	PolicyARN                                string         `json:"PolicyARN"`
	Alarms                                   []alarmSummary `json:"Alarms,omitempty"`
}

// alarmSummary mirrors the CloudWatch alarm reference returned by AWS in policy descriptions.
type alarmSummary struct {
	AlarmARN  string `json:"AlarmARN"`
	AlarmName string `json:"AlarmName"`
}

type describeScalingPoliciesOutput struct {
	NextToken       string                 `json:"NextToken,omitempty"`
	ScalingPolicies []scalingPolicySummary `json:"ScalingPolicies"`
}

func (h *Handler) handleDescribeScalingPolicies(
	_ context.Context,
	in *describeScalingPoliciesInput,
) (*describeScalingPoliciesOutput, error) {
	policies, nextToken := h.Backend.DescribeScalingPolicies(DescribeScalingPoliciesFilter{
		ServiceNamespace:  in.ServiceNamespace,
		ResourceID:        in.ResourceID,
		ScalableDimension: in.ScalableDimension,
		PolicyNames:       in.PolicyNames,
		PolicyARNs:        in.PolicyARNs,
		MaxResults:        in.MaxResults,
		NextToken:         in.NextToken,
	})
	items := make([]scalingPolicySummary, 0, len(policies))
	for _, p := range policies {
		items = append(items, scalingPolicySummary{
			ServiceNamespace:                         p.ServiceNamespace,
			ResourceID:                               p.ResourceID,
			ScalableDimension:                        p.ScalableDimension,
			PolicyName:                               p.PolicyName,
			PolicyType:                               p.PolicyType,
			PolicyARN:                                p.ARN,
			CreationTime:                             epochSecondsPtr(p.CreationTime),
			LastModifiedTime:                         epochSecondsPtr(p.LastModifiedTime),
			TargetTrackingScalingPolicyConfiguration: p.TargetTrackingConfig,
			StepScalingPolicyConfiguration:           p.StepScalingConfig,
		})
	}

	return &describeScalingPoliciesOutput{ScalingPolicies: items, NextToken: nextToken}, nil
}

type describeScalingActivitiesInput struct {
	ServiceNamespace           string `json:"ServiceNamespace"`
	ResourceID                 string `json:"ResourceId,omitempty"`
	ScalableDimension          string `json:"ScalableDimension,omitempty"`
	NextToken                  string `json:"NextToken,omitempty"`
	MaxResults                 int32  `json:"MaxResults,omitempty"`
	IncludeNotScaledActivities bool   `json:"IncludeNotScaledActivities,omitempty"`
}

type scalingActivitySummary struct {
	StartTime         *float64 `json:"StartTime,omitempty"`
	EndTime           *float64 `json:"EndTime,omitempty"`
	ActivityID        string   `json:"ActivityId"`
	ServiceNamespace  string   `json:"ServiceNamespace"`
	ResourceID        string   `json:"ResourceId"`
	ScalableDimension string   `json:"ScalableDimension"`
	Description       string   `json:"Description"`
	Cause             string   `json:"Cause"`
	StatusCode        string   `json:"StatusCode"`
	StatusMessage     string   `json:"StatusMessage,omitempty"`
}

type describeScalingActivitiesOutput struct {
	NextToken         string                   `json:"NextToken,omitempty"`
	ScalingActivities []scalingActivitySummary `json:"ScalingActivities"`
}

func (h *Handler) handleDescribeScalingActivities(
	_ context.Context,
	in *describeScalingActivitiesInput,
) (*describeScalingActivitiesOutput, error) {
	if in.ServiceNamespace == "" {
		return nil, fmt.Errorf("%w: ServiceNamespace is required", ErrValidation)
	}

	activities, nextToken := h.Backend.DescribeScalingActivities(DescribeScalingActivitiesFilter{
		ServiceNamespace:  in.ServiceNamespace,
		ResourceID:        in.ResourceID,
		ScalableDimension: in.ScalableDimension,
		MaxResults:        in.MaxResults,
		NextToken:         in.NextToken,
	})

	items := make([]scalingActivitySummary, 0, len(activities))
	for _, a := range activities {
		items = append(items, scalingActivitySummary{
			ActivityID:        a.ActivityID,
			ServiceNamespace:  a.ServiceNamespace,
			ResourceID:        a.ResourceID,
			ScalableDimension: a.ScalableDimension,
			Description:       a.Description,
			Cause:             a.Cause,
			StatusCode:        a.StatusCode,
			StatusMessage:     a.StatusMessage,
			StartTime:         epochSecondsPtr(a.StartTime),
			EndTime:           epochSecondsPtr(a.EndTime),
		})
	}

	return &describeScalingActivitiesOutput{ScalingActivities: items, NextToken: nextToken}, nil
}

type scalableTargetActionInput struct {
	MinCapacity *int32 `json:"MinCapacity,omitempty"`
	MaxCapacity *int32 `json:"MaxCapacity,omitempty"`
}

type putScheduledActionInput struct {
	ScalableTargetAction *scalableTargetActionInput `json:"ScalableTargetAction,omitempty"`
	ServiceNamespace     string                     `json:"ServiceNamespace"`
	ResourceID           string                     `json:"ResourceId"`
	ScalableDimension    string                     `json:"ScalableDimension"`
	ScheduledActionName  string                     `json:"ScheduledActionName"`
	Schedule             string                     `json:"Schedule"`
	Timezone             string                     `json:"Timezone,omitempty"`
	StartTime            string                     `json:"StartTime,omitempty"`
	EndTime              string                     `json:"EndTime,omitempty"`
}

type putScheduledActionOutput struct {
	ScheduledActionARN string `json:"ScheduledActionARN"`
}

func (h *Handler) handlePutScheduledAction(
	_ context.Context,
	in *putScheduledActionInput,
) (*putScheduledActionOutput, error) {
	const timeLayout = time.RFC3339

	var sta *ScalableTargetAction
	if in.ScalableTargetAction != nil {
		sta = &ScalableTargetAction{
			MinCapacity: in.ScalableTargetAction.MinCapacity,
			MaxCapacity: in.ScalableTargetAction.MaxCapacity,
		}
	}

	var startTime, endTime *time.Time

	if in.StartTime != "" {
		t, err := time.Parse(timeLayout, in.StartTime)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid StartTime: %s", errInvalidRequest, in.StartTime)
		}

		startTime = &t
	}

	if in.EndTime != "" {
		t, err := time.Parse(timeLayout, in.EndTime)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid EndTime: %s", errInvalidRequest, in.EndTime)
		}

		endTime = &t
	}

	a, err := h.Backend.PutScheduledAction(
		in.ServiceNamespace, in.ResourceID, in.ScalableDimension,
		in.ScheduledActionName, in.Schedule, in.Timezone,
		startTime, endTime, sta,
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
	ServiceNamespace     string   `json:"ServiceNamespace"`
	ResourceID           string   `json:"ResourceId,omitempty"`
	ScalableDimension    string   `json:"ScalableDimension,omitempty"`
	NextToken            string   `json:"NextToken,omitempty"`
	ScheduledActionNames []string `json:"ScheduledActionNames,omitempty"`
	MaxResults           int32    `json:"MaxResults,omitempty"`
}

type scalableTargetActionSummary struct {
	MinCapacity *int32 `json:"MinCapacity,omitempty"`
	MaxCapacity *int32 `json:"MaxCapacity,omitempty"`
}

type scheduledActionSummary struct {
	ScalableTargetAction *scalableTargetActionSummary `json:"ScalableTargetAction,omitempty"`
	CreationTime         *float64                     `json:"CreationTime,omitempty"`
	LastModifiedTime     *float64                     `json:"LastModifiedTime,omitempty"`
	StartTime            *float64                     `json:"StartTime,omitempty"`
	EndTime              *float64                     `json:"EndTime,omitempty"`
	ServiceNamespace     string                       `json:"ServiceNamespace"`
	ResourceID           string                       `json:"ResourceId"`
	ScalableDimension    string                       `json:"ScalableDimension"`
	ScheduledActionName  string                       `json:"ScheduledActionName"`
	Schedule             string                       `json:"Schedule"`
	ScheduledActionARN   string                       `json:"ScheduledActionARN"`
	Timezone             string                       `json:"Timezone,omitempty"`
}

type describeScheduledActionsOutput struct {
	NextToken        string                   `json:"NextToken,omitempty"`
	ScheduledActions []scheduledActionSummary `json:"ScheduledActions"`
}

func (h *Handler) handleDescribeScheduledActions(
	_ context.Context,
	in *describeScheduledActionsInput,
) (*describeScheduledActionsOutput, error) {
	actions, nextToken := h.Backend.DescribeScheduledActions(DescribeScheduledActionsFilter{
		ServiceNamespace:     in.ServiceNamespace,
		ResourceID:           in.ResourceID,
		ScalableDimension:    in.ScalableDimension,
		ScheduledActionNames: in.ScheduledActionNames,
		MaxResults:           in.MaxResults,
		NextToken:            in.NextToken,
	})
	items := make([]scheduledActionSummary, 0, len(actions))
	for _, a := range actions {
		item := scheduledActionSummary{
			ServiceNamespace:    a.ServiceNamespace,
			ResourceID:          a.ResourceID,
			ScalableDimension:   a.ScalableDimension,
			ScheduledActionName: a.ScheduledActionName,
			Schedule:            a.Schedule,
			Timezone:            a.Timezone,
			ScheduledActionARN:  a.ARN,
			CreationTime:        epochSecondsPtr(a.CreationTime),
			LastModifiedTime:    epochSecondsPtr(a.LastModifiedTime),
		}
		if a.StartTime != nil {
			item.StartTime = epochSecondsPtr(*a.StartTime)
		}

		if a.EndTime != nil {
			item.EndTime = epochSecondsPtr(*a.EndTime)
		}

		if a.ScalableTargetAction != nil {
			item.ScalableTargetAction = &scalableTargetActionSummary{
				MinCapacity: a.ScalableTargetAction.MinCapacity,
				MaxCapacity: a.ScalableTargetAction.MaxCapacity,
			}
		}

		items = append(items, item)
	}

	return &describeScheduledActionsOutput{ScheduledActions: items, NextToken: nextToken}, nil
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

type getPredictiveScalingForecastInput struct {
	ServiceNamespace  string `json:"ServiceNamespace"`
	ResourceID        string `json:"ResourceId"`
	ScalableDimension string `json:"ScalableDimension"`
	PolicyName        string `json:"PolicyName"`
	StartTime         string `json:"StartTime"`
	EndTime           string `json:"EndTime"`
}

type capacityForecastOutput struct {
	Timestamps []string  `json:"Timestamps"`
	Values     []float64 `json:"Values"`
}

type loadForecastOutput struct {
	MetricSpecification string    `json:"MetricSpecification"`
	Timestamps          []string  `json:"Timestamps"`
	Values              []float64 `json:"Values"`
}

type getPredictiveScalingForecastOutput struct {
	CapacityForecast *capacityForecastOutput `json:"CapacityForecast"`
	UpdateTime       string                  `json:"UpdateTime"`
	LoadForecast     []loadForecastOutput    `json:"LoadForecast"`
}

func (h *Handler) handleGetPredictiveScalingForecast(
	_ context.Context,
	in *getPredictiveScalingForecastInput,
) (*getPredictiveScalingForecastOutput, error) {
	const timeLayout = time.RFC3339

	startTime, err := time.Parse(timeLayout, in.StartTime)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid StartTime: %s", errInvalidRequest, in.StartTime)
	}

	endTime, err := time.Parse(timeLayout, in.EndTime)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid EndTime: %s", errInvalidRequest, in.EndTime)
	}

	capacity, load, updateTime, err := h.Backend.GetPredictiveScalingForecast(
		in.ServiceNamespace, in.ResourceID, in.ScalableDimension, in.PolicyName,
		startTime, endTime,
	)
	if err != nil {
		return nil, err
	}

	capTS := make([]string, len(capacity.Timestamps))
	for i, ts := range capacity.Timestamps {
		capTS[i] = ts.UTC().Format(timeLayout)
	}

	loadOut := make([]loadForecastOutput, len(load))
	for i, lf := range load {
		ts := make([]string, len(lf.Timestamps))
		for j, t := range lf.Timestamps {
			ts[j] = t.UTC().Format(timeLayout)
		}

		loadOut[i] = loadForecastOutput{
			Timestamps:          ts,
			Values:              lf.Values,
			MetricSpecification: lf.MetricSpecification,
		}
	}

	return &getPredictiveScalingForecastOutput{
		CapacityForecast: &capacityForecastOutput{
			Timestamps: capTS,
			Values:     capacity.Values,
		},
		LoadForecast: loadOut,
		UpdateTime:   updateTime.UTC().Format(timeLayout),
	}, nil
}

// Reset clears all backend state.
func (h *Handler) Reset() {
	h.Backend.Reset()
}
