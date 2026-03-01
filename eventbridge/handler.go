package eventbridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"strings"
	"sync"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputil"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

var errUnknownOperation = errors.New("UnknownOperationException")

type createEventBusInput struct {
	Name        string `json:"Name"`
	Description string `json:"Description"`
}

type deleteEventBusInput struct {
	Name string `json:"Name"`
}

type listEventBusesInput struct {
	NamePrefix string `json:"NamePrefix"`
	NextToken  string `json:"NextToken"`
	Limit      int    `json:"Limit"`
}

type describeEventBusInput struct {
	Name string `json:"Name"`
}

type deleteRuleInput struct {
	Name         string `json:"Name"`
	EventBusName string `json:"EventBusName"`
}

type listRulesInput struct {
	EventBusName string `json:"EventBusName"`
	NamePrefix   string `json:"NamePrefix"`
	NextToken    string `json:"NextToken"`
	Limit        int    `json:"Limit"`
}

type describeRuleInput struct {
	Name         string `json:"Name"`
	EventBusName string `json:"EventBusName"`
}

type enableRuleInput struct {
	Name         string `json:"Name"`
	EventBusName string `json:"EventBusName"`
}

type disableRuleInput struct {
	Name         string `json:"Name"`
	EventBusName string `json:"EventBusName"`
}

type putTargetsInput struct {
	Rule         string   `json:"Rule"`
	EventBusName string   `json:"EventBusName"`
	Targets      []Target `json:"Targets"`
}

type removeTargetsInput struct {
	Rule         string   `json:"Rule"`
	EventBusName string   `json:"EventBusName"`
	IDs          []string `json:"Ids"`
}

type listTargetsByRuleInput struct {
	Rule         string `json:"Rule"`
	EventBusName string `json:"EventBusName"`
	NextToken    string `json:"NextToken"`
	Limit        int    `json:"Limit"`
}

type putEventsInput struct {
	Entries []EventEntry `json:"Entries"`
}

type listTagsForResourceInput struct {
	ResourceARN string `json:"ResourceARN"`
}

type ebTag struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

type tagResourceInput struct {
	ResourceARN string  `json:"ResourceARN"`
	Tags        []ebTag `json:"Tags"`
}

type untagResourceInput struct {
	ResourceARN string   `json:"ResourceARN"`
	TagKeys     []string `json:"TagKeys"`
}

// Handler is the Echo HTTP service handler for EventBridge operations.
type Handler struct {
	Backend   StorageBackend
	Logger    *slog.Logger
	scheduler *Scheduler
	tags      map[string]map[string]string
	tagsMu    sync.RWMutex
}

// NewHandler creates a new EventBridge handler.
func NewHandler(backend StorageBackend, log *slog.Logger) *Handler {
	return &Handler{Backend: backend, Logger: log, tags: make(map[string]map[string]string)}
}

func (h *Handler) setTags(resourceID string, kv map[string]string) {
	h.tagsMu.Lock()
	defer h.tagsMu.Unlock()
	if h.tags[resourceID] == nil {
		h.tags[resourceID] = make(map[string]string)
	}
	maps.Copy(h.tags[resourceID], kv)
}

func (h *Handler) removeTags(resourceID string, keys []string) {
	h.tagsMu.Lock()
	defer h.tagsMu.Unlock()
	for _, k := range keys {
		delete(h.tags[resourceID], k)
	}
}

func (h *Handler) getTags(resourceID string) map[string]string {
	h.tagsMu.RLock()
	defer h.tagsMu.RUnlock()
	result := make(map[string]string)
	maps.Copy(result, h.tags[resourceID])

	return result
}

// SetScheduler attaches a Scheduler to the handler. The scheduler is started as a
// background worker when StartWorker is called (which satisfies service.BackgroundWorker).
func (h *Handler) SetScheduler(s *Scheduler) {
	h.scheduler = s
}

// StartWorker implements service.BackgroundWorker.
// It starts the EventBridge scheduled-rules scheduler as a background goroutine.
func (h *Handler) StartWorker(ctx context.Context) error {
	if h.scheduler != nil {
		go h.scheduler.Run(ctx)
	}

	return nil
}

// Ensure Handler implements service.BackgroundWorker at compile time.
var _ service.BackgroundWorker = (*Handler)(nil)

// Name returns the service name.
func (h *Handler) Name() string { return "EventBridge" }

// GetSupportedOperations returns all mocked EventBridge operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateEventBus",
		"DeleteEventBus",
		"ListEventBuses",
		"DescribeEventBus",
		"PutRule",
		"DeleteRule",
		"ListRules",
		"DescribeRule",
		"EnableRule",
		"DisableRule",
		"PutTargets",
		"RemoveTargets",
		"ListTargetsByRule",
		"PutEvents",
		"ListTagsForResource",
		"TagResource",
		"UntagResource",
	}
}

// RouteMatcher returns a matcher for EventBridge requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		target := c.Request().Header.Get("X-Amz-Target")

		return strings.HasPrefix(target, "AmazonEventBridge.") || strings.HasPrefix(target, "AWSEvents.")
	}
}

// MatchPriority returns the routing priority for the EventBridge handler.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the operation name from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	parts := strings.Split(target, ".")
	const targetParts = 2
	if len(parts) == targetParts {
		return parts[1]
	}

	return "Unknown"
}

// ExtractResource extracts the resource name from the request body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputil.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var data map[string]any
	if uerr := json.Unmarshal(body, &data); uerr != nil {
		return ""
	}

	for _, key := range []string{"Name", "Rule", "EventBusName"} {
		if v, ok := data[key].(string); ok && v != "" {
			return v
		}
	}

	return ""
}

// Handler returns the Echo handler function for EventBridge requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"EventBridge", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

type actionFn func([]byte) (any, error)

func (h *Handler) eventBusActions() map[string]actionFn {
	return map[string]actionFn{
		"CreateEventBus": func(b []byte) (any, error) {
			var input createEventBusInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			bus, err := h.Backend.CreateEventBus(input.Name, input.Description)
			if err != nil {
				return nil, err
			}

			return map[string]string{"EventBusArn": bus.Arn}, nil
		},
		"DeleteEventBus": func(b []byte) (any, error) {
			var input deleteEventBusInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			if err := h.Backend.DeleteEventBus(input.Name); err != nil {
				return nil, err
			}

			return map[string]any{}, nil
		},
		"ListEventBuses": func(b []byte) (any, error) {
			var input listEventBusesInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			buses, next, err := h.Backend.ListEventBuses(input.NamePrefix, input.NextToken)
			if err != nil {
				return nil, err
			}

			return map[string]any{"EventBuses": buses, "NextToken": next}, nil
		},
		"DescribeEventBus": func(b []byte) (any, error) {
			var input describeEventBusInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			bus, err := h.Backend.DescribeEventBus(input.Name)
			if err != nil {
				return nil, err
			}

			return bus, nil
		},
	}
}

func (h *Handler) ruleActions() map[string]actionFn {
	return map[string]actionFn{
		"PutRule": func(b []byte) (any, error) {
			var input PutRuleInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			rule, err := h.Backend.PutRule(input)
			if err != nil {
				return nil, err
			}

			return map[string]string{"RuleArn": rule.Arn}, nil
		},
		"DeleteRule": func(b []byte) (any, error) {
			var input deleteRuleInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			if err := h.Backend.DeleteRule(input.Name, input.EventBusName); err != nil {
				return nil, err
			}

			return map[string]any{}, nil
		},
		"ListRules": func(b []byte) (any, error) {
			var input listRulesInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			rules, next, err := h.Backend.ListRules(input.EventBusName, input.NamePrefix, input.NextToken)
			if err != nil {
				return nil, err
			}

			return map[string]any{"Rules": rules, "NextToken": next}, nil
		},
		"DescribeRule": func(b []byte) (any, error) {
			var input describeRuleInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeRule(input.Name, input.EventBusName)
		},
	}
}

func (h *Handler) ruleStateActions() map[string]actionFn {
	return map[string]actionFn{
		"EnableRule": func(b []byte) (any, error) {
			var input enableRuleInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			if err := h.Backend.EnableRule(input.Name, input.EventBusName); err != nil {
				return nil, err
			}

			return map[string]any{}, nil
		},
		"DisableRule": func(b []byte) (any, error) {
			var input disableRuleInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			if err := h.Backend.DisableRule(input.Name, input.EventBusName); err != nil {
				return nil, err
			}

			return map[string]any{}, nil
		},
	}
}

func (h *Handler) targetActions() map[string]actionFn {
	return map[string]actionFn{
		"PutTargets": func(b []byte) (any, error) {
			var input putTargetsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			failed, err := h.Backend.PutTargets(input.Rule, input.EventBusName, input.Targets)
			if err != nil {
				return nil, err
			}
			if failed == nil {
				failed = []FailedEntry{}
			}

			return map[string]any{
				"FailedEntryCount": len(failed),
				"FailedEntries":    failed,
			}, nil
		},
		"RemoveTargets": func(b []byte) (any, error) {
			var input removeTargetsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			failed, err := h.Backend.RemoveTargets(input.Rule, input.EventBusName, input.IDs)
			if err != nil {
				return nil, err
			}
			if failed == nil {
				failed = []FailedEntry{}
			}

			return map[string]any{
				"FailedEntryCount": len(failed),
				"FailedEntries":    failed,
			}, nil
		},
		"ListTargetsByRule": func(b []byte) (any, error) {
			var input listTargetsByRuleInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			targets, next, err := h.Backend.ListTargetsByRule(input.Rule, input.EventBusName, input.NextToken)
			if err != nil {
				return nil, err
			}

			return map[string]any{"Targets": targets, "NextToken": next}, nil
		},
	}
}

func (h *Handler) eventsActions() map[string]actionFn {
	return map[string]actionFn{
		"PutEvents": func(b []byte) (any, error) {
			var input putEventsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			entries := h.Backend.PutEvents(input.Entries)

			return map[string]any{
				"FailedEntryCount": 0,
				"Entries":          entries,
			}, nil
		},
	}
}

func (h *Handler) tagActions() map[string]actionFn {
	return map[string]actionFn{
		"ListTagsForResource": func(b []byte) (any, error) {
			var input listTagsForResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			tagMap := h.getTags(input.ResourceARN)
			tagList := make([]map[string]string, 0, len(tagMap))
			for k, v := range tagMap {
				tagList = append(tagList, map[string]string{"Key": k, "Value": v})
			}

			return map[string]any{"Tags": tagList}, nil
		},
		"TagResource": func(b []byte) (any, error) {
			var input tagResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			kv := make(map[string]string, len(input.Tags))
			for _, t := range input.Tags {
				kv[t.Key] = t.Value
			}
			h.setTags(input.ResourceARN, kv)

			return map[string]any{}, nil
		},
		"UntagResource": func(b []byte) (any, error) {
			var input untagResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			h.removeTags(input.ResourceARN, input.TagKeys)

			return map[string]any{}, nil
		},
	}
}

func (h *Handler) dispatchTable() map[string]actionFn {
	table := make(map[string]actionFn)
	maps.Copy(table, h.eventBusActions())
	maps.Copy(table, h.ruleActions())
	maps.Copy(table, h.ruleStateActions())
	maps.Copy(table, h.targetActions())
	maps.Copy(table, h.eventsActions())
	maps.Copy(table, h.tagActions())

	return table
}

// dispatch routes the action to the correct handler function.
func (h *Handler) dispatch(_ context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.dispatchTable()[action]
	if !ok {
		return nil, fmt.Errorf("%w:%s", errUnknownOperation, action)
	}

	response, err := fn(body)
	if err != nil {
		return nil, err
	}

	return json.Marshal(response)
}

// handleError writes a standardized JSON error response.
func (h *Handler) handleError(ctx context.Context, c *echo.Context, action string, reqErr error) error {
	log := logger.Load(ctx)
	c.Response().Header().Set("Content-Type", "application/x-amz-json-1.1")

	var errType string
	var statusCode int

	switch {
	case errors.Is(reqErr, ErrEventBusNotFound), errors.Is(reqErr, ErrRuleNotFound):
		errType = "ResourceNotFoundException"
		statusCode = http.StatusNotFound
	case errors.Is(reqErr, ErrEventBusAlreadyExists):
		errType = "ResourceAlreadyExistsException"
		statusCode = http.StatusConflict
	case errors.Is(reqErr, ErrCannotDeleteDefaultBus):
		errType = "IllegalArgumentException"
		statusCode = http.StatusBadRequest
	case errors.Is(reqErr, ErrInvalidParameter):
		errType = "InvalidParameterException"
		statusCode = http.StatusBadRequest
	case errors.Is(reqErr, errUnknownOperation):
		errType = "UnknownOperationException"
		statusCode = http.StatusBadRequest
	default:
		errType = "InternalServerError"
		statusCode = http.StatusInternalServerError
	}

	if statusCode == http.StatusInternalServerError {
		log.ErrorContext(ctx, "EventBridge internal error", "error", reqErr, "action", action)
	} else {
		log.WarnContext(ctx, "EventBridge request error", "error", reqErr, "action", action)
	}

	errResp := service.JSONErrorResponse{
		Type:    errType,
		Message: reqErr.Error(),
	}

	payload, _ := json.Marshal(errResp)

	return c.JSONBlob(statusCode, payload)
}
