package eventbridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

var errUnknownOperation = errors.New("UnknownOperationException")

type createEventBusInput struct {
	Tags        map[string]string `json:"Tags,omitempty"`
	Name        string            `json:"Name"`
	Description string            `json:"Description"`
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

type tagResourceInput struct {
	ResourceARN string       `json:"ResourceARN"`
	Tags        []svcTags.KV `json:"Tags"`
}

type untagResourceInput struct {
	ResourceARN string   `json:"ResourceARN"`
	TagKeys     []string `json:"TagKeys"`
}

// Handler is the Echo HTTP service handler for EventBridge operations.
type Handler struct {
	Backend        StorageBackend
	ops            map[string]actionFn
	scheduler      *Scheduler
	archiveJanitor *ArchiveJanitor
	tags           map[string]*svcTags.Tags
	tagsMu         *lockmetrics.RWMutex
	DefaultRegion  string
}

// NewHandler creates a new EventBridge handler.
func NewHandler(backend StorageBackend) *Handler {
	h := &Handler{
		Backend:       backend,
		DefaultRegion: config.DefaultRegion,
		tags:          make(map[string]*svcTags.Tags),
		tagsMu:        lockmetrics.New("eb.tags"),
	}
	h.ops = h.buildOps()

	return h
}

func (h *Handler) setTags(resourceID string, kv map[string]string) {
	h.tagsMu.Lock("setTags")
	defer h.tagsMu.Unlock()
	if h.tags[resourceID] == nil {
		h.tags[resourceID] = svcTags.New("eb." + resourceID + ".tags")
	}
	h.tags[resourceID].Merge(kv)
}

func (h *Handler) removeTags(resourceID string, keys []string) {
	h.tagsMu.RLock("removeTags")
	t := h.tags[resourceID]
	h.tagsMu.RUnlock()
	if t != nil {
		t.DeleteKeys(keys)
	}
}

func (h *Handler) getTags(resourceID string) map[string]string {
	h.tagsMu.RLock("getTags")
	t := h.tags[resourceID]
	h.tagsMu.RUnlock()
	if t == nil {
		return map[string]string{}
	}

	return t.Clone()
}

// SetScheduler attaches a Scheduler to the handler. The scheduler is started as a
// background worker when StartWorker is called (which satisfies service.BackgroundWorker).
func (h *Handler) SetScheduler(s *Scheduler) {
	h.scheduler = s
}

// SetArchiveJanitor attaches an archive janitor to the handler.
func (h *Handler) SetArchiveJanitor(j *ArchiveJanitor) {
	h.archiveJanitor = j
}

// StartWorker implements service.BackgroundWorker.
// It starts the EventBridge scheduled-rules scheduler as a background goroutine.
func (h *Handler) StartWorker(ctx context.Context) error {
	if h.scheduler != nil {
		go h.scheduler.Run(ctx)
	}
	if h.archiveJanitor != nil {
		go h.archiveJanitor.Run(ctx)
	}

	return nil
}

// Shutdown implements service.Shutdowner.
// It cancels the backend's internal lifecycle context and waits for all
// in-flight delivery goroutines to finish. If ctx expires before Close
// returns, Shutdown returns immediately so the process shutdown is not blocked.
func (h *Handler) Shutdown(ctx context.Context) {
	type closer interface{ Close() }

	b, ok := h.Backend.(closer)
	if !ok {
		return
	}

	done := make(chan struct{})

	go func() {
		b.Close()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
	}
}

// Ensure Handler implements service.BackgroundWorker and service.Shutdowner at compile time.
var (
	_ service.BackgroundWorker = (*Handler)(nil)
	_ service.Shutdowner       = (*Handler)(nil)
)

// Name returns the service name.
func (h *Handler) Name() string { return "EventBridge" }

// GetSupportedOperations returns all mocked EventBridge operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateEventBus",
		"DeleteEventBus",
		"ListEventBuses",
		"DescribeEventBus",
		"UpdateEventBus",
		"PutRule",
		"DeleteRule",
		"ListRules",
		"DescribeRule",
		"EnableRule",
		"DisableRule",
		"PutTargets",
		"RemoveTargets",
		"ListTargetsByRule",
		"ListRuleNamesByTarget",
		"PutEvents",
		"PutPartnerEvents",
		"ListTagsForResource",
		"TagResource",
		"UntagResource",
		"ActivateEventSource",
		"DeactivateEventSource",
		"DescribeEventSource",
		"ListEventSources",
		"CancelReplay",
		"DescribeReplay",
		"ListReplays",
		"StartReplay",
		"CreateApiDestination",
		"DeleteApiDestination",
		"DescribeApiDestination",
		"ListApiDestinations",
		"UpdateApiDestination",
		"CreateArchive",
		"DeleteArchive",
		"DescribeArchive",
		"ListArchives",
		"UpdateArchive",
		"CreateConnection",
		"DeleteConnection",
		"DescribeConnection",
		"ListConnections",
		"UpdateConnection",
		"DeauthorizeConnection",
		"CreateEndpoint",
		"DeleteEndpoint",
		"DescribeEndpoint",
		"ListEndpoints",
		"UpdateEndpoint",
		"CreatePartnerEventSource",
		"DeletePartnerEventSource",
		"DescribePartnerEventSource",
		"ListPartnerEventSources",
		"ListPartnerEventSourceAccounts",
		"TestEventPattern",
		"PutPermission",
		"RemovePermission",
		"GetEventBusPolicy",
		"PutEventBusPolicy",
		"CreatePipe",
		"DeletePipe",
		"DescribePipe",
		"ListPipes",
		"UpdatePipe",
		// Schema Registry operations.
		"CreateRegistry",
		"DeleteRegistry",
		"DescribeRegistry",
		"ListRegistries",
		"UpdateRegistry",
		"CreateSchema",
		"DeleteSchema",
		"DescribeSchema",
		"ListSchemas",
		"SearchSchemas",
		"UpdateSchema",
		"ListSchemaVersions",
		"DescribeSchemaVersion",
		"DeleteSchemaVersion",
		"GetDiscoveredSchema",
		"PutCodeBinding",
		"DescribeCodeBinding",
		"ListCodeBindings",
		"GetCodeBindingSource",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "events" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this EventBridge instance handles.
func (h *Handler) ChaosRegions() []string { return []string{config.DefaultRegion} }

// RouteMatcher returns a matcher for EventBridge requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		target := c.Request().Header.Get("X-Amz-Target")

		return strings.HasPrefix(target, "AmazonEventBridge.") ||
			strings.HasPrefix(target, "AWSEvents.") ||
			strings.HasPrefix(target, "AWSSchemas.")
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
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var data map[string]any
	if uerr := json.Unmarshal(body, &data); uerr != nil {
		return ""
	}

	for _, key := range []string{"Name", "Rule", "EventBusName", "ReplayName", "ArchiveName"} {
		if v, ok := data[key].(string); ok && v != "" {
			return v
		}
	}

	return ""
}

// Handler returns the Echo handler function for EventBridge requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		region := httputils.ExtractRegionFromRequest(c.Request(), h.DefaultRegion)
		ctx := context.WithValue(c.Request().Context(), regionContextKey{}, region)
		c.SetRequest(c.Request().WithContext(ctx))

		return service.HandleTarget(
			c, logger.Load(ctx),
			"EventBridge", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

type actionFn func(context.Context, []byte) (any, error)

type createEventBusOutput struct {
	EventBusArn string `json:"EventBusArn"`
}

type deleteEventBusOutput struct{}

type listEventBusesOutput struct {
	NextToken  string     `json:"NextToken"`
	EventBuses []EventBus `json:"EventBuses"`
}

type putRuleOutput struct {
	RuleArn string `json:"RuleArn"`
}

type deleteRuleOutput struct{}

type listRulesOutput struct {
	NextToken string `json:"NextToken"`
	Rules     []Rule `json:"Rules"`
}

type enableRuleOutput struct{}

type disableRuleOutput struct{}

type putTargetsOutput struct {
	FailedEntries    []FailedEntry `json:"FailedEntries"`
	FailedEntryCount int           `json:"FailedEntryCount"`
}

type removeTargetsOutput struct {
	FailedEntries    []FailedEntry `json:"FailedEntries"`
	FailedEntryCount int           `json:"FailedEntryCount"`
}

type listTargetsByRuleOutput struct {
	NextToken string   `json:"NextToken"`
	Targets   []Target `json:"Targets"`
}

type putEventsOutput struct {
	Entries          []EventResultEntry `json:"Entries"`
	FailedEntryCount int                `json:"FailedEntryCount"`
}

type listTagsForResourceOutput struct {
	Tags []svcTags.KV `json:"Tags"`
}

type tagResourceOutput struct{}

type untagResourceOutput struct{}

func (h *Handler) eventBusActions() map[string]actionFn {
	return map[string]actionFn{
		"CreateEventBus": func(ctx context.Context, b []byte) (any, error) {
			var input createEventBusInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			bus, err := h.Backend.CreateEventBus(ctx, input.Name, input.Description)
			if err != nil {
				return nil, err
			}
			if len(input.Tags) > 0 {
				h.setTags(bus.Arn, input.Tags)
			}

			return &createEventBusOutput{EventBusArn: bus.Arn}, nil
		},
		"DeleteEventBus": func(ctx context.Context, b []byte) (any, error) {
			var input deleteEventBusInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			if err := h.Backend.DeleteEventBus(ctx, input.Name); err != nil {
				return nil, err
			}

			return &deleteEventBusOutput{}, nil
		},
		"ListEventBuses": func(ctx context.Context, b []byte) (any, error) {
			var input listEventBusesInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			buses, next, err := h.Backend.ListEventBuses(ctx, input.NamePrefix, input.NextToken)
			if err != nil {
				return nil, err
			}

			return &listEventBusesOutput{EventBuses: buses, NextToken: next}, nil
		},
		"DescribeEventBus": func(ctx context.Context, b []byte) (any, error) {
			var input describeEventBusInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			bus, err := h.Backend.DescribeEventBus(ctx, input.Name)
			if err != nil {
				return nil, err
			}

			return bus, nil
		},
	}
}

func (h *Handler) ruleActions() map[string]actionFn {
	return map[string]actionFn{
		"PutRule": func(ctx context.Context, b []byte) (any, error) {
			var input PutRuleInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			rule, err := h.Backend.PutRule(ctx, input)
			if err != nil {
				return nil, err
			}
			if len(input.Tags) > 0 {
				h.setTags(rule.Arn, input.Tags)
			}

			return &putRuleOutput{RuleArn: rule.Arn}, nil
		},
		"DeleteRule": func(ctx context.Context, b []byte) (any, error) {
			var input deleteRuleInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			if err := h.Backend.DeleteRule(ctx, input.Name, input.EventBusName); err != nil {
				return nil, err
			}

			return &deleteRuleOutput{}, nil
		},
		"ListRules": func(ctx context.Context, b []byte) (any, error) {
			var input listRulesInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			rules, next, err := h.Backend.ListRules(
				ctx,
				input.EventBusName,
				input.NamePrefix,
				input.NextToken,
			)
			if err != nil {
				return nil, err
			}

			return &listRulesOutput{Rules: rules, NextToken: next}, nil
		},
		"DescribeRule": func(ctx context.Context, b []byte) (any, error) {
			var input describeRuleInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeRule(ctx, input.Name, input.EventBusName)
		},
	}
}

func (h *Handler) ruleStateActions() map[string]actionFn {
	return map[string]actionFn{
		"EnableRule": func(ctx context.Context, b []byte) (any, error) {
			var input enableRuleInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			if err := h.Backend.EnableRule(ctx, input.Name, input.EventBusName); err != nil {
				return nil, err
			}

			return &enableRuleOutput{}, nil
		},
		"DisableRule": func(ctx context.Context, b []byte) (any, error) {
			var input disableRuleInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			if err := h.Backend.DisableRule(ctx, input.Name, input.EventBusName); err != nil {
				return nil, err
			}

			return &disableRuleOutput{}, nil
		},
	}
}

func (h *Handler) targetActions() map[string]actionFn {
	return map[string]actionFn{
		"PutTargets": func(ctx context.Context, b []byte) (any, error) {
			var input putTargetsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			failed, err := h.Backend.PutTargets(ctx, input.Rule, input.EventBusName, input.Targets)
			if err != nil {
				return nil, err
			}
			if failed == nil {
				failed = []FailedEntry{}
			}

			return &putTargetsOutput{
				FailedEntryCount: len(failed),
				FailedEntries:    failed,
			}, nil
		},
		"RemoveTargets": func(ctx context.Context, b []byte) (any, error) {
			var input removeTargetsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			failed, err := h.Backend.RemoveTargets(ctx, input.Rule, input.EventBusName, input.IDs)
			if err != nil {
				return nil, err
			}
			if failed == nil {
				failed = []FailedEntry{}
			}

			return &removeTargetsOutput{
				FailedEntryCount: len(failed),
				FailedEntries:    failed,
			}, nil
		},
		"ListTargetsByRule": func(ctx context.Context, b []byte) (any, error) {
			var input listTargetsByRuleInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			targets, next, err := h.Backend.ListTargetsByRule(
				ctx,
				input.Rule,
				input.EventBusName,
				input.NextToken,
			)
			if err != nil {
				return nil, err
			}

			return &listTargetsByRuleOutput{Targets: targets, NextToken: next}, nil
		},
	}
}

func (h *Handler) eventsActions() map[string]actionFn {
	return map[string]actionFn{
		"PutEvents": func(ctx context.Context, b []byte) (any, error) {
			var input putEventsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			entries := h.Backend.PutEvents(ctx, input.Entries)

			return &putEventsOutput{
				FailedEntryCount: 0,
				Entries:          entries,
			}, nil
		},
	}
}

func (h *Handler) tagActions() map[string]actionFn {
	return map[string]actionFn{
		"ListTagsForResource": func(ctx context.Context, b []byte) (any, error) { //nolint:revive // existing issue.
			var input listTagsForResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			tagMap := h.getTags(input.ResourceARN)
			tagList := make([]svcTags.KV, 0, len(tagMap))
			for k, v := range tagMap {
				tagList = append(tagList, svcTags.KV{Key: k, Value: v})
			}

			return &listTagsForResourceOutput{Tags: tagList}, nil
		},
		"TagResource": func(ctx context.Context, b []byte) (any, error) { //nolint:revive // existing issue.
			var input tagResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			kv := make(map[string]string, len(input.Tags))
			for _, t := range input.Tags {
				kv[t.Key] = t.Value
			}
			h.setTags(input.ResourceARN, kv)

			return &tagResourceOutput{}, nil
		},
		"UntagResource": func(ctx context.Context, b []byte) (any, error) { //nolint:revive // existing issue.
			var input untagResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			h.removeTags(input.ResourceARN, input.TagKeys)

			return &untagResourceOutput{}, nil
		},
	}
}

type createAPIDestinationOutput struct {
	APIDestinationArn   string  `json:"ApiDestinationArn"`
	APIDestinationState string  `json:"ApiDestinationState"`
	CreationTime        float64 `json:"CreationTime"`
	LastModifiedTime    float64 `json:"LastModifiedTime"`
}

type createArchiveOutput struct {
	ArchiveArn   string  `json:"ArchiveArn"`
	State        string  `json:"State"`
	StateReason  string  `json:"StateReason,omitempty"`
	CreationTime float64 `json:"CreationTime"`
}

type createConnectionOutput struct {
	ConnectionArn    string  `json:"ConnectionArn"`
	ConnectionState  string  `json:"ConnectionState"`
	CreationTime     float64 `json:"CreationTime"`
	LastModifiedTime float64 `json:"LastModifiedTime"`
}

type createEndpointOutput struct {
	Arn         string `json:"Arn"`
	EndpointID  string `json:"EndpointId"`
	EndpointURL string `json:"EndpointUrl"`
	State       string `json:"State"`
}

type createPartnerEventSourceOutput struct {
	EventSourceArn string `json:"EventSourceArn"`
}

type cancelReplayOutput struct {
	ReplayArn   string `json:"ReplayArn"`
	State       string `json:"State"`
	StateReason string `json:"StateReason,omitempty"`
}

type deauthorizeConnectionOutput struct {
	ConnectionArn    string  `json:"ConnectionArn"`
	ConnectionState  string  `json:"ConnectionState"`
	LastModifiedTime float64 `json:"LastModifiedTime"`
}

type (
	activateEventSourceOutput   struct{}
	deactivateEventSourceOutput struct{}
	deleteAPIDestinationOutput  struct{}
)

// timeToEpochSeconds converts a time.Time to a float64 Unix epoch seconds value,
// as required by the AWS JSON protocol for timestamp fields.
func timeToEpochSeconds(t time.Time) float64 {
	return float64(t.Unix())
}

// archiveResponse is the handler-level DTO for Archive objects.
// Timestamps are float64 Unix epoch seconds as required by the AWS JSON protocol.
type archiveResponse struct {
	ArchiveName    string  `json:"ArchiveName"`
	ArchiveArn     string  `json:"ArchiveArn"`
	Description    string  `json:"Description,omitempty"`
	EventPattern   string  `json:"EventPattern,omitempty"`
	EventSourceArn string  `json:"EventSourceArn"`
	State          string  `json:"State"`
	StateReason    string  `json:"StateReason,omitempty"`
	CreationTime   float64 `json:"CreationTime"`
	EventCount     int64   `json:"EventCount"`
	RetentionDays  int     `json:"RetentionDays,omitempty"`
	SizeBytes      int64   `json:"SizeBytes"`
}

func archiveToResponse(a *Archive) *archiveResponse {
	if a == nil {
		return nil
	}

	return &archiveResponse{
		CreationTime:   timeToEpochSeconds(a.CreationTime),
		ArchiveName:    a.ArchiveName,
		ArchiveArn:     a.ArchiveArn,
		Description:    a.Description,
		EventPattern:   a.EventPattern,
		EventSourceArn: a.EventSourceArn,
		State:          a.State,
		StateReason:    a.StateReason,
		EventCount:     a.EventCount,
		RetentionDays:  a.RetentionDays,
		SizeBytes:      a.SizeBytes,
	}
}

// connectionResponse is the handler-level DTO for Connection objects.
type connectionResponse struct {
	AuthParameters     *ConnectionAuthParameters `json:"AuthParameters,omitempty"`
	ConnectionArn      string                    `json:"ConnectionArn"`
	AuthorizationType  string                    `json:"AuthorizationType"`
	ConnectionState    string                    `json:"ConnectionState"`
	Description        string                    `json:"Description,omitempty"`
	Name               string                    `json:"Name"`
	SecretArn          string                    `json:"SecretArn,omitempty"`
	StateReason        string                    `json:"StateReason,omitempty"`
	CreationTime       float64                   `json:"CreationTime"`
	LastAuthorizedTime float64                   `json:"LastAuthorizedTime,omitempty"`
	LastModifiedTime   float64                   `json:"LastModifiedTime"`
}

func connectionToResponse(c *Connection) *connectionResponse {
	if c == nil {
		return nil
	}

	r := &connectionResponse{
		AuthParameters:    c.AuthParameters,
		ConnectionArn:     c.ConnectionArn,
		AuthorizationType: c.AuthorizationType,
		ConnectionState:   c.ConnectionState,
		CreationTime:      timeToEpochSeconds(c.CreationTime),
		Description:       c.Description,
		LastModifiedTime:  timeToEpochSeconds(c.LastModifiedTime),
		Name:              c.Name,
		SecretArn:         c.SecretArn,
		StateReason:       c.StateReason,
	}

	if !c.LastAuthorizedTime.IsZero() {
		r.LastAuthorizedTime = timeToEpochSeconds(c.LastAuthorizedTime)
	}

	return r
}

// apiDestinationResponse is the handler-level DTO for APIDestination objects.
type apiDestinationResponse struct {
	APIDestinationArn            string  `json:"ApiDestinationArn"`
	APIDestinationState          string  `json:"ApiDestinationState"`
	ConnectionArn                string  `json:"ConnectionArn"`
	Description                  string  `json:"Description,omitempty"`
	HTTPMethod                   string  `json:"HttpMethod"`
	InvocationEndpoint           string  `json:"InvocationEndpoint"`
	Name                         string  `json:"Name"`
	CreationTime                 float64 `json:"CreationTime"`
	LastModifiedTime             float64 `json:"LastModifiedTime"`
	InvocationRateLimitPerSecond int     `json:"InvocationRateLimitPerSecond,omitempty"`
}

func apiDestinationToResponse(d *APIDestination) *apiDestinationResponse {
	if d == nil {
		return nil
	}

	return &apiDestinationResponse{
		CreationTime:                 timeToEpochSeconds(d.CreationTime),
		LastModifiedTime:             timeToEpochSeconds(d.LastModifiedTime),
		APIDestinationArn:            d.APIDestinationArn,
		APIDestinationState:          d.APIDestinationState,
		ConnectionArn:                d.ConnectionArn,
		Description:                  d.Description,
		HTTPMethod:                   d.HTTPMethod,
		InvocationEndpoint:           d.InvocationEndpoint,
		Name:                         d.Name,
		InvocationRateLimitPerSecond: d.InvocationRateLimitPerSecond,
	}
}

func (h *Handler) eventSourceActions() map[string]actionFn {
	return map[string]actionFn{
		"ActivateEventSource": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				Name string `json:"Name"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			if err := h.Backend.ActivateEventSource(ctx, input.Name); err != nil {
				return nil, err
			}

			return &activateEventSourceOutput{}, nil
		},
		"DeactivateEventSource": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				Name string `json:"Name"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			if err := h.Backend.DeactivateEventSource(ctx, input.Name); err != nil {
				return nil, err
			}

			return &deactivateEventSourceOutput{}, nil
		},
		"CreatePartnerEventSource": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				Name    string `json:"Name"`
				Account string `json:"Account"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			src, err := h.Backend.CreatePartnerEventSource(ctx, input.Name, input.Account)
			if err != nil {
				return nil, err
			}

			return &createPartnerEventSourceOutput{EventSourceArn: src.Arn}, nil
		},
	}
}

func (h *Handler) replayAndConnectionActions() map[string]actionFn {
	return map[string]actionFn{
		"CancelReplay": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				ReplayName string `json:"ReplayName"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			replay, err := h.Backend.CancelReplay(ctx, input.ReplayName)
			if err != nil {
				return nil, err
			}

			return &cancelReplayOutput{
				ReplayArn:   replay.ReplayArn,
				State:       replay.State,
				StateReason: replay.StateReason,
			}, nil
		},
		"CreateConnection": func(ctx context.Context, b []byte) (any, error) {
			var input CreateConnectionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			conn, err := h.Backend.CreateConnection(ctx, input)
			if err != nil {
				return nil, err
			}

			return &createConnectionOutput{
				ConnectionArn:    conn.ConnectionArn,
				ConnectionState:  conn.ConnectionState,
				CreationTime:     timeToEpochSeconds(conn.CreationTime),
				LastModifiedTime: timeToEpochSeconds(conn.LastModifiedTime),
			}, nil
		},
		"DeauthorizeConnection": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				Name string `json:"Name"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			conn, err := h.Backend.DeauthorizeConnection(ctx, input.Name)
			if err != nil {
				return nil, err
			}

			return &deauthorizeConnectionOutput{
				ConnectionArn:    conn.ConnectionArn,
				ConnectionState:  conn.ConnectionState,
				LastModifiedTime: timeToEpochSeconds(conn.LastModifiedTime),
			}, nil
		},
	}
}

func (h *Handler) apiDestinationAndArchiveActions() map[string]actionFn {
	return map[string]actionFn{
		"CreateApiDestination": func(ctx context.Context, b []byte) (any, error) {
			var input CreateAPIDestinationInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			dst, err := h.Backend.CreateAPIDestination(ctx, input)
			if err != nil {
				return nil, err
			}

			return &createAPIDestinationOutput{
				APIDestinationArn:   dst.APIDestinationArn,
				APIDestinationState: dst.APIDestinationState,
				CreationTime:        timeToEpochSeconds(dst.CreationTime),
				LastModifiedTime:    timeToEpochSeconds(dst.LastModifiedTime),
			}, nil
		},
		"CreateArchive": func(ctx context.Context, b []byte) (any, error) {
			var input CreateArchiveInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			archive, err := h.Backend.CreateArchive(ctx, input)
			if err != nil {
				return nil, err
			}

			return &createArchiveOutput{
				ArchiveArn:   archive.ArchiveArn,
				CreationTime: timeToEpochSeconds(archive.CreationTime),
				State:        archive.State,
				StateReason:  archive.StateReason,
			}, nil
		},
		"DeleteApiDestination": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				Name string `json:"Name"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			if err := h.Backend.DeleteAPIDestination(ctx, input.Name); err != nil {
				return nil, err
			}

			return &deleteAPIDestinationOutput{}, nil
		},
		"CreateEndpoint": func(ctx context.Context, b []byte) (any, error) {
			var input CreateEndpointInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			ep, err := h.Backend.CreateEndpoint(ctx, input)
			if err != nil {
				return nil, err
			}

			return &createEndpointOutput{
				Arn:         ep.Arn,
				EndpointID:  ep.EndpointID,
				EndpointURL: ep.EndpointURL,
				State:       ep.State,
			}, nil
		},
	}
}

// extendedArchiveActions returns CRUD actions for archives beyond Create.
func (h *Handler) extendedArchiveActions() map[string]actionFn {
	return map[string]actionFn{
		"DeleteArchive": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				ArchiveName string `json:"ArchiveName"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return &struct{}{}, h.Backend.DeleteArchive(ctx, input.ArchiveName)
		},
		"DescribeArchive": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				ArchiveName string `json:"ArchiveName"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			archive, err := h.Backend.DescribeArchive(ctx, input.ArchiveName)
			if err != nil {
				return nil, err
			}

			return archiveToResponse(archive), nil
		},
		"ListArchives": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				NamePrefix string `json:"NamePrefix"`
				NextToken  string `json:"NextToken"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			archives, next, err := h.Backend.ListArchives(ctx, input.NamePrefix, input.NextToken)
			if err != nil {
				return nil, err
			}

			archiveResponses := make([]archiveResponse, len(archives))
			for i, a := range archives {
				archiveResponses[i] = *archiveToResponse(&a)
			}

			return &struct {
				NextToken string            `json:"NextToken,omitempty"`
				Archives  []archiveResponse `json:"Archives"`
			}{Archives: archiveResponses, NextToken: next}, nil
		},
		"UpdateArchive": func(ctx context.Context, b []byte) (any, error) {
			var input UpdateArchiveInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			archive, err := h.Backend.UpdateArchive(ctx, input)
			if err != nil {
				return nil, err
			}

			return &struct {
				ArchiveArn   string  `json:"ArchiveArn"`
				State        string  `json:"State"`
				StateReason  string  `json:"StateReason,omitempty"`
				CreationTime float64 `json:"CreationTime"`
			}{
				ArchiveArn:   archive.ArchiveArn,
				CreationTime: timeToEpochSeconds(archive.CreationTime),
				State:        archive.State,
				StateReason:  archive.StateReason,
			}, nil
		},
	}
}

// extendedConnectionActions returns CRUD actions for connections beyond Create/Deauthorize.
func (h *Handler) extendedConnectionActions() map[string]actionFn {
	return map[string]actionFn{
		"DeleteConnection": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				Name string `json:"Name"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return &struct{}{}, h.Backend.DeleteConnection(ctx, input.Name)
		},
		"DescribeConnection": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				Name string `json:"Name"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			conn, err := h.Backend.DescribeConnection(ctx, input.Name)
			if err != nil {
				return nil, err
			}

			return connectionToResponse(conn), nil
		},
		"ListConnections": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				NamePrefix string `json:"NamePrefix"`
				NextToken  string `json:"NextToken"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			conns, next, err := h.Backend.ListConnections(ctx, input.NamePrefix, input.NextToken)
			if err != nil {
				return nil, err
			}

			connResponses := make([]connectionResponse, len(conns))
			for i, c := range conns {
				connResponses[i] = *connectionToResponse(&c)
			}

			return &struct {
				NextToken   string               `json:"NextToken,omitempty"`
				Connections []connectionResponse `json:"Connections"`
			}{Connections: connResponses, NextToken: next}, nil
		},
		"UpdateConnection": func(ctx context.Context, b []byte) (any, error) {
			var input UpdateConnectionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			conn, err := h.Backend.UpdateConnection(ctx, input)
			if err != nil {
				return nil, err
			}

			return &struct {
				ConnectionArn    string  `json:"ConnectionArn"`
				ConnectionState  string  `json:"ConnectionState"`
				CreationTime     float64 `json:"CreationTime"`
				LastModifiedTime float64 `json:"LastModifiedTime"`
			}{
				ConnectionArn:    conn.ConnectionArn,
				ConnectionState:  conn.ConnectionState,
				CreationTime:     timeToEpochSeconds(conn.CreationTime),
				LastModifiedTime: timeToEpochSeconds(conn.LastModifiedTime),
			}, nil
		},
	}
}

// extendedEndpointActions returns CRUD actions for endpoints beyond Create.
func (h *Handler) extendedEndpointActions() map[string]actionFn {
	return map[string]actionFn{
		"DeleteEndpoint": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				Name string `json:"Name"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return &struct{}{}, h.Backend.DeleteEndpoint(ctx, input.Name)
		},
		"DescribeEndpoint": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				Name string `json:"Name"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeEndpoint(ctx, input.Name)
		},
		"ListEndpoints": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				NamePrefix string `json:"NamePrefix"`
				NextToken  string `json:"NextToken"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			eps, next, err := h.Backend.ListEndpoints(ctx, input.NamePrefix, input.NextToken)
			if err != nil {
				return nil, err
			}

			return &struct {
				NextToken string     `json:"NextToken,omitempty"`
				Endpoints []Endpoint `json:"Endpoints"`
			}{Endpoints: eps, NextToken: next}, nil
		},
		"UpdateEndpoint": func(ctx context.Context, b []byte) (any, error) {
			var input UpdateEndpointInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			ep, err := h.Backend.UpdateEndpoint(ctx, input)
			if err != nil {
				return nil, err
			}

			return &struct {
				Arn         string `json:"Arn"`
				EndpointID  string `json:"EndpointId"`
				EndpointURL string `json:"EndpointUrl"`
				State       string `json:"State"`
			}{
				Arn:         ep.Arn,
				EndpointID:  ep.EndpointID,
				EndpointURL: ep.EndpointURL,
				State:       ep.State,
			}, nil
		},
	}
}

// extendedAPIDestinationActions returns Describe/List/Update for API destinations.
func (h *Handler) extendedAPIDestinationActions() map[string]actionFn {
	return map[string]actionFn{
		"DescribeApiDestination": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				Name string `json:"Name"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			dst, err := h.Backend.DescribeAPIDestination(ctx, input.Name)
			if err != nil {
				return nil, err
			}

			return apiDestinationToResponse(dst), nil
		},
		"ListApiDestinations": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				NamePrefix string `json:"NamePrefix"`
				NextToken  string `json:"NextToken"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			dsts, next, err := h.Backend.ListAPIDestinations(ctx, input.NamePrefix, input.NextToken)
			if err != nil {
				return nil, err
			}

			dstResponses := make([]apiDestinationResponse, len(dsts))
			for i, d := range dsts {
				dstResponses[i] = *apiDestinationToResponse(&d)
			}

			return &struct {
				NextToken       string                   `json:"NextToken,omitempty"`
				APIDestinations []apiDestinationResponse `json:"ApiDestinations"`
			}{APIDestinations: dstResponses, NextToken: next}, nil
		},
		"UpdateApiDestination": func(ctx context.Context, b []byte) (any, error) {
			var input UpdateAPIDestinationInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			dst, err := h.Backend.UpdateAPIDestination(ctx, input)
			if err != nil {
				return nil, err
			}

			return &struct {
				APIDestinationArn   string  `json:"ApiDestinationArn"`
				APIDestinationState string  `json:"ApiDestinationState"`
				CreationTime        float64 `json:"CreationTime"`
				LastModifiedTime    float64 `json:"LastModifiedTime"`
			}{
				APIDestinationArn:   dst.APIDestinationArn,
				APIDestinationState: dst.APIDestinationState,
				CreationTime:        timeToEpochSeconds(dst.CreationTime),
				LastModifiedTime:    timeToEpochSeconds(dst.LastModifiedTime),
			}, nil
		},
	}
}

// extendedEventSourceActions returns Describe/List for event sources.
func (h *Handler) extendedEventSourceActions() map[string]actionFn {
	return map[string]actionFn{
		"DescribeEventSource": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				Name string `json:"Name"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeEventSource(ctx, input.Name)
		},
		"ListEventSources": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				NamePrefix string `json:"NamePrefix"`
				NextToken  string `json:"NextToken"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			srcs, next, err := h.Backend.ListEventSources(ctx, input.NamePrefix, input.NextToken)
			if err != nil {
				return nil, err
			}

			return &struct {
				NextToken    string        `json:"NextToken,omitempty"`
				EventSources []EventSource `json:"EventSources"`
			}{EventSources: srcs, NextToken: next}, nil
		},
	}
}

// extendedPartnerSourceActions returns CRUD actions for partner event sources beyond Create.
func (h *Handler) extendedPartnerSourceActions() map[string]actionFn {
	return map[string]actionFn{
		"DeletePartnerEventSource": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				Name string `json:"Name"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return &struct{}{}, h.Backend.DeletePartnerEventSource(ctx, input.Name)
		},
		"DescribePartnerEventSource": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				Name string `json:"Name"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribePartnerEventSource(ctx, input.Name)
		},
		"ListPartnerEventSources": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				NamePrefix string `json:"NamePrefix"`
				NextToken  string `json:"NextToken"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			srcs, next, err := h.Backend.ListPartnerEventSources(ctx, input.NamePrefix, input.NextToken)
			if err != nil {
				return nil, err
			}

			return &struct {
				NextToken           string               `json:"NextToken,omitempty"`
				PartnerEventSources []PartnerEventSource `json:"PartnerEventSources"`
			}{PartnerEventSources: srcs, NextToken: next}, nil
		},
		"ListPartnerEventSourceAccounts": func(_ context.Context, _ []byte) (any, error) {
			// ListPartnerEventSourceAccounts returns accounts that have been
			// granted access to a partner event source. Cross-account metadata
			// has no meaningful in-process simulation; return empty list.
			return &struct {
				NextToken                  string `json:"NextToken,omitempty"`
				PartnerEventSourceAccounts []any  `json:"PartnerEventSourceAccounts"`
			}{PartnerEventSourceAccounts: []any{}}, nil
		},
		"PutPartnerEvents": func(ctx context.Context, b []byte) (any, error) {
			var input putEventsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			entries := h.Backend.PutPartnerEvents(ctx, input.Entries)

			return &putEventsOutput{
				FailedEntryCount: 0,
				Entries:          entries,
			}, nil
		},
	}
}

// extendedReplayActions returns Describe/List/Start replay actions.
func (h *Handler) extendedReplayActions() map[string]actionFn {
	return map[string]actionFn{
		"DescribeReplay": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				ReplayName string `json:"ReplayName"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeReplay(ctx, input.ReplayName)
		},
		"ListReplays": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				NamePrefix string `json:"NamePrefix"`
				NextToken  string `json:"NextToken"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			replays, next, err := h.Backend.ListReplays(ctx, input.NamePrefix, input.NextToken)
			if err != nil {
				return nil, err
			}

			return &struct {
				NextToken string   `json:"NextToken,omitempty"`
				Replays   []Replay `json:"Replays"`
			}{Replays: replays, NextToken: next}, nil
		},
		"StartReplay": func(ctx context.Context, b []byte) (any, error) {
			var input StartReplayInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			replay, err := h.Backend.StartReplay(ctx, input)
			if err != nil {
				return nil, err
			}

			return &struct {
				ReplayArn       string  `json:"ReplayArn"`
				State           string  `json:"State"`
				StateReason     string  `json:"StateReason,omitempty"`
				ReplayStartTime float64 `json:"ReplayStartTime"`
			}{
				ReplayArn:       replay.ReplayArn,
				ReplayStartTime: timeToEpochSeconds(replay.ReplayStartTime),
				State:           replay.State,
				StateReason:     replay.StateReason,
			}, nil
		},
	}
}

// extendedMiscActions returns misc new operations.
func (h *Handler) extendedMiscActions() map[string]actionFn {
	return map[string]actionFn{
		"ListRuleNamesByTarget": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				EventBusName string `json:"EventBusName"`
				NextToken    string `json:"NextToken"`
				TargetArn    string `json:"TargetArn"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			names, next, err := h.Backend.ListRuleNamesByTarget(
				ctx,
				input.TargetArn,
				input.EventBusName,
				input.NextToken,
			)
			if err != nil {
				return nil, err
			}

			return &struct {
				NextToken string   `json:"NextToken,omitempty"`
				RuleNames []string `json:"RuleNames"`
			}{RuleNames: names, NextToken: next}, nil
		},
		"TestEventPattern": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				Event        string `json:"Event"`
				EventPattern string `json:"EventPattern"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			result, err := h.Backend.TestEventPattern(ctx, input.EventPattern, input.Event)
			if err != nil {
				return nil, err
			}

			return &struct {
				Result bool `json:"Result"`
			}{Result: result}, nil
		},
		"UpdateEventBus": func(ctx context.Context, b []byte) (any, error) {
			var input UpdateEventBusInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			bus, err := h.Backend.UpdateEventBus(ctx, input)
			if err != nil {
				return nil, err
			}

			return &struct {
				Arn         string `json:"Arn"`
				Description string `json:"Description,omitempty"`
				Name        string `json:"Name"`
			}{Arn: bus.Arn, Description: bus.Description, Name: bus.Name}, nil
		},
		"PutPermission": func(ctx context.Context, b []byte) (any, error) {
			var input PutPermissionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return &struct{}{}, h.Backend.PutPermission(ctx, input)
		},
		"RemovePermission": func(ctx context.Context, b []byte) (any, error) {
			var input RemovePermissionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return &struct{}{}, h.Backend.RemovePermission(ctx, input)
		},
	}
}

func (h *Handler) policyActions() map[string]actionFn {
	return map[string]actionFn{
		"GetEventBusPolicy": func(ctx context.Context, b []byte) (any, error) {
			var input GetEventBusPolicyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			policy, err := h.Backend.GetEventBusPolicy(ctx, input.EventBusName)
			if err != nil {
				return nil, err
			}

			return &struct {
				Policy string `json:"Policy,omitempty"`
			}{Policy: policy}, nil
		},
		"PutEventBusPolicy": func(ctx context.Context, b []byte) (any, error) {
			var input PutEventBusPolicyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return &struct{}{}, h.Backend.PutEventBusPolicy(ctx, input)
		},
	}
}

func (h *Handler) pipesActions() map[string]actionFn {
	return map[string]actionFn{
		"CreatePipe": func(ctx context.Context, b []byte) (any, error) {
			var input CreatePipeInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			pipe, err := h.Backend.CreatePipe(ctx, input)
			if err != nil {
				return nil, err
			}

			return &struct {
				Arn          string  `json:"Arn"`
				CurrentState string  `json:"CurrentState"`
				Name         string  `json:"Name"`
				CreationTime float64 `json:"CreationTime"`
			}{
				Arn:          pipe.Arn,
				CreationTime: timeToEpochSeconds(pipe.CreationTime),
				CurrentState: pipe.CurrentState,
				Name:         pipe.Name,
			}, nil
		},
		"DeletePipe": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				Name string `json:"Name"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return &struct{}{}, h.Backend.DeletePipe(ctx, input.Name)
		},
		"DescribePipe": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				Name string `json:"Name"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribePipe(ctx, input.Name)
		},
		"ListPipes": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				NamePrefix string `json:"NamePrefix"`
				NextToken  string `json:"NextToken"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			pipes, next, err := h.Backend.ListPipes(ctx, input.NamePrefix, input.NextToken)
			if err != nil {
				return nil, err
			}

			return &struct {
				NextToken string `json:"NextToken,omitempty"`
				Pipes     []Pipe `json:"Pipes"`
			}{Pipes: pipes, NextToken: next}, nil
		},
		"UpdatePipe": func(ctx context.Context, b []byte) (any, error) {
			var input UpdatePipeInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			pipe, err := h.Backend.UpdatePipe(ctx, input)
			if err != nil {
				return nil, err
			}

			return &struct {
				Arn              string  `json:"Arn"`
				CurrentState     string  `json:"CurrentState"`
				Name             string  `json:"Name"`
				LastModifiedTime float64 `json:"LastModifiedTime"`
			}{
				Arn:              pipe.Arn,
				CurrentState:     pipe.CurrentState,
				LastModifiedTime: timeToEpochSeconds(pipe.LastModifiedTime),
				Name:             pipe.Name,
			}, nil
		},
	}
}

func (h *Handler) schemaRegistryActions() map[string]actionFn {
	table := make(map[string]actionFn)
	maps.Copy(table, h.registryActions())
	maps.Copy(table, h.schemaActions())
	maps.Copy(table, h.schemaVersionActions())
	maps.Copy(table, h.codeBindingActions())

	return table
}

func (h *Handler) registryActions() map[string]actionFn {
	return map[string]actionFn{
		"CreateRegistry": func(ctx context.Context, b []byte) (any, error) {
			var input CreateRegistryInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.CreateRegistry(ctx, input)
		},
		"DeleteRegistry": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				RegistryName string `json:"RegistryName"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return &struct{}{}, h.Backend.DeleteRegistry(ctx, input.RegistryName)
		},
		"DescribeRegistry": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				RegistryName string `json:"RegistryName"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeRegistry(ctx, input.RegistryName)
		},
		"ListRegistries": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				NamePrefix string `json:"NamePrefix"`
				NextToken  string `json:"NextToken"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			regs, next, err := h.Backend.ListRegistries(ctx, input.NamePrefix, input.NextToken)
			if err != nil {
				return nil, err
			}

			return &struct {
				NextToken  string           `json:"NextToken,omitempty"`
				Registries []SchemaRegistry `json:"Registries"`
			}{Registries: regs, NextToken: next}, nil
		},
		"UpdateRegistry": func(ctx context.Context, b []byte) (any, error) {
			var input UpdateRegistryInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.UpdateRegistry(ctx, input)
		},
	}
}

func (h *Handler) schemaActions() map[string]actionFn {
	return map[string]actionFn{
		"CreateSchema": func(ctx context.Context, b []byte) (any, error) {
			var input CreateSchemaInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.CreateSchema(ctx, input)
		},
		"DeleteSchema": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				RegistryName string `json:"RegistryName"`
				SchemaName   string `json:"SchemaName"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return &struct{}{}, h.Backend.DeleteSchema(ctx, input.RegistryName, input.SchemaName)
		},
		"DescribeSchema": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				RegistryName  string `json:"RegistryName"`
				SchemaName    string `json:"SchemaName"`
				SchemaVersion string `json:"SchemaVersion"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeSchema(
				ctx,
				input.RegistryName,
				input.SchemaName,
				input.SchemaVersion,
			)
		},
		"ListSchemas": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				RegistryName     string `json:"RegistryName"`
				SchemaNamePrefix string `json:"SchemaNamePrefix"`
				NextToken        string `json:"NextToken"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			schemas, next, err := h.Backend.ListSchemas(
				ctx,
				input.RegistryName,
				input.SchemaNamePrefix,
				input.NextToken,
			)
			if err != nil {
				return nil, err
			}

			return &struct {
				NextToken string   `json:"NextToken,omitempty"`
				Schemas   []Schema `json:"Schemas"`
			}{Schemas: schemas, NextToken: next}, nil
		},
		"SearchSchemas": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				RegistryName string `json:"RegistryName"`
				Keywords     string `json:"Keywords"`
				NextToken    string `json:"NextToken"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			schemas, next, err := h.Backend.SearchSchemas(
				ctx,
				input.RegistryName,
				input.Keywords,
				input.NextToken,
			)
			if err != nil {
				return nil, err
			}

			return &struct {
				NextToken string   `json:"NextToken,omitempty"`
				Schemas   []Schema `json:"Schemas"`
			}{Schemas: schemas, NextToken: next}, nil
		},
		"UpdateSchema": func(ctx context.Context, b []byte) (any, error) {
			var input UpdateSchemaInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.UpdateSchema(ctx, input)
		},
	}
}

func (h *Handler) schemaVersionActions() map[string]actionFn {
	return map[string]actionFn{
		"ListSchemaVersions": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				RegistryName string `json:"RegistryName"`
				SchemaName   string `json:"SchemaName"`
				NextToken    string `json:"NextToken"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			versions, next, err := h.Backend.ListSchemaVersions(
				ctx,
				input.RegistryName,
				input.SchemaName,
				input.NextToken,
			)
			if err != nil {
				return nil, err
			}

			return &struct {
				NextToken      string          `json:"NextToken,omitempty"`
				SchemaVersions []SchemaVersion `json:"SchemaVersions"`
			}{SchemaVersions: versions, NextToken: next}, nil
		},
		"DescribeSchemaVersion": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				RegistryName  string `json:"RegistryName"`
				SchemaName    string `json:"SchemaName"`
				SchemaVersion string `json:"SchemaVersion"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeSchemaVersion(
				ctx,
				input.RegistryName,
				input.SchemaName,
				input.SchemaVersion,
			)
		},
		"DeleteSchemaVersion": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				RegistryName  string `json:"RegistryName"`
				SchemaName    string `json:"SchemaName"`
				SchemaVersion string `json:"SchemaVersion"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return &struct{}{}, h.Backend.DeleteSchemaVersion(
				ctx,
				input.RegistryName,
				input.SchemaName,
				input.SchemaVersion,
			)
		},
		"GetDiscoveredSchema": func(ctx context.Context, b []byte) (any, error) {
			var input GetDiscoveredSchemaInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			content, err := h.Backend.GetDiscoveredSchema(ctx, input)
			if err != nil {
				return nil, err
			}

			return &struct {
				Content string `json:"Content"`
			}{Content: content}, nil
		},
	}
}

func (h *Handler) codeBindingActions() map[string]actionFn {
	return map[string]actionFn{
		"PutCodeBinding": func(ctx context.Context, b []byte) (any, error) {
			var input PutCodeBindingInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.PutCodeBinding(ctx, input)
		},
		"DescribeCodeBinding": func(ctx context.Context, b []byte) (any, error) {
			var input DescribeCodeBindingInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeCodeBinding(ctx, input)
		},
		"ListCodeBindings": func(ctx context.Context, b []byte) (any, error) {
			var input ListCodeBindingsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			bindings, next, err := h.Backend.ListCodeBindings(ctx, input)
			if err != nil {
				return nil, err
			}

			return &struct {
				NextToken    string        `json:"NextToken,omitempty"`
				CodeBindings []CodeBinding `json:"CodeBindings"`
			}{CodeBindings: bindings, NextToken: next}, nil
		},
		"GetCodeBindingSource": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				RegistryName  string `json:"RegistryName"`
				SchemaName    string `json:"SchemaName"`
				Language      string `json:"Language"`
				SchemaVersion string `json:"SchemaVersion"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			src, err := h.Backend.GetCodeBindingSource(
				ctx,
				input.RegistryName, input.SchemaName, input.Language, input.SchemaVersion,
			)
			if err != nil {
				return nil, err
			}

			return &struct {
				Body string `json:"Body"`
			}{Body: src}, nil
		},
	}
}

func (h *Handler) newOpsActions() map[string]actionFn {
	table := make(map[string]actionFn)
	maps.Copy(table, h.eventSourceActions())
	maps.Copy(table, h.replayAndConnectionActions())
	maps.Copy(table, h.apiDestinationAndArchiveActions())
	maps.Copy(table, h.extendedArchiveActions())
	maps.Copy(table, h.extendedConnectionActions())
	maps.Copy(table, h.extendedEndpointActions())
	maps.Copy(table, h.extendedAPIDestinationActions())
	maps.Copy(table, h.extendedEventSourceActions())
	maps.Copy(table, h.extendedPartnerSourceActions())
	maps.Copy(table, h.extendedReplayActions())
	maps.Copy(table, h.extendedMiscActions())
	maps.Copy(table, h.policyActions())
	maps.Copy(table, h.pipesActions())
	maps.Copy(table, h.schemaRegistryActions())

	return table
}

func (h *Handler) buildOps() map[string]actionFn {
	table := make(map[string]actionFn)
	maps.Copy(table, h.eventBusActions())
	maps.Copy(table, h.ruleActions())
	maps.Copy(table, h.ruleStateActions())
	maps.Copy(table, h.targetActions())
	maps.Copy(table, h.eventsActions())
	maps.Copy(table, h.tagActions())
	maps.Copy(table, h.newOpsActions())

	return table
}

// dispatch routes the action to the correct handler function.
func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.ops[action]
	if !ok {
		return nil, fmt.Errorf("%w:%s", errUnknownOperation, action)
	}

	response, err := fn(ctx, body)
	if err != nil {
		return nil, err
	}

	return json.Marshal(response)
}

// handleError writes a standardized JSON error response.
func (h *Handler) handleError(
	ctx context.Context,
	c *echo.Context,
	action string,
	reqErr error,
) error {
	log := logger.Load(ctx)
	c.Response().Header().Set("Content-Type", "application/x-amz-json-1.1")

	var errType string
	var statusCode int

	switch {
	case errors.Is(reqErr, ErrEventBusNotFound),
		errors.Is(reqErr, ErrRuleNotFound),
		errors.Is(reqErr, ErrNotFound):
		errType = "ResourceNotFoundException"
		statusCode = http.StatusNotFound
	case errors.Is(reqErr, ErrEventBusAlreadyExists), errors.Is(reqErr, ErrAlreadyExists):
		errType = "ResourceAlreadyExistsException"
		statusCode = http.StatusConflict
	case errors.Is(reqErr, ErrCannotDeleteDefaultBus):
		errType = "IllegalStatusException"
		statusCode = http.StatusBadRequest
	case errors.Is(reqErr, ErrInvalidParameter):
		errType = "InvalidParameterException"
		statusCode = http.StatusBadRequest
	case errors.Is(reqErr, ErrInvalidState):
		errType = "InvalidStateException"
		statusCode = http.StatusBadRequest
	case errors.Is(reqErr, ErrResourceLimitExceeded):
		errType = "ResourceLimitExceededException"
		statusCode = http.StatusBadRequest
	case errors.Is(reqErr, ErrForbiddenOperation):
		errType = "ForbiddenException"
		statusCode = http.StatusForbidden
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

// Reset clears all in-memory state from the backend. It is used by the
// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
func (h *Handler) Reset() {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		b.Reset()
	}

	// Clear handler-level tag state so that tags don't bleed across test runs.
	h.tagsMu.Lock("Reset")
	defer h.tagsMu.Unlock()

	for _, t := range h.tags {
		t.Close()
	}

	h.tags = make(map[string]*svcTags.Tags)
}
