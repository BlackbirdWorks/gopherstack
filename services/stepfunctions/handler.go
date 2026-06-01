package stepfunctions

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
	"github.com/blackbirdworks/gopherstack/services/stepfunctions/asl"
)

var errUnknownOperation = errors.New("UnknownOperationException")

type createStateMachineInput struct {
	TracingConfiguration    *TracingConfiguration    `json:"tracingConfiguration,omitempty"`
	LoggingConfiguration    *LoggingConfiguration    `json:"loggingConfiguration,omitempty"`
	EncryptionConfiguration *EncryptionConfiguration `json:"encryptionConfiguration,omitempty"`
	Name                    string                   `json:"name"`
	Definition              string                   `json:"definition"`
	RoleArn                 string                   `json:"roleArn"`
	Type                    string                   `json:"type"`
	Tags                    []sfnTagEntry            `json:"tags,omitempty"`
	Publish                 bool                     `json:"publish,omitempty"`
}

type deleteStateMachineInput struct {
	StateMachineArn string `json:"stateMachineArn"`
}

type updateStateMachineInput struct {
	TracingConfiguration    *TracingConfiguration    `json:"tracingConfiguration,omitempty"`
	LoggingConfiguration    *LoggingConfiguration    `json:"loggingConfiguration,omitempty"`
	EncryptionConfiguration *EncryptionConfiguration `json:"encryptionConfiguration,omitempty"`
	StateMachineArn         string                   `json:"stateMachineArn"`
	Definition              string                   `json:"definition"`
	RoleArn                 string                   `json:"roleArn"`
	Publish                 bool                     `json:"publish,omitempty"`
}

type listStateMachinesInput struct {
	NextToken  string `json:"nextToken"`
	MaxResults int    `json:"maxResults"`
}

type describeStateMachineInput struct {
	StateMachineArn string `json:"stateMachineArn"`
}

type sfnListTagsForResourceInput struct {
	ResourceArn string `json:"resourceArn"`
}

type sfnTagResourceInput struct {
	Tags        *tags.Tags `json:"tags"`
	ResourceArn string     `json:"resourceArn"`
}

type sfnUntagResourceInput struct {
	ResourceArn string   `json:"resourceArn"`
	TagKeys     []string `json:"tagKeys"`
}

type startExecutionInput struct {
	StateMachineArn string `json:"stateMachineArn"`
	Name            string `json:"name"`
	Input           string `json:"input"`
}

type startSyncExecutionInput struct {
	StateMachineArn string `json:"stateMachineArn"`
	Name            string `json:"name"`
	Input           string `json:"input"`
}

type stopExecutionInput struct {
	ExecutionArn string `json:"executionArn"`
	Error        string `json:"error"`
	Cause        string `json:"cause"`
}

type describeExecutionInput struct {
	ExecutionArn string `json:"executionArn"`
}

type listExecutionsInput struct {
	StateMachineArn string `json:"stateMachineArn"`
	StatusFilter    string `json:"statusFilter"`
	NextToken       string `json:"nextToken"`
	MaxResults      int    `json:"maxResults"`
}

type getExecutionHistoryInput struct {
	ExecutionArn string `json:"executionArn"`
	NextToken    string `json:"nextToken"`
	MaxResults   int    `json:"maxResults"`
	ReverseOrder bool   `json:"reverseOrder"`
}

// Handler is the Echo HTTP service handler for Step Functions operations.
type Handler struct {
	Backend StorageBackend
	tags    map[string]*tags.Tags
	tagsMu  *lockmetrics.RWMutex
}

// NewHandler creates a new Step Functions handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{
		Backend: backend,
		tags:    make(map[string]*tags.Tags),
		tagsMu:  lockmetrics.New("sfn.tags"),
	}
}

func (h *Handler) setTags(resourceID string, kv map[string]string) {
	h.tagsMu.Lock("setTags")
	defer h.tagsMu.Unlock()
	if h.tags[resourceID] == nil {
		h.tags[resourceID] = tags.New("sfn." + resourceID + ".tags")
	}
	h.tags[resourceID].Merge(kv)
}

func (h *Handler) removeTags(resourceID string, keys []string) {
	h.tagsMu.RLock("removeTags")
	defer h.tagsMu.RUnlock()

	t := h.tags[resourceID]
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

// Name returns the service name.
func (h *Handler) Name() string { return "StepFunctions" }

// StartWorker starts the background janitor for execution pruning.
// It implements service.BackgroundWorker.
func (h *Handler) StartWorker(ctx context.Context) error {
	if sfnBk, ok := h.Backend.(*InMemoryBackend); ok {
		janitor := NewJanitor(sfnBk, sfnBk.settings)
		go janitor.Run(ctx)
	}

	return nil
}

// GetSupportedOperations returns all mocked Step Functions operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateActivity",
		"CreateStateMachine",
		"CreateStateMachineAlias",
		"DeleteActivity",
		"DeleteStateMachine",
		"DeleteStateMachineAlias",
		"DeleteStateMachineVersion",
		"DescribeActivity",
		"DescribeExecution",
		"DescribeMapRun",
		"DescribeStateMachine",
		"DescribeStateMachineAlias",
		"DescribeStateMachineForExecution",
		"DescribeStateMachineVersion",
		"GetActivityTask",
		"GetExecutionHistory",
		"ListActivities",
		"ListExecutions",
		"ListMapRuns",
		"ListStateMachineAliases",
		"ListStateMachineVersions",
		"ListStateMachines",
		"ListTagsForResource",
		"PublishStateMachineVersion",
		"RedriveExecution",
		"SendTaskFailure",
		"SendTaskHeartbeat",
		"SendTaskSuccess",
		"StartExecution",
		"StartSyncExecution",
		"StopExecution",
		"TagResource",
		"TestState",
		"UntagResource",
		"UpdateMapRun",
		"UpdateStateMachine",
		"UpdateStateMachineAlias",
		"ValidateStateMachineDefinition",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "states" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this Step Functions instance handles.
func (h *Handler) ChaosRegions() []string {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		if r := b.Region(); r != "" {
			return []string{r}
		}
	}

	return []string{config.DefaultRegion}
}

// RouteMatcher returns a matcher for Step Functions requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		target := c.Request().Header.Get("X-Amz-Target")

		return strings.HasPrefix(target, "AmazonStates.") || strings.HasPrefix(target, "AWSStepFunctions.")
	}
}

const stepFunctionsMatchPriority = 100

// MatchPriority returns the routing priority for the Step Functions handler.
func (h *Handler) MatchPriority() int { return stepFunctionsMatchPriority }

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

	for _, key := range []string{"name", "stateMachineArn", "executionArn"} {
		if v, ok := data[key].(string); ok && v != "" {
			return v
		}
	}

	return ""
}

// Handler returns the Echo handler function for Step Functions requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"StepFunctions", "application/x-amz-json-1.0",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

type actionFn func([]byte) (any, error)

type createStateMachineOutput struct {
	StateMachineArn string  `json:"stateMachineArn"`
	CreationDate    float64 `json:"creationDate"`
}

type deleteStateMachineOutput struct{}

type sfnTagEntry struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type listTagsForResourceOutput struct {
	Tags []sfnTagEntry `json:"tags"`
}

type tagResourceOutput struct{}

type untagResourceOutput struct{}

type listStateMachinesOutput struct {
	NextToken     string         `json:"nextToken"`
	StateMachines []StateMachine `json:"stateMachines"`
}

type updateStateMachineOutput struct {
	UpdateDate float64 `json:"updateDate"`
}

type startExecutionOutput struct {
	ExecutionArn string  `json:"executionArn"`
	StartDate    float64 `json:"startDate"`
}

type stopExecutionOutput struct {
	StopDate *float64 `json:"stopDate"`
}

type listExecutionsOutput struct {
	NextToken  string      `json:"nextToken"`
	Executions []Execution `json:"executions"`
}

type getExecutionHistoryOutput struct {
	NextToken string         `json:"nextToken"`
	Events    []HistoryEvent `json:"events"`
}

type validateStateMachineDefinitionOutput struct {
	Result      string `json:"result"`
	Diagnostics []any  `json:"diagnostics"`
}

type createActivityInput struct {
	Name string `json:"name"`
}

type createActivityOutput struct {
	ActivityArn  string  `json:"activityArn"`
	CreationDate float64 `json:"creationDate"`
}

type deleteActivityInput struct {
	ActivityArn string `json:"activityArn"`
}

type deleteActivityOutput struct{}

type describeActivityInput struct {
	ActivityArn string `json:"activityArn"`
}

type listActivitiesInput struct {
	NextToken  string `json:"nextToken"`
	MaxResults int    `json:"maxResults"`
}

type listActivitiesOutput struct {
	NextToken  string     `json:"nextToken"`
	Activities []Activity `json:"activities"`
}

type getActivityTaskInput struct {
	ActivityArn string `json:"activityArn"`
	WorkerName  string `json:"workerName"`
}

type getActivityTaskOutput struct {
	TaskToken string `json:"taskToken"`
	Input     string `json:"input"`
}

type sendTaskSuccessInput struct {
	TaskToken string `json:"taskToken"`
	Output    string `json:"output"`
}

type sendTaskSuccessOutput struct{}

type sendTaskFailureInput struct {
	TaskToken string `json:"taskToken"`
	Error     string `json:"error"`
	Cause     string `json:"cause"`
}

type sendTaskFailureOutput struct{}

type sendTaskHeartbeatInput struct {
	TaskToken string `json:"taskToken"`
}

type sendTaskHeartbeatOutput struct{}

// ── Version / Alias request/response types ────────────────────────────────────

type publishStateMachineVersionInput struct {
	StateMachineArn string `json:"stateMachineArn"`
	Description     string `json:"description"`
	RevisionID      string `json:"revisionId"`
}

type describeStateMachineVersionInput struct {
	StateMachineVersionArn string `json:"stateMachineVersionArn"`
}

type deleteStateMachineVersionInput struct {
	StateMachineVersionArn string `json:"stateMachineVersionArn"`
}

type listStateMachineVersionsInput struct {
	StateMachineArn string `json:"stateMachineArn"`
	NextToken       string `json:"nextToken"`
	MaxResults      int    `json:"maxResults"`
}

type listStateMachineVersionsOutput struct {
	NextToken            string                `json:"nextToken,omitempty"`
	StateMachineVersions []StateMachineVersion `json:"stateMachineVersions"`
}

type createStateMachineAliasInput struct {
	Name                 string               `json:"name"`
	StateMachineArn      string               `json:"stateMachineArn"`
	Description          string               `json:"description"`
	RoutingConfiguration []AliasRoutingConfig `json:"routingConfiguration"`
}

type updateStateMachineAliasInput struct {
	StateMachineAliasArn string               `json:"stateMachineAliasArn"`
	Description          string               `json:"description"`
	RoutingConfiguration []AliasRoutingConfig `json:"routingConfiguration"`
}

type deleteStateMachineAliasInput struct {
	StateMachineAliasArn string `json:"stateMachineAliasArn"`
}

type describeStateMachineAliasInput struct {
	StateMachineAliasArn string `json:"stateMachineAliasArn"`
}

type listStateMachineAliasesInput struct {
	StateMachineArn string `json:"stateMachineArn"`
	NextToken       string `json:"nextToken"`
	MaxResults      int    `json:"maxResults"`
}

type listStateMachineAliasesOutput struct {
	NextToken           string              `json:"nextToken,omitempty"`
	StateMachineAliases []StateMachineAlias `json:"stateMachineAliases"`
}

// ── RedriveExecution / DescribeStateMachineForExecution ───────────────────────

type redriveExecutionInput struct {
	ExecutionArn string `json:"executionArn"`
}

type redriveExecutionOutput struct {
	RedriveDate float64 `json:"redriveDate"`
}

type describeStateMachineForExecutionInput struct {
	ExecutionArn string `json:"executionArn"`
}

// createStateMachineAction handles CreateStateMachine and applies tracing/logging/encryption
// configuration and inline tags when supplied in the request body.
func (h *Handler) createStateMachineAction(b []byte) (any, error) {
	var input createStateMachineInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	sm, err := h.Backend.CreateStateMachine(input.Name, input.Definition, input.RoleArn, input.Type)
	if err != nil {
		return nil, err
	}

	if input.TracingConfiguration != nil || input.LoggingConfiguration != nil || input.EncryptionConfiguration != nil {
		if cfgErr := h.Backend.SetStateMachineConfigurations(
			sm.StateMachineArn, input.TracingConfiguration, input.LoggingConfiguration, input.EncryptionConfiguration,
		); cfgErr != nil {
			return nil, cfgErr
		}
	}

	// Apply inline tags when provided.
	if len(input.Tags) > 0 {
		kv := make(map[string]string, len(input.Tags))
		for _, t := range input.Tags {
			kv[t.Key] = t.Value
		}
		h.setTags(sm.StateMachineArn, kv)
	}

	// When publish=true, immediately publish a version of the new state machine.
	if input.Publish {
		_, _ = h.Backend.PublishStateMachineVersion(sm.StateMachineArn, "", "")
	}

	return &createStateMachineOutput{
		StateMachineArn: sm.StateMachineArn,
		CreationDate:    sm.CreationDate,
	}, nil
}

// updateStateMachineAction handles UpdateStateMachine and applies tracing/logging/encryption
// configuration when supplied in the request body.
func (h *Handler) updateStateMachineAction(b []byte) (any, error) {
	var input updateStateMachineInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	updateDate, err := h.Backend.UpdateStateMachine(input.StateMachineArn, input.Definition, input.RoleArn)
	if err != nil {
		return nil, err
	}

	if input.TracingConfiguration != nil || input.LoggingConfiguration != nil || input.EncryptionConfiguration != nil {
		if cfgErr := h.Backend.SetStateMachineConfigurations(
			input.StateMachineArn,
			input.TracingConfiguration,
			input.LoggingConfiguration,
			input.EncryptionConfiguration,
		); cfgErr != nil {
			return nil, cfgErr
		}
	}

	// When publish=true, immediately publish a version of the updated state machine.
	if input.Publish {
		_, _ = h.Backend.PublishStateMachineVersion(input.StateMachineArn, "", "")
	}

	return &updateStateMachineOutput{UpdateDate: updateDate}, nil
}

func (h *Handler) stateMachineActions() map[string]actionFn {
	m := map[string]actionFn{
		"CreateStateMachine": h.createStateMachineAction,
		"DeleteStateMachine": func(b []byte) (any, error) {
			var input deleteStateMachineInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			if err := h.Backend.DeleteStateMachine(input.StateMachineArn); err != nil {
				return nil, err
			}

			// Clean up tags for the deleted state machine.
			h.tagsMu.Lock("DeleteStateMachine")
			if t, ok := h.tags[input.StateMachineArn]; ok {
				t.Close()
				delete(h.tags, input.StateMachineArn)
			}
			h.tagsMu.Unlock()

			return &deleteStateMachineOutput{}, nil
		},
		"ListStateMachines": func(b []byte) (any, error) {
			var input listStateMachinesInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			sms, next, err := h.Backend.ListStateMachines(input.NextToken, input.MaxResults)
			if err != nil {
				return nil, err
			}

			return &listStateMachinesOutput{StateMachines: sms, NextToken: next}, nil
		},
		"DescribeStateMachine": func(b []byte) (any, error) {
			var input describeStateMachineInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeStateMachine(input.StateMachineArn)
		},
		"UpdateStateMachine": h.updateStateMachineAction,
	}
	maps.Copy(m, h.stateMachineTagActions())
	maps.Copy(m, h.versionActions())
	maps.Copy(m, h.aliasActions())

	return m
}

// versionActions returns handler functions for state machine version operations.
func (h *Handler) versionActions() map[string]actionFn {
	return map[string]actionFn{
		"PublishStateMachineVersion": func(b []byte) (any, error) {
			var input publishStateMachineVersionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.PublishStateMachineVersion(input.StateMachineArn, input.Description, input.RevisionID)
		},
		"DescribeStateMachineVersion": func(b []byte) (any, error) {
			var input describeStateMachineVersionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeStateMachineVersion(input.StateMachineVersionArn)
		},
		"DeleteStateMachineVersion": func(b []byte) (any, error) {
			var input deleteStateMachineVersionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			if err := h.Backend.DeleteStateMachineVersion(input.StateMachineVersionArn); err != nil {
				return nil, err
			}

			return map[string]any{}, nil
		},
		"ListStateMachineVersions": func(b []byte) (any, error) {
			var input listStateMachineVersionsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			versions, next, err := h.Backend.ListStateMachineVersions(
				input.StateMachineArn, input.NextToken, input.MaxResults,
			)
			if err != nil {
				return nil, err
			}

			return &listStateMachineVersionsOutput{StateMachineVersions: versions, NextToken: next}, nil
		},
	}
}

// aliasActions returns handler functions for state machine alias operations.
func (h *Handler) aliasActions() map[string]actionFn {
	return map[string]actionFn{
		"CreateStateMachineAlias": func(b []byte) (any, error) {
			var input createStateMachineAliasInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.CreateStateMachineAlias(
				input.StateMachineArn, input.Name, input.Description, input.RoutingConfiguration,
			)
		},
		"UpdateStateMachineAlias": func(b []byte) (any, error) {
			var input updateStateMachineAliasInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.UpdateStateMachineAlias(
				input.StateMachineAliasArn, input.Description, input.RoutingConfiguration,
			)
		},
		"DeleteStateMachineAlias": func(b []byte) (any, error) {
			var input deleteStateMachineAliasInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			if err := h.Backend.DeleteStateMachineAlias(input.StateMachineAliasArn); err != nil {
				return nil, err
			}

			return map[string]any{}, nil
		},
		"DescribeStateMachineAlias": func(b []byte) (any, error) {
			var input describeStateMachineAliasInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeStateMachineAlias(input.StateMachineAliasArn)
		},
		"ListStateMachineAliases": func(b []byte) (any, error) {
			var input listStateMachineAliasesInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			aliases, next, err := h.Backend.ListStateMachineAliases(
				input.StateMachineArn, input.NextToken, input.MaxResults,
			)
			if err != nil {
				return nil, err
			}

			return &listStateMachineAliasesOutput{StateMachineAliases: aliases, NextToken: next}, nil
		},
	}
}

const (
	maxTagKeyLen       = 128
	maxTagValueLen     = 256
	maxTagsPerResource = 50
)

// validateTags checks AWS tag constraints: key 1-128 chars, value 0-256 chars,
// and that adding newTags will not push the resource over 50 total tags.
func validateTags(existing, newTags map[string]string) error {
	for k, v := range newTags {
		if len(k) == 0 || len(k) > maxTagKeyLen {
			return fmt.Errorf("%w: tag key must be 1-%d characters", ErrTagPolicyViolation, maxTagKeyLen)
		}

		if len(v) > maxTagValueLen {
			return fmt.Errorf("%w: tag value must be 0-%d characters", ErrTagPolicyViolation, maxTagValueLen)
		}
	}

	merged := len(existing)
	for k := range newTags {
		if _, exists := existing[k]; !exists {
			merged++
		}
	}

	if merged > maxTagsPerResource {
		return fmt.Errorf("%w: resource cannot have more than %d tags", ErrTagPolicyViolation, maxTagsPerResource)
	}

	return nil
}

// stateMachineTagActions returns tag-related actions for state machines.
func (h *Handler) stateMachineTagActions() map[string]actionFn {
	return map[string]actionFn{
		"ListTagsForResource": func(b []byte) (any, error) {
			var input sfnListTagsForResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			tagMap := h.getTags(input.ResourceArn)
			tagList := make([]sfnTagEntry, 0, len(tagMap))
			for k, v := range tagMap {
				tagList = append(tagList, sfnTagEntry{Key: k, Value: v})
			}

			return &listTagsForResourceOutput{Tags: tagList}, nil
		},
		"TagResource": func(b []byte) (any, error) {
			var input sfnTagResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			var kv map[string]string
			if input.Tags != nil {
				kv = input.Tags.Clone()
			}

			existing := h.getTags(input.ResourceArn)
			if err := validateTags(existing, kv); err != nil {
				return nil, err
			}

			h.setTags(input.ResourceArn, kv)

			return &tagResourceOutput{}, nil
		},
		"UntagResource": func(b []byte) (any, error) {
			var input sfnUntagResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			h.removeTags(input.ResourceArn, input.TagKeys)

			return &untagResourceOutput{}, nil
		},
	}
}

func (h *Handler) executionActions() map[string]actionFn {
	return map[string]actionFn{
		"StartExecution":                   h.handleStartExecution,
		"StartSyncExecution":               h.handleStartSyncExecution,
		"StopExecution":                    h.handleStopExecution,
		"RedriveExecution":                 h.handleRedriveExecution,
		"DescribeExecution":                h.handleDescribeExecution,
		"DescribeStateMachineForExecution": h.handleDescribeStateMachineForExecution,
		"ListExecutions":                   h.handleListExecutions,
		"GetExecutionHistory":              h.handleGetExecutionHistory,
	}
}

func (h *Handler) handleRedriveExecution(b []byte) (any, error) {
	var input redriveExecutionInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	exec, err := h.Backend.RedriveExecution(input.ExecutionArn)
	if err != nil {
		return nil, err
	}

	return &redriveExecutionOutput{RedriveDate: exec.StartDate}, nil
}

func (h *Handler) handleDescribeStateMachineForExecution(b []byte) (any, error) {
	var input describeStateMachineForExecutionInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	return h.Backend.DescribeStateMachineForExecution(input.ExecutionArn)
}

func (h *Handler) handleStartExecution(b []byte) (any, error) {
	var input startExecutionInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	exec, err := h.Backend.StartExecution(input.StateMachineArn, input.Name, input.Input)
	if err != nil {
		return nil, err
	}

	return &startExecutionOutput{ExecutionArn: exec.ExecutionArn, StartDate: exec.StartDate}, nil
}

func (h *Handler) handleStartSyncExecution(b []byte) (any, error) {
	var input startSyncExecutionInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	return h.Backend.StartSyncExecution(input.StateMachineArn, input.Name, input.Input)
}

func (h *Handler) handleStopExecution(b []byte) (any, error) {
	var input stopExecutionInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	if err := h.Backend.StopExecution(input.ExecutionArn, input.Error, input.Cause); err != nil {
		return nil, err
	}

	exec, err := h.Backend.DescribeExecution(input.ExecutionArn)
	if err != nil {
		return nil, err
	}

	return &stopExecutionOutput{StopDate: exec.StopDate}, nil
}

func (h *Handler) handleDescribeExecution(b []byte) (any, error) {
	var input describeExecutionInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	return h.Backend.DescribeExecution(input.ExecutionArn)
}

func (h *Handler) handleListExecutions(b []byte) (any, error) {
	var input listExecutionsInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	execs, next, err := h.Backend.ListExecutions(
		input.StateMachineArn, input.StatusFilter, input.NextToken, input.MaxResults,
	)
	if err != nil {
		return nil, err
	}

	return &listExecutionsOutput{Executions: execs, NextToken: next}, nil
}

func (h *Handler) handleGetExecutionHistory(b []byte) (any, error) {
	var input getExecutionHistoryInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	events, next, err := h.Backend.GetExecutionHistory(
		input.ExecutionArn, input.NextToken, input.MaxResults, input.ReverseOrder,
	)
	if err != nil {
		return nil, err
	}

	return &getExecutionHistoryOutput{Events: events, NextToken: next}, nil
}

func (h *Handler) dispatchTable() map[string]actionFn {
	table := make(map[string]actionFn)
	maps.Copy(table, h.stateMachineActions())
	maps.Copy(table, h.executionActions())
	maps.Copy(table, h.activityActions())
	maps.Copy(table, h.utilActions())
	maps.Copy(table, h.mapRunActions())

	return table
}

// activityActions returns handler functions for activity-related operations.
func (h *Handler) activityActions() map[string]actionFn {
	return map[string]actionFn{
		"CreateActivity":    h.handleCreateActivity,
		"DeleteActivity":    h.handleDeleteActivity,
		"DescribeActivity":  h.handleDescribeActivity,
		"ListActivities":    h.handleListActivities,
		"SendTaskSuccess":   h.handleSendTaskSuccess,
		"SendTaskFailure":   h.handleSendTaskFailure,
		"SendTaskHeartbeat": h.handleSendTaskHeartbeat,
	}
}

func (h *Handler) handleCreateActivity(b []byte) (any, error) {
	var input createActivityInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	a, err := h.Backend.CreateActivity(input.Name)
	if err != nil {
		return nil, err
	}

	return &createActivityOutput{ActivityArn: a.ActivityArn, CreationDate: a.CreationDate}, nil
}

func (h *Handler) handleDeleteActivity(b []byte) (any, error) {
	var input deleteActivityInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteActivity(input.ActivityArn); err != nil {
		return nil, err
	}

	return &deleteActivityOutput{}, nil
}

func (h *Handler) handleDescribeActivity(b []byte) (any, error) {
	var input describeActivityInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	return h.Backend.DescribeActivity(input.ActivityArn)
}

func (h *Handler) handleListActivities(b []byte) (any, error) {
	var input listActivitiesInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	acts, next, err := h.Backend.ListActivities(input.NextToken, input.MaxResults)
	if err != nil {
		return nil, err
	}

	return &listActivitiesOutput{Activities: acts, NextToken: next}, nil
}

func (h *Handler) handleSendTaskSuccess(b []byte) (any, error) {
	var input sendTaskSuccessInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	if err := h.Backend.SendTaskSuccess(input.TaskToken, input.Output); err != nil {
		return nil, err
	}

	return &sendTaskSuccessOutput{}, nil
}

func (h *Handler) handleSendTaskFailure(b []byte) (any, error) {
	var input sendTaskFailureInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	if err := h.Backend.SendTaskFailure(input.TaskToken, input.Error, input.Cause); err != nil {
		return nil, err
	}

	return &sendTaskFailureOutput{}, nil
}

func (h *Handler) handleSendTaskHeartbeat(b []byte) (any, error) {
	var input sendTaskHeartbeatInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	if err := h.Backend.SendTaskHeartbeat(input.TaskToken); err != nil {
		return nil, err
	}

	return &sendTaskHeartbeatOutput{}, nil
}

type validateStateMachineDefinitionInput struct {
	Definition string `json:"definition"`
}

// mapRunActions returns handler functions for Map Run operations.
func (h *Handler) mapRunActions() map[string]actionFn {
	return map[string]actionFn{
		"DescribeMapRun": func(_ []byte) (any, error) {
			// DescribeMapRun describes a Map Run. In-process simulation returns a stub.
			return map[string]any{"MapRunArn": "", "ExecutionArn": "", "Status": "SUCCEEDED"}, nil
		},
		"ListMapRuns": func(_ []byte) (any, error) {
			// ListMapRuns lists Map Runs for an execution. In-process simulation returns empty list.
			return map[string]any{"MapRuns": []any{}}, nil
		},
		"TestState": func(body []byte) (any, error) {
			return h.handleTestState(body)
		},
		"UpdateMapRun": func(_ []byte) (any, error) {
			// UpdateMapRun updates a Map Run's concurrency. In-process simulation is a no-op.
			return map[string]any{}, nil
		},
	}
}

// utilActions returns utility operations like definition validation.
func (h *Handler) utilActions() map[string]actionFn {
	return map[string]actionFn{
		"ValidateStateMachineDefinition": func(b []byte) (any, error) {
			var input validateStateMachineDefinitionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			if _, err := asl.Parse(input.Definition); err != nil {
				//nolint:nilerr // parse error is returned as Result:FAIL in the response body
				return &validateStateMachineDefinitionOutput{
					Result: "FAIL",
					Diagnostics: []any{map[string]string{
						"message": err.Error(),
						"code":    "SCHEMA_VALIDATION_FAILED",
					}},
				}, nil
			}

			return &validateStateMachineDefinitionOutput{Result: "OK", Diagnostics: []any{}}, nil
		},
	}
}

// dispatch routes the action to the correct handler function.
func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	// GetActivityTask is context-aware (long-poll up to 60 seconds).
	if action == "GetActivityTask" {
		return h.handleGetActivityTask(ctx, body)
	}

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

// handleGetActivityTask handles GetActivityTask with context support for long-polling.
func (h *Handler) handleGetActivityTask(ctx context.Context, body []byte) ([]byte, error) {
	var input getActivityTaskInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, err
	}

	task, err := h.Backend.GetActivityTask(ctx, input.ActivityArn, input.WorkerName)
	if err != nil {
		return nil, err
	}

	if task == nil {
		return json.Marshal(&getActivityTaskOutput{})
	}

	return json.Marshal(&getActivityTaskOutput{
		TaskToken: task.TaskToken,
		Input:     task.Input,
	})
}

type testStateInput struct {
	Definition string `json:"definition"`
	Input      string `json:"input"`
	RoleArn    string `json:"roleArn,omitempty"`
}

type testStateOutput struct {
	Output string `json:"output,omitempty"`
	Error  string `json:"error,omitempty"`
	Cause  string `json:"cause,omitempty"`
	Status string `json:"status"`
}

// handleTestState executes a single state definition in isolation and returns its output.
// The definition is a JSON object mapping a single state name to its state definition.
func (h *Handler) handleTestState(body []byte) (any, error) {
	var input testStateInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, err
	}

	// Wrap the state definition in a minimal state machine.
	var states map[string]json.RawMessage
	if err := json.Unmarshal([]byte(input.Definition), &states); err != nil {
		return nil, fmt.Errorf("%w: invalid state definition JSON: %w", ErrInvalidDefinition, err)
	}

	if len(states) != 1 {
		return nil, fmt.Errorf("%w: TestState definition must contain exactly one state", ErrInvalidDefinition)
	}

	var stateName string

	for k := range states {
		stateName = k
	}

	smDef := fmt.Sprintf(`{"StartAt":%q,"States":%s}`, stateName, input.Definition)

	sm, err := asl.Parse(smDef)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidDefinition, err)
	}

	var lambdaInvoker asl.LambdaInvoker

	if bk, ok := h.Backend.(*InMemoryBackend); ok {
		bk.mu.RLock("TestState")
		lambdaInvoker = bk.lambdaInvoker
		bk.mu.RUnlock()
	}

	executor := asl.NewExecutor(sm, lambdaInvoker, nil)

	stateInput := input.Input
	if stateInput == "" {
		stateInput = "{}"
	}

	result, execErr := executor.Execute(context.Background(), "test-state", stateInput)
	if execErr != nil {
		out := &testStateOutput{Status: "FAILED", Error: execErr.Error()}

		return out, nil //nolint:nilerr // execution errors are encoded in the response body
	}

	if result.Error != "" {
		return &testStateOutput{Status: "FAILED", Error: result.Error, Cause: result.Cause}, nil
	}

	outputBytes, _ := json.Marshal(result.Output)

	return &testStateOutput{Status: "SUCCEEDED", Output: string(outputBytes)}, nil
}

// handleError writes a standardized JSON error response.
func (h *Handler) handleError(ctx context.Context, c *echo.Context, action string, reqErr error) error {
	log := logger.Load(ctx)
	c.Response().Header().Set("Content-Type", "application/x-amz-json-1.0")

	errType, statusCode := classifyError(reqErr)

	if statusCode == http.StatusInternalServerError {
		log.ErrorContext(ctx, "StepFunctions internal error", "error", reqErr, "action", action)
	} else {
		log.WarnContext(ctx, "StepFunctions request error", "error", reqErr, "action", action)
	}

	errResp := service.JSONErrorResponse{
		Type:    errType,
		Message: reqErr.Error(),
	}

	payload, _ := json.Marshal(errResp)

	return c.JSONBlob(statusCode, payload)
}

func classifyError(reqErr error) (string, int) {
	type mapping struct {
		err    error
		kind   string
		status int
	}

	mappings := []mapping{
		{ErrStateMachineDoesNotExist, "StateMachineDoesNotExist", http.StatusNotFound},
		{ErrStateMachineVersionDoesNotExist, "StateMachineVersionDoesNotExist", http.StatusNotFound},
		{ErrStateMachineAliasDoesNotExist, "StateMachineAliasDoesNotExist", http.StatusNotFound},
		{ErrExecutionDoesNotExist, "ExecutionDoesNotExist", http.StatusNotFound},
		{ErrActivityDoesNotExist, "ActivityDoesNotExist", http.StatusNotFound},
		{ErrTaskTokenNotFound, "TaskDoesNotExist", http.StatusNotFound},
		{ErrStateMachineAlreadyExists, "StateMachineAlreadyExists", http.StatusConflict},
		{ErrStateMachineAliasAlreadyExists, "StateMachineAliasAlreadyExists", http.StatusConflict},
		{ErrExecutionAlreadyExists, "ExecutionAlreadyExists", http.StatusConflict},
		{ErrActivityAlreadyExists, "ActivityAlreadyExists", http.StatusConflict},
		{ErrExecutionNotRedrivable, "ExecutionNotRedrivable", http.StatusBadRequest},
		{ErrInvalidDefinition, "InvalidDefinition", http.StatusBadRequest},
		{ErrInvalidExecutionType, "InvalidExecutionType", http.StatusBadRequest},
		{ErrInvalidExecutionInput, "InvalidExecutionInput", http.StatusBadRequest},
		{ErrInvalidName, "InvalidName", http.StatusBadRequest},
		{ErrInvalidRoleArn, "InvalidArn", http.StatusBadRequest},
		{ErrInvalidRoutingConfiguration, "InvalidRoutingConfiguration", http.StatusBadRequest},
		{ErrTagPolicyViolation, "TagPolicyViolation", http.StatusBadRequest},
		{ErrTaskTokenAlreadyExists, "TaskTokenAlreadyExists", http.StatusBadRequest},
		{errUnknownOperation, "UnknownOperationException", http.StatusBadRequest},
	}

	for _, m := range mappings {
		if errors.Is(reqErr, m.err) {
			return m.kind, m.status
		}
	}

	return "InternalServerError", http.StatusInternalServerError
}

// Reset clears all in-memory state from the backend. It is used by the
// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
func (h *Handler) Reset() {
	// Close and clear all Tags objects to avoid lockmetrics leaks.
	h.tagsMu.Lock("Reset")
	for _, t := range h.tags {
		t.Close()
	}
	h.tags = make(map[string]*tags.Tags)
	h.tagsMu.Unlock()

	if b, ok := h.Backend.(*InMemoryBackend); ok {
		b.Reset()
	}
}

// Shutdown implements service.Shutdowner.
// It cancels all running execution goroutines and releases associated resources.
// Destroy() calls each cancel func synchronously and returns immediately; the
// ASL goroutines exit asynchronously. If ctx expires before Destroy returns,
// Shutdown returns early so the process shutdown is not blocked.
func (h *Handler) Shutdown(ctx context.Context) {
	type destroyer interface{ Destroy() }

	b, ok := h.Backend.(destroyer)
	if !ok {
		return
	}

	done := make(chan struct{})

	go func() {
		b.Destroy()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
	}
}

// Ensure Handler implements service.Shutdowner at compile time.
var _ service.Shutdowner = (*Handler)(nil)
