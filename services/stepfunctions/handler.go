package stepfunctions

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
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
	"github.com/blackbirdworks/gopherstack/services/stepfunctions/asl"
)

var errUnknownOperation = errors.New("UnknownOperationException")

type createStateMachineInput struct {
	Name       string `json:"name"`
	Definition string `json:"definition"`
	RoleArn    string `json:"roleArn"`
	Type       string `json:"type"`
}

type deleteStateMachineInput struct {
	StateMachineArn string `json:"stateMachineArn"`
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

// Name returns the service name.
func (h *Handler) Name() string { return "StepFunctions" }

// GetSupportedOperations returns all mocked Step Functions operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateStateMachine",
		"DeleteStateMachine",
		"ListStateMachines",
		"DescribeStateMachine",
		"UpdateStateMachine",
		"StartExecution",
		"StopExecution",
		"DescribeExecution",
		"ListExecutions",
		"GetExecutionHistory",
		"ListTagsForResource",
		"TagResource",
		"UntagResource",
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
	UpdateDate time.Time `json:"updateDate"`
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

type listStateMachineVersionsOutput struct {
	StateMachineVersions []any `json:"stateMachineVersions"`
}

func (h *Handler) stateMachineActions() map[string]actionFn {
	m := map[string]actionFn{
		"CreateStateMachine": func(b []byte) (any, error) {
			var input createStateMachineInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			sm, err := h.Backend.CreateStateMachine(input.Name, input.Definition, input.RoleArn, input.Type)
			if err != nil {
				return nil, err
			}

			return &createStateMachineOutput{
				StateMachineArn: sm.StateMachineArn,
				CreationDate:    sm.CreationDate,
			}, nil
		},
		"DeleteStateMachine": func(b []byte) (any, error) {
			var input deleteStateMachineInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			if err := h.Backend.DeleteStateMachine(input.StateMachineArn); err != nil {
				return nil, err
			}

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
		"UpdateStateMachine": func(_ []byte) (any, error) {
			return &updateStateMachineOutput{UpdateDate: time.Now().UTC()}, nil
		},
	}
	maps.Copy(m, h.stateMachineTagActions())

	return m
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
		"StartExecution": func(b []byte) (any, error) {
			var input startExecutionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			exec, err := h.Backend.StartExecution(input.StateMachineArn, input.Name, input.Input)
			if err != nil {
				return nil, err
			}

			return &startExecutionOutput{
				ExecutionArn: exec.ExecutionArn,
				StartDate:    exec.StartDate,
			}, nil
		},
		"StopExecution": func(b []byte) (any, error) {
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
		},
		"DescribeExecution": func(b []byte) (any, error) {
			var input describeExecutionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeExecution(input.ExecutionArn)
		},
		"ListExecutions": func(b []byte) (any, error) {
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
		},
		"GetExecutionHistory": func(b []byte) (any, error) {
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
		},
	}
}

func (h *Handler) dispatchTable() map[string]actionFn {
	table := make(map[string]actionFn)
	maps.Copy(table, h.stateMachineActions())
	maps.Copy(table, h.executionActions())
	maps.Copy(table, h.utilActions())

	return table
}

type validateStateMachineDefinitionInput struct {
	Definition string `json:"definition"`
}

// utilActions returns stubs for utility operations like definition validation.
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
		"ListStateMachineVersions": func(_ []byte) (any, error) {
			return &listStateMachineVersionsOutput{StateMachineVersions: []any{}}, nil
		},
	}
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
	switch {
	case errors.Is(reqErr, ErrStateMachineDoesNotExist):
		return "StateMachineDoesNotExist", http.StatusNotFound
	case errors.Is(reqErr, ErrExecutionDoesNotExist):
		return "ExecutionDoesNotExist", http.StatusNotFound
	case errors.Is(reqErr, ErrStateMachineAlreadyExists):
		return "StateMachineAlreadyExists", http.StatusConflict
	case errors.Is(reqErr, ErrExecutionAlreadyExists):
		return "ExecutionAlreadyExists", http.StatusConflict
	case errors.Is(reqErr, ErrInvalidDefinition):
		return "InvalidDefinition", http.StatusBadRequest
	case errors.Is(reqErr, errUnknownOperation):
		return "UnknownOperationException", http.StatusBadRequest
	default:
		return "InternalServerError", http.StatusInternalServerError
	}
}

// Reset clears all in-memory state from the backend. It is used by the
// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
func (h *Handler) Reset() {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		b.Reset()
	}
}
