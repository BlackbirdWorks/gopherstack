package cloudwatchlogs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputil"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

var errUnknownOperation = errors.New("UnknownOperationException")

type createLogGroupInput struct {
	LogGroupName string `json:"logGroupName"`
}

type deleteLogGroupInput struct {
	LogGroupName string `json:"logGroupName"`
}

type describeLogGroupsInput struct {
	LogGroupNamePrefix string `json:"logGroupNamePrefix"`
	NextToken          string `json:"nextToken"`
	Limit              int    `json:"limit"`
}

type createLogStreamInput struct {
	LogGroupName  string `json:"logGroupName"`
	LogStreamName string `json:"logStreamName"`
}

type describeLogStreamsInput struct {
	LogGroupName        string `json:"logGroupName"`
	LogStreamNamePrefix string `json:"logStreamNamePrefix"`
	NextToken           string `json:"nextToken"`
	Limit               int    `json:"limit"`
}

type putLogEventsInput struct {
	LogGroupName  string          `json:"logGroupName"`
	LogStreamName string          `json:"logStreamName"`
	LogEvents     []InputLogEvent `json:"logEvents"`
}

type getLogEventsInput struct {
	StartTime     *int64 `json:"startTime"`
	EndTime       *int64 `json:"endTime"`
	LogGroupName  string `json:"logGroupName"`
	LogStreamName string `json:"logStreamName"`
	NextToken     string `json:"nextToken"`
	Limit         int    `json:"limit"`
}

type filterLogEventsInput struct {
	StartTime      *int64   `json:"startTime"`
	EndTime        *int64   `json:"endTime"`
	LogGroupName   string   `json:"logGroupName"`
	FilterPattern  string   `json:"filterPattern"`
	NextToken      string   `json:"nextToken"`
	LogStreamNames []string `json:"logStreamNames"`
	Limit          int      `json:"limit"`
}

type listTagsLogGroupInput struct {
	LogGroupName string `json:"logGroupName"`
}

type listTagsForResourceInput struct {
	ResourceArn string `json:"resourceArn"`
}

type tagLogGroupInput struct {
	Tags         *tags.Tags `json:"tags"`
	LogGroupName string     `json:"logGroupName"`
}

type untagLogGroupInput struct {
	LogGroupName string   `json:"logGroupName"`
	Tags         []string `json:"tags"`
}

// Handler is the Echo HTTP service handler for CloudWatch Logs operations.
type Handler struct {
	Backend StorageBackend
	Logger  *slog.Logger
	tags    map[string]*tags.Tags
	tagsMu  *lockmetrics.RWMutex
}

// NewHandler creates a new CloudWatch Logs handler.
func NewHandler(backend StorageBackend, log *slog.Logger) *Handler {
	return &Handler{
		Backend: backend,
		Logger:  log,
		tags:    make(map[string]*tags.Tags),
		tagsMu:  lockmetrics.New("cwl.tags"),
	}
}

func (h *Handler) setTags(resourceID string, kv map[string]string) {
	h.tagsMu.Lock("setTags")
	defer h.tagsMu.Unlock()
	if h.tags[resourceID] == nil {
		h.tags[resourceID] = tags.New("cwl." + resourceID + ".tags")
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
func (h *Handler) Name() string { return "CloudWatchLogs" }

// GetSupportedOperations returns all mocked CloudWatch Logs operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateLogGroup",
		"DeleteLogGroup",
		"DescribeLogGroups",
		"CreateLogStream",
		"DescribeLogStreams",
		"PutLogEvents",
		"GetLogEvents",
		"FilterLogEvents",
		"ListTagsLogGroup",
		"ListTagsForResource",
		"TagLogGroup",
		"UntagLogGroup",
		"PutRetentionPolicy",
		"DeleteRetentionPolicy",
	}
}

// RouteMatcher returns a matcher for CloudWatch Logs requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		target := c.Request().Header.Get("X-Amz-Target")

		return strings.HasPrefix(target, "Logs_20140328.")
	}
}

const cloudWatchLogsMatchPriority = 100

// MatchPriority returns the routing priority for the CloudWatch Logs handler.
func (h *Handler) MatchPriority() int { return cloudWatchLogsMatchPriority }

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

	for _, key := range []string{"logGroupName", "logStreamName"} {
		if v, ok := data[key].(string); ok && v != "" {
			return v
		}
	}

	return ""
}

// Handler returns the Echo handler function for CloudWatch Logs requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"CloudWatchLogs", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

type actionFn func([]byte) (any, error)

type createLogGroupOutput struct{}

type deleteLogGroupOutput struct{}

type describeLogGroupsOutput struct {
	NextToken string     `json:"nextToken,omitempty"`
	LogGroups []LogGroup `json:"logGroups"`
}

type createLogStreamOutput struct{}

type describeLogStreamsOutput struct {
	NextToken  string      `json:"nextToken,omitempty"`
	LogStreams []LogStream `json:"logStreams"`
}

type putLogEventsOutput struct {
	NextSequenceToken string `json:"nextSequenceToken"`
}

type getLogEventsOutput struct {
	NextForwardToken  string           `json:"nextForwardToken"`
	NextBackwardToken string           `json:"nextBackwardToken"`
	Events            []OutputLogEvent `json:"events"`
}

type filterLogEventsOutput struct {
	NextToken string           `json:"nextToken,omitempty"`
	Events    []OutputLogEvent `json:"events"`
}

type listTagsLogGroupOutput struct {
	Tags map[string]string `json:"tags"`
}

type listTagsForResourceOutput struct {
	Tags map[string]string `json:"tags"`
}

type tagLogGroupOutput struct{}

type untagLogGroupOutput struct{}

type putRetentionPolicyOutput struct{}

type deleteRetentionPolicyOutput struct{}

func (h *Handler) logGroupActions() map[string]actionFn {
	return map[string]actionFn{
		"CreateLogGroup": func(b []byte) (any, error) {
			var input createLogGroupInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			if _, err := h.Backend.CreateLogGroup(input.LogGroupName); err != nil {
				return nil, err
			}

			return &createLogGroupOutput{}, nil
		},
		"DeleteLogGroup": func(b []byte) (any, error) {
			var input deleteLogGroupInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			if err := h.Backend.DeleteLogGroup(input.LogGroupName); err != nil {
				return nil, err
			}

			return &deleteLogGroupOutput{}, nil
		},
		"DescribeLogGroups": func(b []byte) (any, error) {
			var input describeLogGroupsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			groups, next, err := h.Backend.DescribeLogGroups(input.LogGroupNamePrefix, input.NextToken, input.Limit)
			if err != nil {
				return nil, err
			}

			return &describeLogGroupsOutput{LogGroups: groups, NextToken: next}, nil
		},
	}
}

func (h *Handler) logStreamActions() map[string]actionFn {
	return map[string]actionFn{
		"CreateLogStream": func(b []byte) (any, error) {
			var input createLogStreamInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			if _, err := h.Backend.CreateLogStream(input.LogGroupName, input.LogStreamName); err != nil {
				return nil, err
			}

			return &createLogStreamOutput{}, nil
		},
		"DescribeLogStreams": func(b []byte) (any, error) {
			var input describeLogStreamsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			streams, next, err := h.Backend.DescribeLogStreams(
				input.LogGroupName, input.LogStreamNamePrefix, input.NextToken, input.Limit)
			if err != nil {
				return nil, err
			}

			return &describeLogStreamsOutput{LogStreams: streams, NextToken: next}, nil
		},
	}
}

func (h *Handler) logEventActions() map[string]actionFn {
	return map[string]actionFn{
		"PutLogEvents": func(b []byte) (any, error) {
			var input putLogEventsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			next, err := h.Backend.PutLogEvents(input.LogGroupName, input.LogStreamName, input.LogEvents)
			if err != nil {
				return nil, err
			}

			return &putLogEventsOutput{NextSequenceToken: next}, nil
		},
		"GetLogEvents": func(b []byte) (any, error) {
			var input getLogEventsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			evts, fwd, bwd, err := h.Backend.GetLogEvents(
				input.LogGroupName, input.LogStreamName, input.StartTime, input.EndTime,
				input.Limit, input.NextToken)
			if err != nil {
				return nil, err
			}

			return &getLogEventsOutput{
				Events:            evts,
				NextForwardToken:  fwd,
				NextBackwardToken: bwd,
			}, nil
		},
		"FilterLogEvents": func(b []byte) (any, error) {
			var input filterLogEventsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			evts, next, err := h.Backend.FilterLogEvents(
				input.LogGroupName, input.LogStreamNames, input.FilterPattern,
				input.StartTime, input.EndTime, input.Limit, input.NextToken)
			if err != nil {
				return nil, err
			}

			return &filterLogEventsOutput{Events: evts, NextToken: next}, nil
		},
	}
}

func (h *Handler) logTagActions() map[string]actionFn {
	return map[string]actionFn{
		"ListTagsLogGroup": func(b []byte) (any, error) {
			var input listTagsLogGroupInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return &listTagsLogGroupOutput{Tags: h.getTags(input.LogGroupName)}, nil
		},
		"ListTagsForResource": func(b []byte) (any, error) {
			var input listTagsForResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return &listTagsForResourceOutput{Tags: h.getTags(input.ResourceArn)}, nil
		},
		"TagLogGroup": func(b []byte) (any, error) {
			var input tagLogGroupInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			var kv map[string]string
			if input.Tags != nil {
				kv = input.Tags.Clone()
			}
			h.setTags(input.LogGroupName, kv)

			return &tagLogGroupOutput{}, nil
		},
		"UntagLogGroup": func(b []byte) (any, error) {
			var input untagLogGroupInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			h.removeTags(input.LogGroupName, input.Tags)

			return &untagLogGroupOutput{}, nil
		},
		"PutRetentionPolicy": func(_ []byte) (any, error) {
			// Stub: accept any retention days, return success.
			return &putRetentionPolicyOutput{}, nil
		},
		"DeleteRetentionPolicy": func(_ []byte) (any, error) {
			return &deleteRetentionPolicyOutput{}, nil
		},
	}
}

func (h *Handler) dispatchTable() map[string]actionFn {
	table := make(map[string]actionFn)
	maps.Copy(table, h.logGroupActions())
	maps.Copy(table, h.logStreamActions())
	maps.Copy(table, h.logEventActions())
	maps.Copy(table, h.logTagActions())

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
	case errors.Is(reqErr, ErrLogGroupNotFound), errors.Is(reqErr, ErrLogStreamNotFound):
		errType = "ResourceNotFoundException"
		statusCode = http.StatusNotFound
	case errors.Is(reqErr, ErrLogGroupAlreadyExists), errors.Is(reqErr, ErrLogStreamAlreadyExist):
		errType = "ResourceAlreadyExistsException"
		statusCode = http.StatusConflict
	case errors.Is(reqErr, errUnknownOperation):
		errType = "UnknownOperationException"
		statusCode = http.StatusBadRequest
	default:
		errType = "InternalServerError"
		statusCode = http.StatusInternalServerError
	}

	if statusCode == http.StatusInternalServerError {
		log.ErrorContext(ctx, "CloudWatchLogs internal error", "error", reqErr, "action", action)
	} else {
		log.WarnContext(ctx, "CloudWatchLogs request error", "error", reqErr, "action", action)
	}

	errResp := service.JSONErrorResponse{
		Type:    errType,
		Message: reqErr.Error(),
	}

	payload, _ := json.Marshal(errResp)

	return c.JSONBlob(statusCode, payload)
}
