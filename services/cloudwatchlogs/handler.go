package cloudwatchlogs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
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

type putRetentionPolicyInput struct {
	LogGroupName    string `json:"logGroupName"`
	RetentionInDays int32  `json:"retentionInDays"`
}

type deleteRetentionPolicyInput struct {
	LogGroupName string `json:"logGroupName"`
}

type putSubscriptionFilterInput struct {
	FilterPattern  string `json:"filterPattern"`
	FilterName     string `json:"filterName"`
	LogGroupName   string `json:"logGroupName"`
	DestinationArn string `json:"destinationArn"`
}

type describeSubscriptionFiltersInput struct {
	FilterNamePrefix string `json:"filterNamePrefix"`
	LogGroupName     string `json:"logGroupName"`
	NextToken        string `json:"nextToken"`
	Limit            int    `json:"limit"`
}

type deleteSubscriptionFilterInput struct {
	FilterName   string `json:"filterName"`
	LogGroupName string `json:"logGroupName"`
}

type startQueryInput struct {
	LogGroupName        string   `json:"logGroupName"`
	QueryString         string   `json:"queryString"`
	LogGroupNames       []string `json:"logGroupNames"`
	LogGroupIdentifiers []string `json:"logGroupIdentifiers"`
	StartTime           int64    `json:"startTime"`
	EndTime             int64    `json:"endTime"`
}

type startQueryOutput struct {
	QueryID string `json:"queryId"`
}

type getQueryResultsInput struct {
	QueryID string `json:"queryId"`
}

type getQueryResultsOutput struct {
	Status     QueryStatus     `json:"status"`
	Results    [][]ResultField `json:"results"`
	Statistics QueryStatistics `json:"statistics"`
}

type stopQueryInput struct {
	QueryID string `json:"queryId"`
}

type stopQueryOutput struct {
	Success bool `json:"success"`
}

type describeQueriesInput struct {
	LogGroupName string `json:"logGroupName"`
	Status       string `json:"status"`
	NextToken    string `json:"nextToken"`
	MaxResults   int    `json:"maxResults"`
}

type describeQueriesOutput struct {
	NextToken string      `json:"nextToken,omitempty"`
	Queries   []QueryInfo `json:"queries"`
}

// Handler is the Echo HTTP service handler for CloudWatch Logs operations.
type Handler struct {
	Backend StorageBackend
	janitor *Janitor
	tags    map[string]*tags.Tags
	tagsMu  *lockmetrics.RWMutex
}

// NewHandler creates a new CloudWatch Logs handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{
		Backend: backend,
		tags:    make(map[string]*tags.Tags),
		tagsMu:  lockmetrics.New("cwl.tags"),
	}
}

// WithJanitor attaches a background janitor to the handler.
// The janitor periodically evicts log events that have aged past their log
// group's retention policy. interval=0 uses the default of one minute.
func (h *Handler) WithJanitor(interval time.Duration) *Handler {
	if memBackend, ok := h.Backend.(*InMemoryBackend); ok {
		h.janitor = NewJanitor(memBackend, interval)
	}

	return h
}

// StartWorker starts the background janitor if it is configured.
func (h *Handler) StartWorker(ctx context.Context) error {
	if h.janitor != nil {
		go h.janitor.Run(ctx)
	}

	return nil
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
		"PutSubscriptionFilter",
		"DescribeSubscriptionFilters",
		"DeleteSubscriptionFilter",
		"StartQuery",
		"GetQueryResults",
		"StopQuery",
		"DescribeQueries",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "logs" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this CloudWatch Logs instance handles.
func (h *Handler) ChaosRegions() []string { return []string{config.DefaultRegion} }

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
	body, err := httputils.ReadBody(c.Request())
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

type putSubscriptionFilterOutput struct{}

type describeSubscriptionFiltersOutput struct {
	NextToken           string               `json:"nextToken,omitempty"`
	SubscriptionFilters []SubscriptionFilter `json:"subscriptionFilters"`
}

type deleteSubscriptionFilterOutput struct{}

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
		"PutRetentionPolicy": func(b []byte) (any, error) {
			var input putRetentionPolicyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			days := input.RetentionInDays
			if err := h.Backend.SetRetentionPolicy(input.LogGroupName, &days); err != nil {
				return nil, err
			}

			return &putRetentionPolicyOutput{}, nil
		},
		"DeleteRetentionPolicy": func(b []byte) (any, error) {
			var input deleteRetentionPolicyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			if err := h.Backend.SetRetentionPolicy(input.LogGroupName, nil); err != nil {
				return nil, err
			}

			return &deleteRetentionPolicyOutput{}, nil
		},
	}
}

func (h *Handler) subscriptionFilterActions() map[string]actionFn {
	return map[string]actionFn{
		"PutSubscriptionFilter": func(b []byte) (any, error) {
			var input putSubscriptionFilterInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			if err := h.Backend.PutSubscriptionFilter(
				input.LogGroupName, input.FilterName, input.FilterPattern, input.DestinationArn,
			); err != nil {
				return nil, err
			}

			return &putSubscriptionFilterOutput{}, nil
		},
		"DescribeSubscriptionFilters": func(b []byte) (any, error) {
			var input describeSubscriptionFiltersInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			filters, next, err := h.Backend.DescribeSubscriptionFilters(
				input.LogGroupName, input.FilterNamePrefix, input.NextToken, input.Limit,
			)
			if err != nil {
				return nil, err
			}

			return &describeSubscriptionFiltersOutput{SubscriptionFilters: filters, NextToken: next}, nil
		},
		"DeleteSubscriptionFilter": func(b []byte) (any, error) {
			var input deleteSubscriptionFilterInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			if err := h.Backend.DeleteSubscriptionFilter(input.LogGroupName, input.FilterName); err != nil {
				return nil, err
			}

			return &deleteSubscriptionFilterOutput{}, nil
		},
	}
}

// normalizeLogGroupIdentifier converts a log group identifier to a log group name.
// Log group identifiers may be ARNs (arn:...:log-group:<name>); in that case the
// log group name is extracted. Non-ARN identifiers are returned unchanged.
func normalizeLogGroupIdentifier(id string) string {
	const logGroupToken = ":log-group:"
	if idx := strings.LastIndex(id, logGroupToken); idx >= 0 {
		return id[idx+len(logGroupToken):]
	}

	return id
}

func (h *Handler) handleStartQuery(b []byte) (any, error) {
	var input startQueryInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	logGroups := input.LogGroupNames
	if len(logGroups) == 0 && input.LogGroupName != "" {
		logGroups = []string{input.LogGroupName}
	}
	for _, id := range input.LogGroupIdentifiers {
		logGroups = append(logGroups, normalizeLogGroupIdentifier(id))
	}

	queryID := uuid.New().String()
	if _, err := h.Backend.StartQuery(
		queryID,
		input.QueryString,
		logGroups,
		input.StartTime,
		input.EndTime,
	); err != nil {
		return nil, err
	}

	return &startQueryOutput{QueryID: queryID}, nil
}

func (h *Handler) handleGetQueryResults(b []byte) (any, error) {
	var input getQueryResultsInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	results, stats, status, err := h.Backend.GetQueryResults(input.QueryID)
	if err != nil {
		return nil, err
	}

	return &getQueryResultsOutput{Results: results, Statistics: stats, Status: status}, nil
}

func (h *Handler) handleStopQuery(b []byte) (any, error) {
	var input stopQueryInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	if err := h.Backend.StopQuery(input.QueryID); err != nil {
		return nil, err
	}

	return &stopQueryOutput{Success: true}, nil
}

func (h *Handler) handleDescribeQueries(b []byte) (any, error) {
	var input describeQueriesInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	queries, next, err := h.Backend.DescribeQueries(
		input.LogGroupName, input.Status, input.NextToken, input.MaxResults,
	)
	if err != nil {
		return nil, err
	}

	return &describeQueriesOutput{Queries: queries, NextToken: next}, nil
}

func (h *Handler) insightsActions() map[string]actionFn {
	return map[string]actionFn{
		"StartQuery":      h.handleStartQuery,
		"GetQueryResults": h.handleGetQueryResults,
		"StopQuery":       h.handleStopQuery,
		"DescribeQueries": h.handleDescribeQueries,
	}
}

func (h *Handler) dispatchTable() map[string]actionFn {
	table := make(map[string]actionFn)
	maps.Copy(table, h.logGroupActions())
	maps.Copy(table, h.logStreamActions())
	maps.Copy(table, h.logEventActions())
	maps.Copy(table, h.logTagActions())
	maps.Copy(table, h.subscriptionFilterActions())
	maps.Copy(table, h.insightsActions())

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
	case errors.Is(reqErr, ErrLogGroupNotFound), errors.Is(reqErr, ErrLogStreamNotFound),
		errors.Is(reqErr, ErrSubscriptionFilterNotFound), errors.Is(reqErr, ErrQueryNotFound):
		errType = "ResourceNotFoundException"
		statusCode = http.StatusNotFound
	case errors.Is(reqErr, ErrLogGroupAlreadyExists), errors.Is(reqErr, ErrLogStreamAlreadyExist):
		errType = "ResourceAlreadyExistsException"
		statusCode = http.StatusConflict
	case errors.Is(reqErr, ErrSubscriptionFilterLimitExceed):
		errType = "LimitExceededException"
		statusCode = http.StatusBadRequest
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

// Reset clears all in-memory state from the backend. It is used by the
// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
func (h *Handler) Reset() {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		b.Reset()
	}
}
