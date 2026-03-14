package timestreamquery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	timestreamQueryService      = "timestream"
	timestreamQueryTargetPrefix = "Timestream_20181101."
	contentType                 = "application/x-amz-json-1.0"
	endpointCachePeriod         = int64(1440)
	queryProgressPercentage     = 100.0
)

// ErrUnknownOperation is returned when an unrecognized operation is requested.
var ErrUnknownOperation = errors.New("unknown operation")

// Handler is the Echo HTTP handler for the Timestream Query service.
type Handler struct {
	Backend      *InMemoryBackend
	supportedOps map[string]bool
}

// NewHandler creates a new Timestream Query handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	h := &Handler{Backend: backend}
	ops := h.GetSupportedOperations()
	h.supportedOps = make(map[string]bool, len(ops))
	for _, op := range ops {
		h.supportedOps[op] = true
	}

	return h
}

// Name returns the handler name.
func (h *Handler) Name() string { return "TimestreamQuery" }

// GetSupportedOperations returns all supported Timestream Query operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"DescribeEndpoints",
		"Query",
		"CancelQuery",
		"CreateScheduledQuery",
		"DeleteScheduledQuery",
		"DescribeScheduledQuery",
		"ExecuteScheduledQuery",
		"ListScheduledQueries",
		"UpdateScheduledQuery",
		"TagResource",
		"UntagResource",
		"ListTagsForResource",
	}
}

// ChaosServiceName returns the service name for chaos injection.
func (h *Handler) ChaosServiceName() string { return timestreamQueryService }

// ChaosOperations returns the operations subject to chaos injection.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns the regions for chaos injection.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a matcher that identifies Timestream Query requests.
// It only matches operations explicitly supported by this handler to avoid
// intercepting operations belonging to other Timestream services (e.g. TimestreamWrite)
// that share the same X-Amz-Target prefix.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		target := c.Request().Header.Get("X-Amz-Target")
		if !strings.HasPrefix(target, timestreamQueryTargetPrefix) {
			return false
		}

		operation := strings.TrimPrefix(target, timestreamQueryTargetPrefix)

		return h.supportedOps[operation]
	}
}

// MatchPriority returns the matching priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation returns the operation name from the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	return strings.TrimPrefix(c.Request().Header.Get("X-Amz-Target"), timestreamQueryTargetPrefix)
}

// ExtractResource returns the ARN or name from the request body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var req struct {
		Arn  string `json:"Arn"`
		Name string `json:"Name"`
	}

	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		return ""
	}

	if req.Arn != "" {
		return req.Arn
	}

	return req.Name
}

// Handler returns the Echo handler function for Timestream Query requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "timestreamquery: failed to read request body", "error", err)

			return c.String(http.StatusInternalServerError, "internal server error")
		}

		op := h.ExtractOperation(c)
		result, dispErr := h.dispatch(ctx, op, body, c.Request().Host)

		if dispErr != nil {
			return h.handleError(c, dispErr)
		}

		if result == nil {
			return c.JSONBlob(http.StatusOK, []byte(`{}`))
		}

		c.Response().Header().Set("Content-Type", contentType)

		return c.JSONBlob(http.StatusOK, result)
	}
}

func (h *Handler) dispatch(_ context.Context, op string, body []byte, host string) ([]byte, error) {
	switch op {
	case "DescribeEndpoints":
		return h.handleDescribeEndpoints(host)
	case "Query":
		return h.handleQuery(body)
	case "CancelQuery":
		return h.handleCancelQuery(body)
	case "CreateScheduledQuery":
		return h.handleCreateScheduledQuery(body)
	case "DeleteScheduledQuery":
		return h.handleDeleteScheduledQuery(body)
	case "DescribeScheduledQuery":
		return h.handleDescribeScheduledQuery(body)
	case "ExecuteScheduledQuery":
		return h.handleExecuteScheduledQuery(body)
	case "ListScheduledQueries":
		return h.handleListScheduledQueries()
	case "UpdateScheduledQuery":
		return h.handleUpdateScheduledQuery(body)
	case "TagResource":
		return h.handleTagResource(body)
	case "UntagResource":
		return h.handleUntagResource(body)
	case "ListTagsForResource":
		return h.handleListTagsForResource(body)
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnknownOperation, op)
	}
}

func (h *Handler) handleDescribeEndpoints(host string) ([]byte, error) {
	return json.Marshal(map[string]any{
		"Endpoints": []map[string]any{
			{
				"Address":              host,
				"CachePeriodInMinutes": endpointCachePeriod,
			},
		},
	})
}

func (h *Handler) handleQuery(body []byte) ([]byte, error) {
	var req struct {
		QueryString string `json:"QueryString"`
		NextToken   string `json:"NextToken"`
		MaxRows     int    `json:"MaxRows"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	if req.QueryString == "" {
		return nil, fmt.Errorf("%w: QueryString is required", ErrInvalidRequest)
	}

	result := h.Backend.Query(req.QueryString)

	return json.Marshal(map[string]any{
		"QueryId":    result.QueryID,
		"Rows":       result.Rows,
		"ColumnInfo": result.Columns,
		"QueryStatus": map[string]any{
			"ProgressPercentage":     queryProgressPercentage,
			"CumulativeBytesScanned": int64(0),
			"CumulativeBytesMetered": int64(0),
		},
	})
}

func (h *Handler) handleCancelQuery(body []byte) ([]byte, error) {
	var req struct {
		QueryID string `json:"QueryId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	if req.QueryID == "" {
		return nil, fmt.Errorf("%w: QueryId is required", ErrInvalidRequest)
	}

	if err := h.Backend.CancelQuery(req.QueryID); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}

type createScheduledQueryInput struct {
	ErrorReportConfiguration struct {
		S3Configuration *struct {
			BucketName string `json:"BucketName"`
		} `json:"S3Configuration"`
	} `json:"ErrorReportConfiguration"`
	NotificationConfiguration struct {
		SnsConfiguration *struct {
			TopicArn string `json:"TopicArn"`
		} `json:"SnsConfiguration"`
	} `json:"NotificationConfiguration"`
	ScheduledQueryExecutionRoleArn string `json:"ScheduledQueryExecutionRoleArn"`
	QueryString                    string `json:"QueryString"`
	Name                           string `json:"Name"`
	ScheduleConfiguration          struct {
		ScheduleExpression string `json:"ScheduleExpression"`
	} `json:"ScheduleConfiguration"`
	TargetConfiguration struct {
		TimestreamConfiguration *struct {
			DatabaseName string `json:"DatabaseName"`
			TableName    string `json:"TableName"`
		} `json:"TimestreamConfiguration"`
	} `json:"TargetConfiguration"`
	Tags []struct {
		Key   string `json:"Key"`
		Value string `json:"Value"`
	} `json:"Tags"`
}

func (h *Handler) handleCreateScheduledQuery(body []byte) ([]byte, error) {
	var req createScheduledQueryInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidRequest)
	}

	if req.QueryString == "" {
		return nil, fmt.Errorf("%w: QueryString is required", ErrInvalidRequest)
	}

	notificationTopicArn := ""
	if req.NotificationConfiguration.SnsConfiguration != nil {
		notificationTopicArn = req.NotificationConfiguration.SnsConfiguration.TopicArn
	}

	errorReportBucket := ""
	if req.ErrorReportConfiguration.S3Configuration != nil {
		errorReportBucket = req.ErrorReportConfiguration.S3Configuration.BucketName
	}

	targetDB := ""
	targetTable := ""

	if req.TargetConfiguration.TimestreamConfiguration != nil {
		targetDB = req.TargetConfiguration.TimestreamConfiguration.DatabaseName
		targetTable = req.TargetConfiguration.TimestreamConfiguration.TableName
	}

	tags := make(map[string]string, len(req.Tags))

	for _, t := range req.Tags {
		tags[t.Key] = t.Value
	}

	sq, err := h.Backend.CreateScheduledQuery(
		req.Name, req.QueryString,
		req.ScheduleConfiguration.ScheduleExpression,
		req.ScheduledQueryExecutionRoleArn,
		notificationTopicArn, errorReportBucket,
		targetDB, targetTable,
		tags,
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"Arn": sq.Arn,
	})
}

func (h *Handler) handleDeleteScheduledQuery(body []byte) ([]byte, error) {
	var req struct {
		ScheduledQueryArn string `json:"ScheduledQueryArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	if req.ScheduledQueryArn == "" {
		return nil, fmt.Errorf("%w: ScheduledQueryArn is required", ErrInvalidRequest)
	}

	if err := h.Backend.DeleteScheduledQuery(req.ScheduledQueryArn); err != nil {
		return nil, err
	}

	return nil, nil
}

func (h *Handler) handleDescribeScheduledQuery(body []byte) ([]byte, error) {
	var req struct {
		ScheduledQueryArn string `json:"ScheduledQueryArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	if req.ScheduledQueryArn == "" {
		return nil, fmt.Errorf("%w: ScheduledQueryArn is required", ErrInvalidRequest)
	}

	sq, err := h.Backend.DescribeScheduledQuery(req.ScheduledQueryArn)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"ScheduledQuery": scheduledQueryToView(sq),
	})
}

func (h *Handler) handleExecuteScheduledQuery(body []byte) ([]byte, error) {
	var req struct {
		ScheduledQueryArn string  `json:"ScheduledQueryArn"`
		InvocationTime    float64 `json:"InvocationTime"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	if req.ScheduledQueryArn == "" {
		return nil, fmt.Errorf("%w: ScheduledQueryArn is required", ErrInvalidRequest)
	}

	if req.InvocationTime == 0 {
		return nil, fmt.Errorf("%w: InvocationTime is required", ErrInvalidRequest)
	}

	invocationTime := time.Unix(int64(req.InvocationTime), 0)

	if err := h.Backend.ExecuteScheduledQuery(req.ScheduledQueryArn, invocationTime); err != nil {
		return nil, err
	}

	return nil, nil
}

func (h *Handler) handleListScheduledQueries() ([]byte, error) {
	list := h.Backend.ListScheduledQueries()

	items := make([]map[string]any, 0, len(list))
	for _, sq := range list {
		items = append(items, map[string]any{
			"Arn":   sq.Arn,
			"Name":  sq.Name,
			"State": sq.State,
		})
	}

	return json.Marshal(map[string]any{
		"ScheduledQueries": items,
	})
}

func (h *Handler) handleUpdateScheduledQuery(body []byte) ([]byte, error) {
	var req struct {
		ScheduledQueryArn string `json:"ScheduledQueryArn"`
		State             string `json:"State"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	if req.ScheduledQueryArn == "" {
		return nil, fmt.Errorf("%w: ScheduledQueryArn is required", ErrInvalidRequest)
	}

	if req.State == "" {
		return nil, fmt.Errorf("%w: State is required", ErrInvalidRequest)
	}

	if err := h.Backend.UpdateScheduledQuery(req.ScheduledQueryArn, req.State); err != nil {
		return nil, err
	}

	return nil, nil
}

func (h *Handler) handleTagResource(body []byte) ([]byte, error) {
	var req struct {
		ResourceARN string `json:"ResourceARN"`
		Tags        []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"Tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	if req.ResourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceARN is required", ErrInvalidRequest)
	}

	tags := make(map[string]string, len(req.Tags))

	for _, t := range req.Tags {
		tags[t.Key] = t.Value
	}

	if err := h.Backend.TagResource(req.ResourceARN, tags); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}

func (h *Handler) handleUntagResource(body []byte) ([]byte, error) {
	var req struct {
		ResourceARN string   `json:"ResourceARN"`
		TagKeys     []string `json:"TagKeys"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	if req.ResourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceARN is required", ErrInvalidRequest)
	}

	if err := h.Backend.UntagResource(req.ResourceARN, req.TagKeys); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}

func (h *Handler) handleListTagsForResource(body []byte) ([]byte, error) {
	var req struct {
		ResourceARN string `json:"ResourceARN"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	if req.ResourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceARN is required", ErrInvalidRequest)
	}

	tags, err := h.Backend.ListTagsForResource(req.ResourceARN)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"Tags": tags,
	})
}

func (h *Handler) handleError(c *echo.Context, err error) error {
	c.Response().Header().Set("Content-Type", contentType)

	switch {
	case errors.Is(err, ErrNotFound):
		return c.JSONBlob(http.StatusBadRequest, errorPayload("ResourceNotFoundException", err.Error()))
	case errors.Is(err, ErrAlreadyExists):
		return c.JSONBlob(http.StatusBadRequest, errorPayload("ConflictException", err.Error()))
	case errors.Is(err, ErrInvalidRequest):
		return c.JSONBlob(http.StatusBadRequest, errorPayload("ValidationException", err.Error()))
	case errors.Is(err, ErrUnknownOperation):
		return c.JSONBlob(http.StatusBadRequest, errorPayload("ValidationException", err.Error()))
	default:
		return c.JSONBlob(http.StatusInternalServerError, errorPayload("InternalServerException", err.Error()))
	}
}

func errorPayload(errType, msg string) []byte {
	b, _ := json.Marshal(map[string]string{
		"__type":  errType,
		"message": msg,
	})

	return b
}

// scheduledQueryToView converts a ScheduledQuery to an API response map.
func scheduledQueryToView(sq *ScheduledQuery) map[string]any {
	view := map[string]any{
		"Arn":          sq.Arn,
		"Name":         sq.Name,
		"State":        sq.State,
		"QueryString":  sq.QueryString,
		"CreationTime": epochSeconds(sq.CreationTime),
	}

	if sq.ScheduleExpression != "" {
		view["ScheduleConfiguration"] = map[string]any{
			"ScheduleExpression": sq.ScheduleExpression,
		}
	}

	if sq.ExecutionRoleArn != "" {
		view["ScheduledQueryExecutionRoleArn"] = sq.ExecutionRoleArn
	}

	if sq.NotificationTopicArn != "" {
		view["NotificationConfiguration"] = map[string]any{
			"SnsConfiguration": map[string]string{
				"TopicArn": sq.NotificationTopicArn,
			},
		}
	}

	if sq.ErrorReportS3BucketName != "" {
		view["ErrorReportConfiguration"] = map[string]any{
			"S3Configuration": map[string]string{
				"BucketName": sq.ErrorReportS3BucketName,
			},
		}
	}

	if sq.TargetDatabase != "" {
		view["TargetConfiguration"] = map[string]any{
			"TimestreamConfiguration": map[string]string{
				"DatabaseName": sq.TargetDatabase,
				"TableName":    sq.TargetTable,
			},
		}
	}

	if !sq.LastRunTime.IsZero() {
		view["LastRunSummary"] = map[string]any{
			"InvocationTime": epochSeconds(sq.LastRunTime),
		}
	}

	return view
}

// epochSeconds converts a [time.Time] to Unix epoch seconds as float64.
func epochSeconds(t time.Time) float64 {
	return float64(t.Unix()) + float64(t.Nanosecond())/1e9
}

// ErrInvalidRequest indicates a bad request.
var ErrInvalidRequest = errors.New("ValidationException")
