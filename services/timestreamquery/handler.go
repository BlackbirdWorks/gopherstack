package timestreamquery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	keyArn = "Arn"
)

const (
	opTagResource         = "TagResource"
	opUntagResource       = "UntagResource"
	opListTagsForResource = "ListTagsForResource"
)

const (
	timestreamQueryService      = "timestream"
	timestreamQueryTargetPrefix = "Timestream_20181101."
	contentType                 = "application/x-amz-json-1.0"
	endpointCachePeriod         = int64(1440)
	queryProgressPercentage     = 100.0
)

// writeServiceTagOps returns the set of tag operations shared between the
// Timestream Write and Query services.  The Write service provides a unified
// tag store for all Timestream resource types, so the Query RouteMatcher must
// not claim these operations.
func writeServiceTagOps() map[string]bool {
	return map[string]bool{
		opTagResource:         true,
		opUntagResource:       true,
		opListTagsForResource: true,
	}
}

// ErrUnknownOperation is returned when an unrecognized operation is requested.
var ErrUnknownOperation = errors.New("unknown operation")

// Handler is the Echo HTTP handler for the Timestream Query service.
type Handler struct {
	Backend      StorageBackend
	supportedOps map[string]bool
}

// NewHandler creates a new Timestream Query handler.
func NewHandler(backend StorageBackend) *Handler {
	h := &Handler{Backend: backend}
	ops := h.GetSupportedOperations()
	h.supportedOps = make(map[string]bool, len(ops))

	for _, op := range ops {
		h.supportedOps[op] = true
	}

	return h
}

// Reset clears handler state, delegating to the backend if it supports Reset.
func (h *Handler) Reset() {
	if r, ok := h.Backend.(interface{ Reset() }); ok {
		r.Reset()
	}
}

// Name returns the handler name.
func (h *Handler) Name() string { return "TimestreamQuery" }

// GetSupportedOperations returns all supported Timestream Query operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CancelQuery",
		"CreateScheduledQuery",
		"DeleteScheduledQuery",
		"DescribeAccountSettings",
		"DescribeEndpoints",
		"DescribeScheduledQuery",
		"ExecuteScheduledQuery",
		"ListScheduledQueries",
		opListTagsForResource,
		"PrepareQuery",
		"Query",
		opTagResource,
		opUntagResource,
		"UpdateAccountSettings",
		"UpdateScheduledQuery",
	}
}

// ChaosServiceName returns the service name for chaos injection.
func (h *Handler) ChaosServiceName() string { return timestreamQueryService }

// ChaosOperations returns the operations subject to chaos injection.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns the default region for chaos injection.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a matcher that identifies Timestream Query requests.
// It only matches operations explicitly supported by this handler to avoid
// intercepting operations belonging to other Timestream services (e.g. TimestreamWrite)
// that share the same X-Amz-Target prefix.  Tag operations (TagResource,
// UntagResource, ListTagsForResource) are intentionally excluded: they are
// routed to the TimestreamWrite handler which provides a single unified tag
// store for all Timestream resource types.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		target := c.Request().Header.Get("X-Amz-Target")
		if !strings.HasPrefix(target, timestreamQueryTargetPrefix) {
			return false
		}

		operation := strings.TrimPrefix(target, timestreamQueryTargetPrefix)

		// Defer shared tag operations to the TimestreamWrite handler so that
		// database/table ARNs and scheduled-query ARNs all share the same tag
		// store under a single endpoint.
		if writeServiceTagOps()[operation] {
			return false
		}

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
// It checks ScheduledQueryArn, ResourceARN, Arn, and Name fields in order.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var req struct {
		ScheduledQueryArn string `json:"ScheduledQueryArn"`
		ResourceARN       string `json:"ResourceARN"`
		Arn               string `json:"Arn"`
		Name              string `json:"Name"`
	}

	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		return ""
	}

	if req.ScheduledQueryArn != "" {
		return req.ScheduledQueryArn
	}

	if req.ResourceARN != "" {
		return req.ResourceARN
	}

	if req.Arn != "" {
		return req.Arn
	}

	return req.Name
}

// Handler returns the Echo handler function for Timestream Query requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		// Resolve the per-request region (from SigV4 / X-Amz-Region) and attach
		// it to the context so backend operations are region-scoped.
		region := httputils.ExtractRegionFromRequest(c.Request(), h.Backend.Region())
		ctx := context.WithValue(c.Request().Context(), regionContextKey{}, region)
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

		c.Response().Header().Set("Content-Type", contentType)

		if result == nil {
			return c.JSONBlob(http.StatusOK, []byte(`{}`))
		}

		return c.JSONBlob(http.StatusOK, result)
	}
}

func (h *Handler) dispatch(ctx context.Context, op string, body []byte, host string) ([]byte, error) {
	switch op {
	case "DescribeEndpoints":
		return h.handleDescribeEndpoints(host)
	case "Query":
		return h.handleQuery(ctx, body)
	case "CancelQuery":
		return h.handleCancelQuery(ctx, body)
	default:
		return h.dispatchScheduledQueryAndTagOps(ctx, op, body)
	}
}

func (h *Handler) dispatchScheduledQueryAndTagOps(ctx context.Context, op string, body []byte) ([]byte, error) {
	switch op {
	case "CreateScheduledQuery":
		return h.handleCreateScheduledQuery(ctx, body)
	case "DeleteScheduledQuery":
		return h.handleDeleteScheduledQuery(ctx, body)
	case "DescribeScheduledQuery":
		return h.handleDescribeScheduledQuery(ctx, body)
	case "ExecuteScheduledQuery":
		return h.handleExecuteScheduledQuery(ctx, body)
	case "ListScheduledQueries":
		return h.handleListScheduledQueries(ctx, body)
	case "UpdateScheduledQuery":
		return h.handleUpdateScheduledQuery(ctx, body)
	case opTagResource:
		return h.handleTagResource(ctx, body)
	case opUntagResource:
		return h.handleUntagResource(ctx, body)
	case opListTagsForResource:
		return h.handleListTagsForResource(ctx, body)
	default:
		return h.dispatchAccountOps(ctx, op, body)
	}
}

func (h *Handler) dispatchAccountOps(ctx context.Context, op string, body []byte) ([]byte, error) {
	switch op {
	case "DescribeAccountSettings":
		return h.handleDescribeAccountSettings(ctx)
	case "PrepareQuery":
		return h.handlePrepareQuery(ctx, body)
	case "UpdateAccountSettings":
		return h.handleUpdateAccountSettings(ctx, body)
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

func (h *Handler) handleQuery(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		QueryInsights *struct {
			Mode string `json:"Mode"`
		} `json:"QueryInsights"`
		QueryString string `json:"QueryString"`
		ClientToken string `json:"ClientToken"`
		NextToken   string `json:"NextToken"`
		MaxRows     int32  `json:"MaxRows"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	insightsMode := ""
	if req.QueryInsights != nil {
		insightsMode = req.QueryInsights.Mode
	}

	if req.NextToken == "" && req.QueryString == "" {
		return nil, fmt.Errorf("%w: QueryString is required", ErrValidation)
	}

	page, err := h.Backend.QueryWithOptions(ctx, QueryOptions{
		QueryString:  req.QueryString,
		ClientToken:  req.ClientToken,
		NextToken:    req.NextToken,
		MaxRows:      req.MaxRows,
		InsightsMode: insightsMode,
	})
	if err != nil {
		return nil, err
	}

	resp := map[string]any{
		"QueryId":    page.QueryID,
		"Rows":       marshalRows(page.Rows),
		"ColumnInfo": marshalColumnInfos(page.Columns),
		"QueryStatus": map[string]any{
			"ProgressPercentage":     page.QueryStatus.ProgressPercentage,
			"CumulativeBytesScanned": page.QueryStatus.CumulativeBytesScanned,
			"CumulativeBytesMetered": page.QueryStatus.CumulativeBytesMetered,
		},
	}
	if page.NextToken != "" {
		resp["NextToken"] = page.NextToken
	}
	if page.Insights != nil {
		resp["QueryInsightsResponse"] = page.Insights
	}

	return json.Marshal(resp)
}

// marshalRows converts []Row to JSON-serialisable form.
func marshalRows(rows []Row) []map[string]any {
	out := make([]map[string]any, len(rows))
	for i, r := range rows {
		data := make([]any, len(r.Data))
		for j, d := range r.Data {
			data[j] = d
		}
		out[i] = map[string]any{"Data": data}
	}

	return out
}

// marshalColumnInfos converts []ColumnInfo to JSON-serialisable form.
func marshalColumnInfos(cols []ColumnInfo) []map[string]any {
	out := make([]map[string]any, len(cols))
	for i, c := range cols {
		out[i] = map[string]any{
			"Name": c.Name,
			"Type": map[string]any{"ScalarType": c.Type.ScalarType},
		}
	}

	return out
}

func (h *Handler) handleCancelQuery(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		QueryID string `json:"QueryId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	if req.QueryID == "" {
		return nil, fmt.Errorf("%w: QueryId is required", ErrValidation)
	}

	if err := h.Backend.CancelQuery(ctx, req.QueryID); err != nil {
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

func (h *Handler) handleCreateScheduledQuery(ctx context.Context, body []byte) ([]byte, error) {
	var req createScheduledQueryInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	if req.QueryString == "" {
		return nil, fmt.Errorf("%w: QueryString is required", ErrValidation)
	}

	if req.ScheduledQueryExecutionRoleArn == "" {
		return nil, fmt.Errorf("%w: ScheduledQueryExecutionRoleArn is required", ErrValidation)
	}

	if req.ScheduleConfiguration.ScheduleExpression == "" {
		return nil, fmt.Errorf("%w: ScheduleConfiguration.ScheduleExpression is required", ErrValidation)
	}

	if req.NotificationConfiguration.SnsConfiguration == nil ||
		req.NotificationConfiguration.SnsConfiguration.TopicArn == "" {
		return nil, fmt.Errorf(
			"%w: NotificationConfiguration.SnsConfiguration.TopicArn is required",
			ErrValidation,
		)
	}

	if req.ErrorReportConfiguration.S3Configuration == nil ||
		req.ErrorReportConfiguration.S3Configuration.BucketName == "" {
		return nil, fmt.Errorf(
			"%w: ErrorReportConfiguration.S3Configuration.BucketName is required",
			ErrValidation,
		)
	}

	notificationTopicArn := req.NotificationConfiguration.SnsConfiguration.TopicArn
	errorReportBucket := req.ErrorReportConfiguration.S3Configuration.BucketName

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
		ctx,
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
		keyArn: sq.Arn,
	})
}

func (h *Handler) handleDeleteScheduledQuery(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ScheduledQueryArn string `json:"ScheduledQueryArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	if req.ScheduledQueryArn == "" {
		return nil, fmt.Errorf("%w: ScheduledQueryArn is required", ErrValidation)
	}

	if err := h.Backend.DeleteScheduledQuery(ctx, req.ScheduledQueryArn); err != nil {
		return nil, err
	}

	return nil, nil
}

func (h *Handler) handleDescribeScheduledQuery(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ScheduledQueryArn string `json:"ScheduledQueryArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	if req.ScheduledQueryArn == "" {
		return nil, fmt.Errorf("%w: ScheduledQueryArn is required", ErrValidation)
	}

	sq, err := h.Backend.DescribeScheduledQuery(ctx, req.ScheduledQueryArn)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"ScheduledQuery": scheduledQueryToView(sq),
	})
}

func (h *Handler) handleExecuteScheduledQuery(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ScheduledQueryArn string  `json:"ScheduledQueryArn"`
		InvocationTime    float64 `json:"InvocationTime"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	if req.ScheduledQueryArn == "" {
		return nil, fmt.Errorf("%w: ScheduledQueryArn is required", ErrValidation)
	}

	if req.InvocationTime == 0 {
		return nil, fmt.Errorf("%w: InvocationTime is required", ErrValidation)
	}

	invocationTime := time.Unix(int64(req.InvocationTime), 0)

	if err := h.Backend.ExecuteScheduledQuery(ctx, req.ScheduledQueryArn, invocationTime); err != nil {
		return nil, err
	}

	return nil, nil
}

func (h *Handler) handleListScheduledQueries(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken  string `json:"NextToken"`
		MaxResults int32  `json:"MaxResults"`
	}
	if len(body) > 0 {
		_ = json.Unmarshal(body, &req)
	}

	result := h.Backend.ListScheduledQueriesEnriched(ctx, req.NextToken, req.MaxResults)

	resp := map[string]any{
		"ScheduledQueries": result.Items,
	}
	if result.NextToken != "" {
		resp["NextToken"] = result.NextToken
	}

	return json.Marshal(resp)
}

func (h *Handler) handleUpdateScheduledQuery(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ScheduledQueryArn string `json:"ScheduledQueryArn"`
		State             string `json:"State"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	if req.ScheduledQueryArn == "" {
		return nil, fmt.Errorf("%w: ScheduledQueryArn is required", ErrValidation)
	}

	if req.State == "" {
		return nil, fmt.Errorf("%w: State is required", ErrValidation)
	}

	if err := h.Backend.UpdateScheduledQuery(ctx, req.ScheduledQueryArn, req.State); err != nil {
		return nil, err
	}

	return nil, nil
}

func (h *Handler) handleTagResource(ctx context.Context, body []byte) ([]byte, error) {
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
		return nil, fmt.Errorf("%w: ResourceARN is required", ErrValidation)
	}

	tags := make(map[string]string, len(req.Tags))

	for _, t := range req.Tags {
		tags[t.Key] = t.Value
	}

	if err := h.Backend.TagResource(ctx, req.ResourceARN, tags); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}

func (h *Handler) handleUntagResource(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ResourceARN string   `json:"ResourceARN"`
		TagKeys     []string `json:"TagKeys"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	if req.ResourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceARN is required", ErrValidation)
	}

	if err := h.Backend.UntagResource(ctx, req.ResourceARN, req.TagKeys); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}

func (h *Handler) handleListTagsForResource(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ResourceARN string `json:"ResourceARN"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	if req.ResourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceARN is required", ErrValidation)
	}

	tags, err := h.Backend.ListTagsForResource(ctx, req.ResourceARN)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"Tags": tags,
	})
}

func (h *Handler) handleDescribeAccountSettings(ctx context.Context) ([]byte, error) {
	settings := h.Backend.DescribeAccountSettings(ctx)

	return json.Marshal(buildAccountSettingsResponse(settings))
}

func buildAccountSettingsResponse(settings AccountSettings) map[string]any {
	resp := map[string]any{
		"QueryPricingModel": settings.QueryPricingModel,
	}
	if settings.MaxQueryTCU != nil {
		resp["MaxQueryTCU"] = *settings.MaxQueryTCU
	}
	if settings.LastUpdatedTime != nil {
		resp["LastUpdatedTime"] = settings.LastUpdatedTime.Unix()
	}
	if settings.QueryCompute != nil {
		resp["QueryCompute"] = settings.QueryCompute
	}

	return resp
}

func (h *Handler) handlePrepareQuery(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		QueryString  string `json:"QueryString"`
		ValidateOnly bool   `json:"ValidateOnly"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	if req.QueryString == "" {
		return nil, fmt.Errorf("%w: QueryString is required", ErrValidation)
	}

	result, err := h.Backend.PrepareQuery(ctx, req.QueryString, req.ValidateOnly)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"QueryString": result.QueryString,
		"Columns":     marshalColumnInfos(result.Columns),
		"Parameters":  marshalColumnInfos(result.Parameters),
	})
}

func (h *Handler) handleUpdateAccountSettings(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		MaxQueryTCU       *int32 `json:"MaxQueryTCU"`
		QueryPricingModel string `json:"QueryPricingModel"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	settings, err := h.Backend.UpdateAccountSettings(ctx, req.QueryPricingModel, req.MaxQueryTCU)
	if err != nil {
		return nil, err
	}

	return json.Marshal(buildAccountSettingsResponse(settings))
}

func (h *Handler) handleError(c *echo.Context, err error) error {
	c.Response().Header().Set("Content-Type", contentType)

	switch {
	case errors.Is(err, ErrNotFound):
		return c.JSONBlob(http.StatusBadRequest, errorPayload("ResourceNotFoundException", err.Error()))
	case errors.Is(err, ErrAlreadyExists):
		return c.JSONBlob(http.StatusConflict, errorPayload("ConflictException", err.Error()))
	case errors.Is(err, ErrValidation):
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
		keyArn:         sq.Arn,
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

	if summary := buildLastRunSummary(sq); summary != nil {
		view["LastRunSummary"] = summary
	}

	now := time.Now()
	view["NextInvocationTime"] = epochSeconds(nextInvocationTime(sq.ScheduleExpression, now))
	if !sq.LastRunTime.IsZero() {
		view["PreviousInvocationTime"] = epochSeconds(sq.LastRunTime)
	}

	if len(sq.Tags) > 0 {
		tagKeys := make([]string, 0, len(sq.Tags))
		for k := range sq.Tags {
			tagKeys = append(tagKeys, k)
		}

		sort.Strings(tagKeys)

		tagList := make([]map[string]string, 0, len(tagKeys))
		for _, k := range tagKeys {
			tagList = append(tagList, map[string]string{"Key": k, "Value": sq.Tags[k]})
		}

		view["Tags"] = tagList
	}

	return view
}

// epochSeconds converts a [time.Time] to Unix epoch seconds as float64.
func epochSeconds(t time.Time) float64 {
	return float64(t.Unix()) + float64(t.Nanosecond())/1e9
}
