package timestreamquery

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
)

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
	ClientToken                    string `json:"ClientToken"`
	KmsKeyID                       string `json:"KmsKeyId"`
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
		targetDB, targetTable, req.ClientToken, req.KmsKeyID,
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

	if sq.KmsKeyID != "" {
		view["KmsKeyId"] = sq.KmsKeyID
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
		tagKeys := collections.SortedKeys(sq.Tags)

		tagList := make([]map[string]string, 0, len(tagKeys))
		for _, k := range tagKeys {
			tagList = append(tagList, map[string]string{"Key": k, "Value": sq.Tags[k]})
		}

		view["Tags"] = tagList
	}

	return view
}
