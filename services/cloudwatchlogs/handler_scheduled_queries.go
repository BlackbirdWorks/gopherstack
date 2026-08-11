package cloudwatchlogs

import (
	"context"
	"encoding/json"
)

type createScheduledQueryInput struct {
	DestinationConfiguration *ScheduledQueryDestinationConfig `json:"destinationConfiguration"`
	Tags                     map[string]string                `json:"tags"`
	ScheduleExpression       string                           `json:"scheduleExpression"`
	QueryLanguage            string                           `json:"queryLanguage"`
	Name                     string                           `json:"name"`
	ExecutionRoleArn         string                           `json:"executionRoleArn"`
	State                    string                           `json:"state"`
	Description              string                           `json:"description"`
	Timezone                 string                           `json:"timezone"`
	QueryString              string                           `json:"queryString"`
	LogGroupIdentifiers      []string                         `json:"logGroupIdentifiers"`
	EndTimeOffset            int64                            `json:"endTimeOffset"`
	StartTimeOffset          int64                            `json:"startTimeOffset"`
	ScheduleStartTime        int64                            `json:"scheduleStartTime"`
	ScheduleEndTime          int64                            `json:"scheduleEndTime"`
}

type createScheduledQueryOutput struct {
	ScheduledQueryArn string `json:"scheduledQueryArn,omitempty"`
	State             string `json:"state,omitempty"`
}

// --- DeleteScheduledQuery ---.
type deleteScheduledQueryInput struct {
	ScheduledQueryArn string `json:"scheduledQueryArn"`
}

type deleteScheduledQueryOutput struct{}

// --- ListScheduledQueries ---.
type listScheduledQueriesInput struct {
	NextToken  string `json:"nextToken"`
	MaxResults int    `json:"maxResults"`
}

type listScheduledQueriesOutput struct {
	NextToken        string           `json:"nextToken,omitempty"`
	ScheduledQueries []map[string]any `json:"scheduledQueries"`
}

// scheduledQuerySummaryToWire renders a ScheduledQuery in
// ListScheduledQueriesOutput's real ScheduledQuerySummary shape, which is
// narrower than GetScheduledQueryOutput's: no queryString/executionRoleArn/
// queryLanguage/logGroupIdentifiers/description/endTimeOffset/
// startTimeOffset/scheduleStartTime/scheduleEndTime (confirmed via
// types.ScheduledQuerySummary). A previous revision reused the full
// ScheduledQuery shape for List, over-sharing fields real
// ListScheduledQueries never returns.
func scheduledQuerySummaryToWire(sq *ScheduledQuery) map[string]any {
	entry := map[string]any{
		keyScheduledQueryArn: sq.ScheduledQueryArn,
		keyName:              sq.Name,
		keyState:             sq.State,
		keyCreationTime:      sq.CreationTime,
	}

	if sq.ScheduleExpression != "" {
		entry["scheduleExpression"] = sq.ScheduleExpression
	}

	if sq.ScheduleType != "" {
		entry["scheduleType"] = sq.ScheduleType
	}

	if sq.LastExecutionStatus != "" {
		entry["lastExecutionStatus"] = sq.LastExecutionStatus
	}

	if sq.LastTriggeredTime != 0 {
		entry["lastTriggeredTime"] = sq.LastTriggeredTime
	}

	if sq.LastUpdatedTime != 0 {
		entry["lastUpdatedTime"] = sq.LastUpdatedTime
	}

	if sq.Timezone != "" {
		entry["timezone"] = sq.Timezone
	}

	if sq.DestinationConfiguration != nil {
		entry["destinationConfiguration"] = sq.DestinationConfiguration
	}

	return entry
}

// --- UpdateScheduledQuery ---.
type updateScheduledQueryInput struct {
	ScheduledQueryArn string `json:"scheduledQueryArn"`
	State             string `json:"state"`
}

type updateScheduledQueryOutput struct{}

// --- GetScheduledQuery ---.
type getScheduledQueryInput struct {
	ScheduledQueryArn string `json:"scheduledQueryArn"`
}

// --- GetScheduledQueryHistory ---.
type getScheduledQueryHistoryInput struct {
	ScheduledQueryArn string `json:"scheduledQueryArn"`
	NextToken         string `json:"nextToken"`
	MaxResults        int    `json:"maxResults"`
}

type getScheduledQueryHistoryOutput struct {
	NextToken                  string                     `json:"nextToken,omitempty"`
	ScheduledQueryRunSummaries []ScheduledQueryRunSummary `json:"scheduledQueryRunSummaries"`
}

func (h *Handler) handleCreateScheduledQuery(
	ctx context.Context, //nolint:revive // existing issue.
	b []byte,
) (any, error) {
	var input createScheduledQueryInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	queryArn, err := h.Backend.CreateScheduledQuery(ScheduledQueryCreateParams{
		DestinationConfiguration: input.DestinationConfiguration,
		ScheduleExpression:       input.ScheduleExpression,
		QueryLanguage:            input.QueryLanguage,
		Name:                     input.Name,
		ExecutionRoleArn:         input.ExecutionRoleArn,
		State:                    input.State,
		Description:              input.Description,
		Timezone:                 input.Timezone,
		QueryString:              input.QueryString,
		LogGroupIdentifiers:      input.LogGroupIdentifiers,
		EndTimeOffset:            input.EndTimeOffset,
		StartTimeOffset:          input.StartTimeOffset,
		ScheduleStartTime:        input.ScheduleStartTime,
		ScheduleEndTime:          input.ScheduleEndTime,
	})
	if err != nil {
		return nil, err
	}

	if len(input.Tags) > 0 {
		h.setTags(queryArn, input.Tags)
	}

	// The backend defaults an empty state to statusEnabled; reflect the effective value in the response.
	effectiveState := input.State
	if effectiveState == "" {
		effectiveState = statusEnabled
	}

	return &createScheduledQueryOutput{ScheduledQueryArn: queryArn, State: effectiveState}, nil
}

func (h *Handler) handleDeleteScheduledQuery(
	ctx context.Context, //nolint:revive // existing issue.
	b []byte,
) (any, error) {
	var input deleteScheduledQueryInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	if err := h.Backend.DeleteScheduledQuery(input.ScheduledQueryArn); err != nil {
		return nil, err
	}

	return &deleteScheduledQueryOutput{}, nil
}

func (h *Handler) handleListScheduledQueries(
	ctx context.Context, //nolint:revive // existing issue.
	b []byte,
) (any, error) {
	var input listScheduledQueriesInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	queries, next, err := h.Backend.ListScheduledQueries(input.MaxResults, input.NextToken)
	if err != nil {
		return nil, err
	}

	wire := make([]map[string]any, 0, len(queries))
	for i := range queries {
		wire = append(wire, scheduledQuerySummaryToWire(&queries[i]))
	}

	return &listScheduledQueriesOutput{ScheduledQueries: wire, NextToken: next}, nil
}

func (h *Handler) handleUpdateScheduledQuery(
	ctx context.Context, //nolint:revive // existing issue.
	b []byte,
) (any, error) {
	var input updateScheduledQueryInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	if err := h.Backend.UpdateScheduledQuery(input.ScheduledQueryArn, input.State); err != nil {
		return nil, err
	}

	return &updateScheduledQueryOutput{}, nil
}

func (h *Handler) handleGetScheduledQuery(
	ctx context.Context, //nolint:revive // existing issue.
	b []byte,
) (any, error) {
	var input getScheduledQueryInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	sq, err := h.Backend.GetScheduledQuery(input.ScheduledQueryArn)
	if err != nil {
		return nil, err
	}

	// Real GetScheduledQueryOutput's members (creationTime, description,
	// destinationConfiguration, ...) sit flat at the top level of the response
	// -- there is no "scheduledQuery" wrapper object (confirmed via
	// deserializers.go's awsAwsjson11_deserializeOpDocumentGetScheduledQueryOutput).
	// A previous revision wrapped the response under a "scheduledQuery" key, so
	// a real SDK client's GetScheduledQueryOutput fields were never populated.
	return sq, nil
}

func (h *Handler) handleGetScheduledQueryHistory(
	ctx context.Context, //nolint:revive // existing issue.
	b []byte,
) (any, error) {
	var input getScheduledQueryHistoryInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	summaries, next, err := h.Backend.GetScheduledQueryHistory(
		input.ScheduledQueryArn,
		input.NextToken,
		input.MaxResults,
	)
	if err != nil {
		return nil, err
	}

	return &getScheduledQueryHistoryOutput{ScheduledQueryRunSummaries: summaries, NextToken: next}, nil
}
