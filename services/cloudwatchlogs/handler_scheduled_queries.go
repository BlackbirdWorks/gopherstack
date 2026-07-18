package cloudwatchlogs

import (
	"context"
	"encoding/json"
)

type createScheduledQueryInput struct {
	Name               string `json:"name"`
	QueryString        string `json:"queryString"`
	ScheduleExpression string `json:"scheduleExpression"`
	ExecutionRoleArn   string `json:"executionRoleArn"`
	State              string `json:"state"`
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
	ScheduledQueries []ScheduledQuery `json:"scheduledQueries"`
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

type getScheduledQueryOutput struct {
	ScheduledQuery *ScheduledQuery `json:"scheduledQuery,omitempty"`
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

	queryArn, err := h.Backend.CreateScheduledQuery(
		input.Name, input.QueryString, input.ScheduleExpression, input.ExecutionRoleArn, input.State,
	)
	if err != nil {
		return nil, err
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

	return &listScheduledQueriesOutput{ScheduledQueries: queries, NextToken: next}, nil
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

	return &getScheduledQueryOutput{ScheduledQuery: sq}, nil
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
