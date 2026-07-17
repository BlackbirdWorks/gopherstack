package cloudwatchlogs

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"
)

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

// --- ListLogGroupsForQuery ---.
type listLogGroupsForQueryInput struct {
	QueryID string `json:"queryId"`
}

type listLogGroupsForQueryOutput struct {
	LogGroupIdentifiers []string `json:"logGroupIdentifiers"`
}

func (h *Handler) handleStartQuery(ctx context.Context, b []byte) (any, error) {
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
		ctx,
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

func (h *Handler) handleGetQueryResults(ctx context.Context, b []byte) (any, error) { //nolint:revive // existing issue.
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

func (h *Handler) handleStopQuery(ctx context.Context, b []byte) (any, error) { //nolint:revive // existing issue.
	var input stopQueryInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	if err := h.Backend.StopQuery(input.QueryID); err != nil {
		return nil, err
	}

	return &stopQueryOutput{Success: true}, nil
}

func (h *Handler) handleDescribeQueries(ctx context.Context, b []byte) (any, error) { //nolint:revive // existing issue.
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

func (h *Handler) handleListLogGroupsForQuery(
	ctx context.Context, //nolint:revive // existing issue.
	b []byte,
) (any, error) {
	var input listLogGroupsForQueryInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	groups, err := h.Backend.ListLogGroupsForQuery(input.QueryID)
	if err != nil {
		return nil, err
	}

	return &listLogGroupsForQueryOutput{LogGroupIdentifiers: groups}, nil
}
