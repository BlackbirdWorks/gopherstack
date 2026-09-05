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
	QueryID   string `json:"queryId"`
	NextToken string `json:"nextToken"`
	MaxItems  int    `json:"maxItems"`
}

type getQueryResultsOutput struct {
	Status     QueryStatus     `json:"status"`
	NextToken  string          `json:"nextToken,omitempty"`
	Results    [][]ResultField `json:"results"`
	Statistics QueryStatistics `json:"statistics"`
}

// maxGetQueryResultsItems is GetQueryResultsInput.MaxItems' documented
// per-request maximum, also used as the default when the caller omits it
// (api_op_GetQueryResults.go:64: "The maximum is 10,000 log events per
// request").
const maxGetQueryResultsItems = 10_000

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
	QueryID    string `json:"queryId"`
	NextToken  string `json:"nextToken"`
	MaxResults int    `json:"maxResults"`
}

type listLogGroupsForQueryOutput struct {
	NextToken           string   `json:"nextToken,omitempty"`
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

	maxItems := input.MaxItems
	if maxItems <= 0 || maxItems > maxGetQueryResultsItems {
		maxItems = maxGetQueryResultsItems
	}

	startIdx := parseNextToken(input.NextToken)
	if startIdx >= len(results) {
		return &getQueryResultsOutput{Results: [][]ResultField{}, Statistics: stats, Status: status}, nil
	}

	end := startIdx + maxItems

	var outToken string
	if end < len(results) {
		outToken = encodeNextToken(end)
	} else {
		end = len(results)
	}

	return &getQueryResultsOutput{
		Results:    results[startIdx:end],
		Statistics: stats,
		Status:     status,
		NextToken:  outToken,
	}, nil
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

	limit := input.MaxResults
	if limit <= 0 {
		limit = defaultDescribeLimit
	}

	startIdx := parseNextToken(input.NextToken)
	if startIdx >= len(groups) {
		return &listLogGroupsForQueryOutput{LogGroupIdentifiers: []string{}}, nil
	}

	end := startIdx + limit

	var outToken string
	if end < len(groups) {
		outToken = encodeNextToken(end)
	} else {
		end = len(groups)
	}

	return &listLogGroupsForQueryOutput{
		LogGroupIdentifiers: groups[startIdx:end],
		NextToken:           outToken,
	}, nil
}
