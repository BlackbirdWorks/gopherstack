package cloudwatchlogs

import (
	"context"
	"encoding/json"
)

// --- PutMetricFilter ---.
type putMetricFilterInput struct {
	FilterPattern         string                 `json:"filterPattern"`
	FilterName            string                 `json:"filterName"`
	LogGroupName          string                 `json:"logGroupName"`
	MetricTransformations []MetricTransformation `json:"metricTransformations"`
}

type putMetricFilterOutput struct{}

// --- DescribeMetricFilters ---.
type describeMetricFiltersInput struct {
	FilterNamePrefix string `json:"filterNamePrefix"`
	LogGroupName     string `json:"logGroupName"`
	MetricName       string `json:"metricName"`
	MetricNamespace  string `json:"metricNamespace"`
	NextToken        string `json:"nextToken"`
	Limit            int    `json:"limit"`
}

type describeMetricFiltersOutput struct {
	NextToken     string         `json:"nextToken,omitempty"`
	MetricFilters []MetricFilter `json:"metricFilters"`
}

// --- DeleteMetricFilter ---.
type deleteMetricFilterInput struct {
	FilterName   string `json:"filterName"`
	LogGroupName string `json:"logGroupName"`
}

type deleteMetricFilterOutput struct{}

// --- TestMetricFilter ---.
type testMetricFilterInput struct {
	FilterPattern    string   `json:"filterPattern"`
	LogEventMessages []string `json:"logEventMessages"`
}

type testMetricFilterOutput struct {
	Matches []MetricFilterMatchRecord `json:"matches"`
}

func (h *Handler) handlePutMetricFilter(ctx context.Context, b []byte) (any, error) {
	var input putMetricFilterInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	if err := h.Backend.PutMetricFilter(
		ctx,
		input.LogGroupName,
		input.FilterName,
		input.FilterPattern,
		input.MetricTransformations,
	); err != nil {
		return nil, err
	}

	return &putMetricFilterOutput{}, nil
}

func (h *Handler) handleDescribeMetricFilters(ctx context.Context, b []byte) (any, error) {
	var input describeMetricFiltersInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	filters, next, err := h.Backend.DescribeMetricFilters(
		ctx,
		input.LogGroupName,
		input.FilterNamePrefix,
		input.MetricName,
		input.MetricNamespace,
		input.NextToken,
		input.Limit,
	)
	if err != nil {
		return nil, err
	}

	return &describeMetricFiltersOutput{MetricFilters: filters, NextToken: next}, nil
}

func (h *Handler) handleDeleteMetricFilter(ctx context.Context, b []byte) (any, error) {
	var input deleteMetricFilterInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	if err := h.Backend.DeleteMetricFilter(ctx, input.LogGroupName, input.FilterName); err != nil {
		return nil, err
	}

	return &deleteMetricFilterOutput{}, nil
}

func (h *Handler) handleTestMetricFilter(
	ctx context.Context, //nolint:revive // existing issue.
	b []byte,
) (any, error) {
	var input testMetricFilterInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	matches, err := h.Backend.TestMetricFilter(input.FilterPattern, input.LogEventMessages)
	if err != nil {
		return nil, err
	}

	return &testMetricFilterOutput{Matches: matches}, nil
}
