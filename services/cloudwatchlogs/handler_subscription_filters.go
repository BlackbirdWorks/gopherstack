package cloudwatchlogs

import (
	"context"
	"encoding/json"
)

type putSubscriptionFilterInput struct {
	FilterPattern  string `json:"filterPattern"`
	FilterName     string `json:"filterName"`
	LogGroupName   string `json:"logGroupName"`
	DestinationArn string `json:"destinationArn"`
	RoleArn        string `json:"roleArn,omitempty"`
	Distribution   string `json:"distribution,omitempty"`
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

type putSubscriptionFilterOutput struct{}

type describeSubscriptionFiltersOutput struct {
	NextToken           string               `json:"nextToken,omitempty"`
	SubscriptionFilters []SubscriptionFilter `json:"subscriptionFilters"`
}

type deleteSubscriptionFilterOutput struct{}

func (h *Handler) subscriptionFilterActions() map[string]actionFn {
	return map[string]actionFn{
		"PutSubscriptionFilter": func(ctx context.Context, b []byte) (any, error) {
			var input putSubscriptionFilterInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			if err := h.Backend.PutSubscriptionFilter(
				ctx,
				input.LogGroupName, input.FilterName, input.FilterPattern, input.DestinationArn,
				input.RoleArn, input.Distribution,
			); err != nil {
				return nil, err
			}

			return &putSubscriptionFilterOutput{}, nil
		},
		"DescribeSubscriptionFilters": func(ctx context.Context, b []byte) (any, error) {
			var input describeSubscriptionFiltersInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			filters, next, err := h.Backend.DescribeSubscriptionFilters(
				ctx,
				input.LogGroupName, input.FilterNamePrefix, input.NextToken, input.Limit,
			)
			if err != nil {
				return nil, err
			}

			return &describeSubscriptionFiltersOutput{SubscriptionFilters: filters, NextToken: next}, nil
		},
		"DeleteSubscriptionFilter": func(ctx context.Context, b []byte) (any, error) {
			var input deleteSubscriptionFilterInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			if err := h.Backend.DeleteSubscriptionFilter(ctx, input.LogGroupName, input.FilterName); err != nil {
				return nil, err
			}

			return &deleteSubscriptionFilterOutput{}, nil
		},
	}
}
