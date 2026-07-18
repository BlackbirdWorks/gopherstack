package cloudwatchlogs

import (
	"context"
	"encoding/json"
)

type createLogStreamInput struct {
	LogGroupName  string `json:"logGroupName"`
	LogStreamName string `json:"logStreamName"`
}

type describeLogStreamsInput struct {
	LogGroupName        string `json:"logGroupName"`
	LogStreamNamePrefix string `json:"logStreamNamePrefix"`
	NextToken           string `json:"nextToken"`
	OrderBy             string `json:"orderBy"`
	Limit               int    `json:"limit"`
	Descending          bool   `json:"descending"`
}

type deleteLogStreamInput struct {
	LogGroupName  string `json:"logGroupName"`
	LogStreamName string `json:"logStreamName"`
}

type createLogStreamOutput struct{}

type deleteLogStreamOutput struct{}

type describeLogStreamsOutput struct {
	NextToken  string      `json:"nextToken,omitempty"`
	LogStreams []LogStream `json:"logStreams"`
}

func (h *Handler) logStreamActions() map[string]actionFn {
	return map[string]actionFn{
		"CreateLogStream": func(ctx context.Context, b []byte) (any, error) {
			var input createLogStreamInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			if _, err := h.Backend.CreateLogStream(ctx, input.LogGroupName, input.LogStreamName); err != nil {
				return nil, err
			}

			return &createLogStreamOutput{}, nil
		},
		"DeleteLogStream": func(ctx context.Context, b []byte) (any, error) {
			var input deleteLogStreamInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			if err := h.Backend.DeleteLogStream(ctx, input.LogGroupName, input.LogStreamName); err != nil {
				return nil, err
			}

			return &deleteLogStreamOutput{}, nil
		},
		"DescribeLogStreams": func(ctx context.Context, b []byte) (any, error) {
			var input describeLogStreamsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			streams, next, err := h.Backend.DescribeLogStreams(
				ctx,
				input.LogGroupName,
				input.LogStreamNamePrefix,
				input.NextToken,
				input.OrderBy,
				input.Descending,
				input.Limit,
			)
			if err != nil {
				return nil, err
			}

			return &describeLogStreamsOutput{LogStreams: streams, NextToken: next}, nil
		},
	}
}
