package stepfunctions

import (
	"encoding/json"
)

type describeMapRunInput struct {
	MapRunArn string `json:"mapRunArn"`
}

type listMapRunsInput struct {
	ExecutionArn string `json:"executionArn"`
	NextToken    string `json:"nextToken"`
	MaxResults   int    `json:"maxResults"`
}

type updateMapRunInput struct {
	MapRunArn                  string  `json:"mapRunArn"`
	MaxConcurrency             int     `json:"maxConcurrency,omitempty"`
	ToleratedFailureCount      int     `json:"toleratedFailureCount,omitempty"`
	ToleratedFailurePercentage float64 `json:"toleratedFailurePercentage,omitempty"`
}

type listMapRunsOutput struct {
	NextToken string   `json:"nextToken,omitempty"`
	MapRuns   []MapRun `json:"mapRuns"`
}

// mapRunActions returns handler functions for Map Run operations.
func (h *Handler) mapRunActions() map[string]actionFn {
	return map[string]actionFn{
		"DescribeMapRun": func(b []byte) (any, error) {
			var input describeMapRunInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeMapRun(input.MapRunArn)
		},
		"ListMapRuns": func(b []byte) (any, error) {
			var input listMapRunsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			runs, next, err := h.Backend.ListMapRuns(
				input.ExecutionArn,
				input.NextToken,
				input.MaxResults,
			)
			if err != nil {
				return nil, err
			}

			return &listMapRunsOutput{NextToken: next, MapRuns: runs}, nil
		},
		"TestState": func(body []byte) (any, error) {
			return h.handleTestState(body)
		},
		"UpdateMapRun": func(b []byte) (any, error) {
			var input updateMapRunInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.UpdateMapRun(
				input.MapRunArn,
				input.MaxConcurrency,
				input.ToleratedFailureCount,
				input.ToleratedFailurePercentage,
			)
		},
	}
}
