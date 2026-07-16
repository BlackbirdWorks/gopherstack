package eventbridge

import (
	"context"
	"encoding/json"
)

type cancelReplayOutput struct {
	ReplayArn   string `json:"ReplayArn"`
	State       string `json:"State"`
	StateReason string `json:"StateReason,omitempty"`
}

// replayActions returns the CancelReplay action.
func (h *Handler) replayActions() map[string]actionFn {
	return map[string]actionFn{
		"CancelReplay": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				ReplayName string `json:"ReplayName"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			replay, err := h.Backend.CancelReplay(ctx, input.ReplayName)
			if err != nil {
				return nil, err
			}

			return &cancelReplayOutput{
				ReplayArn:   replay.ReplayArn,
				State:       replay.State,
				StateReason: replay.StateReason,
			}, nil
		},
	}
}

// extendedReplayActions returns Describe/List/Start replay actions.
func (h *Handler) extendedReplayActions() map[string]actionFn {
	return map[string]actionFn{
		"DescribeReplay": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				ReplayName string `json:"ReplayName"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeReplay(ctx, input.ReplayName)
		},
		"ListReplays": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				NamePrefix string `json:"NamePrefix"`
				NextToken  string `json:"NextToken"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			replays, next, err := h.Backend.ListReplays(ctx, input.NamePrefix, input.NextToken)
			if err != nil {
				return nil, err
			}

			return &struct {
				NextToken string   `json:"NextToken,omitempty"`
				Replays   []Replay `json:"Replays"`
			}{Replays: replays, NextToken: next}, nil
		},
		"StartReplay": func(ctx context.Context, b []byte) (any, error) {
			var input StartReplayInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			replay, err := h.Backend.StartReplay(ctx, input)
			if err != nil {
				return nil, err
			}

			return &struct {
				ReplayArn       string  `json:"ReplayArn"`
				State           string  `json:"State"`
				StateReason     string  `json:"StateReason,omitempty"`
				ReplayStartTime float64 `json:"ReplayStartTime"`
			}{
				ReplayArn:       replay.ReplayArn,
				ReplayStartTime: timeToEpochSeconds(replay.ReplayStartTime),
				State:           replay.State,
				StateReason:     replay.StateReason,
			}, nil
		},
	}
}
