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

// replayListResponse is the handler-level DTO for ListReplays entries.
// Timestamps are float64 Unix epoch seconds as required by the AWS JSON
// protocol (a raw time.Time field would json.Marshal to an RFC3339 string
// instead). Matches real AWS's types.Replay shape used by ListReplaysOutput,
// which -- unlike DescribeReplayOutput -- has no Destination or Description
// member.
type replayListResponse struct {
	ReplayName            string  `json:"ReplayName"`
	EventSourceArn        string  `json:"EventSourceArn,omitempty"`
	State                 string  `json:"State"`
	StateReason           string  `json:"StateReason,omitempty"`
	EventEndTime          float64 `json:"EventEndTime,omitempty"`
	EventLastReplayedTime float64 `json:"EventLastReplayedTime,omitempty"`
	EventStartTime        float64 `json:"EventStartTime,omitempty"`
	ReplayEndTime         float64 `json:"ReplayEndTime,omitempty"`
	ReplayStartTime       float64 `json:"ReplayStartTime,omitempty"`
}

// describeReplayResponse is the handler-level DTO for DescribeReplay, which
// additionally echoes Destination and Description -- both real
// DescribeReplayOutput members absent from types.Replay/ListReplaysOutput.
type describeReplayResponse struct {
	Destination *ReplayDestination `json:"Destination,omitempty"`
	Description string             `json:"Description,omitempty"`
	replayListResponse
}

func replayToListResponse(r *Replay) replayListResponse {
	resp := replayListResponse{
		ReplayName:     r.ReplayName,
		EventSourceArn: r.EventSourceArn,
		State:          r.State,
		StateReason:    r.StateReason,
	}
	if !r.EventStartTime.IsZero() {
		resp.EventStartTime = timeToEpochSeconds(r.EventStartTime)
	}
	if !r.EventEndTime.IsZero() {
		resp.EventEndTime = timeToEpochSeconds(r.EventEndTime)
	}
	if !r.ReplayStartTime.IsZero() {
		resp.ReplayStartTime = timeToEpochSeconds(r.ReplayStartTime)
	}
	if !r.ReplayEndTime.IsZero() {
		resp.ReplayEndTime = timeToEpochSeconds(r.ReplayEndTime)
	}

	return resp
}

func replayToDescribeResponse(r *Replay) *describeReplayResponse {
	if r == nil {
		return nil
	}

	return &describeReplayResponse{
		replayListResponse: replayToListResponse(r),
		Description:        r.Description,
		Destination:        r.Destination,
	}
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
			replay, err := h.Backend.DescribeReplay(ctx, input.ReplayName)
			if err != nil {
				return nil, err
			}

			return replayToDescribeResponse(replay), nil
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

			responses := make([]replayListResponse, len(replays))
			for i := range replays {
				responses[i] = replayToListResponse(&replays[i])
			}

			return &struct {
				NextToken string               `json:"NextToken,omitempty"`
				Replays   []replayListResponse `json:"Replays"`
			}{Replays: responses, NextToken: next}, nil
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
