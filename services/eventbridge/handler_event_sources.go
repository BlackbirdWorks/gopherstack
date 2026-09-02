package eventbridge

import (
	"context"
	"encoding/json"
)

type (
	activateEventSourceOutput   struct{}
	deactivateEventSourceOutput struct{}
)

func (h *Handler) eventSourceActions() map[string]actionFn {
	return map[string]actionFn{
		"ActivateEventSource": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				Name string `json:"Name"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			if err := h.Backend.ActivateEventSource(ctx, input.Name); err != nil {
				return nil, err
			}

			return &activateEventSourceOutput{}, nil
		},
		"DeactivateEventSource": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				Name string `json:"Name"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			if err := h.Backend.DeactivateEventSource(ctx, input.Name); err != nil {
				return nil, err
			}

			return &deactivateEventSourceOutput{}, nil
		},
	}
}

// eventSourceResponse is the handler-level DTO for EventSource. Timestamps
// are float64 Unix epoch seconds as required by the AWS JSON protocol (a raw
// time.Time field would json.Marshal to an RFC3339 string instead). Matches
// real AWS's types.EventSource shape, also used by DescribeEventSourceOutput.
type eventSourceResponse struct {
	Arn            string  `json:"Arn,omitempty"`
	CreatedBy      string  `json:"CreatedBy,omitempty"`
	Name           string  `json:"Name,omitempty"`
	State          string  `json:"State,omitempty"`
	CreationTime   float64 `json:"CreationTime,omitempty"`
	ExpirationTime float64 `json:"ExpirationTime,omitempty"`
}

func eventSourceToResponse(src *EventSource) *eventSourceResponse {
	if src == nil {
		return nil
	}

	resp := &eventSourceResponse{
		Arn:       src.Arn,
		CreatedBy: src.CreatedBy,
		Name:      src.Name,
		State:     src.State,
	}

	if !src.CreationTime.IsZero() {
		resp.CreationTime = timeToEpochSeconds(src.CreationTime)
	}

	if !src.ExpirationTime.IsZero() {
		resp.ExpirationTime = timeToEpochSeconds(src.ExpirationTime)
	}

	return resp
}

// extendedEventSourceActions returns Describe/List for event sources.
func (h *Handler) extendedEventSourceActions() map[string]actionFn {
	return map[string]actionFn{
		"DescribeEventSource": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				Name string `json:"Name"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			src, err := h.Backend.DescribeEventSource(ctx, input.Name)
			if err != nil {
				return nil, err
			}

			return eventSourceToResponse(src), nil
		},
		"ListEventSources": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				NamePrefix string `json:"NamePrefix"`
				NextToken  string `json:"NextToken"`
				Limit      int32  `json:"Limit"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			srcs, next, err := h.Backend.ListEventSources(ctx, input.NamePrefix, input.NextToken, int(input.Limit))
			if err != nil {
				return nil, err
			}

			responses := make([]eventSourceResponse, len(srcs))
			for i := range srcs {
				responses[i] = *eventSourceToResponse(&srcs[i])
			}

			return &struct {
				NextToken    string                `json:"NextToken,omitempty"`
				EventSources []eventSourceResponse `json:"EventSources"`
			}{EventSources: responses, NextToken: next}, nil
		},
	}
}
