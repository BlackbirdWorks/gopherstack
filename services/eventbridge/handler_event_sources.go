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

			return h.Backend.DescribeEventSource(ctx, input.Name)
		},
		"ListEventSources": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				NamePrefix string `json:"NamePrefix"`
				NextToken  string `json:"NextToken"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			srcs, next, err := h.Backend.ListEventSources(ctx, input.NamePrefix, input.NextToken)
			if err != nil {
				return nil, err
			}

			return &struct {
				NextToken    string        `json:"NextToken,omitempty"`
				EventSources []EventSource `json:"EventSources"`
			}{EventSources: srcs, NextToken: next}, nil
		},
	}
}
