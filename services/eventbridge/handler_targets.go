package eventbridge

import (
	"context"
	"encoding/json"
)

type putTargetsInput struct {
	Rule         string   `json:"Rule"`
	EventBusName string   `json:"EventBusName"`
	Targets      []Target `json:"Targets"`
}

type removeTargetsInput struct {
	Rule         string   `json:"Rule"`
	EventBusName string   `json:"EventBusName"`
	IDs          []string `json:"Ids"`
}

type listTargetsByRuleInput struct {
	Rule         string `json:"Rule"`
	EventBusName string `json:"EventBusName"`
	NextToken    string `json:"NextToken"`
	Limit        int    `json:"Limit"`
}

type putTargetsOutput struct {
	FailedEntries    []FailedEntry `json:"FailedEntries"`
	FailedEntryCount int           `json:"FailedEntryCount"`
}

type removeTargetsOutput struct {
	FailedEntries    []FailedEntry `json:"FailedEntries"`
	FailedEntryCount int           `json:"FailedEntryCount"`
}

type listTargetsByRuleOutput struct {
	NextToken string   `json:"NextToken,omitempty"`
	Targets   []Target `json:"Targets"`
}

func (h *Handler) targetActions() map[string]actionFn {
	return map[string]actionFn{
		"PutTargets": func(ctx context.Context, b []byte) (any, error) {
			var input putTargetsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			failed, err := h.Backend.PutTargets(ctx, input.Rule, input.EventBusName, input.Targets)
			if err != nil {
				return nil, err
			}
			if failed == nil {
				failed = []FailedEntry{}
			}

			return &putTargetsOutput{
				FailedEntryCount: len(failed),
				FailedEntries:    failed,
			}, nil
		},
		"RemoveTargets": func(ctx context.Context, b []byte) (any, error) {
			var input removeTargetsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			failed, err := h.Backend.RemoveTargets(ctx, input.Rule, input.EventBusName, input.IDs)
			if err != nil {
				return nil, err
			}
			if failed == nil {
				failed = []FailedEntry{}
			}

			return &removeTargetsOutput{
				FailedEntryCount: len(failed),
				FailedEntries:    failed,
			}, nil
		},
		"ListTargetsByRule": func(ctx context.Context, b []byte) (any, error) {
			var input listTargetsByRuleInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			targets, next, err := h.Backend.ListTargetsByRule(
				ctx,
				input.Rule,
				input.EventBusName,
				input.NextToken,
				input.Limit,
			)
			if err != nil {
				return nil, err
			}

			return &listTargetsByRuleOutput{Targets: targets, NextToken: next}, nil
		},
	}
}
