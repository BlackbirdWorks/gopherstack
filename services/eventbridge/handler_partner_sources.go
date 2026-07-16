package eventbridge

import (
	"context"
	"encoding/json"
)

type createPartnerEventSourceOutput struct {
	EventSourceArn string `json:"EventSourceArn"`
}

// partnerSourceActions returns the CreatePartnerEventSource action.
func (h *Handler) partnerSourceActions() map[string]actionFn {
	return map[string]actionFn{
		"CreatePartnerEventSource": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				Name    string `json:"Name"`
				Account string `json:"Account"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			src, err := h.Backend.CreatePartnerEventSource(ctx, input.Name, input.Account)
			if err != nil {
				return nil, err
			}

			return &createPartnerEventSourceOutput{EventSourceArn: src.Arn}, nil
		},
	}
}

// extendedPartnerSourceActions returns CRUD actions for partner event sources beyond Create.
func (h *Handler) extendedPartnerSourceActions() map[string]actionFn {
	return map[string]actionFn{
		"DeletePartnerEventSource": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				Name string `json:"Name"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return &struct{}{}, h.Backend.DeletePartnerEventSource(ctx, input.Name)
		},
		"DescribePartnerEventSource": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				Name string `json:"Name"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribePartnerEventSource(ctx, input.Name)
		},
		"ListPartnerEventSources": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				NamePrefix string `json:"NamePrefix"`
				NextToken  string `json:"NextToken"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			srcs, next, err := h.Backend.ListPartnerEventSources(ctx, input.NamePrefix, input.NextToken)
			if err != nil {
				return nil, err
			}

			return &struct {
				NextToken           string               `json:"NextToken,omitempty"`
				PartnerEventSources []PartnerEventSource `json:"PartnerEventSources"`
			}{PartnerEventSources: srcs, NextToken: next}, nil
		},
		"ListPartnerEventSourceAccounts": func(_ context.Context, _ []byte) (any, error) {
			// ListPartnerEventSourceAccounts returns accounts that have been
			// granted access to a partner event source. Cross-account metadata
			// has no meaningful in-process simulation; return empty list.
			return &struct {
				NextToken                  string `json:"NextToken,omitempty"`
				PartnerEventSourceAccounts []any  `json:"PartnerEventSourceAccounts"`
			}{PartnerEventSourceAccounts: []any{}}, nil
		},
		"PutPartnerEvents": func(ctx context.Context, b []byte) (any, error) {
			var input putEventsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			entries, err := h.Backend.PutPartnerEvents(ctx, input.Entries)
			if err != nil {
				return nil, err
			}

			return &putEventsOutput{
				FailedEntryCount: countFailedEntries(entries),
				Entries:          entries,
			}, nil
		},
	}
}
