package eventbridge

import (
	"context"
	"encoding/json"
	"fmt"
)

type createPartnerEventSourceOutput struct {
	EventSourceArn string `json:"EventSourceArn"`
}

// partnerEventSourceAccountResponse is the handler-level DTO for
// PartnerEventSourceAccountInfo. Timestamps are float64 Unix epoch seconds
// as required by the AWS JSON protocol.
type partnerEventSourceAccountResponse struct {
	Account        string  `json:"Account,omitempty"`
	State          string  `json:"State,omitempty"`
	CreationTime   float64 `json:"CreationTime,omitempty"`
	ExpirationTime float64 `json:"ExpirationTime,omitempty"`
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
				Limit      int32  `json:"Limit"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			srcs, next, err := h.Backend.ListPartnerEventSources(
				ctx,
				input.NamePrefix,
				input.NextToken,
				int(input.Limit),
			)
			if err != nil {
				return nil, err
			}

			return &struct {
				NextToken           string               `json:"NextToken,omitempty"`
				PartnerEventSources []PartnerEventSource `json:"PartnerEventSources"`
			}{PartnerEventSources: srcs, NextToken: next}, nil
		},
		"ListPartnerEventSourceAccounts": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				EventSourceName string `json:"EventSourceName"`
				NextToken       string `json:"NextToken"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			if input.EventSourceName == "" {
				return nil, fmt.Errorf("%w: EventSourceName is required", ErrInvalidParameter)
			}

			accounts, err := h.Backend.ListPartnerEventSourceAccounts(ctx, input.EventSourceName)
			if err != nil {
				return nil, err
			}

			out := make([]partnerEventSourceAccountResponse, 0, len(accounts))
			for _, a := range accounts {
				out = append(out, partnerEventSourceAccountResponse{
					Account:        a.Account,
					CreationTime:   timeToEpochSeconds(a.CreationTime),
					ExpirationTime: timeToEpochSeconds(a.ExpirationTime),
					State:          a.State,
				})
			}

			return &struct {
				PartnerEventSourceAccounts []partnerEventSourceAccountResponse `json:"PartnerEventSourceAccounts"`
			}{PartnerEventSourceAccounts: out}, nil
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
