package eventbridge

import (
	"context"
	"encoding/json"
)

type createEventBusInput struct {
	Tags        map[string]string `json:"Tags,omitempty"`
	Name        string            `json:"Name"`
	Description string            `json:"Description"`
}

type deleteEventBusInput struct {
	Name string `json:"Name"`
}

type listEventBusesInput struct {
	NamePrefix string `json:"NamePrefix"`
	NextToken  string `json:"NextToken"`
	Limit      int    `json:"Limit"`
}

type describeEventBusInput struct {
	Name string `json:"Name"`
}

type createEventBusOutput struct {
	EventBusArn string `json:"EventBusArn"`
}

type deleteEventBusOutput struct{}

type listEventBusesOutput struct {
	NextToken  string     `json:"NextToken,omitempty"`
	EventBuses []EventBus `json:"EventBuses"`
}

func (h *Handler) eventBusActions() map[string]actionFn {
	return map[string]actionFn{
		"CreateEventBus": func(ctx context.Context, b []byte) (any, error) {
			var input createEventBusInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			bus, err := h.Backend.CreateEventBus(ctx, input.Name, input.Description)
			if err != nil {
				return nil, err
			}
			if len(input.Tags) > 0 {
				h.setTags(bus.Arn, input.Tags)
			}

			return &createEventBusOutput{EventBusArn: bus.Arn}, nil
		},
		"DeleteEventBus": func(ctx context.Context, b []byte) (any, error) {
			var input deleteEventBusInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			// Capture ARN before deletion so we can clean up tags.
			bus, _ := h.Backend.DescribeEventBus(ctx, input.Name)
			if err := h.Backend.DeleteEventBus(ctx, input.Name); err != nil {
				return nil, err
			}
			if bus != nil {
				h.clearResourceTags(bus.Arn)
			}

			return &deleteEventBusOutput{}, nil
		},
		"ListEventBuses": func(ctx context.Context, b []byte) (any, error) {
			var input listEventBusesInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			buses, next, err := h.Backend.ListEventBuses(ctx, input.NamePrefix, input.NextToken, input.Limit)
			if err != nil {
				return nil, err
			}

			return &listEventBusesOutput{EventBuses: buses, NextToken: next}, nil
		},
		"DescribeEventBus": func(ctx context.Context, b []byte) (any, error) {
			var input describeEventBusInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			bus, err := h.Backend.DescribeEventBus(ctx, input.Name)
			if err != nil {
				return nil, err
			}

			return bus, nil
		},
	}
}

// eventBusManagementActions returns the UpdateEventBus, PutPermission and
// RemovePermission actions (event bus mutation ops not covered by eventBusActions
// or policyActions).
func (h *Handler) eventBusManagementActions() map[string]actionFn {
	return map[string]actionFn{
		"UpdateEventBus": func(ctx context.Context, b []byte) (any, error) {
			var input UpdateEventBusInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			bus, err := h.Backend.UpdateEventBus(ctx, input)
			if err != nil {
				return nil, err
			}

			return &struct {
				Arn         string `json:"Arn"`
				Description string `json:"Description,omitempty"`
				Name        string `json:"Name"`
			}{Arn: bus.Arn, Description: bus.Description, Name: bus.Name}, nil
		},
		"PutPermission": func(ctx context.Context, b []byte) (any, error) {
			var input PutPermissionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return &struct{}{}, h.Backend.PutPermission(ctx, input)
		},
		"RemovePermission": func(ctx context.Context, b []byte) (any, error) {
			var input RemovePermissionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return &struct{}{}, h.Backend.RemovePermission(ctx, input)
		},
	}
}

func (h *Handler) policyActions() map[string]actionFn {
	return map[string]actionFn{
		"GetEventBusPolicy": func(ctx context.Context, b []byte) (any, error) {
			var input GetEventBusPolicyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			policy, err := h.Backend.GetEventBusPolicy(ctx, input.EventBusName)
			if err != nil {
				return nil, err
			}

			return &struct {
				Policy string `json:"Policy,omitempty"`
			}{Policy: policy}, nil
		},
		"PutEventBusPolicy": func(ctx context.Context, b []byte) (any, error) {
			var input PutEventBusPolicyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return &struct{}{}, h.Backend.PutEventBusPolicy(ctx, input)
		},
	}
}
