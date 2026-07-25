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
	Description string `json:"Description,omitempty"`
}

type deleteEventBusOutput struct{}

type listEventBusesOutput struct {
	NextToken  string             `json:"NextToken,omitempty"`
	EventBuses []eventBusResponse `json:"EventBuses"`
}

// eventBusResponse is the handler-level DTO for EventBus objects. Timestamps
// are float64 Unix epoch seconds as required by the AWS JSON protocol (a raw
// time.Time would json.Marshal to an RFC3339 string instead). Policy is
// resolved from the backend's separate policy store at response time -- see
// EventBus.Policy's doc comment in models.go.
type eventBusResponse struct {
	Name             string  `json:"Name"`
	Arn              string  `json:"Arn"`
	Description      string  `json:"Description,omitempty"`
	Policy           string  `json:"Policy,omitempty"`
	CreationTime     float64 `json:"CreationTime"`
	LastModifiedTime float64 `json:"LastModifiedTime,omitempty"`
}

func (h *Handler) eventBusToResponse(ctx context.Context, bus *EventBus) *eventBusResponse {
	if bus == nil {
		return nil
	}

	policy, _ := h.Backend.GetEventBusPolicy(ctx, bus.Name)

	resp := &eventBusResponse{
		Name:         bus.Name,
		Arn:          bus.Arn,
		Description:  bus.Description,
		Policy:       policy,
		CreationTime: timeToEpochSeconds(bus.CreatedTime),
	}
	if !bus.LastModifiedTime.IsZero() {
		resp.LastModifiedTime = timeToEpochSeconds(bus.LastModifiedTime)
	}

	return resp
}

func (h *Handler) eventBusActions() map[string]actionFn {
	return map[string]actionFn{
		"CreateEventBus":   h.handleCreateEventBus,
		"DeleteEventBus":   h.handleDeleteEventBus,
		"ListEventBuses":   h.handleListEventBuses,
		"DescribeEventBus": h.handleDescribeEventBus,
	}
}

func (h *Handler) handleCreateEventBus(ctx context.Context, b []byte) (any, error) {
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

	return &createEventBusOutput{EventBusArn: bus.Arn, Description: bus.Description}, nil
}

func (h *Handler) handleDeleteEventBus(ctx context.Context, b []byte) (any, error) {
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
}

func (h *Handler) handleListEventBuses(ctx context.Context, b []byte) (any, error) {
	var input listEventBusesInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	buses, next, err := h.Backend.ListEventBuses(ctx, input.NamePrefix, input.NextToken, input.Limit)
	if err != nil {
		return nil, err
	}

	responses := make([]eventBusResponse, len(buses))
	for i := range buses {
		responses[i] = *h.eventBusToResponse(ctx, &buses[i])
	}

	return &listEventBusesOutput{EventBuses: responses, NextToken: next}, nil
}

func (h *Handler) handleDescribeEventBus(ctx context.Context, b []byte) (any, error) {
	var input describeEventBusInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	bus, err := h.Backend.DescribeEventBus(ctx, input.Name)
	if err != nil {
		return nil, err
	}

	return h.eventBusToResponse(ctx, bus), nil
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
