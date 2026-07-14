package eventbridge

import (
	"context"
	"encoding/json"
)

type createEndpointOutput struct {
	Arn         string `json:"Arn"`
	EndpointID  string `json:"EndpointId"`
	EndpointURL string `json:"EndpointUrl"`
	State       string `json:"State"`
}

// endpointActions returns the CreateEndpoint action.
func (h *Handler) endpointActions() map[string]actionFn {
	return map[string]actionFn{
		"CreateEndpoint": func(ctx context.Context, b []byte) (any, error) {
			var input CreateEndpointInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			ep, err := h.Backend.CreateEndpoint(ctx, input)
			if err != nil {
				return nil, err
			}

			return &createEndpointOutput{
				Arn:         ep.Arn,
				EndpointID:  ep.EndpointID,
				EndpointURL: ep.EndpointURL,
				State:       ep.State,
			}, nil
		},
	}
}

// extendedEndpointActions returns CRUD actions for endpoints beyond Create.
func (h *Handler) extendedEndpointActions() map[string]actionFn {
	return map[string]actionFn{
		"DeleteEndpoint": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				Name string `json:"Name"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return &struct{}{}, h.Backend.DeleteEndpoint(ctx, input.Name)
		},
		"DescribeEndpoint": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				Name string `json:"Name"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeEndpoint(ctx, input.Name)
		},
		"ListEndpoints": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				NamePrefix string `json:"NamePrefix"`
				NextToken  string `json:"NextToken"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			eps, next, err := h.Backend.ListEndpoints(ctx, input.NamePrefix, input.NextToken)
			if err != nil {
				return nil, err
			}

			return &struct {
				NextToken string     `json:"NextToken,omitempty"`
				Endpoints []Endpoint `json:"Endpoints"`
			}{Endpoints: eps, NextToken: next}, nil
		},
		"UpdateEndpoint": func(ctx context.Context, b []byte) (any, error) {
			var input UpdateEndpointInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			ep, err := h.Backend.UpdateEndpoint(ctx, input)
			if err != nil {
				return nil, err
			}

			return &struct {
				Arn         string `json:"Arn"`
				EndpointID  string `json:"EndpointId"`
				EndpointURL string `json:"EndpointUrl"`
				State       string `json:"State"`
			}{
				Arn:         ep.Arn,
				EndpointID:  ep.EndpointID,
				EndpointURL: ep.EndpointURL,
				State:       ep.State,
			}, nil
		},
	}
}
