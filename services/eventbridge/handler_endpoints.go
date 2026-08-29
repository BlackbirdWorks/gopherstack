package eventbridge

import (
	"context"
	"encoding/json"
)

// createEndpointOutput matches real CreateEndpointOutput
// (eventbridge@v1.48.4 deserializers.go): Arn/EventBuses/Name/
// ReplicationConfig/RoleArn/RoutingConfig/State -- notably NOT EndpointId or
// EndpointUrl, which the real op does not return synchronously (a client
// must call DescribeEndpoint separately for those). EndpointID/EndpointURL
// were previously emitted here as invented fields (harmless -- no real
// field to decode them into) while EventBuses/Name/ReplicationConfig/
// RoleArn/RoutingConfig, all already known from the just-created backend
// object, were dropped.
type createEndpointOutput struct {
	ReplicationConfig *ReplicationConfig `json:"ReplicationConfig,omitempty"`
	RoutingConfig     *RoutingConfig     `json:"RoutingConfig,omitempty"`
	Arn               string             `json:"Arn"`
	Name              string             `json:"Name"`
	RoleArn           string             `json:"RoleArn,omitempty"`
	State             string             `json:"State"`
	EventBuses        []EndpointEventBus `json:"EventBuses,omitempty"`
}

// endpointResponse is the handler-level DTO for Endpoint objects. Timestamps
// are float64 Unix epoch seconds as required by the AWS JSON protocol (a raw
// time.Time field would json.Marshal to an RFC3339 string instead).
type endpointResponse struct {
	ReplicationConfig *ReplicationConfig `json:"ReplicationConfig,omitempty"`
	RoutingConfig     *RoutingConfig     `json:"RoutingConfig,omitempty"`
	Name              string             `json:"Name"`
	RoleArn           string             `json:"RoleArn,omitempty"`
	EndpointURL       string             `json:"EndpointUrl"`
	EndpointID        string             `json:"EndpointId"`
	Description       string             `json:"Description,omitempty"`
	Arn               string             `json:"Arn"`
	State             string             `json:"State"`
	StateReason       string             `json:"StateReason,omitempty"`
	EventBuses        []EndpointEventBus `json:"EventBuses,omitempty"`
	LastModifiedTime  float64            `json:"LastModifiedTime,omitempty"`
	CreationTime      float64            `json:"CreationTime,omitempty"`
}

func endpointToResponse(ep *Endpoint) *endpointResponse {
	if ep == nil {
		return nil
	}

	resp := &endpointResponse{
		ReplicationConfig: ep.ReplicationConfig,
		RoutingConfig:     ep.RoutingConfig,
		RoleArn:           ep.RoleArn,
		EndpointURL:       ep.EndpointURL,
		Name:              ep.Name,
		EndpointID:        ep.EndpointID,
		Description:       ep.Description,
		Arn:               ep.Arn,
		State:             ep.State,
		StateReason:       ep.StateReason,
		EventBuses:        ep.EventBuses,
	}
	if !ep.CreationTime.IsZero() {
		resp.CreationTime = timeToEpochSeconds(ep.CreationTime)
	}
	if !ep.LastModifiedTime.IsZero() {
		resp.LastModifiedTime = timeToEpochSeconds(ep.LastModifiedTime)
	}

	return resp
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
				Arn:               ep.Arn,
				Name:              ep.Name,
				State:             ep.State,
				RoleArn:           ep.RoleArn,
				EventBuses:        ep.EventBuses,
				ReplicationConfig: ep.ReplicationConfig,
				RoutingConfig:     ep.RoutingConfig,
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
			ep, err := h.Backend.DescribeEndpoint(ctx, input.Name)
			if err != nil {
				return nil, err
			}

			return endpointToResponse(ep), nil
		},
		"ListEndpoints": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				NamePrefix string `json:"NamePrefix"`
				NextToken  string `json:"NextToken"`
				MaxResults int    `json:"MaxResults"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			eps, next, err := h.Backend.ListEndpoints(ctx, input.NamePrefix, input.NextToken, input.MaxResults)
			if err != nil {
				return nil, err
			}

			responses := make([]endpointResponse, len(eps))
			for i := range eps {
				responses[i] = *endpointToResponse(&eps[i])
			}

			return &struct {
				NextToken string             `json:"NextToken,omitempty"`
				Endpoints []endpointResponse `json:"Endpoints"`
			}{Endpoints: responses, NextToken: next}, nil
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

			// Real UpdateEndpointOutput (eventbridge@v1.48.4 deserializers.go)
			// has Arn/EndpointId/EndpointUrl/EventBuses/Name/ReplicationConfig/
			// RoleArn/RoutingConfig/State -- EventBuses/Name/ReplicationConfig/
			// RoleArn/RoutingConfig were previously dropped despite being
			// already known from the just-updated backend object.
			return &struct {
				ReplicationConfig *ReplicationConfig `json:"ReplicationConfig,omitempty"`
				RoutingConfig     *RoutingConfig     `json:"RoutingConfig,omitempty"`
				Arn               string             `json:"Arn"`
				EndpointID        string             `json:"EndpointId"`
				EndpointURL       string             `json:"EndpointUrl"`
				Name              string             `json:"Name"`
				RoleArn           string             `json:"RoleArn,omitempty"`
				State             string             `json:"State"`
				EventBuses        []EndpointEventBus `json:"EventBuses,omitempty"`
			}{
				Arn:               ep.Arn,
				EndpointID:        ep.EndpointID,
				EndpointURL:       ep.EndpointURL,
				Name:              ep.Name,
				State:             ep.State,
				RoleArn:           ep.RoleArn,
				EventBuses:        ep.EventBuses,
				ReplicationConfig: ep.ReplicationConfig,
				RoutingConfig:     ep.RoutingConfig,
			}, nil
		},
	}
}
