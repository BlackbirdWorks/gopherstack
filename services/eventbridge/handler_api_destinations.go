package eventbridge

import (
	"context"
	"encoding/json"
)

type createAPIDestinationOutput struct {
	APIDestinationArn   string  `json:"ApiDestinationArn"`
	APIDestinationState string  `json:"ApiDestinationState"`
	CreationTime        float64 `json:"CreationTime"`
	LastModifiedTime    float64 `json:"LastModifiedTime"`
}

type deleteAPIDestinationOutput struct{}

// apiDestinationActions returns the CreateApiDestination and DeleteApiDestination actions.
func (h *Handler) apiDestinationActions() map[string]actionFn {
	return map[string]actionFn{
		"CreateApiDestination": func(ctx context.Context, b []byte) (any, error) {
			var input CreateAPIDestinationInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			dst, err := h.Backend.CreateAPIDestination(ctx, input)
			if err != nil {
				return nil, err
			}

			return &createAPIDestinationOutput{
				APIDestinationArn:   dst.APIDestinationArn,
				APIDestinationState: dst.APIDestinationState,
				CreationTime:        timeToEpochSeconds(dst.CreationTime),
				LastModifiedTime:    timeToEpochSeconds(dst.LastModifiedTime),
			}, nil
		},
		"DeleteApiDestination": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				Name string `json:"Name"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			if err := h.Backend.DeleteAPIDestination(ctx, input.Name); err != nil {
				return nil, err
			}

			return &deleteAPIDestinationOutput{}, nil
		},
	}
}

// apiDestinationResponse is the handler-level DTO for DescribeApiDestination.
type apiDestinationResponse struct {
	APIDestinationArn            string  `json:"ApiDestinationArn"`
	APIDestinationState          string  `json:"ApiDestinationState"`
	ConnectionArn                string  `json:"ConnectionArn"`
	Description                  string  `json:"Description,omitempty"`
	HTTPMethod                   string  `json:"HttpMethod"`
	InvocationEndpoint           string  `json:"InvocationEndpoint"`
	Name                         string  `json:"Name"`
	CreationTime                 float64 `json:"CreationTime"`
	LastModifiedTime             float64 `json:"LastModifiedTime"`
	InvocationRateLimitPerSecond int     `json:"InvocationRateLimitPerSecond,omitempty"`
}

func apiDestinationToResponse(d *APIDestination) *apiDestinationResponse {
	if d == nil {
		return nil
	}

	return &apiDestinationResponse{
		CreationTime:                 timeToEpochSeconds(d.CreationTime),
		LastModifiedTime:             timeToEpochSeconds(d.LastModifiedTime),
		APIDestinationArn:            d.APIDestinationArn,
		APIDestinationState:          d.APIDestinationState,
		ConnectionArn:                d.ConnectionArn,
		Description:                  d.Description,
		HTTPMethod:                   d.HTTPMethod,
		InvocationEndpoint:           d.InvocationEndpoint,
		Name:                         d.Name,
		InvocationRateLimitPerSecond: d.InvocationRateLimitPerSecond,
	}
}

// apiDestinationSummary is ListApiDestinations' item shape (real
// "ApiDestination" type, eventbridge@v1.48.4 deserializers.go's
// awsAwsjson11_deserializeDocumentApiDestination case list): no Description
// at all, unlike DescribeApiDestination's apiDestinationResponse above.
type apiDestinationSummary struct {
	APIDestinationArn            string  `json:"ApiDestinationArn"`
	APIDestinationState          string  `json:"ApiDestinationState"`
	ConnectionArn                string  `json:"ConnectionArn"`
	HTTPMethod                   string  `json:"HttpMethod"`
	InvocationEndpoint           string  `json:"InvocationEndpoint"`
	Name                         string  `json:"Name"`
	CreationTime                 float64 `json:"CreationTime"`
	LastModifiedTime             float64 `json:"LastModifiedTime"`
	InvocationRateLimitPerSecond int     `json:"InvocationRateLimitPerSecond,omitempty"`
}

func apiDestinationToSummary(d *APIDestination) apiDestinationSummary {
	return apiDestinationSummary{
		CreationTime:                 timeToEpochSeconds(d.CreationTime),
		LastModifiedTime:             timeToEpochSeconds(d.LastModifiedTime),
		APIDestinationArn:            d.APIDestinationArn,
		APIDestinationState:          d.APIDestinationState,
		ConnectionArn:                d.ConnectionArn,
		HTTPMethod:                   d.HTTPMethod,
		InvocationEndpoint:           d.InvocationEndpoint,
		Name:                         d.Name,
		InvocationRateLimitPerSecond: d.InvocationRateLimitPerSecond,
	}
}

// extendedAPIDestinationActions returns Describe/List/Update for API destinations.
func (h *Handler) extendedAPIDestinationActions() map[string]actionFn {
	return map[string]actionFn{
		"DescribeApiDestination": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				Name string `json:"Name"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			dst, err := h.Backend.DescribeAPIDestination(ctx, input.Name)
			if err != nil {
				return nil, err
			}

			return apiDestinationToResponse(dst), nil
		},
		"ListApiDestinations": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				NamePrefix string `json:"NamePrefix"`
				NextToken  string `json:"NextToken"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			dsts, next, err := h.Backend.ListAPIDestinations(ctx, input.NamePrefix, input.NextToken)
			if err != nil {
				return nil, err
			}

			dstResponses := make([]apiDestinationSummary, len(dsts))
			for i, d := range dsts {
				dstResponses[i] = apiDestinationToSummary(&d)
			}

			return &struct {
				NextToken       string                  `json:"NextToken,omitempty"`
				APIDestinations []apiDestinationSummary `json:"ApiDestinations"`
			}{APIDestinations: dstResponses, NextToken: next}, nil
		},
		"UpdateApiDestination": func(ctx context.Context, b []byte) (any, error) {
			var input UpdateAPIDestinationInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			dst, err := h.Backend.UpdateAPIDestination(ctx, input)
			if err != nil {
				return nil, err
			}

			return &struct {
				APIDestinationArn   string  `json:"ApiDestinationArn"`
				APIDestinationState string  `json:"ApiDestinationState"`
				CreationTime        float64 `json:"CreationTime"`
				LastModifiedTime    float64 `json:"LastModifiedTime"`
			}{
				APIDestinationArn:   dst.APIDestinationArn,
				APIDestinationState: dst.APIDestinationState,
				CreationTime:        timeToEpochSeconds(dst.CreationTime),
				LastModifiedTime:    timeToEpochSeconds(dst.LastModifiedTime),
			}, nil
		},
	}
}
