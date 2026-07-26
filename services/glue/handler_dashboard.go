package glue

import (
	"context"
)

type getDashboardURLInput struct {
	ResourceID    string `json:"ResourceId"`
	ResourceType  string `json:"ResourceType"`
	RequestOrigin string `json:"RequestOrigin,omitempty"`
}

type getDashboardURLOutput struct {
	URL string `json:"Url"`
}

func (h *Handler) handleGetDashboardURL(_ context.Context, in *getDashboardURLInput) (*getDashboardURLOutput, error) {
	url, err := h.Backend.GetDashboardURL(in.ResourceID, in.ResourceType, in.RequestOrigin)
	if err != nil {
		return nil, err
	}

	return &getDashboardURLOutput{URL: url}, nil
}

type getSessionEndpointInput struct {
	SessionID string `json:"SessionId"`
}

type getSessionEndpointOutput struct {
	SparkConnect *SessionEndpoint `json:"SparkConnect"`
}

func (h *Handler) handleGetSessionEndpoint(
	_ context.Context,
	in *getSessionEndpointInput,
) (*getSessionEndpointOutput, error) {
	ep, err := h.Backend.GetSessionEndpoint(in.SessionID)
	if err != nil {
		return nil, err
	}

	return &getSessionEndpointOutput{SparkConnect: ep}, nil
}
