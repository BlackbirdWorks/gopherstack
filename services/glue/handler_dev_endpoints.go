package glue

import (
	"context"
)

type batchGetDevEndpointsInput struct {
	DevEndpointNames []string `json:"DevEndpointNames"`
}

type batchGetDevEndpointsOutput struct {
	DevEndpoints         []*DevEndpoint `json:"DevEndpoints"`
	DevEndpointsNotFound []string       `json:"DevEndpointsNotFound"`
}

func (h *Handler) handleBatchGetDevEndpoints(
	_ context.Context,
	in *batchGetDevEndpointsInput,
) (*batchGetDevEndpointsOutput, error) {
	found, missing := h.Backend.BatchGetDevEndpoints(in.DevEndpointNames)

	return &batchGetDevEndpointsOutput{DevEndpoints: found, DevEndpointsNotFound: missing}, nil
}

// createDevEndpointInput holds input for CreateDevEndpoint.
type createDevEndpointInput struct {
	EndpointName string `json:"EndpointName"`
}

// createDevEndpointOutput holds the result for CreateDevEndpoint.
type createDevEndpointOutput struct {
	EndpointName string `json:"EndpointName"`
	Status       string `json:"Status"`
}

func (h *Handler) handleCreateDevEndpoint(
	_ context.Context,
	in *createDevEndpointInput,
) (*createDevEndpointOutput, error) {
	dep, err := h.Backend.CreateDevEndpoint(in.EndpointName)
	if err != nil {
		return nil, err
	}

	return &createDevEndpointOutput{EndpointName: dep.EndpointName, Status: dep.Status}, nil
}

// deleteDevEndpointInput holds input for DeleteDevEndpoint.
type deleteDevEndpointInput struct {
	EndpointName string `json:"EndpointName"`
}

func (h *Handler) handleDeleteDevEndpoint(
	_ context.Context,
	in *deleteDevEndpointInput,
) (*emptyOutput, error) {
	if err := h.Backend.DeleteDevEndpoint(in.EndpointName); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

// getDevEndpointInput holds input for GetDevEndpoint.
type getDevEndpointInput struct {
	EndpointName string `json:"EndpointName"`
}

// getDevEndpointOutput holds the result for GetDevEndpoint.
type getDevEndpointOutput struct {
	DevEndpoint *DevEndpoint `json:"DevEndpoint"`
}

func (h *Handler) handleGetDevEndpoint(
	_ context.Context,
	in *getDevEndpointInput,
) (*getDevEndpointOutput, error) {
	dep, err := h.Backend.GetDevEndpoint(in.EndpointName)
	if err != nil {
		return nil, err
	}

	return &getDevEndpointOutput{DevEndpoint: dep}, nil
}

// getDevEndpointsInput holds input for GetDevEndpoints.
type getDevEndpointsInput struct{}

// getDevEndpointsOutput holds the result for GetDevEndpoints.
type getDevEndpointsOutput struct {
	DevEndpoints []*DevEndpoint `json:"DevEndpoints"`
}

func (h *Handler) handleGetDevEndpoints(
	_ context.Context,
	_ *getDevEndpointsInput,
) (*getDevEndpointsOutput, error) {
	return &getDevEndpointsOutput{DevEndpoints: h.Backend.GetAllDevEndpoints()}, nil
}

// listDevEndpointsInput holds input for ListDevEndpoints.
type listDevEndpointsInput struct{}

// listDevEndpointsOutput holds the result for ListDevEndpoints.
type listDevEndpointsOutput struct {
	DevEndpointNames []string `json:"DevEndpointNames"`
}

func (h *Handler) handleListDevEndpoints(
	_ context.Context,
	_ *listDevEndpointsInput,
) (*listDevEndpointsOutput, error) {
	deps := h.Backend.GetAllDevEndpoints()
	names := make([]string, 0, len(deps))
	for _, d := range deps {
		names = append(names, d.EndpointName)
	}

	return &listDevEndpointsOutput{DevEndpointNames: names}, nil
}

// updateDevEndpointInput holds input for UpdateDevEndpoint.
type updateDevEndpointInput struct {
	AddArguments map[string]string `json:"AddArguments,omitempty"`
	EndpointName string            `json:"EndpointName"`
}

func (h *Handler) handleUpdateDevEndpoint(
	_ context.Context,
	in *updateDevEndpointInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.UpdateDevEndpoint(in.EndpointName, in.AddArguments)
}
