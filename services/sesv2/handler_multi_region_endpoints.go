package sesv2

import (
	"encoding/json"
	"fmt"

	"github.com/labstack/echo/v5"
)

// multi-region endpoint handlers

type createMultiRegionEndpointInput struct {
	EndpointName string `json:"EndpointName"`
}

func (h *Handler) handleCreateMultiRegionEndpoint(c *echo.Context) (any, error) {
	var in createMultiRegionEndpointInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	status, err := h.Backend.CreateMultiRegionEndpoint(in.EndpointName)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyStatus: status}, nil
}

func (h *Handler) handleGetMultiRegionEndpoint(name string) (any, error) {
	result, err := h.Backend.GetMultiRegionEndpoint(name)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (h *Handler) handleDeleteMultiRegionEndpoint(name string) (any, error) {
	if err := h.Backend.DeleteMultiRegionEndpoint(name); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

func (h *Handler) handleListMultiRegionEndpoints(c *echo.Context) (any, error) {
	nextToken := c.QueryParam("NextToken")

	items, next, err := h.Backend.ListMultiRegionEndpoints(nextToken, 0)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"MultiRegionEndpoints": items,
		keyNextToken:           next,
	}, nil
}
