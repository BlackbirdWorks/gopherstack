package sesv2

import (
	"encoding/json"
	"fmt"

	"github.com/labstack/echo/v5"
)

type createDedicatedIPPoolInput struct {
	PoolName    string     `json:"PoolName"`
	ScalingMode string     `json:"ScalingMode"`
	Tags        []tagEntry `json:"Tags"`
}

func (h *Handler) handleCreateDedicatedIPPool(c *echo.Context) (any, error) {
	var in createDedicatedIPPoolInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	if _, err := h.Backend.CreateDedicatedIPPool(in.PoolName, in.ScalingMode, tagsFromEntries(in.Tags)); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

// dedicated IP pool handlers

func (h *Handler) handleGetDedicatedIPPool(poolName string) (any, error) {
	pool, err := h.Backend.GetDedicatedIPPool(poolName)
	if err != nil {
		return nil, err
	}

	return map[string]any{"DedicatedIpPool": toDedicatedIPPoolOutput(pool)}, nil
}

func (h *Handler) handleDeleteDedicatedIPPool(poolName string) (any, error) {
	if err := h.Backend.DeleteDedicatedIPPool(poolName); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

func (h *Handler) handleListDedicatedIPPools(c *echo.Context) (any, error) {
	nextToken := c.QueryParam("NextToken")
	pg := h.Backend.ListDedicatedIPPools(nextToken, 0)

	return map[string]any{
		"DedicatedIpPools": pg.Data,
		keyNextToken:       pg.Next,
	}, nil
}

type putDedicatedIPPoolScalingInput struct {
	ScalingMode string `json:"ScalingMode"`
}

func (h *Handler) handlePutDedicatedIPPoolScalingAttributes(
	c *echo.Context,
	poolName string,
) (any, error) {
	var in putDedicatedIPPoolScalingInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	if err := h.Backend.PutDedicatedIPPoolScalingAttributes(poolName, in.ScalingMode); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}
