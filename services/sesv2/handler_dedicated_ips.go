package sesv2

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"strconv"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

func (h *Handler) handleGetDedicatedIP(ip string) (any, error) {
	info, err := h.Backend.GetDedicatedIP(ip)
	if err != nil {
		return nil, err
	}

	return map[string]any{"DedicatedIp": info}, nil
}

func (h *Handler) handleGetDedicatedIps(c *echo.Context) (any, error) {
	poolName := c.QueryParam("PoolName")
	nextToken := c.QueryParam("NextToken")

	pageSize := 0
	if v := c.QueryParam("PageSize"); v != "" {
		pageSize, _ = strconv.Atoi(v)
	}

	pg := h.Backend.GetDedicatedIps(poolName, nextToken, pageSize)

	return map[string]any{
		"DedicatedIps": pg.Data,
		keyNextToken:   pg.Next,
	}, nil
}

type putDedicatedIPInPoolInput struct {
	DestinationPoolName string `json:"DestinationPoolName"`
}

func (h *Handler) handlePutDedicatedIPInPool(c *echo.Context, ip string) (any, error) {
	var in putDedicatedIPInPoolInput

	if err := json.UnmarshalDecode(jsontext.NewDecoder(c.Request().Body), &in, jsonx.V1()); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	if err := h.Backend.PutDedicatedIPInPool(ip, in.DestinationPoolName); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

type putDedicatedIPWarmupInput struct {
	WarmupPercentage int `json:"WarmupPercentage"`
}

func (h *Handler) handlePutDedicatedIPWarmupAttributes(c *echo.Context, ip string) (any, error) {
	var in putDedicatedIPWarmupInput

	if err := json.UnmarshalDecode(jsontext.NewDecoder(c.Request().Body), &in, jsonx.V1()); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	if err := h.Backend.PutDedicatedIPWarmupAttributes(ip, in.WarmupPercentage); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}
