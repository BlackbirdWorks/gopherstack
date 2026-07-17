package sesv2

import (
	"encoding/json"
	"fmt"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleGetDedicatedIP(ip string) (any, error) {
	info, err := h.Backend.GetDedicatedIP(ip)
	if err != nil {
		return nil, err
	}

	return map[string]any{"DedicatedIp": info}, nil
}

func (h *Handler) handleGetDedicatedIps() (any, error) {
	ips := h.Backend.GetDedicatedIps()

	return map[string]any{"DedicatedIps": ips}, nil
}

type putDedicatedIPInPoolInput struct {
	DestinationPoolName string `json:"DestinationPoolName"`
}

func (h *Handler) handlePutDedicatedIPInPool(c *echo.Context, ip string) (any, error) {
	var in putDedicatedIPInPoolInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
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

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	if err := h.Backend.PutDedicatedIPWarmupAttributes(ip, in.WarmupPercentage); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}
