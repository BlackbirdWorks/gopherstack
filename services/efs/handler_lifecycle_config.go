package efs

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

type putLifecycleConfigBody struct {
	LifecyclePolicies []LifecyclePolicy `json:"LifecyclePolicies"`
}

func (h *Handler) handleDescribeLifecycleConfiguration(c *echo.Context, fileSystemID string) error {
	policies, err := h.Backend.DescribeLifecycleConfiguration(h.contextWithRegion(c), fileSystemID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"LifecyclePolicies": policies,
	})
}

func (h *Handler) handlePutLifecycleConfiguration(
	c *echo.Context,
	fileSystemID string,
	body []byte,
) error {
	var in putLifecycleConfigBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("BadRequest", "invalid request body"))
	}

	stored, err := h.Backend.PutLifecycleConfiguration(
		h.contextWithRegion(c),
		fileSystemID,
		in.LifecyclePolicies,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"LifecyclePolicies": stored,
	})
}
