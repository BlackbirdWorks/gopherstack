package cloudtrail

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

// --- DeleteResourcePolicy ---

type deleteResourcePolicyBody struct {
	ResourceArn string `json:"ResourceArn"`
}

func (h *Handler) handleDeleteResourcePolicy(c *echo.Context, body []byte) error {
	var in deleteResourcePolicyBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	if err := h.Backend.DeleteResourcePolicy(in.ResourceArn); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// --- GetResourcePolicy ---

type getResourcePolicyBody struct {
	ResourceArn string `json:"ResourceArn"`
}

func (h *Handler) handleGetResourcePolicy(c *echo.Context, body []byte) error {
	var in getResourcePolicyBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	rp, err := h.Backend.GetResourcePolicy(in.ResourceArn)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyResourceArn:   rp.ResourceARN,
		"ResourcePolicy": rp.ResourcePolicy,
	})
}

// --- PutResourcePolicy ---

type putResourcePolicyBody struct {
	ResourceArn    string `json:"ResourceArn"`
	ResourcePolicy string `json:"ResourcePolicy"`
}

func (h *Handler) handlePutResourcePolicy(c *echo.Context, body []byte) error {
	var in putResourcePolicyBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	rp := h.Backend.PutResourcePolicy(in.ResourceArn, in.ResourcePolicy)

	return c.JSON(http.StatusOK, map[string]any{
		keyResourceArn:   rp.ResourceARN,
		"ResourcePolicy": rp.ResourcePolicy,
	})
}
