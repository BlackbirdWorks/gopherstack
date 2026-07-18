package efs

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleDescribeFileSystemPolicy(c *echo.Context, fileSystemID string) error {
	policy, err := h.Backend.DescribeFileSystemPolicy(h.contextWithRegion(c), fileSystemID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyFileSystemID: fileSystemID,
		"Policy":        policy,
	})
}

func (h *Handler) handleDeleteFileSystemPolicy(c *echo.Context, fileSystemID string) error {
	if err := h.Backend.DeleteFileSystemPolicy(h.contextWithRegion(c), fileSystemID); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

type putFileSystemPolicyBody struct {
	Policy                         string `json:"Policy"`
	BypassPolicyLockoutSafetyCheck bool   `json:"BypassPolicyLockoutSafetyCheck"`
}

func (h *Handler) handlePutFileSystemPolicy(
	c *echo.Context,
	fileSystemID string,
	body []byte,
) error {
	var in putFileSystemPolicyBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("BadRequest", "invalid request body"))
	}

	if err := h.Backend.PutFileSystemPolicy(h.contextWithRegion(c), fileSystemID, in.Policy); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyFileSystemID: fileSystemID,
		"Policy":        in.Policy,
	})
}
