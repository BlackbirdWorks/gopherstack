package efs

import (
	"encoding/json/v2"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

func (h *Handler) handleDescribeMountTargetSecurityGroups(
	c *echo.Context,
	mountTargetID string,
) error {
	groups, err := h.Backend.DescribeMountTargetSecurityGroups(
		h.contextWithRegion(c),
		mountTargetID,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"SecurityGroups": groups,
	})
}

type modifyMountTargetSGBody struct {
	SecurityGroups []string `json:"SecurityGroups"`
}

func (h *Handler) handleModifyMountTargetSecurityGroups(
	c *echo.Context,
	mountTargetID string,
	body []byte,
) error {
	var in modifyMountTargetSGBody
	if err := json.Unmarshal(body, &in, jsonx.V1()); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("BadRequest", "invalid request body"))
	}

	ctx := h.contextWithRegion(c)
	if err := h.Backend.ModifyMountTargetSecurityGroups(ctx, mountTargetID, in.SecurityGroups); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}
