package mq

import (
	"net/http"
	"strconv"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleListConfigurationRevisions(c *echo.Context, configID string) error {
	revisions, err := h.Backend.ListConfigurationRevisions(configID)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"configurationId": configID,
		"revisions":       revisions,
	})
}

func (h *Handler) handleDescribeConfigurationRevision(c *echo.Context, configID, revisionStr string) error {
	parsed, err := strconv.ParseInt(revisionStr, 10, 32)
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid revision number"))
	}

	revision := int32(parsed)

	rev, data, err := h.Backend.DescribeConfigurationRevision(configID, revision)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"configurationId": configID,
		"created":         rev.Created,
		"description":     rev.Description,
		"revision":        rev.Revision,
		"data":            data,
	})
}
