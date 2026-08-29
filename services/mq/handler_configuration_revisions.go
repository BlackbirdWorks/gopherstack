package mq

import (
	"net/http"
	"strconv"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

func (h *Handler) handleListConfigurationRevisions(c *echo.Context, configID string) error {
	revisions, err := h.Backend.ListConfigurationRevisions(configID)
	if err != nil {
		return h.writeError(c, err)
	}

	q := c.Request().URL.Query()
	nextToken := q.Get("nextToken")
	maxResults := 0

	if s := q.Get("maxResults"); s != "" {
		if n, parseErr := strconv.Atoi(s); parseErr == nil && n > 0 && n <= 100 {
			maxResults = n
		}
	}

	pg := page.New(revisions, nextToken, maxResults, mqDefaultPageSize)

	resp := map[string]any{
		"configurationId": configID,
		"revisions":       pg.Data,
	}
	if pg.Next != "" {
		resp["nextToken"] = pg.Next
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleDescribeConfigurationRevision(
	c *echo.Context,
	configID, revisionStr string,
) error {
	parsed, err := strconv.ParseInt(revisionStr, 10, 32)
	if err != nil {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("BadRequestException", "invalid revision number"),
		)
	}

	revision := int32(parsed)

	rev, data, err := h.Backend.DescribeConfigurationRevision(configID, revision)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"configurationId": configID,
		keyCreated:        rev.Created,
		"description":     rev.Description,
		"revision":        rev.Revision,
		"data":            data,
	})
}
